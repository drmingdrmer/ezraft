//! The harness these cluster tests share: a KV application, storage that survives a
//! node instance, and the waits that make cluster state assertable.
//!
//! Each file under `tests/` is its own binary and uses a subset of this, so unused
//! items here are expected rather than dead.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use ezraft::EzApp;
use ezraft::EzConfig;
use ezraft::EzEntry;
use ezraft::EzMeta;
use ezraft::EzRaft;
use ezraft::EzSnapshot;
use ezraft::EzSnapshotMeta;
use ezraft::EzStorage;
use ezraft::Loaded;
use ezraft::Persist;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, derive_more::Display)]
pub enum Request {
    #[display("Set({key})")]
    Set { key: String, value: String },
    #[display("Get({key})")]
    Get { key: String },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub value: Option<String>,
}

pub fn set(key: &str, value: &str) -> Request {
    Request::Set {
        key: key.into(),
        value: value.into(),
    }
}

pub fn get(key: &str) -> Request {
    Request::Get { key: key.into() }
}

/// KV app; the framework owns the value, so tests read the applied state
/// back through `EzRaft::read` to compare the whole state instead of
/// sampling it key by key.
#[derive(Default, Serialize, Deserialize)]
pub struct KvSm {
    pub data: BTreeMap<String, String>,

    /// Which bucket of `APPLIED` this state records its applies in, empty unless a test asked
    /// for them. Part of the serialized state, so it survives a snapshot restore and a restart
    /// and replayed applies land in the same bucket as the originals.
    #[serde(default)]
    tag: String,
}

/// Every `apply` this process ran, in order, per tag
static APPLIED: Mutex<BTreeMap<String, Vec<String>>> = Mutex::new(BTreeMap::new());

pub fn tagged(tag: &str) -> KvSm {
    KvSm {
        tag: tag.into(),
        ..KvSm::default()
    }
}

pub fn applied(tag: &str) -> Vec<String> {
    APPLIED.lock().unwrap().get(tag).cloned().unwrap_or_default()
}

#[async_trait]
impl EzApp for KvSm {
    type Request = Request;
    type Response = Response;

    async fn apply(&mut self, req: Request) -> Response {
        if !self.tag.is_empty() {
            APPLIED.lock().unwrap().entry(self.tag.clone()).or_default().push(req.to_string());
        }

        match req {
            Request::Set { key, value } => {
                self.data.insert(key, value);
                Response { value: None }
            }
            Request::Get { key } => Response {
                value: self.data.get(&key).cloned(),
            },
        }
    }

    type ReadRequest = String;
    type ReadResponse = Option<String>;

    fn read(&self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
    }
}

/// What one node has "written to disk"; kept outside the storage instance so a
/// restarted node starts from it, exactly like a process reading real files.
#[derive(Default)]
pub struct Disk {
    pub meta: EzMeta,
    /// Log entries as serialized bytes, the same shape a real disk would hold.
    pub logs: BTreeMap<u64, Vec<u8>>,
    pub snapshot: Option<(EzSnapshotMeta, Vec<u8>)>,
}

#[derive(Clone, Default)]
pub struct MemStorage {
    pub disk: Arc<Mutex<Disk>>,
}

#[async_trait]
impl EzStorage<KvSm> for MemStorage {
    async fn load(&mut self) -> io::Result<Loaded> {
        let disk = self.disk.lock().unwrap();
        let snapshot = disk.snapshot.as_ref().map(|(meta, data)| EzSnapshot {
            meta: meta.clone(),
            snapshot: Cursor::new(data.clone()),
        });
        Ok(Loaded {
            meta: disk.meta.clone(),
            snapshot,
        })
    }

    async fn persist(&mut self, op: Persist<KvSm>) -> io::Result<()> {
        let mut disk = self.disk.lock().unwrap();
        match op {
            Persist::Meta(meta) => disk.meta = meta,
            Persist::LogEntry(entry) => {
                disk.logs.insert(entry.log_id.1, serde_json::to_vec(&entry)?);
            }
            Persist::Snapshot(snapshot) => {
                disk.snapshot = Some((snapshot.meta, snapshot.snapshot.into_inner()));
            }
            Persist::DeleteLogs { from, to } => disk.logs.retain(|&index, _| !(from..to).contains(&index)),
        }
        Ok(())
    }

    async fn read_logs(&mut self, start: u64, end: u64) -> io::Result<Vec<EzEntry<KvSm>>> {
        let disk = self.disk.lock().unwrap();
        (start..end)
            .map(|index| {
                let data =
                    disk.logs.get(&index).ok_or_else(|| io::Error::other(format!("missing log entry {}", index)))?;
                Ok(serde_json::from_slice(data)?)
            })
            .collect()
    }
}

/// Grab a free port; the listener is dropped so EzRaft can bind it.
pub fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

/// Short heartbeat so elections and failovers finish in test time.
pub fn config() -> EzConfig {
    EzConfig {
        heartbeat_interval: Duration::from_millis(100),
        ..EzConfig::default()
    }
}

/// Upper bound for every cluster-state wait in these tests
pub const WAIT: Option<Duration> = Some(Duration::from_secs(30));

pub fn expected_map(range: std::ops::Range<u32>) -> BTreeMap<String, String> {
    range.map(|i| (format!("k{}", i), format!("v{}", i))).collect()
}

/// Write through a node's HTTP API, whichever node it is.
///
/// `EzRaft::write` only writes where it is called and fails elsewhere with
/// `ForwardToLeader`; finding the leader belongs to the server layer, and this
/// is the path that exercises it. So any test that does not know which node
/// leads - after a failover, a transfer, or a demotion - writes through here.
pub async fn http_write(addr: &str, req: Request) -> io::Result<Response> {
    let resp = reqwest::Client::new()
        .post(format!("http://{}/api/write", addr))
        .json(&req)
        .send()
        .await
        .map_err(io::Error::other)?;

    let status = resp.status();
    let text = resp.text().await.map_err(io::Error::other)?;
    assert!(status.is_success(), "POST /api/write responded {}: {}", status, text);

    Ok(serde_json::from_str(&text)?)
}

/// Serve a node in the background, which is also where a join's promotion is
/// collected - a joined node reaches the voter set only once this is running.
pub fn spawn_serve(node: &EzRaft<KvSm>) {
    let node = node.clone();
    tokio::spawn(async move { node.serve().await });
}

/// A founding node, serving and leading.
pub async fn founding_node() -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::create(&addr, KvSm::default(), MemStorage::default(), config()).await?;
    spawn_serve(&node);

    // Leading is not enough. `create` writes the founding membership and winning the election
    // does not commit it, so a caller that changes the membership next is refused for having one
    // already in flight - which is a race it loses only under load.
    node.inner()
        .wait(WAIT)
        .metrics(
            |m| m.current_leader == Some(0) && m.committed_membership_config.log_id().is_some(),
            "founding node leads, with its own membership committed",
        )
        .await
        .map_err(io::Error::other)?;
    Ok((addr, node))
}

/// A node that joined asking to be a voter, serving. Returns before the
/// promotion lands, so callers wait on the voter set.
pub async fn joined_voter(seed: &str) -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::join(&addr, seed, KvSm::default(), MemStorage::default(), config()).await?;
    spawn_serve(&node);
    Ok((addr, node))
}

/// A node that joined as a learner, serving.
pub async fn joined_learner(seed: &str) -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::join_as_learner(&addr, seed, KvSm::default(), MemStorage::default(), config()).await?;
    spawn_serve(&node);
    Ok((addr, node))
}

/// Wait until `node` has *committed* exactly `voters`, as a uniform config.
///
/// Committed, not merely appended: an appended membership is still a change in
/// flight, and the next one a test asks for is refused while it is. Uniform,
/// not joint: a joint config still counts the old voter set, so a test that
/// killed a node on the strength of it would be racing.
pub async fn wait_for_voters(node: &EzRaft<KvSm>, voters: BTreeSet<u64>, reason: &str) -> io::Result<()> {
    node.inner()
        .wait(WAIT)
        .metrics(
            |m| *m.committed_membership_config.membership().get_joint_config() == [voters.clone()],
            reason,
        )
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

/// Wait until `node` has applied everything `leader` had logged when asked.
///
/// Anchored to the leader's index, because a node that has replicated nothing
/// has its own `last_applied` and `last_log_index` both unset, and comparing
/// those two would pass instantly.
pub async fn wait_for_applied(node: &EzRaft<KvSm>, leader: &EzRaft<KvSm>) -> io::Result<()> {
    let target = leader.metrics().await.last_log_index.expect("the leader has written entries");
    node.inner()
        .wait(WAIT)
        .metrics(
            |m| m.last_applied.map(|log_id| log_id.index) >= Some(target),
            "applied the leader's log",
        )
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

/// Ask a node for a membership change and return its answer, redirect and all.
pub async fn admin_answer(addr: &str, body: serde_json::Value) -> io::Result<Redirect> {
    let resp = reqwest::Client::new()
        .post(format!("http://{}/api/membership", addr))
        .json(&body)
        .send()
        .await
        .map_err(io::Error::other)?;

    let status = resp.status();
    let text = resp.text().await.map_err(io::Error::other)?;
    assert!(
        status.is_success(),
        "POST /api/membership responded {}: {}",
        status,
        text
    );

    Ok(serde_json::from_str(&text)?)
}

/// What an admin endpoint answers: done, or the leader to ask instead.
pub type Redirect = Result<(), Option<String>>;

/// Drive a membership change over HTTP, the way an operator would.
///
/// A redirect is a test failure: these tests always ask the leader, so being
/// told to ask someone else means the test set itself up wrong.
pub async fn admin_post(addr: &str, body: serde_json::Value) -> io::Result<()> {
    admin_answer(addr, body)
        .await?
        .map_err(|leader| io::Error::other(format!("/api/membership redirected to {:?}", leader)))
}
