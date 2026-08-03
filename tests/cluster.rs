//! Cluster-level tests driving EzRaft through its public API only: real HTTP
//! between nodes, in-memory persistence that survives a node instance so a
//! restart reads exactly what the previous incarnation persisted.

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
use ezraft::admin::AdminClient;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, derive_more::Display)]
enum Request {
    #[display("Set({key})")]
    Set { key: String, value: String },
    #[display("Get({key})")]
    Get { key: String },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Response {
    value: Option<String>,
}

fn set(key: &str, value: &str) -> Request {
    Request::Set {
        key: key.into(),
        value: value.into(),
    }
}

fn get(key: &str) -> Request {
    Request::Get { key: key.into() }
}

/// KV app; the framework owns the value, so tests read the applied state
/// back through `EzRaft::read` to compare the whole state instead of
/// sampling it key by key.
#[derive(Default, Serialize, Deserialize)]
struct KvSm {
    data: BTreeMap<String, String>,
}

#[async_trait]
impl EzApp for KvSm {
    type Request = Request;
    type Response = Response;

    async fn apply(&mut self, req: Request) -> Response {
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
struct Disk {
    meta: EzMeta,
    /// Log entries as serialized bytes, the same shape a real disk would hold.
    logs: BTreeMap<u64, Vec<u8>>,
    snapshot: Option<(EzSnapshotMeta, Vec<u8>)>,
}

#[derive(Clone, Default)]
struct MemStorage {
    disk: Arc<Mutex<Disk>>,
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
fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

/// Short heartbeat so elections and failovers finish in test time.
fn config() -> EzConfig {
    EzConfig {
        heartbeat_interval: Duration::from_millis(100),
        ..EzConfig::default()
    }
}

/// Upper bound for every cluster-state wait in these tests
const WAIT: Option<Duration> = Some(Duration::from_secs(30));

fn expected_map(range: std::ops::Range<u32>) -> BTreeMap<String, String> {
    range.map(|i| (format!("k{}", i), format!("v{}", i))).collect()
}

/// Write through a node's HTTP API, whichever node it is.
///
/// `EzRaft::write` only writes where it is called and fails elsewhere with
/// `ForwardToLeader`; finding the leader belongs to the server layer, and this
/// is the path that exercises it. So any test that does not know which node
/// leads - after a failover, a transfer, or a demotion - writes through here.
async fn http_write(addr: &str, req: Request) -> io::Result<Response> {
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
fn spawn_serve(node: &EzRaft<KvSm>) {
    let node = node.clone();
    tokio::spawn(async move { node.serve().await });
}

/// A founding node, serving and leading.
async fn founding_node() -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::create(&addr, KvSm::default(), MemStorage::default(), config()).await?;
    spawn_serve(&node);
    node.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "founding node leads")
        .await
        .map_err(io::Error::other)?;
    Ok((addr, node))
}

/// A node that joined asking to be a voter, serving. Returns before the
/// promotion lands, so callers wait on the voter set.
async fn joined_voter(seed: &str) -> io::Result<(String, EzRaft<KvSm>)> {
    let addr = free_addr();
    let node = EzRaft::join(&addr, seed, KvSm::default(), MemStorage::default(), config()).await?;
    spawn_serve(&node);
    Ok((addr, node))
}

/// A node that joined as a learner, serving.
async fn joined_learner(seed: &str) -> io::Result<(String, EzRaft<KvSm>)> {
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
async fn wait_for_voters(node: &EzRaft<KvSm>, voters: BTreeSet<u64>, reason: &str) -> io::Result<()> {
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
async fn wait_for_applied(node: &EzRaft<KvSm>, leader: &EzRaft<KvSm>) -> io::Result<()> {
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
async fn admin_answer(addr: &str, body: serde_json::Value) -> io::Result<Redirect> {
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
type Redirect = Result<(), Option<String>>;

/// Drive a membership change over HTTP, the way an operator would.
///
/// A redirect is a test failure: these tests always ask the leader, so being
/// told to ask someone else means the test set itself up wrong.
async fn admin_post(addr: &str, body: serde_json::Value) -> io::Result<()> {
    admin_answer(addr, body)
        .await?
        .map_err(|leader| io::Error::other(format!("/api/membership redirected to {:?}", leader)))
}

/// Joining plus serving must be all it takes to become a voter, and those
/// voters must keep the cluster alive when the founding leader dies.
#[tokio::test(flavor = "multi_thread")]
async fn join_promotes_to_voter_and_cluster_survives_leader_death() -> io::Result<()> {
    let addr_a = free_addr();
    let a = EzRaft::create(&addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let a = a.clone();
        async move { a.serve().await }
    });
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "founding node leads")
        .await
        .map_err(io::Error::other)?;

    let addr_b = free_addr();
    let b = EzRaft::join(&addr_b, &addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let b = b.clone();
        async move { b.serve().await }
    });

    let addr_c = free_addr();
    let c = EzRaft::join(&addr_c, &addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let c = c.clone();
        async move { c.serve().await }
    });

    // Every node must see the final uniform config with all three voters before
    // the leader may die: a joint config still counts the founding node in its
    // old majority, so killing it mid-change would legitimately lose quorum.
    let voters = BTreeSet::from([0, b.node_id(), c.node_id()]);
    for node in [&a, &b, &c] {
        wait_for_voters(node, voters.clone(), "every promoted node is a voter").await?;
    }

    assert_eq!(Response { value: None }, a.write(set("k1", "v1")).await?);

    // A direct read serves from local memory - no consensus round, no log
    // entry: the app's `read` answers the key from its own map.
    let value: Option<String> = reqwest::Client::new()
        .post(format!("http://{}/api/read", addr_a))
        .json("k1")
        .send()
        .await
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)?;
    assert_eq!(Some("v1".to_string()), value);

    // Kill the leader; the two remaining voters still form a quorum.
    assert!(a.is_leader());
    a.inner().shutdown().await.map_err(io::Error::other)?;

    b.inner()
        .wait(WAIT)
        .metrics(
            |m| matches!(m.current_leader, Some(id) if id != 0),
            "a surviving node takes over",
        )
        .await
        .map_err(io::Error::other)?;

    // The new leader accepts writes (reached from a follower via forwarding)
    // and still has the data acknowledged before the failover.
    assert_eq!(Response { value: None }, http_write(&addr_b, set("k2", "v2")).await?);
    assert_eq!(
        Response {
            value: Some("v1".into())
        },
        http_write(&addr_b, get("k1")).await?
    );
    assert_eq!(
        Response {
            value: Some("v2".into())
        },
        http_write(&addr_b, get("k2")).await?
    );

    Ok(())
}

/// A node that joins as a learner must replicate the log and stay a learner
/// however long it runs, until it is explicitly promoted.
#[tokio::test(flavor = "multi_thread")]
async fn learner_joins_and_stays_a_learner_until_promoted() -> io::Result<()> {
    let addr_a = free_addr();
    let a = EzRaft::create(&addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let a = a.clone();
        async move { a.serve().await }
    });
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "founding node leads")
        .await
        .map_err(io::Error::other)?;

    let addr_b = free_addr();
    let b = EzRaft::join_as_learner(&addr_b, &addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let b = b.clone();
        async move { b.serve().await }
    });
    let b_id = b.node_id();

    for i in 1..3 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    // Being caught up is the whole condition a promotion waits for, so once the
    // learner has applied everything, nothing but an explicit promote is left
    // to make it a voter. Anchored to the leader's index: a node that has
    // replicated nothing yet has its own `last_applied` and `last_log_index`
    // both unset, and comparing those two would pass instantly.
    let target = a.metrics().await.last_log_index.expect("the leader has written entries");
    b.inner()
        .wait(WAIT)
        .metrics(
            |m| m.last_applied.map(|log_id| log_id.index) >= Some(target),
            "learner applied the leader's log",
        )
        .await
        .map_err(io::Error::other)?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(BTreeSet::from([0]), membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        BTreeSet::from([b_id]),
        membership.learner_ids().collect::<BTreeSet<_>>()
    );

    // A learner is a full replica; it just does not vote.
    assert_eq!(expected_map(1..3), b.read(|app| app.data.clone()).await?);

    // A node the cluster has never heard of cannot be promoted.
    let unknown = a.promote(b_id + 1000).await.unwrap_err();
    assert!(
        unknown.to_string().contains(&format!("Learner {} not found", b_id + 1000)),
        "unexpected error: {}",
        unknown
    );

    a.promote(b_id).await?;

    let voters = BTreeSet::from([0, b_id]);
    for node in [&a, &b] {
        wait_for_voters(node, voters.clone(), "the promoted learner is a voter everywhere").await?;
    }

    // Promoting again is a no-op rather than an error.
    a.promote(b_id).await?;

    Ok(())
}

/// A snapshot must land on disk when built, and a restarted node must rebuild
/// the full state from that snapshot plus the log entries after it.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_survives_restart() -> io::Result<()> {
    let addr = free_addr();
    let storage = MemStorage::default();

    let a = EzRaft::create(&addr, KvSm::default(), storage.clone(), config()).await?;
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "single node leads")
        .await
        .map_err(io::Error::other)?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    a.inner().trigger().snapshot().await.map_err(io::Error::other)?;
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.snapshot.is_some(), "snapshot built")
        .await
        .map_err(io::Error::other)?;

    // The snapshot must be on disk with the full applied state: a restart
    // reads only the disk, so an unpersisted snapshot is lost data.
    {
        let disk = storage.disk.lock().unwrap();
        let (meta, data) = disk.snapshot.as_ref().expect("snapshot persisted to storage");
        let snapshot_state: KvSm = serde_json::from_slice(data)?;
        assert_eq!(expected_map(0..10), snapshot_state.data);
        assert!(meta.last_log_id.is_some());
    }

    // More writes after the snapshot: the restart must replay these on top.
    for i in 10..15 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    a.inner().shutdown().await.map_err(io::Error::other)?;
    drop(a);

    // Restart on the same disk with an empty state machine.
    let restarted = EzRaft::create(&addr, KvSm::default(), storage.clone(), config()).await?;
    restarted
        .inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "restarted node leads")
        .await
        .map_err(io::Error::other)?;
    restarted
        .inner()
        .wait(WAIT)
        .metrics(
            |m| m.last_applied.map(|log_id| log_id.index) == m.last_log_index,
            "log tail replayed",
        )
        .await
        .map_err(io::Error::other)?;

    // After the linearizable barrier, a local read must serve every write
    // acknowledged before the shutdown.
    restarted.linearizable().await?;
    assert_eq!(expected_map(0..15), restarted.read(|app| app.data.clone()).await?);

    // And the restarted node serves reads over the rebuilt state.
    assert_eq!(
        Response {
            value: Some("v14".into())
        },
        restarted.write(get("k14")).await?
    );

    Ok(())
}

/// A node that joins after the leader purged its log can only be brought up by
/// a full snapshot over the network; its promotion waits for that transfer and
/// must still end in a voter with the complete state.
#[tokio::test(flavor = "multi_thread")]
async fn lagging_joiner_catches_up_from_snapshot() -> io::Result<()> {
    let addr_a = free_addr();
    let a = EzRaft::create(&addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let a = a.clone();
        async move { a.serve().await }
    });
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "founding node leads")
        .await
        .map_err(io::Error::other)?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    // Snapshot, then purge every covered entry: whoever joins now cannot be
    // caught up by log replay.
    a.inner().trigger().snapshot().await.map_err(io::Error::other)?;
    let snapshot_index = a
        .inner()
        .wait(WAIT)
        .metrics(|m| m.snapshot.is_some(), "snapshot built")
        .await
        .map_err(io::Error::other)?
        .snapshot
        .unwrap()
        .index;
    a.inner().trigger().purge_log(snapshot_index).await.map_err(io::Error::other)?;
    a.inner()
        .wait(WAIT)
        .metrics(
            |m| m.purged.map(|log_id| log_id.index) == Some(snapshot_index),
            "log purged up to the snapshot",
        )
        .await
        .map_err(io::Error::other)?;

    let addr_b = free_addr();
    let b = EzRaft::join(&addr_b, &addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let b = b.clone();
        async move { b.serve().await }
    });

    let voters = BTreeSet::from([0, b.node_id()]);
    wait_for_voters(&b, voters, "snapshot-fed joiner promoted to voter").await?;

    // The whole pre-purge state must have arrived through the snapshot.
    assert_eq!(expected_map(0..10), b.read(|app| app.data.clone()).await?);

    // And the pair keeps working past the transfer.
    assert_eq!(Response { value: None }, http_write(&addr_b, set("k10", "v10")).await?);
    assert_eq!(
        Response {
            value: Some("v0".into())
        },
        http_write(&addr_b, get("k0")).await?
    );

    Ok(())
}

/// A demo-sized write load must drive the whole persist lifecycle on its own:
/// with `snapshot_interval` configured, a snapshot reaches storage and the log
/// entries it covers are deleted, all without a manual trigger.
#[tokio::test(flavor = "multi_thread")]
async fn automatic_snapshot_purges_old_logs() -> io::Result<()> {
    let addr = free_addr();
    let storage = MemStorage::default();

    let config = EzConfig {
        snapshot_interval: 5,
        ..config()
    };
    let a = EzRaft::create(&addr, KvSm::default(), storage.clone(), config).await?;
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "single node leads")
        .await
        .map_err(io::Error::other)?;

    for i in 0..12 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    let metrics = a
        .inner()
        .wait(WAIT)
        .metrics(
            |m| m.snapshot.is_some() && m.purged.is_some(),
            "snapshot built and log purged by the interval policy alone",
        )
        .await
        .map_err(io::Error::other)?;

    let disk = storage.disk.lock().unwrap();
    let (meta, data) = disk.snapshot.as_ref().expect("snapshot persisted to storage");
    assert!(meta.last_log_id.is_some());
    let snapshot_state: KvSm = serde_json::from_slice(data)?;
    assert!(!snapshot_state.data.is_empty());

    // DeleteLogs must have reached storage: every entry at or below the purged
    // index is gone, and entries above it survive.
    let purged_index = metrics.purged.unwrap().index;
    let min_kept = *disk.logs.keys().next().expect("entries above the purge point remain");
    assert!(
        min_kept > purged_index,
        "min kept index {} must be above purged index {}",
        min_kept,
        purged_index
    );

    Ok(())
}

/// Demoting a voter must take it out of the quorum without taking it out of
/// the cluster: it keeps receiving the log, and the voters left still commit.
#[tokio::test(flavor = "multi_thread")]
async fn demoted_voter_becomes_a_learner_and_keeps_replicating() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (_, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;

    a.demote(c.node_id()).await?;

    let voters = BTreeSet::from([0, b.node_id()]);
    wait_for_voters(&a, voters.clone(), "the demoted node left the voter set").await?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(voters, membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        BTreeSet::from([c.node_id()]),
        membership.learner_ids().collect::<BTreeSet<_>>()
    );

    // This write proves both halves: the two remaining voters are a quorum on
    // their own, and the demoted node is still sent what they commit.
    assert_eq!(Response { value: None }, a.write(set("k1", "v1")).await?);
    wait_for_applied(&c, &a).await?;
    assert_eq!(expected_map(1..2), c.read(|app| app.data.clone()).await?);

    Ok(())
}

/// Demoting the last voter must be refused: a cluster with no voter can never
/// commit again, not even the change that would give it one back.
#[tokio::test(flavor = "multi_thread")]
async fn demote_refuses_to_empty_the_voter_set() -> io::Result<()> {
    let (_, a) = founding_node().await?;

    let err = a.demote(0).await.unwrap_err();
    assert!(
        err.to_string().contains("new membership cannot be empty"),
        "unexpected error: {}",
        err
    );

    // The refusal must have changed nothing: still one voter, still committing.
    let metrics = a.metrics().await;
    assert_eq!(
        BTreeSet::from([0]),
        metrics.membership_config.membership().voter_ids().collect::<BTreeSet<_>>()
    );
    assert_eq!(Response { value: None }, a.write(set("k1", "v1")).await?);

    Ok(())
}

/// A leader may demote itself, and openraft leaves it leading: a leader still
/// in the membership, voter or learner, keeps leading. Pinned here because it
/// surprises - the node making every decision is no longer counted in any of
/// them - and because the cluster must still be correct that way.
#[tokio::test(flavor = "multi_thread")]
async fn demoted_leader_keeps_leading_outside_the_quorum() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (addr_b, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    a.demote(0).await?;

    let voters = BTreeSet::from([b.node_id(), c.node_id()]);
    wait_for_voters(&a, voters.clone(), "the demoted leader left the voter set").await?;

    let metrics = a.metrics().await;
    let membership = metrics.committed_membership_config.membership();
    assert_eq!(voters, membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(BTreeSet::from([0]), membership.learner_ids().collect::<BTreeSet<_>>());

    // Still leading, and still committing - on a quorum of the two voters,
    // which it is not part of.
    assert!(a.is_leader());
    assert_eq!(Response { value: None }, http_write(&addr_a, set("k1", "v1")).await?);

    // And when it does go, the voters elect one of their own.
    a.inner().shutdown().await.map_err(io::Error::other)?;
    b.inner()
        .wait(WAIT)
        .metrics(
            |m| matches!(m.current_leader, Some(id) if id != 0),
            "a voter takes over from the demoted leader",
        )
        .await
        .map_err(io::Error::other)?;
    assert_eq!(Response { value: None }, http_write(&addr_b, set("k2", "v2")).await?);

    Ok(())
}

/// Leadership must be transferable to a named node. openraft's default for
/// the transfer RPC reports "not implemented", so without ezraft implementing
/// it the transfer is dropped and the leader simply keeps leading - which is
/// what makes the assertion here specific: leadership moves, and to the node
/// that was asked for.
#[tokio::test(flavor = "multi_thread")]
async fn leadership_transfers_to_the_named_node() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (_, b) = joined_voter(&addr_a).await?;
    let (addr_c, c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    let target = c.node_id();
    a.inner().trigger().transfer_leader(target).await.map_err(io::Error::other)?;

    c.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(target), "the named node takes leadership")
        .await
        .map_err(io::Error::other)?;

    assert_eq!(Response { value: None }, http_write(&addr_c, set("k1", "v1")).await?);

    Ok(())
}

/// Removing the leader must hand the cluster on: openraft steps a leader down
/// once it is out of the membership entirely, and the voters left carry on.
#[tokio::test(flavor = "multi_thread")]
async fn removed_leader_hands_over_leadership() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (addr_b, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    a.remove_node(0).await?;

    b.inner()
        .wait(WAIT)
        .metrics(
            |m| matches!(m.current_leader, Some(id) if id != 0),
            "leadership moves off the removed node",
        )
        .await
        .map_err(io::Error::other)?;

    let voters = BTreeSet::from([b.node_id(), c.node_id()]);
    wait_for_voters(&b, voters.clone(), "the removed leader left the cluster").await?;

    let metrics = b.metrics().await;
    let membership = metrics.committed_membership_config.membership();
    assert_eq!(
        voters.iter().copied().collect::<Vec<_>>(),
        membership.nodes().map(|(id, _)| *id).collect::<Vec<_>>()
    );

    assert_eq!(Response { value: None }, http_write(&addr_b, set("k1", "v1")).await?);

    Ok(())
}

/// Removing must work on a learner and on a voter alike, and leave nothing of
/// either behind in the membership.
#[tokio::test(flavor = "multi_thread")]
async fn removed_nodes_leave_the_membership() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (_, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_learner(&addr_a).await?;

    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;

    // A learner has no quorum to leave, only a node entry to drop; a voter has
    // both, and one change has to do them together.
    a.remove_node(c.node_id()).await?;
    a.remove_node(b.node_id()).await?;

    wait_for_voters(&a, BTreeSet::from([0]), "the removed voter left the voter set").await?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(vec![0], membership.nodes().map(|(id, _)| *id).collect::<Vec<_>>());
    assert!(membership.learner_ids().next().is_none());

    assert_eq!(Response { value: None }, a.write(set("k1", "v1")).await?);

    Ok(())
}

/// The admin API must drive the whole lifecycle an operator needs: one
/// endpoint promotes and demotes, another removes.
#[tokio::test(flavor = "multi_thread")]
async fn admin_api_changes_roles_and_removes_nodes() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (_, b) = joined_learner(&addr_a).await?;
    let b_id = b.node_id();

    admin_post(
        &addr_a,
        serde_json::json!({"op": "SetRole", "node_id": b_id, "role": "Voter"}),
    )
    .await?;
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "promoted over HTTP").await?;

    admin_post(
        &addr_a,
        serde_json::json!({"op": "SetRole", "node_id": b_id, "role": "Learner"}),
    )
    .await?;
    wait_for_voters(&a, BTreeSet::from([0]), "demoted over HTTP").await?;

    let metrics = a.metrics().await;
    assert_eq!(
        BTreeSet::from([b_id]),
        metrics.membership_config.membership().learner_ids().collect::<BTreeSet<_>>()
    );

    admin_post(&addr_a, serde_json::json!({"op": "Remove", "node_id": b_id})).await?;
    a.inner()
        .wait(WAIT)
        .metrics(
            |m| m.membership_config.membership().get_node(&b_id).is_none(),
            "removed over HTTP",
        )
        .await
        .map_err(io::Error::other)?;

    Ok(())
}

/// Metrics are the one admin endpoint no join drives, and the whole
/// `RaftMetrics` crosses the wire as JSON, so a field that does not survive
/// the round trip shows up here and nowhere else.
///
/// Compared against the same node's in-process metrics, whole: what HTTP
/// answers is what the node has. A follower is asked on purpose - metrics are
/// never followed to the leader, and its own view is the point of asking it.
#[tokio::test(flavor = "multi_thread")]
async fn admin_client_reads_the_metrics_of_the_node_it_asks() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (addr_b, b) = joined_voter(&addr_a).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;
    assert!(!b.is_leader(), "b has to be a follower for this to be its own view");

    // Applied, not merely committed: the two reads below are one after the
    // other, and a follower still applying the membership entry moves its
    // `last_applied` between them.
    wait_for_applied(&b, &a).await?;

    assert_eq!(b.metrics().await, AdminClient::new(&addr_b).metrics::<KvSm>().await?);

    // The same client hands out ids, which is how a joining node gets one.
    let fresh = AdminClient::new(&addr_a).node_id().await?;
    assert!(fresh > b.node_id(), "an id past every one handed out so far");

    Ok(())
}

/// A membership change asked of a follower must come back with the leader's
/// address rather than being done or quietly dropped. `EzRaft` does not
/// forward - it is a node, not a client - so this redirect is the whole
/// mechanism, and it is what the join path's request loop follows.
#[tokio::test(flavor = "multi_thread")]
async fn admin_api_redirects_a_follower_to_the_leader() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (addr_b, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_learner(&addr_a).await?;
    let c_id = c.node_id();

    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;
    assert!(!b.is_leader(), "b has to be a follower for this to be a redirect");

    // Asked of the follower: answered with where to ask instead, and nothing done.
    let promote = serde_json::json!({"op": "SetRole", "node_id": c_id, "role": "Voter"});
    assert_eq!(Err(Some(addr_a.clone())), admin_answer(&addr_b, promote.clone()).await?);
    assert!(
        !a.metrics().await.membership_config.membership().voter_ids().any(|id| id == c_id),
        "the redirected request must not have promoted anything"
    );

    // The same request to the address it named does it.
    admin_post(&addr_a, promote).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([0, b.node_id(), c_id]),
        "the leader did what the follower would not",
    )
    .await?;

    Ok(())
}

/// A node caught up by a snapshot that has no log entries after it.
///
/// This is the one path where the purge openraft runs after installing a
/// snapshot moves `last_purged` past a `last_log_id` that no later append comes
/// along to repair. Everywhere else - a joining node, a follower still being
/// replicated to - entries follow the install and hide it. `save_meta` keeps the
/// two ordered; without that the node reads back an inverted pair and openraft
/// refuses to start it, calling the log store corrupted.
///
/// The learner joins and only then starts serving, which is what leaves it with
/// nothing but the snapshot to catch up from: `add_learner` does not wait for
/// the node it adds, so the leader can compact its whole log first.
#[tokio::test(flavor = "multi_thread")]
async fn restart_after_a_snapshot_with_no_log_after_it() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;

    // A learner, so `a` keeps a quorum by itself while this node stays dark.
    let addr_c = free_addr();
    let disk_c = MemStorage::default();
    let c = EzRaft::join_as_learner(&addr_c, &addr_a, KvSm::default(), disk_c.clone(), config()).await?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    // Snapshot at `a`'s last entry and purge everything it covers.
    a.inner().trigger().snapshot().await.map_err(io::Error::other)?;
    let snapshot_index = a
        .inner()
        .wait(WAIT)
        .metrics(|m| m.snapshot.is_some(), "snapshot built")
        .await
        .map_err(io::Error::other)?
        .snapshot
        .unwrap()
        .index;
    a.inner().trigger().purge_log(snapshot_index).await.map_err(io::Error::other)?;
    a.inner()
        .wait(WAIT)
        .metrics(
            |m| m.purged.map(|log_id| log_id.index) == Some(snapshot_index),
            "log purged up to the snapshot",
        )
        .await
        .map_err(io::Error::other)?;
    assert_eq!(
        Some(snapshot_index),
        a.metrics().await.last_log_index,
        "the snapshot has to cover the whole log, or an append would follow it"
    );

    // Only now can the leader reach it, and only a snapshot can catch it up.
    spawn_serve(&c);
    wait_for_applied(&c, &a).await?;
    assert_eq!(expected_map(0..10), c.read(|app| app.data.clone()).await?);

    let meta = disk_c.disk.lock().unwrap().meta.clone();
    assert!(
        meta.last_purged <= meta.last_log_id,
        "persisted an inverted log range: last_purged={:?} last_log_id={:?}",
        meta.last_purged,
        meta.last_log_id
    );

    // Opening the store again is the real check: it is where openraft reads
    // both positions back and refuses an inverted pair. A fresh address because
    // this node is only constructed, never served.
    c.inner().shutdown().await.map_err(io::Error::other)?;
    drop(c);
    EzRaft::join_as_learner(&free_addr(), &addr_a, KvSm::default(), disk_c, config()).await?;

    Ok(())
}
