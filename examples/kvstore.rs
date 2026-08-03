//! EzRaft KV Store Example
//!
//! A simple distributed key-value store built on EzRaft.
//! Run multiple instances to form a cluster:
//!
//! ```bash
//! # Terminal 1 (first node - creates cluster)
//! cargo run --example kvstore -- --addr 127.0.0.1:8080
//!
//! # Terminal 2 (joins via seed node)
//! cargo run --example kvstore -- --addr 127.0.0.1:8081 --seed 127.0.0.1:8080
//!
//! # Terminal 3 (joins via seed node)
//! cargo run --example kvstore -- --addr 127.0.0.1:8082 --seed 127.0.0.1:8080
//!
//! # A read replica: replicates the log, never votes, and writes never wait for it
//! cargo run --example kvstore -- --addr 127.0.0.1:8083 --seed 127.0.0.1:8080 --learner
//! ```
//!
//! Then drive it through the write API, which takes the `Request` type below as JSON,
//! and read a key back directly - reads never need the log:
//!
//! ```bash
//! curl -X POST 127.0.0.1:8080/api/write -H 'Content-Type: application/json' \
//!     -d '{"Set": {"key": "hello", "value": "world"}}'
//!
//! curl -X POST 127.0.0.1:8080/api/read -H 'Content-Type: application/json' \
//!     -d '{"Get": {"key": "hello"}}'
//! ```

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;

use clap::Parser;
use ezraft::EzApp;
use ezraft::EzConfig;
use ezraft::EzRaft;
use ezraft::FileStorage;
use serde::Deserialize;
use serde::Serialize;
use tracing_subscriber::EnvFilter;

// Define application request types
//
// Reads are deliberately not requests: they are served from local state via
// `POST /api/read` (or `EzRaft::read` in code) with no consensus round and no log
// entry. The alternative - a `Get` variant applied through the log - is worth its
// cost only when a read must be linearizable from any node, not just the leader.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set { key: String, value: String },
    Delete { key: String },
}

// Define application response type
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}

// Define application read types
//
// A read never enters the log, so it is not bound to what a write can say: `Prefix` asks a
// question the key-at-a-time write API has no way to phrase.
#[derive(Serialize, Deserialize, Debug)]
pub enum ReadRequest {
    Get { key: String },
    Prefix { prefix: String },
}

// The entries the read matched, empty when none did.
pub type ReadResponse = BTreeMap<String, String>;

// The application: its state plus one method of business logic.
// Snapshots are derived from the state via serde, hence the serde derives.
#[derive(Default, Serialize, Deserialize)]
struct KvApp {
    data: BTreeMap<String, String>,
}

#[async_trait::async_trait]
impl EzApp for KvApp {
    type Request = Request;
    type Response = Response;
    type ReadRequest = ReadRequest;
    type ReadResponse = ReadResponse;

    async fn apply(&mut self, req: Request) -> Response {
        match req {
            Request::Set { key, value } => {
                // The replaced value, if any: a caller that overwrites a key learns what was
                // there without a second round trip - and, unlike a read it issues itself, the
                // answer is the one this very entry replaced.
                let value = self.data.insert(key, value);
                Response { value }
            }
            Request::Delete { key } => {
                let value = self.data.remove(&key);
                Response { value }
            }
        }
    }

    // Serves `POST /api/read` straight from the map. Both variants are indexed lookups, not
    // scans: `range` starts the walk at the prefix instead of at the first key.
    fn read(&self, req: ReadRequest) -> ReadResponse {
        match req {
            ReadRequest::Get { key } => self.data.get(&key).map(|value| (key, value.clone())).into_iter().collect(),
            ReadRequest::Prefix { prefix } => self
                .data
                .range(prefix.clone()..)
                .take_while(|(key, _)| key.starts_with(&prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }
}

/// Command-line arguments for the KV store
#[derive(clap::Parser)]
struct Args {
    /// HTTP bind address (e.g., "127.0.0.1:8080")
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    /// Seed node address to join existing cluster
    #[arg(long)]
    seed: Option<String>,

    /// Join as a learner: replicate the log and answer reads, but never vote
    ///
    /// Adds read capacity without adding a node that writes have to wait for. Promote it with
    /// `POST /api/membership` whenever you decide to.
    #[arg(long)]
    learner: bool,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Without this, everything Raft reports about a cluster that is not working goes nowhere.
    // Warnings and errors by default; set RUST_LOG=info (or debug) to follow what Raft is doing.
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();

    let args = Args::parse();
    let addr = args.addr;
    let seed = args.seed;

    // The founding node is the cluster's first voter and cannot be anything else: a cluster whose
    // only member does not vote can never commit, not even the change that would fix it.
    if args.learner && seed.is_none() {
        return Err(io::Error::other(
            "--learner needs --seed: the founding node has to be a voter",
        ));
    }

    // Create app and storage (use addr for directory name)
    let base_dir = PathBuf::from(format!("./data/{}", addr.replace(':', "-")));

    // The bundled storage: readable JSON files, no fsync. A deployment implements
    // `EzStorage` itself - see the caveats on `FileStorage`.
    let app = KvApp::default();
    let storage = FileStorage::new(base_dir).await?;

    // Create EzRaft instance: the first node starts the cluster, the rest join it
    let config = EzConfig::default();
    let raft = match &seed {
        Some(seed) if args.learner => EzRaft::join_as_learner(&addr, seed, app, storage, config).await?,
        Some(seed) => EzRaft::join(&addr, seed, app, storage, config).await?,
        None => EzRaft::create(&addr, app, storage, config).await?,
    };

    println!("Node {} listening on {}", raft.node_id(), addr);
    print_next_steps(&addr, seed.is_some());

    // Start HTTP server
    raft.serve().await?;

    Ok(())
}

/// Print the commands that drive the node just started
///
/// A node that announces only that it is listening leaves the reader with nothing to type: the
/// body `/api/write` takes is this file's `Request` enum as JSON, which cannot be guessed from
/// outside the source.
fn print_next_steps(addr: &str, seed_given: bool) {
    // Without a seed this node forms a cluster of its own, and every other node must be pointed
    // at it. A reader who starts a second node without `--seed` gets two separate clusters that
    // never merge, so the invitation to join is printed with the address already filled in.
    if !seed_given && let Some(next) = next_addr(addr) {
        println!(
            "\nNo --seed given, so this node is the cluster's founding member. \
             Every other node joins through it - from another terminal:\n    \
             cargo run --example kvstore -- --addr {next} --seed {addr}"
        );
    }

    println!(
        r#"
Write a key - any node accepts one, a follower forwards it to the leader:
    curl -X POST {addr}/api/write -H 'Content-Type: application/json' \
        -d '{{"Set": {{"key": "hello", "value": "world"}}}}'

Read it back - answered from this node's memory, no consensus round:
    curl -X POST {addr}/api/read -H 'Content-Type: application/json' \
        -d '{{"Get": {{"key": "hello"}}}}'

Read every key sharing a prefix - a read the write API cannot phrase:
    curl -X POST {addr}/api/read -H 'Content-Type: application/json' \
        -d '{{"Prefix": {{"prefix": "he"}}}}'

Cluster state - leader, term, log index, membership:
    curl {addr}/api/metrics

Change a node's role - a voter counts towards a quorum, a learner does not:
    curl -X POST {addr}/api/membership -H 'Content-Type: application/json' \
        -d '{{"op": "SetRole", "node_id": 1, "role": "Voter"}}'
"#
    );
}

/// The address one port up, to suggest where the next node could listen
fn next_addr(addr: &str) -> Option<String> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?.checked_add(1)?;
    Some(format!("{}:{}", host, port))
}
