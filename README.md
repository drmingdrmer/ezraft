# EzRaft

[![CI](https://github.com/drmingdrmer/ezraft/actions/workflows/ci.yaml/badge.svg)](https://github.com/drmingdrmer/ezraft/actions/workflows/ci.yaml)
[![Crates.io](https://img.shields.io/crates/v/ezraft.svg)](https://crates.io/crates/ezraft)
[![Docs](https://docs.rs/ezraft/badge.svg)](https://docs.rs/ezraft)
[![License](https://img.shields.io/crates/l/ezraft.svg)](https://github.com/drmingdrmer/ezraft#license)

A beginner-friendly Raft consensus framework built on
[OpenRaft](https://github.com/databendlabs/openraft).

Run your application on several machines at once, all holding the same state, so the
service survives losing some of them. That is [Raft](https://raft.github.io/); EzRaft
reduces it to two traits: `EzApp` holds your state and applies requests to it,
`EzStorage` puts bytes on disk. Elections, replication, membership, snapshots and
transport are internal.

Where to draw that line is the point of the crate - a search for the smallest API still
enough to build a distributed key-value store. An API is judged by building with it, so
the example below is a whole working service; if part of it weighs more than the job
deserves, that is worth reporting.

- **Two methods**: `apply()` performs a write, `read()` answers a read - each with a
  request and a response type you choose. That is the whole application interface.
- **Your own types**: requests, responses and state are your structs. Serde moves them
  between nodes; nothing here deals in byte vectors.
- **Storage included**: `FileStorage` persists to disk out of the box. Writing your own
  is three methods.
- **Networking included**: every node serves the Raft RPCs its peers need and an HTTP
  API for your app. There is no transport to write.

## Example

A replicated key-value service, whole. First the application: the two methods above,
and the four types they carry.

```rust
#[derive(Serialize, Deserialize, Debug, Clone, derive_more::Display)]
enum Request {
    #[display("Set({key})")]
    Set { key: String, value: String },
}

/// The application *is* the replicated state: a snapshot is this struct, serialized.
#[derive(Default, Serialize, Deserialize)]
struct KvApp {
    data: BTreeMap<String, String>,
}

#[async_trait]
impl EzApp for KvApp {
    type Request = Request;
    type Response = Option<String>;

    /// Called once per committed entry, in log order, on every node.
    async fn apply(&mut self, req: Request) -> Option<String> {
        match req {
            Request::Set { key, value } => self.data.insert(key, value),
        }
    }

    type ReadRequest = String;
    type ReadResponse = Option<String>;

    /// Answered from local state: no consensus round, no log entry.
    fn read(&self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
    }
}
```

Then `main`, which is where the cluster comes from. `FileStorage` is the bundled
`EzStorage`, so there is nothing else to implement:

```rust
/// One binary, run once per machine: `kvstore <own-addr> [addr of a node already in it]`
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().expect("usage: kvstore <addr> [seed_addr]");
    let seed = args.next();

    let storage = FileStorage::new(format!("./data/{addr}")).await?;
    let (app, config) = (KvApp::default(), EzConfig::default());

    // The first node creates the cluster; every other one joins through a node in it
    // and is given an id, the membership, and the log so far.
    let raft = match seed {
        None => EzRaft::create(addr, app, storage, config).await?,
        Some(seed) => EzRaft::join(addr, seed, app, storage, config).await?,
    };

    // The Raft RPCs peers need, plus the app API below. Runs until shutdown.
    raft.serve().await
}
```

Three of those form a cluster that survives losing any one of them:

```bash
kvstore 127.0.0.1:8080                  # creates it
kvstore 127.0.0.1:8081 127.0.0.1:8080   # joins
kvstore 127.0.0.1:8082 127.0.0.1:8080   # joins
```

## Drive it

```bash
# A write goes through the log: replicated and committed before it answers.
curl -X POST 127.0.0.1:8080/api/write -H 'Content-Type: application/json' \
    -d '{"Set": {"key": "hello", "value": "world"}}'
# null  - Set answers with the value it replaced, if there was one

# A read is answered from that node's own memory: no consensus round, no log entry.
curl -X POST 127.0.0.1:8082/api/read -H 'Content-Type: application/json' -d '"hello"'
# "world"  - from a node you never wrote to

curl 127.0.0.1:8080/api/metrics   # leader, term, log index, membership
```

## Status

**Experimental.** The API is the thing being searched for, so it changes until the
crate stabilizes. A real service built on it is the feedback the search needs most -
pin the version, and read the CHANGELOG before upgrading.

**Next phase: Stable API.** Once the design exploration matures, EzRaft will provide a
stable API with well-considered abstractions - exposing what users need while hiding
unnecessary complexity.

## Documentation

- [Guide](docs/guide.md) - run a cluster, write a service against it, and the API,
  configuration and HTTP endpoints that make it up
- [API documentation](https://docs.rs/ezraft)
- [`examples/kvstore.rs`](examples/kvstore.rs) - a complete key-value service
- [CHANGELOG](CHANGELOG.md)

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
