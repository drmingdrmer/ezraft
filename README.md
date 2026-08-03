# EzRaft

[![CI](https://github.com/drmingdrmer/ezraft/actions/workflows/ci.yaml/badge.svg)](https://github.com/drmingdrmer/ezraft/actions/workflows/ci.yaml)
[![Crates.io](https://img.shields.io/crates/v/ezraft.svg)](https://crates.io/crates/ezraft)
[![Docs](https://docs.rs/ezraft/badge.svg)](https://docs.rs/ezraft)
[![License](https://img.shields.io/crates/l/ezraft.svg)](https://github.com/drmingdrmer/ezraft#license)

A beginner-friendly Raft consensus framework built on
[OpenRaft](https://github.com/databendlabs/openraft).

Run your application on several machines at once, all holding the same state, so the
service survives losing some of them. That is what [Raft](https://raft.github.io/) is
for, and EzRaft reduces it to two traits: `EzApp` holds your state and applies requests
to it, `EzStorage` puts bytes on disk. Elections, replication, membership, snapshots and
the transport between machines are handled internally.

- **Minimal user API**: 4 methods total (3 storage + 1 app) vs 21+ in OpenRaft
- **Smart defaults**: 10/12 Raft types pre-configured, users specify only Request/Response
- **Built-in networking**: HTTP layer included, no user code needed
- **Type-safe**: Works directly with your types, not byte vectors

## Example

A replicated key-value service, whole. `FileStorage` is the bundled `EzStorage`, so the
application is all that is left to write:

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

**Experimental.** EzRaft is primarily an API design laboratory for exploring intuitive
interface patterns. The APIs may change until the crate stabilizes. Production
applications are not the primary audience.

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
