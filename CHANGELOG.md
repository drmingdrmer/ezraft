# Changelog

## v0.2.0

Two features:

- **Learners** - a node that receives the log and answers reads without being
  counted in the quorum a write must reach.
  Methods: `EzRaft::join_as_learner`, `EzRaft::promote`, `EzRaft::demote`,
  `EzRaft::remove_node`.
  Endpoints: `POST /api/node_id`, `POST /api/membership`.
- **Reads the application defines** - a read request and a read response of the
  app's own types, in place of one key and a JSON value.
  Method: `EzApp::read`.
  Endpoint: `POST /api/read`.

Learners:

Every step of a node's life in a cluster is now a call:

- `EzRaft::join_as_learner` joins without asking to be promoted - a read replica
  that costs writes nothing. `EzRaft::join` joins and asks for the voter role,
  which `EzRaft::serve` collects: the leader grants it only once the node has
  caught up, and catching up is what the server `serve` starts makes possible.
- `EzRaft::promote` makes a learner a voter. It returns once the node *is* one,
  so it lasts as long as bringing that node up to date takes: for a large state,
  a whole snapshot transfer.
- `EzRaft::demote` makes a voter a learner. The node stays in the cluster and
  keeps receiving the log; it stops being counted. Demoting the leader is
  allowed - it commits the change and then steps down. Demoting the last voter
  is refused, because a cluster with no voter can never commit again, not even
  the change that would give it one back.
- `EzRaft::remove_node` takes a node out, voter or learner alike, in one change
  so there is no window in which a node the caller asked to remove is still
  being waited for.
- `promote`, `demote` and `remove_node` are leader-only: on a follower they fail
  rather than look for one. `POST /api/membership` is the same three over HTTP,
  accepted by any node and answering the leader's address when it is not the one
  to ask.
- `--learner` in the `kvstore` example.

Reads:

`EzApp::read` takes the application's own request type and answers with its own
response type. It used to take one `&str` key and answer with an
`Option<serde_json::Value>`, which could ask exactly one question. A read never
enters the log, so it is not confined to what the write API can phrase: a range,
a prefix, a filter, a query spanning several keys are each one read now, where
before they were a write that did not need the log, or nothing at all.

`POST /api/read` carries the request as its JSON body. `examples/kvstore.rs`
serves both a keyed lookup and a prefix scan through the one method.

Example:

    POST /api/read  {"Get": {"key": "hello"}}       -> {"hello":"world"}
    POST /api/read  {"Prefix": {"prefix": "he"}}    -> {"hello":"world"}

Also new:

- `admin::AdminClient` speaks the admin API - `node_id`, `membership` and
  `metrics` - from Rust rather than from `curl`, following the redirect to the
  leader and retrying what a cluster that is still starting up fails at. It is
  what `EzRaft::join` joins with. Metrics are the exception to both: they are
  the asked node's own view, so they are neither followed nor retried.

Breaking:

- `EzApp::read` changed signature, and `GET /api/read?key=...` became
  `POST /api/read`. A query string cannot carry an arbitrary read. The framework
  also stopped turning a missing value into a 404: what "nothing found" means
  belongs to the response type, and a handler passing an answer through has no
  business reading it. `EzRaft::read`, the in-process closure API, is unchanged.

  Upgrade tip:

      // was
      fn read(&self, key: &str) -> Option<serde_json::Value> {
          self.data.get(key).map(|v| serde_json::Value::String(v.clone()))
      }

      // now - the smallest port, for an app that only ever looked up a key
      type ReadRequest = String;
      type ReadResponse = Option<String>;

      fn read(&self, key: String) -> Option<String> {
          self.data.get(&key).cloned()
      }

- A node created with `EzRaft::join` must now call `EzRaft::serve`, which is
  where the promotion it asked for is awaited and where a failed one is
  reported. Nodes are no longer promoted automatically either: a cluster is
  fault-tolerant once its nodes have joined *and* served.

- `POST /api/join` is gone, replaced by `POST /api/node_id` and
  `POST /api/membership`. Taking an id is split from changing the membership
  because that is what lets a node exist, and run, before it is a member of
  anything; the rest is one endpoint because adding a node, changing its role
  and removing it are one decision made in stages - whether a node is a member,
  and whether it counts towards a quorum. Both answer the result or the address
  of the leader to ask instead.

  Example:

      POST /api/node_id     -> 2

      POST /api/membership  {"op": "Add",     "node_id": 2, "addr": "127.0.0.1:8081"}
      POST /api/membership  {"op": "SetRole", "node_id": 2, "role": "Voter"}
      POST /api/membership  {"op": "Remove",  "node_id": 2}

- `POST /api/change_membership` and `EzRaft::change_membership` are gone. They
  took openraft's `ChangeMembers` as-is, which put an openraft type in ezraft's
  wire protocol and left the HTTP API free to change under an openraft alpha
  bump. The endpoints above cover every operation ezraft describes; a caller
  that genuinely needs an arbitrary `ChangeMembers` reaches openraft directly.

  Upgrade tip:

      // was
      raft.change_membership(change).await?;

      // now - typed openraft errors, and `retain` is yours to choose
      raft.inner().change_membership(change, false).await?;

- Forwarding to the leader moved out of `EzRaft` and into the server layer.
  `EzRaft::write` now writes only where it is called and fails on a follower,
  naming the leader; `POST /api/write` is what takes a write on any node and
  forwards it. An `EzRaft` is a node, and a node has no business knowing how to
  reach another one - that is what the transport around it is for.

  Upgrade tip:

      // in-process, on a node that may not be the leader: no longer forwards
      raft.write(req).await?;

      // reach the cluster through its front door instead
      POST http://<any-node>/api/write

- `EzRaft::read` returns `Result` and takes a `'static` closure. It reaches the
  state machine through openraft's `Raft::with_state_machine` now, rather than
  through a private handle held beside it, so the closure runs on the state
  machine's own task and must own what it captures. The error is a shut-down
  node, and nothing else.

  Upgrade tip:

      // was
      let key = &query.key;
      let value = raft.read(|app| app.read(key)).await;

      // now - capture by value, and handle a shut-down node
      let key = query.key.clone();
      let value = raft.read(move |app| app.read(&key)).await?;

- `StorageAdapter` is gone, split into `storage::adapter::LogStore` and
  `storage::adapter::StateMachineStore` - the two stores openraft asks for,
  which one type used to serve by implementing all four storage traits and
  being passed twice. `ORRaft`, which `EzRaft::inner` returns, is now
  `Raft<OpenRaftTypes<T>, StateMachineStore<T>>`. Code that only calls methods
  on `EzRaft::inner()` is unaffected; code that names the type is not.

Fixed:

- `transfer_leader` was never implemented. `RaftNetworkV2` supplies a default
  that reports "not implemented", so every leadership transfer failed
  silently - including the one a leader makes on its way out of the voter set,
  which left the cluster to notice its silence and time out instead.

## v0.1.1

- Pin `openraft` to `=0.10.0-alpha.32`. Its pre-release series breaks API
  between alphas, and ezraft implements its storage, network and type-config
  traits, so a caret requirement let a downstream `cargo update` pull an
  incompatible alpha into a build of ezraft that had not changed.

## v0.1.0

Initial release. Imported from openraft's `experimental/ezraft`, where the
crate was developed as a workspace member.
