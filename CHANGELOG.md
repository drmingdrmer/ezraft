# Changelog

## v0.2.0

Learner support, and an `EzApp` that asks for less. A node can join and stay a
learner, and any member can be moved between voter and learner or taken out of
the cluster. A read now takes the app's own request and response types, and a
request no longer needs a hand-written `Display`.

Breaking:

- `EzApp::read` takes the app's own request type and answers with its own
  response type: `fn read(&self, req: Self::ReadRequest) -> Self::ReadResponse`,
  where both are new associated types. It was `fn read(&self, key: &str) ->
  Option<serde_json::Value>`, which could express one shape of question - look
  up one key - and left anything else to a write that did not need the log.

  `GET /api/read?key=...` becomes `POST /api/read` with the request as its JSON
  body, because a query string cannot carry an arbitrary read. The framework no
  longer turns a missing value into a 404, either: what "nothing found" means
  belongs to the response type, and the handler does not read the answer it is
  passing on. `EzRaft::read`, the in-process closure API, is unchanged.

  `examples/kvstore.rs` now serves a prefix scan next to its keyed lookup - the
  read that the old signature had no way to phrase.

  Example:

      POST /api/read  {"Get": {"key": "hello"}}       -> {"hello":"world"}
      POST /api/read  {"Prefix": {"prefix": "he"}}    -> {"hello":"world"}

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

- `EzApp::Request` no longer needs `Display`, only `Debug`. openraft asks its
  request type for both; `EzRequest`, a transparent newtype, answers the
  `Display` half with `Debug`, so a hand-written impl is off the list of things
  a new application has to write. No output is lost - this crate renders an
  entry's payload with `Debug` either way.

  Breaking only where the request type is named to openraft: an entry's payload
  is `EntryPayload<EzRequest<T::Request>, ..>`, and `inner().client_write` takes
  an `EzRequest`. `EzRaft::write`, `EzApp::apply` and the HTTP API are unchanged,
  and so is the format on the wire and on disk.

  Upgrade tip:

      // was
      #[derive(Serialize, Deserialize, Debug, Clone, derive_more::Display)]
      enum Request {
          #[display("Set({key})")]
          Set { key: String, value: String },
      }

      // now
      #[derive(Serialize, Deserialize, Debug, Clone)]
      enum Request {
          Set { key: String, value: String },
      }

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
  forwards it. `promote`, `demote` and `remove_node` are leader-only for the
  same reason, with the admin endpoints answering the leader's address. An
  `EzRaft` is a node, and a node has no business knowing how to reach another
  one - that is what the transport around it is for.

  Upgrade tip:

      // in-process, on a node that may not be the leader: no longer forwards
      raft.write(req).await?;

      // reach the cluster through its front door instead
      POST http://<any-node>/api/write

- `StorageAdapter` is gone, split into `storage::adapter::LogStore` and
  `storage::adapter::StateMachineStore` - the two stores openraft asks for,
  which one type used to serve by implementing all four storage traits and
  being passed twice. `ORRaft`, which `EzRaft::inner` returns, is now
  `Raft<OpenRaftTypes<T>, StateMachineStore<T>>`. Code that only calls methods
  on `EzRaft::inner()` is unaffected; code that names the type is not.

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

- A node created with `EzRaft::join` must now call `EzRaft::serve`. `join`
  starts the promotion to voter and `serve` collects it, because the leader
  grants it only once the node has caught up, which it can only do through the
  server `serve` starts. A promotion that fails is returned from `serve`.

- Nodes are no longer promoted automatically. A cluster is fault-tolerant once
  its nodes have joined *and* served, not once they have joined.

New:

- `admin::AdminClient` speaks the admin API - `node_id`, `membership` and
  `metrics` - from Rust rather than from `curl`, following the redirect to the
  leader and retrying what a cluster that is still starting up fails at. It is
  what `EzRaft::join` joins with. Metrics are the exception to both: they are
  the asked node's own view, so they are neither followed nor retried.
- `EzRaft::join_as_learner` joins without asking to be promoted, for a read
  replica that costs writes nothing.
- `EzRaft::promote`, `EzRaft::demote` and `EzRaft::remove_node`. Callable on
  any node; a follower forwards to the leader, the way `write` does.
- `--learner` in the `kvstore` example.

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
