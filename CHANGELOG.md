# Changelog

## Unreleased

Learner support. A node can join and stay a learner, and any member can be
moved between voter and learner or taken out of the cluster.

Breaking, in three ways:

- `POST /api/join` is gone, replaced by `POST /api/node_id`,
  `POST /api/add_node`, `POST /api/change_node_role` and
  `POST /api/remove_node`. Joining is the first three - take an id, enter the
  membership as a learner, ask for the voter role - and splitting them is what
  lets a node exist and run before it is a full member. Each answers the result
  or the address of the leader to ask instead.

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

- A node created with `EzRaft::join` must now call `EzRaft::serve`. `join`
  starts the promotion to voter and `serve` collects it, because the leader
  grants it only once the node has caught up, which it can only do through the
  server `serve` starts. A promotion that fails is returned from `serve`.

- Nodes are no longer promoted automatically. A cluster is fault-tolerant once
  its nodes have joined *and* served, not once they have joined.

New:

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
