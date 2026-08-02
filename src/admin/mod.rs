//! The admin HTTP protocol: joining a cluster, changing who is in it, and asking after it
//!
//! Three endpoints, with [`AdminClient`] as the Rust side of all three:
//!
//! | endpoint | body | does |
//! | --- | --- | --- |
//! | `POST /api/node_id` | none | Hands out an id no other node has held |
//! | `POST /api/membership` | [`MembershipChange`] | Adds a node, changes its role, or removes it |
//! | `GET /api/metrics` | none | Answers with the asked node's view of the cluster |
//!
//! The two `POST`s answer a [`Redirect`], so one sent to a node that does not lead says where the
//! leader is instead of failing. Metrics never redirect: they are what the node asked has.
//!
//! Two membership endpoints rather than one, because taking an id is not a membership change: it
//! is what lets a node exist, and run, before it is a member of anything. Joining is one of each
//! and then a second membership change - take an id, enter as a learner, ask to be promoted - and
//! [`EzRaft::join`](crate::EzRaft::join) does all three.

pub mod client;
pub mod membership_change;
pub mod redirect;

pub use client::AdminClient;
pub use membership_change::MembershipChange;
pub use redirect::Redirect;
