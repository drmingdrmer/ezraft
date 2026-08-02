//! The admin HTTP protocol: joining a cluster, and changing who is in it
//!
//! Two endpoints, each answering a [`Redirect`] so that a request sent to a node that does not
//! lead says where the leader is instead of failing:
//!
//! | endpoint | body | does |
//! | --- | --- | --- |
//! | `POST /api/node_id` | none | Hands out an id no other node has held |
//! | `POST /api/membership` | [`MembershipChange`] | Adds a node, changes its role, or removes it |
//!
//! Two rather than one, because taking an id is not a membership change: it is what lets a node
//! exist, and run, before it is a member of anything. Joining is one of each and then a second
//! membership change - take an id, enter as a learner, ask to be promoted - and
//! [`EzRaft::join`](crate::EzRaft::join) does all three.

pub(crate) mod client;
pub mod membership_change;
pub mod redirect;

pub use membership_change::MembershipChange;
pub use redirect::Redirect;
