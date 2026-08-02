//! The admin HTTP protocol: joining a cluster, and changing who is in it
//!
//! Four endpoints, each answering a [`Redirect`] so that a request sent to a node that does not
//! lead says where the leader is instead of failing:
//!
//! | endpoint | body | does |
//! | --- | --- | --- |
//! | `POST /api/node_id` | none | Hands out an id no other node has held |
//! | `POST /api/add_node` | [`AddNodeRequest`] | Puts a node in the membership, as a learner |
//! | `POST /api/change_node_role` | [`ChangeNodeRoleRequest`] | Makes a member a voter or a learner |
//! | `POST /api/remove_node` | [`RemoveNodeRequest`] | Takes a node out of the cluster |
//!
//! Joining is the first two, then the third: take an id, enter as a learner, ask to be promoted.
//! [`EzRaft::join`](crate::EzRaft::join) does all three.

pub mod add_node;
pub mod change_node_role;
pub(crate) mod client;
pub mod redirect;
pub mod remove_node;

pub use add_node::AddNodeRequest;
pub use change_node_role::ChangeNodeRoleRequest;
pub use redirect::Redirect;
pub use remove_node::RemoveNodeRequest;
