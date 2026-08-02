//! Body of `POST /api/remove_node`

use serde::Deserialize;
use serde::Serialize;

/// Take a node out of the cluster entirely
///
/// Unlike making it a learner, this ends the cluster's interest in the node: it stops being
/// replicated to and is forgotten. The node's own process is not stopped by this - that is the
/// operator's to do, and doing it first is what leaves the cluster nothing to talk to.
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveNodeRequest {
    pub node_id: u64,
}
