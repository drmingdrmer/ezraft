//! Body of `POST /api/change_node_role`

use serde::Deserialize;
use serde::Serialize;

use crate::node_role::NodeRole;

/// Make a member of the cluster a voter or a learner
///
/// One request for both directions, because they are one decision: whether this node is counted in
/// the quorum. Asking for the role a node already holds changes nothing.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeNodeRoleRequest {
    pub node_id: u64,
    pub role: NodeRole,
}
