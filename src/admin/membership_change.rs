//! Body of `POST /api/membership`

use serde::Deserialize;
use serde::Serialize;

use crate::node_role::NodeRole;

/// A change to who is in the cluster
///
/// One request for every such change, because they are one decision made in stages: whether a
/// node is a member at all, and whether it counts towards a quorum.
///
/// Tagged by `op` rather than wrapped, so the JSON stays flat:
///
/// ```json
/// {"op": "Add",     "node_id": 2, "addr": "127.0.0.1:8081"}
/// {"op": "SetRole", "node_id": 2, "role": "Voter"}
/// {"op": "Remove",  "node_id": 2}
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum MembershipChange {
    /// Put a node in the membership, as a learner
    ///
    /// Always a learner, whichever role the node is headed for: one added straight to the voter
    /// set would be counted in the new configuration's quorum before it could answer anything,
    /// and the change would wait forever on its own acknowledgement. `SetRole` follows.
    ///
    /// The id must be one `POST /api/node_id` handed out, which is what makes it unique.
    Add {
        node_id: u64,

        /// Address of the node's HTTP server, which every peer reaches it through
        addr: String,
    },

    /// Make a member a voter or a learner
    ///
    /// Both directions are this one variant, because they are one decision: whether the node is
    /// counted in the quorum a write must reach. Asking for the role a node already holds changes
    /// nothing.
    ///
    /// Becoming a voter is answered only once the node has caught up, so that request lasts as
    /// long as the catch-up - for a large state, a whole snapshot transfer - and the node must be
    /// serving, or it can never catch up.
    SetRole { node_id: u64, role: NodeRole },

    /// Take a node out of the cluster entirely
    ///
    /// Unlike making it a learner, this ends the cluster's interest in the node: it stops being
    /// replicated to and is forgotten. The node's own process is not stopped by this - that is the
    /// operator's to do, and doing it first is what leaves the cluster nothing to talk to.
    Remove { node_id: u64 },
}
