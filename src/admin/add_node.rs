//! Body of `POST /api/add_node`

use serde::Deserialize;
use serde::Serialize;

/// Put a node in the membership, as a learner
///
/// The id must be one `POST /api/node_id` handed out, which is what makes it unique.
#[derive(Debug, Serialize, Deserialize)]
pub struct AddNodeRequest {
    pub node_id: u64,

    /// Address of the node's HTTP server, which every peer reaches it through
    pub addr: String,
}
