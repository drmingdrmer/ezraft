//! Raft metadata the framework asks storage to keep

use serde::Deserialize;
use serde::Serialize;

use crate::entry::EzLogId;
use crate::type_config::EzVote;

/// Raft metadata managed by the framework
///
/// The framework updates this structure and you persist it via [`crate::EzStorage::persist`].
/// You don't need to understand the Raft details - just serialize and store it.
#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct EzMeta {
    /// This node's ID (assigned when joining cluster)
    pub node_id: Option<u64>,

    /// Current vote (term and node_id voted for)
    pub vote: Option<EzVote>,

    /// Last log entry (term, index)
    pub last_log_id: Option<EzLogId>,

    /// Last purged log entry (term, index)
    pub last_purged: Option<EzLogId>,

    /// Whether this node asked to be a voter and has not seen it happen yet
    ///
    /// Set by [`EzRaft::join`](crate::EzRaft::join) once the cluster has this node as a learner,
    /// cleared once the promotion to voter is confirmed. It is what tells the two apart on the
    /// next start: a join that never finished resumes its promotion, while a node that finished
    /// one and was demoted later stays the learner the membership says it is.
    #[serde(default)]
    pub promotion_pending: bool,
}
