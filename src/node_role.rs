//! What a node is in a cluster

use serde::Deserialize;
use serde::Serialize;

/// Whether a node votes
///
/// The two roles differ in one thing, and everything else follows from it: a voter is counted in
/// the quorum a write must reach, a learner is not. So voters are what makes a cluster survive
/// failures - three tolerate one, five tolerate two - and learners are replicas that cost a write
/// nothing, because it never waits for them.
///
/// A node always *enters* a cluster as a learner, whichever role it is headed for. See
/// [`EzRaft::join`](crate::EzRaft::join) for why that is forced rather than chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum NodeRole {
    /// Replicates the log and answers reads, without voting
    #[default]
    Learner,

    /// Counts towards a quorum
    Voter,
}
