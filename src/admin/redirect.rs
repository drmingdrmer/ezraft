//! What every admin endpoint answers

/// An admin endpoint's answer: the result, or the leader to ask instead
///
/// Membership is the leader's to change, so a node that does not lead answers with where the
/// leader is rather than with a failure. `Err(None)` means it knows of no leader either, which is
/// a moment to wait out - an election in flight resolves within a few heartbeats.
pub type Redirect<R> = Result<R, Option<String>>;
