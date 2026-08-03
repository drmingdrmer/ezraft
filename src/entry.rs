//! The Raft log entry

use openraft::BasicNode;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::entry::RaftEntry;
use openraft::entry::RaftPayload;
use openraft::log_id::RaftLogId;
use openraft::vote::leader_id_std::CommittedLeaderId;

use crate::app::EzApp;

/// Log ID type (term, index)
///
/// A tuple that implements `RaftLogId` via OpenRaft's blanket implementation.
pub type EzLogId = (u64, u64);

/// Committed leader ID: the term of the leader that proposed a log entry
pub type EzCommittedLeaderId = CommittedLeaderId<u64>;

/// A request on its way through the log
///
/// openraft asks its request type for `Display` as well as `Debug`, to name requests in logs and
/// errors. This wrapper answers that with `Debug`, which keeps a hand-written `Display` off
/// [`EzApp::Request`] - a chore with nothing to show for it here, since [`EzEntry`] renders its
/// payload with `Debug` anyway. Transparent to serde, so it is invisible on the wire and on disk.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct EzRequest<D>(pub D);

impl<D> std::fmt::Display for EzRequest<D>
where D: std::fmt::Debug
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

/// Entry payload with EzRaft's node id and node types
type EzEntryPayload<T> = EntryPayload<EzRequest<<T as EzApp>::Request>, u64, BasicNode>;

/// A Raft log entry with EzRaft's simplified log ID type
///
/// Wraps the entry's log ID (term, index) and payload.
/// This is the native Entry type used throughout EzRaft.
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(bound = "")]
pub struct EzEntry<T>
where T: EzApp
{
    /// Log ID (term, index)
    pub log_id: EzLogId,

    /// Entry payload (Normal request, Blank, or Membership change)
    pub payload: EzEntryPayload<T>,
}

// Manually implement Clone to avoid T: Clone bound; only T::Request must be Clone
impl<T> Clone for EzEntry<T>
where T: EzApp
{
    fn clone(&self) -> Self {
        Self {
            log_id: self.log_id,
            payload: self.payload.clone(),
        }
    }
}

// Manually implement Debug to avoid T: Debug bound
impl<T> std::fmt::Debug for EzEntry<T>
where T: EzApp
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EzEntry").field("log_id", &self.log_id).field("payload", &self.payload).finish()
    }
}

// Manually implement Display
impl<T> std::fmt::Display for EzEntry<T>
where T: EzApp
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EzEntry(log_id: ({}, {}), payload: {:?})",
            self.log_id.0, self.log_id.1, self.payload
        )
    }
}

// Implement RaftPayload trait
impl<T> RaftPayload<u64, BasicNode> for EzEntry<T>
where T: EzApp
{
    fn get_membership(&self) -> Option<Membership<u64, BasicNode>> {
        self.payload.get_membership()
    }
}

// Implement openraft::RaftEntry trait so EzEntry works with OpenRaft
impl<T> RaftEntry for EzEntry<T>
where T: EzApp
{
    type CommittedLeaderId = EzCommittedLeaderId;
    type D = EzRequest<T::Request>;
    type NodeId = u64;
    type Node = BasicNode;

    fn new(log_id: LogId<EzCommittedLeaderId>, payload: EzEntryPayload<T>) -> Self {
        Self {
            log_id: log_id.to_type(),
            payload,
        }
    }

    fn log_id_parts(&self) -> (&EzCommittedLeaderId, u64) {
        RaftLogId::log_id_parts(&self.log_id)
    }

    fn set_log_id(&mut self, new: LogId<EzCommittedLeaderId>) {
        self.log_id = new.to_type();
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;

    /// Deliberately not Clone: entries must clone for any `EzApp`.
    #[derive(serde::Deserialize, serde::Serialize)]
    struct Marker;

    #[async_trait]
    impl EzApp for Marker {
        type Request = String;
        type Response = String;

        async fn apply(&mut self, req: String) -> String {
            req
        }

        fn read(&self, _key: &str) -> Option<serde_json::Value> {
            None
        }
    }

    #[test]
    fn ez_entry_clones_without_t_clone() {
        let entry = EzEntry::<Marker> {
            log_id: (7, 9),
            payload: EntryPayload::Normal(EzRequest("hello".to_string())),
        };

        let clone = entry.clone();
        assert_eq!((7, 9), clone.log_id);
        let EntryPayload::Normal(req) = clone.payload else {
            panic!("payload variant changed by clone");
        };
        assert_eq!("hello", req.0);
    }
    /// [`EzRequest`] is transparent to serde, so a log written before it still reads back.
    #[test]
    fn ez_request_is_invisible_on_the_wire() {
        let json = r#"{"log_id":[7,9],"payload":{"Normal":"hello"}}"#;

        let entry = EzEntry::<Marker> {
            log_id: (7, 9),
            payload: EntryPayload::Normal(EzRequest("hello".to_string())),
        };
        assert_eq!(json, serde_json::to_string(&entry).unwrap());

        let back: EzEntry<Marker> = serde_json::from_str(json).unwrap();
        assert_eq!((7, 9), back.log_id);
        let EntryPayload::Normal(req) = back.payload else {
            panic!("payload variant changed by the round trip");
        };
        assert_eq!("hello", req.0);
    }
}
