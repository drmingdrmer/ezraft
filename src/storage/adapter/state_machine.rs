//! The state machine half of the storage adapter: the application, and its snapshot

use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::Arc;

use futures::StreamExt;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use openraft::alias::LogIdOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use tokio::sync::Mutex;

use crate::app::EzApp;
use crate::snapshot::EzSnapshot;
use crate::snapshot::EzSnapshotData;
use crate::snapshot::EzSnapshotMeta;
use crate::storage::Persist;
use crate::storage::adapter::SharedStorage;
use crate::type_config::OpenRaftTypes;

/// Internal state machine wrapper that tracks Raft metadata
/// alongside the user's application
struct StateMachineState<T>
where T: EzApp
{
    /// User's application: the state machine value itself
    app: T,

    /// Last log ID applied to the state machine
    last_applied: Option<LogIdOf<OpenRaftTypes<T>>>,

    /// Last membership applied to the state machine
    membership: StoredMembershipOf<OpenRaftTypes<T>>,
}

/// The state machine: the user's application, what it has applied, and its snapshot
pub struct StateMachineStore<T>
where T: EzApp
{
    storage: SharedStorage<T>,

    sm_state: Arc<Mutex<StateMachineState<T>>>,

    /// The snapshot last written or loaded, kept so that serving one to a lagging follower does
    /// not re-run the startup-only [`EzStorage::load`](crate::storage::EzStorage::load).
    snapshot: Arc<Mutex<Option<EzSnapshot>>>,
}

impl<T> Clone for StateMachineStore<T>
where T: EzApp
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sm_state: self.sm_state.clone(),
            snapshot: self.snapshot.clone(),
        }
    }
}

impl<T> StateMachineStore<T>
where T: EzApp
{
    /// Restore the state machine from the snapshot the storage loaded, if there is one
    ///
    /// The snapshot data is restored here, not just its position: reporting `last_applied` at
    /// the snapshot makes openraft re-apply only the log tail after it, and skip installing this
    /// snapshot itself.
    pub(super) fn new(storage: SharedStorage<T>, app: T, snapshot: Option<EzSnapshot>) -> Result<Self, std::io::Error> {
        let mut app = app;

        let (last_applied, membership) = match &snapshot {
            Some(snap) => {
                app = serde_json::from_slice(snap.snapshot.get_ref())?;
                (snap.meta.last_log_id, snap.meta.last_membership.clone())
            }
            None => (None, StoredMembership::new(None, Membership::default())),
        };

        let sm_state = StateMachineState {
            app,
            last_applied,
            membership,
        };

        Ok(Self {
            storage,
            sm_state: Arc::new(Mutex::new(sm_state)),
            snapshot: Arc::new(Mutex::new(snapshot)),
        })
    }

    /// Run a closure over the applied application state
    pub async fn read<F, R>(&self, read: F) -> R
    where F: FnOnce(&T) -> R {
        let sm = self.sm_state.lock().await;
        read(&sm.app)
    }
}

impl<T> RaftStateMachine<OpenRaftTypes<T>> for StateMachineStore<T>
where T: EzApp
{
    type SnapshotData = EzSnapshotData;

    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<OpenRaftTypes<T>>>, StoredMembershipOf<OpenRaftTypes<T>>), std::io::Error> {
        let sm = self.sm_state.lock().await;
        Ok((sm.last_applied, sm.membership.clone()))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), std::io::Error>
    where Strm: futures::Stream<Item = Result<EntryResponder<OpenRaftTypes<T>>, std::io::Error>> + OptionalSend + Unpin
    {
        let mut sm = self.sm_state.lock().await;

        let mut entries = entries;
        while let Some(res) = entries.next().await {
            let (entry, responder) = res.map_err(std::io::Error::other)?;

            // Update last_applied for every entry
            let (term, index) = entry.log_id;
            let log_id = LogId::new_term_index(term, index);
            sm.last_applied = Some(log_id);

            let resp = match entry.payload {
                EntryPayload::Normal(req) => Some(sm.app.apply(req).await),
                EntryPayload::Membership(membership) => {
                    sm.membership = StoredMembership::new(Some(log_id), membership);
                    None
                }
                EntryPayload::Blank => None,
            };

            if let Some(responder) = responder {
                responder.send(resp);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        snapshot_meta: &EzSnapshotMeta,
        snapshot_data: EzSnapshotData,
    ) -> Result<(), std::io::Error> {
        // Extract snapshot data
        let mut cursor = snapshot_data;
        cursor.seek(SeekFrom::Start(0))?;
        let mut data = Vec::new();
        cursor.read_to_end(&mut data)?;

        // Update storage state
        {
            let mut cached = self.snapshot.lock().await;
            let mut storage = self.storage.lock().await;
            storage.persist(Persist::Snapshot(new_snapshot(snapshot_meta, &data))).await?;
            *cached = Some(new_snapshot(snapshot_meta, &data));
        }

        // The log positions are not touched here. openraft purges the log up to the snapshot
        // right after this, and that purge records both of them: `last_purged` directly, and
        // `last_log_id` through the invariant [`Self::save_meta`] keeps.

        // Update state machine state and restore user state from snapshot
        {
            let mut sm = self.sm_state.lock().await;
            sm.last_applied = snapshot_meta.last_log_id;
            sm.membership = snapshot_meta.last_membership.clone();
            sm.app = serde_json::from_slice(&data)?;
        }

        Ok(())
    }

    /// Serving a snapshot never touches the storage, so a lagging follower being caught up does
    /// not queue behind an append.
    async fn get_current_snapshot(&mut self) -> Result<Option<EzSnapshot>, std::io::Error> {
        let cached = self.snapshot.lock().await;
        Ok(cached.as_ref().map(|snap| new_snapshot(&snap.meta, snap.snapshot.get_ref())))
    }
}

impl<T> RaftSnapshotBuilder<OpenRaftTypes<T>> for StateMachineStore<T>
where T: EzApp
{
    type SnapshotData = EzSnapshotData;

    async fn build_snapshot(&mut self) -> Result<EzSnapshot, std::io::Error> {
        // Get current state machine state and build snapshot data
        let (last_applied, last_membership, snapshot_data) = {
            let sm = self.sm_state.lock().await;
            let data = serde_json::to_vec(&sm.app)?;
            (sm.last_applied, sm.membership.clone(), data)
        };

        let snapshot_id = match last_applied {
            Some(log_id) => format!("{}-{}", log_id.leader_id.term, log_id.index),
            None => "0-0".to_string(),
        };

        let snapshot_meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        // Persist before returning: openraft purges logs covered by this snapshot right after,
        // and a durable purge point with no durable snapshot is an unrecoverable state.
        {
            let mut cached = self.snapshot.lock().await;
            let mut storage = self.storage.lock().await;
            storage.persist(Persist::Snapshot(new_snapshot(&snapshot_meta, &snapshot_data))).await?;
            *cached = Some(new_snapshot(&snapshot_meta, &snapshot_data));
        }

        Ok(new_snapshot(&snapshot_meta, &snapshot_data))
    }
}

/// Build a [`EzSnapshot`] from its parts
fn new_snapshot(meta: &EzSnapshotMeta, data: &[u8]) -> EzSnapshot {
    Snapshot {
        meta: meta.clone(),
        snapshot: Cursor::new(data.to_vec()),
    }
}
