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

        // Deserialized before anything keeps it. Bytes that do not parse - a corrupt transfer, a
        // peer of an incompatible version - are this transfer's problem, and persisting them would
        // make them every later start's problem too: [`Self::new`] reads the stored snapshot and
        // would fail on the same bytes, with the state they replaced already gone.
        let app = serde_json::from_slice(&data)?;

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
            sm.app = app;
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use openraft::LogId;
    use openraft::Membership;
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;
    use crate::entry::EzEntry;
    use crate::meta::EzMeta;
    use crate::storage::EzStorage;
    use crate::storage::Loaded;
    use crate::storage::adapter::open;

    #[derive(Default, Serialize, Deserialize)]
    struct Counter {
        count: u64,
    }

    #[async_trait]
    impl EzApp for Counter {
        type Request = u64;
        type Response = u64;

        async fn apply(&mut self, req: u64) -> u64 {
            self.count += req;
            self.count
        }

        type ReadRequest = ();
        type ReadResponse = u64;

        fn read(&self, _req: ()) -> u64 {
            self.count
        }
    }

    /// A snapshot as it sits in storage: its metadata, and the bytes of the state
    type StoredSnapshot = Option<(EzSnapshotMeta, Vec<u8>)>;

    /// Storage that keeps the snapshot it was last asked to persist
    #[derive(Clone, Default)]
    struct SnapshotStore {
        snapshot: Arc<StdMutex<StoredSnapshot>>,
    }

    #[async_trait]
    impl EzStorage<Counter> for SnapshotStore {
        async fn load(&mut self) -> Result<Loaded, std::io::Error> {
            Ok(Loaded {
                meta: EzMeta::default(),
                snapshot: None,
            })
        }

        async fn persist(&mut self, op: Persist<Counter>) -> Result<(), std::io::Error> {
            if let Persist::Snapshot(snapshot) = op {
                *self.snapshot.lock().unwrap() = Some((snapshot.meta, snapshot.snapshot.into_inner()));
            }
            Ok(())
        }

        async fn read_logs(&mut self, _start: u64, _end: u64) -> Result<Vec<EzEntry<Counter>>, std::io::Error> {
            unreachable!("these tests only install snapshots")
        }
    }

    fn snapshot_meta(index: u64) -> EzSnapshotMeta {
        let last_log_id = LogId::new_term_index(1, index);

        EzSnapshotMeta {
            last_log_id: Some(last_log_id),
            last_membership: StoredMembership::new(Some(last_log_id), Membership::default()),
            snapshot_id: format!("1-{}", index),
        }
    }

    /// A snapshot whose bytes do not deserialize must be refused whole. Keeping it would outlive
    /// the failed transfer: startup restores from the stored snapshot, so the next start would
    /// fail on the same bytes - with the state they replaced already gone.
    #[tokio::test]
    async fn an_invalid_snapshot_leaves_the_last_valid_one_in_place() {
        let store = SnapshotStore::default();
        let persisted = store.snapshot.clone();
        let (_log, mut sm) = open(store, Counter::default()).await.unwrap();

        let valid = serde_json::to_vec(&Counter { count: 7 }).unwrap();
        sm.install_snapshot(&snapshot_meta(10), Cursor::new(valid.clone())).await.unwrap();

        let err = sm.install_snapshot(&snapshot_meta(20), Cursor::new(b"not a counter".to_vec())).await.unwrap_err();
        assert_eq!(std::io::ErrorKind::InvalidData, err.kind(), "unexpected error: {}", err);

        // Nothing of it is kept: not in storage, not in the cache that serves lagging followers,
        // not in the state machine.
        assert_eq!(Some((snapshot_meta(10), valid.clone())), *persisted.lock().unwrap());

        let cached = sm.get_current_snapshot().await.unwrap().map(|snap| (snap.meta, snap.snapshot.into_inner()));
        assert_eq!(Some((snapshot_meta(10), valid)), cached);

        assert_eq!(7, sm.read(|app| app.count).await);
    }
}
