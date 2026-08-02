//! Internal storage adapter
//!
//! This module bridges the user's [`EzApp`] and [`EzStorage`] traits
//! to OpenRaft's [`RaftLogStorage`] and [`RaftStateMachine`] traits.
//!
//! Users don't interact with this module directly - it's used internally by [`crate::EzRaft`].

use std::fmt::Debug;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::ops::RangeBounds;
use std::sync::Arc;

use futures::StreamExt;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use openraft::alias::LogIdOf;
use openraft::alias::StoredMembershipOf;
use openraft::log_id::LogIndexOptionExt;
use openraft::log_id::RaftLogId;
use openraft::storage::EntryResponder;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use tokio::sync::Mutex;

use crate::app::EzApp;
use crate::meta::EzMeta;
use crate::snapshot::EzSnapshot;
use crate::snapshot::EzSnapshotData;
use crate::snapshot::EzSnapshotMeta;
use crate::storage::EzStorage;
use crate::storage::Loaded;
use crate::storage::Persist;
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

/// The user's storage, reached by both stores
///
/// Shared only because [`EzStorage::persist`] takes `&mut self`, so every write serializes
/// through it whichever store makes it. **Take a store's own cache before this, never the
/// reverse, and never two caches at once** - that ordering is the whole reason these cannot
/// deadlock.
type SharedStorage<T> = Arc<Mutex<Box<dyn EzStorage<T>>>>;

/// The log: entries, and the metadata that bounds them
///
/// Only metadata is cached in memory - entries are read from user storage on demand.
pub struct LogStore<T>
where T: EzApp
{
    storage: SharedStorage<T>,

    /// Raft metadata: the vote, and the positions bounding the log
    meta: Arc<Mutex<EzMeta>>,
}

/// The state machine: the user's application, what it has applied, and its snapshot
pub struct StateMachineStore<T>
where T: EzApp
{
    storage: SharedStorage<T>,

    sm_state: Arc<Mutex<StateMachineState<T>>>,

    /// The snapshot last written or loaded, kept so that serving one to a lagging follower does
    /// not re-run the startup-only [`EzStorage::load`].
    snapshot: Arc<Mutex<Option<EzSnapshot>>>,
}

// Hand-written, because deriving would demand `T: Clone` of an application that has no reason
// to be cloneable. Both stores are handles: cloning one shares its state.
impl<T> Clone for LogStore<T>
where T: EzApp
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            meta: self.meta.clone(),
        }
    }
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

/// Open the user's storage as the two stores openraft asks for
///
/// One [`EzStorage::load`], because there is one storage behind both: the metadata it returns
/// belongs to the log, the snapshot to the state machine.
pub async fn open<T>(
    mut user_storage: impl EzStorage<T>,
    app: T,
) -> Result<(LogStore<T>, StateMachineStore<T>), std::io::Error>
where
    T: EzApp,
{
    // Load initial metadata and snapshot
    let Loaded { meta, snapshot } = user_storage.load().await?;

    let mut app = app;

    // Initialize state machine state from snapshot or defaults.
    //
    // The snapshot data must be restored here, not just its position: reporting
    // `last_applied` at the snapshot makes openraft re-apply only the log tail after it, and
    // skip installing this snapshot itself.
    let (last_applied, last_membership) = match &snapshot {
        Some(snap) => {
            app = serde_json::from_slice(snap.snapshot.get_ref())?;
            (snap.meta.last_log_id, snap.meta.last_membership.clone())
        }
        None => (None, StoredMembership::new(None, Membership::default())),
    };

    let storage: Box<dyn EzStorage<T>> = Box::new(user_storage);
    let storage: SharedStorage<T> = Arc::new(Mutex::new(storage));

    let sm_state = StateMachineState {
        app,
        last_applied,
        membership: last_membership,
    };

    let log = LogStore {
        storage: storage.clone(),
        meta: Arc::new(Mutex::new(meta)),
    };

    let sm = StateMachineStore {
        storage,
        sm_state: Arc::new(Mutex::new(sm_state)),
        snapshot: Arc::new(Mutex::new(snapshot)),
    };

    Ok((log, sm))
}

impl<T> LogStore<T>
where T: EzApp
{
    /// Update metadata and persist to storage
    pub async fn save_meta(&self, f: impl FnOnce(&mut EzMeta)) -> Result<(), std::io::Error> {
        let mut meta = self.meta.lock().await;
        f(&mut meta);

        let mut storage = self.storage.lock().await;
        persist_meta(&mut meta, &mut **storage).await
    }

    /// Get the current node_id from cached metadata
    pub async fn node_id(&self) -> Option<u64> {
        self.meta.lock().await.node_id
    }
}

/// Write metadata out, with the log positions made consistent first
///
/// openraft reads both positions at startup and rejects `last_log_id < last_purged` as a corrupt
/// store. Installing a snapshot is what inverts them - the purge that follows moves the purge
/// point past a log that stopped short of the snapshot - so the repair belongs on the one path
/// every metadata write takes, not at the call sites.
///
/// Takes the two guards rather than the adapter, because most callers already hold both and the
/// mutex is not reentrant.
async fn persist_meta<T>(meta: &mut EzMeta, storage: &mut dyn EzStorage<T>) -> Result<(), std::io::Error>
where T: EzApp {
    meta.last_log_id = meta.last_log_id.max(meta.last_purged);

    storage.persist(Persist::Meta(meta.clone())).await
}

impl<T> RaftLogStorage<OpenRaftTypes<T>> for LogStore<T>
where T: EzApp
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<OpenRaftTypes<T>>, std::io::Error> {
        let meta = self.meta.lock().await;
        let last = meta.last_log_id.map(|(t, i)| LogId::new_term_index(t, i));
        let last_purged = meta.last_purged.map(|(t, i)| LogId::new_term_index(t, i));

        Ok(LogState {
            last_log_id: last,
            last_purged_log_id: last_purged,
        })
    }

    async fn save_vote(&mut self, vote: &<OpenRaftTypes<T> as RaftTypeConfig>::Vote) -> Result<(), std::io::Error> {
        self.save_meta(|m| m.vote = Some(*vote)).await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<OpenRaftTypes<T>>) -> Result<(), std::io::Error>
    where
        I: IntoIterator<Item = <OpenRaftTypes<T> as RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // Both locks for the whole batch and the metadata that describes it, so no reader can
        // observe a log that reaches past the last_log_id recorded for it.
        let res = async {
            let mut meta = self.meta.lock().await;
            let mut storage = self.storage.lock().await;

            let mut last_log_id = None;

            // Save all log entries
            for entry in entries {
                last_log_id = Some(entry.log_id);
                storage.persist(Persist::LogEntry(entry)).await?;
            }

            // Update metadata once with the last entry's log_id
            if let Some(log_id) = last_log_id {
                meta.last_log_id = Some(log_id);
                persist_meta(&mut meta, &mut **storage).await?;
            }

            Ok::<_, std::io::Error>(())
        }
        .await;

        // openraft is waiting on this callback; returning the error alone would leave the append
        // in flight forever.
        match res {
            Ok(()) => {
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.io_completed(Err(std::io::Error::other(e.to_string())));
                Err(e)
            }
        }
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<OpenRaftTypes<T>>>) -> Result<(), std::io::Error> {
        let from = last_log_id.map(|id| id.index).next_index();

        // Both held across the delete and the position it moves, so no reader sees the log
        // shrink out from under the last_log_id recorded for it.
        let mut meta = self.meta.lock().await;
        let mut storage = self.storage.lock().await;

        storage.persist(Persist::DeleteLogs { from, to: u64::MAX }).await?;

        meta.last_log_id = last_log_id.map(|id| id.to_type());
        persist_meta(&mut meta, &mut **storage).await
    }

    async fn purge(&mut self, log_id: LogIdOf<OpenRaftTypes<T>>) -> Result<(), std::io::Error> {
        let mut meta = self.meta.lock().await;
        let mut storage = self.storage.lock().await;

        storage
            .persist(Persist::DeleteLogs {
                from: 0,
                to: log_id.index + 1,
            })
            .await?;

        meta.last_purged = Some(log_id.to_type());
        persist_meta(&mut meta, &mut **storage).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

impl<T> RaftLogReader<OpenRaftTypes<T>> for LogStore<T>
where T: EzApp
{
    async fn read_vote(&mut self) -> Result<Option<<OpenRaftTypes<T> as RaftTypeConfig>::Vote>, std::io::Error> {
        Ok(self.meta.lock().await.vote)
    }

    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<OpenRaftTypes<T> as RaftTypeConfig>::Entry>, std::io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        // Both held until the entries have been read: a purge landing in between would leave the
        // clamped range pointing at entries user storage has already deleted.
        let meta = self.meta.lock().await;
        let mut storage = self.storage.lock().await;

        // Available log range: [lo, hi)
        let lo = meta.last_purged.map(|(_, i)| i).next_index();
        let hi = meta.last_log_id.map(|(_, i)| i).next_index();

        let start = match range.start_bound() {
            std::ops::Bound::Included(&x) => x,
            std::ops::Bound::Excluded(&x) => x + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&x) => x + 1,
            std::ops::Bound::Excluded(&x) => x,
            std::ops::Bound::Unbounded => hi,
        };

        // Clamp to available range
        let start = std::cmp::max(start, lo);
        let end = std::cmp::min(end, hi);

        if start >= end {
            return Ok(Vec::new());
        }

        // Load only the requested range from user storage
        storage.read_logs(start, end).await
    }
}

impl<T> StateMachineStore<T>
where T: EzApp
{
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
