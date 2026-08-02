//! The log half of the storage adapter: entries, and the metadata that bounds them

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::alias::LogIdOf;
use openraft::log_id::LogIndexOptionExt;
use openraft::log_id::RaftLogId;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use tokio::sync::Mutex;

use crate::app::EzApp;
use crate::meta::EzMeta;
use crate::storage::EzStorage;
use crate::storage::Persist;
use crate::storage::adapter::SharedStorage;
use crate::type_config::OpenRaftTypes;

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

impl<T> LogStore<T>
where T: EzApp
{
    pub(super) fn new(storage: SharedStorage<T>, meta: EzMeta) -> Self {
        Self {
            storage,
            meta: Arc::new(Mutex::new(meta)),
        }
    }

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
