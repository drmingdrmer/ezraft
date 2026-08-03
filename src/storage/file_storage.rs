//! A file-based [`EzStorage`]

use std::io;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;

use async_trait::async_trait;
use tokio::fs;

use crate::app::EzApp;
use crate::entry::EzEntry;
use crate::meta::EzMeta;
use crate::snapshot::EzSnapshot;
use crate::snapshot::EzSnapshotMeta;
use crate::storage::EzStorage;
use crate::storage::Loaded;
use crate::storage::Persist;

/// Keeps one node's Raft state as JSON files under a directory
///
/// Enough to get a cluster running without writing storage code first, and shaped to be read:
/// every file is JSON, one log entry per file, so `ls` and `cat` show what Raft persisted. It
/// is not the storage a deployment keeps.
///
/// # Layout
///
/// ```text
/// <base_dir>/meta.json        Raft metadata (node id, vote, log positions)
/// <base_dir>/logs/log-<idx>   one log entry per file
/// <base_dir>/snapshot         snapshot metadata on the first line, application state after it
/// ```
///
/// A snapshot is one file because its two halves must not be mixed: metadata naming a log
/// position the state next to it does not cover would make startup skip the entries in between.
/// Metadata and state are written together and published by a single rename, so a process that
/// dies mid-write leaves the previous snapshot whole. `meta.json` is replaced the same way rather
/// than overwritten, since it is the only copy of the node's id and vote.
///
/// # Write your own when these matter
///
/// - **Durability.** Writes are not `fsync`ed, so a machine crash can lose a vote or a log entry
///   this node already acknowledged - the one assumption [`EzStorage`] cannot do without. Anything
///   beyond a demo needs storage that returns from `persist` only once the bytes would survive a
///   power loss.
/// - **Log size.** One file and one directory entry per entry costs an inode and a round of
///   metadata I/O per write. A log-structured file or an embedded store amortizes both.
/// - **Encoding.** JSON is readable and slow, and it cannot represent every Rust type a request
///   might hold.
///
/// [`EzStorage`] is three methods, and this file is the worked example of implementing them.
pub struct FileStorage {
    base_dir: PathBuf,
}

impl FileStorage {
    /// Open (and create if missing) the directory holding this node's state
    ///
    /// One directory per node: two nodes sharing it would overwrite each other's log.
    pub async fn new(base_dir: impl Into<PathBuf>) -> Result<Self, io::Error> {
        let base_dir = base_dir.into();
        fs::create_dir_all(&base_dir).await?;
        Ok(Self { base_dir })
    }

    fn meta_path(&self) -> PathBuf {
        self.base_dir.join("meta.json")
    }

    fn logs_dir(&self) -> PathBuf {
        self.base_dir.join("logs")
    }

    fn log_path(&self, index: u64) -> PathBuf {
        self.logs_dir().join(format!("log-{}", index))
    }

    fn snapshot_path(&self) -> PathBuf {
        self.base_dir.join("snapshot")
    }

    /// Replace `path` with `bytes` in one step
    ///
    /// The bytes go to a temporary file first and are published by renaming it over `path`, so a
    /// process that dies anywhere in here leaves either the previous file or the new one, never
    /// half of each. This is atomicity against a crash of the process only: without `fsync` a
    /// power loss can still land the rename ahead of the bytes it publishes.
    async fn replace(&self, path: PathBuf, bytes: Vec<u8>) -> Result<(), io::Error> {
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");

        fs::write(&tmp, bytes).await?;
        fs::rename(tmp, path).await
    }

    /// Delete every log file whose index satisfies `remove`
    async fn remove_logs(&self, remove: impl Fn(u64) -> bool) -> Result<(), io::Error> {
        let mut dir = match fs::read_dir(self.logs_dir()).await {
            Ok(dir) => dir,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };

        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name();
            let index = name.to_str().and_then(|n| n.strip_prefix("log-")).and_then(|n| n.parse::<u64>().ok());

            let Some(index) = index else {
                return Err(io::Error::other(format!("unexpected file in log dir: {:?}", name)));
            };

            if remove(index) {
                fs::remove_file(entry.path()).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<T> EzStorage<T> for FileStorage
where T: EzApp
{
    async fn load(&mut self) -> Result<Loaded, io::Error> {
        // Load meta (use default if not found)
        let meta = match fs::read(&self.meta_path()).await {
            Ok(data) => serde_json::from_slice(&data)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => EzMeta::default(),
            Err(e) => return Err(e),
        };

        // Load snapshot (optional)
        let snapshot = match fs::read(&self.snapshot_path()).await {
            Ok(file) => {
                // The metadata line ends at the first newline: JSON escapes the newlines inside
                // it, so the first raw one is where the application state begins.
                let Some(end_of_meta) = file.iter().position(|byte| *byte == b'\n') else {
                    return Err(io::Error::other("snapshot file has no metadata line"));
                };

                let snap_meta: EzSnapshotMeta = serde_json::from_slice(&file[..end_of_meta])?;
                Some(EzSnapshot {
                    meta: snap_meta,
                    snapshot: Cursor::new(file[end_of_meta + 1..].to_vec()),
                })
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };

        Ok(Loaded { meta, snapshot })
    }

    async fn persist(&mut self, op: Persist<T>) -> Result<(), io::Error> {
        match op {
            Persist::Meta(meta) => {
                self.replace(self.meta_path(), serde_json::to_vec_pretty(&meta)?).await?;
            }
            Persist::LogEntry(entry) => {
                fs::create_dir_all(&self.logs_dir()).await?;
                let (_, index) = entry.log_id;
                fs::write(self.log_path(index), serde_json::to_vec(&entry)?).await?;
            }
            Persist::Snapshot(snapshot) => {
                let mut file = serde_json::to_vec(&snapshot.meta)?;
                file.push(b'\n');

                // Extract data from cursor, appending it after the metadata line
                let mut cursor = snapshot.snapshot;
                cursor.seek(SeekFrom::Start(0))?;
                cursor.read_to_end(&mut file)?;

                self.replace(self.snapshot_path(), file).await?;
            }
            Persist::DeleteLogs { from, to } => self.remove_logs(|index| (from..to).contains(&index)).await?,
        }
        Ok(())
    }

    async fn read_logs(&mut self, start: u64, end: u64) -> Result<Vec<EzEntry<T>>, io::Error> {
        let mut logs = Vec::new();

        // Every index in the range must be there: a gap handed back to Raft would look like a
        // shorter log rather than the missing entry it is.
        for index in start..end {
            let data = fs::read(&self.log_path(index)).await?;
            logs.push(serde_json::from_slice(&data)?);
        }

        Ok(logs)
    }
}

#[cfg(test)]
mod tests {
    use openraft::LogId;
    use openraft::Membership;
    use openraft::StoredMembership;
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;
    use crate::type_config::EzVote;

    #[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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

    /// A directory of its own per test, cleared on entry so a run starts empty and left behind on
    /// failure to be looked at
    async fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ezraft-file-storage-{}", name));

        if fs::try_exists(&dir).await.unwrap() {
            fs::remove_dir_all(&dir).await.unwrap();
        }

        dir
    }

    fn snapshot(index: u64, data: &[u8]) -> EzSnapshot {
        let last_log_id = LogId::new_term_index(1, index);

        EzSnapshot {
            meta: EzSnapshotMeta {
                last_log_id: Some(last_log_id),
                last_membership: StoredMembership::new(Some(last_log_id), Membership::default()),
                snapshot_id: format!("1-{}", index),
            },
            snapshot: Cursor::new(data.to_vec()),
        }
    }

    /// Load what a storage at `dir` holds, as the pair a caller compares against
    async fn load(dir: &PathBuf) -> (EzMeta, Option<(EzSnapshotMeta, Vec<u8>)>) {
        let mut storage = FileStorage::new(dir).await.unwrap();
        let loaded = EzStorage::<Counter>::load(&mut storage).await.unwrap();

        let snapshot = loaded.snapshot.map(|snap| (snap.meta, snap.snapshot.into_inner()));
        (loaded.meta, snapshot)
    }

    async fn persist(dir: &PathBuf, op: Persist<Counter>) {
        let mut storage = FileStorage::new(dir).await.unwrap();
        storage.persist(op).await.unwrap();
    }

    /// The bug this guards: metadata and state used to be two files written one after the other,
    /// so a process that died in between published the new metadata over the old state. Startup
    /// accepted that pair and reported a `last_log_id` the state did not cover, letting openraft
    /// skip replaying the entries between the two generations.
    ///
    /// A crash mid-replacement is simulated by leaving behind exactly what `replace` writes before
    /// its rename: a complete temporary file, never published.
    #[tokio::test]
    async fn a_snapshot_interrupted_mid_write_leaves_the_previous_one_whole() {
        let dir = test_dir("interrupted-snapshot").await;

        persist(&dir, Persist::Snapshot(snapshot(10, b"first"))).await;

        let mut interrupted = serde_json::to_vec(&snapshot(20, b"").meta).unwrap();
        interrupted.push(b'\n');
        interrupted.extend_from_slice(b"second");
        fs::write(dir.join("snapshot.tmp"), interrupted).await.unwrap();

        let (_meta, loaded) = load(&dir).await;
        assert_eq!(Some((snapshot(10, b"first").meta, b"first".to_vec())), loaded);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    /// The other half of the contract: a replacement that does complete is visible whole.
    #[tokio::test]
    async fn a_completed_snapshot_replaces_both_halves() {
        let dir = test_dir("replaced-snapshot").await;

        persist(&dir, Persist::Snapshot(snapshot(10, b"first"))).await;
        persist(&dir, Persist::Snapshot(snapshot(20, b"second"))).await;

        let (_meta, loaded) = load(&dir).await;
        assert_eq!(Some((snapshot(20, b"second").meta, b"second".to_vec())), loaded);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    /// The metadata line ends at the first newline, so state containing newlines of its own must
    /// come back byte for byte.
    #[tokio::test]
    async fn snapshot_state_holding_newlines_round_trips() {
        let dir = test_dir("newline-snapshot").await;

        let data = b"{\n  \"count\": 7\n}\n";
        persist(&dir, Persist::Snapshot(snapshot(10, data))).await;

        let (_meta, loaded) = load(&dir).await;
        assert_eq!(Some((snapshot(10, data).meta, data.to_vec())), loaded);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    /// `meta.json` is the only copy of the node's id and vote, so an interrupted write must not
    /// leave it half-rewritten - a node whose metadata does not parse cannot start at all.
    #[tokio::test]
    async fn meta_interrupted_mid_write_keeps_the_previous_value() {
        let dir = test_dir("interrupted-meta").await;

        let meta = EzMeta {
            node_id: Some(7),
            vote: Some(EzVote::new(3, 7)),
            last_log_id: Some((3, 42)),
            last_purged: None,
        };
        persist(&dir, Persist::Meta(meta.clone())).await;

        fs::write(dir.join("meta.json.tmp"), b"{\"node_id\":9,\"vo").await.unwrap();

        let (loaded, _snapshot) = load(&dir).await;
        assert_eq!(meta, loaded);

        fs::remove_dir_all(&dir).await.unwrap();
    }
}
