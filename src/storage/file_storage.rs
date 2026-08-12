//! A file-based [`EzStorage`]

use std::io;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use tokio::fs;
use tokio::io::AsyncWriteExt;

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
/// <base_dir>/snapshot         length-prefixed snapshot metadata, then application state
/// ```
///
/// The snapshot file is the one thing here that is not plain JSON: eight big-endian bytes giving
/// the length of the metadata, the metadata JSON, then the application state. A length says where
/// the metadata ends whatever it contains, which a separator cannot - both halves are bytes this
/// file does not get to constrain.
///
/// A snapshot is one file because its two halves must not be mixed: metadata naming a log
/// position the state next to it does not cover would make startup skip the entries in between.
/// Metadata and state are written together and published by a single rename, so a process that
/// dies mid-write leaves the previous snapshot whole. `meta.json` is replaced the same way rather
/// than overwritten, since it is the only copy of the node's id and vote.
///
/// # Durability
///
/// `persist` returns only once what it wrote would survive a power loss, which is what
/// [`EzStorage`] asks of an implementation: a vote or a log entry this node acknowledged is still
/// there after the machine loses power mid-write.
///
/// That takes two `fsync`s per call, because a file and the name that reaches it are separate
/// writes. Flushing the bytes leaves the directory entry naming them in memory, so a directory is
/// fsynced after every rename, creation and deletion inside it, and each directory this storage
/// creates is fsynced into its own parent.
///
/// This holds on Unix. `std::fs` cannot open a directory on Windows, so a build there keeps the
/// file `fsync`s and the crash atomicity of the rename, and leaves entry durability to the
/// filesystem.
///
/// # Write your own when these matter
///
/// - **Log size.** One file and one directory entry per entry costs an inode, a round of metadata
///   I/O, and the two `fsync`s above per write. A log-structured file or an embedded store
///   amortizes all of them across a batch.
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
        create_dir_durable(&base_dir).await?;

        let storage = Self { base_dir };

        // Created here rather than before the first entry, so that writing one costs no directory
        // creation and the durability of this directory's own entry is settled once.
        create_dir_durable(&storage.logs_dir()).await?;

        Ok(storage)
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
    /// half of each. The bytes are made durable before the rename that publishes them, and the
    /// directory after it, so a power loss cannot land the rename ahead of what it names.
    async fn replace(&self, path: PathBuf, bytes: Vec<u8>) -> Result<(), io::Error> {
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);

        write_durable(&tmp, &bytes).await?;
        fs::rename(&tmp, &path).await?;

        let dir = path.parent().expect("a path under base_dir has a parent");
        sync_dir(dir).await
    }

    /// Delete every log file whose index satisfies `remove`
    async fn remove_logs(&self, remove: impl Fn(u64) -> bool) -> Result<(), io::Error> {
        let mut dir = match fs::read_dir(self.logs_dir()).await {
            Ok(dir) => dir,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };

        let mut deleted = false;

        while let Some(entry) = dir.next_entry().await? {
            let name = entry.file_name();
            let index = name.to_str().and_then(|n| n.strip_prefix("log-")).and_then(|n| n.parse::<u64>().ok());

            let Some(index) = index else {
                return Err(io::Error::other(format!("unexpected file in log dir: {:?}", name)));
            };

            if remove(index) {
                fs::remove_file(entry.path()).await?;
                deleted = true;
            }
        }

        // A deletion is a change to the directory, and it is durable on the same terms as the
        // creation of an entry: once the directory itself is fsynced. One flush covers the batch.
        if deleted {
            sync_dir(&self.logs_dir()).await?;
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
                let (meta_len, rest) = split_meta_len(&file)?;
                if rest.len() < meta_len {
                    return Err(io::Error::other(format!(
                        "snapshot file claims {} bytes of metadata, {} bytes follow the length",
                        meta_len,
                        rest.len()
                    )));
                }

                let (meta_bytes, data) = rest.split_at(meta_len);
                let snap_meta: EzSnapshotMeta = serde_json::from_slice(meta_bytes)?;
                Some(EzSnapshot {
                    meta: snap_meta,
                    snapshot: Cursor::new(data.to_vec()),
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
                let (_, index) = entry.log_id;
                let bytes = serde_json::to_vec(&entry)?;

                // Written in place rather than through `replace`, which the two positions openraft
                // keeps make safe: it appends only past the `last_log_id` it recorded, and
                // `truncate_after` lowers that record before the entries it drops are written
                // over. Either way the index being written is one no read reaches until this call
                // returns, so a file a crash cut short is never read, and the append that follows
                // replaces it.
                write_durable(&self.log_path(index), &bytes).await?;
                sync_dir(&self.logs_dir()).await?;
            }
            Persist::Snapshot(snapshot) => {
                let meta_bytes = serde_json::to_vec(&snapshot.meta)?;

                let mut file = (meta_bytes.len() as u64).to_be_bytes().to_vec();
                file.extend_from_slice(&meta_bytes);

                // Extract data from cursor, appending it after the metadata
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

/// Write `bytes` to `path`, and return once they would survive a power loss
///
/// This makes the file's own contents durable. The name that reaches them is a write to the
/// directory holding it, which [`sync_dir`] is for.
async fn write_durable(path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    let mut file = fs::File::create(path).await?;
    file.write_all(bytes).await?;
    file.sync_all().await
}

/// Make a directory's own list of names durable
///
/// Creating, renaming or deleting a file rewrites the directory naming it, and that is a write of
/// its own: `fsync` on the file settles the bytes and leaves the name reaching them in memory. A
/// power loss there takes back the creation or the deletion, whole.
///
/// Unix only. `std::fs` opens no handle to a directory on Windows, so a build there has nothing to
/// flush and leaves this to the filesystem.
async fn sync_dir(dir: &Path) -> Result<(), io::Error> {
    if !cfg!(unix) {
        return Ok(());
    }

    let dir = fs::File::open(dir).await?;
    dir.sync_all().await
}

/// Create `dir` and every missing parent, and make each new directory's entry durable
///
/// A directory that is created but never flushed into its parent can go away whole in a power
/// loss, taking with it files that were fsynced one by one - a node that started, voted, and came
/// back with no memory of the vote it acknowledged.
async fn create_dir_durable(dir: &Path) -> Result<(), io::Error> {
    // Innermost first, stopping at the first ancestor that is already there: an existing directory
    // needs no new entry and no flush. The empty path is what `parent` gives above a relative root.
    let mut missing = Vec::new();
    let mut cursor = dir;

    while !cursor.as_os_str().is_empty() && !fs::try_exists(cursor).await? {
        missing.push(cursor);

        let Some(parent) = cursor.parent() else {
            break;
        };
        cursor = parent;
    }

    for dir in missing.iter().rev() {
        let created = fs::create_dir(dir).await;
        match created {
            Ok(()) => {}
            // Another node opening its own directory beside this one built the shared parent first.
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }

        let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) else {
            continue;
        };
        sync_dir(parent).await?;
    }

    Ok(())
}

/// Read the snapshot file's leading metadata length, and return it with the bytes after it
fn split_meta_len(file: &[u8]) -> Result<(usize, &[u8]), io::Error> {
    const LEN_BYTES: usize = size_of::<u64>();

    if file.len() < LEN_BYTES {
        return Err(io::Error::other(format!(
            "snapshot file is {} bytes, too short for a {}-byte metadata length",
            file.len(),
            LEN_BYTES
        )));
    }

    let (len, rest) = file.split_at(LEN_BYTES);
    let len = u64::from_be_bytes(len.try_into().expect("split at the width of a u64"));

    // A length wider than a `usize` cannot address anything this process can hold, and a file that
    // claims one is not a snapshot this build wrote.
    let len =
        usize::try_from(len).map_err(|_| io::Error::other(format!("snapshot metadata length {} is absurd", len)))?;

    Ok((len, rest))
}

#[cfg(test)]
mod tests {
    use openraft::EntryPayload;
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

    fn log_entry(term: u64, index: u64, request: u64) -> EzEntry<Counter> {
        EzEntry {
            log_id: (term, index),
            payload: EntryPayload::Normal(request),
        }
    }

    /// The pair a caller compares an entry against: its log id and the request it carries
    fn parts(entry: &EzEntry<Counter>) -> ((u64, u64), u64) {
        let EntryPayload::Normal(request) = &entry.payload else {
            panic!("expected a normal payload, got {:?}", entry.payload);
        };
        (entry.log_id, *request)
    }

    /// A snapshot file as `persist` lays it out: the metadata length, the metadata, the state
    fn framed(snapshot: EzSnapshot) -> Vec<u8> {
        let meta_bytes = serde_json::to_vec(&snapshot.meta).unwrap();

        let mut file = (meta_bytes.len() as u64).to_be_bytes().to_vec();
        file.extend_from_slice(&meta_bytes);
        file.extend_from_slice(snapshot.snapshot.get_ref());
        file
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

        fs::write(dir.join("snapshot.tmp"), framed(snapshot(20, b"second"))).await.unwrap();

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

    /// The metadata is found by its length, not by a separator, so the state is opaque: newlines,
    /// null bytes and something that looks like a length header of its own all come back untouched.
    #[tokio::test]
    async fn snapshot_state_is_returned_byte_for_byte() {
        let dir = test_dir("opaque-snapshot").await;

        let data = b"\0\0\0\0\0\0\0\x07{\n  \"count\": 7\n}\n";
        persist(&dir, Persist::Snapshot(snapshot(10, data))).await;

        let (_meta, loaded) = load(&dir).await;
        assert_eq!(Some((snapshot(10, data).meta, data.to_vec())), loaded);

        fs::remove_dir_all(&dir).await.unwrap();
    }

    /// A file too short to hold the length, or one whose length runs past its own end, is reported
    /// rather than sliced into: a half-written snapshot must not be read as a whole one.
    ///
    /// Only the metadata is framed, so state cut short is not detectable here - it simply runs to
    /// the end of the file. It does not have to be: a snapshot becomes visible by rename, so a
    /// published file is one that was written whole.
    #[tokio::test]
    async fn a_snapshot_file_shorter_than_it_claims_is_refused() {
        let dir = test_dir("truncated-snapshot").await;
        let whole = framed(snapshot(10, b"state"));
        let meta_len = whole.len() - size_of::<u64>() - b"state".len();

        // Too short to hold the length at all, then cut inside the metadata the length describes.
        for cut in [4, size_of::<u64>() + meta_len - 3] {
            fs::create_dir_all(&dir).await.unwrap();
            fs::write(dir.join("snapshot"), &whole[..cut]).await.unwrap();

            let mut storage = FileStorage::new(&dir).await.unwrap();
            let err = EzStorage::<Counter>::load(&mut storage).await.expect_err("a truncated file is refused");
            assert!(
                err.to_string().contains("snapshot file"),
                "cut at {}: unexpected error: {}",
                cut,
                err
            );
        }

        fs::remove_dir_all(&dir).await.unwrap();
    }

    /// Entries come back exactly as they went in, and a delete takes its range and nothing else.
    ///
    /// Both paths write and flush the log directory, which is the half of durability an `fsync` of
    /// the entry file alone does not cover.
    #[tokio::test]
    async fn log_entries_round_trip_and_a_delete_takes_only_its_range() {
        let dir = test_dir("log-entries").await;
        let mut storage = FileStorage::new(&dir).await.unwrap();

        for index in 1..=4 {
            storage.persist(Persist::LogEntry(log_entry(7, index, index * 10))).await.unwrap();
        }

        let all = EzStorage::<Counter>::read_logs(&mut storage, 1, 5).await.unwrap();
        let read_back: Vec<((u64, u64), u64)> = all.iter().map(parts).collect();
        assert_eq!(vec![((7, 1), 10), ((7, 2), 20), ((7, 3), 30), ((7, 4), 40)], read_back);

        // Annotated because `DeleteLogs` carries no application type to infer one from.
        let delete: Persist<Counter> = Persist::DeleteLogs { from: 2, to: 4 };
        storage.persist(delete).await.unwrap();

        // The bounds of the deleted range are the entries that must still be there.
        let first = EzStorage::<Counter>::read_logs(&mut storage, 1, 2).await.unwrap();
        assert_eq!(vec![((7, 1), 10)], first.iter().map(parts).collect::<Vec<_>>());

        let last = EzStorage::<Counter>::read_logs(&mut storage, 4, 5).await.unwrap();
        assert_eq!(vec![((7, 4), 40)], last.iter().map(parts).collect::<Vec<_>>());

        // A deleted index is gone rather than empty: `read_logs` reports it instead of handing
        // back a short log that Raft would read as the end of the entries.
        let gone = EzStorage::<Counter>::read_logs(&mut storage, 2, 3).await.expect_err("index 2 was deleted");
        assert_eq!(io::ErrorKind::NotFound, gone.kind());

        fs::remove_dir_all(&dir).await.unwrap();
    }

    /// A first start lays out its own directories, however many levels are missing, and each one
    /// is fsynced into its parent before anything is written inside it.
    #[tokio::test]
    async fn a_missing_directory_chain_is_created() {
        let root = test_dir("missing-chain").await;
        let base_dir = root.join("cluster").join("node-1");

        let mut storage = FileStorage::new(&base_dir).await.unwrap();
        storage.persist(Persist::LogEntry(log_entry(1, 1, 5))).await.unwrap();

        let entry_exists = fs::try_exists(base_dir.join("logs").join("log-1")).await.unwrap();
        assert!(entry_exists);

        let entries = EzStorage::<Counter>::read_logs(&mut storage, 1, 2).await.unwrap();
        assert_eq!(vec![((1, 1), 5)], entries.iter().map(parts).collect::<Vec<_>>());

        // Opening the same directories again is the ordinary restart, and must not fail on what
        // the first run already created.
        let reopened = FileStorage::new(&base_dir).await;
        assert!(reopened.is_ok());

        fs::remove_dir_all(&root).await.unwrap();
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
