//! Snapshots and the log they replace: persistence, restart, replay, catching a
//! lagging node up, and purging what a snapshot covers
//!
//! Every test drives EzRaft through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;

use common::*;
use ezraft::EzConfig;
use ezraft::EzRaft;

/// A snapshot must land on disk when built, and a restarted node must rebuild
/// the full state from that snapshot plus the log entries after it.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_survives_restart() -> io::Result<()> {
    let addr = free_addr();
    let storage = MemStorage::default();

    let a = EzRaft::create(&addr, KvSm::default(), storage.clone(), config()).await?;
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "single node leads")
        .await
        .map_err(io::Error::other)?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    a.inner().trigger().snapshot().await.map_err(io::Error::other)?;
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.snapshot.is_some(), "snapshot built")
        .await
        .map_err(io::Error::other)?;

    // The snapshot must be on disk with the full applied state: a restart
    // reads only the disk, so an unpersisted snapshot is lost data.
    {
        let disk = storage.disk.lock().unwrap();
        let (meta, data) = disk.snapshot.as_ref().expect("snapshot persisted to storage");
        let snapshot_state: KvSm = serde_json::from_slice(data)?;
        assert_eq!(expected_map(0..10), snapshot_state.data);
        assert!(meta.last_log_id.is_some());
    }

    // More writes after the snapshot: the restart must replay these on top.
    for i in 10..15 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    a.inner().shutdown().await.map_err(io::Error::other)?;
    drop(a);

    // Restart on the same disk with an empty state machine.
    let restarted = EzRaft::create(&addr, KvSm::default(), storage.clone(), config()).await?;
    restarted
        .inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "restarted node leads")
        .await
        .map_err(io::Error::other)?;
    restarted
        .inner()
        .wait(WAIT)
        .metrics(
            |m| m.last_applied.map(|log_id| log_id.index) == m.last_log_index,
            "log tail replayed",
        )
        .await
        .map_err(io::Error::other)?;

    // After the linearizable barrier, a local read must serve every write
    // acknowledged before the shutdown.
    restarted.linearizable().await?;
    assert_eq!(expected_map(0..15), restarted.read(|app| app.data.clone()).await?);

    // And the restarted node serves reads over the rebuilt state.
    assert_eq!(
        Response {
            value: Some("v14".into())
        },
        restarted.write(get("k14")).await?
    );

    Ok(())
}

/// `apply` is not exactly-once per entry, and the trait documents it as replayable because of
/// this: a restart resumes applying at the snapshot, so every entry after it reaches `apply`
/// again. An app that acted on the outside world in there would act twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_reapplies_the_entries_after_the_snapshot() -> io::Result<()> {
    let addr = free_addr();
    let storage = MemStorage::default();

    let a = EzRaft::create(&addr, tagged("replay"), storage.clone(), config()).await?;
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "single node leads")
        .await
        .map_err(io::Error::other)?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    a.inner().trigger().snapshot().await.map_err(io::Error::other)?;
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.snapshot.is_some(), "snapshot built")
        .await
        .map_err(io::Error::other)?;

    // These land after the snapshot, so they are the ones a restart has to replay.
    for i in 10..15 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    let first_run: Vec<String> = (0..15).map(|i| format!("Set(k{})", i)).collect();
    assert_eq!(first_run, applied("replay"), "each write applied once while running");

    a.inner().shutdown().await.map_err(io::Error::other)?;
    drop(a);

    // Restart on the same disk with an empty state machine: the snapshot restores the state up
    // to k9, and the log carries k10..k15 back through `apply`.
    let restarted = EzRaft::create(&addr, tagged("replay"), storage.clone(), config()).await?;
    restarted
        .inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "restarted node leads")
        .await
        .map_err(io::Error::other)?;
    restarted
        .inner()
        .wait(WAIT)
        .metrics(
            |m| m.last_applied.map(|log_id| log_id.index) == m.last_log_index,
            "log tail replayed",
        )
        .await
        .map_err(io::Error::other)?;

    let replayed: Vec<String> = (10..15).map(|i| format!("Set(k{})", i)).collect();
    assert_eq!(
        [first_run, replayed].concat(),
        applied("replay"),
        "the entries after the snapshot ran twice"
    );

    // Replaying them a second time is what makes the state whole again.
    assert_eq!(expected_map(0..15), restarted.read(|app| app.data.clone()).await?);

    Ok(())
}

/// A node that joins after the leader purged its log can only be brought up by
/// a full snapshot over the network; its promotion waits for that transfer and
/// must still end in a voter with the complete state.
#[tokio::test(flavor = "multi_thread")]
async fn lagging_joiner_catches_up_from_snapshot() -> io::Result<()> {
    let addr_a = free_addr();
    let a = EzRaft::create(&addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let a = a.clone();
        async move { a.serve().await }
    });
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "founding node leads")
        .await
        .map_err(io::Error::other)?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    // Snapshot, then purge every covered entry: whoever joins now cannot be
    // caught up by log replay.
    a.inner().trigger().snapshot().await.map_err(io::Error::other)?;
    let snapshot_index = a
        .inner()
        .wait(WAIT)
        .metrics(|m| m.snapshot.is_some(), "snapshot built")
        .await
        .map_err(io::Error::other)?
        .snapshot
        .unwrap()
        .index;
    a.inner().trigger().purge_log(snapshot_index).await.map_err(io::Error::other)?;
    a.inner()
        .wait(WAIT)
        .metrics(
            |m| m.purged.map(|log_id| log_id.index) == Some(snapshot_index),
            "log purged up to the snapshot",
        )
        .await
        .map_err(io::Error::other)?;

    let addr_b = free_addr();
    let b = EzRaft::join(&addr_b, &addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let b = b.clone();
        async move { b.serve().await }
    });

    let voters = BTreeSet::from([0, b.node_id()]);
    wait_for_voters(&b, voters, "snapshot-fed joiner promoted to voter").await?;

    // The whole pre-purge state must have arrived through the snapshot.
    assert_eq!(expected_map(0..10), b.read(|app| app.data.clone()).await?);

    // And the pair keeps working past the transfer.
    assert_eq!(Response { value: None }, http_write(&addr_b, set("k10", "v10")).await?);
    assert_eq!(
        Response {
            value: Some("v0".into())
        },
        http_write(&addr_b, get("k0")).await?
    );

    Ok(())
}

/// A demo-sized write load must drive the whole persist lifecycle on its own:
/// with `snapshot_interval` configured, a snapshot reaches storage and the log
/// entries it covers are deleted, all without a manual trigger.
#[tokio::test(flavor = "multi_thread")]
async fn automatic_snapshot_purges_old_logs() -> io::Result<()> {
    let addr = free_addr();
    let storage = MemStorage::default();

    let config = EzConfig {
        snapshot_interval: 5,
        ..config()
    };
    let a = EzRaft::create(&addr, KvSm::default(), storage.clone(), config).await?;
    a.inner()
        .wait(WAIT)
        .metrics(|m| m.current_leader == Some(0), "single node leads")
        .await
        .map_err(io::Error::other)?;

    for i in 0..12 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    let metrics = a
        .inner()
        .wait(WAIT)
        .metrics(
            |m| m.snapshot.is_some() && m.purged.is_some(),
            "snapshot built and log purged by the interval policy alone",
        )
        .await
        .map_err(io::Error::other)?;

    let disk = storage.disk.lock().unwrap();
    let (meta, data) = disk.snapshot.as_ref().expect("snapshot persisted to storage");
    assert!(meta.last_log_id.is_some());
    let snapshot_state: KvSm = serde_json::from_slice(data)?;
    assert!(!snapshot_state.data.is_empty());

    // DeleteLogs must have reached storage: every entry at or below the purged
    // index is gone, and entries above it survive.
    let purged_index = metrics.purged.unwrap().index;
    let min_kept = *disk.logs.keys().next().expect("entries above the purge point remain");
    assert!(
        min_kept > purged_index,
        "min kept index {} must be above purged index {}",
        min_kept,
        purged_index
    );

    Ok(())
}

/// A node caught up by a snapshot that has no log entries after it.
///
/// This is the one path where the purge openraft runs after installing a
/// snapshot moves `last_purged` past a `last_log_id` that no later append comes
/// along to repair. Everywhere else - a joining node, a follower still being
/// replicated to - entries follow the install and hide it. `save_meta` keeps the
/// two ordered; without that the node reads back an inverted pair and openraft
/// refuses to start it, calling the log store corrupted.
///
/// The learner joins and only then starts serving, which is what leaves it with
/// nothing but the snapshot to catch up from: `add_learner` does not wait for
/// the node it adds, so the leader can compact its whole log first.
#[tokio::test(flavor = "multi_thread")]
async fn restart_after_a_snapshot_with_no_log_after_it() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;

    // A learner, so `a` keeps a quorum by itself while this node stays dark.
    let addr_c = free_addr();
    let disk_c = MemStorage::default();
    let c = EzRaft::join_as_learner(&addr_c, &addr_a, KvSm::default(), disk_c.clone(), config()).await?;

    for i in 0..10 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    // Snapshot at `a`'s last entry and purge everything it covers.
    a.inner().trigger().snapshot().await.map_err(io::Error::other)?;
    let snapshot_index = a
        .inner()
        .wait(WAIT)
        .metrics(|m| m.snapshot.is_some(), "snapshot built")
        .await
        .map_err(io::Error::other)?
        .snapshot
        .unwrap()
        .index;
    a.inner().trigger().purge_log(snapshot_index).await.map_err(io::Error::other)?;
    a.inner()
        .wait(WAIT)
        .metrics(
            |m| m.purged.map(|log_id| log_id.index) == Some(snapshot_index),
            "log purged up to the snapshot",
        )
        .await
        .map_err(io::Error::other)?;
    assert_eq!(
        Some(snapshot_index),
        a.metrics().await.last_log_index,
        "the snapshot has to cover the whole log, or an append would follow it"
    );

    // Only now can the leader reach it, and only a snapshot can catch it up.
    spawn_serve(&c);
    wait_for_applied(&c, &a).await?;
    assert_eq!(expected_map(0..10), c.read(|app| app.data.clone()).await?);

    let meta = disk_c.disk.lock().unwrap().meta.clone();
    assert!(
        meta.last_purged <= meta.last_log_id,
        "persisted an inverted log range: last_purged={:?} last_log_id={:?}",
        meta.last_purged,
        meta.last_log_id
    );

    // Opening the store again is the real check: it is where openraft reads
    // both positions back and refuses an inverted pair. A fresh address because
    // this node is only constructed, never served.
    c.inner().shutdown().await.map_err(io::Error::other)?;
    drop(c);
    EzRaft::join_as_learner(&free_addr(), &addr_a, KvSm::default(), disk_c, config()).await?;

    Ok(())
}
