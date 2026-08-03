//! Starting a node: what `create`, `join` and `serve` must refuse, report, or finish
//!
//! Every test drives EzRaft through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use common::*;
use ezraft::EzConfig;
use ezraft::EzRaft;

/// `serve` must report a server that cannot start, even for a joining voter whose promotion is
/// still pending. The promotion is waiting for this node to catch up, which it can only do
/// through the server that just failed, so collecting the promotion first would sit on the bind
/// error for the twenty attempts and ninety seconds each it still has to spend.
#[tokio::test(flavor = "multi_thread")]
async fn serve_reports_a_server_that_cannot_bind_instead_of_awaiting_promotion() -> io::Result<()> {
    let (seed, _seed_node) = founding_node().await?;

    // Hold the address the node is about to claim, so its server cannot bind it.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap().to_string();

    let node = EzRaft::join(&addr, &seed, KvSm::default(), MemStorage::default(), config()).await?;

    // Comfortably below the promotion's own retry budget, and far above what returning a bind
    // error takes.
    let served = tokio::time::timeout(Duration::from_secs(5), node.serve())
        .await
        .map_err(|_| io::Error::other("serve is still waiting for a promotion its server cannot deliver"))?;

    let err = served.expect_err("serve reports the bind failure");
    assert_eq!(
        io::ErrorKind::AddrInUse,
        err.kind(),
        "the error is the bind failure itself: {}",
        err
    );

    Ok(())
}

/// Converting the configuration is the one step of a join that can fail on its arguments alone,
/// so it happens before the cluster is asked for an id or told about this node. A caller that
/// mistyped a duration must not leave a learner behind for someone to remove by hand.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_with_an_unusable_config_leaves_the_cluster_untouched() -> io::Result<()> {
    let (seed, seed_node) = founding_node().await?;

    let unusable = EzConfig {
        heartbeat_interval: Duration::ZERO,
        ..config()
    };
    let err = EzRaft::join(free_addr(), &seed, KvSm::default(), MemStorage::default(), unusable)
        .await
        .map(|_| ())
        .expect_err("a config the node cannot run with is refused");

    let membership = seed_node.metrics().await.membership_config.membership().clone();
    assert_eq!(
        BTreeSet::from([0]),
        membership.voter_ids().collect::<BTreeSet<_>>(),
        "the voter set is untouched, after: {}",
        err
    );
    assert_eq!(
        BTreeSet::<u64>::new(),
        membership.learner_ids().collect::<BTreeSet<_>>(),
        "no learner was added, after: {}",
        err
    );

    Ok(())
}

/// Joining is two steps - enter the membership as a learner, then be promoted - and a process that
/// dies between them comes back a learner. Restarting repeats neither step: the id is persisted, so
/// the seed is not asked again, and a node is whatever the membership says it is. The promotion is
/// retried the way any learner is promoted, with `promote` on the leader.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_promotion_is_finished_by_promote_not_by_restarting() -> io::Result<()> {
    let (seed, seed_node) = founding_node().await?;

    // A learner in the membership with its id persisted is exactly the state a join whose
    // promotion never landed leaves behind.
    let addr = free_addr();
    let storage = MemStorage::default();
    let first = EzRaft::join_as_learner(&addr, &seed, KvSm::default(), storage.clone(), config()).await?;
    let node_id = first.node_id();
    drop(first);

    // Restarting as a voter join, which is what the caller who wanted a voter would run again.
    let restarted = EzRaft::join(&addr, &seed, KvSm::default(), storage.clone(), config()).await?;
    spawn_serve(&restarted);

    // Being caught up is the only thing a promotion waits for, so a restart that meant to promote
    // would have done it by now.
    seed_node.write(set("k1", "v1")).await?;
    wait_for_applied(&restarted, &seed_node).await?;

    let membership = seed_node.metrics().await.committed_membership_config.membership().clone();
    assert_eq!(BTreeSet::from([0]), membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        BTreeSet::from([node_id]),
        membership.learner_ids().collect::<BTreeSet<_>>(),
        "a restart repeats neither step of the join"
    );

    // And the retry is a plain promotion.
    seed_node.promote(node_id).await?;

    let voters = BTreeSet::from([0, node_id]);
    for node in [&seed_node, &restarted] {
        wait_for_voters(node, voters.clone(), "the retried promotion made it a voter").await?;
    }

    Ok(())
}
