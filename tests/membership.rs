//! Who is in the cluster and who counts towards a quorum: joining, promotion,
//! demotion and removal
//!
//! Every test drives EzRaft through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;

use common::*;
use ezraft::EzRaft;

/// Joining plus serving must be all it takes to become a voter, and those
/// voters must keep the cluster alive when the founding leader dies.
#[tokio::test(flavor = "multi_thread")]
async fn join_promotes_to_voter_and_cluster_survives_leader_death() -> io::Result<()> {
    let addr_a = free_addr();
    let a = EzRaft::create(&addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let a = a.clone();
        async move { a.serve().await }
    });
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    let addr_b = free_addr();
    let b = EzRaft::join(&addr_b, &addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let b = b.clone();
        async move { b.serve().await }
    });

    let addr_c = free_addr();
    let c = EzRaft::join(&addr_c, &addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let c = c.clone();
        async move { c.serve().await }
    });

    // Every node must see the final uniform config with all three voters before
    // the leader may die: a joint config still counts the founding node in its
    // old majority, so killing it mid-change would legitimately lose quorum.
    let voters = BTreeSet::from([0, b.node_id(), c.node_id()]);
    for node in [&a, &b, &c] {
        wait_for_voters(node, voters.clone(), "every promoted node is a voter").await?;
    }

    assert_eq!(Response { value: None }, a.write(set("k1", "v1")).await?);

    // A direct read serves from local memory - no consensus round, no log
    // entry: the app's `read` answers the key from its own map.
    let value: Option<String> = reqwest::Client::new()
        .post(format!("http://{}/api/read", addr_a))
        .json("k1")
        .send()
        .await
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)?;
    assert_eq!(Some("v1".to_string()), value);

    // Kill the leader; the two remaining voters still form a quorum.
    assert!(a.is_leader());
    a.shutdown().await?;

    b.wait_metrics(
        WAIT,
        |m| matches!(m.current_leader, Some(id) if id != 0),
        "a surviving node takes over",
    )
    .await?;

    // The new leader accepts writes (reached from a follower via forwarding)
    // and still has the data acknowledged before the failover.
    assert_eq!(Response { value: None }, http_write(&addr_b, set("k2", "v2")).await?);
    assert_eq!(
        Response {
            value: Some("v1".into())
        },
        http_write(&addr_b, get("k1")).await?
    );
    assert_eq!(
        Response {
            value: Some("v2".into())
        },
        http_write(&addr_b, get("k2")).await?
    );

    Ok(())
}

/// A node that joins as a learner must replicate the log and stay a learner
/// however long it runs, until it is explicitly promoted.
#[tokio::test(flavor = "multi_thread")]
async fn learner_joins_and_stays_a_learner_until_promoted() -> io::Result<()> {
    let addr_a = free_addr();
    let a = EzRaft::create(&addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let a = a.clone();
        async move { a.serve().await }
    });
    a.wait_metrics(WAIT, |m| m.current_leader == Some(0), "founding node leads").await?;

    let addr_b = free_addr();
    let b = EzRaft::join_as_learner(&addr_b, &addr_a, KvSm::default(), MemStorage::default(), config()).await?;
    tokio::spawn({
        let b = b.clone();
        async move { b.serve().await }
    });
    let b_id = b.node_id();

    for i in 1..3 {
        a.write(set(&format!("k{}", i), &format!("v{}", i))).await?;
    }

    // Being caught up is the whole condition a promotion waits for, so once the
    // learner has applied everything, nothing but an explicit promote is left
    // to make it a voter. Anchored to the leader's index: a node that has
    // replicated nothing yet has its own `last_applied` and `last_log_index`
    // both unset, and comparing those two would pass instantly.
    let target = a.metrics().await.last_log_index.expect("the leader has written entries");
    b.wait_metrics(
        WAIT,
        |m| m.last_applied.map(|log_id| log_id.index) >= Some(target),
        "learner applied the leader's log",
    )
    .await?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(BTreeSet::from([0]), membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        BTreeSet::from([b_id]),
        membership.learner_ids().collect::<BTreeSet<_>>()
    );

    // A learner is a full replica; it just does not vote.
    assert_eq!(expected_map(1..3), b.read(|app| app.data.clone()).await?);

    // A node the cluster has never heard of cannot be promoted.
    let unknown = a.promote(b_id + 1000).await.unwrap_err();
    assert!(
        unknown.to_string().contains(&format!("Learner {} not found", b_id + 1000)),
        "unexpected error: {}",
        unknown
    );

    a.promote(b_id).await?;

    let voters = BTreeSet::from([0, b_id]);
    for node in [&a, &b] {
        wait_for_voters(node, voters.clone(), "the promoted learner is a voter everywhere").await?;
    }

    // Promoting again is a no-op rather than an error.
    a.promote(b_id).await?;

    Ok(())
}

/// Demoting a voter must take it out of the quorum without taking it out of
/// the cluster: it keeps receiving the log, and the voters left still commit.
#[tokio::test(flavor = "multi_thread")]
async fn demoted_voter_becomes_a_learner_and_keeps_replicating() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (_, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;

    a.demote(c.node_id()).await?;

    let voters = BTreeSet::from([0, b.node_id()]);
    wait_for_voters(&a, voters.clone(), "the demoted node left the voter set").await?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(voters, membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(
        BTreeSet::from([c.node_id()]),
        membership.learner_ids().collect::<BTreeSet<_>>()
    );

    // This write proves both halves: the two remaining voters are a quorum on
    // their own, and the demoted node is still sent what they commit.
    assert_eq!(Response { value: None }, a.write(set("k1", "v1")).await?);
    wait_for_applied(&c, &a).await?;
    assert_eq!(expected_map(1..2), c.read(|app| app.data.clone()).await?);

    Ok(())
}

/// Demoting the last voter must be refused: a cluster with no voter can never
/// commit again, not even the change that would give it one back.
#[tokio::test(flavor = "multi_thread")]
async fn demote_refuses_to_empty_the_voter_set() -> io::Result<()> {
    let (_, a) = founding_node().await?;

    let err = a.demote(0).await.unwrap_err();
    assert!(
        err.to_string().contains("new membership cannot be empty"),
        "unexpected error: {}",
        err
    );

    // The refusal must have changed nothing: still one voter, still committing.
    let metrics = a.metrics().await;
    assert_eq!(
        BTreeSet::from([0]),
        metrics.membership_config.membership().voter_ids().collect::<BTreeSet<_>>()
    );
    assert_eq!(Response { value: None }, a.write(set("k1", "v1")).await?);

    Ok(())
}

/// Removing must work on a learner and on a voter alike, and leave nothing of
/// either behind in the membership.
#[tokio::test(flavor = "multi_thread")]
async fn removed_nodes_leave_the_membership() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (_, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_learner(&addr_a).await?;

    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;

    // A learner has no quorum to leave, only a node entry to drop; a voter has
    // both, and one change has to do them together.
    a.remove_node(c.node_id()).await?;
    a.remove_node(b.node_id()).await?;

    wait_for_voters(&a, BTreeSet::from([0]), "the removed voter left the voter set").await?;

    let metrics = a.metrics().await;
    let membership = metrics.membership_config.membership();
    assert_eq!(vec![0], membership.nodes().map(|(id, _)| *id).collect::<Vec<_>>());
    assert!(membership.learner_ids().next().is_none());

    assert_eq!(Response { value: None }, a.write(set("k1", "v1")).await?);

    Ok(())
}
