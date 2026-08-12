//! Which node leads, and what moves leadership: demotion, an explicit transfer,
//! and removal
//!
//! Every test drives EzRaft through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;

use common::*;

/// A leader may demote itself, and openraft leaves it leading: a leader still
/// in the membership, voter or learner, keeps leading. Pinned here because it
/// surprises - the node making every decision is no longer counted in any of
/// them - and because the cluster must still be correct that way.
#[tokio::test(flavor = "multi_thread")]
async fn demoted_leader_keeps_leading_outside_the_quorum() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (addr_b, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    a.demote(0).await?;

    let voters = BTreeSet::from([b.node_id(), c.node_id()]);
    wait_for_voters(&a, voters.clone(), "the demoted leader left the voter set").await?;

    let metrics = a.metrics().await;
    let membership = metrics.committed_membership_config.membership();
    assert_eq!(voters, membership.voter_ids().collect::<BTreeSet<_>>());
    assert_eq!(BTreeSet::from([0]), membership.learner_ids().collect::<BTreeSet<_>>());

    // Still leading, and still committing - on a quorum of the two voters,
    // which it is not part of.
    assert!(a.is_leader());
    assert_eq!(Response { value: None }, http_write(&addr_a, set("k1", "v1")).await?);

    // And when it does go, the voters elect one of their own.
    a.shutdown().await?;
    b.wait_metrics(
        WAIT,
        |m| matches!(m.current_leader, Some(id) if id != 0),
        "a voter takes over from the demoted leader",
    )
    .await?;
    assert_eq!(Response { value: None }, http_write(&addr_b, set("k2", "v2")).await?);

    Ok(())
}

/// Leadership must be transferable to a named node. openraft's default for
/// the transfer RPC reports "not implemented", so without ezraft implementing
/// it the transfer is dropped and the leader simply keeps leading - which is
/// what makes the assertion here specific: leadership moves, and to the node
/// that was asked for.
#[tokio::test(flavor = "multi_thread")]
async fn leadership_transfers_to_the_named_node() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (_, b) = joined_voter(&addr_a).await?;
    let (addr_c, c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    let target = c.node_id();
    a.transfer_leader(target).await?;

    c.wait_metrics(
        WAIT,
        |m| m.current_leader == Some(target),
        "the named node takes leadership",
    )
    .await?;

    assert_eq!(Response { value: None }, http_write(&addr_c, set("k1", "v1")).await?);

    Ok(())
}

/// Removing the leader must hand the cluster on: openraft steps a leader down
/// once it is out of the membership entirely, and the voters left carry on.
#[tokio::test(flavor = "multi_thread")]
async fn removed_leader_hands_over_leadership() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (addr_b, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_voter(&addr_a).await?;

    let all = BTreeSet::from([0, b.node_id(), c.node_id()]);
    wait_for_voters(&a, all, "every joined node is a voter").await?;
    assert!(a.is_leader());

    a.remove_node(0).await?;

    b.wait_metrics(
        WAIT,
        |m| matches!(m.current_leader, Some(id) if id != 0),
        "leadership moves off the removed node",
    )
    .await?;

    let voters = BTreeSet::from([b.node_id(), c.node_id()]);
    wait_for_voters(&b, voters.clone(), "the removed leader left the cluster").await?;

    let metrics = b.metrics().await;
    let membership = metrics.committed_membership_config.membership();
    assert_eq!(
        voters.iter().copied().collect::<Vec<_>>(),
        membership.nodes().map(|(id, _)| *id).collect::<Vec<_>>()
    );

    assert_eq!(Response { value: None }, http_write(&addr_b, set("k1", "v1")).await?);

    Ok(())
}
