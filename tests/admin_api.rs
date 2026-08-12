//! The admin HTTP API and `AdminClient`: the operator's way in, redirects included
//!
//! Every test drives EzRaft through its public API and real HTTP between nodes.

mod common;

use std::collections::BTreeSet;
use std::io;

use common::*;
use ezraft::admin::AdminClient;

/// The admin API must drive the whole lifecycle an operator needs: one
/// endpoint promotes and demotes, another removes.
#[tokio::test(flavor = "multi_thread")]
async fn admin_api_changes_roles_and_removes_nodes() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (_, b) = joined_learner(&addr_a).await?;
    let b_id = b.node_id();

    admin_post(
        &addr_a,
        serde_json::json!({"op": "SetRole", "node_id": b_id, "role": "Voter"}),
    )
    .await?;
    wait_for_voters(&a, BTreeSet::from([0, b_id]), "promoted over HTTP").await?;

    admin_post(
        &addr_a,
        serde_json::json!({"op": "SetRole", "node_id": b_id, "role": "Learner"}),
    )
    .await?;
    wait_for_voters(&a, BTreeSet::from([0]), "demoted over HTTP").await?;

    let metrics = a.metrics().await;
    assert_eq!(
        BTreeSet::from([b_id]),
        metrics.membership_config.membership().learner_ids().collect::<BTreeSet<_>>()
    );

    admin_post(&addr_a, serde_json::json!({"op": "Remove", "node_id": b_id})).await?;
    a.wait_metrics(
        WAIT,
        |m| m.membership_config.membership().get_node(&b_id).is_none(),
        "removed over HTTP",
    )
    .await?;

    Ok(())
}

/// Metrics are the one admin endpoint no join drives, and the whole
/// `RaftMetrics` crosses the wire as JSON, so a field that does not survive
/// the round trip shows up here and nowhere else.
///
/// Compared against the same node's in-process metrics, whole: what HTTP
/// answers is what the node has. A follower is asked on purpose - metrics are
/// never followed to the leader, and its own view is the point of asking it.
#[tokio::test(flavor = "multi_thread")]
async fn admin_client_reads_the_metrics_of_the_node_it_asks() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (addr_b, b) = joined_voter(&addr_a).await?;
    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;
    assert!(!b.is_leader(), "b has to be a follower for this to be its own view");

    // Applied, not merely committed: the two reads below are one after the
    // other, and a follower still applying the membership entry moves its
    // `last_applied` between them.
    wait_for_applied(&b, &a).await?;

    assert_eq!(b.metrics().await, AdminClient::new(&addr_b).metrics::<KvSm>().await?);

    // The same client hands out ids, which is how a joining node gets one.
    let fresh = AdminClient::new(&addr_a).node_id().await?;
    assert!(fresh > b.node_id(), "an id past every one handed out so far");

    Ok(())
}

/// A membership change asked of a follower must come back with the leader's
/// address rather than being done or quietly dropped. `EzRaft` does not
/// forward - it is a node, not a client - so this redirect is the whole
/// mechanism, and it is what the join path's request loop follows.
#[tokio::test(flavor = "multi_thread")]
async fn admin_api_redirects_a_follower_to_the_leader() -> io::Result<()> {
    let (addr_a, a) = founding_node().await?;
    let (addr_b, b) = joined_voter(&addr_a).await?;
    let (_, c) = joined_learner(&addr_a).await?;
    let c_id = c.node_id();

    wait_for_voters(&a, BTreeSet::from([0, b.node_id()]), "the joining voter is promoted").await?;
    assert!(!b.is_leader(), "b has to be a follower for this to be a redirect");

    // Asked of the follower: answered with where to ask instead, and nothing done.
    let promote = serde_json::json!({"op": "SetRole", "node_id": c_id, "role": "Voter"});
    assert_eq!(Err(Some(addr_a.clone())), admin_answer(&addr_b, promote.clone()).await?);
    assert!(
        !a.metrics().await.membership_config.membership().voter_ids().any(|id| id == c_id),
        "the redirected request must not have promoted anything"
    );

    // The same request to the address it named does it.
    admin_post(&addr_a, promote).await?;
    wait_for_voters(
        &a,
        BTreeSet::from([0, b.node_id(), c_id]),
        "the leader did what the follower would not",
    )
    .await?;

    Ok(())
}
