//! Driving the admin endpoints from a node that wants something from the cluster

use std::io;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::sleep;

use crate::admin::add_node::AddNodeRequest;
use crate::admin::change_node_role::ChangeNodeRoleRequest;
use crate::admin::redirect::Redirect;
use crate::node_role::NodeRole;

/// How many times an admin request is attempted before the node gives up
const ADMIN_ATTEMPTS: usize = 20;

/// How long to wait before attempting an admin request again
const ADMIN_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// How long a single admin request may take before the target is given up on
const ADMIN_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a membership change may take before the leader is given up on
///
/// Far longer than the rest, because the leader answers only once the change is committed, and
/// bringing a new voter up to date is part of that - for a large state, a whole snapshot transfer.
const MEMBERSHIP_CHANGE_TIMEOUT: Duration = Duration::from_secs(90);

/// Take a fresh node id from the cluster
pub(crate) async fn request_node_id(seed_addr: &str) -> Result<u64, io::Error> {
    admin_request(seed_addr, "node_id", &(), ADMIN_TIMEOUT).await
}

/// Ask the cluster to add this node to its membership, as a learner
pub(crate) async fn request_add_node(seed_addr: &str, node_id: u64, my_addr: &str) -> Result<(), io::Error> {
    let req = AddNodeRequest {
        node_id,
        addr: my_addr.to_string(),
    };
    admin_request(seed_addr, "add_node", &req, ADMIN_TIMEOUT).await
}

/// Ask the cluster to make a node a voter or a learner
///
/// A promotion is answered only once the node has caught up, so this call is as long as that
/// catch-up and must be awaited somewhere that is not holding up the node's own HTTP server -
/// [`EzRaft::serve`](crate::EzRaft::serve) is that place.
pub(crate) async fn request_change_node_role(seed_addr: String, node_id: u64, role: NodeRole) -> Result<(), io::Error> {
    let req = ChangeNodeRoleRequest { node_id, role };
    admin_request(&seed_addr, "change_node_role", &req, MEMBERSHIP_CHANGE_TIMEOUT).await
}

/// Drive one admin endpoint to an answer
///
/// Follows the redirect when the target is not the leader, and retries the transient conditions a
/// starting cluster is full of: a seed still binding its socket, no leader elected yet, or another
/// node's membership change still in flight. A cluster admits one change at a time, so nodes
/// started together take turns here instead of failing.
async fn admin_request<Req, Resp>(
    seed_addr: &str,
    endpoint: &str,
    req: &Req,
    timeout: Duration,
) -> Result<Resp, io::Error>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let client = client(timeout)?;

    let mut target_addr = seed_addr.to_string();
    let mut last_err = format!("cluster did not answer {}", endpoint);

    for _ in 0..ADMIN_ATTEMPTS {
        // Every way an attempt can fail is as transient as the rest here: the seed may still be
        // binding its HTTP socket, since serving starts concurrently with cluster formation.
        let redirect = match admin_attempt(&client, &target_addr, endpoint, req).await {
            Ok(redirect) => redirect,
            Err(e) => {
                last_err = e.to_string();
                sleep(ADMIN_RETRY_INTERVAL).await;
                continue;
            }
        };

        match redirect {
            Ok(resp) => return Ok(resp),
            Err(Some(leader)) => {
                last_err = format!("{} redirected to {}", target_addr, leader);
                target_addr = leader;
            }
            Err(None) => {
                last_err = format!("{} knows of no leader", target_addr);
                sleep(ADMIN_RETRY_INTERVAL).await;
            }
        }
    }

    Err(io::Error::other(format!(
        "{} gave up after {} attempts: {}",
        endpoint, ADMIN_ATTEMPTS, last_err
    )))
}

/// One POST to one admin endpoint, and what it answered
///
/// Errors on anything that is not an admin answer - an unreachable node, a non-success status, a
/// body that will not parse - and leaves it to the caller whether that is worth another attempt.
async fn admin_attempt<Req, Resp>(
    client: &reqwest::Client,
    addr: &str,
    endpoint: &str,
    req: &Req,
) -> Result<Redirect<Resp>, io::Error>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let url = format!("http://{}/api/{}", addr, endpoint);

    let resp = client
        .post(&url)
        .json(req)
        .send()
        .await
        .map_err(|e| io::Error::other(format!("request to {} failed: {}", url, e)))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(io::Error::other(format!("{} responded {}: {}", url, status, body)));
    }

    resp.json().await.map_err(|e| io::Error::other(format!("failed to parse {} response: {}", url, e)))
}

/// An HTTP client for one admin exchange
///
/// `no_proxy` because these addresses are cluster-internal: a proxy configured for outbound
/// traffic would swallow a request meant for a peer on the same network.
fn client(timeout: Duration) -> Result<reqwest::Client, io::Error> {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(timeout)
        .build()
        .map_err(|e| io::Error::other(e.to_string()))
}
