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

/// How long a role change may take before the leader is given up on
///
/// Far longer than the rest, because the leader answers only once the change is committed, and
/// bringing a new voter up to date is part of that - for a large state, a whole snapshot transfer.
const ROLE_CHANGE_TIMEOUT: Duration = Duration::from_secs(90);

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
    admin_request(&seed_addr, "change_node_role", &req, ROLE_CHANGE_TIMEOUT).await
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
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(timeout)
        .build()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let mut target_addr = seed_addr.to_string();
    let mut last_err = format!("cluster did not answer {}", endpoint);

    for _ in 0..ADMIN_ATTEMPTS {
        let url = format!("http://{}/api/{}", target_addr, endpoint);

        // A send failure is as transient as the rest: the seed may still be binding its HTTP
        // socket, since serving starts concurrently with cluster formation.
        let resp = match client.post(&url).json(req).send().await {
            Ok(resp) => resp,
            Err(e) => {
                last_err = format!("request to {} failed: {}", url, e);
                sleep(ADMIN_RETRY_INTERVAL).await;
                continue;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            last_err = format!("{} responded {}: {}", url, status, body);
            sleep(ADMIN_RETRY_INTERVAL).await;
            continue;
        }

        let redirect: Redirect<Resp> = resp
            .json()
            .await
            .map_err(|e| io::Error::other(format!("failed to parse {} response: {}", url, e)))?;

        match redirect {
            Ok(resp) => return Ok(resp),
            Err(Some(leader)) => {
                last_err = format!("{} redirected to {}", url, leader);
                target_addr = leader;
            }
            Err(None) => {
                last_err = format!("{} knows of no leader", url);
                sleep(ADMIN_RETRY_INTERVAL).await;
            }
        }
    }

    Err(io::Error::other(format!(
        "{} gave up after {} attempts: {}",
        endpoint, ADMIN_ATTEMPTS, last_err
    )))
}
