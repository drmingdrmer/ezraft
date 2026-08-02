//! Driving the admin endpoints from whoever wants something from the cluster

use std::io;
use std::time::Duration;

use openraft::RaftMetrics;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::sleep;

use crate::admin::membership_change::MembershipChange;
use crate::admin::redirect::Redirect;
use crate::app::EzApp;
use crate::type_config::OpenRaftTypes;

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

/// The admin API of a cluster, reached through one of its nodes
///
/// The address is where to ask, not who must answer: a request only the leader can decide is
/// followed to whichever node the answer names, so any member of the cluster will do. It is also
/// retried, because a cluster that is still starting up is full of conditions that pass on their
/// own - a node still binding its socket, an election in flight, another membership change
/// holding the one slot a cluster admits at a time.
///
/// [`Self::metrics`] is the exception: it asks one node and reports what that node said.
#[derive(Debug, Clone)]
pub struct AdminClient {
    /// The node to ask
    addr: String,
}

impl AdminClient {
    /// An admin client that asks the node serving `addr`
    pub fn new(addr: impl ToString) -> Self {
        Self { addr: addr.to_string() }
    }

    /// Take a fresh node id from the cluster
    pub async fn node_id(&self) -> Result<u64, io::Error> {
        self.request("node_id", &(), ADMIN_TIMEOUT).await
    }

    /// Ask the cluster to change who is in it
    pub async fn membership(&self, change: MembershipChange) -> Result<(), io::Error> {
        // A promotion is answered only once the node has caught up, so that call is as long as
        // the catch-up; the rest are answered as soon as the leader commits them.
        let timeout = match change {
            MembershipChange::SetRole { .. } => MEMBERSHIP_CHANGE_TIMEOUT,
            _ => ADMIN_TIMEOUT,
        };

        self.request("membership", &change, timeout).await
    }

    /// Read the asked node's view of the cluster: who leads, how far its log goes, who is in it
    ///
    /// Not followed to the leader and not retried, unlike the rest, because one node's view is
    /// the whole point of asking it - a follower that has fallen behind can only say so itself.
    pub async fn metrics<T>(&self) -> Result<RaftMetrics<OpenRaftTypes<T>>, io::Error>
    where T: EzApp {
        let url = format!("http://{}/api/metrics", self.addr);

        answer(&url, client(ADMIN_TIMEOUT)?.get(&url)).await
    }

    /// Drive one admin endpoint to an answer
    ///
    /// Follows the redirect when the target is not the leader, and retries the transient
    /// conditions a starting cluster is full of: a seed still binding its socket, no leader
    /// elected yet, or another node's membership change still in flight. A cluster admits one
    /// change at a time, so nodes started together take turns here instead of failing.
    async fn request<Req, Resp>(&self, endpoint: &str, req: &Req, timeout: Duration) -> Result<Resp, io::Error>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let client = client(timeout)?;

        let mut target_addr = self.addr.clone();
        let mut last_err = format!("cluster did not answer {}", endpoint);

        for _ in 0..ADMIN_ATTEMPTS {
            let url = format!("http://{}/api/{}", target_addr, endpoint);

            // Every way an attempt can fail is as transient as the rest here: the seed may still
            // be binding its HTTP socket, since serving starts concurrently with cluster
            // formation.
            let redirect: Redirect<Resp> = match answer(&url, client.post(&url).json(req)).await {
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
}

/// Send one request to one endpoint, and read what it answered
///
/// Errors on anything that is not an answer - an unreachable node, a non-success status, a body
/// that will not parse - and leaves it to the caller whether that is worth another attempt.
async fn answer<Resp>(url: &str, req: reqwest::RequestBuilder) -> Result<Resp, io::Error>
where Resp: DeserializeOwned {
    let resp = req.send().await.map_err(|e| io::Error::other(format!("request to {} failed: {}", url, e)))?;

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
