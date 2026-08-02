//! Main EzRaft API
//!
//! This module provides the primary [`EzRaft`] struct that users interact with.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::Raft;
use openraft::ReadPolicy;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::ClientWriteError;
use openraft::errors::InitializeError;
use openraft::errors::RaftError;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::admin::client::request_add_node;
use crate::admin::client::request_change_node_role;
use crate::admin::client::request_node_id;
use crate::app::EzApp;
use crate::config::EzConfig;
use crate::network::EzNetworkFactory;
use crate::node_role::NodeRole;
use crate::storage::EzStorage;
use crate::storage::adapter::StorageAdapter;
use crate::type_config::OpenRaftTypes;

/// Type alias for OpenRaft types (more readable than `OpenRaftTypes<T>`)
type ORTypes<T> = OpenRaftTypes<T>;

/// The internal OpenRaft instance, with EzRaft's storage adapter as its state machine
pub type ORRaft<T> = Raft<ORTypes<T>, Arc<StorageAdapter<T>>>;

/// A promotion in flight, handed from [`EzRaft::join`] to [`EzRaft::serve`]
type Promotion = Arc<Mutex<Option<JoinHandle<Result<(), io::Error>>>>>;

/// EzRaft - A simplified Raft interface
///
/// This struct wraps OpenRaft's `Raft` and provides a simplified API.
/// Users create an instance with their app and storage, then call
/// methods to initialize the cluster, write data, and serve HTTP requests.
///
/// # Type Parameters
///
/// - `T`: The application (implements `EzApp`)
pub struct EzRaft<T>
where T: EzApp
{
    /// Node ID
    node_id: u64,

    /// HTTP bind address
    addr: String,

    /// Storage adapter (bridges user storage/state machine to OpenRaft)
    storage: Arc<StorageAdapter<T>>,

    /// Internal OpenRaft instance
    raft: ORRaft<T>,

    /// The promotion [`Self::join`] started, for [`Self::serve`] to finish
    ///
    /// Shared and taken rather than owned, because [`EzRaft`] is cloned freely and only the one
    /// call to `serve` may await it. `None` once taken, and for a node that never asked to be
    /// promoted.
    promotion: Promotion,
}

impl<T> Clone for EzRaft<T>
where T: EzApp
{
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            addr: self.addr.clone(),
            storage: self.storage.clone(),
            raft: self.raft.clone(),
            promotion: self.promotion.clone(),
        }
    }
}

impl<T> EzRaft<T>
where T: EzApp
{
    /// Start a new cluster with this node as its only member
    ///
    /// Exactly one node of a cluster is created this way; every other node uses [`Self::join`].
    /// Creating two nodes separately gives two one-node clusters that will never merge.
    ///
    /// # Arguments
    ///
    /// * `http_addr` - Address to bind HTTP server (e.g., "127.0.0.1:8080")
    /// * `app` - User's application (state machine)
    /// * `storage` - User's storage implementation
    /// * `config` - EzRaft configuration (use `EzConfig::default()` for sensible defaults)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let raft = EzRaft::create("127.0.0.1:8080", app, storage, config).await?;
    /// ```
    pub async fn create(
        http_addr: impl ToString,
        app: T,
        storage: impl EzStorage<T>,
        config: EzConfig,
    ) -> Result<Self, io::Error> {
        Self::new(http_addr, app, storage, config, None).await
    }

    /// Join the cluster that `seed_addr` belongs to, and stay a learner
    ///
    /// Same as [`Self::join`] without the promotion: this node replicates the log and answers
    /// reads, but never votes, so it is a read replica and costs the cluster no write latency.
    /// [`Self::promote`], called on the leader, makes it a voter later.
    pub async fn join_as_learner(
        http_addr: impl ToString,
        seed_addr: impl ToString,
        app: T,
        storage: impl EzStorage<T>,
        config: EzConfig,
    ) -> Result<Self, io::Error> {
        Self::new(
            http_addr,
            app,
            storage,
            config,
            Some((seed_addr.to_string(), NodeRole::Learner)),
        )
        .await
    }

    /// Join the cluster that `seed_addr` belongs to, and become a voter
    ///
    /// Three steps, of which this call finishes the first two: the seed hands out a node id, adds
    /// this node to the cluster as a *learner*, and then the promotion to *voter* is started in
    /// the background and left for [`Self::serve`] to finish. It has to be left, because a node
    /// is promoted only once it has caught up, and it cannot receive anything until its own HTTP
    /// server is running - which is what `serve` starts. So **a joining node must call `serve`**,
    /// and that is where a failed promotion surfaces.
    ///
    /// The seed does not have to be the leader.
    ///
    /// On restart the persisted id is reused, the seed is not contacted, and no promotion is
    /// started: a node that has already joined is whatever the cluster's membership says it is,
    /// so restarting a node that was deliberately left a learner does not quietly make it a
    /// voter. Passing an address that has since left the cluster is therefore harmless too.
    ///
    /// # Arguments
    ///
    /// * `http_addr` - Address to bind HTTP server (e.g., "127.0.0.1:8081")
    /// * `seed_addr` - Address of any node already in the cluster
    /// * `app` - User's application (state machine)
    /// * `storage` - User's storage implementation
    /// * `config` - EzRaft configuration (use `EzConfig::default()` for sensible defaults)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let raft = EzRaft::join("127.0.0.1:8081", "127.0.0.1:8080", app, storage, config).await?;
    /// ```
    pub async fn join(
        http_addr: impl ToString,
        seed_addr: impl ToString,
        app: T,
        storage: impl EzStorage<T>,
        config: EzConfig,
    ) -> Result<Self, io::Error> {
        Self::new(
            http_addr,
            app,
            storage,
            config,
            Some((seed_addr.to_string(), NodeRole::Voter)),
        )
        .await
    }

    async fn new(
        http_addr: impl ToString,
        app: T,
        storage: impl EzStorage<T>,
        config: EzConfig,
        seed: Option<(String, NodeRole)>,
    ) -> Result<Self, io::Error> {
        let http_addr = http_addr.to_string();

        // Create storage adapter that bridges user traits to OpenRaft
        let adapter = StorageAdapter::new(storage, app).await?;
        let adapter = Arc::new(adapter);

        // Determine node_id, and whether a promotion has to follow. A node that already has an id
        // has joined before, so neither the seed nor the promotion applies to it again: what it
        // is now, the membership decides.
        let mut promote_via = None;

        let node_id = if let Some(id) = adapter.node_id().await {
            // Use persisted node_id (restart case)
            id
        } else if let Some((seed_addr, role)) = &seed {
            // Join existing cluster via seed node: take an id, then enter the membership as a
            // learner. Always a learner, whichever role was asked for - a node cannot be added
            // straight to the voter set, because the new configuration's quorum would count a
            // node that is not answering yet, and the change would wait forever on its own ack.
            let id = request_node_id(seed_addr).await?;
            request_add_node(seed_addr, id, &http_addr).await?;
            adapter.save_meta(|m| m.node_id = Some(id)).await?;

            if *role == NodeRole::Voter {
                promote_via = Some(seed_addr.clone());
            }
            id
        } else {
            // First node in cluster
            let id = 0;
            adapter.save_meta(|m| m.node_id = Some(id)).await?;
            id
        };

        let (log_store, sm_store) = (adapter.clone(), adapter.clone());

        // Convert EzConfig to OpenRaft Config
        let raft_config = config.to_raft_config()?;
        let raft_config = Arc::new(raft_config);

        // Create network factory
        let network = EzNetworkFactory::new()?;

        // Create OpenRaft instance
        let raft = Raft::new(node_id, raft_config, network, log_store, sm_store)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;

        // The created node starts the cluster with itself as its only member. On restart it loads
        // id 0 from storage and comes back here, where initializing is rightly refused; every
        // other refusal means the node cannot run.
        if node_id == 0 {
            let nodes = BTreeMap::from_iter([(node_id, BasicNode::new(http_addr.clone()))]);
            match raft.initialize(nodes).await {
                Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {}
                Err(e) => return Err(io::Error::other(e.to_string())),
            }
        }

        // Asking now, answered later. The leader holds the request until this node has caught up,
        // which cannot happen until `serve` is running, so the wait belongs there and not here.
        let promotion =
            promote_via.map(|seed_addr| tokio::spawn(request_change_node_role(seed_addr, node_id, NodeRole::Voter)));

        Ok(Self {
            node_id,
            addr: http_addr,
            storage: adapter,
            raft,
            promotion: Arc::new(Mutex::new(promotion)),
        })
    }

    /// Write a request to the Raft log
    ///
    /// This proposes a client request to the Raft cluster.
    /// The request will be replicated and applied to the state machine once committed.
    ///
    /// Only a leader can accept a write. Calling this on a follower forwards the request to the
    /// leader over HTTP and returns the leader's answer, so a caller never has to track which
    /// node is currently in charge. Moments without a usable leader - an election in flight, a
    /// just-elected leader that has not confirmed its lease - are waited out for up to ten
    /// seconds before the write fails.
    ///
    /// # Arguments
    ///
    /// * `req` - User's request type
    ///
    /// # Returns
    ///
    /// The response from the state machine's `apply()` method
    ///
    /// # Example
    ///
    /// ```ignore
    /// let req = Request::Set { key: "foo".into(), value: "bar".into() };
    /// let resp = raft.write(req).await?;
    /// ```
    pub async fn write(&self, req: T::Request) -> Result<T::Response, io::Error> {
        let mut last_err = String::new();

        for _ in 0..WRITE_ATTEMPTS {
            let err = match self.raft.client_write(req.clone()).await {
                // A user write is always answered with `Some` by `apply`; `None` exists only
                // for framework-generated entries.
                Ok(resp) => return resp.data.ok_or_else(|| io::Error::other("write produced no response")),
                Err(e) => e,
            };

            let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &err else {
                return Err(io::Error::other(err.to_string()));
            };

            // Forwarding to ourselves would repeat this call over HTTP forever.
            match forward.leader_node.as_ref().map(|n| n.addr.as_str()) {
                Some(leader) if leader != self.addr => return forward_write::<T>(leader, &req).await,
                // No usable leader: an election is in flight, or a just-elected leader has not
                // confirmed its quorum lease yet. Both resolve within heartbeats, so wait them
                // out instead of bothering the caller.
                _ => {
                    last_err = err.to_string();
                    sleep(WRITE_RETRY_INTERVAL).await;
                }
            }
        }

        Err(io::Error::other(format!(
            "write gave up after {} attempts: {}",
            WRITE_ATTEMPTS, last_err
        )))
    }

    /// Read the applied state directly, without going through the log
    ///
    /// Runs the closure over this node's applied application state and returns its result. The
    /// read is local and cheap - no consensus round, no log entry - and therefore not
    /// linearizable on its own: this node, leader included, may not have applied the latest
    /// acknowledged write yet. Call [`Self::linearizable`] first when read-your-writes matters.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let value = raft.read(|app| app.data.get("foo").cloned()).await;
    /// ```
    pub async fn read<F, R>(&self, read: F) -> R
    where F: FnOnce(&T) -> R {
        let sm = self.storage.sm_state.lock().await;
        read(&sm.app)
    }

    /// Wait until a local read would be linearizable
    ///
    /// Confirms this node is still the leader with a quorum round-trip, then waits until the
    /// local state machine has applied everything committed up to that point. A [`Self::read`]
    /// issued after this returns sees every write acknowledged before this call.
    ///
    /// Only the leader can serve linearizable reads, so on a follower this returns an error
    /// instead of forwarding: forwarding cannot make this node's local state current.
    pub async fn linearizable(&self) -> Result<(), io::Error> {
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    /// Add a learner node to the cluster
    ///
    /// A learner receives log replication but does not vote. Every node is admitted this way,
    /// and stays a learner until [`Self::promote`] makes it a voter: a node cannot be added
    /// straight to the voter set, because the new configuration's quorum would count a node that
    /// has not received the response telling it its own id, and so cannot answer anything yet.
    ///
    /// Returns as soon as replication to the new node is set up; the node catches up in the
    /// background. Waiting here would deadlock the join handler, whose caller cannot answer any
    /// Raft RPC until it gets its node id back.
    ///
    /// # Arguments
    ///
    /// * `node_id` - ID of the new learner node
    /// * `addr` - Address of the new learner node
    pub(crate) async fn add_learner(&self, node_id: u64, addr: String) -> Result<(), io::Error> {
        let node = BasicNode::new(addr);
        self.raft.add_learner(node_id, node, false).await.map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }

    /// Make a learner a voter
    ///
    /// Only voters count towards a quorum, so a cluster tolerates a node failure only once the
    /// nodes that joined it have been promoted: three voters tolerate one failure, five tolerate
    /// two. Joining replicates the log; it does not change who decides. Until this is called the
    /// new node is a read replica, which is also what to leave it as if that is all you want.
    ///
    /// Returns once the node is a voter. Bringing it up to date is part of the change, so this
    /// lasts as long as that catch-up takes - for a large state, a whole snapshot transfer.
    ///
    /// Fails if the node is not a learner of this cluster, and if another membership change is
    /// still in flight, since a cluster admits one at a time. Both are the caller's to retry -
    /// on the joining path that is [`Self::join`]'s promotion request, which retries like every
    /// other request it makes. Only a leader can change membership and this does not forward, so
    /// call it on the leader.
    pub async fn promote(&self, node_id: u64) -> Result<(), io::Error> {
        self.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([node_id]))).await
    }

    /// Make a voter a learner
    ///
    /// The node stays in the cluster and keeps receiving the log; it stops being counted in the
    /// quorum a write must reach. Use it to take a node out of the decision-making without taking
    /// it out of the cluster - before shutting it down, or to leave it as a read replica.
    ///
    /// Demoting the last voter is refused: a cluster with no voter can never commit again, not
    /// even the change that would give it one back. Demoting below a quorum is *not* refused,
    /// because it is a legitimate thing to intend, and Raft's answer to it is honest - the
    /// cluster stops committing until enough voters return.
    ///
    /// Demoting the current leader is allowed. It commits the change and then steps down, and the
    /// remaining voters elect one of their own.
    ///
    /// Only a leader can change membership and this does not forward, so call it on the leader.
    pub async fn demote(&self, node_id: u64) -> Result<(), io::Error> {
        // `retain` is what makes this a demotion rather than a removal: the node stays in the
        // membership, as a learner.
        self.raft
            .change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([node_id])), true)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }

    /// Take a node out of the cluster
    ///
    /// The cluster stops replicating to it and forgets it. Works on a voter and on a learner
    /// alike: a voter is removed from the quorum and the membership in one change, so there is no
    /// window in which a node the caller asked to remove is still being waited for.
    ///
    /// This does not stop the node's own process - that is the operator's to do. Doing it first is
    /// what leaves the cluster nothing to talk to; a node removed while still running keeps
    /// serving stale reads to anyone who asks it.
    ///
    /// Only a leader can change membership and this does not forward, so call it on the leader.
    pub async fn remove_node(&self, node_id: u64) -> Result<(), io::Error> {
        let metrics = self.metrics().await;
        let node = BTreeSet::from([node_id]);

        // A voter and a learner are removed differently, because a voter is in two places at
        // once. Taking it out of the voter set with `retain` off drops its node entry too, but
        // only once the joint config that still counts it has resolved - dropping that entry any
        // sooner would leave the joint config holding a voter with no address to reach it at. A
        // learner is in no config, so its entry is all there is to drop.
        if metrics.membership_config.membership().voter_ids().any(|id| id == node_id) {
            self.raft
                .change_membership(ChangeMembers::RemoveVoters(node), false)
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;

            return Ok(());
        }

        self.change_membership(ChangeMembers::RemoveNodes(node)).await
    }

    /// Change the cluster membership
    ///
    /// This modifies the cluster membership using OpenRaft's `ChangeMembers`.
    pub async fn change_membership(&self, change: ChangeMembers<u64, BasicNode>) -> Result<(), io::Error> {
        self.raft.change_membership(change, false).await.map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    /// Check if this node is the leader
    ///
    /// Reports this node's own state, which is what a caller asking "am I the leader" wants. It
    /// is not a guarantee that the answer is still true elsewhere: a deposed leader does not
    /// find out until it hears from the new one. Nothing here needs that guarantee --
    /// [`Self::write`] finds the leader on its own -- and code that does want a linearizable
    /// read calls [`Self::linearizable`] first.
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    /// Get the current cluster metrics
    ///
    /// Returns information about the Raft cluster state.
    pub async fn metrics(&self) -> openraft::RaftMetrics<ORTypes<T>> {
        self.raft.metrics().borrow_watched().clone()
    }

    /// Start the HTTP server
    ///
    /// This starts the HTTP server that handles:
    /// - Internal Raft RPC (append entries, vote, install snapshot)
    /// - Admin API (join, add learner, change membership, metrics)
    ///
    /// This method blocks until the server is stopped, so a caller with other work to do spawns
    /// it: `tokio::spawn(raft.clone().serve())`. Start it as early as possible. Peers reach a
    /// node only through this server, so a node that has joined a cluster but is not serving yet
    /// cannot be replicated to, and holds up every quorum it is counted in.
    ///
    /// This is also where the promotion started by [`Self::join`] is collected. The server goes
    /// up first, because the promotion completes only once this node has caught up and it can
    /// only catch up through that server; a promotion that fails is returned here, and stops the
    /// server with it. A node that joined as a learner, or was created, has nothing to collect
    /// and simply serves.
    pub async fn serve(self) -> Result<(), io::Error> {
        let promotion = self.promotion.lock().unwrap().take();
        let server = tokio::spawn(crate::server::run(self));

        if let Some(promotion) = promotion {
            let promoted = promotion.await.map_err(io::Error::other)?;

            if let Err(e) = promoted {
                // Nothing else will stop it: the caller is holding this call, not the server.
                server.abort();
                return Err(e);
            }
        }

        server.await.map_err(io::Error::other)?
    }

    /// Get the node ID
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get the HTTP address
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Get a reference to the internal OpenRaft instance
    ///
    /// This provides access to advanced OpenRaft APIs if needed.
    pub fn inner(&self) -> &ORRaft<T> {
        &self.raft
    }
}

/// How many times a write retries while the cluster has no usable leader
const WRITE_ATTEMPTS: usize = 20;

/// How long to wait before retrying a write
const WRITE_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// How long a forwarded write may take before the leader is given up on
///
/// Generous, because the leader has to replicate and commit the request before answering.
const FORWARD_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Send a write to the leader's `/api/write` endpoint and return what it applied
///
/// The leader is asked to do the write on this node's behalf, so the answer is the same one the
/// caller would have got from writing to the leader directly.
async fn forward_write<T>(leader_addr: &str, req: &T::Request) -> Result<T::Response, io::Error>
where T: EzApp {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(FORWARD_WRITE_TIMEOUT)
        .build()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let url = format!("http://{}/api/write", leader_addr);

    let resp = client
        .post(&url)
        .json(req)
        .send()
        .await
        .map_err(|e| io::Error::other(format!("forwarding write to {} failed: {}", url, e)))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(io::Error::other(format!("{} responded {}: {}", url, status, body)));
    }

    resp.json().await.map_err(|e| io::Error::other(format!("failed to parse write response: {}", e)))
}
