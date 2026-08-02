//! HTTP server for EzRaft
//!
//! This module provides the HTTP server that handles:
//! - Internal Raft RPC (append entries, vote)
//! - Admin API (joining, node roles, removal, metrics)

use std::io::Cursor;

use actix_web::App;
use actix_web::HttpServer;
use actix_web::web;
use actix_web::web::Data;
use openraft::Snapshot;
use openraft::errors::Infallible;
use openraft::errors::decompose::DecomposeResult;
use openraft::raft;
use openraft::raft::SnapshotResponse;
use serde::Deserialize;

use crate::admin::AddNodeRequest;
use crate::admin::ChangeNodeRoleRequest;
use crate::admin::Redirect;
use crate::admin::RemoveNodeRequest;
use crate::app::EzApp;
use crate::network::SnapshotTransfer;
use crate::node_role::NodeRole;
use crate::raft::EzRaft;
use crate::type_config::OpenRaftTypes;

/// Type alias for OpenRaft types
type C<T> = OpenRaftTypes<T>;

/// HTTP server wrapper for EzRaft
///
/// This is both a working server and a sample to copy. The `/raft/*` routes and the admin
/// handlers are what every deployment needs and can be taken as they are. The application
/// routes cannot be: `POST /api/write` and `GET /api/read?key=...` are the smallest pair that
/// exercises an app, not the API a real service exposes - that one has an endpoint per
/// operation, with the path, parameters and encoding each operation deserves.
///
/// So copy this file into your own crate and rewrite the application handlers there.
/// [`EzRaft::write`] and [`EzRaft::read`] are the only two entry points they need - the first
/// goes through the log, the second reads local state.
pub struct EzServer<T>
where T: EzApp
{
    raft: EzRaft<T>,
}

impl<T> EzServer<T>
where T: EzApp
{
    pub fn new(raft: EzRaft<T>) -> Self {
        Self { raft }
    }

    /// Run the HTTP server
    pub async fn run(self) -> std::io::Result<()> {
        let addr = self.raft.addr().to_string();
        let server_data = Data::new(self);

        let server = HttpServer::new(move || {
            App::new()
                .app_data(server_data.clone())
                // Raft internal RPC
                .route("/raft/append", web::post().to(Self::handle_append))
                .route("/raft/vote", web::post().to(Self::handle_vote))
                .route("/raft/snapshot", web::post().to(Self::handle_snapshot))
                .route("/raft/transfer_leader", web::post().to(Self::handle_transfer_leader))
                // Application API
                .route("/api/write", web::post().to(Self::handle_write))
                .route("/api/read", web::get().to(Self::handle_read))
                // Admin API
                .route("/api/node_id", web::post().to(Self::handle_node_id))
                .route("/api/add_node", web::post().to(Self::handle_add_node))
                .route("/api/change_node_role", web::post().to(Self::handle_change_node_role))
                .route("/api/remove_node", web::post().to(Self::handle_remove_node))
                .route("/api/metrics", web::get().to(Self::handle_metrics))
        })
        .bind(&addr)?;

        server.run().await
    }

    /// Raft append entries RPC handler
    ///
    /// The body is the `Result` the peer's [`crate::network::Network`] expects; only a
    /// [`Fatal`](openraft::errors::Fatal) error becomes an HTTP error status.
    async fn handle_append(
        req: web::Json<raft::AppendEntriesRequest<C<T>>>,
        ez: Data<Self>,
    ) -> Result<web::Json<Result<raft::AppendEntriesResponse<C<T>>, Infallible>>, actix_web::Error> {
        let resp = ez
            .raft
            .inner()
            .append_entries(req.into_inner())
            .await
            .decompose()
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("append_entries failed: {}", e)))?;

        Ok(web::Json(resp))
    }

    /// Raft vote RPC handler
    async fn handle_vote(
        req: web::Json<raft::VoteRequest<C<T>>>,
        ez: Data<Self>,
    ) -> Result<web::Json<Result<raft::VoteResponse<C<T>>, Infallible>>, actix_web::Error> {
        let resp = ez
            .raft
            .inner()
            .vote(req.into_inner())
            .await
            .decompose()
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("vote failed: {}", e)))?;

        Ok(web::Json(resp))
    }

    /// Raft transfer leadership RPC handler
    ///
    /// A leader hands leadership over rather than stopping and leaving the cluster to notice its
    /// silence: an election costs a timeout, a transfer costs a round trip. A leader demoted out
    /// of the voter set does this on its way out.
    async fn handle_transfer_leader(
        req: web::Json<raft::TransferLeaderRequest<C<T>>>,
        ez: Data<Self>,
    ) -> Result<web::Json<Result<raft::TransferLeaderResponse<C<T>>, Infallible>>, actix_web::Error> {
        let resp = ez
            .raft
            .inner()
            .handle_transfer_leader(req.into_inner())
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("transfer_leader failed: {}", e)))?;

        Ok(web::Json(Ok(resp)))
    }

    /// Raft install snapshot RPC handler
    ///
    /// A leader falls back to this when a follower lags behind the purged log. The snapshot
    /// arrives whole in a single request.
    async fn handle_snapshot(
        req: web::Json<SnapshotTransfer>,
        ez: Data<Self>,
    ) -> Result<web::Json<Result<SnapshotResponse<C<T>>, Infallible>>, actix_web::Error> {
        let SnapshotTransfer { vote, meta, data } = req.into_inner();
        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(data),
        };

        let resp = ez
            .raft
            .inner()
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("install_snapshot failed: {}", e)))?;

        Ok(web::Json(Ok(resp)))
    }

    /// Application write API handler
    ///
    /// Takes the application's own request type as JSON, runs it through Raft, and returns
    /// whatever the state machine's `apply` produced. This is how a client drives the cluster.
    async fn handle_write(
        req: web::Json<T::Request>,
        ez: Data<Self>,
    ) -> Result<web::Json<T::Response>, actix_web::Error> {
        let resp = ez
            .raft
            .write(req.into_inner())
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("write failed: {}", e)))?;

        Ok(web::Json(resp))
    }

    /// Application read API handler
    ///
    /// `GET /api/read?key=...` answers a keyed read from local memory via [`EzApp::read`]: the
    /// write API puts keys in, this reads one back. `key` is required - a request without it is
    /// a 400 - and a key the app does not hold is a 404.
    ///
    /// Reads cost no consensus round and no log entry, and are as fresh as this node's
    /// replication - a read that must be linearizable goes through [`EzRaft::write`] instead.
    async fn handle_read(
        query: web::Query<ReadQuery>,
        ez: Data<Self>,
    ) -> Result<web::Json<serde_json::Value>, actix_web::Error> {
        let key = &query.key;

        let Some(value) = ez.raft.read(|app| app.read(key)).await else {
            return Err(actix_web::error::ErrorNotFound(format!("no value for key {:?}", key)));
        };

        Ok(web::Json(value))
    }

    /// Metrics API handler
    async fn handle_metrics(ez: Data<Self>) -> Result<web::Json<openraft::RaftMetrics<C<T>>>, actix_web::Error> {
        let metrics = ez.raft.metrics().await;
        Ok(web::Json(metrics))
    }

    /// Node id API handler
    ///
    /// Hands out an id no other node has held, taken from the index of a blank log entry: the log
    /// is the one counter every node already agrees on. Ids are therefore unique but not
    /// consecutive.
    ///
    /// This does not add anything to the cluster - [`Self::handle_add_node`] does that - so a
    /// node that takes an id and dies costs the cluster one unused number.
    async fn handle_node_id(ez: Data<Self>) -> Result<web::Json<Redirect<u64>>, actix_web::Error> {
        let leader = match Self::leader_or_redirect(&ez).await {
            Ok(leader) => leader,
            Err(redirect) => return Ok(web::Json(Err(redirect))),
        };

        let write_result = leader
            .inner()
            .write_blank()
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("node id write failed: {}", e)))?;

        Ok(web::Json(Ok(write_result.log_id.index)))
    }

    /// Add node API handler
    ///
    /// Adds a node to the membership as a learner, which replicates the log without voting.
    /// Making it a voter is [`Self::handle_change_node_role`], and separate for a reason: a node
    /// added straight to the voter set would be counted in the new configuration's quorum before
    /// it could answer anything, and the change would wait forever on its own acknowledgement.
    async fn handle_add_node(
        req: web::Json<AddNodeRequest>,
        ez: Data<Self>,
    ) -> Result<web::Json<Redirect<()>>, actix_web::Error> {
        let leader = match Self::leader_or_redirect(&ez).await {
            Ok(leader) => leader,
            Err(redirect) => return Ok(web::Json(Err(redirect))),
        };

        leader
            .add_learner(req.node_id, req.addr.clone())
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("add_node failed: {}", e)))?;

        Ok(web::Json(Ok(())))
    }

    /// Change node role API handler
    ///
    /// One endpoint for both directions, because they are one decision: whether this node counts
    /// towards a quorum.
    ///
    /// A promotion holds the answer until it is done, which means until the node has caught up, so
    /// that request is as long as the catch-up - and the node being promoted must be serving, or
    /// it can never catch up and this times out. A demotion has nothing to wait for.
    async fn handle_change_node_role(
        req: web::Json<ChangeNodeRoleRequest>,
        ez: Data<Self>,
    ) -> Result<web::Json<Redirect<()>>, actix_web::Error> {
        let leader = match Self::leader_or_redirect(&ez).await {
            Ok(leader) => leader,
            Err(redirect) => return Ok(web::Json(Err(redirect))),
        };

        let changed = match req.role {
            NodeRole::Voter => leader.promote(req.node_id).await,
            NodeRole::Learner => leader.demote(req.node_id).await,
        };

        changed.map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("change_node_role to {:?} failed: {}", req.role, e))
        })?;

        Ok(web::Json(Ok(())))
    }

    /// Remove node API handler
    ///
    /// Takes a node out of the cluster, whether it votes or not.
    async fn handle_remove_node(
        req: web::Json<RemoveNodeRequest>,
        ez: Data<Self>,
    ) -> Result<web::Json<Redirect<()>>, actix_web::Error> {
        let leader = match Self::leader_or_redirect(&ez).await {
            Ok(leader) => leader,
            Err(redirect) => return Ok(web::Json(Err(redirect))),
        };

        leader
            .remove_node(req.node_id)
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("remove_node failed: {}", e)))?;

        Ok(web::Json(Ok(())))
    }

    /// This node's [`EzRaft`] if it leads, or where to ask instead
    ///
    /// Membership is the leader's to change, so every admin handler starts here. The redirect is
    /// `None` when this node knows of no leader, which is a moment to wait out rather than a
    /// failure. Both answers come from one reading of the metrics, so they cannot disagree.
    async fn leader_or_redirect(ez: &Data<Self>) -> Result<&EzRaft<T>, Option<String>> {
        let metrics = ez.raft.metrics().await;

        if metrics.current_leader == Some(metrics.id) {
            return Ok(&ez.raft);
        }

        Err(metrics
            .current_leader
            .and_then(|leader_id| metrics.membership_config.membership().get_node(&leader_id).map(|n| n.addr.clone())))
    }
}

/// Run the HTTP server (convenience function)
pub(crate) async fn run<T>(raft: EzRaft<T>) -> std::io::Result<()>
where T: EzApp {
    EzServer::new(raft).run().await
}

/// Query for [`EzServer::handle_read`]
#[derive(Debug, Deserialize)]
struct ReadQuery {
    /// Key to read, passed to [`EzApp::read`]
    key: String,
}
