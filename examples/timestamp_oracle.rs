//! Monotonic Unix timestamp oracle built on EzRaft.
//!
//! Run three voting nodes so the service survives one node stopping:
//!
//! ~~~bash
//! cargo run --example timestamp_oracle -- --addr 127.0.0.1:8080
//! cargo run --example timestamp_oracle -- --addr 127.0.0.1:8081 --seed 127.0.0.1:8080
//! cargo run --example timestamp_oracle -- --addr 127.0.0.1:8082 --seed 127.0.0.1:8080
//! ~~~
//!
//! Request a timestamp from the current leader:
//!
//! ~~~bash
//! curl -X POST 127.0.0.1:8080/time
//! # "2026-05-29T09:46:40.000000Z"
//! ~~~
//!
//! A timestamp is an RFC 3339 UTC string with microsecond precision, so the
//! strings compare in the same order as the microseconds behind them. A
//! leader-only background task keeps timestamp space reserved by committing
//! log entries. Request handlers serve from that space locally and never wait
//! for a Raft write.
//! Every request validates the local OpenRaft leader lease but performs no
//! network round trip. Set another reservation duration with
//! "--reservation-us".
//!
//! The committed state is a single high-water mark: each reservation advances
//! it to the leader's wall-clock "now + width", and the leader serves from the
//! interval between the old and the new mark. A new leader starts at the mark,
//! above every value an earlier leader could have served, so it never reuses
//! one; unused values become gaps. Lease validation also stops an old leader
//! from serving after its lease expires. Like every lease-based algorithm,
//! this assumes negligible clock drift between nodes.
//!
//! Reserving only up to "now + width" bounds how far timestamps run ahead of
//! wall-clock time, at a cost after a leader switch: until the new leader's
//! "now + width" passes the mark left by the previous one, it reserves nothing
//! and requests fail. With equal widths that pause is at most about half a
//! reservation, and an election usually absorbs it. This is the
//! availability/accuracy trade-off controlled by "--reservation-us".

use std::io;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use actix_web::web;
use chrono::DateTime;
use chrono::SecondsFormat;
use clap::Parser;
use ezraft::EzApp;
use ezraft::EzConfig;
use ezraft::EzRaft;
use ezraft::FileStorage;
use openraft::ReadPolicy;
use openraft::ServerState;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::error;
use tracing::warn;
use tracing_subscriber::EnvFilter;

/// Replicated command for reserving timestamp space.
#[derive(Serialize, Deserialize, Debug, Clone, derive_more::Display)]
#[display("Reserve({reserve_upto_us})")]
struct Reserve {
    /// Exclusive upper bound to reserve up to, in microseconds since the Unix
    /// epoch; the leader sends its wall-clock "now + width". A bound at or
    /// below the current mark reserves nothing, which makes retries harmless.
    reserve_upto_us: u64,
}

/// Half-open timestamp interval `[start, end)` committed by Raft.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct Interval {
    /// First timestamp available to the leader that reserved the interval.
    start: u64,
    /// High-water mark that all future leaders must start at or after.
    end: u64,
}

/// Replicated timestamp high-water mark.
#[derive(Default, Serialize, Deserialize)]
struct TimeState {
    /// Exclusive end of the latest committed interval, i.e. the first
    /// timestamp no leader has reserved yet.
    reserved_end: u64,
}

#[async_trait::async_trait]
impl EzApp for TimeState {
    type Request = Reserve;
    type Response = Interval;

    async fn apply(&mut self, req: Reserve) -> Self::Response {
        let start = self.reserved_end;
        let end = start.max(req.reserve_upto_us);
        self.reserved_end = end;
        Interval { start, end }
    }

    type ReadRequest = ();

    type ReadResponse = u64;

    fn read(&self, _req: ()) -> u64 {
        self.reserved_end
    }
}

/// Timestamps `[next, end)` the leader of `term` has reserved but not yet
/// handed out; empty when `next >= end`, and requests then fail until the
/// next reservation commits.
#[derive(Default)]
struct Reserved {
    term: u64,
    next: u64,
    end: u64,
}

impl Reserved {
    /// Removes and returns the next timestamp, or `None` if `leader_term` has none left.
    fn take(&mut self, leader_term: u64, now: u64) -> Option<u64> {
        let timestamp = now.max(self.next);
        if self.term != leader_term || timestamp >= self.end {
            return None;
        }
        self.next = timestamp + 1;
        Some(timestamp)
    }

    /// Whether `leader_term` holds every timestamp below `until`.
    fn covers(&self, leader_term: u64, until: u64) -> bool {
        self.term == leader_term && self.next < self.end && self.end >= until
    }

    /// Extends a contiguous reservation of the same term, or replaces it.
    fn install(&mut self, leader_term: u64, interval: Interval) {
        if self.term != leader_term || self.end != interval.start {
            self.next = interval.start;
        }
        self.term = leader_term;
        self.end = interval.end;
    }
}

/// Per-process state shared by all timestamp HTTP handlers.
struct TimeService {
    /// Local Raft node used for lease checks and interval reservations.
    raft: EzRaft<TimeState>,
    /// How far past wall-clock time each reservation extends the committed
    /// mark, in microseconds.
    reservation_width: NonZeroU64,
    /// Leader-local reservation; the lock serializes timestamp issuance and refill.
    reserved: Mutex<Reserved>,
}

impl TimeService {
    async fn next_timestamp(&self) -> io::Result<u64> {
        let mut reserved = self.reserved.lock().await;
        let term = self.leader_term().await?;
        reserved
            .take(term, unix_timestamp_micros())
            .ok_or_else(|| io::Error::other("no reserved timestamp is currently available"))
    }

    async fn run_reserver(&self) {
        let refresh_interval = Duration::from_micros(self.reservation_width.get() / 2);
        loop {
            // Returns at once while this node is already the leader, so a follower parks
            // here instead of waking every interval, and a fresh leader reserves as soon as
            // it is elected rather than up to one interval later.
            let wait = self.raft.inner().wait(None);
            if let Err(error) = wait.state(ServerState::Leader, "reserve timestamps").await {
                error!("timestamp reservation task stopped: {error}");
                return;
            }
            if let Err(error) = self.refill().await {
                warn!("failed to reserve timestamps: {error}");
            }
            tokio::time::sleep(refresh_interval).await;
        }
    }

    async fn refill(&self) -> io::Result<()> {
        let term = self.leader_term().await?;
        let now = unix_timestamp_micros();
        let width = self.reservation_width.get();
        let refill_until = now.saturating_add(width / 2);
        let covered = self.reserved.lock().await.covers(term, refill_until);
        if covered {
            return Ok(());
        }

        // The interval may commit under a newer term than `term`, leaving a stale tag. That
        // needs no re-check: this node's committed log then holds the newer term, so
        // `leader_term` never returns `term` again and every later `take` rejects the tag.
        let upto = now.saturating_add(width);
        let interval = self.raft.write(Reserve { reserve_upto_us: upto }).await?;
        self.reserved.lock().await.install(term, interval);
        Ok(())
    }

    async fn leader_term(&self) -> io::Result<u64> {
        let read_log_id = self
            .raft
            .inner()
            .ensure_linearizable(ReadPolicy::LeaseRead)
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(read_log_id.committed_leader_id().term)
    }
}

fn unix_timestamp_micros() -> u64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_micros();
    u64::try_from(micros).expect("microseconds since the Unix epoch overflow u64")
}

/// Renders microseconds since the Unix epoch as "2026-05-29T09:46:40.000000Z".
///
/// The field widths are fixed, so two rendered timestamps compare as strings
/// the same way their microsecond values compare as numbers.
fn format_timestamp(micros: u64) -> String {
    let micros = i64::try_from(micros).expect("timestamp overflows i64 microseconds");
    let time = DateTime::from_timestamp_micros(micros).expect("timestamp is out of chrono's range");
    time.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Returns the next monotonic timestamp as a JSON string.
async fn get_time(service: web::Data<TimeService>) -> Result<web::Json<String>, actix_web::Error> {
    let micros = service.next_timestamp().await.map_err(actix_web::error::ErrorServiceUnavailable)?;
    Ok(web::Json(format_timestamp(micros)))
}

/// Command-line configuration for one timestamp oracle node.
#[derive(Parser)]
struct Args {
    /// HTTP bind address for Raft and application requests.
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    /// Address of a node in the cluster to join.
    #[arg(long)]
    seed: Option<String>,

    /// Reserve timestamp space this far ahead of wall-clock time, in microseconds.
    #[arg(long, default_value = "1000000")]
    reservation_us: NonZeroU64,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();

    let args = Args::parse();
    let data_dir = PathBuf::from(format!("./data/timestamp-oracle/{}", args.addr.replace(':', "-")));
    let storage = FileStorage::new(data_dir).await?;
    let config = EzConfig::default();
    let app = TimeState::default();

    let raft = match &args.seed {
        Some(seed) => EzRaft::join(&args.addr, seed, app, storage, config).await?,
        None => EzRaft::create(&args.addr, app, storage, config).await?,
    };

    let service = web::Data::new(TimeService {
        raft: raft.clone(),
        reservation_width: args.reservation_us,
        reserved: Mutex::new(Reserved::default()),
    });
    let reserver = service.clone();
    tokio::spawn(async move { reserver.run_reserver().await });
    println!("Node {} listening on {}", raft.node_id(), args.addr);
    println!("Request a timestamp:\n    curl -X POST {}/time", args.addr);

    raft.serve_with(move |config| {
        config.app_data(service.clone()).route("/time", web::post().to(get_time));
    })
    .await
}
