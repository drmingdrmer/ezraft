//! End-to-end test of the time-server example, including leader replacement.

use std::fs::File;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use chrono::DateTime;
use serde_json::Value;

const WAIT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const RESERVATION_US: &str = "10000000";

struct Node {
    child: Child,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

static TIME_SERVER_BIN: LazyLock<PathBuf> = LazyLock::new(|| {
    let cargo = std::env::var_os("CARGO").expect("cargo sets CARGO for tests");
    let out = Command::new(cargo)
        .args(["build", "--package", "ezraft", "--example", "time_server"])
        .arg("--message-format=json-render-diagnostics")
        .stderr(Stdio::inherit())
        .output()
        .expect("failed to run cargo");
    assert!(out.status.success(), "building the time-server example failed");

    String::from_utf8(out.stdout)
        .expect("cargo emits UTF-8")
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|msg| msg["reason"] == "compiler-artifact" && msg["target"]["name"] == "time_server")
        .and_then(|msg| msg["executable"].as_str().map(PathBuf::from))
        .expect("cargo reported no executable for the time-server example")
});

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn spawn_node(work_dir: &Path, addr: &str, seed: Option<&str>) -> io::Result<Node> {
    let log = File::create(work_dir.join(format!("node-{}.log", addr.replace(':', "-"))))?;
    let mut cmd = Command::new(&*TIME_SERVER_BIN);
    cmd.current_dir(work_dir)
        .args(["--addr", addr, "--reservation-us", RESERVATION_US])
        .stdout(log.try_clone()?)
        .stderr(log)
        .stdin(Stdio::null());
    if let Some(seed) = seed {
        cmd.args(["--seed", seed]);
    }
    Ok(Node { child: cmd.spawn()? })
}

async fn timestamp_response(addrs: &[&str]) -> io::Result<Value> {
    if addrs.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "no node addresses"));
    }

    let client = reqwest::Client::new();
    let deadline = Instant::now() + WAIT;
    loop {
        let mut last_error = String::new();
        for addr in addrs {
            match client.post(format!("http://{}/time", addr)).send().await {
                Ok(response) if response.status().is_success() => {
                    return response.json().await.map_err(io::Error::other);
                }
                Ok(response) => last_error = format!("{} returned {}", addr, response.status()),
                Err(error) => last_error = format!("{}: {}", addr, error),
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "no leader served a timestamp: {}",
                last_error
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Requests a timestamp and converts the RFC 3339 string back to microseconds,
/// the unit the assertions and the replicated high-water mark both use.
async fn get_timestamp(addrs: &[&str]) -> io::Result<u64> {
    let response = timestamp_response(addrs).await?;
    let rendered = response
        .as_str()
        .ok_or_else(|| io::Error::other(format!("invalid timestamp response: {}", response)))?;
    let time = DateTime::parse_from_rfc3339(rendered).map_err(io::Error::other)?;
    let micros = time.timestamp_micros();
    u64::try_from(micros).map_err(io::Error::other)
}

async fn reserved_end(addr: &str) -> io::Result<u64> {
    reqwest::Client::new()
        .post(format!("http://{}/api/read", addr))
        .json(&Value::Null)
        .send()
        .await
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)
}

fn unix_timestamp_micros() -> io::Result<u64> {
    let micros = SystemTime::now().duration_since(UNIX_EPOCH).map_err(io::Error::other)?.as_micros();
    u64::try_from(micros).map_err(io::Error::other)
}

async fn wait_for_voters(addr: &str, count: usize) -> io::Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + WAIT;
    loop {
        if let Ok(response) = client.get(format!("http://{}/api/metrics", addr)).send().await
            && let Ok(metrics) = response.json::<Value>().await
        {
            let configs = &metrics["committed_membership_config"]["membership"]["configs"];
            if configs.as_array().is_some_and(|configs| {
                configs.len() == 1 && configs[0].as_array().is_some_and(|voters| voters.len() == count)
            }) {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            return Err(io::Error::other(format!("cluster never reached {} voters", count)));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn timestamp_increases_across_a_leader_switch() -> io::Result<()> {
    let work_dir = std::env::temp_dir().join(format!("ezraft-time-server-{}", std::process::id()));
    std::fs::create_dir_all(&work_dir)?;

    let addr_a = free_addr();
    let a = spawn_node(&work_dir, &addr_a, None)?;
    wait_for_voters(&addr_a, 1).await?;

    let addr_b = free_addr();
    let b = spawn_node(&work_dir, &addr_b, Some(&addr_a))?;
    wait_for_voters(&addr_a, 2).await?;

    let addr_c = free_addr();
    let c = spawn_node(&work_dir, &addr_c, Some(&addr_a))?;
    wait_for_voters(&addr_a, 3).await?;

    let before = unix_timestamp_micros()?;
    let first = get_timestamp(&[&addr_a]).await?;
    let reserved = reserved_end(&addr_a).await?;
    assert!(first >= before);
    assert!(first < reserved);

    let follower_status = reqwest::Client::new()
        .post(format!("http://{}/time", addr_b))
        .send()
        .await
        .map_err(io::Error::other)?
        .status();
    assert_eq!(reqwest::StatusCode::SERVICE_UNAVAILABLE, follower_status);

    let second = get_timestamp(&[&addr_a]).await?;
    assert!(second > first);
    assert!(second < reserved);

    let reserved_before_failover = reserved_end(&addr_a).await?;
    drop(a);
    let survivors = [addr_b.as_str(), addr_c.as_str()];
    let third = get_timestamp(&survivors).await?;
    assert!(third >= reserved_before_failover);
    assert!(get_timestamp(&survivors).await? > third);

    drop(b);
    drop(c);
    std::fs::remove_dir_all(&work_dir)?;
    Ok(())
}
