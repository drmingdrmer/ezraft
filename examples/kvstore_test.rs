//! End-to-end test of the kvstore example: spawn real `kvstore` processes,
//! form a three-node cluster, and drive it purely over HTTP - the same
//! commands the example's doc header shows, minus the terminals.

mod harness;

use std::io;
use std::time::Instant;

use harness::*;
use serde_json::Value;
use serde_json::json;

/// POST a request to a node's write API, retrying while the node's HTTP
/// server is still binding or the cluster has no usable leader yet.
async fn write(addr: &str, req: Value) -> io::Result<Value> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + WAIT;
    loop {
        let res = client.post(format!("http://{}/api/write", addr)).json(&req).send().await;
        match res {
            Ok(resp) if resp.status().is_success() => return resp.json().await.map_err(io::Error::other),
            _ if Instant::now() < deadline => tokio::time::sleep(POLL_INTERVAL).await,
            Ok(resp) => return Err(io::Error::other(format!("write to {} failed: {}", addr, resp.status()))),
            Err(e) => return Err(io::Error::other(e)),
        }
    }
}

/// Poll a node's direct-read endpoint until `key` serves exactly `expected`,
/// where `None` is a key the node does not hold - the app answers that with no
/// entry. Getting there proves the state reached that node's own state machine.
async fn wait_for_key(addr: &str, key: &str, expected: Option<&str>) -> io::Result<()> {
    let client = reqwest::Client::new();
    let want = expected.map_or_else(|| json!({}), |value| json!({ key: value }));
    let deadline = Instant::now() + WAIT;
    let mut last = None;
    loop {
        // An error is the node's HTTP server still binding; retry until the deadline.
        let get = json!({"Get": {"key": key}});
        if let Ok(resp) = client.post(format!("http://{}/api/read", addr)).json(&get).send().await {
            let status = resp.status();
            if !status.is_success() {
                return Err(io::Error::other(format!(
                    "read {} from {} failed: {}",
                    key, addr, status
                )));
            }

            let entries: Value = resp.json().await.map_err(io::Error::other)?;
            if entries == want {
                return Ok(());
            }
            last = Some(entries);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "{} never served {:?} for {}, last seen: {:?}",
                addr, expected, key, last
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_node_kvstore_cluster_writes_and_reads() -> io::Result<()> {
    let example = Example::build("kvstore")?;

    let addr_a = free_addr();
    let a = example.spawn(&addr_a, None)?;

    // The first write doubles as the readiness wait: it only succeeds once the
    // founding node serves HTTP and leads.
    assert_eq!(
        json!({"value": null}),
        write(&addr_a, json!({"Set": {"key": "k1", "value": "v1"}})).await?
    );

    let addr_b = free_addr();
    let addr_c = free_addr();
    let b = example.spawn(&addr_b, Some(&addr_a))?;
    let c = example.spawn(&addr_c, Some(&addr_a))?;

    // Replication must deliver the pre-join write to both joiners.
    wait_for_key(&addr_b, "k1", Some("v1")).await?;
    wait_for_key(&addr_c, "k1", Some("v1")).await?;

    // Writes sent to joiners are forwarded to the leader: a set through one,
    // and a delete through the other returning the removed value.
    assert_eq!(
        json!({"value": null}),
        write(&addr_b, json!({"Set": {"key": "k2", "value": "v2"}})).await?
    );
    assert_eq!(
        json!({"value": "v1"}),
        write(&addr_c, json!({"Delete": {"key": "k1"}})).await?
    );

    // Overwriting a key hands back the value it replaced.
    assert_eq!(
        json!({"value": "v2"}),
        write(&addr_a, json!({"Set": {"key": "k2", "value": "v2b"}})).await?
    );

    // Every node converges on the exact final state: the overwritten key reads
    // back everywhere, and the deleted key answers with no entry at all.
    for addr in [&addr_a, &addr_b, &addr_c] {
        wait_for_key(addr, "k2", Some("v2b")).await?;
        wait_for_key(addr, "k1", None).await?;
    }

    // One read answering with every match, which is the shape a key-at-a-time
    // request cannot express.
    let prefixed: Value = reqwest::Client::new()
        .post(format!("http://{}/api/read", addr_a))
        .json(&json!({"Prefix": {"prefix": "k"}}))
        .send()
        .await
        .map_err(io::Error::other)?
        .json()
        .await
        .map_err(io::Error::other)?;
    assert_eq!(json!({"k2": "v2b"}), prefixed);

    drop(a);
    drop(b);
    drop(c);
    example.cleanup()?;
    Ok(())
}
