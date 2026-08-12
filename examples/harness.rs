//! The harness the `*_test.rs` end-to-end tests share: build an example, run its
//! nodes as real child processes, and clean up after a passing test.
//!
//! Each test file is its own binary and uses a subset of this, so unused items
//! here are expected rather than dead.

#![allow(dead_code)]

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;

/// Upper bound for every wait in these tests
pub const WAIT: Duration = Duration::from_secs(30);

pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// An example's child process, killed when the test ends, pass or fail.
pub struct Node {
    child: Child,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A built example binary and the scratch directory its nodes run in.
pub struct Example {
    bin: PathBuf,
    work_dir: PathBuf,
    node_args: Vec<String>,
}

impl Example {
    /// Build example `name` and create the work directory its nodes share.
    ///
    /// The test cannot assume the example is already built: `cargo test --tests`,
    /// which `cargo llvm-cov` runs, builds test targets only. Nor can it derive the
    /// path from its own executable, because cargo has more than one target
    /// directory layout. Cargo offers no `CARGO_BIN_EXE_*` for examples, so ask it.
    pub fn build(name: &str) -> io::Result<Self> {
        let cargo = std::env::var_os("CARGO").expect("cargo sets CARGO for the tests it runs");
        let out = Command::new(cargo)
            .args(["build", "--package", "ezraft", "--example", name])
            .arg("--message-format=json-render-diagnostics")
            .stderr(Stdio::inherit())
            .output()
            .expect("failed to run cargo");
        assert!(out.status.success(), "building the {} example failed", name);

        let rendered = String::from_utf8(out.stdout).expect("cargo emits UTF-8");
        let artifact = rendered
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|msg| msg["reason"] == "compiler-artifact" && msg["target"]["name"] == name)
            .unwrap_or_else(|| panic!("cargo reported no artifact for the {} example", name));
        let executable = artifact["executable"]
            .as_str()
            .unwrap_or_else(|| panic!("cargo reported no executable for the {} example", name));

        let work_dir = std::env::temp_dir().join(format!("ezraft-{}-{}", name, std::process::id()));
        std::fs::create_dir_all(&work_dir)?;

        Ok(Self {
            bin: PathBuf::from(executable),
            work_dir,
            node_args: Vec::new(),
        })
    }

    /// Extra command line arguments, passed to every node this example spawns.
    pub fn node_args(mut self, args: &[&str]) -> Self {
        self.node_args = args.iter().map(|arg| arg.to_string()).collect();
        self
    }

    /// Spawn one node; its `./data/<addr>` dir and log land in the work directory.
    ///
    /// `seed` is the address of a node already in the cluster, or `None` to found
    /// a new one.
    pub fn spawn(&self, addr: &str, seed: Option<&str>) -> io::Result<Node> {
        let log_name = format!("node-{}.log", addr.replace(':', "-"));
        let log = File::create(self.work_dir.join(log_name))?;
        let mut cmd = Command::new(&self.bin);
        cmd.current_dir(&self.work_dir)
            .args(["--addr", addr])
            .args(&self.node_args)
            .stdout(log.try_clone()?)
            .stderr(log)
            .stdin(Stdio::null());
        if let Some(seed) = seed {
            cmd.args(["--seed", seed]);
        }
        Ok(Node { child: cmd.spawn()? })
    }

    /// Remove the work directory. Call this only on success, so that a failure
    /// leaves the data dirs and logs behind for debugging.
    pub fn cleanup(&self) -> io::Result<()> {
        std::fs::remove_dir_all(&self.work_dir)
    }
}

/// Grab a free port; the listener is dropped so the child process can bind it.
pub fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}
