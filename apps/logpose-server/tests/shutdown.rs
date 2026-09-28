//! Process-level shutdown of the `logpose-server` binary.

#![cfg(unix)]

use anyhow as _;
use logpose_api_grpc as _;
use logpose_api_rest as _;
use logpose_config as _;
use logpose_core as _;
use logpose_telemetry as _;
use tokio as _;
use tracing as _;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const ETCD_ENDPOINTS_ENV: &str = "LOGPOSE_TEST_ETCD_ENDPOINTS";

/// Two ports that were free a moment ago, for the REST and gRPC listeners.
fn free_ports() -> (u16, u16) {
    let rest = TcpListener::bind("127.0.0.1:0").expect("bind a free REST port");
    let grpc = TcpListener::bind("127.0.0.1:0").expect("bind a free gRPC port");
    (
        rest.local_addr().expect("REST addr").port(),
        grpc.local_addr().expect("gRPC addr").port(),
    )
}

/// A started server and the ports it listens on.
struct Server {
    child: Child,
    rest_port: u16,
    grpc_port: u16,
}

/// Start the server on `root`, with `extra` appended to its TOML configuration.
fn start_server(node_name: &str, root: &Path, log: &Path, extra: &str) -> Server {
    let (rest_port, grpc_port) = free_ports();
    let config = format!(
        "node_name = \"{node_name}\"\n\
         rest_host = \"127.0.0.1\"\n\
         rest_port = {rest_port}\n\
         grpc_host = \"127.0.0.1\"\n\
         grpc_port = {grpc_port}\n\
         log_filter = \"info\"\n\
         storage_root = \"{}\"\n\
         {extra}",
        root.display()
    );
    let log = std::fs::File::create(log).expect("create the server log");
    let child = Command::new(env!("CARGO_BIN_EXE_logpose-server"))
        .env("LOGPOSE_CONFIG", config)
        .env_remove("RUST_BACKTRACE")
        .stdin(Stdio::null())
        .stdout(log.try_clone().expect("clone the log handle"))
        .stderr(log)
        .spawn()
        .expect("spawn logpose-server");
    Server {
        child,
        rest_port,
        grpc_port,
    }
}

/// The body of a `GET` on the REST listener, when it answers `200`.
fn get(rest_port: u16, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", rest_port)).ok()?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response
        .starts_with("HTTP/1.1 200")
        .then(|| {
            response
                .split_once("\r\n\r\n")
                .map(|(_, body)| body.to_owned())
        })
        .flatten()
}

/// Wait until `ready` holds for the server, failing if it exits first.
fn wait_until(server: &mut Server, log: &Path, what: &str, ready: impl Fn(u16) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready(server.rest_port) {
        let exited = server.child.try_wait().expect("poll the server");
        assert!(
            exited.is_none(),
            "server exited with {exited:?} before it was {what}:\n{}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        assert!(Instant::now() < deadline, "server never became {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_until_healthy(server: &mut Server, log: &Path) {
    wait_until(server, log, "healthy", |port| {
        get(port, "/health").is_some()
    });
}

fn signal(child: &Child, name: &str) {
    let status = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(child.id().to_string())
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -{name} failed");
}

fn wait_for_exit(child: &mut Child, log: &Path) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().expect("poll the server") {
            return status;
        }
        let timed_out = Instant::now() >= deadline;
        if timed_out {
            let _ = child.kill();
            let _ = child.wait();
        }
        assert!(
            !timed_out,
            "server did not exit within 30 seconds of the signal:\n{}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Signal the server, wait for it to exit, and require a clean stop.
fn stop_cleanly(server: &mut Server, log: &Path, name: &str) -> String {
    signal(&server.child, name);
    let status = wait_for_exit(&mut server.child, log);
    let output = std::fs::read_to_string(log).unwrap_or_default();
    assert!(
        status.success(),
        "SIG{name} should stop the server cleanly, got {status}:\n{output}"
    );
    assert!(
        output.contains("LogPose server stopped"),
        "SIG{name} should log the clean stop:\n{output}"
    );
    output
}

fn temp_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("logpose-server-{label}-"))
        .tempdir()
        .expect("temp dir")
}

#[test]
fn sigterm_and_sigint_stop_the_server_cleanly() {
    let temp = temp_dir("shutdown");
    let root = temp.path().join("data");
    for name in ["TERM", "INT"] {
        let log = temp.path().join(format!("server-{name}.log"));
        let mut server = start_server("shutdown-test", &root, &log, "");
        wait_until_healthy(&mut server, &log);
        stop_cleanly(&mut server, &log, name);
    }
}

#[test]
fn a_request_in_flight_is_answered_before_the_server_stops() {
    let temp = temp_dir("in-flight");
    let log = temp.path().join("server.log");
    let mut server = start_server("in-flight", &temp.path().join("data"), &log, "");
    wait_until_healthy(&mut server, &log);

    // The server is still reading this request's body when the signal arrives.
    let body = br#"{"top_k": 1}"#;
    let mut stream = TcpStream::connect(("127.0.0.1", server.rest_port)).expect("connect");
    let head = format!(
        "POST /v2/databases/default/collections/missing/query HTTP/1.1\r\n\
         Host: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).expect("send the head");
    stream.write_all(&body[..1]).expect("send part of the body");
    std::thread::sleep(Duration::from_millis(200));
    signal(&server.child, "TERM");
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        server.child.try_wait().expect("poll the server").is_none(),
        "the server must wait for the request in flight:\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );

    stream
        .write_all(&body[1..])
        .expect("send the rest of the body");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read the response");
    assert!(
        response.starts_with("HTTP/1.1 404"),
        "the request in flight should be answered: {response}"
    );
    let status = wait_for_exit(&mut server.child, &log);
    assert!(status.success(), "{status}");
}

#[test]
fn connections_that_never_finish_a_request_do_not_keep_the_server_running() {
    let temp = temp_dir("hung-clients");
    let log = temp.path().join("server.log");
    let mut server = start_server(
        "hung-clients",
        &temp.path().join("data"),
        &log,
        "drain_timeout_ms = 300\n",
    );
    wait_until_healthy(&mut server, &log);

    // A REST request whose head never ends, and a gRPC connection that never sends a byte.
    let mut rest = TcpStream::connect(("127.0.0.1", server.rest_port)).expect("connect REST");
    rest.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n")
        .expect("send part of a head");
    let grpc = TcpStream::connect(("127.0.0.1", server.grpc_port)).expect("connect gRPC");
    std::thread::sleep(Duration::from_millis(200));

    let output = stop_cleanly(&mut server, &log, "TERM");
    assert!(
        output.contains("drain timeout"),
        "the server should report the connections it closed:\n{output}"
    );
    drop((rest, grpc));
}

#[test]
fn a_stopped_leader_hands_leadership_to_the_next_node_at_once() {
    let Ok(endpoints) = std::env::var(ETCD_ENDPOINTS_ENV) else {
        eprintln!(
            "skipping a_stopped_leader_hands_leadership_to_the_next_node_at_once: \
             {ETCD_ENDPOINTS_ENV} is not set"
        );
        return;
    };
    let endpoints = endpoints
        .split(',')
        .map(|endpoint| format!("\"{}\"", endpoint.trim()))
        .collect::<Vec<_>>()
        .join(", ");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos();
    // Leases this long outlive the test: only a revoke on shutdown frees the leader key.
    let etcd = format!(
        "[metadata]\nbackend = \"etcd\"\n\n[metadata.etcd]\n\
         endpoints = [{endpoints}]\n\
         key_prefix = \"/logpose-server-shutdown-{}-{nanos}\"\n\
         membership_ttl_secs = 60\nleadership_ttl_secs = 60\n",
        std::process::id()
    );
    let leads = |name: &'static str| {
        move |port: u16| {
            get(port, "/v2/runtime/status").is_some_and(|status| {
                status.contains("\"is_local_leader\":true")
                    && status.contains(&format!("\"leader_node\":\"{name}\""))
            })
        }
    };
    let temp = temp_dir("handover");

    let log_a = temp.path().join("server-a.log");
    let mut a = start_server("node-a", &temp.path().join("a"), &log_a, &etcd);
    wait_until(&mut a, &log_a, "the leader", leads("node-a"));
    stop_cleanly(&mut a, &log_a, "TERM");

    // The first tick of the next node campaigns; a leader key left behind would hold it off
    // for the whole 60-second lease.
    let log_b = temp.path().join("server-b.log");
    let mut b = start_server("node-b", &temp.path().join("b"), &log_b, &etcd);
    let started = Instant::now();
    wait_until(&mut b, &log_b, "the leader", leads("node-b"));
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "node-b took {:?} to lead",
        started.elapsed()
    );
    stop_cleanly(&mut b, &log_b, "TERM");
}
