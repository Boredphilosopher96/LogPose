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

/// Two ports that were free a moment ago, for the REST and gRPC listeners.
fn free_ports() -> (u16, u16) {
    let rest = TcpListener::bind("127.0.0.1:0").expect("bind a free REST port");
    let grpc = TcpListener::bind("127.0.0.1:0").expect("bind a free gRPC port");
    (
        rest.local_addr().expect("REST addr").port(),
        grpc.local_addr().expect("gRPC addr").port(),
    )
}

fn start_server(root: &Path, log: &Path) -> (Child, u16) {
    let (rest_port, grpc_port) = free_ports();
    let config = format!(
        "node_name = \"shutdown-test\"\n\
         rest_host = \"127.0.0.1\"\n\
         rest_port = {rest_port}\n\
         grpc_host = \"127.0.0.1\"\n\
         grpc_port = {grpc_port}\n\
         log_filter = \"info\"\n\
         storage_root = \"{}\"\n",
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
    (child, rest_port)
}

fn healthy(rest_port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", rest_port)) else {
        return false;
    };
    let request = "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response.starts_with("HTTP/1.1 200")
}

fn wait_until_healthy(child: &mut Child, rest_port: u16, log: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !healthy(rest_port) {
        let exited = child.try_wait().expect("poll the server");
        assert!(
            exited.is_none(),
            "server exited with {exited:?} before it was healthy:\n{}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        assert!(Instant::now() < deadline, "server never became healthy");
        std::thread::sleep(Duration::from_millis(50));
    }
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

#[test]
fn sigterm_and_sigint_stop_the_server_cleanly() {
    let temp = tempfile::Builder::new()
        .prefix("logpose-server-shutdown-")
        .tempdir()
        .expect("temp dir");
    let root = temp.path().join("data");
    for name in ["TERM", "INT"] {
        let log = temp.path().join(format!("server-{name}.log"));
        let (mut child, rest_port) = start_server(&root, &log);
        wait_until_healthy(&mut child, rest_port, &log);
        signal(&child, name);
        let status = wait_for_exit(&mut child, &log);
        let output = std::fs::read_to_string(&log).unwrap_or_default();
        assert!(
            status.success(),
            "SIG{name} should stop the server cleanly, got {status}:\n{output}"
        );
        assert!(
            output.contains("LogPose server stopped"),
            "SIG{name} should log the clean stop:\n{output}"
        );
    }
}
