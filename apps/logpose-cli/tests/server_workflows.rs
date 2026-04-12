//! End-to-end tests for server-backed CLI workflows.

use clap as _;
use logpose_client as _;
use logpose_config::LogPoseConfig;
use logpose_core::AppState;
use logpose_query as _;
use logpose_storage as _;
use logpose_telemetry as _;
use logpose_types as _;
use serde as _;
use serde_json::Value;
use std::{
    fs,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Runtime;

#[test]
fn diagnostics_status_reports_server_metadata_and_endpoints() {
    let fixture = TestServerFixture::spawn("cli-diagnostics");

    let output = fixture.run_cli(["diagnostics", "status"]);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let payload: Value = serde_json::from_str(&stdout).expect("status should print json");

    assert_eq!(payload["product"], "LogPose");
    assert_eq!(payload["node_name"], "cli-diagnostics");
    assert_eq!(payload["profile"], "debug");
    assert_eq!(payload["rest_endpoint"], fixture.rest_endpoint());
    assert_eq!(payload["grpc_endpoint"], fixture.grpc_endpoint());
    assert!(
        payload["version"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "version should be non-empty"
    );
    assert!(
        payload["git_sha"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "git_sha should be non-empty"
    );
}

#[test]
fn data_commands_run_against_the_server_over_grpc() {
    let fixture = TestServerFixture::spawn("cli-server-workflow");
    let input_path = fixture.temp_root.join("records.jsonl");
    fs::write(
        &input_path,
        r#"{"id":"alpha","vector":[1.0,0.0],"metadata":{"color":"red","kind":"keep"}}
{"id":"beta","vector":[0.5,0.0],"metadata":{"color":"green","kind":"drop"}}
{"id":"gamma","vector":[0.8,0.0],"metadata":{"color":"blue","kind":"keep"}}"#,
    )
    .expect("jsonl input should be written");

    let create = fixture.run_cli([
        "data",
        "create-collection",
        "--name",
        "colors",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    let create_stdout = String::from_utf8(create.stdout).expect("stdout should be utf8");
    let create_body: Value =
        serde_json::from_str(&create_stdout).expect("create output should be valid json");
    assert_eq!(create_body["name"], "colors");

    let get = fixture.run_cli(["data", "get-collection", "--collection", "colors"]);
    let get_stdout = String::from_utf8(get.stdout).expect("stdout should be utf8");
    let get_body: Value =
        serde_json::from_str(&get_stdout).expect("collection output should be valid json");
    assert_eq!(get_body["name"], "colors");
    assert_eq!(get_body["metric"], "dot");

    fixture.run_cli([
        "data",
        "put",
        "--collection",
        "colors",
        "--input",
        input_path.to_str().expect("input path should be utf8"),
    ]);

    let query = fixture.run_cli([
        "data",
        "query",
        "--collection",
        "colors",
        "--top-k",
        "3",
        "--filter",
        "kind=keep",
        "--vector",
        "1.0,0.0",
    ]);
    let query_stdout = String::from_utf8(query.stdout).expect("stdout should be utf8");
    let query_body: Value =
        serde_json::from_str(&query_stdout).expect("query output should be valid json");
    let matches = query_body["matches"]
        .as_array()
        .expect("matches should be an array");
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0]["id"], "alpha");
    assert_eq!(matches[1]["id"], "gamma");

    let stats = fixture.run_cli(["data", "stats", "--collection", "colors"]);
    let stats_stdout = String::from_utf8(stats.stdout).expect("stdout should be utf8");
    let stats_body: Value =
        serde_json::from_str(&stats_stdout).expect("stats output should be valid json");
    assert_eq!(stats_body["collection_name"], "colors");
    assert_eq!(stats_body["live_record_count"], 3);

    let flush = fixture.run_cli(["data", "flush", "--collection", "colors"]);
    let flush_stdout = String::from_utf8(flush.stdout).expect("stdout should be utf8");
    let flush_body: Value =
        serde_json::from_str(&flush_stdout).expect("flush output should be valid json");
    assert!(flush_body["manifest_generation"].as_u64().is_some());

    let compact = fixture.run_cli(["data", "compact", "--collection", "colors"]);
    let compact_stdout = String::from_utf8(compact.stdout).expect("stdout should be utf8");
    let compact_body: Value =
        serde_json::from_str(&compact_stdout).expect("compact output should be valid json");
    assert!(compact_body["manifest_generation"].as_u64().is_some());

    let inspect = fixture.run_cli(["data", "inspect", "--collection", "colors", "--manifest"]);
    let inspect_stdout = String::from_utf8(inspect.stdout).expect("stdout should be utf8");
    let inspect_body: Value =
        serde_json::from_str(&inspect_stdout).expect("inspect output should be valid json");
    assert_eq!(inspect_body["target"], "manifest");
}

struct TestServerFixture {
    temp_root: PathBuf,
    rest_addr: SocketAddr,
    grpc_addr: SocketAddr,
    runtime: Runtime,
    server: tokio::task::JoinHandle<()>,
}

impl TestServerFixture {
    fn spawn(node_name: &str) -> Self {
        let temp_root = unique_temp_dir(node_name);
        let storage_root = temp_root.join("data");
        let rest_addr = reserve_local_addr();
        let grpc_addr = reserve_local_addr();
        let runtime = Runtime::new().expect("runtime should build");
        let state = Arc::new(AppState::new(LogPoseConfig {
            node_name: node_name.to_owned(),
            rest_host: rest_addr.ip().to_string(),
            rest_port: rest_addr.port(),
            grpc_host: grpc_addr.ip().to_string(),
            grpc_port: grpc_addr.port(),
            log_filter: "info".to_owned(),
            storage_root,
        }));
        let server = runtime.spawn(async move {
            let rest_state = Arc::clone(&state);
            let grpc_state = Arc::clone(&state);
            let _ = tokio::try_join!(
                async move {
                    logpose_api_rest::serve(rest_state)
                        .await
                        .map_err(anyhow::Error::from)
                },
                async move {
                    logpose_api_grpc::serve(grpc_state)
                        .await
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                }
            );
        });

        wait_for_port(grpc_addr);

        Self {
            temp_root,
            rest_addr,
            grpc_addr,
            runtime,
            server,
        }
    }

    fn rest_endpoint(&self) -> String {
        format!("http://{}", self.rest_addr)
    }

    fn grpc_endpoint(&self) -> String {
        format!("http://{}", self.grpc_addr)
    }

    fn run_cli<const N: usize>(&self, args: [&str; N]) -> Output {
        let config = render_config(
            "cli-test",
            &self.temp_root.join("client-data"),
            self.rest_addr,
            self.grpc_addr,
        );
        let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
            .current_dir(&self.temp_root)
            .env("LOGPOSE_CONFIG", config)
            .args(args)
            .output()
            .expect("cli should run");

        assert!(
            output.status.success(),
            "command failed with stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
}

impl Drop for TestServerFixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = self.runtime.block_on(async { (&mut self.server).await });
    }
}

fn render_config(
    node_name: &str,
    storage_root: &Path,
    rest_addr: SocketAddr,
    grpc_addr: SocketAddr,
) -> String {
    format!(
        r#"node_name = "{node_name}"
rest_host = "{rest_host}"
rest_port = {rest_port}
grpc_host = "{grpc_host}"
grpc_port = {grpc_port}
log_filter = "info"
storage_root = "{storage_root}""#,
        rest_host = rest_addr.ip(),
        rest_port = rest_addr.port(),
        grpc_host = grpc_addr.ip(),
        grpc_port = grpc_addr.port(),
        storage_root = storage_root.display(),
    )
}

fn reserve_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener should expose addr");
    drop(listener);
    address
}

fn wait_for_port(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(address).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }

    assert!(
        TcpStream::connect(address).is_ok(),
        "timed out waiting for server at {address}"
    );
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}
