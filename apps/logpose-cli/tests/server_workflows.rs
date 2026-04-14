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

    assert_eq!(payload["metadata"]["product"], "LogPose");
    assert_eq!(payload["metadata"]["node_name"], "cli-diagnostics");
    assert_eq!(payload["metadata"]["profile"], "debug");
    assert_eq!(payload["role"], "combined");
    assert_eq!(payload["rest_endpoint"], fixture.rest_endpoint());
    assert_eq!(payload["grpc_endpoint"], fixture.grpc_endpoint());
    assert_eq!(payload["storage_engine"], "local");
    assert!(
        payload["metadata"]["version"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "version should be non-empty"
    );
    assert!(
        payload["metadata"]["git_sha"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "git_sha should be non-empty"
    );
}

#[test]
fn diagnostics_placement_reports_local_assignment() {
    let fixture = TestServerFixture::spawn("cli-placement");

    fixture.run_cli([
        "data",
        "create-collection",
        "--name",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);

    let output = fixture.run_cli(["diagnostics", "placement", "--collection", "documents"]);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let payload: Value = serde_json::from_str(&stdout).expect("placement should print json");

    assert_eq!(payload["collection_name"], "documents");
    assert_eq!(payload["assigned_node"], "cli-placement");
    assert_eq!(payload["assigned_role"], "data");
    assert_eq!(payload["route_kind"], "local");
}

#[test]
fn data_only_nodes_reject_collection_creation_over_cli_transport() {
    let fixture =
        TestServerFixture::spawn_with_role("cli-data-only", logpose_types::NodeRole::Data);

    let output = fixture.run_cli_expect_failure([
        "data",
        "create-collection",
        "--name",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");

    assert!(stderr.contains("failed to create collection"));
    assert!(
        stderr
            .contains("data-only nodes cannot accept control-plane collection lifecycle mutations")
    );
}

#[test]
fn control_only_nodes_reject_collection_creation_over_cli_transport() {
    let fixture =
        TestServerFixture::spawn_with_role("cli-control-only", logpose_types::NodeRole::Control);

    let output = fixture.run_cli_expect_failure([
        "data",
        "create-collection",
        "--name",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");

    assert!(stderr.contains("failed to create collection"));
    assert!(stderr.contains("without a local data plane"));
}

#[test]
fn diagnostics_status_preserves_server_reported_wildcard_listener_addresses() {
    let fixture = TestServerFixture::spawn_with_listener_hosts(
        "cli-wildcard",
        logpose_types::NodeRole::Combined,
        "0.0.0.0",
        "0.0.0.0",
    );
    let output = fixture.run_cli_with_config(
        ["diagnostics", "status"],
        render_config_with_hosts(
            "cli-wildcard",
            logpose_types::NodeRole::Combined,
            &fixture.temp_root.join("client-data"),
            "0.0.0.0",
            fixture.rest_addr.port(),
            "0.0.0.0",
            fixture.grpc_addr.port(),
        ),
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let payload: Value = serde_json::from_str(&stdout).expect("status should print json");

    assert_eq!(
        payload["rest_endpoint"],
        format!("http://0.0.0.0:{}", fixture.rest_addr.port())
    );
    assert_eq!(
        payload["grpc_endpoint"],
        format!("http://0.0.0.0:{}", fixture.grpc_addr.port())
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

    let profiled_query = fixture.run_cli([
        "data",
        "query",
        "--collection",
        "colors",
        "--top-k",
        "1",
        "--where",
        "kind:eq:keep",
        "--profile",
        "--vector",
        "1.0,0.0",
    ]);
    let profiled_query_stdout =
        String::from_utf8(profiled_query.stdout).expect("stdout should be utf8");
    let profiled_query_body: Value =
        serde_json::from_str(&profiled_query_stdout).expect("query output should be valid json");
    assert_eq!(profiled_query_body["matches"][0]["id"], "alpha");
    assert!(profiled_query_body["diagnostics"].is_object());
    assert!(profiled_query_body["diagnostics"]["stage_timings"].is_object());
    assert_eq!(
        profiled_query_body["diagnostics"]["chosen_plan"],
        "vector_first_exact"
    );
    assert!(
        profiled_query_body["diagnostics"]["candidates_reranked"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        profiled_query_body["diagnostics"]["candidates_merged"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        profiled_query_body["diagnostics"]["unit_scan_mix"]["mutable_exact"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["planning_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["prefilter_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["candidate_generation_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["postfilter_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["rerank_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["merge_micros"]
            .as_u64()
            .is_some()
    );

    let stats = fixture.run_cli(["data", "stats", "--collection", "colors"]);
    let stats_stdout = String::from_utf8(stats.stdout).expect("stdout should be utf8");
    let stats_body: Value =
        serde_json::from_str(&stats_stdout).expect("stats output should be valid json");
    assert_eq!(stats_body["collection_name"], "colors");
    assert_eq!(stats_body["live_record_count"], 3);
    assert_eq!(stats_body["deleted_record_count"], 0);
    assert_eq!(stats_body["mutable_op_count"], 3);
    assert_eq!(stats_body["segment_count"], 0);
    assert!(stats_body["maintenance"].is_object());
    assert_eq!(
        stats_body["query_units"]
            .as_array()
            .expect("query_units should be an array")
            .len(),
        1
    );
    assert_eq!(
        stats_body["query_units"][0]["artifact_stats"]
            .as_array()
            .expect("mutable artifact stats should be an array")
            .len(),
        1
    );
    assert_eq!(
        stats_body["query_units"][0]["artifact_stats"][0]["kind"],
        "mutable_delta"
    );
    assert!(
        stats_body["query_units"][0]["component_bytes"]["mutable_delta"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );

    let wal = fixture.run_cli(["data", "inspect", "--collection", "colors", "--wal"]);
    let wal_stdout = String::from_utf8(wal.stdout).expect("stdout should be utf8");
    let wal_body: Value =
        serde_json::from_str(&wal_stdout).expect("wal output should be valid json");
    assert_eq!(wal_body["target"], "wal");
    assert_eq!(
        wal_body["payload"]["records"]
            .as_array()
            .expect("wal records should be an array")
            .len(),
        3
    );

    let maintenance =
        fixture.run_cli(["data", "inspect", "--collection", "colors", "--maintenance"]);
    let maintenance_stdout = String::from_utf8(maintenance.stdout).expect("stdout should be utf8");
    let maintenance_body: Value =
        serde_json::from_str(&maintenance_stdout).expect("maintenance output should be valid json");
    assert_eq!(maintenance_body["target"], "maintenance");

    let flush = fixture.run_cli(["data", "flush", "--collection", "colors"]);
    let flush_stdout = String::from_utf8(flush.stdout).expect("stdout should be utf8");
    let flush_body: Value =
        serde_json::from_str(&flush_stdout).expect("flush output should be valid json");
    assert!(flush_body["manifest_generation"].as_u64().is_some());

    let immutable_stats = fixture.run_cli(["data", "stats", "--collection", "colors"]);
    let immutable_stats_stdout =
        String::from_utf8(immutable_stats.stdout).expect("stdout should be utf8");
    let immutable_stats_body: Value =
        serde_json::from_str(&immutable_stats_stdout).expect("stats output should be valid json");
    let immutable_unit = immutable_stats_body["query_units"]
        .as_array()
        .expect("query units should be an array")
        .iter()
        .find(|unit| unit["tier"] == "immutable")
        .expect("immutable unit should be present after flush");
    assert!(
        immutable_unit["artifact_stats"]
            .as_array()
            .is_some_and(|artifacts| artifacts.len() >= 2)
    );
    assert!(
        immutable_unit["component_bytes"]["ann_graph"]
            .as_u64()
            .is_some()
    );

    let manifest = fixture.run_cli(["data", "inspect", "--collection", "colors", "--manifest"]);
    let manifest_stdout = String::from_utf8(manifest.stdout).expect("stdout should be utf8");
    let manifest_body: Value =
        serde_json::from_str(&manifest_stdout).expect("manifest output should be valid json");
    assert_eq!(manifest_body["target"], "manifest");
    let segment_id = manifest_body["payload"]["segments"][0]["segment_id"]
        .as_str()
        .expect("segment id should be a string")
        .to_owned();

    let segment = fixture.run_cli([
        "data",
        "inspect",
        "--collection",
        "colors",
        "--segment",
        &segment_id,
    ]);
    let segment_stdout = String::from_utf8(segment.stdout).expect("stdout should be utf8");
    let segment_body: Value =
        serde_json::from_str(&segment_stdout).expect("segment output should be valid json");
    assert_eq!(
        segment_body["target"]
            .as_str()
            .expect("segment target should be a string"),
        format!("segment:{segment_id}")
    );
    assert_eq!(
        segment_body["payload"]["records"]
            .as_array()
            .expect("segment records should be an array")
            .len(),
        3
    );
    assert_eq!(segment_body["payload"]["segment"]["index_kind"], "hnsw");
    assert_eq!(
        segment_body["payload"]["artifacts"]
            .as_array()
            .expect("segment artifacts should be an array")
            .len(),
        2
    );
    assert!(
        segment_body["payload"]["hnsw_index"]["node_count"]
            .as_u64()
            .is_some_and(|count| count >= 3)
    );

    let ann_profiled_query = fixture.run_cli([
        "data",
        "query",
        "--collection",
        "colors",
        "--top-k",
        "1",
        "--where",
        "kind:eq:keep",
        "--profile",
        "--vector",
        "1.0,0.0",
    ]);
    let ann_profiled_query_stdout =
        String::from_utf8(ann_profiled_query.stdout).expect("stdout should be utf8");
    let ann_profiled_query_body: Value = serde_json::from_str(&ann_profiled_query_stdout)
        .expect("query output should be valid json");
    assert_eq!(ann_profiled_query_body["matches"][0]["id"], "alpha");
    assert_eq!(
        ann_profiled_query_body["diagnostics"]["chosen_plan"],
        "vector_first_ann"
    );
    assert!(
        ann_profiled_query_body["diagnostics"]["unit_scan_mix"]["immutable_ann"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        ann_profiled_query_body["diagnostics"]["stage_timings"]["candidate_generation_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert!(
        ann_profiled_query_body["diagnostics"]["stage_timings"]["rerank_micros"]
            .as_u64()
            .is_some()
    );

    let compact = fixture.run_cli(["data", "compact", "--collection", "colors"]);
    let compact_stdout = String::from_utf8(compact.stdout).expect("stdout should be utf8");
    let compact_body: Value =
        serde_json::from_str(&compact_stdout).expect("compact output should be valid json");
    assert!(compact_body["manifest_generation"].as_u64().is_some());
}

#[test]
fn profiled_query_surfaces_cooperative_filtered_ann_diagnostics() {
    let fixture = TestServerFixture::spawn("cli-cooperative-filtered-ann");
    let input_path = fixture.temp_root.join("cooperative-records.jsonl");
    let records = (0..12)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            format!(
                r#"{{"id":"doc-{index}","vector":[{},0.0],"metadata":{{"kind":"{kind}","version":{index}}}}}"#,
                index as f32 + 1.0
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&input_path, records).expect("jsonl input should be written");

    fixture.run_cli([
        "data",
        "create-collection",
        "--name",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    fixture.run_cli([
        "data",
        "put",
        "--collection",
        "documents",
        "--input",
        input_path.to_str().expect("input path should be utf8"),
    ]);
    fixture.run_cli(["data", "flush", "--collection", "documents"]);

    let profiled_query = fixture.run_cli([
        "data",
        "query",
        "--collection",
        "documents",
        "--top-k",
        "2",
        "--where",
        "kind:eq:keep",
        "--profile",
        "--vector",
        "1.0,0.0",
    ]);
    let profiled_query_stdout =
        String::from_utf8(profiled_query.stdout).expect("stdout should be utf8");
    let profiled_query_body: Value =
        serde_json::from_str(&profiled_query_stdout).expect("query output should be valid json");
    assert_eq!(profiled_query_body["matches"][0]["id"], "doc-8");
    assert_eq!(profiled_query_body["matches"][1]["id"], "doc-4");
    assert_eq!(
        profiled_query_body["diagnostics"]["chosen_plan"],
        "cooperative_filtered_ann"
    );
    assert_eq!(
        profiled_query_body["diagnostics"]["planner_reason"],
        "filtered ann traversal is cheaper than exact scan for this selectivity"
    );
    assert_eq!(
        profiled_query_body["diagnostics"]["estimated_selectivity"],
        Value::from(0.25)
    );
    assert_eq!(profiled_query_body["diagnostics"]["units_considered"], 2);
    assert_eq!(profiled_query_body["diagnostics"]["units_pruned"], 0);
    assert_eq!(profiled_query_body["diagnostics"]["units_scanned"], 1);
    let candidates_before_filter = profiled_query_body["diagnostics"]["candidates_before_filter"]
        .as_u64()
        .expect("candidates before filter should be numeric");
    let candidates_after_filter = profiled_query_body["diagnostics"]["candidates_after_filter"]
        .as_u64()
        .expect("candidates after filter should be numeric");
    assert!(candidates_before_filter >= 2);
    assert!(candidates_after_filter >= 2);
    assert!(candidates_after_filter <= candidates_before_filter);
    assert_eq!(
        profiled_query_body["diagnostics"]["fallback_reason"],
        Value::Null
    );
    assert_eq!(profiled_query_body["diagnostics"]["rerank_count"], 1);
    assert!(
        profiled_query_body["diagnostics"]["candidates_reranked"]
            .as_u64()
            .is_some_and(|count| count == candidates_after_filter)
    );
    assert!(
        profiled_query_body["diagnostics"]["candidates_merged"]
            .as_u64()
            .is_some_and(|count| count == candidates_after_filter)
    );
    assert!(
        profiled_query_body["diagnostics"]["unit_scan_mix"]["immutable_ann"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert_eq!(
        profiled_query_body["diagnostics"]["stage_timings"]["prefilter_micros"],
        Value::from(0)
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["planning_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["candidate_generation_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["postfilter_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["rerank_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert!(
        profiled_query_body["diagnostics"]["stage_timings"]["merge_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
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
        Self::spawn_with_role(node_name, logpose_types::NodeRole::Combined)
    }

    fn spawn_with_role(node_name: &str, node_role: logpose_types::NodeRole) -> Self {
        Self::spawn_with_listener_hosts(node_name, node_role, "127.0.0.1", "127.0.0.1")
    }

    fn spawn_with_listener_hosts(
        node_name: &str,
        node_role: logpose_types::NodeRole,
        rest_host: &str,
        grpc_host: &str,
    ) -> Self {
        let temp_root = unique_temp_dir(node_name);
        let storage_root = temp_root.join("data");
        let rest_addr = reserve_local_addr();
        let grpc_addr = reserve_local_addr();
        let runtime = Runtime::new().expect("runtime should build");
        let state = Arc::new(AppState::new(LogPoseConfig {
            node_name: node_name.to_owned(),
            node_role,
            rest_host: rest_host.to_owned(),
            rest_port: rest_addr.port(),
            grpc_host: grpc_host.to_owned(),
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
        let output = self.run_cli_raw(args);
        assert!(
            output.status.success(),
            "command failed with stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn run_cli_expect_failure<const N: usize>(&self, args: [&str; N]) -> Output {
        let output = self.run_cli_raw(args);
        assert!(
            !output.status.success(),
            "command unexpectedly succeeded with stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn run_cli_raw<const N: usize>(&self, args: [&str; N]) -> Output {
        let config = render_config(
            "cli-test",
            logpose_types::NodeRole::Combined,
            &self.temp_root.join("client-data"),
            self.rest_addr,
            self.grpc_addr,
        );
        self.run_cli_with_config(args, config)
    }

    fn run_cli_with_config<const N: usize>(&self, args: [&str; N], config: String) -> Output {
        Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
            .current_dir(&self.temp_root)
            .env("LOGPOSE_CONFIG", config)
            .args(args)
            .output()
            .expect("cli should run")
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
    node_role: logpose_types::NodeRole,
    storage_root: &Path,
    rest_addr: SocketAddr,
    grpc_addr: SocketAddr,
) -> String {
    render_config_with_hosts(
        node_name,
        node_role,
        storage_root,
        &rest_addr.ip().to_string(),
        rest_addr.port(),
        &grpc_addr.ip().to_string(),
        grpc_addr.port(),
    )
}

fn render_config_with_hosts(
    node_name: &str,
    node_role: logpose_types::NodeRole,
    storage_root: &Path,
    rest_host: &str,
    rest_port: u16,
    grpc_host: &str,
    grpc_port: u16,
) -> String {
    format!(
        r#"node_name = "{node_name}"
node_role = "{node_role}"
rest_host = "{rest_host}"
rest_port = {rest_port}
grpc_host = "{grpc_host}"
grpc_port = {grpc_port}
log_filter = "info"
storage_root = "{storage_root}""#,
        node_role = node_role.as_str(),
        rest_host = rest_host,
        rest_port = rest_port,
        grpc_host = grpc_host,
        grpc_port = grpc_port,
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
