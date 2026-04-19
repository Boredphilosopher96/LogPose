//! Validation-focused integration tests for the LogPose CLI.

use anyhow as _;
use clap as _;
use crossterm as _;
use insta as _;
use logpose_api_grpc as _;
use logpose_api_rest as _;
use logpose_auth as _;
use logpose_catalog as _;
use logpose_cli as _;
use logpose_client as _;
use logpose_config as _;
use logpose_core as _;
use logpose_query as _;
use logpose_storage as _;
use logpose_telemetry as _;
use logpose_types as _;
use ratatui as _;
use serde as _;
use serde_json as _;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio as _;
use walkdir as _;

#[path = "support/server_fixture.rs"]
mod support;

use support::TestServerFixture;

#[test]
fn query_rejects_malformed_vector_as_cli_validation() {
    let temp_root = TempRoot::new("cli-query-invalid-vector");

    let output = run_cli_without_assert(
        temp_root.path(),
        ["query", "colors", "--top-k", "1", "--vector", "1.0,wat"],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("invalid value"));
    assert!(stderr.contains("--vector"));
}

#[test]
fn query_requires_complete_snapshot_pair_as_cli_validation() {
    let temp_root = TempRoot::new("cli-query-invalid-snapshot");

    let output = run_cli_without_assert(
        temp_root.path(),
        [
            "query",
            "colors",
            "--top-k",
            "1",
            "--vector",
            "1.0,0.0",
            "--snapshot-manifest-generation",
            "1",
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--snapshot-visible-seq-no"));
}

#[test]
fn query_requires_complete_read_barrier_pair_as_cli_validation() {
    let temp_root = TempRoot::new("cli-query-invalid-read-barrier");

    let output = run_cli_without_assert(
        temp_root.path(),
        [
            "query",
            "colors",
            "--top-k",
            "1",
            "--vector",
            "1.0,0.0",
            "--read-barrier-manifest-generation",
            "1",
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--read-barrier-visible-seq-no"));
}

#[test]
fn collection_stats_requires_complete_snapshot_pair_as_cli_validation() {
    let temp_root = TempRoot::new("cli-stats-invalid-snapshot");

    let output = run_cli_without_assert(
        temp_root.path(),
        [
            "collection",
            "stats",
            "colors",
            "--snapshot-manifest-generation",
            "1",
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--snapshot-visible-seq-no"));
}

#[test]
fn collection_stats_requires_complete_read_barrier_pair_as_cli_validation() {
    let temp_root = TempRoot::new("cli-stats-invalid-read-barrier");

    let output = run_cli_without_assert(
        temp_root.path(),
        [
            "collection",
            "stats",
            "colors",
            "--read-barrier-manifest-generation",
            "1",
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("--read-barrier-visible-seq-no"));
}

#[test]
fn query_rejects_mixing_snapshot_and_read_barrier_as_cli_validation() {
    let temp_root = TempRoot::new("cli-query-mixed-read-constraints");

    let output = run_cli_without_assert(
        temp_root.path(),
        [
            "query",
            "colors",
            "--top-k",
            "1",
            "--vector",
            "1.0,0.0",
            "--snapshot-manifest-generation",
            "1",
            "--snapshot-visible-seq-no",
            "1",
            "--read-barrier-manifest-generation",
            "1",
            "--read-barrier-visible-seq-no",
            "1",
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("cannot be used with"));
}

#[test]
fn query_rejects_non_scalar_filter_values_as_cli_validation() {
    let temp_root = TempRoot::new("cli-query-invalid-filter");

    let output = run_cli_without_assert(
        temp_root.path(),
        [
            "query",
            "colors",
            "--top-k",
            "1",
            "--vector",
            "1.0,0.0",
            "--filter",
            r#"kind=json:{"nested":true}"#,
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("query filters must contain only scalar JSON values"));
}

#[test]
fn query_rejects_unsupported_where_operators_as_cli_validation() {
    let temp_root = TempRoot::new("cli-query-invalid-where");

    let output = run_cli_without_assert(
        temp_root.path(),
        [
            "query",
            "colors",
            "--top-k",
            "1",
            "--vector",
            "1.0,0.0",
            "--where",
            "kind:between:keep",
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("unsupported where operator"));
}

#[test]
fn query_surfaces_local_predicate_json_errors_before_connection_failures() {
    let temp_root = TempRoot::new("cli-query-invalid-predicate-json");
    let predicate_path = temp_root.path().join("predicate.json");
    fs::write(&predicate_path, "{not-valid-json").expect("predicate json should be written");

    let output = run_cli_without_assert(
        temp_root.path(),
        [
            "query",
            "colors",
            "--top-k",
            "1",
            "--vector",
            "1.0,0.0",
            "--predicate-json",
            predicate_path.to_str().expect("path should be utf8"),
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("failed to parse predicate json"));
}

#[test]
fn put_surfaces_local_input_errors_before_connection_failures() {
    let temp_root = TempRoot::new("cli-put-invalid-input");

    let output = run_cli_without_assert(
        temp_root.path(),
        ["record", "put", "colors", "--input", "missing.jsonl"],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("failed to open JSONL input"));
    assert!(stderr.contains("missing.jsonl"));
}

#[test]
fn interactive_collection_create_supports_fuzzy_workflow_search_and_defaults() {
    let fixture = TestServerFixture::spawn("cli-interactive-create");

    let output = fixture.run_cli_with_stdin(["--json", "interactive"], "create\n\ncolors\n2\n\n\n");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(
        output.status.success(),
        "interactive create failed with stdout: {stdout}\nstderr: {stderr}"
    );
    let payload: serde_json::Value =
        serde_json::from_str(&stdout).expect("interactive create should print valid json");

    assert!(stderr.contains("Workflow search"));
    assert!(stderr.contains("Select option"));
    assert!(stderr.contains("Collection name"));
    assert!(stderr.contains("Embedding dimensions"));
    assert!(stderr.contains("Distance metric search"));
    assert!(stderr.contains("Collection created"));
    assert_eq!(payload["name"], "colors");
    assert_eq!(payload["metric"], "dot");
}

#[test]
fn interactive_create_shortcut_skips_workflow_picker_and_prefills_known_values() {
    let fixture = TestServerFixture::spawn("cli-interactive-create-shortcut");

    let output = fixture.run_cli_with_stdin(
        [
            "--json",
            "interactive",
            "--create",
            "--name",
            "colors",
            "--dimensions",
            "2",
        ],
        "\n\n",
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(
        output.status.success(),
        "interactive create shortcut failed with stdout: {stdout}\nstderr: {stderr}"
    );
    let payload: serde_json::Value =
        serde_json::from_str(&stdout).expect("interactive create shortcut should print valid json");

    assert!(!stderr.contains("Workflow search"));
    assert!(!stderr.contains("Collection name"));
    assert!(!stderr.contains("Embedding dimensions"));
    assert!(stderr.contains("Distance metric"));
    assert_eq!(payload["name"], "colors");
    assert_eq!(payload["metric"], "dot");
}

#[test]
fn interactive_record_put_supports_fuzzy_file_picker() {
    let fixture = TestServerFixture::spawn("cli-interactive-put");
    fixture.run_cli([
        "collection",
        "create",
        "colors",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);

    let input_path = fixture.temp_root.join("records.jsonl");
    fs::write(
        &input_path,
        r#"{"id":"alpha","vector":[1.0,0.0],"metadata":{"color":"red"}}"#,
    )
    .expect("jsonl input should be written");

    let output = fixture.run_cli_with_stdin(["--json", "interactive"], "put\n\n\n\nrecords\n\n");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(
        output.status.success(),
        "interactive put failed with stdout: {stdout}\nstderr: {stderr}"
    );
    let payload: serde_json::Value =
        serde_json::from_str(&stdout).expect("interactive put should print valid json");

    assert!(stderr.contains("Workflow search"));
    assert!(stderr.contains("Choose A Collection"));
    assert!(stderr.contains("File search"));
    assert!(stderr.contains("records.jsonl"));
    assert!(stderr.contains("Write completed"));
    assert_eq!(payload["applied_ops"], 1);
}

fn run_cli_without_assert<const N: usize>(
    working_dir: &Path,
    args: [&str; N],
) -> std::process::Output {
    let config = r#"node_name = "cli-test"
node_role = "combined"
rest_host = "127.0.0.1"
rest_port = 18080
grpc_host = "127.0.0.1"
grpc_port = 15051
log_filter = "info"
storage_root = ".logpose-test""#;

    Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .current_dir(working_dir)
        .env("LOGPOSE_CONFIG", config)
        .args(args)
        .output()
        .expect("cli command should run")
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(prefix: &str) -> Self {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
        fs::create_dir_all(&path).expect("temp dir should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
