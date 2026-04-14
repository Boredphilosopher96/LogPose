//! Validation-focused integration tests for the LogPose CLI.

use anyhow as _;
use clap as _;
use logpose_api_grpc as _;
use logpose_api_rest as _;
use logpose_client as _;
use logpose_config as _;
use logpose_core as _;
use logpose_query as _;
use logpose_storage as _;
use logpose_telemetry as _;
use logpose_types as _;
use serde as _;
use serde_json as _;
use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio as _;

#[test]
fn query_rejects_malformed_vector_as_cli_validation() {
    let temp_root = unique_temp_dir("cli-query-invalid-vector");

    let output = run_cli_without_assert(
        &temp_root,
        [
            "data",
            "query",
            "--collection",
            "colors",
            "--top-k",
            "1",
            "--vector",
            "1.0,wat",
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("invalid value"));
    assert!(stderr.contains("--vector"));
}

#[test]
fn query_requires_complete_snapshot_pair_as_cli_validation() {
    let temp_root = unique_temp_dir("cli-query-invalid-snapshot");

    let output = run_cli_without_assert(
        &temp_root,
        [
            "data",
            "query",
            "--collection",
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
    assert!(stderr.contains("must be provided together"));
}

#[test]
fn query_rejects_non_scalar_filter_values_as_cli_validation() {
    let temp_root = unique_temp_dir("cli-query-invalid-filter");

    let output = run_cli_without_assert(
        &temp_root,
        [
            "data",
            "query",
            "--collection",
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
    let temp_root = unique_temp_dir("cli-query-invalid-where");

    let output = run_cli_without_assert(
        &temp_root,
        [
            "data",
            "query",
            "--collection",
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
    let temp_root = unique_temp_dir("cli-query-invalid-predicate-json");
    let predicate_path = temp_root.join("predicate.json");
    fs::write(&predicate_path, "{not-valid-json").expect("predicate json should be written");

    let output = run_cli_without_assert(
        &temp_root,
        [
            "data",
            "query",
            "--collection",
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
    let temp_root = unique_temp_dir("cli-put-invalid-input");

    let output = run_cli_without_assert(
        &temp_root,
        [
            "data",
            "put",
            "--collection",
            "colors",
            "--input",
            "missing.jsonl",
        ],
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("failed to open JSONL input"));
    assert!(stderr.contains("missing.jsonl"));
}

fn run_cli_without_assert<const N: usize>(
    working_dir: &PathBuf,
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

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}
