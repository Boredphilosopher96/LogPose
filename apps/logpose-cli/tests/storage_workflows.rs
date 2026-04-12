//! Integration tests for local storage workflows exposed by the CLI.

use anyhow as _;
use clap as _;
use logpose_config as _;
use logpose_query as _;
use logpose_storage as _;
use logpose_telemetry as _;
use logpose_types as _;
use serde as _;
use serde_json as _;
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio as _;

#[test]
fn create_collection_put_and_stats_work_via_cli() {
    let temp_root = unique_temp_dir("cli-storage");
    let storage_root = temp_root.join("data");
    let input_path = temp_root.join("records.jsonl");
    fs::write(
        &input_path,
        r#"{"id":"alpha","vector":[1.0,0.0],"metadata":{"color":"red"}}
{"id":"beta","vector":[0.0,1.0],"metadata":{"color":"green"}}"#,
    )
    .expect("jsonl input should be written");

    run_cli(
        &temp_root,
        [
            "data",
            "create-collection",
            "--name",
            "colors",
            "--dimensions",
            "2",
            "--metric",
            "cosine",
        ],
        &storage_root,
    );

    run_cli(
        &temp_root,
        [
            "data",
            "put",
            "--collection",
            "colors",
            "--input",
            input_path.to_str().expect("input path should be utf8"),
        ],
        &storage_root,
    );

    let output = run_cli(
        &temp_root,
        ["data", "stats", "--collection", "colors"],
        &storage_root,
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("\"collection_name\": \"colors\""));
    assert!(stdout.contains("\"live_record_count\": 2"));
}

#[test]
fn create_collection_put_and_query_work_via_cli() {
    let temp_root = unique_temp_dir("cli-query");
    let storage_root = temp_root.join("data");
    let input_path = temp_root.join("records.jsonl");
    fs::write(
        &input_path,
        r#"{"id":"alpha","vector":[1.0,0.0],"metadata":{"color":"red"}}
{"id":"beta","vector":[0.5,0.0],"metadata":{"color":"green"}}
{"id":"gamma","vector":[-1.0,0.0],"metadata":{"color":"blue"}}"#,
    )
    .expect("jsonl input should be written");

    create_collection_and_put_records(&temp_root, &storage_root, &input_path, "dot");

    let limited_output = run_cli(
        &temp_root,
        [
            "data",
            "query",
            "--collection",
            "colors",
            "--top-k",
            "2",
            "--vector",
            "1.0,0.0",
        ],
        &storage_root,
    );
    let limited_stdout = String::from_utf8(limited_output.stdout).expect("stdout should be utf8");
    let limited_response: Value =
        serde_json::from_str(&limited_stdout).expect("query output should be valid json");
    let limited_matches = limited_response["matches"]
        .as_array()
        .expect("matches should be an array");
    assert_eq!(limited_matches.len(), 2);
    assert_eq!(limited_matches[0]["id"], "alpha");
    assert_eq!(limited_matches[1]["id"], "beta");
    assert_eq!(limited_response["returned"], 2);
    assert_eq!(limited_response["top_k"], 2);

    let output = run_cli(
        &temp_root,
        [
            "data",
            "query",
            "--collection",
            "colors",
            "--top-k",
            "10",
            "--vector",
            "1.0,0.0",
        ],
        &storage_root,
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    let response: Value = serde_json::from_str(&stdout).expect("query output should be valid json");

    assert_eq!(response["metric"], "dot");
    assert!(response.get("snapshot").is_some());

    let matches = response["matches"]
        .as_array()
        .expect("matches should be an array");
    assert_eq!(matches.len(), 3);
    assert_eq!(matches[0]["id"], "alpha");
    assert_eq!(matches[1]["id"], "beta");
    assert_eq!(matches[2]["id"], "gamma");
    assert_eq!(response["returned"], 3);
    assert_eq!(response["top_k"], 10);
}

#[test]
fn query_rejects_malformed_vector_as_cli_validation() {
    let temp_root = unique_temp_dir("cli-query-invalid-vector");
    let storage_root = temp_root.join("data");
    let input_path = temp_root.join("records.jsonl");
    fs::write(
        &input_path,
        r#"{"id":"alpha","vector":[1.0,0.0],"metadata":{"color":"red"}}"#,
    )
    .expect("jsonl input should be written");

    create_collection_and_put_records(&temp_root, &storage_root, &input_path, "dot");

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
        &storage_root,
    );

    assert!(!output.status.success(), "command should fail validation");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");
    assert!(stderr.contains("invalid value"));
    assert!(stderr.contains("--vector"));
}

fn create_collection_and_put_records(
    temp_root: &Path,
    storage_root: &Path,
    input_path: &Path,
    metric: &str,
) {
    run_cli(
        &temp_root.to_path_buf(),
        [
            "data",
            "create-collection",
            "--name",
            "colors",
            "--dimensions",
            "2",
            "--metric",
            metric,
        ],
        storage_root,
    );

    run_cli(
        &temp_root.to_path_buf(),
        [
            "data",
            "put",
            "--collection",
            "colors",
            "--input",
            input_path.to_str().expect("input path should be utf8"),
        ],
        storage_root,
    );
}

fn run_cli<const N: usize>(
    working_dir: &PathBuf,
    args: [&str; N],
    storage_root: &Path,
) -> std::process::Output {
    let output = run_cli_without_assert(working_dir, args, storage_root);

    assert!(
        output.status.success(),
        "command failed with stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run_cli_without_assert<const N: usize>(
    working_dir: &PathBuf,
    args: [&str; N],
    storage_root: &Path,
) -> std::process::Output {
    let config = format!(
        r#"node_name = "cli-test"
rest_host = "127.0.0.1"
rest_port = 8080
grpc_host = "127.0.0.1"
grpc_port = 50051
log_filter = "info"
storage_root = "{}""#,
        storage_root.display()
    );

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
