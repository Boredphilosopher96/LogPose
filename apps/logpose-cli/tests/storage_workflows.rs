//! Integration tests for local storage workflows exposed by the CLI.

use anyhow as _;
use clap as _;
use logpose_config as _;
use logpose_storage as _;
use logpose_telemetry as _;
use logpose_types as _;
use serde as _;
use serde_json as _;
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

fn run_cli<const N: usize>(
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

    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .current_dir(working_dir)
        .env("LOGPOSE_CONFIG", config)
        .args(args)
        .output()
        .expect("cli command should run");

    assert!(
        output.status.success(),
        "command failed with stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
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
