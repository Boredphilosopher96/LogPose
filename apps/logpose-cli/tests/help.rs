//! Integration tests for the LogPose CLI.

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
use std::process::Command;
use tokio as _;

#[test]
fn help_includes_operator_workflows() {
    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .arg("--help")
        .output()
        .expect("cli help should execute");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("diagnostics"));
    assert!(stdout.contains("admin"));
    assert!(stdout.contains("data"));
}

#[test]
fn data_help_includes_storage_workflows() {
    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .args(["data", "--help"])
        .output()
        .expect("data help should execute");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("create-collection"));
    assert!(stdout.contains("get-collection"));
    assert!(stdout.contains("put"));
    assert!(stdout.contains("delete"));
    assert!(stdout.contains("flush"));
    assert!(stdout.contains("compact"));
    assert!(stdout.contains("query"));
    assert!(stdout.contains("stats"));
    assert!(stdout.contains("inspect"));
}
