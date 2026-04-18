//! Integration tests for the LogPose CLI.

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
use std::process::Command;
use tokio as _;
use walkdir as _;

#[test]
fn top_level_help_includes_primary_workflows_and_interactive_entrypoint() {
    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .arg("--help")
        .output()
        .expect("cli help should execute");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("status"));
    assert!(stdout.contains("database"));
    assert!(stdout.contains("collection"));
    assert!(stdout.contains("record"));
    assert!(stdout.contains("query"));
    assert!(stdout.contains("inspect"));
    assert!(stdout.contains("config"));
    assert!(stdout.contains("interactive"));
    assert!(stdout.contains("--json"));
    assert!(stdout.contains("--output <OUTPUT>"));
    assert!(stdout.contains("--auth-token <TOKEN>"));
    assert!(!stdout.contains("\ntenant\n"));
    assert!(!stdout.contains("--interactive <MODE>"));
}

#[test]
fn collection_create_help_lists_metric_values_and_examples() {
    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .args(["collection", "create", "--help"])
        .output()
        .expect("collection create help should execute");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("Create a collection"));
    assert!(stdout.contains("--dimensions <DIMENSIONS>"));
    assert!(stdout.contains("--metric <METRIC>"));
    assert!(stdout.contains("possible values:"));
    assert!(stdout.contains("cosine"));
    assert!(stdout.contains("dot"));
    assert!(stdout.contains("l2"));
    assert!(stdout.contains("--database <DATABASE>"));
    assert!(!stdout.contains("--tenant <TENANT>"));
    assert!(stdout.contains("Examples:"));
    assert!(stdout.contains("logpose collection create colors --dimensions 768 --metric cosine"));
    assert!(stdout.contains("logpose interactive"));
}

#[test]
fn interactive_help_describes_search_defaults_and_file_picker() {
    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .args(["interactive", "--help"])
        .output()
        .expect("interactive help should execute");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("interactive"));
    assert!(stdout.contains("dashboard"));
    assert!(stdout.contains("json view"));
    assert!(stdout.contains("command preview"));
    assert!(stdout.contains("--workflow <WORKFLOW>"));
    assert!(stdout.contains("--create"));
    assert!(stdout.contains("--name <NAME>"));
    assert!(stdout.contains("--database <DATABASE>"));
    assert!(!stdout.contains("--tenant <TENANT>"));
}

#[test]
fn query_help_explains_input_formats_and_examples() {
    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .args(["query", "--help"])
        .output()
        .expect("query help should execute");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("--vector <VECTOR>"));
    assert!(stdout.contains("0.12,-0.44,0.90"));
    assert!(stdout.contains("--database <DATABASE>"));
    assert!(!stdout.contains("--tenant <TENANT>"));
    assert!(stdout.contains("--filter <FIELD=VALUE>"));
    assert!(stdout.contains("kind=article"));
    assert!(stdout.contains("score=json:7"));
    assert!(stdout.contains("--where <FIELD:OP[:VALUE]>"));
    assert!(stdout.contains("eq, ne, lt, lte, gt, gte, exists, is_null"));
    assert!(stdout.contains("--explain <MODE>"));
    assert!(stdout.contains("plan"));
    assert!(stdout.contains("profile"));
}

#[test]
fn inspect_help_lists_supported_targets() {
    let output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .args(["inspect", "--help"])
        .output()
        .expect("inspect help should execute");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");
    assert!(stdout.contains("manifest"));
    assert!(stdout.contains("wal"));
    assert!(stdout.contains("maintenance"));
    assert!(stdout.contains("segment"));
}
