//! End-to-end tests for server-backed CLI workflows.

use clap as _;
use crossterm as _;
use insta as _;
use logpose_auth::{AuthenticationMode, DatabaseRole};
use logpose_catalog as _;
use logpose_cli as _;
use logpose_client as _;
use logpose_query as _;
use logpose_storage as _;
use logpose_telemetry as _;
use logpose_types as _;
use ratatui as _;
use serde as _;
use serde_json::{Value, json};
use std::{fs, process::Command};
use walkdir as _;

#[path = "support/server_fixture.rs"]
mod support;

use support::{TestServerFixture, render_config_with_hosts};

fn query_response_body(payload: &Value) -> &Value {
    assert!(
        payload.get("response").is_none(),
        "CLI query JSON should be flattened instead of wrapped in a response envelope"
    );
    payload
}

fn scoped_response_body(payload: &Value) -> &Value {
    assert!(
        payload.get("response").is_none(),
        "CLI scoped JSON should be flattened instead of wrapped in a response envelope"
    );
    payload
}

#[test]
fn diagnostics_status_defaults_to_human_summary() {
    let fixture = TestServerFixture::spawn("cli-diagnostics-human");

    let output = fixture.run_cli(["status"]);
    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf8");

    assert!(stdout.contains("Runtime Status"));
    assert!(stdout.contains("Node: cli-diagnostics-human"));
    assert!(stdout.contains("Role: combined"));
    assert!(stdout.contains(&fixture.rest_endpoint()));
    assert!(stdout.contains(&fixture.grpc_endpoint()));
    assert!(!stdout.trim_start().starts_with('{'));
}

#[test]
fn diagnostics_status_reports_server_metadata_and_endpoints_as_json() {
    let fixture = TestServerFixture::spawn("cli-diagnostics");

    let output = fixture.run_cli_json(&["status"]);
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
fn authenticated_status_requires_token_and_accepts_flag_or_env() {
    let fixture = TestServerFixture::spawn_with_auth("cli-auth-status");

    let missing = fixture.run_cli_expect_failure(["status"]);
    let missing_stderr = String::from_utf8(missing.stderr).expect("stderr should be utf8");
    assert!(missing_stderr.contains("failed to fetch runtime status"));
    assert!(missing_stderr.contains("missing bearer token"));

    let token = fixture
        .auth_token
        .as_deref()
        .expect("auth fixture should expose operator token");
    let flagged = fixture.run_cli_json(&["--auth-token", token, "status"]);
    let flagged_stdout = String::from_utf8(flagged.stdout).expect("stdout should be utf8");
    let flagged_body: Value =
        serde_json::from_str(&flagged_stdout).expect("status should print json");
    assert_eq!(flagged_body["metadata"]["node_name"], "cli-auth-status");

    let env_config = render_config_with_hosts(
        "cli-auth-status",
        logpose_types::NodeRole::Combined,
        &fixture.temp_root.join("client-data"),
        &fixture.rest_addr.ip().to_string(),
        fixture.rest_addr.port(),
        &fixture.grpc_addr.ip().to_string(),
        fixture.grpc_addr.port(),
    );
    let env_output = Command::new(env!("CARGO_BIN_EXE_logpose-cli"))
        .current_dir(&fixture.temp_root)
        .env("LOGPOSE_CONFIG", env_config)
        .env("LOGPOSE_AUTH_TOKEN", token)
        .args(["--json", "status"])
        .output()
        .expect("cli should run");
    assert!(
        env_output.status.success(),
        "env-auth command failed with stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&env_output.stdout),
        String::from_utf8_lossy(&env_output.stderr)
    );
}

#[test]
fn database_policy_commands_round_trip_over_grpc() {
    let fixture = TestServerFixture::spawn("cli-database-policy");
    let policy_path = fixture.temp_root.join("policy.json");
    fs::write(
        &policy_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "database_name": "default",
            "authentication_mode": AuthenticationMode::ExternalToken,
            "role_bindings": [
                {
                    "database_name": "default",
                    "principal_name": "writer",
                    "role": DatabaseRole::ReadWrite
                },
                {
                    "database_name": "default",
                    "principal_name": "reader",
                    "role": DatabaseRole::ReadOnly
                }
            ]
        }))
        .expect("policy json should serialize"),
    )
    .expect("policy input should be written");

    let set = fixture.run_cli([
        "database",
        "policy",
        "set",
        "--input",
        policy_path.to_str().expect("path should be utf8"),
    ]);
    let set_stdout = String::from_utf8(set.stdout).expect("stdout should be utf8");
    assert!(set_stdout.contains("Database policy updated"));
    assert!(set_stdout.contains("Database: default"));
    assert!(set_stdout.contains("external_token"));
    assert!(set_stdout.contains("writer"));
    assert!(set_stdout.contains("read_write"));

    let show = fixture.run_cli_json(&["database", "policy", "show"]);
    let show_stdout = String::from_utf8(show.stdout).expect("stdout should be utf8");
    let show_body: Value =
        serde_json::from_str(&show_stdout).expect("policy output should be valid json");

    assert_eq!(show_body["database_name"], "default");
    assert_eq!(show_body["authentication_mode"], "external_token");
    assert_eq!(show_body["role_bindings"][0]["principal_name"], "writer");
    assert_eq!(show_body["role_bindings"][0]["role"], "read_write");
    assert_eq!(show_body["role_bindings"][1]["principal_name"], "reader");
    assert_eq!(show_body["role_bindings"][1]["role"], "read_only");
}

#[test]
fn database_commands_round_trip_over_grpc() {
    let fixture = TestServerFixture::spawn_with_auth("cli-namespace");

    let database = fixture.run_cli_json(&[
        "--auth-token",
        "operator-secret",
        "database",
        "put",
        "analytics",
    ]);
    let database_stdout = String::from_utf8(database.stdout).expect("stdout should be utf8");
    let database_body: Value =
        serde_json::from_str(&database_stdout).expect("database should print json");
    assert_eq!(database_body["name"], "analytics");

    let database_show = fixture.run_cli_json(&[
        "--auth-token",
        "operator-secret",
        "database",
        "show",
        "analytics",
    ]);
    let database_show_stdout =
        String::from_utf8(database_show.stdout).expect("stdout should be utf8");
    let database_show_body: Value =
        serde_json::from_str(&database_show_stdout).expect("database should print json");
    assert_eq!(database_show_body["name"], "analytics");

    let databases = fixture.run_cli_json(&["--auth-token", "operator-secret", "database", "list"]);
    let databases_stdout = String::from_utf8(databases.stdout).expect("stdout should be utf8");
    let databases_body: Value =
        serde_json::from_str(&databases_stdout).expect("database list should print json");
    let databases = databases_body
        .as_array()
        .expect("databases should be an array");
    assert!(
        databases
            .iter()
            .any(|database| database["name"] == "default"),
        "default database should still be visible"
    );
    assert!(
        databases
            .iter()
            .any(|database| database["name"] == "analytics"),
        "new database should be visible"
    );
}

#[test]
fn read_only_auth_token_can_read_but_cannot_write_over_cli() {
    let fixture = TestServerFixture::spawn_with_auth("cli-auth-readonly");
    let policy_path = fixture.temp_root.join("readonly-policy.json");
    fs::write(
        &policy_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "database_name": "default",
            "authentication_mode": AuthenticationMode::ExternalToken,
            "role_bindings": [
                {
                    "database_name": "default",
                    "principal_name": "reader",
                    "role": DatabaseRole::ReadOnly
                }
            ]
        }))
        .expect("policy json should serialize"),
    )
    .expect("policy input should be written");

    fixture.run_cli([
        "--auth-token",
        "operator-secret",
        "database",
        "policy",
        "set",
        "--input",
        policy_path.to_str().expect("path should be utf8"),
    ]);
    fixture.run_cli([
        "--auth-token",
        "operator-secret",
        "collection",
        "create",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);

    let stats = fixture.run_cli_json(&[
        "--auth-token",
        "reader-secret",
        "collection",
        "stats",
        "documents",
    ]);
    let stats_stdout = String::from_utf8(stats.stdout).expect("stdout should be utf8");
    let stats_body: Value = serde_json::from_str(&stats_stdout).expect("stats should print json");
    assert_eq!(stats_body["database_name"], "default");

    let denied = fixture.run_cli_expect_failure([
        "--auth-token",
        "reader-secret",
        "record",
        "delete",
        "documents",
        "alpha",
    ]);
    let denied_stderr = String::from_utf8(denied.stderr).expect("stderr should be utf8");
    assert!(denied_stderr.contains("failed to delete record"));
    assert!(denied_stderr.contains("not allowed"));
}

#[test]
fn diagnostics_placement_reports_local_assignment() {
    let fixture = TestServerFixture::spawn("cli-placement");

    fixture.run_cli([
        "collection",
        "create",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);

    let output = fixture.run_cli_json(&["collection", "placement", "documents"]);
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
        "collection",
        "create",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");

    assert!(stderr.contains("failed to create collection"));
    assert!(stderr.contains(
        "WRONG_NODE_ROLE: node 'cli-data-only' is running as 'data' and cannot accept \
         control-plane collection lifecycle mutations"
    ));
    assert!(stderr.contains("code: FAILED_PRECONDITION"), "{stderr}");
    assert!(
        stderr.contains("metadata: node=cli-data-only, node_role=data"),
        "{stderr}"
    );
    assert!(!stderr.contains("retry:"), "{stderr}");
}

#[test]
fn server_validation_errors_print_their_reason_and_field_violations() {
    let fixture = TestServerFixture::spawn("cli-typed-errors");
    let input_path = fixture.temp_root.join("duplicate-keys.jsonl");
    fs::write(
        &input_path,
        "{\"id\":\"alpha\",\"vector\":[1.0,0.0]}\n{\"id\":\"alpha\",\"vector\":[0.0,1.0]}\n",
    )
    .expect("jsonl input should be written");
    fixture.run_cli([
        "collection",
        "create",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);

    let output = fixture.run_cli_expect_failure([
        "record",
        "put",
        "documents",
        "--input",
        input_path.to_str().expect("input path should be utf8"),
    ]);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");

    assert!(
        stderr.contains("INVALID_ARGUMENT: write batch includes primary key"),
        "{stderr}"
    );
    assert!(stderr.contains("code: INVALID_ARGUMENT"), "{stderr}");
    assert!(
        stderr.contains("field records[1]: write batch includes primary key"),
        "{stderr}"
    );
    // A batch commits atomically: the advice must not suggest a partial commit.
    assert!(
        stderr.contains(
            "failed to write records; each batch commits atomically, so the failing batch was \
             applied in full or not at all; verify collection state before retrying it"
        ),
        "{stderr}"
    );
    assert!(!stderr.contains("partially"), "{stderr}");
}

#[test]
fn records_that_do_not_fit_the_schema_fail_before_reaching_the_server() {
    let fixture = TestServerFixture::spawn("cli-schema-errors");
    let input_path = fixture.temp_root.join("wrong-dimensions.jsonl");
    fs::write(&input_path, r#"{"id":"alpha","vector":[1.0,0.0,0.5]}"#)
        .expect("jsonl input should be written");
    fixture.run_cli([
        "collection",
        "create",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);

    let output = fixture.run_cli_expect_failure([
        "record",
        "put",
        "documents",
        "--input",
        input_path.to_str().expect("input path should be utf8"),
    ]);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");

    assert!(
        stderr.contains("JSONL record on line 1 does not fit the schema"),
        "{stderr}"
    );
    assert!(
        stderr.contains("vector field 'vector' expects 2 dimensions, found 3"),
        "{stderr}"
    );
}

#[test]
fn invalid_collection_schemas_name_the_rejected_field() {
    let fixture = TestServerFixture::spawn("cli-schema-create-errors");
    let output = fixture.run_cli_expect_failure([
        "collection",
        "create",
        "documents",
        "--dimensions",
        "0",
        "--metric",
        "dot",
    ]);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");

    assert!(stderr.contains("code: INVALID_ARGUMENT"), "{stderr}");
    assert!(stderr.contains("field vectors[0].dimensions:"), "{stderr}");
}

#[test]
fn control_only_nodes_reject_collection_creation_over_cli_transport() {
    let fixture =
        TestServerFixture::spawn_with_role("cli-control-only", logpose_types::NodeRole::Control);

    let output = fixture.run_cli_expect_failure([
        "collection",
        "create",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    let stderr = String::from_utf8(output.stderr).expect("stderr should be utf8");

    assert!(stderr.contains("failed to create collection"));
    assert!(stderr.contains(
        "is running as 'control' and cannot accept control-plane collection lifecycle mutations"
    ));
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
        ["--json", "status"],
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
        r#"{"id":"alpha","vector":[1.0,0.0],"color":"red","kind":"keep"}
{"id":"beta","vector":[0.5,0.0],"color":"green","kind":"drop"}
{"id":"gamma","vector":[0.8,0.0],"color":"blue","kind":"keep"}"#,
    )
    .expect("jsonl input should be written");

    let create = fixture.run_cli([
        "collection",
        "create",
        "colors",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    let create_stdout = String::from_utf8(create.stdout).expect("stdout should be utf8");
    assert!(create_stdout.contains("Collection created"));
    assert!(create_stdout.contains("colors"));

    let get = fixture.run_cli_json(&["collection", "show", "colors"]);
    let get_stdout = String::from_utf8(get.stdout).expect("stdout should be utf8");
    let get_body: Value =
        serde_json::from_str(&get_stdout).expect("collection output should be valid json");
    assert_eq!(get_body["name"], "colors");
    assert_eq!(get_body["schema"]["vectors"][0]["metric"], "dot");

    fixture.run_cli([
        "record",
        "put",
        "colors",
        "--input",
        input_path.to_str().expect("input path should be utf8"),
    ]);

    let query = fixture.run_cli_json(&[
        "query",
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
    let query_response = query_response_body(&query_body);
    let matches = query_response["hits"]
        .as_array()
        .expect("hits should be an array");
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0]["record"]["id"], "alpha");
    assert_eq!(matches[1]["record"]["id"], "gamma");

    let profiled_query = fixture.run_cli_json(&[
        "query",
        "colors",
        "--top-k",
        "1",
        "--where",
        "kind:eq:keep",
        "--explain",
        "profile",
        "--vector",
        "1.0,0.0",
    ]);
    let profiled_query_stdout =
        String::from_utf8(profiled_query.stdout).expect("stdout should be utf8");
    let profiled_query_body: Value =
        serde_json::from_str(&profiled_query_stdout).expect("query output should be valid json");
    let profiled_query_response = query_response_body(&profiled_query_body);
    assert_eq!(profiled_query_response["hits"][0]["record"]["id"], "alpha");
    assert!(profiled_query_response["diagnostics"].is_object());
    assert!(profiled_query_response["diagnostics"]["stage_timings"].is_object());
    assert_eq!(
        profiled_query_response["diagnostics"]["chosen_plan"],
        "predicate_first_exact"
    );
    assert!(
        profiled_query_response["diagnostics"]["candidates_merged"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        profiled_query_response["diagnostics"]["unit_scan_mix"]["memtable_scan"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["planning_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["prefilter_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["candidate_generation_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["postfilter_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["rerank_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["merge_micros"]
            .as_u64()
            .is_some()
    );

    let stats = fixture.run_cli_json(&["collection", "stats", "colors"]);
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

    let wal = fixture.run_cli_json(&["inspect", "wal", "colors"]);
    let wal_stdout = String::from_utf8(wal.stdout).expect("stdout should be utf8");
    let wal_body: Value =
        serde_json::from_str(&wal_stdout).expect("wal output should be valid json");
    let wal_response = scoped_response_body(&wal_body);
    assert_eq!(wal_response["target"], "wal");
    assert_eq!(
        wal_response["payload"]["records"]
            .as_array()
            .expect("wal records should be an array")
            .len(),
        3
    );

    let maintenance = fixture.run_cli_json(&["inspect", "maintenance", "colors"]);
    let maintenance_stdout = String::from_utf8(maintenance.stdout).expect("stdout should be utf8");
    let maintenance_body: Value =
        serde_json::from_str(&maintenance_stdout).expect("maintenance output should be valid json");
    let maintenance_response = scoped_response_body(&maintenance_body);
    assert_eq!(maintenance_response["target"], "maintenance");

    let flush = fixture.run_cli_json(&["collection", "flush", "colors"]);
    let flush_stdout = String::from_utf8(flush.stdout).expect("stdout should be utf8");
    let flush_body: Value =
        serde_json::from_str(&flush_stdout).expect("flush output should be valid json");
    let flush_response = scoped_response_body(&flush_body);
    assert!(flush_response["manifest_generation"].as_u64().is_some());

    let immutable_stats = fixture.run_cli_json(&["collection", "stats", "colors"]);
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
            .is_some_and(
                |artifacts| artifacts.iter().any(|artifact| artifact["file_name"]
                    .as_str()
                    .is_some_and(|name| name.ends_with(".seg")))
            )
    );
    assert!(
        immutable_unit["component_bytes"]["segment"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );

    let manifest = fixture.run_cli_json(&["inspect", "manifest", "colors"]);
    let manifest_stdout = String::from_utf8(manifest.stdout).expect("stdout should be utf8");
    let manifest_body: Value =
        serde_json::from_str(&manifest_stdout).expect("manifest output should be valid json");
    let manifest_response = scoped_response_body(&manifest_body);
    assert_eq!(manifest_response["target"], "manifest");
    let segment_id = manifest_response["payload"]["segments"][0]["segment_id"]
        .as_str()
        .expect("segment id should be a string")
        .to_owned();

    let segment = fixture.run_cli_json(&["inspect", "segment", "colors", &segment_id]);
    let segment_stdout = String::from_utf8(segment.stdout).expect("stdout should be utf8");
    let segment_body: Value =
        serde_json::from_str(&segment_stdout).expect("segment output should be valid json");
    let segment_response = scoped_response_body(&segment_body);
    assert_eq!(
        segment_response["target"]
            .as_str()
            .expect("segment target should be a string"),
        format!("segment:{segment_id}")
    );
    assert_eq!(
        segment_response["payload"]["records"]
            .as_array()
            .expect("segment records should be an array")
            .len(),
        3
    );
    assert_eq!(
        segment_response["payload"]["segment"]["file_name"],
        format!("{segment_id}.seg")
    );
    assert!(
        segment_response["payload"]["sections"]
            .as_array()
            .is_some_and(|sections| sections
                .iter()
                .any(|section| section["kind"] == "VectorF32"))
    );

    let ann_profiled_query = fixture.run_cli_json(&[
        "query",
        "colors",
        "--top-k",
        "1",
        "--where",
        "kind:eq:keep",
        "--explain",
        "profile",
        "--vector",
        "1.0,0.0",
    ]);
    let ann_profiled_query_stdout =
        String::from_utf8(ann_profiled_query.stdout).expect("stdout should be utf8");
    let ann_profiled_query_body: Value = serde_json::from_str(&ann_profiled_query_stdout)
        .expect("query output should be valid json");
    let ann_query_response = query_response_body(&ann_profiled_query_body);
    assert_eq!(ann_query_response["hits"][0]["record"]["id"], "alpha");
    assert_eq!(
        ann_query_response["diagnostics"]["chosen_plan"],
        "predicate_first_exact"
    );
    assert!(
        ann_query_response["diagnostics"]["unit_scan_mix"]["exact_f32"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        ann_query_response["diagnostics"]["stage_timings"]["candidate_generation_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        ann_query_response["diagnostics"]["stage_timings"]["rerank_micros"]
            .as_u64()
            .is_some()
    );

    let compact = fixture.run_cli_json(&["collection", "compact", "colors"]);
    let compact_stdout = String::from_utf8(compact.stdout).expect("stdout should be utf8");
    let compact_body: Value =
        serde_json::from_str(&compact_stdout).expect("compact output should be valid json");
    let compact_response = scoped_response_body(&compact_body);
    assert!(compact_response["manifest_generation"].as_u64().is_some());
}

#[test]
fn count_scroll_scan_and_delete_by_filter_run_against_the_server() {
    let fixture = TestServerFixture::spawn("cli-server-count-scroll");
    let input = fixture.temp_root.join("records.jsonl");
    fs::write(
        &input,
        [
            r#"{"id":"alpha","vector":[1.0,0.0],"kind":"keep","rank":3}"#,
            r#"{"id":"beta","vector":[0.9,0.0],"kind":"drop","rank":1}"#,
            r#"{"id":"gamma","vector":[0.8,0.0],"kind":"keep","rank":2}"#,
            r#"{"id":"delta","vector":[0.7,0.0],"kind":"drop","rank":5}"#,
            r#"{"id":"epsilon","vector":[0.6,0.0],"kind":"keep","rank":4}"#,
        ]
        .join("\n"),
    )
    .expect("jsonl input should be written");
    fixture.run_cli([
        "collection",
        "create",
        "colors",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    fixture.run_cli([
        "record",
        "put",
        "colors",
        "--input",
        input.to_str().expect("input path should be utf8"),
    ]);
    let json = |args: &[&str]| -> Value {
        let output = fixture.run_cli_json(args);
        serde_json::from_slice(&output.stdout).expect("output should be valid json")
    };
    let ids = |body: &Value, key: &str| -> Vec<String> {
        body[key]
            .as_array()
            .expect("an array of records")
            .iter()
            .map(|item| {
                let record = item.get("record").unwrap_or(item);
                record["id"].as_str().unwrap_or_default().to_owned()
            })
            .collect()
    };

    let counted = json(&["count", "colors", "--filter", "kind=keep"]);
    assert_eq!(scoped_response_body(&counted)["count"], 3);

    let first = json(&[
        "scroll",
        "colors",
        "--where",
        "kind:eq:keep",
        "--page-size",
        "2",
    ]);
    assert_eq!(ids(&first, "records"), vec!["alpha", "epsilon"]);
    let cursor = first["next_cursor"]
        .as_str()
        .expect("a first page of two has a cursor")
        .to_owned();
    let second = json(&[
        "scroll",
        "colors",
        "--where",
        "kind:eq:keep",
        "--page-size",
        "2",
        "--cursor",
        &cursor,
    ]);
    assert_eq!(ids(&second, "records"), vec!["gamma"]);
    assert!(second["next_cursor"].is_null(), "{second}");

    // A query without a vector is a filtered scan, in primary key order by default.
    let scanned = json(&[
        "query",
        "colors",
        "--top-k",
        "2",
        "--where",
        "rank:gte:json:2",
    ]);
    assert_eq!(
        ids(query_response_body(&scanned), "hits"),
        vec!["alpha", "delta"]
    );
    assert!(scanned["hits"][0].get("score").is_none(), "{scanned}");

    let deleted = json(&["record", "delete", "colors", "--filter", "kind=drop"]);
    assert_eq!(scoped_response_body(&deleted)["applied_ops"], 2);
    let counted = json(&["count", "colors"]);
    assert_eq!(counted["count"], 3);
}

#[test]
fn query_and_stats_support_read_barrier_flags_against_server() {
    let fixture = TestServerFixture::spawn("cli-server-read-barrier");
    let first_input = fixture.temp_root.join("records-first.jsonl");
    let second_input = fixture.temp_root.join("records-second.jsonl");
    fs::write(
        &first_input,
        r#"{"id":"alpha","vector":[1.0,0.0],"kind":"keep"}"#,
    )
    .expect("first jsonl input should be written");
    fs::write(
        &second_input,
        r#"{"id":"beta","vector":[0.4,0.0],"kind":"keep"}"#,
    )
    .expect("second jsonl input should be written");

    fixture.run_cli([
        "collection",
        "create",
        "colors",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);

    let first_write = fixture.run_cli_json(&[
        "record",
        "put",
        "colors",
        "--input",
        first_input
            .to_str()
            .expect("first input path should be utf8"),
    ]);
    let first_write_stdout = String::from_utf8(first_write.stdout).expect("stdout should be utf8");
    let first_write_body: Value =
        serde_json::from_str(&first_write_stdout).expect("write output should be valid json");
    let barrier = first_write_body["snapshot"].clone();

    fixture.run_cli([
        "record",
        "put",
        "colors",
        "--input",
        second_input
            .to_str()
            .expect("second input path should be utf8"),
    ]);

    let barrier_generation = barrier["manifest_generation"]
        .as_u64()
        .expect("barrier generation should be numeric")
        .to_string();
    let barrier_seq = barrier["visible_seq_no"]
        .as_u64()
        .expect("barrier visible seq should be numeric")
        .to_string();

    let query = fixture.run_cli_json(&[
        "query",
        "colors",
        "--top-k",
        "2",
        "--vector",
        "1.0,0.0",
        "--read-barrier-manifest-generation",
        &barrier_generation,
        "--read-barrier-visible-seq-no",
        &barrier_seq,
    ]);
    let query_stdout = String::from_utf8(query.stdout).expect("stdout should be utf8");
    let query_body: Value =
        serde_json::from_str(&query_stdout).expect("query output should be valid json");
    let query_response = query_response_body(&query_body);
    assert_eq!(query_response["snapshot"]["visible_seq_no"], 2);
    assert_eq!(query_response["hits"][0]["record"]["id"], "alpha");
    assert_eq!(query_response["hits"][1]["record"]["id"], "beta");

    let stats = fixture.run_cli_json(&[
        "collection",
        "stats",
        "colors",
        "--read-barrier-manifest-generation",
        &barrier_generation,
        "--read-barrier-visible-seq-no",
        &barrier_seq,
    ]);
    let stats_stdout = String::from_utf8(stats.stdout).expect("stdout should be utf8");
    let stats_body: Value =
        serde_json::from_str(&stats_stdout).expect("stats output should be valid json");
    assert_eq!(stats_body["visible_seq_no"], 2);
    assert_eq!(stats_body["live_record_count"], 2);
}

#[test]
fn profiled_query_surfaces_filtered_scan_diagnostics() {
    let fixture = TestServerFixture::spawn("cli-cooperative-filtered-ann");
    let input_path = fixture.temp_root.join("cooperative-records.jsonl");
    let records = (0..12)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            format!(
                r#"{{"id":"doc-{index}","vector":[{},0.0],"kind":"{kind}","version":{index}}}"#,
                index as f32 + 1.0
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&input_path, records).expect("jsonl input should be written");

    fixture.run_cli([
        "collection",
        "create",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);
    fixture.run_cli([
        "record",
        "put",
        "documents",
        "--input",
        input_path.to_str().expect("input path should be utf8"),
    ]);
    fixture.run_cli(["collection", "flush", "documents"]);

    let profiled_query = fixture.run_cli_json(&[
        "query",
        "documents",
        "--top-k",
        "2",
        "--where",
        "kind:eq:keep",
        "--explain",
        "profile",
        "--vector",
        "1.0,0.0",
    ]);
    let profiled_query_stdout =
        String::from_utf8(profiled_query.stdout).expect("stdout should be utf8");
    let profiled_query_body: Value =
        serde_json::from_str(&profiled_query_stdout).expect("query output should be valid json");
    let profiled_query_response = query_response_body(&profiled_query_body);
    assert_eq!(profiled_query_response["hits"][0]["record"]["id"], "doc-8");
    assert_eq!(profiled_query_response["hits"][1]["record"]["id"], "doc-4");
    // Twelve rows make a segment without SQ8 codes or a graph: an exact f32 scan of the
    // three rows the filter matches.
    assert_eq!(
        profiled_query_response["diagnostics"]["chosen_plan"],
        "predicate_first_exact"
    );
    assert!(
        profiled_query_response["diagnostics"]["planner_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("exact_f32"))
    );
    assert_eq!(
        profiled_query_response["diagnostics"]["estimated_selectivity"],
        Value::from(0.25)
    );
    assert_eq!(
        profiled_query_response["diagnostics"]["units_considered"],
        2
    );
    assert_eq!(profiled_query_response["diagnostics"]["units_pruned"], 0);
    assert_eq!(profiled_query_response["diagnostics"]["units_scanned"], 1);
    let candidates_before_filter =
        profiled_query_response["diagnostics"]["candidates_before_filter"]
            .as_u64()
            .expect("candidates before filter should be numeric");
    let candidates_after_filter = profiled_query_response["diagnostics"]["candidates_after_filter"]
        .as_u64()
        .expect("candidates after filter should be numeric");
    assert!(candidates_before_filter >= 2);
    assert!(candidates_after_filter >= 2);
    assert!(candidates_after_filter <= candidates_before_filter);
    assert!(profiled_query_response["diagnostics"]["fallback_reason"].is_string());
    assert_eq!(profiled_query_response["diagnostics"]["rerank_count"], 1);
    assert!(
        profiled_query_response["diagnostics"]["candidates_reranked"]
            .as_u64()
            .is_some_and(|count| count == candidates_after_filter)
    );
    assert!(
        profiled_query_response["diagnostics"]["candidates_merged"]
            .as_u64()
            .is_some_and(|count| count == candidates_after_filter)
    );
    assert!(
        profiled_query_response["diagnostics"]["unit_scan_mix"]["exact_f32"]
            .as_u64()
            .is_some_and(|count| count == 1)
    );
    assert_eq!(
        profiled_query_response["diagnostics"]["stage_timings"]["prefilter_micros"],
        Value::from(0)
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["planning_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["candidate_generation_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["postfilter_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["rerank_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        profiled_query_response["diagnostics"]["stage_timings"]["merge_micros"]
            .as_u64()
            .is_some()
    );
}

#[test]
fn typed_schema_commands_manage_collections_and_records() {
    let fixture = TestServerFixture::spawn("cli-typed-schema");
    let schema_path = fixture.temp_root.join("products.json");
    fs::write(
        &schema_path,
        r#"{
  "primary_key": {"name": "sku", "type": "int64"},
  "vectors": [{"name": "embedding", "dimensions": 2, "metric": "cosine"}],
  "fields": [{"name": "title", "type": "string", "nullable": false}]
}"#,
    )
    .expect("schema file should be written");
    let records_path = fixture.temp_root.join("products.jsonl");
    fs::write(
        &records_path,
        "{\"sku\":7,\"embedding\":[3.0,4.0],\"title\":\"lamp\",\"price\":12.5,\"color\":\"red\"}\n\
         {\"sku\":8,\"embedding\":[1.0,0.0],\"title\":\"desk\",\"price\":99.0}\n",
    )
    .expect("records file should be written");

    fixture.run_cli(["database", "put", "shop"]);
    let created = fixture.run_cli_json(&[
        "collection",
        "create",
        "products",
        "--database",
        "shop",
        "--schema",
        schema_path.to_str().expect("schema path should be utf8"),
    ]);
    let created: Value = serde_json::from_slice(&created.stdout).expect("create prints json");
    assert_eq!(created["schema"]["primary_key"]["type"], "int64");
    assert_eq!(created["schema"]["fields"][0]["index"], "inverted");

    let altered = fixture.run_cli_json(&[
        "collection",
        "alter",
        "shop/products",
        "--change",
        r#"{"add_field":{"name":"price","type":"float64"}}"#,
    ]);
    let altered: Value = serde_json::from_slice(&altered.stdout).expect("alter prints json");
    assert_eq!(altered["schema"]["schema_version"], 2);
    assert_eq!(altered["schema"]["fields"][1]["name"], "price");

    let listed = fixture.run_cli_json(&["collection", "list", "--database", "shop"]);
    let listed: Value = serde_json::from_slice(&listed.stdout).expect("list prints json");
    assert_eq!(listed[0]["name"], "products");
    assert_eq!(listed[0]["schema"]["schema_version"], 2);

    fixture.run_cli([
        "record",
        "put",
        "shop/products",
        "--input",
        records_path.to_str().expect("records path should be utf8"),
    ]);

    let fetched = fixture.run_cli_json(&["record", "get", "shop/products", "7", "9"]);
    let fetched: Value = serde_json::from_slice(&fetched.stdout).expect("get prints json");
    assert_eq!(
        fetched["records"],
        json!([{
            "sku": 7,
            "embedding": [0.6, 0.8],
            "title": "lamp",
            "price": 12.5,
            "color": "red"
        }])
    );
    assert_eq!(fetched["missing_keys"], json!([9]));

    let projected = fixture.run_cli_json(&[
        "record",
        "get",
        "shop/products",
        "8",
        "--output-field",
        "price",
    ]);
    let projected: Value = serde_json::from_slice(&projected.stdout).expect("get prints json");
    assert_eq!(projected["records"], json!([{"sku": 8, "price": 99.0}]));

    let human = fixture.run_cli(["record", "get", "shop/products", "8"]);
    let human = String::from_utf8(human.stdout).expect("stdout should be utf8");
    assert!(human.contains("Found: 1"), "{human}");

    let refused = fixture.run_cli_expect_failure(["database", "drop", "shop"]);
    let refused = String::from_utf8(refused.stderr).expect("stderr should be utf8");
    assert!(refused.contains("FAILED_PRECONDITION"), "{refused}");

    fixture.run_cli(["collection", "drop", "shop/products"]);
    let listed = fixture.run_cli_json(&["collection", "list", "--database", "shop"]);
    let listed: Value = serde_json::from_slice(&listed.stdout).expect("list prints json");
    assert_eq!(listed, json!([]));
    fixture.run_cli(["database", "drop", "shop"]);
}
