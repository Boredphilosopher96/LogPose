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
use serde_json::Value;
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
            "authentication_mode": AuthenticationMode::Password,
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
    assert!(set_stdout.contains("password"));
    assert!(set_stdout.contains("writer"));
    assert!(set_stdout.contains("read_write"));

    let show = fixture.run_cli_json(&["database", "policy", "show"]);
    let show_stdout = String::from_utf8(show.stdout).expect("stdout should be utf8");
    let show_body: Value =
        serde_json::from_str(&show_stdout).expect("policy output should be valid json");

    assert_eq!(show_body["database_name"], "default");
    assert_eq!(show_body["authentication_mode"], "password");
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
        r#"{"id":"alpha","vector":[1.0,0.0],"metadata":{"color":"red","kind":"keep"}}
{"id":"beta","vector":[0.5,0.0],"metadata":{"color":"green","kind":"drop"}}
{"id":"gamma","vector":[0.8,0.0],"metadata":{"color":"blue","kind":"keep"}}"#,
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
    assert_eq!(get_body["metric"], "dot");

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
    let matches = query_response["matches"]
        .as_array()
        .expect("matches should be an array");
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0]["id"], "alpha");
    assert_eq!(matches[1]["id"], "gamma");

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
    assert_eq!(profiled_query_response["matches"][0]["id"], "alpha");
    assert!(profiled_query_response["diagnostics"].is_object());
    assert!(profiled_query_response["diagnostics"]["stage_timings"].is_object());
    assert_eq!(
        profiled_query_response["diagnostics"]["chosen_plan"],
        "vector_first_exact"
    );
    assert!(
        profiled_query_response["diagnostics"]["candidates_reranked"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        profiled_query_response["diagnostics"]["candidates_merged"]
            .as_u64()
            .is_some_and(|count| count >= 1)
    );
    assert!(
        profiled_query_response["diagnostics"]["unit_scan_mix"]["mutable_exact"]
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
            .is_some_and(|artifacts| artifacts.len() >= 2)
    );
    assert!(
        immutable_unit["component_bytes"]["ann_graph"]
            .as_u64()
            .is_some()
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
    assert_eq!(segment_response["payload"]["segment"]["index_kind"], "hnsw");
    assert_eq!(
        segment_response["payload"]["artifacts"]
            .as_array()
            .expect("segment artifacts should be an array")
            .len(),
        2
    );
    assert!(
        segment_response["payload"]["hnsw_index"]["node_count"]
            .as_u64()
            .is_some_and(|count| count >= 3)
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
    assert_eq!(ann_query_response["matches"][0]["id"], "alpha");
    assert_eq!(
        ann_query_response["diagnostics"]["chosen_plan"],
        "vector_first_ann"
    );
    assert!(
        ann_query_response["diagnostics"]["unit_scan_mix"]["immutable_ann"]
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
    assert_eq!(profiled_query_response["matches"][0]["id"], "doc-8");
    assert_eq!(profiled_query_response["matches"][1]["id"], "doc-4");
    assert_eq!(
        profiled_query_response["diagnostics"]["chosen_plan"],
        "cooperative_filtered_ann"
    );
    assert_eq!(
        profiled_query_response["diagnostics"]["planner_reason"],
        "filtered ann traversal is cheaper than exact scan for this selectivity"
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
    assert_eq!(
        profiled_query_response["diagnostics"]["fallback_reason"],
        Value::Null
    );
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
        profiled_query_response["diagnostics"]["unit_scan_mix"]["immutable_ann"]
            .as_u64()
            .is_some_and(|count| count >= 1)
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
