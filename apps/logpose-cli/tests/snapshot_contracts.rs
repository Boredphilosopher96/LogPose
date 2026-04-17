//! Snapshot-style operator contract tests for the CLI.

use clap as _;
use crossterm as _;
use insta::assert_json_snapshot;
use logpose_catalog as _;
use logpose_cli as _;
use logpose_client as _;
use logpose_query as _;
use logpose_storage as _;
use logpose_telemetry as _;
use logpose_types as _;
use ratatui as _;
use serde as _;
use serde_json::{Map, Value, json};
use std::fs;
use walkdir as _;

#[path = "support/server_fixture.rs"]
mod support;

use support::TestServerFixture;

#[test]
fn diagnostics_status_snapshot_contract() {
    let fixture = TestServerFixture::spawn("cli-snapshot-status");

    let status = normalize_status(cli_json(&fixture, ["status"]));

    assert_json_snapshot!("diagnostics_status", status);
}

#[test]
fn diagnostics_placement_snapshot_contract() {
    let fixture = TestServerFixture::spawn("cli-snapshot-placement");

    fixture.run_cli([
        "collection",
        "create",
        "documents",
        "--dimensions",
        "2",
        "--metric",
        "dot",
    ]);

    let placement =
        normalize_placement(cli_json(&fixture, ["collection", "placement", "documents"]));

    assert_json_snapshot!("diagnostics_placement", placement);
}

#[test]
fn query_and_inspect_snapshot_contract() {
    let fixture = TestServerFixture::spawn("cli-snapshot-query-inspect");
    let input_path = fixture.temp_root.join("records.jsonl");
    fs::write(
        &input_path,
        r#"{"id":"alpha","vector":[1.0,0.0],"metadata":{"color":"red","kind":"keep"}}
{"id":"beta","vector":[0.5,0.0],"metadata":{"color":"green","kind":"drop"}}
{"id":"gamma","vector":[0.8,0.0],"metadata":{"color":"blue","kind":"keep"}}"#,
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
        input_path.to_str().expect("input path should be utf8"),
    ]);

    let explain = normalize_query(cli_json(
        &fixture,
        [
            "query",
            "colors",
            "--top-k",
            "1",
            "--where",
            "kind:eq:keep",
            "--explain",
            "plan",
            "--vector",
            "1.0,0.0",
        ],
    ));
    let wal = cli_json(&fixture, ["inspect", "wal", "colors"]);

    fixture.run_cli(["collection", "flush", "colors"]);

    let profile = normalize_query(cli_json(
        &fixture,
        [
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
        ],
    ));
    let manifest = cli_json(&fixture, ["inspect", "manifest", "colors"]);
    let segment_id = manifest["payload"]["segments"][0]["segment_id"]
        .as_str()
        .expect("segment id should exist")
        .to_owned();
    let segment = cli_json(
        &fixture,
        ["inspect", "segment", "colors", segment_id.as_str()],
    );

    let snapshot = json!({
        "explain_query": explain,
        "profile_query": profile,
        "wal": project_wal_contract(wal),
        "manifest": project_manifest_contract(manifest, &segment_id),
        "segment": project_segment_contract(segment, &segment_id),
    });

    assert_json_snapshot!("query_and_inspect", snapshot);
}

fn cli_json<const N: usize>(fixture: &TestServerFixture, args: [&str; N]) -> Value {
    let output = fixture.run_cli_json(&args);
    serde_json::from_slice(&output.stdout).expect("cli should print valid json")
}

fn normalize_status(mut value: Value) -> Value {
    value["metadata"]["version"] = json!("[version]");
    value["metadata"]["git_sha"] = json!("[git_sha]");
    value["rest_endpoint"] = json!("http://127.0.0.1:[rest-port]");
    value["grpc_endpoint"] = json!("http://127.0.0.1:[grpc-port]");
    value
}

fn normalize_placement(mut value: Value) -> Value {
    value["collection_id"] = json!("[collection_id]");
    value
}

fn normalize_query(value: Value) -> Value {
    let matches = value["matches"]
        .as_array()
        .expect("query matches should be an array")
        .iter()
        .map(|item| {
            json!({
                "id": item["id"],
                "metadata": item["metadata"],
                "value": item["value"],
            })
        })
        .collect::<Vec<_>>();

    let diagnostics = value
        .get("diagnostics")
        .and_then(Value::as_object)
        .map(|diagnostics| {
            let mut projected = json!({
                "chosen_plan": diagnostics["chosen_plan"],
                "units_considered": diagnostics["units_considered"],
                "units_pruned": diagnostics["units_pruned"],
                "units_scanned": diagnostics["units_scanned"],
            });
            if let Some(stage_timings) = diagnostics.get("stage_timings").and_then(Value::as_object)
            {
                projected["stage_timings"] = Value::Object(
                    stage_timings
                        .keys()
                        .map(|key| (key.clone(), json!("[timing]")))
                        .collect::<Map<String, Value>>(),
                );
            }
            projected
        });

    let mut projected = json!({
        "metric": value["metric"],
        "top_k": value["top_k"],
        "returned": value["returned"],
        "snapshot": value["snapshot"],
        "matches": matches,
    });
    if let Some(diagnostics) = diagnostics {
        projected["diagnostics"] = diagnostics;
    }

    projected
}

fn normalize_segment_scoped(mut value: Value, segment_id: &str) -> Value {
    replace_identifier_strings(&mut value, segment_id, "[segment_id]");
    value
}

fn project_manifest_contract(value: Value, segment_id: &str) -> Value {
    let manifest = normalize_segment_scoped(value, segment_id);
    json!({
        "target": manifest["target"],
        "payload": {
            "generation": manifest["payload"]["generation"],
            "checkpoint_seq_no": manifest["payload"]["checkpoint_seq_no"],
            "segments": manifest["payload"]["segments"]
                .as_array()
                .expect("manifest segments should be an array")
                .iter()
                .map(|segment| {
                    json!({
                        "segment_id": segment["segment_id"],
                        "index_kind": segment["index_kind"],
                        "dimensions": segment["dimensions"],
                        "put_count": segment["put_count"],
                        "delete_count": segment["delete_count"],
                    })
                })
                .collect::<Vec<_>>(),
        }
    })
}

fn project_segment_contract(value: Value, segment_id: &str) -> Value {
    let segment = normalize_segment_scoped(value, segment_id);
    json!({
        "target": segment["target"],
        "payload": {
            "segment": {
                "segment_id": segment["payload"]["segment"]["segment_id"],
                "index_kind": segment["payload"]["segment"]["index_kind"],
                "dimensions": segment["payload"]["segment"]["dimensions"],
                "put_count": segment["payload"]["segment"]["put_count"],
                "delete_count": segment["payload"]["segment"]["delete_count"],
            },
            "flat_index": {
                "segment_id": segment["payload"]["flat_index"]["segment_id"],
                "index_kind": segment["payload"]["flat_index"]["index_kind"],
                "entry_count": segment["payload"]["flat_index"]["entry_count"],
                "put_count": segment["payload"]["flat_index"]["put_count"],
                "delete_count": segment["payload"]["flat_index"]["delete_count"],
            },
            "hnsw_index": {
                "index_kind": segment["payload"]["hnsw_index"]["index_kind"],
                "dimensions": segment["payload"]["hnsw_index"]["dimensions"],
                "node_count": segment["payload"]["hnsw_index"]["node_count"],
            },
            "records": project_records(&segment["payload"]["records"]),
        }
    })
}

fn project_wal_contract(value: Value) -> Value {
    json!({
        "target": value["target"],
        "payload": {
            "checkpoint_seq_no": value["payload"]["checkpoint_seq_no"],
            "records": project_records(&value["payload"]["records"]),
        }
    })
}

fn project_records(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .expect("records should be an array")
        .iter()
        .map(|record| {
            json!({
                "seq_no": record["seq_no"],
                "op": record["op"]["op"],
                "id": record["op"]["id"],
            })
        })
        .collect()
}

fn replace_identifier_strings(value: &mut Value, before: &str, after: &str) {
    match value {
        Value::String(string) => {
            if string.contains(before) {
                *string = string.replace(before, after);
            }
        }
        Value::Array(values) => {
            for item in values {
                replace_identifier_strings(item, before, after);
            }
        }
        Value::Object(values) => {
            for item in values.values_mut() {
                replace_identifier_strings(item, before, after);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}
