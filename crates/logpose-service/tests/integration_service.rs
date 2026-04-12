//! Integration tests for `LogPoseDataService`.

use axum as _;
use axum::body::Body;
use http_body_util as _;
use http_body_util::BodyExt;
use logpose_api_grpc as _;
use logpose_api_grpc::proto;
use logpose_api_grpc::proto::log_pose_service_server::LogPoseService;
use logpose_api_rest as _;
use logpose_catalog as _;
use logpose_config as _;
use logpose_core as _;
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator, QueryRequest,
    ScalarMetadataValue,
};
use logpose_service::{LogPoseDataService, ServiceError};
use logpose_storage::CreateCollectionRequest;
use logpose_types::{DistanceMetric, PutRecord, RecordId, Snapshot, WriteOperation};
use rand as _;
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror as _;
use tonic as _;
use tonic::Request;
use tower as _;
use tower::util::ServiceExt;

#[tokio::test]
async fn service_runs_filtered_query_and_storage_workflow() {
    let root = unique_temp_dir("service-workflow");
    let service = LogPoseDataService::local(&root);

    let descriptor = service
        .create_collection(CreateCollectionRequest {
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");
    assert_eq!(descriptor.name, "documents");

    service
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"color":"red","kind":"keep"}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![3.0, 0.0],
                    metadata: json!({"color":"blue","kind":"drop"}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![2.0, 0.0],
                    metadata: json!({"color":"red","kind":"keep"}),
                }),
            ],
        )
        .await
        .expect("write should succeed");

    let snapshot = service
        .flush("documents")
        .await
        .expect("flush should succeed");

    let filtered = service
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 3,
            snapshot: Some(snapshot.clone()),
            filters: vec![MetadataFilter {
                field: "kind".to_owned(),
                value: ScalarMetadataValue::String("keep".to_owned()),
            }],
            predicate: None,
            explain: ExplainMode::None,
        })
        .await
        .expect("query should succeed");

    assert_eq!(filtered.snapshot, snapshot);
    assert_eq!(
        filtered
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["gamma", "alpha"]
    );

    let stats = service
        .stats("documents")
        .await
        .expect("stats should succeed");
    assert_eq!(stats.manifest_generation, 1);
    assert_eq!(stats.visible_seq_no, 3);
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.live_record_count, 3);
    assert_eq!(stats.mutable_op_count, 0);
    assert_eq!(stats.deleted_record_count, 0);

    let report = service
        .inspect_manifest("documents")
        .await
        .expect("inspect should succeed");
    assert_eq!(report.target, "manifest");

    let compacted = service
        .compact("documents")
        .await
        .expect("compact should succeed");
    assert!(compacted.manifest_generation >= snapshot.manifest_generation);
}

#[tokio::test]
async fn service_rejects_impossible_snapshots() {
    let root = unique_temp_dir("service-invalid-snapshot");
    let service = LogPoseDataService::local(&root);

    service
        .create_collection(CreateCollectionRequest {
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    service
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("write should succeed");

    let error = service
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: Some(Snapshot {
                manifest_generation: 0,
                visible_seq_no: 99,
            }),
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
        })
        .await
        .expect_err("invalid snapshot should error");

    assert!(matches!(
        error,
        ServiceError::InvalidArgument(message) if message.contains("invalid snapshot")
    ));
}

#[tokio::test]
async fn service_rejects_snapshots_below_manifest_checkpoint() {
    let root = unique_temp_dir("service-below-checkpoint-snapshot");
    let service = LogPoseDataService::local(&root);

    service
        .create_collection(CreateCollectionRequest {
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    service
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("write should succeed");

    let flushed = service
        .flush("documents")
        .await
        .expect("flush should succeed");

    let error = service
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: Some(Snapshot {
                manifest_generation: flushed.manifest_generation,
                visible_seq_no: flushed.visible_seq_no - 1,
            }),
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
        })
        .await
        .expect_err("below-checkpoint snapshot should error");

    assert!(matches!(
        error,
        ServiceError::InvalidArgument(message) if message.contains("invalid snapshot")
    ));
}

#[tokio::test]
async fn service_rest_and_grpc_queries_share_profile_diagnostics() {
    let state = Arc::new(logpose_core::AppState::new(test_config(
        "service-query-diagnostics",
    )));
    let rest = logpose_api_rest::router(Arc::clone(&state));
    let grpc = logpose_api_grpc::GrpcLogPoseService::new(Arc::clone(&state));

    state
        .service
        .create_collection(CreateCollectionRequest {
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    state
        .service
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep","version":1}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![2.0, 0.0],
                    metadata: json!({"kind":"drop","version":2}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![5.0, 0.0],
                    metadata: json!({"kind":"keep","version":3}),
                }),
            ],
        )
        .await
        .expect("write should succeed");

    let predicate = Predicate::Comparison(PredicateComparison {
        field: "kind".to_owned(),
        operator: PredicateOperator::Eq,
        value: Some(ScalarMetadataValue::String("keep".to_owned())),
    });

    let service_response = state
        .service
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(predicate.clone()),
            explain: ExplainMode::Profile,
        })
        .await
        .expect("service query should succeed");

    let rest_response = rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/collections/documents/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "vector": [1.0, 0.0],
                        "top_k": 1,
                        "predicate": {
                            "kind": "comparison",
                            "field": "kind",
                            "operator": "eq",
                            "value": "keep"
                        },
                        "explain": "profile"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("rest query should respond");
    let rest_body = serde_json::from_slice::<Value>(
        &rest_response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes(),
    )
    .expect("body should be json");

    let grpc_response = grpc
        .query_collection(Request::new(proto::QueryCollectionRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(proto::Predicate {
                node: Some(proto::predicate::Node::Comparison(
                    proto::PredicateComparison {
                        field: "kind".to_owned(),
                        operator: proto::PredicateOperator::Eq as i32,
                        value: Some(proto::ScalarValue {
                            kind: Some(proto::scalar_value::Kind::StringValue("keep".to_owned())),
                        }),
                    },
                )),
            }),
            explain: proto::ExplainMode::Profile as i32,
        }))
        .await
        .expect("grpc query should succeed")
        .into_inner();

    assert_eq!(service_response.matches[0].id.as_str(), "gamma");
    assert_eq!(rest_body["matches"][0]["id"], "gamma");
    assert_eq!(grpc_response.matches[0].id, "gamma");
    assert!(
        service_response
            .diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.stage_timings.as_ref())
            .is_some()
    );
    assert!(rest_body["diagnostics"]["stage_timings"].is_object());
    assert!(
        grpc_response
            .diagnostics
            .as_ref()
            .and_then(|diagnostics| diagnostics.stage_timings.as_ref())
            .is_some()
    );
}

#[tokio::test]
async fn service_reports_stats_and_inspect_targets_for_maintenance_workflows() {
    let root = unique_temp_dir("service-inspect");
    let service = LogPoseDataService::local(&root);

    service
        .create_collection(CreateCollectionRequest {
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    service
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"version":1}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: json!({"version":1}),
                }),
            ],
        )
        .await
        .expect("write should succeed");
    service
        .flush("documents")
        .await
        .expect("flush should succeed");
    service
        .write(
            "documents",
            vec![WriteOperation::Delete(logpose_types::DeleteRecord {
                id: RecordId::new("alpha"),
            })],
        )
        .await
        .expect("delete should succeed");

    let stats = service
        .stats("documents")
        .await
        .expect("stats should succeed");
    assert_eq!(stats.manifest_generation, 1);
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 1);
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(stats.deleted_record_count, 1);

    let manifest = service
        .inspect_manifest("documents")
        .await
        .expect("manifest inspect should succeed");
    assert_eq!(manifest.target, "manifest");
    let manifest_segments = manifest
        .payload
        .get("segments")
        .and_then(Value::as_array)
        .expect("manifest segments should be an array");
    assert_eq!(manifest_segments.len(), 1);
    let segment_id = manifest_segments[0]["segment_id"]
        .as_str()
        .expect("segment id should be a string")
        .to_owned();

    let wal = service
        .inspect_wal("documents")
        .await
        .expect("wal inspect should succeed");
    assert_eq!(wal.target, "wal");
    assert_eq!(
        wal.payload
            .get("records")
            .and_then(Value::as_array)
            .expect("wal records should be an array")
            .len(),
        1
    );

    let segment = service
        .inspect_segment("documents", segment_id.clone())
        .await
        .expect("segment inspect should succeed");
    assert_eq!(segment.target, format!("segment:{segment_id}"));
    assert_eq!(
        segment
            .payload
            .get("segment")
            .and_then(Value::as_object)
            .and_then(|segment| segment.get("segment_id"))
            .and_then(Value::as_str),
        Some(segment_id.as_str())
    );
    assert_eq!(
        segment
            .payload
            .get("records")
            .and_then(Value::as_array)
            .expect("segment records should be an array")
            .len(),
        2
    );
}

#[tokio::test]
async fn service_maps_missing_collections_to_not_found() {
    let root = unique_temp_dir("service-missing");
    let service = LogPoseDataService::local(&root);

    let error = service
        .get_collection("missing")
        .await
        .expect_err("missing collection should error");

    assert!(matches!(error, ServiceError::NotFound(message) if message.contains("missing")));
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-service-{label}-{suffix}"));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}

fn test_config(label: &str) -> logpose_config::LogPoseConfig {
    logpose_config::LogPoseConfig {
        node_name: label.to_owned(),
        storage_root: unique_temp_dir(label),
        ..logpose_config::LogPoseConfig::default()
    }
}
