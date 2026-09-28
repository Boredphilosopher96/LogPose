//! Integration tests for `LogPoseDataService`.

use async_trait as _;
use axum as _;
use axum::body::Body;
use http_body_util as _;
use http_body_util::BodyExt;
use logpose_api_grpc as _;
use logpose_api_grpc::proto;
use logpose_api_grpc::proto::log_pose_service_server::LogPoseService;
use logpose_api_rest as _;
use logpose_auth as _;
use logpose_catalog as _;
use logpose_config as _;
use logpose_core::RequestAuth;
use logpose_query::{
    ExplainMode, FilterComparison, FilterExpr, FilterOperator, MetadataFilter, QueryPlanKind,
    QueryRequest, ScalarMetadataValue,
};
use logpose_service::LogPoseDataService;
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_storage_etcd as _;
use logpose_types::{
    DistanceMetric, LogPoseError, PutRecord, RecordId, ResourceKind, Snapshot,
    legacy::record_from_put,
    record::{PrimaryKey, Record},
};
use rand as _;
use serde as _;
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
    let service = LogPoseDataService::local(&root).expect("data service should open");

    let descriptor = service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");
    assert_eq!(descriptor.name, "documents");
    let placement: logpose_types::CollectionAssignment = serde_json::from_slice(
        &fs::read(descriptor.root_path.join("placement.json"))
            .expect("placement assignment should be written"),
    )
    .expect("placement assignment should parse");
    assert_eq!(placement.assigned_node, "local");
    assert_eq!(placement.assigned_role, logpose_types::NodeRole::Data);

    service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"color":"red","kind":"keep"}),
                })
                .expect("record"),
                record_from_put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![3.0, 0.0],
                    metadata: json!({"color":"blue","kind":"drop"}),
                })
                .expect("record"),
                record_from_put(PutRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![2.0, 0.0],
                    metadata: json!({"color":"red","kind":"keep"}),
                })
                .expect("record"),
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
            read_barrier: None,
            filters: vec![MetadataFilter {
                field: "kind".to_owned(),
                value: ScalarMetadataValue::String("keep".to_owned()),
            }],
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
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
async fn service_write_ack_returns_immediate_read_snapshot() {
    let root = unique_temp_dir("service-write-ack-session-snapshot");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let first_ack = service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("first write should succeed");

    assert_eq!(first_ack.last_seq_no, 1);
    assert_eq!(first_ack.snapshot.manifest_generation, 0);
    assert_eq!(first_ack.snapshot.visible_seq_no, 1);

    let second_ack = service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("second write should succeed");

    assert_eq!(second_ack.last_seq_no, 2);
    assert_eq!(second_ack.snapshot.manifest_generation, 0);
    assert_eq!(second_ack.snapshot.visible_seq_no, 2);

    let response = service
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: Some(second_ack.snapshot.clone()),
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect("query at write ack snapshot should succeed");

    assert_eq!(response.snapshot, second_ack.snapshot);
    assert_eq!(
        response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );

    let first_stats = service
        .stats_at_snapshot("documents", first_ack.snapshot.clone())
        .await
        .expect("stats at first write snapshot should succeed");
    assert_eq!(first_stats.visible_seq_no, 1);
    assert_eq!(first_stats.live_record_count, 1);

    let second_stats = service
        .stats_at_snapshot("documents", second_ack.snapshot.clone())
        .await
        .expect("stats at second write snapshot should succeed");
    assert_eq!(second_stats.visible_seq_no, 2);
    assert_eq!(second_stats.live_record_count, 2);
}

#[tokio::test]
async fn write_ack_snapshot_is_exact_until_a_flush_supersedes_its_generation() {
    let root = unique_temp_dir("service-write-ack-snapshot-after-rotation");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let ack = service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("write should succeed");
    service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![2.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("write should succeed");

    let request = QueryRequest {
        collection_name: "documents".to_owned(),
        vector: vec![1.0, 0.0],
        top_k: 2,
        snapshot: Some(ack.snapshot.clone()),
        read_barrier: None,
        filters: Vec::new(),
        predicate: None,
        explain: ExplainMode::None,
        snapshot_token: None,
        pin: false,
    };
    let response = service
        .query(request.clone())
        .await
        .expect("a snapshot of the current generation is exact");
    assert_eq!(response.snapshot, ack.snapshot);
    assert_eq!(
        response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha"]
    );

    service
        .flush("documents")
        .await
        .expect("flush should succeed");

    let error = service
        .query(request)
        .await
        .expect_err("nothing pins the superseded generation");
    assert!(
        matches!(&error, LogPoseError::SnapshotExpired { .. }),
        "{error:?}"
    );
    let error = service
        .stats_at_snapshot("documents", ack.snapshot.clone())
        .await
        .expect_err("stats of the superseded generation expire too");
    assert!(
        matches!(&error, LogPoseError::SnapshotExpired { .. }),
        "{error:?}"
    );
    // As a read barrier, the ack is still satisfied: barriers compare positions, not files.
    let response = service
        .query(QueryRequest {
            snapshot: None,
            read_barrier: Some(ack.snapshot),
            top_k: 2,
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect("the barrier is satisfied");
    assert_eq!(response.matches.len(), 2);
}

#[tokio::test]
async fn service_query_read_barrier_advances_to_latest_snapshot() {
    let root = unique_temp_dir("service-query-read-barrier-advances");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let ack = service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("write should succeed");

    let flushed = service
        .flush("documents")
        .await
        .expect("flush should succeed");

    let response = service
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            read_barrier: Some(ack.snapshot.clone()),
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect("read barrier should advance to the latest visible snapshot");

    assert_eq!(response.snapshot, flushed);
    assert_eq!(
        response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha"]
    );
}

#[tokio::test]
async fn service_rejects_unsatisfied_query_read_barrier() {
    let root = unique_temp_dir("service-query-read-barrier-unsatisfied");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("write should succeed");

    let error = service
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            read_barrier: Some(Snapshot {
                manifest_generation: 0,
                visible_seq_no: 2,
            }),
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect_err("barrier above the current snapshot should fail");

    assert!(matches!(
        error,
        LogPoseError::ReadBarrierNotSatisfied { .. }
    ));
}

#[tokio::test]
async fn service_rejects_query_snapshot_and_read_barrier_conflicts() {
    let root = unique_temp_dir("service-query-read-barrier-conflict");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let error = service
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: Some(Snapshot {
                manifest_generation: 0,
                visible_seq_no: 0,
            }),
            read_barrier: Some(Snapshot {
                manifest_generation: 0,
                visible_seq_no: 0,
            }),
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect_err("snapshot and read barrier should conflict");

    assert!(matches!(
        error,
        LogPoseError::InvalidArgument { message, .. }
            if message.contains("snapshot and read_barrier cannot be provided together")
    ));
}

#[tokio::test]
async fn service_stats_read_barrier_advances_to_latest_snapshot() {
    let root = unique_temp_dir("service-stats-read-barrier-advances");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let ack = service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("write should succeed");

    let flushed = service
        .flush("documents")
        .await
        .expect("flush should succeed");

    let stats = service
        .stats_for_read("documents", None, Some(ack.snapshot))
        .await
        .expect("read barrier should advance stats to the latest visible snapshot");

    assert_eq!(stats.manifest_generation, flushed.manifest_generation);
    assert_eq!(stats.visible_seq_no, flushed.visible_seq_no);
    assert_eq!(stats.live_record_count, 1);
}

#[tokio::test]
async fn service_rejects_unsatisfied_stats_read_barrier() {
    let root = unique_temp_dir("service-stats-read-barrier-unsatisfied");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("write should succeed");

    let error = service
        .stats_for_read(
            "documents",
            None,
            Some(Snapshot {
                manifest_generation: 0,
                visible_seq_no: 2,
            }),
        )
        .await
        .expect_err("barrier above the current stats snapshot should fail");

    assert!(matches!(
        error,
        LogPoseError::ReadBarrierNotSatisfied { .. }
    ));
}

#[tokio::test]
async fn service_rejects_impossible_snapshots() {
    let root = unique_temp_dir("service-invalid-snapshot");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
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
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect_err("invalid snapshot should error");

    assert!(matches!(
        error,
        LogPoseError::InvalidArgument { field: Some(field), message }
            if field == "snapshot" && message.contains("invalid snapshot")
    ));
}

#[tokio::test]
async fn service_rejects_snapshots_below_manifest_checkpoint() {
    let root = unique_temp_dir("service-below-checkpoint-snapshot");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
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
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect_err("below-checkpoint snapshot should error");

    assert!(matches!(
        error,
        LogPoseError::InvalidArgument { field: Some(field), message }
            if field == "snapshot" && message.contains("invalid snapshot")
    ));
}

#[tokio::test]
async fn app_state_accepts_database_qualified_collection_references() {
    let state = Arc::new(logpose_core::AppState::new(test_config(
        "service-qualified-default-namespace",
    )));

    state
        .control
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    state
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "default/documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("qualified write should succeed");

    let placement = state
        .control
        .collection_placement("default/documents")
        .await
        .expect("qualified placement lookup should succeed");
    let snapshot = state
        .snapshot("default/documents")
        .await
        .expect("qualified snapshot should succeed");
    let stats = state
        .stats("default/documents")
        .await
        .expect("qualified stats should succeed");
    let inspect = state
        .inspect("default/documents", InspectTarget::Manifest)
        .await
        .expect("qualified inspect should succeed");
    let query = state
        .query(QueryRequest {
            collection_name: "default/documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect("qualified query should succeed");

    assert_eq!(placement.collection_name, "documents");
    assert_eq!(placement.database_name, "default");
    assert_eq!(placement.route_kind, "local");
    assert_eq!(snapshot.visible_seq_no, 1);
    assert_eq!(stats.database_name, "default");
    assert_eq!(stats.collection_name, "documents");
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(inspect.target, "manifest");
    assert_eq!(query.returned, 1);
    assert_eq!(query.matches[0].id.as_str(), "alpha");
}

#[tokio::test]
async fn service_rest_and_grpc_queries_share_profile_diagnostics() {
    let state = Arc::new(logpose_core::AppState::new(test_config(
        "service-query-diagnostics",
    )));
    let rest = logpose_api_rest::router(Arc::clone(&state));
    let grpc = logpose_api_grpc::GrpcLogPoseService::new(Arc::clone(&state));

    state
        .control
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    state
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep","version":1}),
                })
                .expect("record"),
                record_from_put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![2.0, 0.0],
                    metadata: json!({"kind":"drop","version":2}),
                })
                .expect("record"),
                record_from_put(PutRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![5.0, 0.0],
                    metadata: json!({"kind":"keep","version":3}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("write should succeed");

    state
        .flush("documents")
        .await
        .expect("flush should succeed");

    let predicate = FilterExpr::Comparison(FilterComparison {
        field: "kind".to_owned(),
        operator: FilterOperator::Eq,
        value: Some(ScalarMetadataValue::String("keep".to_owned())),
    });

    let service_response = state
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: Some(predicate.clone()),
            explain: ExplainMode::Profile,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect("service query should succeed");

    let rest_response = rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v2/databases/default/collections/documents/query")
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
            read_barrier: None,
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
            database_name: "default".to_owned(),
            snapshot_token: String::new(),
            pin: false,
        }))
        .await
        .expect("grpc query should succeed")
        .into_inner();

    assert_eq!(service_response.matches[0].id.as_str(), "gamma");
    assert_eq!(rest_body["matches"][0]["id"], "gamma");
    assert_eq!(grpc_response.matches[0].id, "gamma");
    let service_diagnostics = service_response
        .diagnostics
        .as_ref()
        .expect("service diagnostics should be present");
    let grpc_diagnostics = grpc_response
        .diagnostics
        .as_ref()
        .expect("grpc diagnostics should be present");
    assert_eq!(
        service_diagnostics.chosen_plan,
        QueryPlanKind::PredicateFirstExact
    );
    assert_eq!(
        rest_body["diagnostics"]["chosen_plan"],
        "predicate_first_exact"
    );
    assert_eq!(
        proto::QueryPlanKind::try_from(grpc_diagnostics.chosen_plan)
            .expect("chosen plan should decode"),
        proto::QueryPlanKind::PredicateFirstExact
    );
    assert_eq!(
        service_diagnostics.candidates_reranked as u64,
        rest_body["diagnostics"]["candidates_reranked"]
            .as_u64()
            .expect("rest rerank count should be numeric")
    );
    assert_eq!(
        service_diagnostics.candidates_merged as u64,
        rest_body["diagnostics"]["candidates_merged"]
            .as_u64()
            .expect("rest merge count should be numeric")
    );
    assert_eq!(
        service_diagnostics.candidates_reranked as u64,
        grpc_diagnostics.candidates_reranked
    );
    assert_eq!(
        service_diagnostics.candidates_merged as u64,
        grpc_diagnostics.candidates_merged
    );
    assert!(service_diagnostics.fallback_reason.is_some());
    assert_eq!(
        rest_body["diagnostics"]["fallback_reason"].as_str(),
        service_diagnostics.fallback_reason.as_deref()
    );
    assert_eq!(
        grpc_diagnostics.fallback_reason,
        service_diagnostics.fallback_reason
    );
    assert_eq!(
        service_diagnostics.unit_scan_mix.get("exact_f32").copied(),
        Some(1)
    );
    assert_eq!(
        rest_body["diagnostics"]["unit_scan_mix"]["exact_f32"],
        Value::from(1)
    );
    assert_eq!(grpc_diagnostics.unit_scan_mix.get("exact_f32"), Some(&1));
    let service_timings = service_diagnostics
        .stage_timings
        .as_ref()
        .expect("service timings should be present");
    assert_eq!(service_timings.prefilter_micros, 0);
    assert_eq!(service_timings.merge_micros, 0);
    assert!(rest_body["diagnostics"]["stage_timings"].is_object());
    assert!(
        rest_body["diagnostics"]["stage_timings"]["planning_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        rest_body["diagnostics"]["stage_timings"]["prefilter_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        rest_body["diagnostics"]["stage_timings"]["candidate_generation_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        rest_body["diagnostics"]["stage_timings"]["postfilter_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        rest_body["diagnostics"]["stage_timings"]["rerank_micros"]
            .as_u64()
            .is_some()
    );
    assert!(
        rest_body["diagnostics"]["stage_timings"]["merge_micros"]
            .as_u64()
            .is_some()
    );
    let grpc_timings = grpc_diagnostics
        .stage_timings
        .as_ref()
        .expect("grpc timings should be present");
    assert_eq!(grpc_timings.prefilter_micros, 0);
    assert_eq!(grpc_timings.merge_micros, 0);
}

#[tokio::test]
async fn service_rest_and_grpc_surface_filtered_segment_scans() {
    let state = Arc::new(logpose_core::AppState::new(test_config(
        "service-filtered-segment-scan",
    )));
    let rest = logpose_api_rest::router(Arc::clone(&state));
    let grpc = logpose_api_grpc::GrpcLogPoseService::new(Arc::clone(&state));

    state
        .control
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let records = (0..12)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            record_from_put(PutRecord {
                id: RecordId::new(format!("doc-{index}")),
                vector: vec![index as f32 + 1.0, 0.0],
                metadata: json!({"kind":kind,"version":index}),
            })
            .expect("record")
        })
        .collect::<Vec<_>>();
    state
        .upsert_records_with_auth(&RequestAuth::default(), "documents", records)
        .await
        .expect("write should succeed");
    state
        .flush("documents")
        .await
        .expect("flush should succeed");

    let predicate = FilterExpr::Comparison(FilterComparison {
        field: "kind".to_owned(),
        operator: FilterOperator::Eq,
        value: Some(ScalarMetadataValue::String("keep".to_owned())),
    });

    let service_response = state
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: Some(predicate.clone()),
            explain: ExplainMode::Profile,
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect("service query should succeed");
    let rest_response = rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v2/databases/default/collections/documents/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "vector": [1.0, 0.0],
                        "top_k": 2,
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
            top_k: 2,
            snapshot: None,
            read_barrier: None,
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
            database_name: "default".to_owned(),
            snapshot_token: String::new(),
            pin: false,
        }))
        .await
        .expect("grpc query should succeed")
        .into_inner();

    assert_eq!(
        service_response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["doc-8", "doc-4"]
    );
    assert_eq!(
        rest_body["matches"]
            .as_array()
            .expect("rest matches should be an array")
            .iter()
            .map(|candidate| candidate["id"].as_str().expect("id should be string"))
            .collect::<Vec<_>>(),
        vec!["doc-8", "doc-4"]
    );
    assert_eq!(
        grpc_response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["doc-8", "doc-4"]
    );
    let diagnostics = service_response
        .diagnostics
        .as_ref()
        .expect("service diagnostics should be present");
    // Twelve rows make a segment without SQ8 codes or a graph: an exact f32 scan of the
    // three rows the filter matches.
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::PredicateFirstExact);
    assert!(diagnostics.planner_reason.contains("exact_f32"));
    assert!((diagnostics.estimated_selectivity - 0.25).abs() <= f32::EPSILON);
    assert!(diagnostics.units_considered >= 1);
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 1);
    assert!(diagnostics.candidates_before_filter >= service_response.returned);
    assert!(diagnostics.candidates_after_filter >= service_response.returned);
    assert!(diagnostics.candidates_after_filter <= diagnostics.candidates_before_filter);
    assert!(diagnostics.candidates_merged >= service_response.returned);
    assert_eq!(diagnostics.rerank_count, 1);
    assert_eq!(
        rest_body["diagnostics"]["chosen_plan"],
        "predicate_first_exact"
    );
    assert_eq!(
        proto::QueryPlanKind::try_from(
            grpc_response
                .diagnostics
                .as_ref()
                .expect("grpc diagnostics should be present")
                .chosen_plan
        )
        .expect("chosen plan should decode"),
        proto::QueryPlanKind::PredicateFirstExact
    );
    let grpc_diagnostics = grpc_response
        .diagnostics
        .as_ref()
        .expect("grpc diagnostics should be present");
    assert_eq!(
        diagnostics.planner_reason,
        rest_body["diagnostics"]["planner_reason"]
            .as_str()
            .expect("rest planner reason should be a string")
    );
    assert!(
        (diagnostics.estimated_selectivity
            - rest_body["diagnostics"]["estimated_selectivity"]
                .as_f64()
                .expect("rest selectivity should be numeric") as f32)
            .abs()
            <= f32::EPSILON
    );
    assert_eq!(
        diagnostics.units_considered as u64,
        rest_body["diagnostics"]["units_considered"]
            .as_u64()
            .expect("rest units considered should be numeric")
    );
    assert_eq!(
        diagnostics.units_pruned as u64,
        rest_body["diagnostics"]["units_pruned"]
            .as_u64()
            .expect("rest units pruned should be numeric")
    );
    assert_eq!(
        diagnostics.units_scanned as u64,
        rest_body["diagnostics"]["units_scanned"]
            .as_u64()
            .expect("rest units scanned should be numeric")
    );
    assert_eq!(
        diagnostics.candidates_before_filter as u64,
        rest_body["diagnostics"]["candidates_before_filter"]
            .as_u64()
            .expect("rest candidate count should be numeric")
    );
    assert_eq!(
        diagnostics.candidates_after_filter as u64,
        rest_body["diagnostics"]["candidates_after_filter"]
            .as_u64()
            .expect("rest filtered candidate count should be numeric")
    );
    assert_eq!(
        diagnostics.candidates_reranked as u64,
        rest_body["diagnostics"]["candidates_reranked"]
            .as_u64()
            .expect("rest rerank count should be numeric")
    );
    assert_eq!(
        diagnostics.candidates_merged as u64,
        rest_body["diagnostics"]["candidates_merged"]
            .as_u64()
            .expect("rest merge count should be numeric")
    );
    assert_eq!(
        diagnostics.candidates_reranked as u64,
        grpc_diagnostics.candidates_reranked
    );
    assert_eq!(
        diagnostics.candidates_merged as u64,
        grpc_diagnostics.candidates_merged
    );
    assert_eq!(
        diagnostics.rerank_count as u64,
        grpc_diagnostics.rerank_count
    );
    assert_eq!(diagnostics.planner_reason, grpc_diagnostics.planner_reason);
    assert!(
        (diagnostics.estimated_selectivity - grpc_diagnostics.estimated_selectivity).abs()
            <= f32::EPSILON
    );
    assert_eq!(
        diagnostics.units_considered as u64,
        grpc_diagnostics.units_considered
    );
    assert_eq!(
        diagnostics.units_pruned as u64,
        grpc_diagnostics.units_pruned
    );
    assert_eq!(
        diagnostics.units_scanned as u64,
        grpc_diagnostics.units_scanned
    );
    assert_eq!(
        diagnostics.candidates_before_filter as u64,
        grpc_diagnostics.candidates_before_filter
    );
    assert_eq!(
        diagnostics.candidates_after_filter as u64,
        grpc_diagnostics.candidates_after_filter
    );
    assert!(diagnostics.fallback_reason.is_some());
    assert_eq!(
        rest_body["diagnostics"]["fallback_reason"].as_str(),
        diagnostics.fallback_reason.as_deref()
    );
    assert_eq!(
        grpc_diagnostics.fallback_reason,
        diagnostics.fallback_reason
    );
    assert_eq!(diagnostics.unit_scan_mix.get("exact_f32").copied(), Some(1));
    assert_eq!(
        rest_body["diagnostics"]["unit_scan_mix"]["exact_f32"],
        Value::from(1)
    );
    assert_eq!(grpc_diagnostics.unit_scan_mix.get("exact_f32"), Some(&1));
    // Timings differ per run; every transport carries all six stages.
    let service_timings = diagnostics
        .stage_timings
        .as_ref()
        .expect("service timings should be present");
    assert_eq!(service_timings.prefilter_micros, 0);
    assert_eq!(service_timings.merge_micros, 0);
    let rest_timings = &rest_body["diagnostics"]["stage_timings"];
    for stage in [
        "planning_micros",
        "prefilter_micros",
        "candidate_generation_micros",
        "postfilter_micros",
        "rerank_micros",
        "merge_micros",
    ] {
        assert!(rest_timings[stage].as_u64().is_some(), "{stage}");
    }
    let grpc_timings = grpc_diagnostics
        .stage_timings
        .as_ref()
        .expect("grpc timings should be present");
    assert_eq!(grpc_timings.prefilter_micros, 0);
    assert_eq!(grpc_timings.merge_micros, 0);
}

#[tokio::test]
async fn service_reports_stats_and_inspect_targets_for_maintenance_workflows() {
    let root = unique_temp_dir("service-inspect");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    service
        .upsert(
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"version":1}),
                })
                .expect("record"),
                record_from_put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: json!({"version":1}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect("write should succeed");
    service
        .flush("documents")
        .await
        .expect("flush should succeed");
    service
        .delete("documents", vec![PrimaryKey::from("alpha")])
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
    // The delete after the flush only set the segment row's deletion bit: the memtable holds
    // no record, but one operation sits above the checkpoint.
    assert!(
        wal.payload
            .get("records")
            .and_then(Value::as_array)
            .expect("wal records should be an array")
            .is_empty()
    );
    assert_eq!(
        wal.payload["visible_seq_no"].as_u64(),
        wal.payload["checkpoint_seq_no"]
            .as_u64()
            .map(|seq_no| seq_no + 1)
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
    let service = LogPoseDataService::local(&root).expect("data service should open");

    let error = service
        .get_collection("missing")
        .await
        .expect_err("missing collection should error");

    assert!(matches!(
        error,
        LogPoseError::NotFound { resource: ResourceKind::Collection, name } if name.contains("missing")
    ));
}

#[tokio::test]
async fn service_rejects_invalid_records_and_schemas_as_invalid_argument() {
    let root = unique_temp_dir("service-invalid-records");
    let service = LogPoseDataService::local(&root).expect("data service should open");

    service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "documents".to_owned(),
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let record = |id: String, vector: Vec<f32>, extra: Value| {
        let mut record = Record::new(id).with_vector("vector", vector);
        if let Value::Object(extra) = extra {
            record.extra = extra;
        }
        record
    };
    for (record, field) in [
        (
            record("a".to_owned(), vec![1.0, 0.0], json!({"id": "x"})),
            "records[0].id",
        ),
        (
            record("a".to_owned(), vec![1.0, 0.0], json!({"vector": 1})),
            "records[0].vector",
        ),
        (
            record("a".to_owned(), vec![1.0, 0.0], json!({"$extra": 1})),
            "records[0].$extra",
        ),
        (
            record("a".to_owned(), vec![f32::INFINITY, 0.0], Value::Null),
            "records[0].vector",
        ),
        (
            record("a".repeat(1_025), vec![1.0, 0.0], Value::Null),
            "records[0].id",
        ),
        (
            record(String::new(), vec![1.0, 0.0], Value::Null),
            "records[0].id",
        ),
        (
            record("a".to_owned(), vec![1.0], Value::Null),
            "records[0].vector",
        ),
    ] {
        let error = service
            .upsert("documents", vec![record.clone()])
            .await
            .expect_err("invalid record should be rejected");
        assert!(
            matches!(
                error,
                LogPoseError::InvalidArgument { .. } | LogPoseError::DimensionMismatch { .. }
            ),
            "{record:?} should be an invalid argument, got {error:?}"
        );
        assert_eq!(
            error.details().field_violations[0].field,
            field,
            "{error:?}"
        );
    }

    let error = service
        .create_collection(CreateCollectionRequest::in_database(
            "default".to_owned(),
            "huge".to_owned(),
            65_537,
            DistanceMetric::Dot,
        ))
        .await
        .expect_err("too many dimensions should be rejected");
    assert!(
        matches!(error, LogPoseError::InvalidArgument { .. }),
        "too many dimensions should be an invalid argument, got {error:?}"
    );
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
