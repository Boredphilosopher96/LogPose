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
use logpose_core as _;
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator, QueryPlanKind,
    QueryRequest, ScalarMetadataValue,
};
use logpose_service::{LogPoseDataService, ServiceError};
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_storage_etcd as _;
use logpose_types::{DistanceMetric, PutRecord, RecordId, Snapshot, WriteOperation};
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
    let service = LogPoseDataService::local(&root);

    let descriptor = service
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
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
            database_name: "default".to_owned(),
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
            database_name: "default".to_owned(),
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
async fn app_state_accepts_database_qualified_collection_references() {
    let state = Arc::new(logpose_core::AppState::new(test_config(
        "service-qualified-default-namespace",
    )));

    state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    state
        .write(
            "default/documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
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
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
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
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    state
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

    state
        .flush("documents")
        .await
        .expect("flush should succeed");

    let predicate = Predicate::Comparison(PredicateComparison {
        field: "kind".to_owned(),
        operator: PredicateOperator::Eq,
        value: Some(ScalarMetadataValue::String("keep".to_owned())),
    });

    let service_response = state
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
            database_name: String::new(),
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
        QueryPlanKind::VectorFirstAnn
    );
    assert_eq!(rest_body["diagnostics"]["chosen_plan"], "vector_first_ann");
    assert_eq!(
        proto::QueryPlanKind::try_from(grpc_diagnostics.chosen_plan)
            .expect("chosen plan should decode"),
        proto::QueryPlanKind::VectorFirstAnn
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
    assert_eq!(service_diagnostics.fallback_reason, None);
    assert_eq!(rest_body["diagnostics"]["fallback_reason"], Value::Null);
    assert_eq!(grpc_diagnostics.fallback_reason, None);
    assert_eq!(
        service_diagnostics
            .unit_scan_mix
            .get("immutable_ann")
            .copied(),
        Some(1)
    );
    assert_eq!(
        rest_body["diagnostics"]["unit_scan_mix"]["immutable_ann"],
        Value::from(1)
    );
    assert_eq!(
        grpc_diagnostics.unit_scan_mix.get("immutable_ann"),
        Some(&1)
    );
    let service_timings = service_diagnostics
        .stage_timings
        .as_ref()
        .expect("service timings should be present");
    assert!(service_timings.planning_micros > 0);
    assert!(service_timings.candidate_generation_micros > 0);
    assert!(service_timings.rerank_micros > 0);
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
    assert!(grpc_timings.planning_micros > 0);
    assert!(grpc_timings.candidate_generation_micros > 0);
    assert!(grpc_timings.rerank_micros > 0);
}

#[tokio::test]
async fn service_rest_and_grpc_surface_cooperative_filtered_ann() {
    let state = Arc::new(logpose_core::AppState::new(test_config(
        "service-cooperative-filtered-ann",
    )));
    let rest = logpose_api_rest::router(Arc::clone(&state));
    let grpc = logpose_api_grpc::GrpcLogPoseService::new(Arc::clone(&state));

    state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    let operations = (0..12)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            WriteOperation::Put(PutRecord {
                id: RecordId::new(format!("doc-{index}")),
                vector: vec![index as f32 + 1.0, 0.0],
                metadata: json!({"kind":kind,"version":index}),
            })
        })
        .collect::<Vec<_>>();
    state
        .write("documents", operations)
        .await
        .expect("write should succeed");
    state
        .flush("documents")
        .await
        .expect("flush should succeed");

    let predicate = Predicate::Comparison(PredicateComparison {
        field: "kind".to_owned(),
        operator: PredicateOperator::Eq,
        value: Some(ScalarMetadataValue::String("keep".to_owned())),
    });

    let service_response = state
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
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
            database_name: String::new(),
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
    assert_eq!(
        diagnostics.chosen_plan,
        QueryPlanKind::CooperativeFilteredAnn
    );
    assert_eq!(
        diagnostics.planner_reason,
        "filtered ann traversal is cheaper than exact scan for this selectivity"
    );
    assert!((diagnostics.estimated_selectivity - 0.25).abs() <= f32::EPSILON);
    assert_eq!(diagnostics.units_considered, 2);
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 1);
    assert!(diagnostics.candidates_before_filter >= service_response.returned);
    assert!(diagnostics.candidates_after_filter >= service_response.returned);
    assert!(diagnostics.candidates_after_filter <= diagnostics.candidates_before_filter);
    assert_eq!(
        diagnostics.candidates_reranked,
        diagnostics.candidates_merged
    );
    assert!(diagnostics.candidates_reranked >= service_response.returned);
    assert_eq!(diagnostics.rerank_count, 1);
    assert_eq!(
        rest_body["diagnostics"]["chosen_plan"],
        "cooperative_filtered_ann"
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
        proto::QueryPlanKind::CooperativeFilteredAnn
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
    assert_eq!(diagnostics.fallback_reason, None);
    assert_eq!(rest_body["diagnostics"]["fallback_reason"], Value::Null);
    assert_eq!(grpc_diagnostics.fallback_reason, None);
    assert_eq!(
        diagnostics.unit_scan_mix.get("immutable_ann").copied(),
        Some(1)
    );
    assert_eq!(
        rest_body["diagnostics"]["unit_scan_mix"]["immutable_ann"],
        Value::from(1)
    );
    assert_eq!(
        grpc_diagnostics.unit_scan_mix.get("immutable_ann"),
        Some(&1)
    );
    let service_timings = diagnostics
        .stage_timings
        .as_ref()
        .expect("service timings should be present");
    let rest_timings = &rest_body["diagnostics"]["stage_timings"];
    assert_eq!(
        service_timings.planning_micros > 0,
        rest_timings["planning_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert_eq!(
        service_timings.prefilter_micros > 0,
        rest_timings["prefilter_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert_eq!(
        service_timings.candidate_generation_micros > 0,
        rest_timings["candidate_generation_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert_eq!(
        service_timings.postfilter_micros > 0,
        rest_timings["postfilter_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert_eq!(
        service_timings.rerank_micros > 0,
        rest_timings["rerank_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    assert_eq!(
        service_timings.merge_micros > 0,
        rest_timings["merge_micros"]
            .as_u64()
            .is_some_and(|micros| micros > 0)
    );
    let grpc_timings = grpc_diagnostics
        .stage_timings
        .as_ref()
        .expect("grpc timings should be present");
    assert_eq!(
        service_timings.planning_micros > 0,
        grpc_timings.planning_micros > 0
    );
    assert_eq!(
        service_timings.prefilter_micros > 0,
        grpc_timings.prefilter_micros > 0
    );
    assert_eq!(
        service_timings.candidate_generation_micros > 0,
        grpc_timings.candidate_generation_micros > 0
    );
    assert_eq!(
        service_timings.postfilter_micros > 0,
        grpc_timings.postfilter_micros > 0
    );
    assert_eq!(
        service_timings.rerank_micros > 0,
        grpc_timings.rerank_micros > 0
    );
    assert_eq!(
        service_timings.merge_micros > 0,
        grpc_timings.merge_micros > 0
    );
}

#[tokio::test]
async fn service_reports_stats_and_inspect_targets_for_maintenance_workflows() {
    let root = unique_temp_dir("service-inspect");
    let service = LogPoseDataService::local(&root);

    service
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
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
