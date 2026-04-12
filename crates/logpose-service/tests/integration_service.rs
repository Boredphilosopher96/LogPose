//! Integration tests for `LogPoseDataService`.

use axum as _;
use http_body_util as _;
use logpose_api_grpc as _;
use logpose_api_rest as _;
use logpose_catalog as _;
use logpose_config as _;
use logpose_core as _;
use logpose_query::{MetadataFilter, QueryRequest, ScalarMetadataValue};
use logpose_service::{LogPoseDataService, ServiceError};
use logpose_storage::CreateCollectionRequest;
use logpose_types::{DistanceMetric, PutRecord, RecordId, WriteOperation};
use rand as _;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror as _;
use tonic as _;
use tower as _;

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
    assert_eq!(stats.live_record_count, 3);
    assert_eq!(stats.mutable_op_count, 0);

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
