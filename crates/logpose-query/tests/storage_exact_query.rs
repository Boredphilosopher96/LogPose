//! Storage-backed exact query integration tests.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_query::{
    ExplainMode, Predicate, PredicateComparison, PredicateOperator, QueryError, QueryRequest,
    query_exact,
};
use logpose_storage::{CreateCollectionRequest, LocalStorageEngine, StorageEngine};
use logpose_types::{DistanceMetric, LogPoseError, PutRecord, RecordId, Snapshot, WriteOperation};
use serde as _;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror as _;

#[tokio::test]
async fn queries_storage_records_and_honors_snapshots() {
    let root = unique_temp_dir("query-storage-snapshots");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({ "tag": "alpha" }),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.5, 0.0],
                    metadata: json!({ "tag": "beta" }),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![-1.0, 0.0],
                    metadata: json!({ "tag": "gamma" }),
                }),
            ],
        )
        .await
        .expect("write should succeed");

    let current = query_exact(
        &engine,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: logpose_query::ExplainMode::None,
        },
    )
    .await
    .expect("query should succeed");

    let snapshot = engine
        .snapshot("documents")
        .await
        .expect("snapshot should succeed");
    assert_eq!(current.metric, DistanceMetric::Dot);
    assert_eq!(current.top_k, 2);
    assert_eq!(current.returned, 2);
    assert_eq!(current.snapshot, snapshot);
    assert_eq!(
        current
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    assert!((current.matches[0].value - 1.0).abs() < 1e-6);
    assert!((current.matches[1].value - 0.5).abs() < 1e-6);

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("delta"),
                vector: vec![3.0, 0.0],
                metadata: json!({ "tag": "delta" }),
            })],
        )
        .await
        .expect("write should succeed");

    let historical = query_exact(
        &engine,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 3,
            snapshot: Some(snapshot.clone()),
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: logpose_query::ExplainMode::None,
        },
    )
    .await
    .expect("historical query should succeed");

    assert_eq!(historical.snapshot, snapshot);
    assert_eq!(
        historical
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta", "gamma"]
    );
}

#[tokio::test]
async fn returns_empty_matches_for_empty_collection() {
    let root = unique_temp_dir("query-empty-collection");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "empty",
            3,
            DistanceMetric::Cosine,
        ))
        .await
        .expect("collection should be created");

    let response = query_exact(
        &engine,
        QueryRequest {
            collection_name: "empty".to_owned(),
            vector: vec![1.0, 0.0, 0.0],
            top_k: 5,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: logpose_query::ExplainMode::None,
        },
    )
    .await
    .expect("query should succeed");

    assert_eq!(response.metric, DistanceMetric::Cosine);
    assert_eq!(response.top_k, 5);
    assert_eq!(response.returned, 0);
    assert_eq!(response.matches.len(), 0);
    assert_eq!(
        response.snapshot,
        Snapshot {
            manifest_generation: 0,
            visible_seq_no: 0
        }
    );
}

#[tokio::test]
async fn rejects_query_vector_with_wrong_collection_dimensions() {
    let root = unique_temp_dir("query-dimension-mismatch");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "embeddings",
            3,
            DistanceMetric::L2,
        ))
        .await
        .expect("collection should be created");

    let result = query_exact(
        &engine,
        QueryRequest {
            collection_name: "embeddings".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: logpose_query::ExplainMode::None,
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(QueryError::RequestVectorDimensionMismatch {
            expected: 3,
            actual: 2
        })
    ));
}

#[tokio::test]
async fn preserves_visibility_through_delete_flush_reopen_and_compaction() {
    let root = unique_temp_dir("query-delete-flush-compact");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "profiles",
            2,
            DistanceMetric::L2,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "profiles",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![0.0, 0.0],
                    metadata: json!({ "version": 1 }),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({ "version": 1 }),
                }),
            ],
        )
        .await
        .expect("write should succeed");

    let before_delete = engine
        .snapshot("profiles")
        .await
        .expect("snapshot should succeed");

    engine
        .write(
            "profiles",
            vec![WriteOperation::Delete(logpose_types::DeleteRecord {
                id: RecordId::new("alpha"),
            })],
        )
        .await
        .expect("delete should succeed");
    engine
        .flush("profiles")
        .await
        .expect("flush should succeed");

    let reopened = LocalStorageEngine::new(&root);

    let historical = query_exact(
        &reopened,
        QueryRequest {
            collection_name: "profiles".to_owned(),
            vector: vec![0.0, 0.0],
            top_k: 2,
            snapshot: Some(before_delete),
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: logpose_query::ExplainMode::None,
        },
    )
    .await
    .expect("historical query should succeed");
    assert_eq!(
        historical
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );

    reopened
        .write(
            "profiles",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("gamma"),
                vector: vec![0.5, 0.0],
                metadata: json!({ "version": 1 }),
            })],
        )
        .await
        .expect("write should succeed");
    reopened
        .flush("profiles")
        .await
        .expect("flush should succeed");
    reopened
        .compact("profiles")
        .await
        .expect("compaction should succeed");

    let current = query_exact(
        &reopened,
        QueryRequest {
            collection_name: "profiles".to_owned(),
            vector: vec![0.0, 0.0],
            top_k: 3,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: logpose_query::ExplainMode::None,
        },
    )
    .await
    .expect("current query should succeed");

    assert_eq!(
        current
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["gamma", "beta"]
    );
}

#[tokio::test]
async fn exists_predicates_match_non_scalar_fields_after_flush() {
    let root = unique_temp_dir("query-exists-non-scalar-after-flush");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({ "details": { "kind": "keep" } }),
            })],
        )
        .await
        .expect("write should succeed");
    engine
        .flush("documents")
        .await
        .expect("flush should succeed");

    let response = query_exact(
        &engine,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "details".to_owned(),
                operator: PredicateOperator::Exists,
                value: None,
            })),
            explain: ExplainMode::Plan,
        },
    )
    .await
    .expect("exists query should succeed");

    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].id.as_str(), "alpha");
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 1);
}

#[tokio::test]
async fn surfaces_unknown_collection_errors_from_storage() {
    let root = unique_temp_dir("query-missing-collection");
    let engine = LocalStorageEngine::new(&root);

    let result = query_exact(
        &engine,
        QueryRequest {
            collection_name: "missing".to_owned(),
            vector: vec![1.0],
            top_k: 1,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: logpose_query::ExplainMode::None,
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(QueryError::Storage(LogPoseError::Message(message)))
            if message.contains("does not exist")
    ));
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-query-{label}-{suffix}"));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}
