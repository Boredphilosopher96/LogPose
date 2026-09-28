//! Storage-backed exact query integration tests.

use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    ExplainMode, FilterExpr, QueryError, QueryRequest, QueryResponse, ReadConsistency, VectorQuery,
};
use logpose_storage::{
    CollectionHandle, CollectionReader, CreateCollectionRequest, Engine, EngineConfig,
};
use logpose_types::{
    CollectionRef, DistanceMetric, LogPoseError, Snapshot,
    record::{ClientOp, PrimaryKey, Record},
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json::json;
use std::{path::Path, sync::Arc};
use thiserror as _;

fn request(
    collection: &str,
    vector: Vec<f32>,
    top_k: usize,
    snapshot: Option<Snapshot>,
    filter: Option<FilterExpr>,
    explain: ExplainMode,
) -> (CollectionRef, QueryRequest) {
    (
        CollectionRef::parse(collection).expect("name"),
        QueryRequest {
            vector: Some(VectorQuery {
                field: None,
                values: vector,
            }),
            top_k,
            filter,
            output_fields: vec!["$extra".to_owned()],
            explain,
            read: ReadConsistency {
                snapshot,
                ..ReadConsistency::default()
            },
            ..QueryRequest::default()
        },
    )
}

async fn query(
    reader: &dyn CollectionReader,
    (collection, request): (CollectionRef, QueryRequest),
) -> Result<QueryResponse, QueryError> {
    logpose_query::query(reader, &collection, request)
        .await
        .map(|result| result.value)
}

fn ids(response: &QueryResponse) -> Vec<String> {
    response
        .hits
        .iter()
        .map(|hit| hit.record.pk.label())
        .collect()
}

#[tokio::test]
async fn queries_storage_records_and_honors_snapshots() {
    let root_dir = unique_temp_dir("query-storage-snapshots");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "documents")
        .write(vec![
            put_with("alpha", vec![1.0, 0.0], json!({ "tag": "alpha" })),
            put_with("beta", vec![0.5, 0.0], json!({ "tag": "beta" })),
            put_with("gamma", vec![-1.0, 0.0], json!({ "tag": "gamma" })),
        ])
        .await
        .expect("write should succeed");

    let current = query(
        &engine,
        request(
            "documents",
            vec![1.0, 0.0],
            2,
            None,
            None,
            ExplainMode::None,
        ),
    )
    .await
    .expect("query should succeed");

    let snapshot = handle(&engine, "documents")
        .snapshot()
        .expect("snapshot should succeed");
    assert_eq!(current.metric, Some(DistanceMetric::Dot));
    assert_eq!(current.top_k, 2);
    assert_eq!(current.hits.len(), 2);
    assert_eq!(current.snapshot, snapshot);
    assert_eq!(ids(&current), vec!["alpha", "beta"]);
    assert!((current.hits[0].score.unwrap_or_default() - 1.0).abs() < 1e-6);
    assert!((current.hits[1].score.unwrap_or_default() - 0.5).abs() < 1e-6);

    handle(&engine, "documents")
        .write(vec![put_with(
            "delta",
            vec![3.0, 0.0],
            json!({ "tag": "delta" }),
        )])
        .await
        .expect("write should succeed");

    let historical = query(
        &engine,
        request(
            "documents",
            vec![1.0, 0.0],
            3,
            Some(snapshot.clone()),
            None,
            ExplainMode::None,
        ),
    )
    .await
    .expect("historical query should succeed");

    assert_eq!(historical.snapshot, snapshot);
    assert_eq!(ids(&historical), vec!["alpha", "beta", "gamma"]);
}

#[tokio::test]
async fn returns_empty_matches_for_empty_collection() {
    let root_dir = unique_temp_dir("query-empty-collection");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("empty", 3, DistanceMetric::Cosine),
    )
    .await
    .expect("collection should be created");

    let response = query(
        &engine,
        request(
            "empty",
            vec![1.0, 0.0, 0.0],
            5,
            None,
            None,
            ExplainMode::None,
        ),
    )
    .await
    .expect("query should succeed");

    assert_eq!(response.metric, Some(DistanceMetric::Cosine));
    assert_eq!(response.top_k, 5);
    assert_eq!(response.hits.len(), 0);
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
    let root_dir = unique_temp_dir("query-dimension-mismatch");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("embeddings", 3, DistanceMetric::L2),
    )
    .await
    .expect("collection should be created");

    let result = query(
        &engine,
        request(
            "embeddings",
            vec![1.0, 0.0],
            1,
            None,
            None,
            ExplainMode::None,
        ),
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
    let root_dir = unique_temp_dir("query-delete-flush-compact");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("profiles", 2, DistanceMetric::L2),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "profiles")
        .write(vec![
            put_with("alpha", vec![0.0, 0.0], json!({ "version": 1 })),
            put_with("beta", vec![1.0, 0.0], json!({ "version": 1 })),
        ])
        .await
        .expect("write should succeed");

    // Pin the state before the delete: the flush below supersedes its manifest generation.
    let (_token, before_delete) = pin(&engine, "profiles").expect("pin should succeed");

    handle(&engine, "profiles")
        .write(vec![delete("alpha")])
        .await
        .expect("delete should succeed");
    handle(&engine, "profiles")
        .flush()
        .await
        .expect("flush should succeed");

    let historical_request = request(
        "profiles",
        vec![0.0, 0.0],
        2,
        Some(before_delete),
        None,
        ExplainMode::None,
    );
    let historical = query(&engine, historical_request.clone())
        .await
        .expect("a pinned historical query should succeed");
    assert_eq!(ids(&historical), vec!["alpha", "beta"]);

    // Pins end with the process: after a reopen the old generation is gone.
    drop(engine);
    let reopened = open(&root);
    let expired = query(&reopened, historical_request)
        .await
        .expect_err("an unpinned historical snapshot expires");
    assert!(
        matches!(
            expired,
            QueryError::Storage(logpose_types::LogPoseError::SnapshotExpired { .. })
        ),
        "{expired}"
    );

    handle(&reopened, "profiles")
        .write(vec![put_with(
            "gamma",
            vec![0.5, 0.0],
            json!({ "version": 1 }),
        )])
        .await
        .expect("write should succeed");
    handle(&reopened, "profiles")
        .flush()
        .await
        .expect("flush should succeed");
    handle(&reopened, "profiles")
        .compact()
        .await
        .expect("compaction should succeed");

    let current = query(
        &reopened,
        request("profiles", vec![0.0, 0.0], 3, None, None, ExplainMode::None),
    )
    .await
    .expect("current query should succeed");

    assert_eq!(ids(&current), vec!["gamma", "beta"]);
}

#[tokio::test]
async fn exists_predicates_match_non_scalar_fields_after_flush() {
    let root_dir = unique_temp_dir("query-exists-non-scalar-after-flush");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "documents")
        .write(vec![put_with(
            "alpha",
            vec![1.0, 0.0],
            json!({ "details": { "kind": "keep" } }),
        )])
        .await
        .expect("write should succeed");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");

    let response = query(
        &engine,
        request(
            "documents",
            vec![1.0, 0.0],
            1,
            None,
            Some(FilterExpr::exists("details")),
            ExplainMode::Plan,
        ),
    )
    .await
    .expect("exists query should succeed");

    assert_eq!(response.hits.len(), 1);
    assert_eq!(response.hits[0].record.pk.label(), "alpha");
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 1);
}

#[tokio::test]
async fn surfaces_unknown_collection_errors_from_storage() {
    let root_dir = unique_temp_dir("query-missing-collection");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let result = query(
        &engine,
        request("missing", vec![1.0], 1, None, None, ExplainMode::None),
    )
    .await;

    assert!(matches!(
        result,
        Err(QueryError::Storage(LogPoseError::NotFound { .. }))
    ));
}

/// A fresh temp directory named `logpose-query-{label}-…`, removed when the returned
/// guard drops, also when the test panics.
fn unique_temp_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("logpose-query-{label}-"))
        .tempdir()
        .expect("temp dir should be created")
}

fn open(root: &Path) -> Engine {
    Engine::open_local(root, EngineConfig::default()).expect("engine should open")
}

async fn create(
    engine: &Engine,
    request: CreateCollectionRequest,
) -> logpose_types::Result<Arc<CollectionHandle>> {
    let descriptor = engine.plan_collection_descriptor(&request)?;
    engine.create_collection(descriptor, None).await
}

fn handle(engine: &Engine, name: &str) -> Arc<CollectionHandle> {
    engine
        .collection(&CollectionRef::parse(name).expect("name"))
        .expect("the collection should be open")
}

fn put_with(id: &str, vector: Vec<f32>, extra: serde_json::Value) -> ClientOp {
    let mut record = Record::new(id).with_vector("vector", vector);
    if let serde_json::Value::Object(extra) = extra {
        record.extra = extra;
    }
    ClientOp::Upsert(record)
}

fn delete(id: &str) -> ClientOp {
    ClientOp::Delete(PrimaryKey::from(id))
}

/// Pin the current state of `name`, returning the token and the snapshot it names.
fn pin(
    engine: &Engine,
    name: &str,
) -> logpose_types::Result<(logpose_storage::SnapshotToken, Snapshot)> {
    let handle = handle(engine, name);
    let token = handle.pin_snapshot()?;
    let snapshot = handle.snapshot_version(&token)?.snapshot();
    Ok((token, snapshot))
}
