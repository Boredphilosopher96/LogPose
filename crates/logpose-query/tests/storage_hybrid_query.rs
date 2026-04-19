//! Storage-backed ANN and hybrid query integration tests.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_query::{
    ExplainMode, Predicate, PredicateComparison, PredicateOperator, QueryPlanKind, QueryRequest,
    ScalarMetadataValue, query_exact,
};
use logpose_storage::{CreateCollectionRequest, LocalStorageEngine, StorageEngine};
use logpose_types::{DistanceMetric, PutRecord, RecordId, WriteOperation};
use serde as _;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror as _;

#[tokio::test]
async fn uses_vector_first_ann_after_flush_and_reranks_exact_vectors() {
    let root = unique_temp_dir("query-hnsw-vector-first");
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
                    vector: vec![0.9, 0.0],
                    metadata: json!({ "kind": "keep", "version": 1 }),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![1.1, 0.0],
                    metadata: json!({ "kind": "keep", "version": 1 }),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![0.8, 0.0],
                    metadata: json!({ "kind": "drop", "version": 1 }),
                }),
            ],
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
            top_k: 2,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::Profile,
        },
    )
    .await
    .expect("query should succeed");
    let exact_ids = exact_ranked_ids(&engine, "documents", &[1.0, 0.0], None)
        .await
        .into_iter()
        .take(2)
        .collect::<Vec<_>>();

    assert_eq!(
        response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        exact_ids.iter().map(String::as_str).collect::<Vec<_>>()
    );
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::VectorFirstAnn);
    assert_eq!(diagnostics.rerank_count, 1);
    assert_eq!(
        diagnostics.unit_scan_mix.get("immutable_ann").copied(),
        Some(1)
    );
    let timings = diagnostics
        .stage_timings
        .expect("timings should be present");
    assert!(timings.candidate_generation_micros > 0);
    assert!(timings.rerank_micros > 0);
}

#[tokio::test]
async fn uses_cooperative_filtered_ann_for_selective_immutable_predicates() {
    let root = unique_temp_dir("query-hnsw-cooperative-filtered");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let operations = (0..12)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            WriteOperation::Put(PutRecord {
                id: RecordId::new(format!("doc-{index}")),
                vector: vec![index as f32 + 1.0, (index % 3) as f32],
                metadata: json!({ "kind": kind, "version": index }),
            })
        })
        .collect::<Vec<_>>();
    engine
        .write("documents", operations)
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
            top_k: 2,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Profile,
        },
    )
    .await
    .expect("query should succeed");
    let exact_ids = exact_ranked_ids(&engine, "documents", &[1.0, 0.0], Some("keep"))
        .await
        .into_iter()
        .take(2)
        .collect::<Vec<_>>();

    assert_eq!(
        response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        exact_ids.iter().map(String::as_str).collect::<Vec<_>>()
    );
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(
        diagnostics.chosen_plan,
        QueryPlanKind::CooperativeFilteredAnn
    );
    assert_eq!(diagnostics.rerank_count, 1);
    assert_eq!(
        diagnostics.unit_scan_mix.get("immutable_ann").copied(),
        Some(1)
    );
    let timings = diagnostics
        .stage_timings
        .expect("timings should be present");
    assert!(timings.candidate_generation_micros > 0);
    assert!(timings.rerank_micros > 0);
}

#[tokio::test]
async fn hybrid_query_prefers_latest_mutable_version_over_stale_immutable_candidate() {
    let root = unique_temp_dir("query-hybrid-latest-visible");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "profiles",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "profiles",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![0.5, 0.0],
                    metadata: json!({ "kind": "keep", "version": 1 }),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.2, 0.0],
                    metadata: json!({ "kind": "keep", "version": 1 }),
                }),
            ],
        )
        .await
        .expect("write should succeed");
    engine
        .flush("profiles")
        .await
        .expect("flush should succeed");

    engine
        .write(
            "profiles",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![2.0, 0.0],
                metadata: json!({ "kind": "keep", "version": 2 }),
            })],
        )
        .await
        .expect("mutable update should succeed");

    let response = query_exact(
        &engine,
        QueryRequest {
            collection_name: "profiles".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Profile,
        },
    )
    .await
    .expect("query should succeed");

    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].id.as_str(), "alpha");
    assert_eq!(response.matches[0].metadata["version"], 2);
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::HybridExactAnnMerge);
    assert_eq!(diagnostics.rerank_count, 1);
    assert_eq!(
        diagnostics.unit_scan_mix.get("mutable_exact").copied(),
        Some(1)
    );
    assert_eq!(
        diagnostics.unit_scan_mix.get("immutable_ann").copied(),
        Some(1)
    );
}

#[tokio::test]
async fn tiny_population_fallback_stays_correct_after_compaction_and_reopen() {
    let root = unique_temp_dir("query-hnsw-fallback-reopen");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "events",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({ "kind": "drop", "version": 1 }),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.8, 0.0],
                    metadata: json!({ "kind": "keep", "version": 1 }),
                }),
            ],
        )
        .await
        .expect("write should succeed");
    engine.flush("events").await.expect("flush should succeed");

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("gamma"),
                vector: vec![0.5, 0.0],
                metadata: json!({ "kind": "drop", "version": 1 }),
            })],
        )
        .await
        .expect("write should succeed");
    engine.flush("events").await.expect("flush should succeed");
    engine
        .compact("events")
        .await
        .expect("compaction should succeed");

    let reopened = LocalStorageEngine::new(&root);
    let response = query_exact(
        &reopened,
        QueryRequest {
            collection_name: "events".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Plan,
        },
    )
    .await
    .expect("query should succeed");

    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].id.as_str(), "beta");
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(
        diagnostics.chosen_plan,
        QueryPlanKind::TinyPopulationExactFallback
    );
    assert!(diagnostics.fallback_reason.is_some());
}

fn unique_temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-{name}-{unique}"));
    if path.exists() {
        fs::remove_dir_all(&path).expect("stale temp dir should be removable");
    }
    path
}

async fn exact_ranked_ids(
    engine: &LocalStorageEngine,
    collection_name: &str,
    query: &[f32],
    kind: Option<&str>,
) -> Vec<String> {
    let mut scored = engine
        .scan_exact(collection_name, None)
        .await
        .expect("scan should succeed")
        .into_iter()
        .filter(|record| kind.is_none_or(|kind| record.metadata["kind"] == kind))
        .map(|record| {
            (
                record.id.to_string(),
                (query[0] * record.vector[0]) + (query[1] * record.vector[1]),
            )
        })
        .collect::<Vec<_>>();
    scored.sort_by(|(left_id, left_value), (right_id, right_value)| {
        right_value
            .total_cmp(left_value)
            .then_with(|| left_id.cmp(right_id))
    });
    scored.into_iter().map(|(id, _)| id).collect()
}
