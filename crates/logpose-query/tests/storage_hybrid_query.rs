//! Storage-backed queries whose segments carry graphs and SQ8 codes: the per-unit strategy
//! the staged planner picks, and results equal to an exact scan.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    ExplainMode, FilterComparison, FilterExpr, FilterOperator, QueryPlanKind, QueryRequest,
    ScalarMetadataValue, query, scan_records,
};
use logpose_storage::{
    CreateCollectionRequest, EngineConfig, IndexPolicy, LocalStorageEngine, ReadOptions,
    StorageEngine,
};
use logpose_types::{CollectionRef, DistanceMetric, PutRecord, RecordId, WriteOperation};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror as _;

/// Rows per test collection: above the exact-scan limit of the default tuning (2,048), so a
/// segment with a graph walks it.
const ROWS: usize = 3_000;
const DIMS: usize = 16;

/// A deterministic unit-variance vector for `seed`.
fn vector(seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..DIMS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 11) as f64 / (1_u64 << 53) as f64 * 2.0 - 1.0) as f32
        })
        .collect()
}

fn query_vector() -> Vec<f32> {
    vector(987_654)
}

fn engine(root: &PathBuf) -> LocalStorageEngine {
    LocalStorageEngine::with_config(
        root,
        EngineConfig {
            index: IndexPolicy {
                graph_min_rows: 1_000,
                sq8_min_rows: 256,
                ..IndexPolicy::default()
            },
            ..EngineConfig::default()
        },
    )
    .expect("storage engine should open")
}

async fn create(engine: &LocalStorageEngine, name: &str) {
    engine
        .create_collection(CreateCollectionRequest::new(
            name,
            DIMS,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");
}

/// `ROWS` random rows, a quarter of them `keep`.
async fn fill(engine: &LocalStorageEngine, name: &str) {
    let operations = (0..ROWS)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            WriteOperation::Put(PutRecord {
                id: RecordId::new(format!("doc-{index:05}")),
                vector: vector(index as u64),
                metadata: json!({ "kind": kind, "version": 1 }),
            })
        })
        .collect::<Vec<_>>();
    engine
        .write(name, operations)
        .await
        .expect("write should succeed");
}

fn request(name: &str, top_k: usize, kind: Option<(&str, FilterOperator)>) -> QueryRequest {
    QueryRequest {
        collection_name: name.to_owned(),
        vector: query_vector(),
        top_k,
        snapshot: None,
        read_barrier: None,
        filters: Vec::new(),
        predicate: kind.map(|(kind, operator)| {
            FilterExpr::Comparison(FilterComparison {
                field: "kind".to_owned(),
                operator,
                value: Some(ScalarMetadataValue::String(kind.to_owned())),
            })
        }),
        explain: ExplainMode::Profile,
        snapshot_token: None,
        pin: false,
    }
}

#[tokio::test]
async fn unfiltered_queries_walk_segment_graphs_and_rerank_exactly() {
    let root = unique_temp_dir("query-graph-unfiltered");
    let engine = engine(&root);
    create(&engine, "documents").await;
    fill(&engine, "documents").await;
    engine
        .flush("documents")
        .await
        .expect("flush should succeed");

    let response = query(&engine, request("documents", 10, None))
        .await
        .expect("query should succeed");
    let exact = exact_ranked_ids(&engine, "documents", &query_vector(), |_| true).await;
    assert_eq!(ids(&response), exact[..10]);
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::VectorFirstAnn);
    assert_eq!(diagnostics.unit_scan_mix.get("graph_admit"), Some(&1));
    assert_eq!(diagnostics.rerank_count, 1);
    assert!(diagnostics.stage_timings.is_some());
}

#[tokio::test]
async fn filters_pick_exact_scans_or_filtered_walks_by_matching_rows() {
    let root = unique_temp_dir("query-graph-filtered");
    let engine = engine(&root);
    create(&engine, "documents").await;
    fill(&engine, "documents").await;
    engine
        .flush("documents")
        .await
        .expect("flush should succeed");

    // 750 `keep` rows are within the exact-scan limit: an exact scan over SQ8 codes.
    let selective = query(
        &engine,
        request("documents", 5, Some(("keep", FilterOperator::Eq))),
    )
    .await
    .expect("query should succeed");
    let exact =
        exact_ranked_ids(&engine, "documents", &query_vector(), |kind| kind == "keep").await;
    assert_eq!(ids(&selective), exact[..5]);
    let diagnostics = selective.diagnostics.expect("diagnostics");
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::PredicateFirstExact);
    assert_eq!(diagnostics.unit_scan_mix.get("exact_sq8"), Some(&1));
    assert_eq!(diagnostics.candidates_after_filter, 750);

    // 2,250 rows that are not `keep` exceed it: an admit-only walk (selectivity 0.75).
    let broad = query(
        &engine,
        request("documents", 5, Some(("keep", FilterOperator::Ne))),
    )
    .await
    .expect("query should succeed");
    let exact =
        exact_ranked_ids(&engine, "documents", &query_vector(), |kind| kind != "keep").await;
    assert_eq!(ids(&broad), exact[..5]);
    let diagnostics = broad.diagnostics.expect("diagnostics");
    assert_eq!(
        diagnostics.chosen_plan,
        QueryPlanKind::CooperativeFilteredAnn
    );
    assert_eq!(diagnostics.unit_scan_mix.get("graph_admit"), Some(&1));
}

#[tokio::test]
async fn memtable_rows_merge_with_segment_walks_and_supersede_stale_rows() {
    let root = unique_temp_dir("query-graph-hybrid");
    let engine = engine(&root);
    create(&engine, "profiles").await;
    fill(&engine, "profiles").await;
    engine
        .flush("profiles")
        .await
        .expect("flush should succeed");
    engine
        .write(
            "profiles",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("doc-01500"),
                vector: query_vector().iter().map(|value| value * 3.0).collect(),
                metadata: json!({ "kind": "drop", "version": 2 }),
            })],
        )
        .await
        .expect("mutable update should succeed");

    let response = query(&engine, request("profiles", 3, None))
        .await
        .expect("query should succeed");
    assert_eq!(response.matches[0].id.as_str(), "doc-01500");
    assert_eq!(response.matches[0].metadata["version"], 2);
    assert_eq!(
        response
            .matches
            .iter()
            .filter(|matched| matched.id.as_str() == "doc-01500")
            .count(),
        1,
        "the superseded segment row is deleted"
    );
    let exact = exact_ranked_ids(&engine, "profiles", &query_vector(), |_| true).await;
    assert_eq!(ids(&response), exact[..3]);
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::HybridExactAnnMerge);
    assert_eq!(diagnostics.unit_scan_mix.get("memtable_scan"), Some(&1));
    assert_eq!(diagnostics.unit_scan_mix.get("graph_admit"), Some(&1));
}

#[tokio::test]
async fn small_filtered_populations_stay_exact_after_compaction_and_reopen() {
    let root = unique_temp_dir("query-graph-reopen");
    let engine = engine(&root);
    create(&engine, "events").await;
    fill(&engine, "events").await;
    engine.flush("events").await.expect("flush should succeed");
    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("late"),
                vector: vector(424_242),
                metadata: json!({ "kind": "rare", "version": 1 }),
            })],
        )
        .await
        .expect("write should succeed");
    engine.flush("events").await.expect("flush should succeed");
    engine
        .compact("events")
        .await
        .expect("compaction should succeed");
    drop(engine);

    let reopened = self::engine(&root);
    let response = query(
        &reopened,
        request("events", 3, Some(("rare", FilterOperator::Eq))),
    )
    .await
    .expect("query should succeed");
    assert_eq!(ids(&response), ["late"]);
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::PredicateFirstExact);
    assert_eq!(diagnostics.candidates_after_filter, 1);
}

fn ids(response: &logpose_query::QueryResponse) -> Vec<String> {
    response
        .matches
        .iter()
        .map(|candidate| candidate.id.to_string())
        .collect()
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

/// Ids of every live row whose `kind` passes `keep`, by exact dot product with `query`
/// (ties by id).
async fn exact_ranked_ids(
    engine: &LocalStorageEngine,
    collection_name: &str,
    query: &[f32],
    keep: impl Fn(&str) -> bool,
) -> Vec<String> {
    let mut scored = scan_records(
        engine,
        &CollectionRef::parse(collection_name).expect("name"),
        ReadOptions::default(),
    )
    .await
    .expect("scan should succeed")
    .into_iter()
    .filter(|record| keep(record.metadata["kind"].as_str().unwrap_or_default()))
    .map(|record| {
        (
            record.id.to_string(),
            query
                .iter()
                .zip(&record.vector)
                .map(|(left, right)| left * right)
                .sum::<f32>(),
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
