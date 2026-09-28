//! Storage-backed queries whose segments carry graphs and SQ8 codes: the per-unit strategy
//! the staged planner picks, and results equal to an exact scan.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    ExplainMode, FilterExpr, QueryPlanKind, QueryRequest, QueryResponse, VectorQuery,
    ops::{ScrollOrder, scroll_view},
};
use logpose_storage::{
    CollectionHandle, CollectionReader, CreateCollectionRequest, Engine, EngineConfig, IndexPolicy,
    Projection, ReadOptions,
};
use logpose_types::{
    CollectionRef, DistanceMetric,
    record::{ClientOp, Record},
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};
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

fn engine(root: &PathBuf) -> Engine {
    Engine::open_local(
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

async fn create(engine: &Engine, name: &str) {
    let descriptor = engine
        .plan_collection_descriptor(&CreateCollectionRequest::new(
            name,
            DIMS,
            DistanceMetric::Dot,
        ))
        .expect("collection should plan");
    engine
        .create_collection(descriptor, None)
        .await
        .expect("collection should be created");
}

fn handle(engine: &Engine, name: &str) -> Arc<CollectionHandle> {
    engine
        .collection(&CollectionRef::parse(name).expect("name"))
        .expect("the collection should be open")
}

fn put(id: &str, vector: Vec<f32>, extra: serde_json::Value) -> ClientOp {
    let mut record = Record::new(id).with_vector("vector", vector);
    if let serde_json::Value::Object(extra) = extra {
        record.extra = extra;
    }
    ClientOp::Upsert(record)
}

/// `ROWS` random rows, a quarter of them `keep`.
async fn fill(engine: &Engine, name: &str) {
    let operations = (0..ROWS)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            put(
                &format!("doc-{index:05}"),
                vector(index as u64),
                json!({ "kind": kind, "version": 1 }),
            )
        })
        .collect::<Vec<_>>();
    handle(engine, name)
        .write(operations)
        .await
        .expect("write should succeed");
}

fn request(name: &str, top_k: usize, filter: Option<FilterExpr>) -> (CollectionRef, QueryRequest) {
    (
        CollectionRef::parse(name).expect("name"),
        QueryRequest {
            vector: Some(VectorQuery {
                field: None,
                values: query_vector(),
            }),
            top_k,
            filter,
            explain: ExplainMode::Profile,
            ..QueryRequest::default()
        },
    )
}

async fn query(
    engine: &Engine,
    (collection, request): (CollectionRef, QueryRequest),
) -> logpose_query::Result<QueryResponse> {
    logpose_query::query(engine, &collection, request)
        .await
        .map(|result| result.value)
}

#[tokio::test]
async fn unfiltered_queries_walk_segment_graphs_and_rerank_exactly() {
    let root_dir = unique_temp_dir("query-graph-unfiltered");
    let root = root_dir.path().to_path_buf();
    let engine = engine(&root);
    create(&engine, "documents").await;
    fill(&engine, "documents").await;
    handle(&engine, "documents")
        .flush()
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
    let root_dir = unique_temp_dir("query-graph-filtered");
    let root = root_dir.path().to_path_buf();
    let engine = engine(&root);
    create(&engine, "documents").await;
    fill(&engine, "documents").await;
    handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");

    // 750 `keep` rows are within the exact-scan limit: an exact scan over SQ8 codes.
    let selective = query(
        &engine,
        request("documents", 5, Some(FilterExpr::eq("kind", "keep"))),
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
        request("documents", 5, Some(FilterExpr::ne("kind", "keep"))),
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
    let root_dir = unique_temp_dir("query-graph-hybrid");
    let root = root_dir.path().to_path_buf();
    let engine = engine(&root);
    create(&engine, "profiles").await;
    fill(&engine, "profiles").await;
    handle(&engine, "profiles")
        .flush()
        .await
        .expect("flush should succeed");
    handle(&engine, "profiles")
        .write(vec![put(
            "doc-01500",
            query_vector().iter().map(|value| value * 3.0).collect(),
            json!({ "kind": "drop", "version": 2 }),
        )])
        .await
        .expect("mutable update should succeed");

    let response = query(&engine, request("profiles", 3, None))
        .await
        .expect("query should succeed");
    assert_eq!(response.hits[0].record.pk.label(), "doc-01500");
    assert_eq!(response.hits[0].record.extra["version"], 2);
    assert_eq!(
        response
            .hits
            .iter()
            .filter(|hit| hit.record.pk.label() == "doc-01500")
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
    let root_dir = unique_temp_dir("query-graph-reopen");
    let root = root_dir.path().to_path_buf();
    let engine = engine(&root);
    create(&engine, "events").await;
    fill(&engine, "events").await;
    handle(&engine, "events")
        .flush()
        .await
        .expect("flush should succeed");
    handle(&engine, "events")
        .write(vec![put(
            "late",
            vector(424_242),
            json!({ "kind": "rare", "version": 1 }),
        )])
        .await
        .expect("write should succeed");
    handle(&engine, "events")
        .flush()
        .await
        .expect("flush should succeed");
    handle(&engine, "events")
        .compact()
        .await
        .expect("compaction should succeed");
    drop(engine);

    let reopened = self::engine(&root);
    let response = query(
        &reopened,
        request("events", 3, Some(FilterExpr::eq("kind", "rare"))),
    )
    .await
    .expect("query should succeed");
    assert_eq!(ids(&response), ["late"]);
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::PredicateFirstExact);
    assert_eq!(diagnostics.candidates_after_filter, 1);
}

fn ids(response: &QueryResponse) -> Vec<String> {
    response
        .hits
        .iter()
        .map(|hit| hit.record.pk.label())
        .collect()
}

/// A fresh temp directory named `logpose-{name}-…`, removed when the returned
/// guard drops, also when the test panics.
fn unique_temp_dir(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("logpose-{name}-"))
        .tempdir()
        .expect("temp dir should be created")
}

/// Ids of every live row whose `kind` passes `keep`, by exact dot product with `query`
/// (ties by id).
async fn exact_ranked_ids(
    engine: &Engine,
    collection_name: &str,
    query: &[f32],
    keep: impl Fn(&str) -> bool,
) -> Vec<String> {
    let view = engine
        .read_view(
            &CollectionRef::parse(collection_name).expect("name"),
            ReadOptions::default(),
        )
        .await
        .expect("view should open");
    let (rows, _) = scroll_view(
        &view,
        None,
        &ScrollOrder::Pk,
        u32::MAX,
        Projection::full(),
        None,
    )
    .await
    .expect("scan should succeed");
    let mut scored = rows
        .into_iter()
        .filter(|row| keep(row.record.extra["kind"].as_str().unwrap_or_default()))
        .map(|row| {
            (
                row.record.pk.label(),
                query
                    .iter()
                    .zip(&row.record.vectors["vector"])
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
