#![allow(missing_docs)]

use async_trait as _;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use logpose_catalog as _;
use logpose_query::{
    ExplainMode, MetadataFilter, QueryPlanKind, QueryRequest, ScalarMetadataValue, filter_records,
    query_exact,
};
use logpose_storage::{CreateCollectionRequest, LocalStorageEngine, StorageEngine};
use logpose_types::{DistanceMetric, PutRecord, RecordId, WriteOperation};
use serde as _;
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror as _;
use tokio::runtime::Runtime;

const DIMENSIONS: usize = 2;

struct BenchFixture {
    engine: LocalStorageEngine,
    request: QueryRequest,
    expected_plan: QueryPlanKind,
    oracle_ids: Vec<String>,
    min_recall: f32,
}

fn phase4_ann_benchmarks(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("runtime should build");
    let fixtures = vec![
        (
            "immutable_ann_unfiltered",
            build_immutable_ann_fixture(&runtime),
        ),
        (
            "immutable_ann_filtered",
            build_filtered_ann_fixture(&runtime),
        ),
        (
            "immutable_tiny_fallback",
            build_tiny_fallback_fixture(&runtime),
        ),
    ];

    for (name, fixture) in &fixtures {
        validate_fixture(&runtime, fixture);
        let mut group = criterion.benchmark_group(format!("phase4/{name}"));
        group.bench_function(
            BenchmarkId::new("exact_baseline", fixture.request.top_k),
            |bencher| {
                bencher.iter(|| {
                    let ids = exact_oracle_ids(&runtime, &fixture.engine, &fixture.request);
                    black_box(ids);
                });
            },
        );
        group.bench_function(
            BenchmarkId::new("planner_query", fixture.request.top_k),
            |bencher| {
                bencher.iter(|| {
                    let response = runtime
                        .block_on(query_exact(&fixture.engine, fixture.request.clone()))
                        .expect("query should succeed");
                    black_box(response);
                });
            },
        );
        group.finish();
    }
}

fn build_immutable_ann_fixture(runtime: &Runtime) -> BenchFixture {
    let engine = LocalStorageEngine::new(unique_temp_dir("phase4-immutable-ann"));
    create_collection(runtime, &engine, "immutable_ann");
    write_records(runtime, &engine, "immutable_ann", 1024, None);
    runtime
        .block_on(engine.flush("immutable_ann"))
        .expect("flush should succeed");

    let request = QueryRequest {
        collection_name: "immutable_ann".to_owned(),
        vector: vec![1.0, 0.0],
        top_k: 10,
        snapshot: None,
        filters: Vec::new(),
        predicate: None,
        explain: ExplainMode::None,
    };
    let oracle_ids = exact_oracle_ids(runtime, &engine, &request);

    BenchFixture {
        engine,
        request,
        expected_plan: QueryPlanKind::VectorFirstAnn,
        oracle_ids,
        min_recall: 0.9,
    }
}

fn build_filtered_ann_fixture(runtime: &Runtime) -> BenchFixture {
    let engine = LocalStorageEngine::new(unique_temp_dir("phase4-filtered-ann"));
    create_collection(runtime, &engine, "filtered_ann");
    write_tail_filtered_records(runtime, &engine, "filtered_ann", 768, "keep");
    runtime
        .block_on(engine.flush("filtered_ann"))
        .expect("flush should succeed");

    let request = QueryRequest {
        collection_name: "filtered_ann".to_owned(),
        vector: vec![1.0, 0.0],
        top_k: 10,
        snapshot: None,
        filters: vec![MetadataFilter {
            field: "bucket".to_owned(),
            value: ScalarMetadataValue::String("keep".to_owned()),
        }],
        predicate: None,
        explain: ExplainMode::None,
    };
    let oracle_ids = exact_oracle_ids(runtime, &engine, &request);

    BenchFixture {
        engine,
        request,
        expected_plan: QueryPlanKind::CooperativeFilteredAnn,
        oracle_ids,
        min_recall: 0.9,
    }
}

fn build_tiny_fallback_fixture(runtime: &Runtime) -> BenchFixture {
    let engine = LocalStorageEngine::new(unique_temp_dir("phase4-tiny-fallback"));
    create_collection(runtime, &engine, "tiny_fallback");
    write_records(runtime, &engine, "tiny_fallback", 8, Some("common"));
    runtime
        .block_on(engine.write(
            "tiny_fallback",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("rare-top"),
                vector: vec![100.0, 0.0],
                metadata: serde_json::json!({"bucket":"rare","ordinal":9999}),
            })],
        ))
        .expect("rare write should succeed");
    runtime
        .block_on(engine.flush("tiny_fallback"))
        .expect("flush should succeed");

    let request = QueryRequest {
        collection_name: "tiny_fallback".to_owned(),
        vector: vec![1.0, 0.0],
        top_k: 3,
        snapshot: None,
        filters: vec![MetadataFilter {
            field: "bucket".to_owned(),
            value: ScalarMetadataValue::String("rare".to_owned()),
        }],
        predicate: None,
        explain: ExplainMode::None,
    };
    let oracle_ids = exact_oracle_ids(runtime, &engine, &request);

    BenchFixture {
        engine,
        request,
        expected_plan: QueryPlanKind::TinyPopulationExactFallback,
        oracle_ids,
        min_recall: 1.0,
    }
}

fn validate_fixture(runtime: &Runtime, fixture: &BenchFixture) {
    let response = runtime
        .block_on(query_exact(
            &fixture.engine,
            QueryRequest {
                explain: ExplainMode::Profile,
                ..fixture.request.clone()
            },
        ))
        .expect("profile query should succeed");
    let diagnostics = response
        .diagnostics
        .as_ref()
        .expect("profile query should return diagnostics");
    assert_eq!(diagnostics.chosen_plan, fixture.expected_plan);

    let actual_ids = response
        .matches
        .iter()
        .map(|candidate| candidate.id.as_str().to_owned())
        .collect::<Vec<_>>();
    let recall = recall_at_k(&fixture.oracle_ids, &actual_ids);
    assert!(
        recall >= fixture.min_recall,
        "recall below envelope for {}: {:.2}",
        fixture.request.collection_name,
        recall
    );
}

fn create_collection(runtime: &Runtime, engine: &LocalStorageEngine, collection_name: &str) {
    runtime
        .block_on(engine.create_collection(CreateCollectionRequest::new(
            collection_name,
            DIMENSIONS,
            DistanceMetric::Dot,
        )))
        .expect("collection should be created");
}

fn write_records(
    runtime: &Runtime,
    engine: &LocalStorageEngine,
    collection_name: &str,
    count: usize,
    filtered_bucket: Option<&str>,
) {
    let operations = (0..count)
        .map(|index| {
            let bucket = match filtered_bucket {
                Some(value) if index % 3 == 0 => value,
                Some(_) => "other",
                None => "all",
            };
            WriteOperation::Put(PutRecord {
                id: RecordId::new(format!("doc-{index:04}")),
                vector: vec![index as f32 + 1.0, ((index * 7) % 13) as f32 / 10.0],
                metadata: serde_json::json!({
                    "bucket": bucket,
                    "ordinal": index,
                }),
            })
        })
        .collect::<Vec<_>>();
    runtime
        .block_on(engine.write(collection_name, operations))
        .expect("writes should succeed");
}

fn write_tail_filtered_records(
    runtime: &Runtime,
    engine: &LocalStorageEngine,
    collection_name: &str,
    count: usize,
    keep_bucket: &str,
) {
    let keep_start = (count * 7) / 12;
    let operations = (0..count)
        .map(|index| {
            let bucket = if index >= keep_start {
                keep_bucket
            } else {
                "other"
            };
            WriteOperation::Put(PutRecord {
                id: RecordId::new(format!("doc-{index:04}")),
                vector: vec![index as f32 + 1.0, ((index * 7) % 13) as f32 / 10.0],
                metadata: serde_json::json!({
                    "bucket": bucket,
                    "ordinal": index,
                }),
            })
        })
        .collect::<Vec<_>>();
    runtime
        .block_on(engine.write(collection_name, operations))
        .expect("writes should succeed");
}

fn exact_oracle_ids(
    runtime: &Runtime,
    engine: &LocalStorageEngine,
    request: &QueryRequest,
) -> Vec<String> {
    let mut records = runtime
        .block_on(engine.scan_exact(&request.collection_name, request.snapshot.clone()))
        .expect("scan should succeed");
    if !request.filters.is_empty() {
        records = filter_records(records, &request.filters);
    }

    let mut scored = records
        .into_iter()
        .map(|record| {
            let value = request.vector[0] * record.vector[0] + request.vector[1] * record.vector[1];
            (record.id.as_str().to_owned(), value)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored
        .into_iter()
        .take(request.top_k)
        .map(|(id, _)| id)
        .collect()
}

fn recall_at_k(expected: &[String], actual: &[String]) -> f32 {
    if expected.is_empty() {
        return 1.0;
    }
    let hits = expected.iter().filter(|id| actual.contains(*id)).count();
    hits as f32 / expected.len() as f32
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    std::env::temp_dir().join(format!("logpose-{prefix}-{unique}"))
}

criterion_group!(benches, phase4_ann_benchmarks);
criterion_main!(benches);
