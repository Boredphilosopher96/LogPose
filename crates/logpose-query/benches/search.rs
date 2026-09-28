//! Staged vector search over segments with graphs and SQ8 codes: unfiltered, a random 10
//! percent filter, and a memtable merge.

#![allow(missing_docs)]

use async_trait as _;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    ExplainMode, FilterComparison, FilterExpr, FilterOperator, QueryRequest, ScalarMetadataValue,
    query,
};
use logpose_storage::{
    CreateCollectionRequest, EngineConfig, IndexPolicy, LocalStorageEngine, StorageEngine,
};
use logpose_types::{DistanceMetric, PutRecord, RecordId, WriteOperation};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror as _;
use tokio::runtime::Runtime;

const ROWS: usize = 20_000;
const DIMENSIONS: usize = 64;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        ((self.0 >> 40) as f32 / (1_u64 << 24) as f32) * 2.0 - 1.0
    }
}

fn setup(runtime: &Runtime) -> LocalStorageEngine {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("logpose-search-bench-{unique}"));
    let engine = LocalStorageEngine::with_config(
        &root,
        EngineConfig {
            index: IndexPolicy {
                graph_min_rows: 5_000,
                ..IndexPolicy::default()
            },
            ..EngineConfig::default()
        },
    )
    .expect("engine");
    runtime.block_on(async {
        engine
            .create_collection(CreateCollectionRequest::new(
                "bench",
                DIMENSIONS,
                DistanceMetric::L2,
            ))
            .await
            .expect("create");
        let mut rng = Rng(7);
        for chunk in 0..ROWS / 1_000 {
            let operations = (0..1_000)
                .map(|offset| {
                    let index = chunk * 1_000 + offset;
                    WriteOperation::Put(PutRecord {
                        id: RecordId::new(format!("row-{index:06}")),
                        vector: (0..DIMENSIONS).map(|_| rng.next()).collect(),
                        metadata: json!({ "bucket": index % 10 }),
                    })
                })
                .collect();
            engine.write("bench", operations).await.expect("write");
        }
        engine.flush("bench").await.expect("flush");
        engine.compact("bench").await.expect("compact");
    });
    engine
}

fn request(vector: Vec<f32>, filtered: bool) -> QueryRequest {
    QueryRequest {
        collection_name: "bench".to_owned(),
        vector,
        top_k: 10,
        snapshot: None,
        read_barrier: None,
        filters: Vec::new(),
        predicate: filtered.then(|| {
            FilterExpr::Comparison(FilterComparison {
                field: "bucket".to_owned(),
                operator: FilterOperator::Eq,
                value: Some(ScalarMetadataValue::Number(3.into())),
            })
        }),
        explain: ExplainMode::None,
        snapshot_token: None,
        pin: false,
    }
}

fn search_benchmarks(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("runtime");
    let engine = setup(&runtime);
    let mut rng = Rng(11);
    let queries = (0..64)
        .map(|_| (0..DIMENSIONS).map(|_| rng.next()).collect::<Vec<f32>>())
        .collect::<Vec<_>>();
    for (name, filtered) in [("unfiltered", false), ("dynamic_filter_10pct", true)] {
        let mut next = 0;
        criterion.bench_function(name, |bench| {
            bench.iter(|| {
                next = (next + 1) % queries.len();
                let response = runtime
                    .block_on(query(&engine, request(queries[next].clone(), filtered)))
                    .expect("query");
                black_box(response.matches.len())
            });
        });
    }
}

criterion_group!(benches, search_benchmarks);
criterion_main!(benches);
