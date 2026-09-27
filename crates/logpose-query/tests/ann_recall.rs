//! End-to-end ANN recall through `LocalStorageEngine` and `query_exact` on clustered data.
//!
//! Clustered vectors are the case where a badly built HNSW graph splits into one island per
//! cluster, so these tests compare every ANN answer against a brute-force oracle.

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
    collections::HashSet,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror as _;

const TOP_K: usize = 10;

#[tokio::test]
async fn ann_plans_keep_high_recall_on_clustered_multi_segment_data() {
    let scenario = RecallScenario {
        name: "query-ann-recall",
        rows_per_segment: 1_000,
        segments: 3,
        dimensions: 32,
        clusters: 16,
        queries: 40,
    };
    let recall = scenario.run().await;
    assert!(
        recall.unfiltered >= 0.95,
        "unfiltered ANN recall@10 was {}",
        recall.unfiltered
    );
    assert!(
        recall.filtered >= 0.9,
        "filtered ANN recall@10 was {}",
        recall.filtered
    );
}

/// Bench-shaped variant: 128 dimensions and 5,000-row segments. Run it in release with
/// `cargo test --release -p logpose-query --test ann_recall -- --ignored`.
#[tokio::test]
#[ignore = "slow in debug builds; run with --release"]
async fn ann_plans_keep_high_recall_on_large_clustered_segments() {
    let scenario = RecallScenario {
        name: "query-ann-recall-large",
        rows_per_segment: 5_000,
        segments: 4,
        dimensions: 128,
        clusters: 32,
        queries: 100,
    };
    let recall = scenario.run().await;
    assert!(
        recall.unfiltered >= 0.95,
        "unfiltered ANN recall@10 was {}",
        recall.unfiltered
    );
    assert!(
        recall.filtered >= 0.9,
        "filtered ANN recall@10 was {}",
        recall.filtered
    );
}

struct RecallScenario {
    name: &'static str,
    rows_per_segment: usize,
    segments: usize,
    dimensions: usize,
    clusters: usize,
    queries: usize,
}

struct Recall {
    unfiltered: f64,
    filtered: f64,
}

struct Row {
    id: String,
    vector: Vec<f32>,
    flagged: bool,
}

impl RecallScenario {
    async fn run(&self) -> Recall {
        let root = unique_temp_dir(self.name);
        let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
        engine
            .create_collection(CreateCollectionRequest::new(
                "documents",
                self.dimensions,
                DistanceMetric::L2,
            ))
            .await
            .expect("collection should be created");

        let mut rng = TestRng::new(42);
        let centers = (0..self.clusters)
            .map(|_| {
                (0..self.dimensions)
                    .map(|_| (rng.next_unit() as f32 * 2.0 - 1.0) * 5.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::with_capacity(self.rows_per_segment * self.segments);
        for segment in 0..self.segments {
            let batch = (0..self.rows_per_segment)
                .map(|offset| {
                    let index = segment * self.rows_per_segment + offset;
                    Row {
                        id: format!("row-{index}"),
                        vector: sample_near(&mut rng, &centers),
                        // Uncorrelated with the clusters, so about 10 percent of every cluster.
                        flagged: rng.next_u64() % 10 == 0,
                    }
                })
                .collect::<Vec<_>>();
            engine
                .write(
                    "documents",
                    batch
                        .iter()
                        .map(|row| {
                            WriteOperation::Put(PutRecord {
                                id: RecordId::new(row.id.clone()),
                                vector: row.vector.clone(),
                                metadata: json!({
                                    "flag": if row.flagged { "hit" } else { "miss" },
                                }),
                            })
                        })
                        .collect(),
                )
                .await
                .expect("write should succeed");
            engine
                .flush("documents")
                .await
                .expect("flush should succeed");
            rows.extend(batch);
        }

        let queries = (0..self.queries)
            .map(|_| sample_near(&mut rng, &centers))
            .collect::<Vec<_>>();
        let mut unfiltered = 0.0;
        let mut filtered = 0.0;
        for query in &queries {
            unfiltered += query_recall(&engine, &rows, query, false).await;
            filtered += query_recall(&engine, &rows, query, true).await;
        }

        let _ = fs::remove_dir_all(&root);
        let recall = Recall {
            unfiltered: unfiltered / queries.len() as f64,
            filtered: filtered / queries.len() as f64,
        };
        eprintln!(
            "{}: recall@{TOP_K} unfiltered {:.3}, filtered {:.3}",
            self.name, recall.unfiltered, recall.filtered
        );
        recall
    }
}

async fn query_recall(
    engine: &LocalStorageEngine,
    rows: &[Row],
    query: &[f32],
    filtered: bool,
) -> f64 {
    let predicate = filtered.then(|| {
        Predicate::Comparison(PredicateComparison {
            field: "flag".to_owned(),
            operator: PredicateOperator::Eq,
            value: Some(ScalarMetadataValue::String("hit".to_owned())),
        })
    });
    let response = query_exact(
        engine,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: query.to_vec(),
            top_k: TOP_K,
            snapshot: None,
            read_barrier: None,
            filters: Vec::new(),
            predicate,
            explain: ExplainMode::Profile,
        },
    )
    .await
    .expect("query should succeed");
    let plan = response
        .diagnostics
        .as_ref()
        .expect("diagnostics should be present")
        .chosen_plan;
    let expected_plan = if filtered {
        QueryPlanKind::CooperativeFilteredAnn
    } else {
        QueryPlanKind::VectorFirstAnn
    };
    assert_eq!(plan, expected_plan, "query should take an HNSW plan");

    let mut exact = rows
        .iter()
        .filter(|row| !filtered || row.flagged)
        .map(|row| (l2(query, &row.vector), row.id.as_str()))
        .collect::<Vec<_>>();
    exact.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(right.1)));
    let truth = exact
        .iter()
        .take(TOP_K)
        .map(|(_, id)| *id)
        .collect::<HashSet<_>>();
    let found = response
        .matches
        .iter()
        .filter(|candidate| truth.contains(candidate.id.as_str()))
        .count();
    found as f64 / truth.len() as f64
}

fn sample_near(rng: &mut TestRng, centers: &[Vec<f32>]) -> Vec<f32> {
    let center = &centers[(rng.next_u64() % centers.len() as u64) as usize];
    center
        .iter()
        .map(|value| value + rng.next_gaussian())
        .collect()
}

fn l2(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(lhs, rhs)| (lhs - rhs) * (lhs - rhs))
        .sum::<f32>()
        .sqrt()
}

/// Deterministic SplitMix64 generator so the test needs no extra dependency.
struct TestRng(u64);

impl TestRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    /// Uniform value in (0, 1].
    fn next_unit(&mut self) -> f64 {
        ((self.next_u64() >> 11) + 1) as f64 / (1u64 << 53) as f64
    }

    fn next_gaussian(&mut self) -> f32 {
        let radius = (-2.0 * self.next_unit().ln()).sqrt();
        let angle = std::f64::consts::TAU * self.next_unit();
        (radius * angle.cos()) as f32
    }
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
