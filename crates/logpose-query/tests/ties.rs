//! Searches over rows that tie in value: every row's vector comes from a pool of three, so
//! hundreds of rows share each exact value, spread over several segments and a memtable in an
//! order unrelated to their keys, with overwrites and deletes. Results with equal values are
//! ordered by key, so a search returns exactly the model's first `k` rows by `(value, key)`,
//! whatever the strategy, the rerank factor, and whether it ran in parallel.

use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{FilterExpr, Force, SearchRequest, SearchTuning, metric_value, search};
use logpose_storage::IndexPolicy;
use logpose_types::{
    DistanceMetric,
    record::{ClientOp, PrimaryKey},
    schema::FieldType,
    value::Value,
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json as _;
use std::collections::BTreeMap;
use thiserror as _;

mod support;

use support::{Fixture, Rng, record};

const DIMS: usize = 4;

/// A row of the model: its pool vector and its bucket.
#[derive(Clone, Copy, Debug)]
struct Row {
    vector: usize,
    bucket: i64,
}

fn key(index: u64) -> String {
    format!("t{index:05}")
}

/// What a search must match beyond parallel and sequential runs agreeing.
#[derive(Clone, Copy, Debug)]
enum Expect {
    /// The model's keys, in order.
    Keys,
    /// The model's values in order (rows of one pool vector), whichever tied rows.
    Vectors,
    /// Nothing else.
    Agreement,
}

struct Scenario {
    fixture: Fixture,
    metric: DistanceMetric,
    pool: Vec<Vec<f32>>,
    model: BTreeMap<String, Row>,
    rng: Rng,
    keys: u64,
}

impl Scenario {
    async fn new(label: &str, seed: u64, metric: DistanceMetric, policy: IndexPolicy) -> Self {
        let fixture =
            Fixture::new(label, DIMS, metric, policy, &[("bucket", FieldType::Int64)]).await;
        let mut rng = Rng::new(seed);
        let pool = (0..3).map(|_| rng.vector(DIMS)).collect();
        Self {
            fixture,
            metric,
            pool,
            model: BTreeMap::new(),
            rng,
            keys: 900,
        }
    }

    /// Upsert `count` random keys (new or overwritten) in random order.
    async fn upsert(&mut self, count: usize) {
        let mut batch = BTreeMap::new();
        for _ in 0..count {
            let row = Row {
                vector: self.rng.below(self.pool.len() as u64) as usize,
                bucket: self.rng.below(4) as i64,
            };
            batch.insert(key(self.rng.below(self.keys)), row);
        }
        // Rows land in the order written: shuffle so row order is not key order.
        let mut records = batch
            .iter()
            .map(|(key, row)| {
                record(
                    key,
                    self.pool[row.vector].clone(),
                    &[("bucket", Value::Int64(row.bucket))],
                )
            })
            .collect::<Vec<_>>();
        for index in (1..records.len()).rev() {
            let other = self.rng.below(index as u64 + 1) as usize;
            records.swap(index, other);
        }
        self.fixture.upsert(records).await;
        self.model.extend(batch);
    }

    async fn delete(&mut self, count: usize) {
        let keys = (0..count)
            .map(|_| key(self.rng.below(self.keys)))
            .collect::<std::collections::BTreeSet<_>>();
        let ops = keys
            .iter()
            .map(|key| ClientOp::Delete(PrimaryKey::from(key.as_str())))
            .collect();
        self.fixture.handle.write(ops).await.expect("delete");
        for key in keys {
            self.model.remove(&key);
        }
    }

    /// Several segments (flushed at different times, so they overlap in keys) and a memtable.
    async fn populate(&mut self) {
        for _ in 0..3 {
            self.upsert(350).await;
            self.delete(40).await;
            self.fixture.flush().await;
        }
        self.upsert(120).await;
        self.delete(30).await;
    }

    /// The model's first `k` rows by `(value, key)`: their keys and pool vectors.
    fn expected(&self, query: &[f32], filter: Option<i64>, k: usize) -> Vec<(String, usize)> {
        let values = self
            .pool
            .iter()
            .map(|vector| metric_value(self.metric, query, vector))
            .collect::<Vec<_>>();
        let mut rows = self
            .model
            .iter()
            .filter(|(_, row)| filter.is_none_or(|bucket| row.bucket == bucket))
            .map(|(key, row)| (values[row.vector], key.clone(), row.vector))
            .collect::<Vec<_>>();
        // Best value first, then key (string keys order as their text).
        rows.sort_by(|left, right| {
            let by_value = match self.metric {
                DistanceMetric::L2 => left.0.total_cmp(&right.0),
                DistanceMetric::Cosine | DistanceMetric::Dot => right.0.total_cmp(&left.0),
            };
            by_value.then_with(|| left.1.cmp(&right.1))
        });
        rows.into_iter()
            .take(k)
            .map(|(_, key, vector)| (key, vector))
            .collect()
    }

    /// Search with every strategy, rerank factor, and parallelism: parallel and sequential
    /// runs always agree, and `expect` says what else must match the model.
    async fn check(&mut self, forces: &[Force], expect: Expect) {
        let view = self.fixture.view().await;
        for _ in 0..4 {
            let query = self.rng.vector(DIMS);
            for filter in [None, Some(1_i64)] {
                for k in [1, 5, 40] {
                    let expected = self.expected(&query, filter, k);
                    for &force in forces {
                        for rerank_factor in [1, 4] {
                            let mut runs = Vec::new();
                            for parallel in [true, false] {
                                let request = SearchRequest {
                                    filter: filter.map(|bucket| FilterExpr::eq("bucket", bucket)),
                                    tuning: SearchTuning {
                                        force,
                                        parallel,
                                        rerank_factor,
                                        ..SearchTuning::default()
                                    },
                                    ..SearchRequest::new(query.clone(), k)
                                };
                                let outcome = search(&view, &request).await.expect("search");
                                let keys = outcome
                                    .hits
                                    .iter()
                                    .map(|hit| match &hit.row.record.pk {
                                        PrimaryKey::String(key) => key.clone(),
                                        other => unreachable!("{other:?}"),
                                    })
                                    .collect::<Vec<_>>();
                                let context = format!(
                                    "{:?} force={force:?} parallel={parallel} \
                                     rerank_factor={rerank_factor} k={k} filter={filter:?} \
                                     strategies={:?}",
                                    self.metric,
                                    outcome
                                        .units
                                        .iter()
                                        .map(|unit| unit.strategy.name())
                                        .collect::<Vec<_>>()
                                );
                                match expect {
                                    Expect::Keys => assert_eq!(
                                        keys,
                                        expected
                                            .iter()
                                            .map(|(key, _)| key.clone())
                                            .collect::<Vec<_>>(),
                                        "{context}"
                                    ),
                                    Expect::Vectors => assert_eq!(
                                        keys.iter()
                                            .map(|key| self.model[key].vector)
                                            .collect::<Vec<_>>(),
                                        expected
                                            .iter()
                                            .map(|(_, vector)| *vector)
                                            .collect::<Vec<_>>(),
                                        "{context}"
                                    ),
                                    Expect::Agreement => {}
                                }
                                runs.push((keys, context));
                            }
                            assert_eq!(runs[0].0, runs[1].0, "{}", runs[0].1);
                        }
                    }
                }
            }
        }
    }
}

/// Segments without vector indexes are scanned exactly in f32: ties are cut by key.
#[tokio::test]
async fn exact_scans_order_tied_rows_by_key() {
    for metric in [
        DistanceMetric::L2,
        DistanceMetric::Dot,
        DistanceMetric::Cosine,
    ] {
        let mut scenario = Scenario::new(
            "ties-f32",
            7,
            metric,
            IndexPolicy {
                graph_min_rows: u32::MAX,
                sq8_min_rows: u32::MAX,
                ..IndexPolicy::default()
            },
        )
        .await;
        scenario.populate().await;
        scenario
            .check(&[Force::Auto, Force::Exact], Expect::Keys)
            .await;
    }
}

/// Segments with SQ8 codes scanned exactly return the best values. Which of the rows tied in
/// value survive is the SQ8 cut's choice (rows with one vector share one code, and that cut
/// orders equal estimates by row, not key, as it always has): keys are not checked.
#[tokio::test]
async fn sq8_scans_return_the_best_values_over_ties() {
    for metric in [DistanceMetric::L2, DistanceMetric::Dot] {
        let mut scenario = Scenario::new(
            "ties-sq8",
            8,
            metric,
            IndexPolicy {
                graph_min_rows: u32::MAX,
                sq8_min_rows: 16,
                ..IndexPolicy::default()
            },
        )
        .await;
        scenario.populate().await;
        scenario
            .check(&[Force::Auto, Force::Exact], Expect::Vectors)
            .await;
    }
}

/// Graph segments: parallel and sequential runs agree for every strategy.
#[tokio::test]
async fn graph_searches_over_ties_agree_in_parallel_and_in_sequence() {
    let mut scenario = Scenario::new(
        "ties-graph",
        9,
        DistanceMetric::L2,
        IndexPolicy {
            graph_min_rows: 40,
            sq8_min_rows: 16,
            ..IndexPolicy::default()
        },
    )
    .await;
    scenario.populate().await;
    scenario
        .check(
            &[
                Force::Auto,
                Force::Exact,
                Force::Walk,
                Force::Admit,
                Force::Acorn,
            ],
            Expect::Agreement,
        )
        .await;
}

/// A segment large enough that its exact scan splits into parallel morsels: the morsels' cuts
/// merge to exactly the sequential scan's.
#[tokio::test]
async fn parallel_morsels_over_ties_equal_a_sequential_scan() {
    let mut scenario = Scenario::new(
        "ties-morsels",
        10,
        DistanceMetric::Dot,
        IndexPolicy {
            graph_min_rows: u32::MAX,
            sq8_min_rows: 16,
            ..IndexPolicy::default()
        },
    )
    .await;
    scenario.keys = 60_000;
    for _ in 0..8 {
        scenario.upsert(4_000).await;
    }
    scenario.delete(500).await;
    scenario.fixture.flush().await;
    scenario.fixture.compact().await;
    let view = scenario.fixture.view().await;
    let outcome = search(
        &view,
        &SearchRequest {
            tuning: SearchTuning {
                force: Force::Exact,
                ..SearchTuning::default()
            },
            ..SearchRequest::new(scenario.rng.vector(DIMS), 10)
        },
    )
    .await
    .expect("search");
    let plan = outcome.plan.render(false);
    assert!(plan.contains("morsels="), "{plan}");
    scenario.check(&[Force::Exact], Expect::Vectors).await;
}
