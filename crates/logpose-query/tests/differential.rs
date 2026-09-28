//! Adversarial differential test of vector search against a brute-force model, for every
//! metric: random upserts (a third of them reusing a small pool of vectors, so graph nodes
//! stand for several rows), partial updates that replace a vector or a field of a row whose
//! old version sits in an indexed segment, deletes, filter writes, flushes, and compactions,
//! with index thresholds low enough that segments get SQ8 codes and graphs.
//!
//! Every search checks, whatever strategy each unit took: no key twice; every hit a live row
//! of the model that matches the filter, read at its latest version; every reported value the
//! exact metric value of the query against that row; hits best first. Segments without vector
//! indexes must return the exact top k; indexed ones must keep recall.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    FilterComparison, FilterExpr, FilterOperator, ScalarMetadataValue, SearchRequest, SearchTuning,
    UnitStrategy, count_view, metric_value, search,
};
use logpose_storage::{IndexPolicy, Projection};
use logpose_types::{
    DistanceMetric,
    record::{ClientOp, PartialUpdate, PrimaryKey, Record},
    schema::FieldType,
    value::Value,
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json as _;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use thiserror as _;

mod support;

use support::{Fixture, Rng};

const DIMS: usize = 6;
const KEYS: u64 = 300;
const WORDS: [&str; 4] = ["ant", "bee", "cat", "dog"];

#[derive(Clone, Debug, PartialEq)]
struct Row {
    vector: Vec<f32>,
    n: Option<i64>,
    s: Option<String>,
}

fn key(index: u64) -> String {
    format!("k{index:04}")
}

fn record(key: &str, row: &Row) -> Record {
    let mut record = Record::new(key).with_vector("vector", row.vector.clone());
    if let Some(n) = row.n {
        record = record.with_field("n", Value::Int64(n));
    }
    if let Some(s) = &row.s {
        record = record.with_field("s", Value::String(s.clone()));
    }
    record
}

fn comparison(
    field: &str,
    operator: FilterOperator,
    value: Option<ScalarMetadataValue>,
) -> FilterExpr {
    FilterExpr::Comparison(FilterComparison {
        field: field.to_owned(),
        operator,
        value,
    })
}

fn random_comparison(rng: &mut Rng) -> FilterExpr {
    let operators = [
        FilterOperator::Eq,
        FilterOperator::Ne,
        FilterOperator::Lt,
        FilterOperator::Gte,
    ];
    match rng.below(7) {
        0 => comparison("n", FilterOperator::IsNull, None),
        1 => comparison("s", FilterOperator::Exists, None),
        2 | 3 => comparison(
            "s",
            [FilterOperator::Eq, FilterOperator::Ne][rng.below(2) as usize],
            Some(ScalarMetadataValue::String(
                WORDS[rng.below(4) as usize].to_owned(),
            )),
        ),
        _ => comparison(
            "n",
            operators[rng.below(4) as usize],
            Some(ScalarMetadataValue::Number(
                (rng.below(12) as i64 - 1).into(),
            )),
        ),
    }
}

fn random_filter(rng: &mut Rng) -> FilterExpr {
    match rng.below(6) {
        0 => FilterExpr::And {
            children: vec![random_comparison(rng), random_comparison(rng)],
        },
        1 => FilterExpr::Or {
            children: vec![random_comparison(rng), random_comparison(rng)],
        },
        2 => FilterExpr::Not {
            child: Box::new(random_comparison(rng)),
        },
        _ => random_comparison(rng),
    }
}

fn matches(filter: &FilterExpr, row: &Row) -> bool {
    match filter {
        FilterExpr::And { children } => children.iter().all(|child| matches(child, row)),
        FilterExpr::Or { children } => children.iter().any(|child| matches(child, row)),
        FilterExpr::Not { child } => !matches(child, row),
        FilterExpr::Comparison(comparison) => {
            let operand = comparison.value.as_ref();
            match comparison.field.as_str() {
                "n" => {
                    let wanted = match operand {
                        Some(ScalarMetadataValue::Number(number)) => number.as_i64(),
                        _ => None,
                    };
                    match comparison.operator {
                        FilterOperator::IsNull => row.n.is_none(),
                        FilterOperator::Exists => row.n.is_some(),
                        FilterOperator::Eq => row.n.zip(wanted).is_some_and(|(v, w)| v == w),
                        FilterOperator::Ne => row.n.zip(wanted).is_some_and(|(v, w)| v != w),
                        FilterOperator::Lt => row.n.zip(wanted).is_some_and(|(v, w)| v < w),
                        FilterOperator::Gte => row.n.zip(wanted).is_some_and(|(v, w)| v >= w),
                        other => unreachable!("{other:?}"),
                    }
                }
                _ => {
                    let wanted = match operand {
                        Some(ScalarMetadataValue::String(text)) => Some(text.as_str()),
                        _ => None,
                    };
                    let value = row.s.as_deref();
                    match comparison.operator {
                        FilterOperator::IsNull => value.is_none(),
                        FilterOperator::Exists => value.is_some(),
                        FilterOperator::Eq => value.zip(wanted).is_some_and(|(v, w)| v == w),
                        FilterOperator::Ne => value.zip(wanted).is_some_and(|(v, w)| v != w),
                        other => unreachable!("{other:?}"),
                    }
                }
            }
        }
    }
}

/// How a metric value compares: `true` when `left` is at least as good as `right`, within
/// `eps`.
fn at_least_as_good(metric: DistanceMetric, left: f32, right: f32, eps: f32) -> bool {
    match metric {
        DistanceMetric::L2 => left <= right + eps,
        DistanceMetric::Cosine | DistanceMetric::Dot => left + eps >= right,
    }
}

fn close(left: f32, right: f32) -> bool {
    (left - right).abs() <= 1e-3 * left.abs().max(right.abs()).max(1.0)
}

fn unit_vector(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    vector.iter().map(|v| v / norm).collect()
}

struct Scenario {
    fixture: Fixture,
    metric: DistanceMetric,
    model: BTreeMap<String, Row>,
    rng: Rng,
    pool: Vec<Vec<f32>>,
    indexed: bool,
    /// Hits at least as good as the model's k-th, and hits wanted.
    recall: (usize, usize),
    strategies: BTreeSet<&'static str>,
    searches: usize,
}

impl Scenario {
    async fn new(seed: u64, metric: DistanceMetric, indexed: bool) -> Self {
        let policy = if indexed {
            IndexPolicy {
                graph_min_rows: 40,
                sq8_min_rows: 16,
                ..IndexPolicy::default()
            }
        } else {
            IndexPolicy {
                graph_min_rows: u32::MAX,
                sq8_min_rows: u32::MAX,
                ..IndexPolicy::default()
            }
        };
        let fixture = Fixture::new(
            "differential",
            DIMS,
            metric,
            policy,
            &[("n", FieldType::Int64), ("s", FieldType::String)],
        )
        .await;
        let mut rng = Rng::new(seed);
        let pool = (0..5).map(|_| rng.vector(DIMS)).collect();
        Self {
            fixture,
            metric,
            model: BTreeMap::new(),
            rng,
            pool,
            indexed,
            recall: (0, 0),
            strategies: BTreeSet::new(),
            searches: 0,
        }
    }

    fn random_vector(&mut self) -> Vec<f32> {
        if self.rng.below(3) == 0 {
            self.pool[self.rng.below(self.pool.len() as u64) as usize].clone()
        } else {
            self.rng.vector(DIMS)
        }
    }

    fn random_row(&mut self) -> Row {
        Row {
            vector: self.random_vector(),
            n: (self.rng.below(5) != 0).then(|| self.rng.below(10) as i64),
            s: (self.rng.below(5) != 0).then(|| WORDS[self.rng.below(4) as usize].to_owned()),
        }
    }

    async fn step(&mut self) {
        match self.rng.below(100) {
            0..40 => {
                let mut batch = BTreeMap::new();
                for _ in 0..=self.rng.below(30) {
                    let row = self.random_row();
                    batch.insert(key(self.rng.below(KEYS)), row);
                }
                let records = batch.iter().map(|(key, row)| record(key, row)).collect();
                self.fixture.upsert(records).await;
                self.model.extend(batch);
            }
            40..50 => {
                // Partial updates of live keys: a new vector or a new `n`.
                let live: Vec<String> = self.model.keys().cloned().collect();
                if live.is_empty() {
                    return;
                }
                let mut ops = Vec::new();
                let mut touched = BTreeSet::new();
                for _ in 0..=self.rng.below(10) {
                    let key = live[self.rng.below(live.len() as u64) as usize].clone();
                    if !touched.insert(key.clone()) {
                        continue;
                    }
                    let mut patch = PartialUpdate::new(key.as_str());
                    let row = self.model.get_mut(&key).expect("live key");
                    if self.rng.below(2) == 0 {
                        let vector = if self.rng.below(3) == 0 {
                            self.pool[self.rng.below(self.pool.len() as u64) as usize].clone()
                        } else {
                            self.rng.vector(DIMS)
                        };
                        patch.vectors.insert("vector".to_owned(), vector.clone());
                        row.vector = vector;
                    } else {
                        let n = self.rng.below(10) as i64;
                        patch.fields.insert("n".to_owned(), Value::Int64(n));
                        row.n = Some(n);
                    }
                    ops.push(ClientOp::Update(patch));
                }
                self.fixture.handle.write(ops).await.expect("update");
            }
            50..60 => {
                let keys = (0..=self.rng.below(10))
                    .map(|_| key(self.rng.below(KEYS)))
                    .collect::<BTreeSet<_>>();
                let ops = keys
                    .iter()
                    .map(|key| ClientOp::Delete(PrimaryKey::from(key.as_str())))
                    .collect();
                self.fixture.handle.write(ops).await.expect("delete");
                for key in keys {
                    self.model.remove(&key);
                }
            }
            60..63 => {
                let filter = random_filter(&mut self.rng);
                let expected: Vec<String> = self
                    .model
                    .iter()
                    .filter(|(_, row)| matches(&filter, row))
                    .map(|(key, _)| key.clone())
                    .collect();
                let ack = self
                    .fixture
                    .handle
                    .delete_by_filter(filter.clone())
                    .await
                    .expect("delete by filter");
                assert_eq!(ack.applied_ops, expected.len(), "delete by {filter:?}");
                for key in expected {
                    self.model.remove(&key);
                }
            }
            63..66 => {
                let filter = random_filter(&mut self.rng);
                let s = WORDS[self.rng.below(4) as usize];
                let mut patch = PartialUpdate::new("ignored");
                patch
                    .fields
                    .insert("s".to_owned(), Value::String(s.to_owned()));
                let ack = self
                    .fixture
                    .handle
                    .update_by_filter(filter.clone(), patch)
                    .await
                    .expect("update by filter");
                let mut updated = 0;
                for row in self.model.values_mut() {
                    if matches(&filter, row) {
                        row.s = Some(s.to_owned());
                        updated += 1;
                    }
                }
                assert_eq!(ack.applied_ops, updated, "update by {filter:?}");
            }
            66..76 => self.fixture.flush().await,
            76..80 => self.fixture.compact().await,
            _ => self.check().await,
        }
    }

    async fn check(&mut self) {
        let view = self.fixture.view().await;
        assert_eq!(
            count_view(&view, None).await.expect("count"),
            self.model.len() as u64
        );
        let filter = random_filter(&mut self.rng);
        assert_eq!(
            count_view(&view, Some(&filter)).await.expect("count"),
            self.model
                .values()
                .filter(|row| matches(&filter, row))
                .count() as u64,
            "count {filter:?}"
        );
        for _ in 0..6 {
            self.search_once(&view).await;
        }
    }

    async fn search_once(&mut self, view: &logpose_storage::ReadView) {
        let metric = self.metric;
        let query = if self.rng.below(4) == 0 {
            // Exactly a pooled vector: many rows tie at the best value.
            self.pool[self.rng.below(self.pool.len() as u64) as usize].clone()
        } else {
            self.rng.vector(DIMS)
        };
        let filter = (self.rng.below(3) != 0).then(|| random_filter(&mut self.rng));
        let k = 1 + self.rng.below(15) as usize;
        let tuning = SearchTuning {
            exact_max_matches: [0, 1, 8, 64, 2048][self.rng.below(5) as usize],
            rerank_factor: [1, 4][self.rng.below(2) as usize],
            ef_escalation: self.rng.below(2) == 0,
            ..SearchTuning::default()
        };
        let ef = [None, Some(1), Some(8)][self.rng.below(3) as usize];
        let request = SearchRequest {
            filter: filter.clone(),
            tuning,
            ef,
            projection: Projection::full(),
            ..SearchRequest::new(query.clone(), k)
        };
        let outcome = search(view, &request).await.expect("search");
        self.searches += 1;
        for unit in &outcome.units {
            self.strategies.insert(unit.strategy.name());
            if unit.memtable {
                assert!(
                    matches!(
                        unit.strategy,
                        UnitStrategy::MemtableScan | UnitStrategy::Empty
                    ),
                    "memtable strategy {:?}",
                    unit.strategy
                );
            }
        }
        let context = format!("{metric:?} k={k} filter={filter:?} tuning={tuning:?} ef={ef:?}");

        // The model's values of every matching row, best first.
        let mut expected: Vec<f32> = self
            .model
            .values()
            .filter(|row| filter.as_ref().is_none_or(|filter| matches(filter, row)))
            .map(|row| metric_value(metric, &query, &row.vector))
            .collect();
        expected.sort_by(|left, right| match metric {
            DistanceMetric::L2 => left.total_cmp(right),
            _ => right.total_cmp(left),
        });
        let wanted = k.min(expected.len());

        let mut seen = HashSet::new();
        let mut previous: Option<f32> = None;
        for hit in &outcome.hits {
            let PrimaryKey::String(name) = &hit.row.record.pk else {
                unreachable!()
            };
            assert!(
                seen.insert(name.clone()),
                "{name} returned twice: {context}"
            );
            assert!(
                self.model.contains_key(name),
                "{name} is deleted but was returned: {context}"
            );
            let row = &self.model[name];
            if let Some(filter) = &filter {
                assert!(
                    matches(filter, row),
                    "{name} does not match the filter: {context}"
                );
            }
            // The row is the latest version: same scalars and vector.
            let got_n = hit.row.record.fields.get("n").and_then(|v| match v {
                Value::Int64(n) => Some(*n),
                _ => None,
            });
            let got_s = hit.row.record.fields.get("s").and_then(|v| match v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            });
            assert_eq!(
                (got_n, got_s),
                (row.n, row.s.clone()),
                "{name} is a stale version: {context}"
            );
            let stored = hit
                .row
                .record
                .vectors
                .get("vector")
                .expect("projected vector");
            let model_vector = if metric == DistanceMetric::Cosine {
                unit_vector(&row.vector)
            } else {
                row.vector.clone()
            };
            assert!(
                stored
                    .iter()
                    .zip(&model_vector)
                    .all(|(left, right)| (left - right).abs() < 1e-4),
                "{name} carries a stale vector: {context}"
            );
            let exact = metric_value(metric, &query, &row.vector);
            assert!(
                close(hit.value, exact),
                "{name}: reported {} but the exact value is {exact}: {context}",
                hit.value
            );
            if let Some(previous) = previous {
                assert!(
                    at_least_as_good(metric, previous, hit.value, 1e-4),
                    "hits are out of order ({previous} before {}): {context}",
                    hit.value
                );
            }
            previous = Some(hit.value);
        }
        assert!(outcome.hits.len() <= k, "{context}");
        if self.indexed {
            if wanted > 0 {
                let kth = expected[wanted - 1];
                self.recall.0 += outcome
                    .hits
                    .iter()
                    .filter(|hit| at_least_as_good(metric, hit.value, kth, 1e-4))
                    .count()
                    .min(wanted);
                self.recall.1 += wanted;
            }
        } else {
            assert_eq!(
                outcome.hits.len(),
                wanted,
                "exact search is short: {context}"
            );
            for (hit, value) in outcome.hits.iter().zip(&expected) {
                assert!(
                    close(hit.value, *value),
                    "exact search value {} differs from the model's {value}: {context}",
                    hit.value
                );
            }
        }
    }
}

async fn run(seed: u64, metric: DistanceMetric, indexed: bool, steps: usize) -> Scenario {
    let mut scenario = Scenario::new(seed, metric, indexed).await;
    for _ in 0..steps {
        scenario.step().await;
    }
    scenario.check().await;
    scenario.fixture.flush().await;
    scenario.check().await;
    scenario.fixture.compact().await;
    scenario.check().await;
    scenario
}

#[tokio::test]
async fn exact_search_matches_the_model_for_every_metric() {
    for metric in [
        DistanceMetric::L2,
        DistanceMetric::Cosine,
        DistanceMetric::Dot,
    ] {
        for seed in [1, 2] {
            let scenario = run(seed, metric, false, 220).await;
            assert!(scenario.searches > 0);
        }
    }
}

#[tokio::test]
async fn indexed_search_returns_only_live_matching_rows_for_every_metric() {
    let mut strategies = BTreeSet::new();
    for metric in [
        DistanceMetric::L2,
        DistanceMetric::Cosine,
        DistanceMetric::Dot,
    ] {
        let mut found = 0;
        let mut wanted = 0;
        for seed in [21, 22, 23] {
            let scenario = run(seed, metric, true, 260).await;
            found += scenario.recall.0;
            wanted += scenario.recall.1;
            strategies.extend(scenario.strategies);
        }
        let recall = found as f64 / wanted.max(1) as f64;
        println!("{metric:?}: recall {recall:.3} over {wanted}");
        assert!(wanted > 0);
        assert!(recall >= 0.9, "{metric:?}: recall {recall} over {wanted}");
    }
    println!("strategies: {strategies:?}");
    for strategy in ["exact_sq8", "graph_admit", "graph_acorn", "memtable_scan"] {
        assert!(
            strategies.contains(strategy),
            "{strategy} never ran: {strategies:?}"
        );
    }
}

/// Candidates that SQ8 cannot tell apart (one quantization bucket) are ordered by their exact
/// f32 distances before the global cut: three segments each hold four rows within 0.02 of the
/// query, all with the same codes, and the nearest row is in the last segment, which a cut on
/// the SQ8 estimates (ties by unit) would drop.
#[tokio::test]
async fn rerank_orders_candidates_sq8_cannot_tell_apart() {
    let fixture = Fixture::new(
        "rerank-ties",
        4,
        DistanceMetric::L2,
        IndexPolicy {
            graph_min_rows: u32::MAX,
            sq8_min_rows: 8,
            ..IndexPolicy::default()
        },
        &[],
    )
    .await;
    let query = vec![0.3_f32; 4];
    let far = |sign: f32, index: usize| {
        let mut vector = vec![sign * 100.0; 4];
        vector[index % 4] = -sign * 100.0;
        vector
    };
    // The nearest row (offset 0.001) is the last row of the last segment.
    let segments = [
        [0.004_f32, 0.005, 0.006, 0.007],
        [0.008, 0.009, 0.010, 0.011],
        [0.012, 0.013, 0.014, 0.001],
    ];
    for (segment, offsets) in segments.iter().enumerate() {
        let mut records = Vec::new();
        for (index, offset) in offsets.iter().enumerate() {
            records.push(
                Record::new(format!("near-{segment}-{index}"))
                    .with_vector("vector", query.iter().map(|v| v + offset).collect()),
            );
        }
        for index in 0..8 {
            let sign = if index % 2 == 0 { 1.0 } else { -1.0 };
            records.push(
                Record::new(format!("far-{segment}-{index}"))
                    .with_vector("vector", far(sign, index)),
            );
        }
        fixture.upsert(records).await;
        fixture.flush().await;
    }
    let view = fixture.view().await;
    let outcome = search(&view, &SearchRequest::new(query.clone(), 1))
        .await
        .expect("search");
    let sq8_units = outcome
        .units
        .iter()
        .filter(|unit| unit.strategy == UnitStrategy::ExactSq8)
        .count();
    assert_eq!(sq8_units, 3, "{:?}", outcome.units);
    let hit = outcome.hits.first().expect("a hit");
    assert_eq!(hit.row.record.pk, PrimaryKey::from("near-2-3"));
    let nearest = query.iter().map(|v| v + 0.001).collect::<Vec<_>>();
    let expected = metric_value(DistanceMetric::L2, &query, &nearest);
    assert!(close(hit.value, expected), "{} vs {expected}", hit.value);
}

/// Results with equal values are ordered by key, however many rows tie: every cut over exact
/// distances (a memtable's scan, an exact segment scan, the global finalists) keeps the rows
/// tied at its boundary. Here twelve rows share one vector, in three segments and the memtable,
/// written in descending key order so that the smallest keys land last in every unit; a cut
/// that broke ties by unit and row returned `t01` (found by the harness v2 model check,
/// seed 1072).
#[tokio::test]
async fn rows_tied_beyond_every_candidate_cut_are_ordered_by_key() {
    let fixture = Fixture::new(
        "ties",
        4,
        DistanceMetric::Dot,
        IndexPolicy {
            graph_min_rows: u32::MAX,
            sq8_min_rows: u32::MAX,
            ..IndexPolicy::default()
        },
        &[],
    )
    .await;
    let tied = vec![1.0, 2.0, 0.0, 0.0];
    let far = vec![-1.0, -1.0, 0.0, 0.0];
    for unit in 0..4 {
        let mut records = Vec::new();
        for index in (0..5).rev() {
            let key = format!("t{:02}", unit * 5 + index);
            records.push(Record::new(key).with_vector("vector", tied.clone()));
            records.push(
                Record::new(format!("u{:02}", unit * 5 + index)).with_vector("vector", far.clone()),
            );
        }
        fixture.upsert(records).await;
        if unit < 3 {
            fixture.flush().await;
        }
    }
    let view = fixture.view().await;
    for top_k in [1, 2, 3, 7] {
        let outcome = search(&view, &SearchRequest::new(vec![1.0, 1.0, 0.0, 0.0], top_k))
            .await
            .expect("search");
        let keys = outcome
            .hits
            .iter()
            .map(|hit| hit.row.record.pk.clone())
            .collect::<Vec<_>>();
        let wanted = (0..top_k)
            .map(|index| PrimaryKey::from(format!("t{index:02}").as_str()))
            .collect::<Vec<_>>();
        assert_eq!(keys, wanted, "top {top_k}");
        assert!(outcome.hits.iter().all(|hit| hit.value == 3.0));
    }
}
