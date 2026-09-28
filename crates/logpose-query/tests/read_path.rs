//! Differential tests of the read path against a brute-force model: random upserts, deletes,
//! delete-by-filter, update-by-filter, flushes, and compactions, then get, count, scroll,
//! order by, and search compared with the model. Exact paths (segments without vector index
//! sections) must match exactly; indexed segments (graphs and SQ8 codes) must keep recall.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    FilterComparison, FilterExpr, FilterOperator, ScalarMetadataValue, ScrollOrder, ScrollRequest,
    SearchRequest, SearchTuning, count_view, metric_value, scroll, scroll_view, search,
};
use logpose_storage::{IndexPolicy, Projection, read::Direction};
use logpose_types::{
    DistanceMetric,
    record::{PartialUpdate, PrimaryKey, Record},
    schema::{ElementType, FieldType},
    value::Value,
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json::{Map, Value as Json, json};
use std::{cmp::Ordering, collections::BTreeMap};
use thiserror as _;

mod support;

use support::{Fixture, Rng};

const DIMS: usize = 8;
const KEYS: u64 = 240;

/// One live row of the model.
#[derive(Clone, Debug, PartialEq)]
struct Row {
    vector: Vec<f32>,
    n: Option<i64>,
    f: Option<f64>,
    s: Option<String>,
    tags: Option<Vec<String>>,
    flag: Option<bool>,
    /// `$extra.d`: absent, JSON null, or a number.
    d: Option<Option<i64>>,
}

const WORDS: [&str; 5] = ["ant", "bee", "cat", "dog", "eel"];

fn random_row(rng: &mut Rng) -> Row {
    let maybe = |rng: &mut Rng| rng.below(5) != 0;
    Row {
        vector: (0..DIMS).map(|_| rng.gaussian()).collect(),
        n: maybe(rng).then(|| rng.below(20) as i64 - 5),
        f: maybe(rng).then(|| (rng.below(16) as f64) * 0.5 - 2.0),
        s: maybe(rng).then(|| WORDS[rng.below(5) as usize].to_owned()),
        tags: maybe(rng).then(|| {
            (0..=rng.below(2))
                .map(|_| WORDS[rng.below(5) as usize].to_owned())
                .collect()
        }),
        flag: maybe(rng).then(|| rng.below(2) == 0),
        d: match rng.below(4) {
            0 => None,
            1 => Some(None),
            _ => Some(Some(rng.below(10) as i64)),
        },
    }
}

fn to_record(key: &str, row: &Row) -> Record {
    let mut record = Record::new(key).with_vector("vector", row.vector.clone());
    if let Some(n) = row.n {
        record = record.with_field("n", Value::Int64(n));
    }
    if let Some(f) = row.f {
        record = record.with_field("f", Value::Float64(f));
    }
    if let Some(s) = &row.s {
        record = record.with_field("s", Value::String(s.clone()));
    }
    if let Some(tags) = &row.tags {
        record = record.with_field(
            "tags",
            Value::Array(tags.iter().cloned().map(Value::String).collect()),
        );
    }
    if let Some(flag) = row.flag {
        record = record.with_field("flag", Value::Bool(flag));
    }
    if let Some(d) = row.d {
        record
            .extra
            .insert("d".to_owned(), d.map_or(Json::Null, Json::from));
    }
    record
}

fn key(index: u64) -> String {
    format!("k{index:04}")
}

// ----- Filters and their reference semantics -----------------------------------------------

#[derive(Clone, Debug)]
enum Operand {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

impl Operand {
    fn scalar(&self) -> ScalarMetadataValue {
        match self {
            Self::Int(value) => ScalarMetadataValue::Number((*value).into()),
            Self::Float(value) => {
                ScalarMetadataValue::Number(serde_json::Number::from_f64(*value).expect("finite"))
            }
            Self::Str(value) => ScalarMetadataValue::String(value.clone()),
            Self::Bool(value) => ScalarMetadataValue::Bool(*value),
        }
    }
}

/// A scalar key of the model, ordered within its kind.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum Key {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

fn keys_of(row: &Row, field: &str) -> Vec<Key> {
    match field {
        "n" => row.n.map(Key::Int).into_iter().collect(),
        "f" => row.f.map(Key::Float).into_iter().collect(),
        "s" => row.s.clone().map(Key::Str).into_iter().collect(),
        "tags" => row
            .tags
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(Key::Str)
            .collect(),
        "flag" => row.flag.map(Key::Bool).into_iter().collect(),
        _ => Vec::new(),
    }
}

fn model_matches(filter: &FilterExpr, row: &Row) -> bool {
    match filter {
        FilterExpr::And { children } => children.iter().all(|child| model_matches(child, row)),
        FilterExpr::Or { children } => children.iter().any(|child| model_matches(child, row)),
        FilterExpr::Not { child } => !model_matches(child, row),
        FilterExpr::Comparison(comparison) if comparison.field == "d" => {
            dynamic_matches(comparison, row.d)
        }
        FilterExpr::Comparison(comparison) => {
            let keys = keys_of(row, &comparison.field);
            let operand = comparison.value.as_ref().map(|value| match value {
                ScalarMetadataValue::Number(number) if comparison.field == "f" => {
                    Key::Float(number.as_f64().expect("f64"))
                }
                ScalarMetadataValue::Number(number) => Key::Int(number.as_i64().expect("i64")),
                ScalarMetadataValue::String(text) => Key::Str(text.clone()),
                ScalarMetadataValue::Bool(value) => Key::Bool(*value),
                ScalarMetadataValue::Null => unreachable!("no null operands"),
            });
            let any = |test: &dyn Fn(&Key) -> bool| keys.iter().any(test);
            match (comparison.operator, operand) {
                (FilterOperator::Exists, _) => !keys.is_empty(),
                (FilterOperator::IsNull, _) => keys.is_empty(),
                (FilterOperator::Eq, Some(value)) => keys.contains(&value),
                (FilterOperator::Ne, Some(value)) => !keys.is_empty() && !keys.contains(&value),
                (FilterOperator::Lt, Some(value)) => any(&|key| key < &value),
                (FilterOperator::Lte, Some(value)) => any(&|key| key <= &value),
                (FilterOperator::Gt, Some(value)) => any(&|key| key > &value),
                (FilterOperator::Gte, Some(value)) => any(&|key| key >= &value),
                _ => false,
            }
        }
    }
}

/// The v1 JSON semantics on `$extra.d`.
fn dynamic_matches(comparison: &FilterComparison, d: Option<Option<i64>>) -> bool {
    let operand = comparison.value.as_ref().and_then(|value| match value {
        ScalarMetadataValue::Number(number) => number.as_i64(),
        _ => None,
    });
    let value = d.flatten();
    match comparison.operator {
        FilterOperator::Exists => d.is_some(),
        FilterOperator::IsNull => d == Some(None),
        FilterOperator::Eq => d.is_some() && value.is_some() && value == operand,
        // A JSON null is a scalar that differs from every number.
        FilterOperator::Ne => d.is_some() && value != operand,
        FilterOperator::Lt => value.zip(operand).is_some_and(|(v, o)| v < o),
        FilterOperator::Lte => value.zip(operand).is_some_and(|(v, o)| v <= o),
        FilterOperator::Gt => value.zip(operand).is_some_and(|(v, o)| v > o),
        FilterOperator::Gte => value.zip(operand).is_some_and(|(v, o)| v >= o),
    }
}

fn comparison(field: &str, operator: FilterOperator, operand: Option<Operand>) -> FilterExpr {
    FilterExpr::Comparison(FilterComparison {
        field: field.to_owned(),
        operator,
        value: operand.map(|operand| operand.scalar()),
    })
}

fn random_comparison(rng: &mut Rng) -> FilterExpr {
    let ordered = [
        FilterOperator::Eq,
        FilterOperator::Ne,
        FilterOperator::Lt,
        FilterOperator::Lte,
        FilterOperator::Gt,
        FilterOperator::Gte,
    ];
    let pick = |rng: &mut Rng, ops: &[FilterOperator]| ops[rng.below(ops.len() as u64) as usize];
    let field = ["n", "f", "s", "tags", "flag", "d"][rng.below(6) as usize];
    if rng.below(6) == 0 {
        let operator = if rng.below(2) == 0 {
            FilterOperator::Exists
        } else {
            FilterOperator::IsNull
        };
        return comparison(field, operator, None);
    }
    match field {
        "n" => comparison(
            "n",
            pick(rng, &ordered),
            Some(Operand::Int(rng.below(20) as i64 - 5)),
        ),
        "f" => comparison(
            "f",
            pick(rng, &ordered),
            Some(Operand::Float((rng.below(16) as f64) * 0.5 - 2.0)),
        ),
        "s" => comparison(
            "s",
            pick(rng, &ordered),
            Some(Operand::Str(WORDS[rng.below(5) as usize].to_owned())),
        ),
        "tags" => comparison(
            "tags",
            pick(rng, &[FilterOperator::Eq, FilterOperator::Ne]),
            Some(Operand::Str(WORDS[rng.below(5) as usize].to_owned())),
        ),
        "flag" => comparison(
            "flag",
            pick(rng, &[FilterOperator::Eq, FilterOperator::Ne]),
            Some(Operand::Bool(rng.below(2) == 0)),
        ),
        _ => comparison(
            "d",
            pick(rng, &ordered),
            Some(Operand::Int(rng.below(10) as i64)),
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

// ----- The scenario -------------------------------------------------------------------------

struct Scenario {
    fixture: Fixture,
    model: BTreeMap<String, Row>,
    rng: Rng,
    /// Whether segments carry vector index sections (search is then approximate).
    indexed: bool,
    recall: (usize, usize),
}

impl Scenario {
    async fn new(seed: u64, indexed: bool) -> Self {
        let policy = if indexed {
            IndexPolicy {
                graph_min_rows: 48,
                sq8_min_rows: 24,
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
            "read-path",
            DIMS,
            DistanceMetric::L2,
            policy,
            &[
                ("n", FieldType::Int64),
                ("f", FieldType::Float64),
                ("s", FieldType::String),
                ("tags", FieldType::Array(ElementType::String)),
                ("flag", FieldType::Bool),
            ],
        )
        .await;
        Self {
            fixture,
            model: BTreeMap::new(),
            rng: Rng::new(seed),
            indexed,
            recall: (0, 0),
        }
    }

    async fn step(&mut self) {
        match self.rng.below(100) {
            0..45 => {
                let mut batch = BTreeMap::new();
                for _ in 0..=self.rng.below(24) {
                    batch.insert(key(self.rng.below(KEYS)), random_row(&mut self.rng));
                }
                let records = batch.iter().map(|(key, row)| to_record(key, row)).collect();
                self.fixture.upsert(records).await;
                self.model.extend(batch);
            }
            45..57 => {
                let keys = (0..=self.rng.below(8))
                    .map(|_| key(self.rng.below(KEYS)))
                    .collect::<std::collections::BTreeSet<_>>();
                let ops = keys
                    .iter()
                    .map(|key| {
                        logpose_types::record::ClientOp::Delete(PrimaryKey::from(key.as_str()))
                    })
                    .collect();
                self.fixture.handle.write(ops).await.expect("delete");
                for key in keys {
                    self.model.remove(&key);
                }
            }
            57..62 => {
                let filter = random_filter(&mut self.rng);
                let expected = self
                    .model
                    .iter()
                    .filter(|(_, row)| model_matches(&filter, row))
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
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
            62..67 => {
                let filter = random_filter(&mut self.rng);
                let n = self.rng.below(20) as i64 - 5;
                let mut patch = PartialUpdate::new("ignored");
                patch.fields.insert("n".to_owned(), Value::Int64(n));
                let ack = self
                    .fixture
                    .handle
                    .update_by_filter(filter.clone(), patch)
                    .await
                    .expect("update by filter");
                let mut updated = 0;
                for row in self.model.values_mut() {
                    if model_matches(&filter, row) {
                        row.n = Some(n);
                        updated += 1;
                    }
                }
                assert_eq!(ack.applied_ops, updated, "update by {filter:?}");
            }
            67..76 => self.fixture.flush().await,
            76..80 => self.fixture.compact().await,
            _ => self.check().await,
        }
    }

    async fn check(&mut self) {
        let view = self.fixture.view().await;
        // Count.
        assert_eq!(
            count_view(&view, None).await.expect("count"),
            self.model.len() as u64
        );
        for _ in 0..4 {
            let filter = random_filter(&mut self.rng);
            let expected = self
                .model
                .values()
                .filter(|row| model_matches(&filter, row))
                .count() as u64;
            assert_eq!(
                count_view(&view, Some(&filter)).await.expect("count"),
                expected,
                "count {filter:?}"
            );
        }
        // Get, including absent keys.
        let keys = (0..12)
            .map(|_| PrimaryKey::from(key(self.rng.below(KEYS))))
            .collect::<Vec<_>>();
        let rows = view.get(&keys, Projection::full()).await.expect("get");
        for (pk, row) in keys.iter().zip(rows) {
            let PrimaryKey::String(name) = pk else {
                unreachable!()
            };
            assert_eq!(
                row.map(|row| normalize(&row.record)),
                self.model
                    .get(name)
                    .map(|expected| normalize(&to_record(name, expected))),
                "get {name}"
            );
        }
        // Scroll by key through a pinned snapshot, page by page.
        let filter = (self.rng.below(2) == 0).then(|| random_filter(&mut self.rng));
        let limit = 1 + self.rng.below(40) as u32;
        let mut cursor = None;
        let mut scrolled = Vec::new();
        loop {
            let page = scroll(
                &self.fixture.engine,
                &self.fixture.reference,
                ScrollRequest {
                    filter: filter.clone(),
                    order: ScrollOrder::Pk,
                    limit,
                    projection: Projection::scalars(),
                    cursor: cursor.take(),
                },
            )
            .await
            .expect("scroll");
            scrolled.extend(page.rows.into_iter().map(|row| row.record.pk));
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        let expected = self
            .model
            .iter()
            .filter(|(_, row)| {
                filter
                    .as_ref()
                    .is_none_or(|filter| model_matches(filter, row))
            })
            .map(|(key, _)| PrimaryKey::from(key.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(scrolled, expected, "scroll by key, filter {filter:?}");
        // Order by a field, first page and the page after it.
        for (field, direction) in [
            ("n", Direction::Ascending),
            ("f", Direction::Descending),
            ("s", Direction::Ascending),
            ("flag", Direction::Descending),
        ] {
            let order = ScrollOrder::Field {
                field: field.to_owned(),
                direction,
            };
            let expected = self.ordered(field, direction);
            let (first, last) = scroll_view(&view, None, &order, 7, Projection::scalars(), None)
                .await
                .expect("order by");
            let first = first
                .into_iter()
                .map(|row| row.record.pk)
                .collect::<Vec<_>>();
            assert_eq!(first, expected[..expected.len().min(7)], "order by {field}");
            if let Some(last) = last {
                let (second, _) =
                    scroll_view(&view, None, &order, 9, Projection::scalars(), Some(&last))
                        .await
                        .expect("order by, next page");
                let second = second
                    .into_iter()
                    .map(|row| row.record.pk)
                    .collect::<Vec<_>>();
                let rest = &expected[first.len()..];
                assert_eq!(
                    second,
                    rest[..rest.len().min(9)],
                    "order by {field}, page 2"
                );
            }
        }
        // Search.
        for _ in 0..3 {
            let query = (0..DIMS).map(|_| self.rng.gaussian()).collect::<Vec<f32>>();
            let filter = (self.rng.below(2) == 0).then(|| random_filter(&mut self.rng));
            let k = 1 + self.rng.below(8) as usize;
            let mut expected = self
                .model
                .iter()
                .filter(|(_, row)| {
                    filter
                        .as_ref()
                        .is_none_or(|filter| model_matches(filter, row))
                })
                .map(|(key, row)| {
                    (
                        metric_value(DistanceMetric::L2, &query, &row.vector),
                        key.clone(),
                    )
                })
                .collect::<Vec<_>>();
            expected.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
            expected.truncate(k);
            let outcome = search(
                &view,
                &SearchRequest {
                    filter: filter.clone(),
                    tuning: SearchTuning {
                        exact_max_matches: if self.indexed { 16 } else { usize::MAX },
                        ..SearchTuning::default()
                    },
                    ..SearchRequest::new(query.clone(), k)
                },
            )
            .await
            .expect("search");
            let actual = outcome
                .hits
                .iter()
                .map(|hit| {
                    let PrimaryKey::String(key) = &hit.row.record.pk else {
                        unreachable!()
                    };
                    (hit.value, key.clone())
                })
                .collect::<Vec<_>>();
            if self.indexed {
                self.recall.0 += expected
                    .iter()
                    .filter(|(_, key)| actual.iter().any(|(_, found)| found == key))
                    .count();
                self.recall.1 += expected.len();
            } else {
                assert_eq!(actual, expected, "exact search, filter {filter:?}");
            }
        }
    }

    /// Model keys ordered by `field` in `direction`, ties by key, rows without a value last.
    fn ordered(&self, field: &str, direction: Direction) -> Vec<PrimaryKey> {
        let mut rows = self
            .model
            .iter()
            .map(|(key, row)| (keys_of(row, field).into_iter().next(), key.clone()))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| match (&left.0, &right.0) {
            (Some(a), Some(b)) => {
                let order = a.partial_cmp(b).unwrap_or(Ordering::Equal);
                let order = if direction == Direction::Descending {
                    order.reverse()
                } else {
                    order
                };
                order.then_with(|| left.1.cmp(&right.1))
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => left.1.cmp(&right.1),
        });
        rows.into_iter()
            .map(|(_, key)| PrimaryKey::from(key.as_str()))
            .collect()
    }
}

/// A record's key, non-null fields, `$extra`, and vector.
type Normalized = (
    PrimaryKey,
    Vec<(String, Json)>,
    Map<String, Json>,
    Option<Vec<f32>>,
);

/// A record without null fields, for comparison.
fn normalize(record: &Record) -> Normalized {
    let mut fields = record
        .fields
        .iter()
        .filter(|(_, value)| !value.is_null())
        .map(|(name, value)| (name.clone(), value.to_json()))
        .collect::<Vec<_>>();
    fields.sort_by(|left, right| left.0.cmp(&right.0));
    (
        record.pk.clone(),
        fields,
        record.extra.clone(),
        record.vectors.get("vector").cloned(),
    )
}

async fn run(seed: u64, indexed: bool, steps: usize) -> (usize, usize) {
    let mut scenario = Scenario::new(seed, indexed).await;
    for _ in 0..steps {
        scenario.step().await;
    }
    scenario.fixture.flush().await;
    scenario.check().await;
    scenario.fixture.compact().await;
    scenario.check().await;
    let _ = json!(null);
    scenario.recall
}

#[tokio::test]
async fn exact_reads_match_a_brute_force_model_through_flushes_and_compactions() {
    for seed in [1, 2, 3] {
        run(seed, false, 260).await;
    }
}

#[tokio::test]
async fn indexed_segments_keep_recall_and_exact_scalar_reads() {
    let mut found = 0;
    let mut wanted = 0;
    for seed in [11, 12, 13] {
        let (hits, total) = run(seed, true, 260).await;
        found += hits;
        wanted += total;
    }
    let recall = found as f64 / wanted.max(1) as f64;
    assert!(wanted > 0);
    assert!(
        recall >= 0.95,
        "recall {recall} over {wanted} expected hits"
    );
}
