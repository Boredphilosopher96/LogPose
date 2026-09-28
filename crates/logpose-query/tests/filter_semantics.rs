//! Differential test of every filter operator against an independent brute-force model.
//!
//! Each logical field is declared several times, once per index kind (none, inverted,
//! sorted, both), with the same value in every copy, so one random filter exercises column
//! scans, inverted postings, and sorted ranges, over the memtable and over flushed and
//! compacted segments. Operands mix `int64` and `float64` numbers (including non-integral range
//! bounds on integer and timestamp fields), timestamps as values and as microseconds, empty
//! arrays, a `json` field, and a dynamic key holding every JSON kind.
//!
//! The model is written from the documented semantics, not from the implementation: a declared
//! field's keys are its value (one per array element, none for null or an empty array);
//! `ne` and `not_in` need a key (nulls never match); `not` is the complement over live rows. A
//! `json` field is a declared field: `exists` when it has a value, `is_null` when it has none.
//! A dynamic key is present or absent: `exists` when present, `is_null` when present and null.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{
    CompiledFilter, FilterExpr, RangeBounds, ScrollOrder, count_view, scroll_view,
};
use logpose_storage::{IndexPolicy, Projection, SchemaChange};
use logpose_types::{
    DistanceMetric,
    record::{ClientOp, PartialUpdate, PrimaryKey, Record},
    schema::{ElementType, FieldIndex, FieldType, ScalarFieldSpec},
    value::{Timestamp, Value},
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json::{Value as Json, json};
use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};
use thiserror as _;

mod support;

use support::{Fixture, Rng};

const KEYS: u64 = 160;
const WORDS: [&str; 4] = ["ant", "bee", "cat", "dog"];

#[derive(Clone, Debug, PartialEq)]
struct Row {
    i: Option<i64>,
    f: Option<f64>,
    s: Option<String>,
    t: Option<i64>,
    b: Option<bool>,
    ai: Option<Vec<i64>>,
    at: Option<Vec<String>>,
    /// A `json` field: never JSON null (that is stored as null).
    j: Option<Json>,
    /// `$extra.x`: absent, or any JSON value (null included).
    x: Option<Json>,
}

/// Every declared copy of each logical field.
const COPIES: [(&str, &[(&str, FieldIndex)]); 8] = [
    (
        "i",
        &[
            ("i_none", FieldIndex::None),
            ("i_inv", FieldIndex::Inverted),
            ("i_sorted", FieldIndex::Sorted),
            ("i_both", FieldIndex::InvertedAndSorted),
        ],
    ),
    (
        "f",
        &[
            ("f_none", FieldIndex::None),
            ("f_inv", FieldIndex::Inverted),
            ("f_both", FieldIndex::InvertedAndSorted),
        ],
    ),
    (
        "s",
        &[
            ("s_none", FieldIndex::None),
            ("s_inv", FieldIndex::Inverted),
            ("s_sorted", FieldIndex::Sorted),
        ],
    ),
    (
        "t",
        &[
            ("t_none", FieldIndex::None),
            ("t_both", FieldIndex::InvertedAndSorted),
        ],
    ),
    (
        "b",
        &[
            ("b_none", FieldIndex::None),
            ("b_inv", FieldIndex::Inverted),
        ],
    ),
    (
        "ai",
        &[
            ("ai_none", FieldIndex::None),
            ("ai_inv", FieldIndex::Inverted),
        ],
    ),
    (
        "at",
        &[
            ("at_none", FieldIndex::None),
            ("at_inv", FieldIndex::Inverted),
        ],
    ),
    ("j", &[("j", FieldIndex::None)]),
];

fn field_type(logical: &str) -> FieldType {
    match logical {
        "i" => FieldType::Int64,
        "f" => FieldType::Float64,
        "s" => FieldType::String,
        "t" => FieldType::Timestamp,
        "b" => FieldType::Bool,
        "ai" => FieldType::Array(ElementType::Int64),
        "at" => FieldType::Array(ElementType::String),
        _ => FieldType::Json,
    }
}

fn logical(field: &str) -> &str {
    if field == "$extra.x" || field == "x" {
        return "x";
    }
    if field == "id" {
        return "id";
    }
    field.split('_').next().unwrap_or(field)
}

fn random_json(rng: &mut Rng, with_null: bool) -> Json {
    match rng.below(if with_null { 8 } else { 7 }) {
        0 => json!(rng.below(5) as i64 - 1),
        1 => json!((rng.below(8) as f64) * 0.5 - 1.0),
        2 => json!(WORDS[rng.below(4) as usize]),
        3 => json!(rng.below(2) == 0),
        4 => json!([rng.below(3)]),
        5 => json!({ "k": rng.below(3) }),
        6 => json!(rng.below(3).to_string()),
        _ => Json::Null,
    }
}

fn random_row(rng: &mut Rng) -> Row {
    let maybe = |rng: &mut Rng| rng.below(4) != 0;
    Row {
        i: maybe(rng).then(|| rng.below(9) as i64 - 3),
        f: maybe(rng).then(|| (rng.below(9) as f64) * 0.5 - 1.0),
        s: maybe(rng).then(|| WORDS[rng.below(4) as usize].to_owned()),
        t: maybe(rng).then(|| (rng.below(6) as i64 - 2) * 1_000_000),
        b: maybe(rng).then(|| rng.below(2) == 0),
        ai: maybe(rng).then(|| (0..rng.below(3)).map(|_| rng.below(5) as i64 - 1).collect()),
        at: maybe(rng).then(|| {
            (0..rng.below(3))
                .map(|_| WORDS[rng.below(4) as usize].to_owned())
                .collect()
        }),
        j: maybe(rng).then(|| random_json(rng, false)),
        x: maybe(rng).then(|| random_json(rng, true)),
    }
}

fn typed_value(logical: &str, row: &Row) -> Value {
    match logical {
        "i" => row.i.map_or(Value::Null, Value::Int64),
        "f" => row.f.map_or(Value::Null, Value::Float64),
        "s" => row.s.clone().map_or(Value::Null, Value::String),
        "t" => row.t.map_or(Value::Null, |micros| {
            Value::Timestamp(Timestamp::from_micros(micros).expect("timestamp"))
        }),
        "b" => row.b.map_or(Value::Null, Value::Bool),
        "ai" => row.ai.clone().map_or(Value::Null, |items| {
            Value::Array(items.into_iter().map(Value::Int64).collect())
        }),
        "at" => row.at.clone().map_or(Value::Null, |items| {
            Value::Array(items.into_iter().map(Value::String).collect())
        }),
        _ => row.j.clone().map_or(Value::Null, Value::Json),
    }
}

fn to_record(key: &str, row: &Row) -> Record {
    let mut record = Record::new(key).with_vector("vector", vec![1.0, 0.0]);
    for (logical, copies) in COPIES {
        let value = typed_value(logical, row);
        if value.is_null() {
            continue;
        }
        for (name, _) in copies {
            record = record.with_field(*name, value.clone());
        }
    }
    if let Some(x) = &row.x {
        record.extra.insert("x".to_owned(), x.clone());
    }
    record
}

// ----- The model -----------------------------------------------------------------------------

/// A model key: numbers compare as numbers whatever their representation.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum Key {
    Num(f64),
    Str(String),
    Bool(bool),
}

fn typed_keys(key: &str, row: &Row, logical: &str) -> Vec<Key> {
    match logical {
        "id" => vec![Key::Str(key.to_owned())],
        "i" => row.i.map(|v| Key::Num(v as f64)).into_iter().collect(),
        "f" => row.f.map(Key::Num).into_iter().collect(),
        "s" => row.s.clone().map(Key::Str).into_iter().collect(),
        "t" => row.t.map(|v| Key::Num(v as f64)).into_iter().collect(),
        "b" => row.b.map(Key::Bool).into_iter().collect(),
        "ai" => row
            .ai
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|v| Key::Num(v as f64))
            .collect(),
        "at" => row
            .at
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(Key::Str)
            .collect(),
        other => unreachable!("no typed field {other}"),
    }
}

fn operand_key(value: &Value) -> Key {
    match value {
        Value::Int64(v) => Key::Num(*v as f64),
        Value::Float64(v) => Key::Num(*v),
        Value::Timestamp(v) => Key::Num(v.as_micros() as f64),
        Value::String(v) => Key::Str(v.clone()),
        Value::Bool(v) => Key::Bool(*v),
        Value::Json(json) => json_key(json).expect("scalar JSON operand"),
        other => unreachable!("no {other:?} operands"),
    }
}

/// A JSON scalar's key; `None` for null, arrays, and objects.
fn json_key(json: &Json) -> Option<Key> {
    match json {
        Json::Number(n) => n.as_f64().map(Key::Num),
        Json::String(s) => Some(Key::Str(s.clone())),
        Json::Bool(b) => Some(Key::Bool(*b)),
        _ => None,
    }
}

fn same_kind(left: &Key, right: &Key) -> bool {
    std::mem::discriminant(left) == std::mem::discriminant(right)
}

fn within(key: &Key, bounds: &RangeBounds) -> bool {
    let test = |bound: &Option<Value>, holds: fn(Ordering) -> bool| {
        bound.as_ref().is_none_or(|bound| {
            let bound = operand_key(bound);
            same_kind(key, &bound) && key.partial_cmp(&bound).is_some_and(holds)
        })
    };
    test(&bounds.gt, |o| o == Ordering::Greater)
        && test(&bounds.gte, |o| o != Ordering::Less)
        && test(&bounds.lt, |o| o == Ordering::Less)
        && test(&bounds.lte, |o| o != Ordering::Greater)
}

fn keys_match(keys: &[Key], filter: &FilterExpr) -> bool {
    match filter {
        FilterExpr::Exists { .. } => !keys.is_empty(),
        FilterExpr::IsNull { .. } => keys.is_empty(),
        FilterExpr::Eq { value, .. } | FilterExpr::Contains { value, .. } => {
            keys.contains(&operand_key(value))
        }
        FilterExpr::Ne { value, .. } => !keys.is_empty() && !keys.contains(&operand_key(value)),
        FilterExpr::In { values, .. } | FilterExpr::ContainsAny { values, .. } => {
            let wanted = values.iter().map(operand_key).collect::<Vec<_>>();
            keys.iter().any(|key| wanted.contains(key))
        }
        FilterExpr::NotIn { values, .. } => {
            let unwanted = values.iter().map(operand_key).collect::<Vec<_>>();
            !keys.is_empty() && keys.iter().all(|key| !unwanted.contains(key))
        }
        FilterExpr::Range { bounds, .. } => keys.iter().any(|key| within(key, bounds)),
        _ => unreachable!(),
    }
}

/// JSON semantics over a possibly absent JSON value.
fn json_match(value: Option<&Json>, filter: &FilterExpr, declared: bool) -> bool {
    let scalar = value.and_then(json_key);
    match filter {
        FilterExpr::Exists { .. } => {
            if declared {
                value.is_some_and(|value| !value.is_null())
            } else {
                value.is_some()
            }
        }
        FilterExpr::IsNull { .. } => {
            if declared {
                value.is_none_or(Json::is_null)
            } else {
                value.is_some_and(Json::is_null)
            }
        }
        FilterExpr::Eq { value: op, .. } => scalar.is_some_and(|key| key == operand_key(op)),
        FilterExpr::In { values, .. } => {
            scalar.is_some_and(|key| values.iter().any(|op| operand_key(op) == key))
        }
        FilterExpr::Ne { value: op, .. } => scalar.is_some_and(|key| key != operand_key(op)),
        FilterExpr::NotIn { values, .. } => {
            scalar.is_some_and(|key| values.iter().all(|op| operand_key(op) != key))
        }
        FilterExpr::Range { bounds, .. } => {
            scalar.is_some_and(|key| !matches!(key, Key::Bool(_)) && within(&key, bounds))
        }
        _ => unreachable!(),
    }
}

fn field_of(filter: &FilterExpr) -> &str {
    match filter {
        FilterExpr::Exists { field }
        | FilterExpr::IsNull { field }
        | FilterExpr::Eq { field, .. }
        | FilterExpr::Ne { field, .. }
        | FilterExpr::Contains { field, .. }
        | FilterExpr::In { field, .. }
        | FilterExpr::NotIn { field, .. }
        | FilterExpr::ContainsAny { field, .. }
        | FilterExpr::Range { field, .. } => field,
        _ => unreachable!(),
    }
}

fn model_matches(filter: &FilterExpr, key: &str, row: &Row) -> bool {
    match filter {
        FilterExpr::And(children) => children.iter().all(|c| model_matches(c, key, row)),
        FilterExpr::Or(children) => children.iter().any(|c| model_matches(c, key, row)),
        FilterExpr::Not(child) => !model_matches(child, key, row),
        leaf => match logical(field_of(leaf)) {
            "x" => json_match(row.x.as_ref(), leaf, false),
            "j" => json_match(row.j.as_ref(), leaf, true),
            logical => keys_match(&typed_keys(key, row, logical), leaf),
        },
    }
}

// ----- Random filters ------------------------------------------------------------------------

fn pick<'a>(rng: &mut Rng, items: &[&'a str]) -> &'a str {
    items[rng.below(items.len() as u64) as usize]
}

fn copy_name(rng: &mut Rng, logical: &str) -> String {
    let copies = COPIES
        .iter()
        .find(|(name, _)| *name == logical)
        .map(|(_, copies)| *copies)
        .expect("logical field");
    copies[rng.below(copies.len() as u64) as usize].0.to_owned()
}

/// An exact operand (for equality) of a logical field.
fn exact_operand(rng: &mut Rng, logical: &str) -> Value {
    match logical {
        "i" | "ai" => {
            let v = rng.below(9) as i64 - 3;
            if rng.below(3) == 0 {
                Value::Float64(v as f64)
            } else {
                Value::Int64(v)
            }
        }
        "f" => {
            if rng.below(3) == 0 {
                Value::Int64(rng.below(5) as i64 - 1)
            } else {
                Value::Float64((rng.below(9) as f64) * 0.5 - 1.0)
            }
        }
        "t" => {
            let micros = (rng.below(6) as i64 - 2) * 1_000_000;
            match rng.below(3) {
                0 => Value::Int64(micros),
                1 => Value::Float64(micros as f64),
                _ => Value::Timestamp(Timestamp::from_micros(micros).expect("timestamp")),
            }
        }
        "s" | "at" => Value::from(pick(rng, &WORDS)),
        "id" => Value::String(format!("k{:04}", rng.below(KEYS))),
        "b" => Value::Bool(rng.below(2) == 0),
        _ => {
            let mut json = random_json(rng, false);
            while json_key(&json).is_none() {
                json = random_json(rng, false);
            }
            Value::Json(json)
        }
    }
}

/// A range bound of a logical field: exact operands plus non-integral ones for integer and
/// timestamp fields.
fn bound_operand(rng: &mut Rng, logical: &str) -> Value {
    match logical {
        "i" if rng.below(3) == 0 => Value::Float64(rng.below(18) as f64 * 0.5 - 3.25),
        "t" if rng.below(3) == 0 => Value::Float64((rng.below(12) as f64 - 4.0) * 500_000.0 + 0.5),
        "j" | "x" => loop {
            if rng.below(4) == 0 {
                break Value::Json(json!(pick(rng, &["0", "1", "b", "c"])));
            }
            let operand = exact_operand(rng, logical);
            if !matches!(operand, Value::Json(Json::Bool(_))) {
                break operand;
            }
        },
        _ => exact_operand(rng, logical),
    }
}

fn random_bounds(rng: &mut Rng, logical: &str) -> RangeBounds {
    let mut bounds = RangeBounds::default();
    let lower = rng.below(3);
    let upper = if lower == 2 {
        rng.below(2)
    } else {
        rng.below(3)
    };
    match lower {
        0 => bounds.gt = Some(bound_operand(rng, logical)),
        1 => bounds.gte = Some(bound_operand(rng, logical)),
        _ => {}
    }
    match upper {
        0 => bounds.lt = Some(bound_operand(rng, logical)),
        1 => bounds.lte = Some(bound_operand(rng, logical)),
        _ => {}
    }
    bounds
}

fn random_leaf(rng: &mut Rng) -> FilterExpr {
    let logical = pick(rng, &["i", "f", "s", "t", "b", "ai", "at", "j", "x", "id"]);
    let field = match logical {
        "x" => {
            if rng.below(2) == 0 {
                "$extra.x".to_owned()
            } else {
                "x".to_owned()
            }
        }
        "id" => "id".to_owned(),
        other => copy_name(rng, other),
    };
    let array = matches!(logical, "ai" | "at");
    let ordered = !matches!(logical, "b");
    let list = |rng: &mut Rng| {
        (0..=rng.below(3))
            .map(|_| exact_operand(rng, logical))
            .collect::<Vec<_>>()
    };
    match rng.below(if array { 9 } else { 7 }) {
        0 => {
            if logical == "id" {
                FilterExpr::Eq {
                    value: exact_operand(rng, logical),
                    field,
                }
            } else {
                FilterExpr::Exists { field }
            }
        }
        1 => {
            if logical == "id" {
                FilterExpr::Ne {
                    value: exact_operand(rng, logical),
                    field,
                }
            } else {
                FilterExpr::IsNull { field }
            }
        }
        2 => FilterExpr::Eq {
            value: exact_operand(rng, logical),
            field,
        },
        3 => FilterExpr::Ne {
            value: exact_operand(rng, logical),
            field,
        },
        4 => FilterExpr::In {
            values: list(rng),
            field,
        },
        5 => FilterExpr::NotIn {
            values: list(rng),
            field,
        },
        6 if ordered => FilterExpr::Range {
            bounds: random_bounds(rng, logical),
            field,
        },
        6 => FilterExpr::Eq {
            value: exact_operand(rng, logical),
            field,
        },
        7 => FilterExpr::Contains {
            value: exact_operand(rng, logical),
            field,
        },
        _ => FilterExpr::ContainsAny {
            values: list(rng),
            field,
        },
    }
}

fn random_filter(rng: &mut Rng, depth: u32) -> FilterExpr {
    if depth == 0 || rng.below(3) == 0 {
        return random_leaf(rng);
    }
    match rng.below(3) {
        0 => FilterExpr::And(
            (0..=rng.below(2))
                .map(|_| random_filter(rng, depth - 1))
                .collect(),
        ),
        1 => FilterExpr::Or(
            (0..=rng.below(2))
                .map(|_| random_filter(rng, depth - 1))
                .collect(),
        ),
        _ => FilterExpr::Not(Box::new(random_filter(rng, depth - 1))),
    }
}

// ----- The scenario --------------------------------------------------------------------------

async fn fixture() -> Fixture {
    let fixture = Fixture::new(
        "filter-semantics",
        2,
        DistanceMetric::Dot,
        IndexPolicy {
            graph_min_rows: u32::MAX,
            sq8_min_rows: u32::MAX,
            ..IndexPolicy::default()
        },
        &[],
    )
    .await;
    for (logical, copies) in COPIES {
        for (name, index) in copies {
            fixture
                .handle
                .alter_schema(SchemaChange::AddField(ScalarFieldSpec {
                    name: (*name).to_owned(),
                    field_type: field_type(logical),
                    index: *index,
                    nullable: true,
                }))
                .await
                .expect("field should be added");
        }
    }
    fixture
}

async fn check(fixture: &Fixture, model: &BTreeMap<String, Row>, rng: &mut Rng, rounds: usize) {
    let view = fixture.view().await;
    let schema = Arc::clone(view.schema());
    for _ in 0..rounds {
        let filter = random_filter(rng, 3);
        let expected = model
            .iter()
            .filter(|(key, row)| model_matches(&filter, key, row))
            .map(|(key, _)| PrimaryKey::from(key.as_str()))
            .collect::<Vec<_>>();
        let count = count_view(&view, Some(&filter)).await.expect("count");
        let (rows, _) = scroll_view(
            &view,
            Some(&filter),
            &ScrollOrder::Pk,
            u32::MAX,
            Projection::scalars(),
            None,
        )
        .await
        .expect("scroll");
        let scrolled = rows
            .into_iter()
            .map(|row| row.record.pk)
            .collect::<Vec<_>>();
        let disagreeing = model
            .iter()
            .filter(|(key, _)| {
                let pk = PrimaryKey::from(key.as_str());
                scrolled.contains(&pk) != expected.contains(&pk)
            })
            .take(3)
            .collect::<Vec<_>>();
        assert!(
            disagreeing.is_empty() && scrolled == expected,
            "scroll {filter:?}: rows the engine and the model disagree on: {disagreeing:?}"
        );
        assert_eq!(count, expected.len() as u64, "count {filter:?}");
        // The reference semantics the crate documents agree with the model too.
        let compiled = CompiledFilter::compile(&schema, &filter).expect("compile");
        let reference = model
            .iter()
            .filter(|(key, row)| compiled.matches_record(&to_record(key, row)))
            .map(|(key, _)| PrimaryKey::from(key.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(reference, expected, "matches_record {filter:?}");
    }
}

async fn run(seed: u64, steps: usize) {
    let fixture = fixture().await;
    let mut model = BTreeMap::new();
    let mut rng = Rng::new(seed);
    for _ in 0..steps {
        match rng.below(100) {
            0..50 => {
                let mut batch = BTreeMap::new();
                for _ in 0..=rng.below(20) {
                    batch.insert(format!("k{:04}", rng.below(KEYS)), random_row(&mut rng));
                }
                fixture
                    .upsert(batch.iter().map(|(key, row)| to_record(key, row)).collect())
                    .await;
                model.extend(batch);
            }
            50..60 => {
                let key = format!("k{:04}", rng.below(KEYS));
                let Some(row) = model.get_mut(&key) else {
                    continue;
                };
                let fresh = random_row(&mut rng);
                let (logical, copies) = COPIES[rng.below(COPIES.len() as u64) as usize];
                let mut update = PartialUpdate::new(key.as_str());
                let value = typed_value(logical, &fresh);
                for (name, _) in copies {
                    update.fields.insert((*name).to_owned(), value.clone());
                }
                match logical {
                    "i" => row.i = fresh.i,
                    "f" => row.f = fresh.f,
                    "s" => row.s = fresh.s,
                    "t" => row.t = fresh.t,
                    "b" => row.b = fresh.b,
                    "ai" => row.ai = fresh.ai,
                    "at" => row.at = fresh.at,
                    _ => row.j = fresh.j,
                }
                fixture
                    .handle
                    .write(vec![ClientOp::Update(update)])
                    .await
                    .expect("update");
            }
            60..68 => {
                let key = format!("k{:04}", rng.below(KEYS));
                fixture
                    .handle
                    .write(vec![ClientOp::Delete(PrimaryKey::from(key.as_str()))])
                    .await
                    .expect("delete");
                model.remove(&key);
            }
            68..76 => fixture.flush().await,
            76..80 => fixture.compact().await,
            _ => check(&fixture, &model, &mut rng, 12).await,
        }
    }
    check(&fixture, &model, &mut rng, 60).await;
    fixture.flush().await;
    check(&fixture, &model, &mut rng, 60).await;
    fixture.compact().await;
    check(&fixture, &model, &mut rng, 60).await;
}

#[tokio::test]
async fn every_operator_matches_a_brute_force_model_over_every_index_kind() {
    for seed in [1, 2, 3, 4] {
        run(seed, 160).await;
    }
}
