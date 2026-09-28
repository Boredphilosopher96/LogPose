//! Scroll paging against a brute-force model: pages through a pinned cursor, ordered by a
//! field with heavy ties (and nulls), must together return exactly the rows of the snapshot the
//! first page read, in `(value, key)` order, whatever is written, flushed, or compacted
//! between pages. Cursors are checked for tampering.

use async_trait as _;
use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{Cursor, FilterExpr, ScrollOrder, ScrollRequest, count_view, scroll};
use logpose_storage::{IndexPolicy, Projection, SchemaChange, read::Direction};
use logpose_types::{
    DistanceMetric,
    record::{ClientOp, PrimaryKey, Record},
    schema::{FieldIndex, FieldType, ScalarFieldSpec},
    value::Value,
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json as _;
use std::{cmp::Ordering, collections::BTreeMap};
use thiserror as _;

mod support;

use support::{Fixture, Rng};

const KEYS: u64 = 200;

/// `g`: three values and nulls, so most rows tie. `h`: a float with ties.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Row {
    g: Option<i64>,
    h: Option<f64>,
}

fn random_row(rng: &mut Rng) -> Row {
    Row {
        g: (rng.below(5) != 0).then(|| rng.below(3) as i64),
        h: (rng.below(5) != 0).then(|| rng.below(4) as f64 * 0.5),
    }
}

fn to_record(key: &str, row: Row) -> Record {
    let mut record = Record::new(key).with_vector("vector", vec![1.0, 0.0]);
    if let Some(g) = row.g {
        for name in ["g_none", "g_sorted"] {
            record = record.with_field(name, Value::Int64(g));
        }
    }
    if let Some(h) = row.h {
        for name in ["h_none", "h_sorted"] {
            record = record.with_field(name, Value::Float64(h));
        }
    }
    record
}

async fn fixture() -> Fixture {
    let fixture = Fixture::new(
        "scroll-semantics",
        2,
        DistanceMetric::Dot,
        IndexPolicy::default(),
        &[],
    )
    .await;
    for (name, field_type, index) in [
        ("g_none", FieldType::Int64, FieldIndex::None),
        ("g_sorted", FieldType::Int64, FieldIndex::Sorted),
        ("h_none", FieldType::Float64, FieldIndex::None),
        (
            "h_sorted",
            FieldType::Float64,
            FieldIndex::InvertedAndSorted,
        ),
    ] {
        fixture
            .handle
            .alter_schema(SchemaChange::AddField(ScalarFieldSpec {
                name: name.to_owned(),
                field_type,
                index,
                nullable: true,
            }))
            .await
            .expect("field should be added");
    }
    fixture
}

/// The model's keys matching `filter` (`g` equal to a value, if any) in `order`.
fn expected(model: &BTreeMap<String, Row>, order: &ScrollOrder, g: Option<i64>) -> Vec<String> {
    let mut rows = model
        .iter()
        .filter(|(_, row)| g.is_none_or(|g| row.g == Some(g)))
        .map(|(key, row)| (key.clone(), *row))
        .collect::<Vec<_>>();
    if let ScrollOrder::Field { field, direction } = order {
        let value = |row: &Row| -> Option<f64> {
            if field.starts_with('g') {
                row.g.map(|g| g as f64)
            } else {
                row.h
            }
        };
        rows.sort_by(|left, right| match (value(&left.1), value(&right.1)) {
            (Some(a), Some(b)) => {
                let order = a.total_cmp(&b);
                let order = if *direction == Direction::Descending {
                    order.reverse()
                } else {
                    order
                };
                order.then_with(|| left.0.cmp(&right.0))
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => left.0.cmp(&right.0),
        });
    }
    rows.into_iter().map(|(key, _)| key).collect()
}

async fn mutate(fixture: &Fixture, model: &mut BTreeMap<String, Row>, rng: &mut Rng) {
    match rng.below(10) {
        0..5 => {
            let mut batch = BTreeMap::new();
            for _ in 0..=rng.below(12) {
                batch.insert(format!("k{:04}", rng.below(KEYS)), random_row(rng));
            }
            fixture
                .upsert(
                    batch
                        .iter()
                        .map(|(key, row)| to_record(key, *row))
                        .collect(),
                )
                .await;
            model.extend(batch);
        }
        5..7 => {
            let key = format!("k{:04}", rng.below(KEYS));
            fixture
                .handle
                .write(vec![ClientOp::Delete(PrimaryKey::from(key.as_str()))])
                .await
                .expect("delete");
            model.remove(&key);
        }
        7..9 => fixture.flush().await,
        _ => fixture.compact().await,
    }
}

async fn run(seed: u64) {
    let fixture = fixture().await;
    let mut model = BTreeMap::new();
    let mut rng = Rng::new(seed);
    for _ in 0..30 {
        mutate(&fixture, &mut model, &mut rng).await;
    }
    for round in 0..24 {
        let field = ["g_none", "g_sorted", "h_none", "h_sorted", ""][rng.below(5) as usize];
        let order = if field.is_empty() {
            ScrollOrder::Pk
        } else {
            ScrollOrder::Field {
                field: field.to_owned(),
                direction: if rng.below(2) == 0 {
                    Direction::Ascending
                } else {
                    Direction::Descending
                },
            }
        };
        let g = (rng.below(3) == 0).then(|| rng.below(3) as i64);
        let filter = g.map(|g| FilterExpr::eq("g_sorted", g));
        let limit = 1 + rng.below(9) as u32;
        let snapshot = expected(&model, &order, g);
        let count = count_view(&fixture.view().await, filter.as_ref())
            .await
            .expect("count");
        assert_eq!(count, snapshot.len() as u64, "count, round {round}");
        let mut cursor = None;
        let mut scrolled = Vec::new();
        loop {
            let page = scroll(
                &fixture.engine,
                &fixture.reference,
                ScrollRequest {
                    filter: filter.clone(),
                    order: order.clone(),
                    limit,
                    projection: Projection::scalars(),
                    cursor: cursor.take(),
                    token: None,
                },
            )
            .await
            .expect("scroll page");
            assert!(page.rows.len() <= limit as usize);
            scrolled.extend(page.rows.into_iter().map(|row| match row.record.pk {
                PrimaryKey::String(key) => key,
                PrimaryKey::Int64(_) => unreachable!(),
            }));
            match page.next {
                Some(next) => {
                    // Round-trip through the text clients see.
                    cursor = Some(next.to_string().parse::<Cursor>().expect("cursor parses"));
                }
                None => break,
            }
            // Writes, flushes, and compactions between pages do not change the scroll.
            for _ in 0..rng.below(3) {
                mutate(&fixture, &mut model, &mut rng).await;
            }
        }
        assert_eq!(
            scrolled, snapshot,
            "round {round}: {order:?}, filter {filter:?}, page size {limit}"
        );
    }
}

#[tokio::test]
async fn scroll_pages_return_the_pinned_snapshot_in_order_through_ties_and_writes() {
    for seed in [1, 2, 3] {
        run(seed).await;
    }
}

#[tokio::test]
async fn tampered_cursors_are_rejected() {
    let fixture = fixture().await;
    let records = (0..10)
        .map(|index| {
            to_record(
                &format!("k{index:04}"),
                Row {
                    g: Some(1),
                    h: None,
                },
            )
        })
        .collect();
    fixture.upsert(records).await;
    let page = scroll(
        &fixture.engine,
        &fixture.reference,
        ScrollRequest {
            filter: None,
            order: ScrollOrder::Field {
                field: "g_sorted".to_owned(),
                direction: Direction::Ascending,
            },
            limit: 3,
            projection: Projection::scalars(),
            cursor: None,
            token: None,
        },
    )
    .await
    .expect("first page");
    let text = page.next.expect("a cursor").to_string();
    const ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    for (index, original) in text.char_indices() {
        for replacement in ALPHABET.chars().filter(|c| *c != original).step_by(7) {
            let mut tampered = text.clone();
            tampered.replace_range(index..=index, &replacement.to_string());
            assert!(
                tampered.parse::<Cursor>().is_err(),
                "a cursor changed at {index} to {replacement} should be rejected"
            );
        }
    }
    assert!(text[..text.len() - 1].parse::<Cursor>().is_err());
    assert!(format!("{text}A").parse::<Cursor>().is_err());
}
