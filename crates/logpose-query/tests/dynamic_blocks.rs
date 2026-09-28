//! Filters on an `$extra` key over segments whose dynamic blocks differ in which keys they hold:
//! a block where no row has the key (but rows have other keys), a block where no row has any
//! key, and a block mixing absent, null, and valued keys, with memtable overwrites and deletes
//! on top. Skipping a block without the key must not change `exists`, `is_null`, their
//! negations, or comparisons.

use criterion as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query::{FilterExpr, count_view};
use logpose_storage::IndexPolicy;
use logpose_types::{
    DistanceMetric,
    record::{ClientOp, PrimaryKey, Record},
};
use rayon as _;
use roaring as _;
use serde as _;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use thiserror as _;

mod support;

use support::Fixture;

/// A row's `$extra`: `x` (absent, or a value, null included) and whether it has `y`.
#[derive(Clone, Debug)]
struct Row {
    x: Option<Json>,
    y: bool,
}

fn row_for(index: u64) -> Row {
    match index {
        // Block 0: every row has `y`, none has `x`.
        0..4_500 => Row { x: None, y: true },
        // No dynamic keys at all.
        4_500..6_000 => Row { x: None, y: false },
        // Absent, null, a number, and a string.
        _ => match index % 4 {
            0 => Row { x: None, y: true },
            1 => Row {
                x: Some(Json::Null),
                y: false,
            },
            2 => Row {
                x: Some(json!(1)),
                y: index % 3 == 0,
            },
            _ => Row {
                x: Some(json!("a")),
                y: true,
            },
        },
    }
}

fn to_record(key: &str, row: &Row) -> Record {
    let mut record = Record::new(key).with_vector("vector", vec![1.0, 0.0]);
    if let Some(x) = &row.x {
        record.extra.insert("x".to_owned(), x.clone());
    }
    if row.y {
        record.extra.insert("y".to_owned(), json!(true));
    }
    record
}

fn key(index: u64) -> String {
    format!("d{index:05}")
}

/// The documented semantics: a key is present or absent; `exists` when present, `is_null`
/// when present and null; comparisons need a present non-null value.
fn matches(filter: &FilterExpr, row: &Row) -> bool {
    match filter {
        FilterExpr::Not(child) => !matches(child, row),
        FilterExpr::Exists { .. } => row.x.is_some(),
        FilterExpr::IsNull { .. } => matches!(row.x, Some(Json::Null)),
        FilterExpr::Eq { .. } => row.x == Some(json!(1)),
        FilterExpr::Ne { .. } => row
            .x
            .as_ref()
            .is_some_and(|x| !x.is_null() && *x != json!(1)),
        other => unreachable!("{other:?}"),
    }
}

#[tokio::test]
async fn block_skipping_keeps_presence_and_null_semantics() {
    let fixture = Fixture::new(
        "dynamic-blocks",
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
    let mut model = BTreeMap::new();
    for chunk in (0..9_000_u64).collect::<Vec<_>>().chunks(1_500) {
        let rows = chunk
            .iter()
            .map(|index| (key(*index), row_for(*index)))
            .collect::<Vec<_>>();
        fixture
            .upsert(rows.iter().map(|(key, row)| to_record(key, row)).collect())
            .await;
        model.extend(rows);
    }
    fixture.flush().await;
    // Overwrites in the memtable (a skipped block's row gains the key) and deletes.
    let overwrites = [
        (
            key(10),
            Row {
                x: Some(Json::Null),
                y: true,
            },
        ),
        (
            key(4_600),
            Row {
                x: Some(json!(1)),
                y: false,
            },
        ),
        (key(7_001), Row { x: None, y: false }),
    ];
    fixture
        .upsert(
            overwrites
                .iter()
                .map(|(key, row)| to_record(key, row))
                .collect(),
        )
        .await;
    model.extend(overwrites);
    let deleted = [key(11), key(7_002), key(7_005)];
    fixture
        .handle
        .write(
            deleted
                .iter()
                .map(|key| ClientOp::Delete(PrimaryKey::from(key.as_str())))
                .collect(),
        )
        .await
        .expect("delete");
    for key in &deleted {
        model.remove(key);
    }
    let leaves = [
        FilterExpr::exists("$extra.x"),
        FilterExpr::is_null("$extra.x"),
        FilterExpr::eq("$extra.x", 1_i64),
        FilterExpr::ne("$extra.x", 1_i64),
    ];
    for round in 0..2 {
        let view = fixture.view().await;
        for leaf in &leaves {
            for filter in [leaf.clone(), FilterExpr::Not(Box::new(leaf.clone()))] {
                let expected = model.values().filter(|row| matches(&filter, row)).count();
                assert_eq!(
                    count_view(&view, Some(&filter)).await.expect("count"),
                    expected as u64,
                    "round {round}: {filter:?}"
                );
            }
        }
        // The second round reads the memtable rows from a segment.
        fixture.flush().await;
    }
}
