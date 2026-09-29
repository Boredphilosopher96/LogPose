//! Unit tests of filter compilation semantics (the per-row reference semantics that the
//! per-unit evaluation follows).

use crate::{CompiledFilter, QueryError};
use criterion as _;
use logpose_types::{
    LogPoseError,
    filter::FilterExpr,
    record::Record,
    schema::{CollectionSchema, CreateCollectionSpec},
    value::{Timestamp, Value},
};
use serde_json::json;
use std::sync::Arc;
use tempfile as _;
use tokio as _;

fn schema() -> Arc<CollectionSchema> {
    let spec: CreateCollectionSpec = serde_json::from_value(json!({
        "name": "items",
        "primary_key": { "name": "sku", "type": "string" },
        "vectors": [{ "name": "embedding", "dimensions": 2 }],
        "fields": [
            { "name": "count", "type": "int64" },
            { "name": "price", "type": "float64" },
            { "name": "seen", "type": "timestamp" },
            { "name": "tags", "type": "array<string>" },
            { "name": "doc", "type": "json" }
        ],
        "dynamic_fields": true
    }))
    .expect("spec should parse");
    Arc::new(spec.to_schema().expect("schema should validate"))
}

fn float(value: f64) -> Value {
    Value::Float64(value)
}

fn seen(micros: i64) -> Value {
    Value::Timestamp(Timestamp::from_micros(micros).expect("timestamp"))
}

fn matches(filter: &FilterExpr, record: &Record) -> bool {
    CompiledFilter::compile(&schema(), filter)
        .expect("filter compiles")
        .matches_record(record)
}

fn item() -> Record {
    let mut record = Record::new("a-1")
        .with_field("count", Value::Int64(3))
        .with_field("price", Value::Float64(2.5))
        .with_field("seen", seen(1_000_000))
        .with_field(
            "tags",
            Value::Array(vec![
                Value::String("red".to_owned()),
                Value::String("blue".to_owned()),
            ]),
        );
    record.extra.insert("color".to_owned(), json!("red"));
    record.extra.insert("gone".to_owned(), json!(null));
    record
}

#[test]
fn integer_fields_compare_numerically_with_integral_and_fractional_bounds() {
    let record = item();
    assert!(matches(&FilterExpr::eq("count", float(3.0)), &record));
    assert!(matches(&FilterExpr::lt("count", float(3.5)), &record));
    assert!(!matches(&FilterExpr::gt("count", float(3.5)), &record));
    assert!(matches(&FilterExpr::gte("count", float(2.1)), &record));
    assert!(matches(
        &FilterExpr::in_values("count", vec![Value::Int64(1), Value::Int64(3)]),
        &record
    ));
    assert!(!matches(
        &FilterExpr::not_in("count", vec![Value::Int64(3)]),
        &record
    ));
}

#[test]
fn floats_and_timestamps_accept_their_operand_forms() {
    let record = item();
    assert!(matches(&FilterExpr::eq("price", float(2.5)), &record));
    assert!(matches(&FilterExpr::gt("price", Value::Int64(2)), &record));
    assert!(matches(&FilterExpr::eq("seen", seen(1_000_000)), &record));
    assert!(matches(
        &FilterExpr::eq("seen", Value::Int64(1_000_000)),
        &record
    ));
    assert!(matches(&FilterExpr::lt("seen", seen(2_000_000)), &record));
    assert!(matches(
        &FilterExpr::lt("seen", float(1_000_000.5)),
        &record
    ));
}

#[test]
fn arrays_match_by_element_and_ne_excludes_rows_holding_the_value() {
    let record = item();
    assert!(matches(&FilterExpr::eq("tags", "blue"), &record));
    assert!(matches(&FilterExpr::contains("tags", "blue"), &record));
    assert!(matches(
        &FilterExpr::contains_any("tags", vec![Value::from("x"), Value::from("red")]),
        &record
    ));
    assert!(!matches(&FilterExpr::ne("tags", "blue"), &record));
    assert!(matches(&FilterExpr::ne("tags", "green"), &record));
    assert!(matches(&FilterExpr::gte("tags", "red"), &record));
    let empty = Record::new("a-2").with_field("tags", Value::Array(Vec::new()));
    assert!(matches(&FilterExpr::is_null("tags"), &empty));
    assert!(!matches(&FilterExpr::ne("tags", "green"), &empty));
}

#[test]
fn nulls_never_match_ne_but_not_includes_them() {
    let missing = Record::new("a-3");
    assert!(!matches(
        &FilterExpr::ne("count", Value::Int64(1)),
        &missing
    ));
    assert!(!matches(
        &FilterExpr::not_in("count", vec![Value::Int64(1)]),
        &missing
    ));
    assert!(matches(
        &FilterExpr::negate(FilterExpr::eq("count", Value::Int64(1))),
        &missing
    ));
    assert!(matches(&FilterExpr::is_null("count"), &missing));
    assert!(!matches(&FilterExpr::exists("count"), &missing));
}

#[test]
fn dynamic_keys_keep_the_json_semantics() {
    let record = item();
    assert!(matches(&FilterExpr::eq("color", "red"), &record));
    assert!(matches(&FilterExpr::eq("$extra.color", "red"), &record));
    assert!(matches(
        &FilterExpr::eq("color", Value::Json(json!("red"))),
        &record
    ));
    assert!(matches(&FilterExpr::exists("gone"), &record));
    assert!(matches(&FilterExpr::is_null("gone"), &record));
    assert!(!matches(&FilterExpr::ne("gone", "x"), &record));
    assert!(!matches(&FilterExpr::exists("absent"), &record));
    assert!(matches(&FilterExpr::ne("color", Value::Int64(1)), &record));
    assert!(!matches(&FilterExpr::lt("color", Value::Int64(1)), &record));
    assert!(matches(&FilterExpr::gte("color", "red"), &record));
}

#[test]
fn the_primary_key_filters_like_a_never_null_field() {
    let record = item();
    assert!(matches(&FilterExpr::eq("sku", "a-1"), &record));
    assert!(matches(&FilterExpr::gt("sku", "a-0"), &record));
    assert!(!matches(&FilterExpr::is_null("sku"), &record));
}

#[test]
fn malformed_filters_and_vector_fields_are_refused_at_their_path() {
    let invalid = [
        (FilterExpr::and(Vec::new()), "filter.and"),
        (FilterExpr::eq("count", "3"), "filter.eq.count"),
        (FilterExpr::eq("count", float(3.5)), "filter.eq.count"),
        (FilterExpr::lt("count", true), "filter.range.count.lt"),
        (FilterExpr::exists("embedding"), "filter.exists"),
        (
            FilterExpr::contains("count", Value::Int64(3)),
            "filter.contains.count",
        ),
        (
            FilterExpr::or(vec![
                FilterExpr::exists("count"),
                FilterExpr::not_in("tags", Vec::new()),
            ]),
            "filter.or[1].not_in.tags",
        ),
    ];
    for (filter, path) in invalid {
        match CompiledFilter::compile(&schema(), &filter) {
            Err(QueryError::Storage(LogPoseError::InvalidArgument {
                field: Some(field), ..
            })) => assert_eq!(field, path, "{filter:?}"),
            other => unreachable!("{filter:?} should be refused, got {other:?}"),
        }
    }
}
