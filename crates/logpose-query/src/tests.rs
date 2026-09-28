//! Unit tests of filter compilation semantics (the per-row reference semantics that the
//! per-unit evaluation follows).

use crate::{CompiledFilter, QueryError};
use async_trait as _;
use criterion as _;
use logpose_types::{
    ScalarMetadataValue,
    filter::{FilterComparison, FilterExpr, FilterOperator},
    record::Record,
    schema::{CollectionSchema, CreateCollectionSpec},
    value::{Timestamp, Value},
};
use serde_json::json;
use std::sync::Arc;
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

fn cmp(field: &str, operator: FilterOperator, value: Option<ScalarMetadataValue>) -> FilterExpr {
    FilterExpr::Comparison(FilterComparison {
        field: field.to_owned(),
        operator,
        value,
    })
}

fn number(value: f64) -> Option<ScalarMetadataValue> {
    Some(ScalarMetadataValue::Number(
        serde_json::Number::from_f64(value).expect("finite"),
    ))
}

fn int(value: i64) -> Option<ScalarMetadataValue> {
    Some(ScalarMetadataValue::Number(value.into()))
}

fn text(value: &str) -> Option<ScalarMetadataValue> {
    Some(ScalarMetadataValue::String(value.to_owned()))
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
        .with_field(
            "seen",
            Value::Timestamp(Timestamp::from_micros(1_000_000).expect("timestamp")),
        )
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
fn integer_fields_compare_numerically_with_integral_and_fractional_operands() {
    let record = item();
    assert!(matches(
        &cmp("count", FilterOperator::Eq, number(3.0)),
        &record
    ));
    assert!(!matches(
        &cmp("count", FilterOperator::Eq, number(3.5)),
        &record
    ));
    assert!(matches(
        &cmp("count", FilterOperator::Lt, number(3.5)),
        &record
    ));
    assert!(!matches(
        &cmp("count", FilterOperator::Gt, number(3.5)),
        &record
    ));
    assert!(matches(
        &cmp("count", FilterOperator::Gte, number(2.1)),
        &record
    ));
    assert!(matches(
        &cmp("count", FilterOperator::Ne, number(3.5)),
        &record
    ));
    // A string never equals an integer, so every row with a value is "not equal".
    assert!(!matches(
        &cmp("count", FilterOperator::Eq, text("3")),
        &record
    ));
    assert!(matches(
        &cmp("count", FilterOperator::Ne, text("3")),
        &record
    ));
}

#[test]
fn floats_and_timestamps_accept_their_operand_forms() {
    let record = item();
    assert!(matches(
        &cmp("price", FilterOperator::Eq, number(2.5)),
        &record
    ));
    assert!(matches(&cmp("price", FilterOperator::Gt, int(2)), &record));
    assert!(matches(
        &cmp("seen", FilterOperator::Eq, text("1970-01-01T00:00:01Z")),
        &record
    ));
    assert!(matches(
        &cmp("seen", FilterOperator::Eq, int(1_000_000)),
        &record
    ));
    assert!(matches(
        &cmp("seen", FilterOperator::Lt, text("1970-01-01T00:00:02Z")),
        &record
    ));
}

#[test]
fn arrays_match_by_element_and_ne_excludes_rows_holding_the_value() {
    let record = item();
    assert!(matches(
        &cmp("tags", FilterOperator::Eq, text("blue")),
        &record
    ));
    assert!(!matches(
        &cmp("tags", FilterOperator::Ne, text("blue")),
        &record
    ));
    assert!(matches(
        &cmp("tags", FilterOperator::Ne, text("green")),
        &record
    ));
    let empty = Record::new("a-2").with_field("tags", Value::Array(Vec::new()));
    assert!(matches(&cmp("tags", FilterOperator::IsNull, None), &empty));
    assert!(!matches(
        &cmp("tags", FilterOperator::Ne, text("green")),
        &empty
    ));
}

#[test]
fn nulls_never_match_ne_but_not_includes_them() {
    let missing = Record::new("a-3");
    let ne = cmp("count", FilterOperator::Ne, int(1));
    assert!(!matches(&ne, &missing));
    let not_eq = FilterExpr::Not {
        child: Box::new(cmp("count", FilterOperator::Eq, int(1))),
    };
    assert!(matches(&not_eq, &missing));
    assert!(matches(
        &cmp("count", FilterOperator::IsNull, None),
        &missing
    ));
    assert!(!matches(
        &cmp("count", FilterOperator::Exists, None),
        &missing
    ));
    assert!(matches(
        &cmp("count", FilterOperator::Eq, Some(ScalarMetadataValue::Null)),
        &missing
    ));
}

#[test]
fn dynamic_keys_keep_the_json_semantics() {
    let record = item();
    assert!(matches(
        &cmp("color", FilterOperator::Eq, text("red")),
        &record
    ));
    assert!(matches(&cmp("gone", FilterOperator::Exists, None), &record));
    assert!(matches(&cmp("gone", FilterOperator::IsNull, None), &record));
    assert!(!matches(
        &cmp("absent", FilterOperator::Exists, None),
        &record
    ));
    assert!(matches(&cmp("color", FilterOperator::Ne, int(1)), &record));
    assert!(!matches(&cmp("color", FilterOperator::Lt, int(1)), &record));
}

#[test]
fn the_primary_key_filters_like_a_never_null_field() {
    let record = item();
    assert!(matches(
        &cmp("sku", FilterOperator::Eq, text("a-1")),
        &record
    ));
    assert!(matches(
        &cmp("sku", FilterOperator::Gt, text("a-0")),
        &record
    ));
    assert!(!matches(&cmp("sku", FilterOperator::IsNull, None), &record));
}

#[test]
fn malformed_filters_and_vector_fields_are_refused() {
    let invalid = [
        FilterExpr::And {
            children: Vec::new(),
        },
        cmp("count", FilterOperator::Eq, None),
        cmp("count", FilterOperator::Exists, int(1)),
        cmp(
            "count",
            FilterOperator::Lt,
            Some(ScalarMetadataValue::Bool(true)),
        ),
        cmp("embedding", FilterOperator::Exists, None),
    ];
    for filter in invalid {
        assert!(
            matches!(
                CompiledFilter::compile(&schema(), &filter),
                Err(QueryError::InvalidPredicate(_))
            ),
            "{filter:?}"
        );
    }
}
