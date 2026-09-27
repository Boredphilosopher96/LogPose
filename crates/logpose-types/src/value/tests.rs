use super::*;
use crate::schema::{ElementType, FieldType};
use serde_json::json;
use std::{cmp::Ordering, collections::BTreeMap};

fn convert(json: serde_json::Value, field_type: FieldType) -> Result<Value, ValueError> {
    Value::from_json(json, field_type)
}

fn parse_json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).expect("test JSON should parse")
}

fn ts(micros: i64) -> Value {
    Value::Timestamp(Timestamp::from_micros(micros).expect("timestamp should be in range"))
}

#[test]
fn null_converts_for_every_type() {
    for field_type in [
        FieldType::Bool,
        FieldType::Int64,
        FieldType::Float64,
        FieldType::String,
        FieldType::Timestamp,
        FieldType::Array(ElementType::String),
        FieldType::Json,
    ] {
        assert_eq!(convert(json!(null), field_type), Ok(Value::Null));
    }
}

#[test]
fn converts_matching_kinds() {
    assert_eq!(convert(json!(true), FieldType::Bool), Ok(Value::Bool(true)));
    assert_eq!(convert(json!(-7), FieldType::Int64), Ok(Value::Int64(-7)));
    assert_eq!(
        convert(json!(12.5), FieldType::Float64),
        Ok(Value::Float64(12.5))
    );
    assert_eq!(
        convert(json!("acme"), FieldType::String),
        Ok(Value::String("acme".to_owned()))
    );
    let document = json!({ "nested": [1, "two", null] });
    assert_eq!(
        convert(document.clone(), FieldType::Json),
        Ok(Value::Json(document))
    );
}

#[test]
fn does_not_coerce_across_kinds() {
    let cases = [
        (json!("5"), FieldType::Int64, "string"),
        (json!(5), FieldType::String, "number"),
        (json!(true), FieldType::Int64, "bool"),
        (json!(1), FieldType::Bool, "number"),
        (json!("1.5"), FieldType::Float64, "string"),
        (json!(true), FieldType::Timestamp, "bool"),
        (json!([1]), FieldType::Int64, "array"),
        (json!({ "a": 1 }), FieldType::String, "object"),
        (
            json!("tag"),
            FieldType::Array(ElementType::String),
            "string",
        ),
    ];
    for (input, field_type, found) in cases {
        assert_eq!(
            convert(input, field_type),
            Err(ValueError::TypeMismatch {
                expected: field_type,
                found
            })
        );
    }
}

#[test]
fn int64_accepts_only_integral_numbers_in_range() {
    assert_eq!(
        convert(json!(1e3), FieldType::Int64),
        Ok(Value::Int64(1000))
    );
    assert_eq!(
        convert(json!(i64::MAX), FieldType::Int64),
        Ok(Value::Int64(i64::MAX))
    );
    assert_eq!(
        convert(parse_json("-9223372036854775808.0"), FieldType::Int64),
        Ok(Value::Int64(i64::MIN))
    );
    assert_eq!(
        convert(json!(1.5), FieldType::Int64),
        Err(ValueError::NotIntegral { value: 1.5 })
    );
    for out_of_range in [
        "9223372036854775808",
        "18446744073709551615",
        "9223372036854775807.0",
        "1e19",
        "-1e19",
    ] {
        assert!(
            matches!(
                convert(parse_json(out_of_range), FieldType::Int64),
                Err(ValueError::IntegerOutOfRange { .. })
            ),
            "{out_of_range} should be out of range"
        );
    }
}

#[test]
fn float64_accepts_exact_integers_only() {
    assert_eq!(
        convert(json!(3), FieldType::Float64),
        Ok(Value::Float64(3.0))
    );
    let largest_exact = 1_i64 << 53;
    assert_eq!(
        convert(json!(largest_exact), FieldType::Float64),
        Ok(Value::Float64(9_007_199_254_740_992.0))
    );
    for inexact in [
        json!(largest_exact + 1),
        json!(i64::MAX),
        json!(u64::MAX),
        json!(-(largest_exact + 1)),
    ] {
        assert!(
            matches!(
                convert(inexact.clone(), FieldType::Float64),
                Err(ValueError::InexactFloat { .. })
            ),
            "{inexact} should be inexact"
        );
    }
    assert_eq!(
        convert(json!(i64::MIN), FieldType::Float64),
        Ok(Value::Float64(-9_223_372_036_854_775_808.0))
    );
}

#[test]
fn float64_rejects_non_finite_and_folds_negative_zero() {
    assert_eq!(Value::float64(f64::NAN), Err(ValueError::NonFiniteFloat));
    assert_eq!(
        Value::float64(f64::INFINITY),
        Err(ValueError::NonFiniteFloat)
    );
    let converted = convert(parse_json("-0.0"), FieldType::Float64);
    assert!(
        matches!(converted, Ok(Value::Float64(zero)) if zero == 0.0 && zero.is_sign_positive()),
        "-0.0 should fold into 0.0, got {converted:?}"
    );
    assert_eq!(
        Value::Float64(f64::NAN).conform(FieldType::Float64),
        Err(ValueError::NonFiniteFloat)
    );
}

#[test]
fn timestamps_accept_rfc3339_and_integer_micros() {
    assert_eq!(
        convert(json!("1970-01-01T00:00:00Z"), FieldType::Timestamp),
        Ok(ts(0))
    );
    assert_eq!(
        convert(json!("2026-09-27T12:30:00.5+02:00"), FieldType::Timestamp),
        Ok(ts(1_790_505_000_500_000))
    );
    assert_eq!(
        convert(json!("2026-09-27t10:30:00.500000z"), FieldType::Timestamp),
        Ok(ts(1_790_505_000_500_000))
    );
    assert_eq!(
        convert(json!("2026-09-27 05:00:00.5-05:30"), FieldType::Timestamp),
        Ok(ts(1_790_505_000_500_000))
    );
    assert_eq!(
        convert(
            json!("2026-09-27T10:30:00.123456789Z"),
            FieldType::Timestamp
        ),
        Ok(ts(1_790_505_000_123_456)),
        "sub-microsecond digits are truncated"
    );
    assert_eq!(
        convert(json!(1_790_505_000_500_000_i64), FieldType::Timestamp),
        Ok(ts(1_790_505_000_500_000))
    );
    assert_eq!(convert(json!(-1), FieldType::Timestamp), Ok(ts(-1)));
    assert_eq!(
        convert(json!("2024-02-29T00:00:00Z"), FieldType::Timestamp),
        Ok(ts(1_709_164_800_000_000))
    );
}

#[test]
fn timestamps_reject_malformed_or_out_of_range_input() {
    for bad in [
        "2026-09-27",
        "2026-09-27T10:30:00",
        "2026-13-01T00:00:00Z",
        "2026-00-01T00:00:00Z",
        "2025-02-29T00:00:00Z",
        "2026-04-31T00:00:00Z",
        "2026-09-27T24:00:00Z",
        "2026-09-27T23:59:60Z",
        "2026-09-27T10:30:00.Z",
        "2026-09-27T10:30:00Zjunk",
        "2026-09-27T10:30:00+2:00",
        "2026-09-27T10:30:00+24:00",
        "26-09-27T10:30:00Z",
        "２026-09-27T10:30:00Z",
    ] {
        assert!(
            matches!(
                convert(json!(bad), FieldType::Timestamp),
                Err(ValueError::Timestamp(TimestampError::InvalidRfc3339 { .. }))
            ),
            "{bad} should be rejected"
        );
    }
    assert!(matches!(
        convert(json!("0000-01-01T00:00:00+00:01"), FieldType::Timestamp),
        Err(ValueError::Timestamp(TimestampError::OutOfRange { .. }))
    ));
    assert!(matches!(
        convert(json!(i64::MAX), FieldType::Timestamp),
        Err(ValueError::Timestamp(TimestampError::OutOfRange { .. }))
    ));
    assert!(matches!(
        convert(json!(1.5), FieldType::Timestamp),
        Err(ValueError::NotIntegral { .. })
    ));
}

#[test]
fn timestamp_bounds_and_formatting() {
    assert_eq!(Timestamp::MIN.to_rfc3339(), "0000-01-01T00:00:00Z");
    assert_eq!(Timestamp::MAX.to_rfc3339(), "9999-12-31T23:59:59.999999Z");
    assert_eq!(
        Timestamp::parse_rfc3339("0000-01-01T00:00:00Z"),
        Ok(Timestamp::MIN)
    );
    assert_eq!(
        Timestamp::parse_rfc3339("9999-12-31T23:59:59.999999Z"),
        Ok(Timestamp::MAX)
    );
    assert!(Timestamp::from_micros(Timestamp::MIN.as_micros() - 1).is_err());
    assert!(Timestamp::from_micros(Timestamp::MAX.as_micros() + 1).is_err());
    assert_eq!(Timestamp::UNIX_EPOCH.to_rfc3339(), "1970-01-01T00:00:00Z");
    let before_epoch = Timestamp::from_micros(-1).expect("in range");
    assert_eq!(before_epoch.to_rfc3339(), "1969-12-31T23:59:59.999999Z");

    for micros in [
        0,
        -1,
        1,
        951_782_400_000_000,
        1_790_505_000_123_456,
        Timestamp::MIN.as_micros(),
        Timestamp::MAX.as_micros(),
        -12_345_678_901_234_567,
    ] {
        let timestamp = Timestamp::from_micros(micros).expect("in range");
        assert_eq!(
            Timestamp::parse_rfc3339(&timestamp.to_rfc3339()),
            Ok(timestamp),
            "{micros} should round-trip"
        );
    }

    let decoded: Result<Timestamp, _> = serde_json::from_value(json!(i64::MIN));
    assert!(decoded.is_err(), "deserialization enforces the range");
}

#[test]
fn arrays_convert_elementwise_without_nulls_or_nesting() {
    assert_eq!(
        convert(json!([1, 2.0, 3]), FieldType::Array(ElementType::Int64)),
        Ok(Value::Array(vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Int64(3)
        ]))
    );
    assert_eq!(
        convert(json!([]), FieldType::Array(ElementType::Bool)),
        Ok(Value::Array(Vec::new()))
    );
    assert_eq!(
        convert(
            json!(["1970-01-01T00:00:00Z", 1]),
            FieldType::Array(ElementType::Timestamp)
        ),
        Ok(Value::Array(vec![ts(0), ts(1)]))
    );
    assert_eq!(
        convert(json!(["a", null]), FieldType::Array(ElementType::String)),
        Err(ValueError::NullArrayElement { index: 1 })
    );
    assert_eq!(
        convert(json!(["a", ["b"]]), FieldType::Array(ElementType::String)),
        Err(ValueError::ArrayElement {
            index: 1,
            source: Box::new(ValueError::TypeMismatch {
                expected: FieldType::String,
                found: "array"
            })
        })
    );
    let error = convert(json!([1, 2.5]), FieldType::Array(ElementType::Int64))
        .expect_err("fractional element should fail");
    assert_eq!(error.to_string(), "array element 1: 2.5 is not an integer");
}

#[test]
fn conform_checks_typed_values_without_coercion() {
    assert_eq!(
        Value::Int64(1).conform(FieldType::Float64),
        Err(ValueError::TypeMismatch {
            expected: FieldType::Float64,
            found: "int64"
        })
    );
    assert_eq!(Value::Null.conform(FieldType::Bool), Ok(Value::Null));
    assert_eq!(
        Value::Float64(-0.0).conform(FieldType::Float64),
        Ok(Value::Float64(0.0))
    );
    assert!(
        Value::Array(vec![Value::Array(Vec::new())])
            .conform(FieldType::Array(ElementType::Int64))
            .is_err()
    );
    assert_eq!(
        Value::Array(vec![Value::Null]).conform(FieldType::Array(ElementType::Int64)),
        Err(ValueError::NullArrayElement { index: 0 })
    );
    assert!(
        Value::String("x".to_owned())
            .conform(FieldType::Json)
            .is_err()
    );
}

#[test]
fn converts_back_to_user_json() {
    let cases = [
        (json!(null), FieldType::Int64),
        (json!(true), FieldType::Bool),
        (json!(42), FieldType::Int64),
        (json!(12.5), FieldType::Float64),
        (json!("acme"), FieldType::String),
        (json!("2026-09-27T10:30:00.500000Z"), FieldType::Timestamp),
        (json!(["a", "b"]), FieldType::Array(ElementType::String)),
        (json!({ "k": [1, 2] }), FieldType::Json),
    ];
    for (input, field_type) in cases {
        let value = convert(input.clone(), field_type).expect("input should convert");
        assert_eq!(value.to_json(), input);
        assert_eq!(convert(value.to_json(), field_type), Ok(value));
    }
    assert_eq!(ts(0).to_json(), json!("1970-01-01T00:00:00Z"));
    assert_eq!(Value::Float64(f64::NAN).to_json(), json!(null));
}

#[test]
fn stored_serialization_is_tagged() {
    let value = Value::Array(vec![Value::Int64(1), ts(5), Value::Null]);
    let stored = serde_json::to_value(&value).expect("value should serialize");
    assert_eq!(
        stored,
        json!({ "array": [{ "int64": 1 }, { "timestamp": 5 }, "null"] })
    );
    let decoded: Value = serde_json::from_value(stored).expect("value should decode");
    assert_eq!(decoded, value);
}

#[test]
fn total_order_is_usable_for_index_keys() {
    assert_eq!(Value::Null.total_cmp(&Value::Bool(false)), Ordering::Less);
    assert_eq!(Value::Int64(-5).total_cmp(&Value::Int64(3)), Ordering::Less);
    assert_eq!(
        Value::Float64(-1.5).total_cmp(&Value::Float64(0.0)),
        Ordering::Less
    );
    assert_eq!(
        Value::String("b".to_owned()).total_cmp(&Value::String("ab".to_owned())),
        Ordering::Greater
    );
    assert_eq!(ts(-1).total_cmp(&ts(0)), Ordering::Less);
    assert_eq!(
        Value::Array(vec![Value::Int64(1)])
            .total_cmp(&Value::Array(vec![Value::Int64(1), Value::Int64(0)])),
        Ordering::Less
    );

    let left = parse_json(r#"{"b": 1, "a": 2}"#);
    let right = parse_json(r#"{"a": 2, "b": 1}"#);
    assert_eq!(
        Value::Json(left).total_cmp(&Value::Json(right)),
        Ordering::Equal
    );
    assert_eq!(
        Value::Json(json!(1)).total_cmp(&Value::Json(json!(1.5))),
        Ordering::Less
    );

    let mut index = BTreeMap::new();
    for (row, value) in [3.5, -2.0, 0.0, 3.5].into_iter().enumerate() {
        let key = Value::float64(value).expect("finite");
        index
            .entry(OrderedValue(key))
            .or_insert_with(Vec::new)
            .push(row);
    }
    let keys: Vec<_> = index.keys().map(|key| key.0.clone()).collect();
    assert_eq!(
        keys,
        vec![
            Value::Float64(-2.0),
            Value::Float64(0.0),
            Value::Float64(3.5)
        ]
    );
    assert_eq!(
        index.get(&OrderedValue(Value::Float64(3.5))),
        Some(&vec![0, 3])
    );
}
