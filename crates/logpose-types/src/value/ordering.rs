//! A total order over [`Value`] for index keys.

use super::Value;
use serde_json::Value as JsonValue;
use std::cmp::Ordering;

impl Value {
    /// Compare two values under a total order suitable for index keys.
    ///
    /// Values of different kinds order by kind: null, bool, int64, float64,
    /// string, timestamp, array, json. Within a kind:
    ///
    /// - floats use [`f64::total_cmp`]; conversion already rejects NaN and
    ///   folds `-0.0` into `0.0`, so this matches numeric order
    /// - strings compare by bytes
    /// - arrays compare element by element, then by length
    /// - JSON compares null, bool, number, string, array, object in that
    ///   order; objects compare as key-sorted entry lists
    ///
    /// A column holds a single type, so cross-kind comparisons only matter
    /// for nulls, which sort first.
    #[must_use]
    pub fn total_cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Bool(left), Self::Bool(right)) => left.cmp(right),
            (Self::Int64(left), Self::Int64(right)) => left.cmp(right),
            (Self::Float64(left), Self::Float64(right)) => left.total_cmp(right),
            (Self::String(left), Self::String(right)) => left.cmp(right),
            (Self::Timestamp(left), Self::Timestamp(right)) => left.cmp(right),
            (Self::Array(left), Self::Array(right)) => cmp_slices(left, right, Self::total_cmp),
            (Self::Json(left), Self::Json(right)) => json_cmp(left, right),
            (left, right) => kind_rank(left).cmp(&kind_rank(right)),
        }
    }
}

/// A [`Value`] with `Eq` and `Ord` from [`Value::total_cmp`], for use as a
/// `BTreeMap` key in scalar indexes.
#[derive(Clone, Debug)]
pub struct OrderedValue(pub Value);

impl PartialEq for OrderedValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OrderedValue {}

impl PartialOrd for OrderedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl From<Value> for OrderedValue {
    fn from(value: Value) -> Self {
        Self(value)
    }
}

fn kind_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int64(_) => 2,
        Value::Float64(_) => 3,
        Value::String(_) => 4,
        Value::Timestamp(_) => 5,
        Value::Array(_) => 6,
        Value::Json(_) => 7,
    }
}

fn cmp_slices<T>(left: &[T], right: &[T], compare: impl Fn(&T, &T) -> Ordering) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| compare(left, right))
        .find(|ordering| ordering.is_ne())
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

fn json_rank(value: &JsonValue) -> u8 {
    match value {
        JsonValue::Null => 0,
        JsonValue::Bool(_) => 1,
        JsonValue::Number(_) => 2,
        JsonValue::String(_) => 3,
        JsonValue::Array(_) => 4,
        JsonValue::Object(_) => 5,
    }
}

fn json_cmp(left: &JsonValue, right: &JsonValue) -> Ordering {
    match (left, right) {
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            // Key-based order (float value, then text) stays transitive when
            // large integers share a float.
            let left_float = left.as_f64().unwrap_or(f64::NAN);
            let right_float = right.as_f64().unwrap_or(f64::NAN);
            left_float
                .total_cmp(&right_float)
                .then_with(|| left.to_string().cmp(&right.to_string()))
        }
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        (JsonValue::Array(left), JsonValue::Array(right)) => cmp_slices(left, right, json_cmp),
        (JsonValue::Object(left), JsonValue::Object(right)) => {
            // Sort entries so the order does not depend on whether serde_json
            // preserves insertion order.
            let mut left: Vec<_> = left.iter().collect();
            let mut right: Vec<_> = right.iter().collect();
            left.sort_by(|a, b| a.0.cmp(b.0));
            right.sort_by(|a, b| a.0.cmp(b.0));
            cmp_slices(
                &left,
                &right,
                |(left_key, left_value), (right_key, right_value)| {
                    left_key
                        .cmp(right_key)
                        .then_with(|| json_cmp(left_value, right_value))
                },
            )
        }
        (left, right) => json_rank(left).cmp(&json_rank(right)),
    }
}
