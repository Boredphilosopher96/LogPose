//! Typed scalar values and their JSON coercion rules.
//!
//! [`Value::from_json`] converts a user JSON value into the [`Value`] for a
//! declared [`FieldType`]. The coercions are deliberately narrow:
//!
//! | target      | accepted JSON                                              |
//! |-------------|------------------------------------------------------------|
//! | any         | `null` becomes [`Value::Null`]; nullability is a record rule |
//! | `bool`      | `true` or `false`                                          |
//! | `int64`     | an integer in `i64` range, or an integral float in range    |
//! | `float64`   | a finite number; integers only if exactly representable     |
//! | `string`    | a string                                                   |
//! | `timestamp` | an RFC 3339 string, or integer microseconds since the epoch |
//! | `array<T>`  | an array whose elements convert to `T`; no null elements    |
//! | `json`      | anything                                                   |
//!
//! Nothing else is coerced: numbers never become strings, strings never
//! become numbers, and booleans never become integers. `-0.0` is stored as
//! `0.0` so equal floats have one index key.

pub mod codec;
mod ordering;
mod timestamp;

pub use codec::CodecError;
pub use ordering::OrderedValue;
pub use timestamp::{Timestamp, TimestampError};

use crate::schema::{ElementType, FieldType};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value as JsonValue};
use thiserror::Error;

/// `2^63` as an `f64`: the first float above the `i64` range.
const I64_UPPER_BOUND: f64 = 9_223_372_036_854_775_808.0;
/// `2^64` as an `f64`: the first float above the `u64` range.
const U64_UPPER_BOUND: f64 = 18_446_744_073_709_551_616.0;

/// Reasons a value does not convert to, or conform to, a field type.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum ValueError {
    /// The value has the wrong kind for the field type.
    #[error("expected {expected}, found {found}")]
    TypeMismatch {
        /// The declared type.
        expected: FieldType,
        /// The kind of value that was supplied.
        found: &'static str,
    },
    /// A float with a fractional part was given for an integer type.
    #[error("{value} is not an integer")]
    NotIntegral {
        /// The rejected number.
        value: f64,
    },
    /// A number is outside the `i64` range.
    #[error("{value} is out of range for int64")]
    IntegerOutOfRange {
        /// The rejected number, as written.
        value: String,
    },
    /// An integer cannot be represented exactly as an `f64`.
    #[error("{value} cannot be represented exactly as float64")]
    InexactFloat {
        /// The rejected integer.
        value: String,
    },
    /// A float is NaN or infinite.
    #[error("float64 values must be finite")]
    NonFiniteFloat,
    /// An array element is null. Arrays cannot hold nulls.
    #[error("array element {index} is null")]
    NullArrayElement {
        /// Position of the null element.
        index: usize,
    },
    /// An array element does not conform to the element type.
    #[error("array element {index}: {source}")]
    ArrayElement {
        /// Position of the bad element.
        index: usize,
        /// Why it was rejected.
        source: Box<ValueError>,
    },
    /// A timestamp is malformed or out of range.
    #[error(transparent)]
    Timestamp(#[from] TimestampError),
}

/// A typed field value.
///
/// Serialized with an explicit type tag, for example `{"int64": 5}`, `"null"`,
/// or `{"timestamp": 1790000000000000}`, so stored values round-trip without
/// a schema. User-facing JSON uses [`Value::from_json`] and
/// [`Value::to_json`] instead.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    /// No value.
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed 64-bit integer.
    Int64(i64),
    /// A finite 64-bit float.
    Float64(f64),
    /// A UTF-8 string.
    String(String),
    /// Microseconds since the Unix epoch.
    Timestamp(Timestamp),
    /// A flat array of non-null values of one element type.
    Array(Vec<Value>),
    /// An arbitrary JSON document.
    Json(JsonValue),
}

impl Value {
    /// Build a float value, rejecting NaN and infinities and storing `-0.0`
    /// as `0.0`.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::NonFiniteFloat`] for NaN or an infinity.
    pub fn float64(value: f64) -> Result<Self, ValueError> {
        if value.is_finite() {
            Ok(Self::Float64(if value == 0.0 { 0.0 } else { value }))
        } else {
            Err(ValueError::NonFiniteFloat)
        }
    }

    /// Whether this is [`Value::Null`].
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Short name of this value's kind, used in error messages.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int64(_) => "int64",
            Self::Float64(_) => "float64",
            Self::String(_) => "string",
            Self::Timestamp(_) => "timestamp",
            Self::Array(_) => "array",
            Self::Json(_) => "json",
        }
    }

    /// Convert user JSON into a value of `field_type`, following the
    /// coercion rules in the [module documentation](self).
    ///
    /// # Errors
    ///
    /// Returns a [`ValueError`] when the JSON does not convert.
    pub fn from_json(json: JsonValue, field_type: FieldType) -> Result<Self, ValueError> {
        match (field_type, json) {
            (_, JsonValue::Null) => Ok(Self::Null),
            (FieldType::Json, json) => Ok(Self::Json(json)),
            (FieldType::Bool, JsonValue::Bool(value)) => Ok(Self::Bool(value)),
            (FieldType::Int64, JsonValue::Number(number)) => {
                number_to_i64(&number).map(Self::Int64)
            }
            (FieldType::Float64, JsonValue::Number(number)) => {
                number_to_f64(&number).and_then(Self::float64)
            }
            (FieldType::String, JsonValue::String(value)) => Ok(Self::String(value)),
            (FieldType::Timestamp, JsonValue::String(value)) => {
                Ok(Self::Timestamp(Timestamp::parse_rfc3339(&value)?))
            }
            (FieldType::Timestamp, JsonValue::Number(number)) => Ok(Self::Timestamp(
                Timestamp::from_micros(number_to_i64(&number)?)?,
            )),
            (FieldType::Array(element), JsonValue::Array(items)) => items
                .into_iter()
                .enumerate()
                .map(|(index, item)| {
                    if item.is_null() {
                        return Err(ValueError::NullArrayElement { index });
                    }
                    Self::from_json(item, element.into()).map_err(|source| {
                        ValueError::ArrayElement {
                            index,
                            source: Box::new(source),
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            (expected, other) => Err(ValueError::TypeMismatch {
                expected,
                found: json_kind(&other),
            }),
        }
    }

    /// Check that an already typed value conforms to `field_type` and return
    /// it in canonical form: `-0.0` becomes `0.0`, and a JSON `null` in a
    /// `json` field becomes [`Value::Null`], matching [`Value::from_json`],
    /// so null has one representation. [`Value::Null`] conforms to every
    /// type; nullability is checked by the record validator. No coercion
    /// happens here: an `Int64` does not conform to `float64`.
    ///
    /// # Errors
    ///
    /// Returns a [`ValueError`] when the value does not conform.
    pub fn conform(self, field_type: FieldType) -> Result<Self, ValueError> {
        match (field_type, self) {
            (_, Self::Null) | (FieldType::Json, Self::Json(JsonValue::Null)) => Ok(Self::Null),
            (FieldType::Float64, Self::Float64(value)) => Self::float64(value),
            (FieldType::Bool, value @ Self::Bool(_))
            | (FieldType::Int64, value @ Self::Int64(_))
            | (FieldType::String, value @ Self::String(_))
            | (FieldType::Timestamp, value @ Self::Timestamp(_))
            | (FieldType::Json, value @ Self::Json(_)) => Ok(value),
            (FieldType::Array(element), Self::Array(items)) => conform_array(items, element),
            (expected, other) => Err(ValueError::TypeMismatch {
                expected,
                found: other.kind(),
            }),
        }
    }

    /// Convert to user-facing JSON. Timestamps become RFC 3339 strings in
    /// UTC. A non-finite float, which only direct construction can produce,
    /// becomes `null`.
    #[must_use]
    pub fn to_json(&self) -> JsonValue {
        self.clone().into_json()
    }

    /// Convert to user-facing JSON, consuming the value.
    #[must_use]
    pub fn into_json(self) -> JsonValue {
        match self {
            Self::Null => JsonValue::Null,
            Self::Bool(value) => JsonValue::Bool(value),
            Self::Int64(value) => JsonValue::Number(value.into()),
            Self::Float64(value) => {
                Number::from_f64(value).map_or(JsonValue::Null, JsonValue::Number)
            }
            Self::String(value) => JsonValue::String(value),
            Self::Timestamp(value) => JsonValue::String(value.to_rfc3339()),
            Self::Array(items) => {
                JsonValue::Array(items.into_iter().map(Self::into_json).collect())
            }
            Self::Json(value) => value,
        }
    }
}

impl From<Value> for JsonValue {
    fn from(value: Value) -> Self {
        value.into_json()
    }
}

fn conform_array(items: Vec<Value>, element: ElementType) -> Result<Value, ValueError> {
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            if item.is_null() {
                return Err(ValueError::NullArrayElement { index });
            }
            item.conform(element.into())
                .map_err(|source| ValueError::ArrayElement {
                    index,
                    source: Box::new(source),
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

/// Kind name of a JSON value, used in error messages.
pub(crate) fn json_kind(json: &JsonValue) -> &'static str {
    match json {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

/// Convert a JSON number to `i64`: integers in range, or integral floats in
/// range.
pub(crate) fn number_to_i64(number: &Number) -> Result<i64, ValueError> {
    if let Some(value) = number.as_i64() {
        return Ok(value);
    }
    if number.is_u64() {
        return Err(ValueError::IntegerOutOfRange {
            value: number.to_string(),
        });
    }
    let value = number
        .as_f64()
        .ok_or_else(|| ValueError::IntegerOutOfRange {
            value: number.to_string(),
        })?;
    if value.fract() != 0.0 {
        return Err(ValueError::NotIntegral { value });
    }
    if !(-I64_UPPER_BOUND..I64_UPPER_BOUND).contains(&value) {
        return Err(ValueError::IntegerOutOfRange {
            value: number.to_string(),
        });
    }
    // In range and integral, so the cast is exact.
    Ok(value as i64)
}

/// Convert a JSON number to `f64`. Integers must be exactly representable.
fn number_to_f64(number: &Number) -> Result<f64, ValueError> {
    let inexact = || ValueError::InexactFloat {
        value: number.to_string(),
    };
    if let Some(value) = number.as_i64() {
        let converted = value as f64;
        // `converted as i64` saturates, so `i64::MAX` would round-trip
        // through 2^63 without the explicit bound check.
        let exact = converted < I64_UPPER_BOUND && converted as i64 == value;
        return if exact { Ok(converted) } else { Err(inexact()) };
    }
    if let Some(value) = number.as_u64() {
        let converted = value as f64;
        let exact = converted < U64_UPPER_BOUND && converted as u64 == value;
        return if exact { Ok(converted) } else { Err(inexact()) };
    }
    number.as_f64().ok_or(ValueError::NonFiniteFloat)
}

#[cfg(test)]
mod tests;
