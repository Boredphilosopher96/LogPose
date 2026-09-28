//! [`MemColumn`]: a memtable's typed values of one scalar field.

use logpose_types::{
    RowId,
    schema::FieldType,
    value::{Timestamp, Value},
};
use std::sync::Arc;

/// The values of one scalar field, one per slot from the slot the field was added. Persistent:
/// clone is O(1) and an append copies O(log n) nodes when shared.
#[derive(Clone)]
pub(crate) enum MemColumn {
    Bool(imbl::Vector<Option<bool>>),
    Int64(imbl::Vector<Option<i64>>),
    Float64(imbl::Vector<Option<f64>>),
    /// Microseconds since the Unix epoch.
    Timestamp(imbl::Vector<Option<i64>>),
    String(imbl::Vector<Option<Arc<str>>>),
    Array(imbl::Vector<Option<Arc<[Value]>>>),
    /// A JSON document.
    Json(imbl::Vector<Option<Arc<Value>>>),
}

impl MemColumn {
    pub(crate) fn new(field_type: FieldType) -> Self {
        match field_type {
            FieldType::Bool => Self::Bool(imbl::Vector::new()),
            FieldType::Int64 => Self::Int64(imbl::Vector::new()),
            FieldType::Float64 => Self::Float64(imbl::Vector::new()),
            FieldType::Timestamp => Self::Timestamp(imbl::Vector::new()),
            FieldType::String => Self::String(imbl::Vector::new()),
            FieldType::Array(_) => Self::Array(imbl::Vector::new()),
            FieldType::Json => Self::Json(imbl::Vector::new()),
        }
    }

    /// Append the next slot's value. A value of another type than the column's (which a
    /// value decoded for the field's type never is) is stored as null.
    pub(crate) fn push(&mut self, value: Option<Value>) {
        match (self, value) {
            (Self::Bool(cells), Some(Value::Bool(value))) => cells.push_back(Some(value)),
            (Self::Int64(cells), Some(Value::Int64(value))) => cells.push_back(Some(value)),
            (Self::Float64(cells), Some(Value::Float64(value))) => cells.push_back(Some(value)),
            (Self::Timestamp(cells), Some(Value::Timestamp(value))) => {
                cells.push_back(Some(value.as_micros()));
            }
            (Self::String(cells), Some(Value::String(value))) => {
                cells.push_back(Some(Arc::from(value)));
            }
            (Self::Array(cells), Some(Value::Array(values))) => {
                cells.push_back(Some(Arc::from(values)));
            }
            (Self::Json(cells), Some(value @ Value::Json(_))) => {
                cells.push_back(Some(Arc::new(value)));
            }
            (Self::Bool(cells), _) => cells.push_back(None),
            (Self::Int64(cells), _) | (Self::Timestamp(cells), _) => cells.push_back(None),
            (Self::Float64(cells), _) => cells.push_back(None),
            (Self::String(cells), _) => cells.push_back(None),
            (Self::Array(cells), _) => cells.push_back(None),
            (Self::Json(cells), _) => cells.push_back(None),
        }
    }

    /// The value at `row` (counted from the column's first slot); `None` for null.
    pub(crate) fn get(&self, row: RowId) -> Option<Value> {
        let row = row as usize;
        match self {
            Self::Bool(cells) => cells.get(row).copied().flatten().map(Value::Bool),
            Self::Int64(cells) => cells.get(row).copied().flatten().map(Value::Int64),
            Self::Float64(cells) => cells.get(row).copied().flatten().map(Value::Float64),
            Self::Timestamp(cells) => cells
                .get(row)
                .copied()
                .flatten()
                .and_then(|micros| Timestamp::from_micros(micros).ok())
                .map(Value::Timestamp),
            Self::String(cells) => cells
                .get(row)
                .and_then(Option::as_ref)
                .map(|value| Value::String(value.to_string())),
            Self::Array(cells) => cells
                .get(row)
                .and_then(Option::as_ref)
                .map(|values| Value::Array(values.to_vec())),
            Self::Json(cells) => cells
                .get(row)
                .and_then(Option::as_ref)
                .map(|value| value.as_ref().clone()),
        }
    }
}
