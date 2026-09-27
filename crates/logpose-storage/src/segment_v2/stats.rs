//! The `Stats` section: postcard [`SegmentStats`].
//!
//! Per field: a zone map (`min`, `max`, `null_count`), the exact distinct
//! count, the 16 most frequent values, and for `int64`, `float64`, and
//! `timestamp` a 32-bucket equi-depth histogram. Array fields describe their
//! elements. Everything is computed exactly at build time, so there is no
//! sketch to merge; compaction recomputes stats from the rows it writes.

use super::{
    column::{ColumnBuf, ColumnData, ElemBuf, VarBuf},
    error::{DecodeResult, Malformed, SegmentError},
    pk::PkColumn,
    vector::VectorBuf,
};
use logpose_types::{
    SeqNo,
    schema::{FieldId, FieldType},
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

pub(crate) const STATS_ENCODING_POSTCARD: u16 = 1;
/// Number of most frequent values kept per field.
pub const TOP_VALUES: usize = 16;
/// Number of equi-depth histogram buckets for numeric fields.
pub const HISTOGRAM_BUCKETS: usize = 32;

/// A scalar value in stats. Postcard-safe, unlike `Value`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StatValue {
    /// A boolean.
    Bool(bool),
    /// A signed 64-bit integer (also `int64` primary keys).
    Int64(i64),
    /// A finite float.
    Float64(f64),
    /// A string (also `string` primary keys).
    String(String),
    /// Microseconds since the Unix epoch.
    Timestamp(i64),
}

impl StatValue {
    fn rank(&self) -> u8 {
        match self {
            Self::Bool(_) => 0,
            Self::Int64(_) => 1,
            Self::Float64(_) => 2,
            Self::String(_) => 3,
            Self::Timestamp(_) => 4,
        }
    }

    /// Total order: by variant, then by value (floats by `total_cmp`).
    #[must_use]
    pub fn total_cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Bool(left), Self::Bool(right)) => left.cmp(right),
            (Self::Int64(left), Self::Int64(right))
            | (Self::Timestamp(left), Self::Timestamp(right)) => left.cmp(right),
            (Self::Float64(left), Self::Float64(right)) => left.total_cmp(right),
            (Self::String(left), Self::String(right)) => left.cmp(right),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

/// A histogram bucket: values in `(previous upper, upper]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Largest value in the bucket.
    pub upper: StatValue,
    /// Number of values in the bucket.
    pub count: u64,
}

/// Statistics of one field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldStats {
    /// The field.
    pub field: FieldId,
    /// Rows whose value (or vector) is null.
    pub null_count: u32,
    /// Non-null values; for arrays, the number of elements.
    pub value_count: u64,
    /// Smallest value (element for arrays); `None` without values or for
    /// `json` and vector fields.
    pub min: Option<StatValue>,
    /// Largest value (element for arrays).
    pub max: Option<StatValue>,
    /// Exact number of distinct values (elements for arrays); `None` for
    /// `json` and vector fields.
    pub distinct: Option<u64>,
    /// Up to 16 most frequent values with counts, most frequent first, ties
    /// in ascending value order.
    pub top: Vec<(StatValue, u64)>,
    /// Equi-depth buckets for `int64`, `float64`, and `timestamp`; empty
    /// otherwise.
    pub histogram: Vec<HistogramBucket>,
}

/// The payload of the `Stats` section.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SegmentStats {
    /// Number of rows.
    pub row_count: u32,
    /// Smallest row sequence number (0 when empty).
    pub min_seq_no: SeqNo,
    /// Largest row sequence number (0 when empty).
    pub max_seq_no: SeqNo,
    /// Smallest primary key.
    pub pk_min: Option<StatValue>,
    /// Largest primary key.
    pub pk_max: Option<StatValue>,
    /// Rows that have dynamic (`$extra`) keys.
    pub dynamic_rows: u32,
    /// One entry per vector and scalar field, ascending by field id.
    pub fields: Vec<FieldStats>,
}

impl SegmentStats {
    /// Stats of `field`.
    #[must_use]
    pub fn field(&self, field: FieldId) -> Option<&FieldStats> {
        self.fields
            .binary_search_by_key(&field, |stats| stats.field)
            .ok()
            .and_then(|index| self.fields.get(index))
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, SegmentError> {
        postcard::to_allocvec(self).map_err(|error| SegmentError::Encode(error.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let (stats, rest) = postcard::take_from_bytes::<Self>(bytes)
            .map_err(|error| Malformed::new(format!("stats do not decode: {error}")))?;
        if !rest.is_empty() {
            return Err(Malformed::new("trailing bytes after stats"));
        }
        if stats
            .fields
            .windows(2)
            .any(|pair| pair[0].field >= pair[1].field)
        {
            return Err(Malformed::new("stats fields are not sorted"));
        }
        Ok(stats)
    }
}

/// Inputs for [`compute`].
pub(crate) struct StatsInput<'a> {
    pub(crate) seqs: &'a [SeqNo],
    pub(crate) pk: &'a PkColumn,
    pub(crate) vectors: &'a [VectorBuf],
    pub(crate) columns: &'a [ColumnBuf],
    pub(crate) dynamic_rows: u32,
}

pub(crate) fn compute(input: &StatsInput<'_>) -> SegmentStats {
    let row_count = u32::try_from(input.seqs.len()).unwrap_or(u32::MAX);
    let mut pk_values: Vec<StatValue> = match input.pk {
        PkColumn::Int64(keys) => keys.iter().copied().map(StatValue::Int64).collect(),
        PkColumn::String { .. } => (0..input.pk.len())
            .filter_map(|row| input.pk.get(row))
            .map(|pk| match pk {
                logpose_types::record::PrimaryKey::String(value) => StatValue::String(value),
                logpose_types::record::PrimaryKey::Int64(value) => StatValue::Int64(value),
            })
            .collect(),
    };
    pk_values.sort_by(StatValue::total_cmp);
    let mut fields = Vec::with_capacity(input.vectors.len() + input.columns.len());
    for vector in input.vectors {
        let nulls = u32::try_from(vector.nulls.len()).unwrap_or(u32::MAX);
        fields.push(FieldStats {
            field: vector.field,
            null_count: nulls,
            value_count: u64::from(vector.rows - nulls),
            min: None,
            max: None,
            distinct: None,
            top: Vec::new(),
            histogram: Vec::new(),
        });
    }
    for column in input.columns {
        fields.push(column_stats(column));
    }
    fields.sort_by_key(|stats| stats.field);
    SegmentStats {
        row_count,
        min_seq_no: input.seqs.iter().copied().min().unwrap_or(0),
        max_seq_no: input.seqs.iter().copied().max().unwrap_or(0),
        pk_min: pk_values.first().cloned(),
        pk_max: pk_values.last().cloned(),
        dynamic_rows: input.dynamic_rows,
        fields,
    }
}

fn column_stats(column: &ColumnBuf) -> FieldStats {
    let null_count = u32::try_from(column.nulls.len()).unwrap_or(u32::MAX);
    let non_null = |row: usize| u32::try_from(row).is_ok_and(|row| !column.nulls.contains(row));
    let timestamp = matches!(
        column.field_type,
        FieldType::Timestamp | FieldType::Array(logpose_types::schema::ElementType::Timestamp)
    );
    let int = |value: i64| {
        if timestamp {
            StatValue::Timestamp(value)
        } else {
            StatValue::Int64(value)
        }
    };
    let values: Option<Vec<StatValue>> = match &column.data {
        ColumnData::Bool(values) => Some(
            values
                .iter()
                .enumerate()
                .filter(|(row, _)| non_null(*row))
                .map(|(_, value)| StatValue::Bool(*value))
                .collect(),
        ),
        ColumnData::Int(values) => Some(
            values
                .iter()
                .enumerate()
                .filter(|(row, _)| non_null(*row))
                .map(|(_, value)| int(*value))
                .collect(),
        ),
        ColumnData::Float(values) => Some(
            values
                .iter()
                .enumerate()
                .filter(|(row, _)| non_null(*row))
                .map(|(_, value)| StatValue::Float64(*value))
                .collect(),
        ),
        ColumnData::Str(values) => Some(
            (0..values.len())
                .filter(|row| non_null(*row))
                .map(|row| string_value(values, row))
                .collect(),
        ),
        ColumnData::Json(_) => None,
        ColumnData::Array { elements, .. } => Some(match elements {
            ElemBuf::Bool(values) => values.iter().copied().map(StatValue::Bool).collect(),
            ElemBuf::Int(values) => values.iter().copied().map(int).collect(),
            ElemBuf::Float(values) => values.iter().copied().map(StatValue::Float64).collect(),
            ElemBuf::Str(values) => (0..values.len())
                .map(|index| string_value(values, index))
                .collect(),
        }),
    };
    let Some(mut values) = values else {
        return FieldStats {
            field: column.field,
            null_count,
            value_count: u64::from(column.rows - null_count),
            min: None,
            max: None,
            distinct: None,
            top: Vec::new(),
            histogram: Vec::new(),
        };
    };
    values.sort_by(StatValue::total_cmp);
    let runs = runs(&values);
    let mut top: Vec<(StatValue, u64)> = runs
        .iter()
        .map(|(value, count)| ((*value).clone(), *count))
        .collect();
    // Stable sort keeps ascending value order among equal counts.
    top.sort_by(|left, right| right.1.cmp(&left.1));
    top.truncate(TOP_VALUES);
    let numeric = matches!(
        column.field_type,
        FieldType::Int64 | FieldType::Float64 | FieldType::Timestamp
    );
    FieldStats {
        field: column.field,
        null_count,
        value_count: values.len() as u64,
        min: values.first().cloned(),
        max: values.last().cloned(),
        distinct: Some(runs.len() as u64),
        top,
        histogram: if numeric {
            histogram(&values)
        } else {
            Vec::new()
        },
    }
}

fn string_value(values: &VarBuf, index: usize) -> StatValue {
    StatValue::String(String::from_utf8_lossy(values.get(index)).into_owned())
}

/// Runs of equal values in a sorted slice.
fn runs(values: &[StatValue]) -> Vec<(&StatValue, u64)> {
    let mut runs: Vec<(&StatValue, u64)> = Vec::new();
    for value in values {
        match runs.last_mut() {
            Some((last, count)) if last.total_cmp(value) == Ordering::Equal => *count += 1,
            _ => runs.push((value, 1)),
        }
    }
    runs
}

/// Equi-depth buckets over sorted values. Equal values never straddle a
/// bucket boundary, so there may be fewer than 32 buckets.
fn histogram(values: &[StatValue]) -> Vec<HistogramBucket> {
    let mut buckets = Vec::new();
    if values.is_empty() {
        return buckets;
    }
    let target = values.len().div_ceil(HISTOGRAM_BUCKETS);
    let mut start = 0;
    while start < values.len() {
        let mut end = (start + target).min(values.len());
        while end < values.len() && values[end].total_cmp(&values[end - 1]) == Ordering::Equal {
            end += 1;
        }
        buckets.push(HistogramBucket {
            upper: values[end - 1].clone(),
            count: (end - start) as u64,
        });
        start = end;
    }
    buckets
}
