//! `ScalarColumn` sections: one typed column per scalar field.
//!
//! ```text
//! offset size field
//!      0    1 encoding        see below
//!      1    1 value_width     bytes per code or value (8 plain numbers,
//!                             1/2/4 dictionary codes, 4 offset-based, 0 bool)
//!      2    2 reserved
//!      4    4 row_count
//!      8    8 nulls_len       0 when no row is null
//!     16    8 dict_len        0 when there is no dictionary
//!     24    8 data_len
//!     32    4 dict_count      number of dictionary entries
//!     36   28 reserved
//!     64    . nulls           RoaringBitmap portable serialization, padded to 8
//!      .    . dict            present for dictionary encodings, padded to 8
//!      .    . data
//! ```
//!
//! | Code | Encoding | Types | Data |
//! | --- | --- | --- | --- |
//! | 1 | `Int64Plain` | `int64`, `timestamp` | `i64[row_count]`, nulls stored as 0 |
//! | 2 | `Float64Plain` | `float64` | `f64[row_count]`, nulls stored as 0.0 |
//! | 3 | `BoolBitmap` | `bool` | RoaringBitmap of true rows |
//! | 4 | `StringDict` | `string` | dict: `u32 offsets[d + 1]`, sorted unique bytes; data: codes |
//! | 5 | `StringPlain` | `string` | `u32 offsets[row_count + 1]`, then bytes |
//! | 6 | `Array` | `array<T>` | `u32 offsets[row_count + 1]` into elements, padding to 8, then a child block encoded as one of 1 to 5 over all elements |
//! | 7 | `JsonValue` | `json` | `u32 offsets[row_count + 1]`, then binary `Value` bytes |
//!
//! `StringPlain` is used when distinct values exceed half the rows. Null rows
//! of offset-based encodings have empty ranges, and a child block never has
//! nulls (arrays cannot hold them).

use super::{
    error::{DecodeResult, Malformed, Region, SegmentError},
    le::{
        Cursor, check_offsets, pad_to, padded, put_f64, put_i64, put_u8, put_u16, put_u32, put_u64,
        put_zeros, range_at, usize_from, usize_from_u64,
    },
};
use logpose_types::{
    schema::{ElementType, FieldId, FieldType},
    value::{Timestamp, Value, codec},
};
use roaring::RoaringBitmap;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) const COLUMN_HEADER_LEN: usize = 64;

/// Encoding codes of a scalar column block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnEncoding {
    /// `i64[row_count]`.
    Int64Plain,
    /// `f64[row_count]`.
    Float64Plain,
    /// Bitmap of true rows.
    BoolBitmap,
    /// Sorted dictionary and fixed-width codes.
    StringDict,
    /// Offsets and bytes.
    StringPlain,
    /// Offsets into a child block of elements.
    Array,
    /// Offsets and binary `Value` bytes.
    JsonValue,
}

impl ColumnEncoding {
    /// The on-disk code.
    #[must_use]
    pub fn code(self) -> u8 {
        match self {
            Self::Int64Plain => 1,
            Self::Float64Plain => 2,
            Self::BoolBitmap => 3,
            Self::StringDict => 4,
            Self::StringPlain => 5,
            Self::Array => 6,
            Self::JsonValue => 7,
        }
    }

    /// The encoding for an on-disk code.
    #[must_use]
    pub fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::Int64Plain,
            2 => Self::Float64Plain,
            3 => Self::BoolBitmap,
            4 => Self::StringDict,
            5 => Self::StringPlain,
            6 => Self::Array,
            7 => Self::JsonValue,
            _ => return None,
        })
    }

    /// Whether this encoding can store values of `field_type`.
    fn fits(self, field_type: FieldType) -> bool {
        matches!(
            (self, field_type),
            (Self::Int64Plain, FieldType::Int64 | FieldType::Timestamp)
                | (Self::Float64Plain, FieldType::Float64)
                | (Self::BoolBitmap, FieldType::Bool)
                | (Self::StringDict | Self::StringPlain, FieldType::String)
                | (Self::Array, FieldType::Array(_))
                | (Self::JsonValue, FieldType::Json)
        )
    }
}

// ---------------------------------------------------------------------------
// Build side
// ---------------------------------------------------------------------------

/// Variable-length byte values, one per row, appended in order.
#[derive(Clone, Debug, Default)]
pub(crate) struct VarBuf {
    pub(crate) bytes: Vec<u8>,
    /// `ends[i]` is one past the last byte of value `i`.
    pub(crate) ends: Vec<usize>,
}

impl VarBuf {
    pub(crate) fn push(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
        self.ends.push(self.bytes.len());
    }

    pub(crate) fn push_empty(&mut self) {
        self.ends.push(self.bytes.len());
    }

    /// Replace the last value, which must be empty, with `value`.
    pub(crate) fn set_last(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
        if let Some(last) = self.ends.last_mut() {
            *last = self.bytes.len();
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.ends.len()
    }

    pub(crate) fn get(&self, index: usize) -> &[u8] {
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        &self.bytes[start..self.ends[index]]
    }

    /// `u32 offsets[len + 1]` then the bytes.
    fn encode_plain(&self, out: &mut Vec<u8>) -> Result<(), SegmentError> {
        put_u32(out, 0);
        for end in &self.ends {
            put_u32(out, offset_u32(*end)?);
        }
        out.extend_from_slice(&self.bytes);
        Ok(())
    }
}

fn offset_u32(value: usize) -> Result<u32, SegmentError> {
    u32::try_from(value).map_err(|_| SegmentError::TooLarge {
        what: "variable-length data over 4 GiB in one column",
    })
}

/// Non-null elements of all arrays of one column.
#[derive(Clone, Debug)]
pub(crate) enum ElemBuf {
    Bool(Vec<bool>),
    Int(Vec<i64>),
    Float(Vec<f64>),
    Str(VarBuf),
}

impl ElemBuf {
    fn new(element: ElementType) -> Self {
        match element {
            ElementType::Bool => Self::Bool(Vec::new()),
            ElementType::Int64 | ElementType::Timestamp => Self::Int(Vec::new()),
            ElementType::Float64 => Self::Float(Vec::new()),
            ElementType::String => Self::Str(VarBuf::default()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Bool(values) => values.len(),
            Self::Int(values) => values.len(),
            Self::Float(values) => values.len(),
            Self::Str(values) => values.len(),
        }
    }
}

/// Typed row values of one column under construction.
#[derive(Clone, Debug)]
pub(crate) enum ColumnData {
    Bool(Vec<bool>),
    Int(Vec<i64>),
    Float(Vec<f64>),
    Str(VarBuf),
    Json(VarBuf),
    Array {
        /// `ends[i]` is one past the last element of row `i`.
        ends: Vec<usize>,
        elements: ElemBuf,
    },
}

/// One scalar column under construction. Rows start null; the last row may
/// be set once.
#[derive(Clone, Debug)]
pub(crate) struct ColumnBuf {
    pub(crate) field: FieldId,
    pub(crate) field_type: FieldType,
    pub(crate) nulls: RoaringBitmap,
    pub(crate) data: ColumnData,
    pub(crate) rows: u32,
}

impl ColumnBuf {
    pub(crate) fn new(field: FieldId, field_type: FieldType) -> Self {
        let data = match field_type {
            FieldType::Bool => ColumnData::Bool(Vec::new()),
            FieldType::Int64 | FieldType::Timestamp => ColumnData::Int(Vec::new()),
            FieldType::Float64 => ColumnData::Float(Vec::new()),
            FieldType::String => ColumnData::Str(VarBuf::default()),
            FieldType::Json => ColumnData::Json(VarBuf::default()),
            FieldType::Array(element) => ColumnData::Array {
                ends: Vec::new(),
                elements: ElemBuf::new(element),
            },
        };
        Self {
            field,
            field_type,
            nulls: RoaringBitmap::new(),
            data,
            rows: 0,
        }
    }

    /// Append a null row.
    pub(crate) fn push_null(&mut self) {
        self.nulls.insert(self.rows);
        self.rows += 1;
        match &mut self.data {
            ColumnData::Bool(values) => values.push(false),
            ColumnData::Int(values) => values.push(0),
            ColumnData::Float(values) => values.push(0.0),
            ColumnData::Str(values) | ColumnData::Json(values) => values.push_empty(),
            ColumnData::Array { ends, elements } => ends.push(elements.len()),
        }
    }

    /// Set the last row to `value`. [`Value::Null`] and a JSON `null` leave
    /// it null.
    pub(crate) fn set_last(&mut self, value: &Value) -> Result<(), SegmentError> {
        let Some(row) = self.rows.checked_sub(1) else {
            return Ok(());
        };
        if matches!(value, Value::Null | Value::Json(JsonValue::Null)) {
            return Ok(());
        }
        if !self.nulls.contains(row) {
            return Err(SegmentError::FieldAlreadySet { field: self.field });
        }
        let field = self.field;
        let field_type = self.field_type;
        let mismatch = |found: &'static str| SegmentError::ValueType {
            field,
            expected: field_type,
            found,
        };
        let index = usize_from(row);
        match (&mut self.data, field_type, value) {
            (ColumnData::Bool(values), _, Value::Bool(value)) => values[index] = *value,
            (ColumnData::Int(values), FieldType::Int64, Value::Int64(value)) => {
                values[index] = *value;
            }
            (ColumnData::Int(values), FieldType::Timestamp, Value::Timestamp(value)) => {
                values[index] = value.as_micros();
            }
            (ColumnData::Float(values), _, Value::Float64(value)) => {
                values[index] =
                    canonical_float(*value).ok_or_else(|| mismatch("a non-finite float"))?;
            }
            (ColumnData::Str(values), _, Value::String(value)) => values.set_last(value.as_bytes()),
            (ColumnData::Json(values), _, Value::Json(_)) => {
                let bytes = codec::encode(value)
                    .map_err(|error| SegmentError::Encode(error.to_string()))?;
                values.set_last(&bytes);
            }
            (
                ColumnData::Array { ends, elements },
                FieldType::Array(element),
                Value::Array(items),
            ) => {
                push_elements(elements, element, items).map_err(mismatch)?;
                if let Some(last) = ends.last_mut() {
                    *last = elements.len();
                }
            }
            (_, _, other) => return Err(mismatch(other.kind())),
        }
        self.nulls.remove(row);
        Ok(())
    }

    /// Encode the section payload. Returns the payload and its encoding.
    pub(crate) fn encode(&self) -> Result<(Vec<u8>, ColumnEncoding), SegmentError> {
        let nulls = (!self.nulls.is_empty()).then_some(&self.nulls);
        let mut out = Vec::new();
        let encoding = match &self.data {
            ColumnData::Bool(values) => {
                encode_bool(&mut out, self.rows, nulls, values.iter().copied())?
            }
            ColumnData::Int(values) => encode_ints(&mut out, self.rows, nulls, values)?,
            ColumnData::Float(values) => encode_floats(&mut out, self.rows, nulls, values)?,
            ColumnData::Str(values) => encode_strings(&mut out, self.rows, nulls, values)?,
            ColumnData::Json(values) => {
                let mut data = Vec::new();
                values.encode_plain(&mut data)?;
                write_block(
                    &mut out,
                    ColumnEncoding::JsonValue,
                    4,
                    self.rows,
                    nulls,
                    None,
                    &data,
                )?;
                ColumnEncoding::JsonValue
            }
            ColumnData::Array { ends, elements } => {
                let mut data = Vec::new();
                put_u32(&mut data, 0);
                for end in ends {
                    put_u32(&mut data, offset_u32(*end)?);
                }
                pad_to(&mut data, 8);
                let count = u32::try_from(elements.len()).map_err(|_| SegmentError::TooLarge {
                    what: "more than 2^32 array elements in one column",
                })?;
                match elements {
                    ElemBuf::Bool(values) => {
                        encode_bool(&mut data, count, None, values.iter().copied())?;
                    }
                    ElemBuf::Int(values) => {
                        encode_ints(&mut data, count, None, values)?;
                    }
                    ElemBuf::Float(values) => {
                        encode_floats(&mut data, count, None, values)?;
                    }
                    ElemBuf::Str(values) => {
                        encode_strings(&mut data, count, None, values)?;
                    }
                }
                write_block(
                    &mut out,
                    ColumnEncoding::Array,
                    4,
                    self.rows,
                    nulls,
                    None,
                    &data,
                )?;
                ColumnEncoding::Array
            }
        };
        Ok((out, encoding))
    }
}

/// Bytes per dictionary code for a dictionary of `entries` values.
fn code_width(entries: usize) -> u8 {
    match entries {
        0..=256 => 1,
        257..=65_536 => 2,
        _ => 4,
    }
}

/// Fold `-0.0` to `0.0`; `None` for NaN and infinities.
fn canonical_float(value: f64) -> Option<f64> {
    value
        .is_finite()
        .then_some(if value == 0.0 { 0.0 } else { value })
}

fn push_elements(
    elements: &mut ElemBuf,
    element: ElementType,
    items: &[Value],
) -> Result<(), &'static str> {
    // Validate first so a bad element leaves the buffer unchanged.
    for item in items {
        let ok = match (element, item) {
            (ElementType::Bool, Value::Bool(_))
            | (ElementType::Int64, Value::Int64(_))
            | (ElementType::String, Value::String(_))
            | (ElementType::Timestamp, Value::Timestamp(_)) => true,
            (ElementType::Float64, Value::Float64(value)) => value.is_finite(),
            _ => false,
        };
        if !ok {
            return Err(match item {
                Value::Null => "a null array element",
                Value::Float64(_) => "a non-finite float",
                other => other.kind(),
            });
        }
    }
    for item in items {
        match (&mut *elements, item) {
            (ElemBuf::Bool(values), Value::Bool(value)) => values.push(*value),
            (ElemBuf::Int(values), Value::Int64(value)) => values.push(*value),
            (ElemBuf::Int(values), Value::Timestamp(value)) => values.push(value.as_micros()),
            (ElemBuf::Float(values), Value::Float64(value)) => {
                values.push(canonical_float(*value).unwrap_or(0.0));
            }
            (ElemBuf::Str(values), Value::String(value)) => values.push(value.as_bytes()),
            _ => return Err(item.kind()),
        }
    }
    Ok(())
}

fn encode_bool(
    out: &mut Vec<u8>,
    rows: u32,
    nulls: Option<&RoaringBitmap>,
    values: impl Iterator<Item = bool>,
) -> Result<ColumnEncoding, SegmentError> {
    let mut trues = RoaringBitmap::new();
    for (row, value) in (0..rows).zip(values) {
        if value && !nulls.is_some_and(|nulls| nulls.contains(row)) {
            trues.insert(row);
        }
    }
    let data = serialize_bitmap(&trues)?;
    write_block(out, ColumnEncoding::BoolBitmap, 0, rows, nulls, None, &data)?;
    Ok(ColumnEncoding::BoolBitmap)
}

fn encode_ints(
    out: &mut Vec<u8>,
    rows: u32,
    nulls: Option<&RoaringBitmap>,
    values: &[i64],
) -> Result<ColumnEncoding, SegmentError> {
    let mut data = Vec::with_capacity(values.len() * 8);
    for value in values {
        put_i64(&mut data, *value);
    }
    write_block(out, ColumnEncoding::Int64Plain, 8, rows, nulls, None, &data)?;
    Ok(ColumnEncoding::Int64Plain)
}

fn encode_floats(
    out: &mut Vec<u8>,
    rows: u32,
    nulls: Option<&RoaringBitmap>,
    values: &[f64],
) -> Result<ColumnEncoding, SegmentError> {
    let mut data = Vec::with_capacity(values.len() * 8);
    for value in values {
        put_f64(&mut data, *value);
    }
    write_block(
        out,
        ColumnEncoding::Float64Plain,
        8,
        rows,
        nulls,
        None,
        &data,
    )?;
    Ok(ColumnEncoding::Float64Plain)
}

fn encode_strings(
    out: &mut Vec<u8>,
    rows: u32,
    nulls: Option<&RoaringBitmap>,
    values: &VarBuf,
) -> Result<ColumnEncoding, SegmentError> {
    let is_null = |row: usize| {
        nulls.is_some_and(|nulls| u32::try_from(row).is_ok_and(|row| nulls.contains(row)))
    };
    let mut distinct: BTreeMap<&[u8], u32> = BTreeMap::new();
    for row in 0..values.len() {
        if !is_null(row) {
            distinct.insert(values.get(row), 0);
        }
    }
    if distinct.len() * 2 > usize_from(rows) {
        let mut data = Vec::new();
        values.encode_plain(&mut data)?;
        write_block(
            out,
            ColumnEncoding::StringPlain,
            4,
            rows,
            nulls,
            None,
            &data,
        )?;
        return Ok(ColumnEncoding::StringPlain);
    }
    let mut dict = Vec::new();
    let mut dict_bytes = Vec::new();
    put_u32(&mut dict, 0);
    for (code, (value, slot)) in distinct.iter_mut().enumerate() {
        *slot = u32::try_from(code).map_err(|_| SegmentError::TooLarge {
            what: "dictionary over 2^32 entries",
        })?;
        dict_bytes.extend_from_slice(value);
        put_u32(&mut dict, offset_u32(dict_bytes.len())?);
    }
    dict.extend_from_slice(&dict_bytes);
    let width = code_width(distinct.len());
    let mut data = Vec::with_capacity(values.len() * usize::from(width));
    for row in 0..values.len() {
        let code = if is_null(row) {
            0
        } else {
            distinct.get(values.get(row)).copied().unwrap_or(0)
        };
        match width {
            // Codes fit their width by construction.
            1 => put_u8(&mut data, code as u8),
            2 => put_u16(&mut data, code as u16),
            _ => put_u32(&mut data, code),
        }
    }
    let dict_count = u32::try_from(distinct.len()).unwrap_or(u32::MAX);
    write_block(
        out,
        ColumnEncoding::StringDict,
        width,
        rows,
        nulls,
        Some((&dict, dict_count)),
        &data,
    )?;
    Ok(ColumnEncoding::StringDict)
}

fn serialize_bitmap(bitmap: &RoaringBitmap) -> Result<Vec<u8>, SegmentError> {
    let mut bytes = Vec::with_capacity(bitmap.serialized_size());
    bitmap
        .serialize_into(&mut bytes)
        .map_err(|error| SegmentError::Encode(error.to_string()))?;
    Ok(bytes)
}

/// Append one column block: header, nulls, dictionary, data.
fn write_block(
    out: &mut Vec<u8>,
    encoding: ColumnEncoding,
    value_width: u8,
    rows: u32,
    nulls: Option<&RoaringBitmap>,
    dict: Option<(&[u8], u32)>,
    data: &[u8],
) -> Result<(), SegmentError> {
    let nulls = nulls.map(serialize_bitmap).transpose()?.unwrap_or_default();
    let (dict, dict_count) = dict.unwrap_or((&[], 0));
    let start = out.len();
    put_u8(out, encoding.code());
    put_u8(out, value_width);
    put_u16(out, 0);
    put_u32(out, rows);
    put_u64(out, nulls.len() as u64);
    put_u64(out, dict.len() as u64);
    put_u64(out, data.len() as u64);
    put_u32(out, dict_count);
    put_zeros(out, COLUMN_HEADER_LEN - (out.len() - start));
    out.extend_from_slice(&nulls);
    pad_to_from(out, start, 8);
    out.extend_from_slice(dict);
    pad_to_from(out, start, 8);
    out.extend_from_slice(data);
    Ok(())
}

/// Pad so that `out.len() - start` is a multiple of `align`.
fn pad_to_from(out: &mut Vec<u8>, start: usize, align: usize) {
    let len = out.len() - start;
    put_zeros(out, padded(len, align) - len);
}

// ---------------------------------------------------------------------------
// Read side
// ---------------------------------------------------------------------------

/// Offsets and bytes of variable-length values.
#[derive(Clone, Debug, PartialEq)]
struct Strings {
    offsets: Vec<u32>,
    bytes: Vec<u8>,
}

impl Strings {
    fn heap_bytes(&self) -> u64 {
        (self.offsets.capacity() * 4 + self.bytes.capacity()) as u64
    }

    fn decode(cursor: &mut Cursor<'_>, count: usize, len: usize) -> DecodeResult<Self> {
        let start = cursor.position();
        let offsets = cursor.u32s(count + 1)?;
        let used = cursor.position() - start;
        let rest = len
            .checked_sub(used)
            .ok_or_else(|| Malformed::new("offsets exceed the data length"))?;
        let bytes = cursor.take(rest)?.to_vec();
        check_offsets(&offsets, bytes.len())?;
        Ok(Self { offsets, bytes })
    }

    fn get(&self, index: usize) -> DecodeResult<&[u8]> {
        range_at(&self.offsets, index)
            .and_then(|range| self.bytes.get(range))
            .ok_or_else(|| Malformed::new(format!("value {index} is out of range")))
    }

    fn get_str(&self, index: usize) -> DecodeResult<&str> {
        std::str::from_utf8(self.get(index)?)
            .map_err(|_| Malformed::new(format!("string {index} is not UTF-8")))
    }

    fn check_utf8(&self) -> DecodeResult<()> {
        for index in 0..self.offsets.len().saturating_sub(1) {
            self.get_str(index)?;
        }
        Ok(())
    }
}

/// Decoded values of one column block.
#[derive(Clone, Debug, PartialEq)]
enum Block {
    Int(Vec<i64>),
    Float(Vec<f64>),
    Bool(RoaringBitmap),
    Dict {
        dict: Strings,
        codes: Vec<u32>,
    },
    Plain(Strings),
    Json(Strings),
    Array {
        offsets: Vec<u32>,
        child: Box<Block>,
    },
}

impl Block {
    fn heap_bytes(&self) -> u64 {
        match self {
            Self::Int(values) => values.capacity() as u64 * 8,
            Self::Float(values) => values.capacity() as u64 * 8,
            Self::Bool(bits) => bits.serialized_size() as u64,
            Self::Dict { dict, codes } => dict.heap_bytes() + codes.capacity() as u64 * 4,
            Self::Plain(strings) | Self::Json(strings) => strings.heap_bytes(),
            Self::Array { offsets, child } => offsets.capacity() as u64 * 4 + child.heap_bytes(),
        }
    }
}

/// One decoded `ScalarColumn` section.
#[derive(Clone, Debug, PartialEq)]
pub struct ScalarColumn {
    region: Region,
    field_type: FieldType,
    row_count: usize,
    nulls: RoaringBitmap,
    block: Block,
}

impl ScalarColumn {
    /// Heap bytes the decoded column holds, which the buffer cache charges beside its bytes.
    #[must_use]
    pub fn heap_bytes(&self) -> u64 {
        self.nulls.serialized_size() as u64 + self.block.heap_bytes()
    }

    /// Decode and validate a payload for a field of `field_type`.
    pub(crate) fn decode(
        bytes: &[u8],
        field_type: FieldType,
        row_count: usize,
        region: Region,
    ) -> DecodeResult<Self> {
        let mut cursor = Cursor::new(bytes);
        let (nulls, block) = decode_block(&mut cursor, field_type, row_count, true)?;
        cursor.finish()?;
        Ok(Self {
            region,
            field_type,
            row_count,
            nulls,
            block,
        })
    }

    /// The field type the column was decoded as.
    #[must_use]
    pub fn field_type(&self) -> FieldType {
        self.field_type
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.row_count
    }

    /// Whether there are no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Rows whose value is null.
    #[must_use]
    pub fn nulls(&self) -> &RoaringBitmap {
        &self.nulls
    }

    /// The value of `row`, [`Value::Null`] for null rows.
    ///
    /// The value can be much larger than the bytes that store it: dictionary
    /// codes repeat their string, and an `array<bool>` cell of any length
    /// can be stored in a few bytes of bitmap.
    ///
    /// # Errors
    ///
    /// Fails if the row is out of range or its stored value is invalid
    /// (only a JSON cell or a timestamp can be, since the rest is checked
    /// when the column is decoded).
    pub fn value(&self, row: usize) -> Result<Value, SegmentError> {
        self.value_inner(row).map_err(|error| error.at(self.region))
    }

    /// Check every stored value that decoding does not already check (JSON
    /// cells and timestamps) without materializing rows, so the work and
    /// memory are bounded by the payload size. Once this passes,
    /// [`value`](Self::value) succeeds for every row.
    pub(crate) fn check_values(&self) -> Result<(), SegmentError> {
        let checked = match (&self.block, self.field_type) {
            (Block::Array { child, .. }, FieldType::Array(element)) => {
                check_stored(child, element.into(), &RoaringBitmap::new())
            }
            (block, field_type) => check_stored(block, field_type, &self.nulls),
        };
        checked.map_err(|error| error.at(self.region))
    }

    pub(crate) fn value_inner(&self, row: usize) -> DecodeResult<Value> {
        if row >= self.row_count {
            return Err(Malformed::new(format!("row {row} is out of range")));
        }
        let row_u32 = u32::try_from(row).map_err(|_| Malformed::new("row out of range"))?;
        if self.nulls.contains(row_u32) {
            return Ok(Value::Null);
        }
        match (&self.block, self.field_type) {
            (Block::Array { offsets, child }, FieldType::Array(element)) => {
                let range = range_at(offsets, row)
                    .ok_or_else(|| Malformed::new("array offsets out of range"))?;
                range
                    .map(|index| element_value(child, element.into(), index))
                    .collect::<DecodeResult<Vec<_>>>()
                    .map(Value::Array)
            }
            (block, field_type) => element_value(block, field_type, row),
        }
    }
}

/// Check each non-null stored value of `block` that decoding leaves
/// unchecked: timestamps must be in range and JSON cells must decode.
fn check_stored(block: &Block, field_type: FieldType, nulls: &RoaringBitmap) -> DecodeResult<()> {
    let count = match (block, field_type) {
        (Block::Int(values), FieldType::Timestamp) => values.len(),
        (Block::Json(strings), _) => strings.offsets.len().saturating_sub(1),
        _ => return Ok(()),
    };
    for index in 0..count {
        if u32::try_from(index).is_ok_and(|index| nulls.contains(index)) {
            continue;
        }
        element_value(block, field_type, index)?;
    }
    Ok(())
}

fn element_value(block: &Block, field_type: FieldType, index: usize) -> DecodeResult<Value> {
    let missing = || Malformed::new(format!("value {index} is out of range"));
    match (block, field_type) {
        (Block::Int(values), FieldType::Int64) => values
            .get(index)
            .copied()
            .map(Value::Int64)
            .ok_or_else(missing),
        (Block::Int(values), FieldType::Timestamp) => {
            let micros = values.get(index).copied().ok_or_else(missing)?;
            Timestamp::from_micros(micros)
                .map(Value::Timestamp)
                .map_err(|error| Malformed::new(error.to_string()))
        }
        (Block::Float(values), _) => values
            .get(index)
            .copied()
            .map(Value::Float64)
            .ok_or_else(missing),
        (Block::Bool(trues), _) => {
            let index = u32::try_from(index).map_err(|_| missing())?;
            Ok(Value::Bool(trues.contains(index)))
        }
        (Block::Dict { dict, codes }, _) => {
            let code = codes.get(index).copied().ok_or_else(missing)?;
            dict.get_str(usize_from(code))
                .map(|value| Value::String(value.to_owned()))
        }
        (Block::Plain(strings), _) => strings
            .get_str(index)
            .map(|value| Value::String(value.to_owned())),
        (Block::Json(strings), _) => {
            let value = codec::decode(strings.get(index)?, FieldType::Json)
                .map_err(|error| Malformed::new(error.to_string()))?;
            if value.is_null() {
                Err(Malformed::new("a non-null JSON cell decodes to null"))
            } else {
                Ok(value)
            }
        }
        _ => Err(Malformed::new("column block does not match the field type")),
    }
}

fn decode_bitmap(bytes: &[u8]) -> DecodeResult<RoaringBitmap> {
    let bitmap = RoaringBitmap::deserialize_from(bytes)
        .map_err(|error| Malformed::new(format!("invalid bitmap: {error}")))?;
    if bitmap.serialized_size() != bytes.len() {
        return Err(Malformed::new(
            "bitmap length does not match its declared length",
        ));
    }
    Ok(bitmap)
}

fn check_rows(bitmap: &RoaringBitmap, row_count: usize, what: &str) -> DecodeResult<()> {
    if bitmap.max().is_some_and(|max| usize_from(max) >= row_count) {
        return Err(Malformed::new(format!(
            "{what} bitmap names a row out of range"
        )));
    }
    Ok(())
}

/// Decode one block. `top` is false for an array's child block.
fn decode_block(
    cursor: &mut Cursor<'_>,
    field_type: FieldType,
    row_count: usize,
    top: bool,
) -> DecodeResult<(RoaringBitmap, Block)> {
    let start = cursor.position();
    let code = cursor.u8()?;
    let encoding = ColumnEncoding::from_code(code)
        .ok_or_else(|| Malformed::new(format!("unknown column encoding {code}")))?;
    if !encoding.fits(field_type) || (!top && encoding == ColumnEncoding::Array) {
        return Err(Malformed::new(format!(
            "encoding {encoding:?} cannot hold {field_type}"
        )));
    }
    let width = cursor.u8()?;
    cursor.zeros(2)?;
    let rows = usize_from(cursor.u32()?);
    if rows != row_count {
        return Err(Malformed::new(format!(
            "column declares {rows} rows, expected {row_count}"
        )));
    }
    let nulls_len = usize_from_u64(cursor.u64()?)?;
    let dict_len = usize_from_u64(cursor.u64()?)?;
    let data_len = usize_from_u64(cursor.u64()?)?;
    let dict_count = usize_from(cursor.u32()?);
    cursor.zeros(COLUMN_HEADER_LEN - (cursor.position() - start))?;

    let nulls = if nulls_len == 0 {
        RoaringBitmap::new()
    } else {
        if !top {
            return Err(Malformed::new("array elements cannot be null"));
        }
        decode_bitmap(cursor.take(nulls_len)?)?
    };
    check_rows(&nulls, row_count, "null")?;
    align_from(cursor, start)?;
    let dict = cursor.take(dict_len)?;
    align_from(cursor, start)?;
    let data = cursor.take(data_len)?;
    if encoding != ColumnEncoding::StringDict && (dict_len != 0 || dict_count != 0) {
        return Err(Malformed::new("unexpected dictionary"));
    }
    let expect_width = |expected: u8| {
        if width == expected {
            Ok(())
        } else {
            Err(Malformed::new(format!(
                "value width {width}, expected {expected}"
            )))
        }
    };
    let mut data_cursor = Cursor::new(data);
    let block = match encoding {
        ColumnEncoding::Int64Plain => {
            expect_width(8)?;
            let values = data_cursor.i64s(row_count)?;
            let is_null = |row: usize| u32::try_from(row).is_ok_and(|row| nulls.contains(row));
            if values
                .iter()
                .enumerate()
                .any(|(row, value)| is_null(row) && *value != 0)
            {
                return Err(Malformed::new("null rows must store 0"));
            }
            Block::Int(values)
        }
        ColumnEncoding::Float64Plain => {
            expect_width(8)?;
            let values = data_cursor.f64s(row_count)?;
            let is_null = |row: usize| u32::try_from(row).is_ok_and(|row| nulls.contains(row));
            for (row, value) in values.iter().enumerate() {
                let canonical = canonical_float(*value)
                    .is_some_and(|canonical| canonical.to_bits() == value.to_bits());
                if !canonical || (is_null(row) && value.to_bits() != 0) {
                    return Err(Malformed::new(format!(
                        "float of row {row} is not canonical"
                    )));
                }
            }
            Block::Float(values)
        }
        ColumnEncoding::BoolBitmap => {
            expect_width(0)?;
            let trues = decode_bitmap(data_cursor.take(data_len)?)?;
            check_rows(&trues, row_count, "true")?;
            if !trues.is_disjoint(&nulls) {
                return Err(Malformed::new("a null row is marked true"));
            }
            Block::Bool(trues)
        }
        ColumnEncoding::StringDict => {
            let mut dict_cursor = Cursor::new(dict);
            let dict = Strings::decode(&mut dict_cursor, dict_count, dict_len)?;
            dict_cursor.finish()?;
            dict.check_utf8()?;
            for index in 1..dict_count {
                if dict.get(index - 1)? >= dict.get(index)? {
                    return Err(Malformed::new("dictionary is not strictly sorted"));
                }
            }
            expect_width(code_width(dict_count))?;
            let codes: Vec<u32> = match width {
                1 => data_cursor
                    .take(row_count)?
                    .iter()
                    .map(|code| u32::from(*code))
                    .collect(),
                2 => data_cursor
                    .take(super::le::checked_mul(row_count, 2)?)?
                    .chunks_exact(2)
                    .map(|chunk| u32::from(u16::from_le_bytes([chunk[0], chunk[1]])))
                    .collect(),
                4 => data_cursor.u32s(row_count)?,
                other => return Err(Malformed::new(format!("invalid code width {other}"))),
            };
            for (row, code) in codes.iter().enumerate() {
                let null = u32::try_from(row).is_ok_and(|row| nulls.contains(row));
                if (null && *code != 0) || (!null && usize_from(*code) >= dict_count) {
                    return Err(Malformed::new(format!(
                        "invalid dictionary code in row {row}"
                    )));
                }
            }
            Block::Dict { dict, codes }
        }
        ColumnEncoding::StringPlain | ColumnEncoding::JsonValue => {
            expect_width(4)?;
            let strings = Strings::decode(&mut data_cursor, row_count, data_len)?;
            check_null_ranges(&strings.offsets, &nulls)?;
            if encoding == ColumnEncoding::StringPlain {
                strings.check_utf8()?;
                Block::Plain(strings)
            } else {
                Block::Json(strings)
            }
        }
        ColumnEncoding::Array => {
            expect_width(4)?;
            let FieldType::Array(element) = field_type else {
                return Err(Malformed::new("array encoding for a non-array field"));
            };
            let offsets = data_cursor.u32s(row_count + 1)?;
            data_cursor.align(8)?;
            let elements = offsets.last().map_or(0, |last| usize_from(*last));
            let child_start = data_cursor.position();
            let (_, child) = decode_block(&mut data_cursor, element.into(), elements, false)?;
            if child_start == data_cursor.position() {
                return Err(Malformed::new("missing child block"));
            }
            check_offsets(&offsets, elements)?;
            check_null_ranges(&offsets, &nulls)?;
            Block::Array {
                offsets,
                child: Box::new(child),
            }
        }
    };
    data_cursor.finish()?;
    Ok((nulls, block))
}

/// Null rows of an offset-based block must have empty ranges.
fn check_null_ranges(offsets: &[u32], nulls: &RoaringBitmap) -> DecodeResult<()> {
    for row in nulls {
        let range = range_at(offsets, usize_from(row))
            .ok_or_else(|| Malformed::new("null row out of range"))?;
        if !range.is_empty() {
            return Err(Malformed::new(format!("null row {row} has a value")));
        }
    }
    Ok(())
}

/// Skip zero padding to the next multiple of 8 from `start`.
fn align_from(cursor: &mut Cursor<'_>, start: usize) -> DecodeResult<()> {
    let len = cursor.position() - start;
    let pad = padded(len, 8) - len;
    cursor.zeros(pad)
}
