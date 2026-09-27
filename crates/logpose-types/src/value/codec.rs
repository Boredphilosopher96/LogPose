//! The binary value codec: one deterministic byte encoding for [`Value`] and
//! JSON.
//!
//! It is used wherever the engine stores a typed value or a JSON document as
//! bytes: WAL row images, memtable `json` and `$extra` cells, segment JSON
//! columns, and dynamic-field blocks. Varints are unsigned LEB128; signed
//! integers are zigzag varints. Every value starts with a one-byte tag:
//!
//! ```text
//! tag  Value            payload
//! 0x00 Null             none
//! 0x01 Bool false       none
//! 0x02 Bool true        none
//! 0x03 Int64            zigzag varint
//! 0x04 Float64          8 bytes LE (finite; -0.0 folded to 0.0)
//! 0x05 String           varint byte length, UTF-8
//! 0x06 Timestamp        zigzag varint microseconds
//! 0x07 Array            varint count, then that many encoded Values
//! 0x08 Json             one JSON node
//!
//! tag  JSON node        payload
//! 0x10 null             none
//! 0x11 false            none
//! 0x12 true             none
//! 0x13 integer (i64)    zigzag varint
//! 0x14 integer (u64)    varint, only for values above i64::MAX
//! 0x15 float            8 bytes LE (finite; -0.0 folded to 0.0)
//! 0x16 string           varint byte length, UTF-8
//! 0x17 array            varint count, then nodes
//! 0x18 object           varint count, then (varint key length, key UTF-8, node),
//!                       keys in strictly increasing byte order
//! ```
//!
//! The encoding is canonical: equal values always encode to equal bytes
//! (object keys sorted, one integer form per number, one zero, and a JSON
//! `null` in a `json` field encoded as [`Value::Null`], as
//! [`Value::conform`] canonicalizes it). Decoding is strict and accepts only
//! canonical bytes, so `encode(decode(bytes)) == bytes` for every accepted
//! input and a corrupt cell is a typed [`CodecError`], never a wrong value.
//! [`decode`] also checks that every tag fits the declared [`FieldType`].
//!
//! Nesting is limited to [`MAX_NESTING_DEPTH`] levels on both sides, so any
//! encodable value decodes without unbounded recursion.

use super::{Timestamp, Value};
use crate::schema::{ElementType, FieldType};
use serde_json::{Map, Number, Value as JsonValue};
use thiserror::Error;

/// Deepest allowed nesting of arrays and JSON containers. A scalar at the
/// top level is at depth 0; each enclosing array or object adds one.
pub const MAX_NESTING_DEPTH: usize = 128;

const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_INT64: u8 = 0x03;
const TAG_FLOAT64: u8 = 0x04;
const TAG_STRING: u8 = 0x05;
const TAG_TIMESTAMP: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_JSON: u8 = 0x08;

const JSON_NULL: u8 = 0x10;
const JSON_FALSE: u8 = 0x11;
const JSON_TRUE: u8 = 0x12;
const JSON_I64: u8 = 0x13;
const JSON_U64: u8 = 0x14;
const JSON_FLOAT: u8 = 0x15;
const JSON_STRING: u8 = 0x16;
const JSON_ARRAY: u8 = 0x17;
const JSON_OBJECT: u8 = 0x18;

/// Longest LEB128 encoding of a `u64`.
const MAX_VARINT_BYTES: usize = 10;

/// Most array elements reserved up front from a declared count. A count is
/// only bounded by the remaining input, and every nesting level reserves
/// its own buffer before its first element is read, so without this cap
/// 128 nested arrays that each declare about `n` elements would reserve
/// about `128 * 32 * n` bytes from an `n`-byte input. Larger arrays grow as
/// their elements are actually decoded.
const MAX_PREALLOCATED_ITEMS: usize = 1024;

/// Reasons a value cannot be encoded, or bytes cannot be decoded.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum CodecError {
    /// A float to encode is NaN or infinite.
    #[error("float values must be finite to be encoded")]
    NonFiniteFloat,
    /// A value nests arrays or JSON containers too deeply.
    #[error("value nests deeper than {max} levels")]
    TooDeep {
        /// The nesting limit.
        max: usize,
    },
    /// The input ended inside a value.
    #[error("unexpected end of input at byte {offset}")]
    UnexpectedEof {
        /// Offset where more bytes were needed.
        offset: usize,
    },
    /// A byte is not a known tag in its position.
    #[error("unknown tag 0x{tag:02x} at byte {offset}")]
    UnknownTag {
        /// The tag byte.
        tag: u8,
        /// Offset of the tag.
        offset: usize,
    },
    /// A tag does not fit the declared field type.
    #[error("tag 0x{tag:02x} at byte {offset} is not a valid {expected} value")]
    TypeMismatch {
        /// The tag byte.
        tag: u8,
        /// Offset of the tag.
        offset: usize,
        /// The type the value was decoded as.
        expected: FieldType,
    },
    /// A varint is longer than ten bytes, overflows `u64`, or is not in its
    /// shortest form.
    #[error("malformed or non-minimal varint at byte {offset}")]
    InvalidVarint {
        /// Offset of the varint.
        offset: usize,
    },
    /// A length or count does not fit in the remaining input.
    #[error("length {len} at byte {offset} exceeds the remaining input")]
    LengthOutOfBounds {
        /// The declared length or count.
        len: u64,
        /// Offset of the length.
        offset: usize,
    },
    /// A string is not valid UTF-8.
    #[error("string at byte {offset} is not valid UTF-8")]
    InvalidUtf8 {
        /// Offset of the string bytes.
        offset: usize,
    },
    /// A value is well formed but not in canonical form: a NaN, infinite, or
    /// negative-zero float, a `u64` integer that fits `i64`, or a JSON
    /// `null` stored as a `json` value instead of as null.
    #[error("non-canonical value at byte {offset}")]
    NonCanonical {
        /// Offset of the value.
        offset: usize,
    },
    /// Object keys are not in strictly increasing byte order, which also
    /// rules out duplicates.
    #[error("object key at byte {offset} is not greater than the previous key")]
    UnsortedKeys {
        /// Offset of the key.
        offset: usize,
    },
    /// An array element is null.
    #[error("array element at byte {offset} is null")]
    NullArrayElement {
        /// Offset of the element.
        offset: usize,
    },
    /// A timestamp is outside years 0000 to 9999.
    #[error("timestamp at byte {offset} is out of range")]
    TimestampOutOfRange {
        /// Offset of the timestamp.
        offset: usize,
    },
    /// Bytes remain after one complete value.
    #[error("{count} trailing bytes after the value")]
    TrailingBytes {
        /// Number of unread bytes.
        count: usize,
    },
}

/// Encode a value.
///
/// # Errors
///
/// Returns [`CodecError::NonFiniteFloat`] for a NaN or infinite float, which
/// only direct construction can produce, or [`CodecError::TooDeep`].
pub fn encode(value: &Value) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    encode_into(value, &mut out)?;
    Ok(out)
}

/// Append the encoding of a value to `out`. On error, `out` is restored to
/// its original length.
///
/// # Errors
///
/// As [`encode`].
pub fn encode_into(value: &Value, out: &mut Vec<u8>) -> Result<(), CodecError> {
    let start = out.len();
    let result = write_value(value, out, 0);
    if result.is_err() {
        out.truncate(start);
    }
    result
}

/// Encode a bare JSON node, such as a row's `$extra` object.
///
/// # Errors
///
/// Returns [`CodecError::TooDeep`], or [`CodecError::NonFiniteFloat`] for a
/// number that has no finite `f64` form.
pub fn encode_json(json: &JsonValue) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    encode_json_into(json, &mut out)?;
    Ok(out)
}

/// Append the encoding of a bare JSON node to `out`. On error, `out` is
/// restored to its original length.
///
/// # Errors
///
/// As [`encode_json`].
pub fn encode_json_into(json: &JsonValue, out: &mut Vec<u8>) -> Result<(), CodecError> {
    let start = out.len();
    let result = write_json(json, out, 0);
    if result.is_err() {
        out.truncate(start);
    }
    result
}

/// Decode one value of `field_type` that spans all of `bytes`.
///
/// [`Value::Null`] fits every type. Arrays must hold non-null elements of
/// the declared element type.
///
/// # Errors
///
/// Returns a [`CodecError`] when the bytes are malformed, not canonical, do
/// not fit `field_type`, or have trailing bytes.
pub fn decode(bytes: &[u8], field_type: FieldType) -> Result<Value, CodecError> {
    let mut reader = Reader::new(bytes);
    let value = reader.value(field_type, 0)?;
    reader.finish()?;
    Ok(value)
}

/// Decode one bare JSON node that spans all of `bytes`.
///
/// # Errors
///
/// Returns a [`CodecError`] when the bytes are malformed, not canonical, or
/// have trailing bytes.
pub fn decode_json(bytes: &[u8]) -> Result<JsonValue, CodecError> {
    let mut reader = Reader::new(bytes);
    let json = reader.json(0)?;
    reader.finish()?;
    Ok(json)
}

fn check_depth(depth: usize) -> Result<(), CodecError> {
    if depth > MAX_NESTING_DEPTH {
        Err(CodecError::TooDeep {
            max: MAX_NESTING_DEPTH,
        })
    } else {
        Ok(())
    }
}

fn write_value(value: &Value, out: &mut Vec<u8>, depth: usize) -> Result<(), CodecError> {
    check_depth(depth)?;
    match value {
        Value::Null | Value::Json(JsonValue::Null) => out.push(TAG_NULL),
        Value::Bool(false) => out.push(TAG_FALSE),
        Value::Bool(true) => out.push(TAG_TRUE),
        Value::Int64(value) => {
            out.push(TAG_INT64);
            write_varint(zigzag(*value), out);
        }
        Value::Float64(value) => {
            out.push(TAG_FLOAT64);
            write_float(*value, out)?;
        }
        Value::String(value) => {
            out.push(TAG_STRING);
            write_str(value, out);
        }
        Value::Timestamp(value) => {
            out.push(TAG_TIMESTAMP);
            write_varint(zigzag(value.as_micros()), out);
        }
        Value::Array(items) => {
            out.push(TAG_ARRAY);
            write_len(items.len(), out);
            for item in items {
                write_value(item, out, depth + 1)?;
            }
        }
        Value::Json(json) => {
            out.push(TAG_JSON);
            write_json(json, out, depth)?;
        }
    }
    Ok(())
}

fn write_json(json: &JsonValue, out: &mut Vec<u8>, depth: usize) -> Result<(), CodecError> {
    check_depth(depth)?;
    match json {
        JsonValue::Null => out.push(JSON_NULL),
        JsonValue::Bool(false) => out.push(JSON_FALSE),
        JsonValue::Bool(true) => out.push(JSON_TRUE),
        JsonValue::Number(number) => write_number(number, out)?,
        JsonValue::String(value) => {
            out.push(JSON_STRING);
            write_str(value, out);
        }
        JsonValue::Array(items) => {
            out.push(JSON_ARRAY);
            write_len(items.len(), out);
            for item in items {
                write_json(item, out, depth + 1)?;
            }
        }
        JsonValue::Object(object) => {
            out.push(JSON_OBJECT);
            write_len(object.len(), out);
            // `serde_json::Map` is sorted unless the `preserve_order` feature
            // is on somewhere in the build, so sort explicitly.
            let mut entries: Vec<(&String, &JsonValue)> = object.iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                write_str(key, out);
                write_json(value, out, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn write_number(number: &Number, out: &mut Vec<u8>) -> Result<(), CodecError> {
    if let Some(value) = number.as_i64() {
        out.push(JSON_I64);
        write_varint(zigzag(value), out);
    } else if let Some(value) = number.as_u64() {
        out.push(JSON_U64);
        write_varint(value, out);
    } else {
        out.push(JSON_FLOAT);
        write_float(number.as_f64().ok_or(CodecError::NonFiniteFloat)?, out)?;
    }
    Ok(())
}

fn write_float(value: f64, out: &mut Vec<u8>) -> Result<(), CodecError> {
    if !value.is_finite() {
        return Err(CodecError::NonFiniteFloat);
    }
    let canonical = if value == 0.0 { 0.0_f64 } else { value };
    out.extend_from_slice(&canonical.to_le_bytes());
    Ok(())
}

fn write_str(value: &str, out: &mut Vec<u8>) {
    write_len(value.len(), out);
    out.extend_from_slice(value.as_bytes());
}

fn write_len(len: usize, out: &mut Vec<u8>) {
    // `usize` is at most 64 bits on every supported target.
    write_varint(len as u64, out);
}

fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Strict cursor over encoded bytes.
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn finish(&self) -> Result<(), CodecError> {
        match self.remaining() {
            0 => Ok(()),
            count => Err(CodecError::TrailingBytes { count }),
        }
    }

    fn byte(&mut self) -> Result<u8, CodecError> {
        let byte = *self
            .bytes
            .get(self.pos)
            .ok_or(CodecError::UnexpectedEof { offset: self.pos })?;
        self.pos += 1;
        Ok(byte)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(CodecError::UnexpectedEof {
                offset: self.bytes.len(),
            })?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn varint(&mut self) -> Result<u64, CodecError> {
        let offset = self.pos;
        let mut value = 0_u64;
        for index in 0..MAX_VARINT_BYTES {
            let byte = self.byte()?;
            let bits = u64::from(byte & 0x7f);
            // The tenth byte may only carry the top bit of a u64.
            if index == MAX_VARINT_BYTES - 1 && bits > 1 {
                return Err(CodecError::InvalidVarint { offset });
            }
            value |= bits << (7 * index);
            if byte & 0x80 == 0 {
                // A zero final byte after the first means a longer-than-needed form.
                if index > 0 && byte == 0 {
                    return Err(CodecError::InvalidVarint { offset });
                }
                return Ok(value);
            }
        }
        Err(CodecError::InvalidVarint { offset })
    }

    /// A length or element count. Every element takes at least one byte, so
    /// a count above the remaining input is corrupt, and checking it here
    /// bounds every allocation by the input size.
    fn len(&mut self) -> Result<usize, CodecError> {
        let offset = self.pos;
        let len = self.varint()?;
        usize::try_from(len)
            .ok()
            .filter(|len| *len <= self.remaining())
            .ok_or(CodecError::LengthOutOfBounds { len, offset })
    }

    fn string(&mut self) -> Result<String, CodecError> {
        let len = self.len()?;
        let offset = self.pos;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| CodecError::InvalidUtf8 { offset })
    }

    fn float(&mut self) -> Result<f64, CodecError> {
        let offset = self.pos;
        let bytes = self.take(8)?;
        let mut array = [0_u8; 8];
        array.copy_from_slice(bytes);
        let value = f64::from_le_bytes(array);
        // Rejects NaN, infinities, and -0.0 (whose sign bit is set).
        if value.is_finite() && !(value == 0.0 && value.is_sign_negative()) {
            Ok(value)
        } else {
            Err(CodecError::NonCanonical { offset })
        }
    }

    fn value(&mut self, field_type: FieldType, depth: usize) -> Result<Value, CodecError> {
        check_depth(depth)?;
        let offset = self.pos;
        let tag = self.byte()?;
        let mismatch = || CodecError::TypeMismatch {
            tag,
            offset,
            expected: field_type,
        };
        match (tag, field_type) {
            (TAG_NULL, _) => Ok(Value::Null),
            (TAG_FALSE, FieldType::Bool) => Ok(Value::Bool(false)),
            (TAG_TRUE, FieldType::Bool) => Ok(Value::Bool(true)),
            (TAG_INT64, FieldType::Int64) => Ok(Value::Int64(unzigzag(self.varint()?))),
            (TAG_FLOAT64, FieldType::Float64) => Ok(Value::Float64(self.float()?)),
            (TAG_STRING, FieldType::String) => Ok(Value::String(self.string()?)),
            (TAG_TIMESTAMP, FieldType::Timestamp) => {
                let micros = unzigzag(self.varint()?);
                Timestamp::from_micros(micros)
                    .map(Value::Timestamp)
                    .map_err(|_| CodecError::TimestampOutOfRange { offset })
            }
            (TAG_ARRAY, FieldType::Array(element)) => self.array(element, depth),
            (TAG_JSON, FieldType::Json) => match self.json(depth)? {
                JsonValue::Null => Err(CodecError::NonCanonical { offset }),
                json => Ok(Value::Json(json)),
            },
            (TAG_NULL..=TAG_JSON, _) => Err(mismatch()),
            _ => Err(CodecError::UnknownTag { tag, offset }),
        }
    }

    fn array(&mut self, element: ElementType, depth: usize) -> Result<Value, CodecError> {
        let count = self.len()?;
        let mut items = Vec::with_capacity(count.min(MAX_PREALLOCATED_ITEMS));
        for _ in 0..count {
            let offset = self.pos;
            let item = self.value(element.into(), depth + 1)?;
            if item.is_null() {
                return Err(CodecError::NullArrayElement { offset });
            }
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn json(&mut self, depth: usize) -> Result<JsonValue, CodecError> {
        check_depth(depth)?;
        let offset = self.pos;
        let tag = self.byte()?;
        match tag {
            JSON_NULL => Ok(JsonValue::Null),
            JSON_FALSE => Ok(JsonValue::Bool(false)),
            JSON_TRUE => Ok(JsonValue::Bool(true)),
            JSON_I64 => Ok(JsonValue::from(unzigzag(self.varint()?))),
            JSON_U64 => {
                let value = self.varint()?;
                if i64::try_from(value).is_ok() {
                    Err(CodecError::NonCanonical { offset })
                } else {
                    Ok(JsonValue::from(value))
                }
            }
            JSON_FLOAT => {
                let value = self.float()?;
                Number::from_f64(value)
                    .map(JsonValue::Number)
                    .ok_or(CodecError::NonCanonical { offset })
            }
            JSON_STRING => Ok(JsonValue::String(self.string()?)),
            JSON_ARRAY => {
                let count = self.len()?;
                let mut items = Vec::with_capacity(count.min(MAX_PREALLOCATED_ITEMS));
                for _ in 0..count {
                    items.push(self.json(depth + 1)?);
                }
                Ok(JsonValue::Array(items))
            }
            JSON_OBJECT => {
                let count = self.len()?;
                let mut object = Map::new();
                for _ in 0..count {
                    let key_offset = self.pos;
                    let key = self.string()?;
                    // Keys arrive in increasing order, so the last key in the
                    // map is the previous one whether or not the map sorts.
                    if object.keys().next_back().is_some_and(|last| *last >= key) {
                        return Err(CodecError::UnsortedKeys { offset: key_offset });
                    }
                    let value = self.json(depth + 1)?;
                    object.insert(key, value);
                }
                Ok(JsonValue::Object(object))
            }
            _ => Err(CodecError::UnknownTag { tag, offset }),
        }
    }
}

#[cfg(test)]
mod tests;
