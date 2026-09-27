//! WAL v2 payload types and their postcard codec.
//!
//! A WAL v2 frame carries one postcard-encoded [`WalPayload`]. Operations are
//! logged as blind writes: a [`RowOp::Put`] carries the complete row image
//! and a [`RowOp::Delete`] carries only the key, so replay never evaluates
//! predicates or reads old rows.
//!
//! Postcard is compact and deterministic but not self-describing, so it
//! cannot decode anything that needs `deserialize_any`. The WAL therefore
//! never postcard-encodes `PrimaryKey` (untagged for JSON), `Value`, or
//! `Record` (both hold `serde_json::Value`). It uses [`WirePk`] for keys,
//! [`ValueBytes`] (the binary value codec in
//! [`logpose_types::value::codec`]) for values, and [`F32Bytes`] for
//! vectors. [`CollectionSchema`] is plain structs and externally tagged
//! enums, so it is postcard-safe; a round-trip test keeps it that way.
//!
//! Rows are keyed by [`FieldId`], never by name. Converting a name-keyed
//! [`Record`] into a [`RowImage`] validates it against the schema first,
//! which is the only place names become ids on the write path.

use logpose_types::{
    DistanceMetric, SeqNo,
    record::{PrimaryKey, Record, RecordError},
    schema::{CollectionSchema, DYNAMIC_FIELD_NAME, FieldId, FieldRef, FieldType, PrimaryKeyType},
    value::{
        Value,
        codec::{self, CodecError},
    },
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess, Visitor},
};
use serde_json::Value as JsonValue;
use std::fmt;
use thiserror::Error;

/// Frame payload. The variant must match the frame's `frame_type`; see
/// [`PayloadKind`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WalPayload {
    /// A batch of row operations; one sequence number per operation.
    WriteBatch(WriteBatchPayload),
    /// A complete new schema; consumes one sequence number.
    SchemaChange(SchemaChangePayload),
    /// A checkpoint marker; consumes no sequence number.
    Checkpoint(CheckpointPayload),
}

/// Which kind of payload a frame holds, and its `frame_type` byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PayloadKind {
    /// [`WalPayload::WriteBatch`], frame type 1.
    WriteBatch,
    /// [`WalPayload::SchemaChange`], frame type 2.
    SchemaChange,
    /// [`WalPayload::Checkpoint`], frame type 3.
    Checkpoint,
}

impl PayloadKind {
    /// The frame header's `frame_type` byte for this kind.
    #[must_use]
    pub fn frame_type(self) -> u8 {
        match self {
            Self::WriteBatch => 1,
            Self::SchemaChange => 2,
            Self::Checkpoint => 3,
        }
    }

    /// The kind for a frame header's `frame_type` byte.
    #[must_use]
    pub fn from_frame_type(frame_type: u8) -> Option<Self> {
        match frame_type {
            1 => Some(Self::WriteBatch),
            2 => Some(Self::SchemaChange),
            3 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

impl fmt::Display for PayloadKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WriteBatch => "write batch",
            Self::SchemaChange => "schema change",
            Self::Checkpoint => "checkpoint",
        })
    }
}

/// Row operations validated against one schema version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriteBatchPayload {
    /// `CollectionSchema::schema_version` the rows were validated against.
    pub schema_version: u64,
    /// One sequence number each, in order, starting at the frame's
    /// `first_seq_no`.
    pub ops: Vec<RowOp>,
}

/// One logged operation. Always a blind write.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RowOp {
    /// Write a complete row image. Upserts, partial updates, and
    /// update-by-filter all become `Put` with the merged full row.
    Put(RowImage),
    /// Delete by key. Delete-by-filter becomes many `Delete`s.
    Delete(WirePk),
}

/// Externally tagged mirror of [`PrimaryKey`], which is untagged for JSON
/// and so cannot be decoded by postcard.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum WirePk {
    /// A signed 64-bit integer key.
    Int64(i64),
    /// A string key.
    String(String),
}

impl WirePk {
    /// The key's type.
    #[must_use]
    pub fn key_type(&self) -> PrimaryKeyType {
        match self {
            Self::Int64(_) => PrimaryKeyType::Int64,
            Self::String(_) => PrimaryKeyType::String,
        }
    }
}

impl From<PrimaryKey> for WirePk {
    fn from(pk: PrimaryKey) -> Self {
        match pk {
            PrimaryKey::Int64(value) => Self::Int64(value),
            PrimaryKey::String(value) => Self::String(value),
        }
    }
}

impl From<WirePk> for PrimaryKey {
    fn from(pk: WirePk) -> Self {
        match pk {
            WirePk::Int64(value) => Self::Int64(value),
            WirePk::String(value) => Self::String(value),
        }
    }
}

/// A row normalized to a schema. Keyed by [`FieldId`], never by name.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RowImage {
    /// The primary key.
    pub pk: WirePk,
    /// Sparse `(FieldId, vector)` pairs sorted by `FieldId`; absent means
    /// null. Cosine vectors are already normalized to unit length.
    pub vectors: Vec<(FieldId, F32Bytes)>,
    /// Sparse `(FieldId, value)` pairs sorted by `FieldId`; absent means
    /// null, and null is never stored. Each value is in the binary value
    /// codec.
    pub scalars: Vec<(FieldId, ValueBytes)>,
    /// Undeclared keys (`$extra`): one JSON object node in the binary value
    /// codec, keys sorted, with no key that the schema the row was
    /// validated against declares or retires. `None` when there are no keys.
    pub dynamic: Option<ValueBytes>,
}

/// The complete new schema after an online change.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchemaChangePayload {
    /// The new schema. Its `schema_version` is the previous one plus 1.
    /// Dropped fields are absent; `next_field_id` guarantees that their ids
    /// are never reassigned.
    pub schema: CollectionSchema,
}

/// Checkpoint marker, written as the first frame of every WAL file and
/// after every flush commit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointPayload {
    /// Generation of the durable manifest.
    pub manifest_generation: u64,
    /// Every operation at or below this sequence number is in segments.
    pub checkpoint_seq_no: SeqNo,
}

/// Reasons a payload cannot be encoded or decoded.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum PayloadError {
    /// Postcard failed to encode the payload.
    #[error("failed to encode WAL payload: {0}")]
    Encode(String),
    /// The bytes are not a valid postcard payload.
    #[error("failed to decode WAL payload: {0}")]
    Decode(String),
    /// Bytes remain after one complete payload.
    #[error("{count} trailing bytes after the WAL payload")]
    TrailingBytes {
        /// Number of unread bytes.
        count: usize,
    },
    /// The payload variant does not match the frame type.
    #[error("frame declares a {expected} payload but holds a {found} payload")]
    KindMismatch {
        /// The kind the frame type declares.
        expected: PayloadKind,
        /// The kind that was decoded.
        found: PayloadKind,
    },
}

impl WalPayload {
    /// The kind of this payload.
    #[must_use]
    pub fn kind(&self) -> PayloadKind {
        match self {
            Self::WriteBatch(_) => PayloadKind::WriteBatch,
            Self::SchemaChange(_) => PayloadKind::SchemaChange,
            Self::Checkpoint(_) => PayloadKind::Checkpoint,
        }
    }

    /// Encode with postcard.
    ///
    /// # Errors
    ///
    /// Returns [`PayloadError::Encode`] if postcard fails, which only an
    /// allocation failure or a serializer bug can cause.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        postcard::to_allocvec(self).map_err(|error| PayloadError::Encode(error.to_string()))
    }

    /// Decode a payload that spans all of `bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`PayloadError::Decode`] for malformed bytes (including a
    /// stored schema that fails validation) or
    /// [`PayloadError::TrailingBytes`].
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        let (payload, rest) = postcard::take_from_bytes::<Self>(bytes)
            .map_err(|error| PayloadError::Decode(error.to_string()))?;
        if rest.is_empty() {
            Ok(payload)
        } else {
            Err(PayloadError::TrailingBytes { count: rest.len() })
        }
    }

    /// Decode a payload and check that it has the kind the frame declares.
    ///
    /// # Errors
    ///
    /// As [`decode`](Self::decode), plus [`PayloadError::KindMismatch`].
    pub fn decode_as(bytes: &[u8], expected: PayloadKind) -> Result<Self, PayloadError> {
        let payload = Self::decode(bytes)?;
        let found = payload.kind();
        if found == expected {
            Ok(payload)
        } else {
            Err(PayloadError::KindMismatch { expected, found })
        }
    }
}

/// A vector as length-prefixed little-endian `f32` bytes.
///
/// Serialized as one byte string, so a 768-dimension vector is one
/// 3,072-byte copy rather than 768 separately encoded floats.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct F32Bytes(Vec<u8>);

impl F32Bytes {
    /// Encode components as little-endian bytes.
    #[must_use]
    pub fn from_f32s(components: &[f32]) -> Self {
        Self(
            components
                .iter()
                .flat_map(|component| component.to_le_bytes())
                .collect(),
        )
    }

    /// Wrap little-endian bytes, which must be a whole number of `f32`s.
    /// Returns `None` otherwise.
    #[must_use]
    pub fn from_le_bytes(bytes: Vec<u8>) -> Option<Self> {
        bytes.len().is_multiple_of(4).then_some(Self(bytes))
    }

    /// The raw little-endian bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Number of components.
    #[must_use]
    pub fn dimensions(&self) -> usize {
        self.0.len() / 4
    }

    /// Iterate over the components.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = f32> + '_ {
        self.0.chunks_exact(4).map(|chunk| {
            let mut bytes = [0_u8; 4];
            bytes.copy_from_slice(chunk);
            f32::from_le_bytes(bytes)
        })
    }

    /// Decode the components.
    #[must_use]
    pub fn to_f32s(&self) -> Vec<f32> {
        self.iter().collect()
    }
}

impl Serialize for F32Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for F32Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = deserializer.deserialize_bytes(ByteBufVisitor)?;
        let len = bytes.len();
        Self::from_le_bytes(bytes)
            .ok_or_else(|| de::Error::invalid_length(len, &"a multiple of 4 bytes"))
    }
}

/// One value, or one JSON node, in the binary value codec.
///
/// The bytes are checked against a field type when decoded, not when
/// deserialized, because the type comes from the reading schema.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ValueBytes(Vec<u8>);

impl ValueBytes {
    /// Encode a value.
    ///
    /// # Errors
    ///
    /// As [`codec::encode`].
    pub fn encode(value: &Value) -> Result<Self, CodecError> {
        codec::encode(value).map(Self)
    }

    /// Encode a bare JSON node, such as a `$extra` object.
    ///
    /// # Errors
    ///
    /// As [`codec::encode_json`].
    pub fn encode_json(json: &JsonValue) -> Result<Self, CodecError> {
        codec::encode_json(json).map(Self)
    }

    /// Wrap already encoded bytes. They are checked when decoded.
    #[must_use]
    pub fn from_encoded(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The encoded bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Decode as a value of `field_type`.
    ///
    /// # Errors
    ///
    /// As [`codec::decode`].
    pub fn decode(&self, field_type: FieldType) -> Result<Value, CodecError> {
        codec::decode(&self.0, field_type)
    }

    /// Decode as a bare JSON node.
    ///
    /// # Errors
    ///
    /// As [`codec::decode_json`].
    pub fn decode_json(&self) -> Result<JsonValue, CodecError> {
        codec::decode_json(&self.0)
    }
}

impl Serialize for ValueBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for ValueBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(ByteBufVisitor).map(Self)
    }
}

/// Accepts a byte string in any of the forms a deserializer may offer.
struct ByteBufVisitor;

impl<'de> Visitor<'de> for ByteBufVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a byte string")
    }

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
        Ok(bytes.to_vec())
    }

    fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
        Ok(bytes)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
        while let Some(byte) = seq.next_element()? {
            bytes.push(byte);
        }
        Ok(bytes)
    }
}

/// Reasons a record cannot become a row image, or a row image cannot be
/// read back under a schema.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum RowImageError {
    /// The record does not fit the schema.
    #[error(transparent)]
    InvalidRecord(#[from] RecordError),
    /// A cosine vector has zero length, so it cannot be normalized.
    #[error("vector field '{field}' is all zeros and cannot be normalized for cosine distance")]
    ZeroNormVector {
        /// The vector field name.
        field: String,
    },
    /// A value could not be encoded.
    #[error("field '{field}' could not be encoded: {source}")]
    Encode {
        /// The field name, or `$extra`.
        field: String,
        /// Why encoding failed.
        source: CodecError,
    },
    /// A stored value does not decode as its field's type.
    #[error("field {field} could not be decoded: {source}")]
    Decode {
        /// The field id.
        field: FieldId,
        /// Why decoding failed.
        source: CodecError,
    },
    /// The stored `$extra` bytes do not decode to a JSON object.
    #[error("dynamic fields could not be decoded: {0}")]
    Dynamic(String),
    /// Field ids are not strictly increasing, so the row is corrupt.
    #[error("field {field} is out of order or repeated in the row image")]
    UnsortedFieldIds {
        /// The offending field id.
        field: FieldId,
    },
    /// The stored key does not have the schema's key type.
    #[error("row key has type {found}, but the schema's key type is {expected}")]
    PrimaryKeyType {
        /// The schema's key type.
        expected: PrimaryKeyType,
        /// The stored key's type.
        found: PrimaryKeyType,
    },
    /// A stored vector does not have the declared dimensions.
    #[error("vector field {field} has {actual} dimensions; the schema declares {expected}")]
    VectorDimensions {
        /// The field id.
        field: FieldId,
        /// Declared dimensions.
        expected: u32,
        /// Stored dimensions.
        actual: usize,
    },
}

impl RowImage {
    /// Validate a record against `schema` and convert it to a row image
    /// keyed by [`FieldId`].
    ///
    /// Cosine vectors are normalized to unit length (in `f64`, then rounded
    /// to `f32`); an all-zero cosine vector is rejected. Scalar values and
    /// `$extra` are encoded with the binary value codec.
    ///
    /// # Errors
    ///
    /// Returns [`RowImageError::InvalidRecord`] when validation fails,
    /// [`RowImageError::ZeroNormVector`], or [`RowImageError::Encode`].
    pub fn from_record(schema: &CollectionSchema, record: Record) -> Result<Self, RowImageError> {
        let mut record = schema.validate_record(record)?;
        let mut vectors = Vec::with_capacity(schema.vectors().len());
        for field in schema.vectors() {
            let Some(mut vector) = record.vectors.remove(&field.name) else {
                continue;
            };
            if field.metric == DistanceMetric::Cosine && !normalize(&mut vector) {
                return Err(RowImageError::ZeroNormVector {
                    field: field.name.clone(),
                });
            }
            vectors.push((field.id, F32Bytes::from_f32s(&vector)));
        }
        vectors.sort_unstable_by_key(|(id, _)| *id);

        let mut scalars = Vec::with_capacity(record.fields.len());
        for (name, value) in &record.fields {
            // Validation keeps only declared scalar fields.
            let Some(field) = schema.scalar_field(name) else {
                continue;
            };
            let bytes = ValueBytes::encode(value).map_err(|source| RowImageError::Encode {
                field: name.clone(),
                source,
            })?;
            scalars.push((field.id, bytes));
        }
        scalars.sort_unstable_by_key(|(id, _)| *id);

        let dynamic = if record.extra.is_empty() {
            None
        } else {
            let object = JsonValue::Object(std::mem::take(&mut record.extra));
            Some(
                ValueBytes::encode_json(&object).map_err(|source| RowImageError::Encode {
                    field: DYNAMIC_FIELD_NAME.to_owned(),
                    source,
                })?,
            )
        };

        Ok(Self {
            pk: record.pk.into(),
            vectors,
            scalars,
            dynamic,
        })
    }

    /// Read the row back as a name-keyed record under `schema`, which may
    /// be newer than the schema the row was written with.
    ///
    /// Values whose [`FieldId`] the schema does not declare (dropped fields)
    /// are skipped, and `$extra` keys that the schema declares or retires
    /// are hidden, following the dynamic field shadowing rule. Fields added
    /// after the row was written read as absent (null).
    ///
    /// # Errors
    ///
    /// Returns a [`RowImageError`] when the row is corrupt or does not match
    /// the schema's key type or vector dimensions.
    pub fn to_record(&self, schema: &CollectionSchema) -> Result<Record, RowImageError> {
        let expected = schema.primary_key_type();
        let found = self.pk.key_type();
        if expected != found {
            return Err(RowImageError::PrimaryKeyType { expected, found });
        }
        let mut record = Record::new(PrimaryKey::from(self.pk.clone()));

        check_sorted(self.vectors.iter().map(|(id, _)| *id))?;
        for (id, vector) in &self.vectors {
            let Some(FieldRef::Vector(field)) = schema.field_by_id(*id) else {
                continue;
            };
            let matches = usize::try_from(field.dimensions)
                .is_ok_and(|dimensions| dimensions == vector.dimensions());
            if !matches {
                return Err(RowImageError::VectorDimensions {
                    field: *id,
                    expected: field.dimensions,
                    actual: vector.dimensions(),
                });
            }
            record.vectors.insert(field.name.clone(), vector.to_f32s());
        }

        check_sorted(self.scalars.iter().map(|(id, _)| *id))?;
        for (id, bytes) in &self.scalars {
            let Some(FieldRef::Scalar(field)) = schema.field_by_id(*id) else {
                continue;
            };
            let value = bytes
                .decode(field.field_type)
                .map_err(|source| RowImageError::Decode { field: *id, source })?;
            if !value.is_null() {
                record.fields.insert(field.name.clone(), value);
            }
        }

        if let Some(dynamic) = &self.dynamic {
            match dynamic.decode_json() {
                Ok(JsonValue::Object(mut extra)) => {
                    schema.retain_visible_dynamic(&mut extra);
                    record.extra = extra;
                }
                Ok(other) => {
                    return Err(RowImageError::Dynamic(format!(
                        "expected an object, found {other}"
                    )));
                }
                Err(error) => return Err(RowImageError::Dynamic(error.to_string())),
            }
        }
        Ok(record)
    }
}

fn check_sorted(ids: impl Iterator<Item = FieldId>) -> Result<(), RowImageError> {
    let mut previous: Option<FieldId> = None;
    for id in ids {
        if previous.is_some_and(|previous| previous >= id) {
            return Err(RowImageError::UnsortedFieldIds { field: id });
        }
        previous = Some(id);
    }
    Ok(())
}

/// Scale a vector to unit length. Returns `false` for an all-zero vector.
fn normalize(vector: &mut [f32]) -> bool {
    let norm = vector
        .iter()
        .map(|component| f64::from(*component) * f64::from(*component))
        .sum::<f64>()
        .sqrt();
    if norm == 0.0 || !norm.is_finite() {
        return false;
    }
    for component in vector.iter_mut() {
        *component = (f64::from(*component) / norm) as f32;
    }
    true
}

#[cfg(test)]
mod tests;
