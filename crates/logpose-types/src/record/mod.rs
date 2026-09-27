//! Records and partial updates validated against a collection schema.
//!
//! A user document is a flat JSON object keyed by field name:
//!
//! ```json
//! { "sku": "A-1", "embedding": [0.1, 0.2], "price": 12.5, "color": "red" }
//! ```
//!
//! [`Record::from_json`] splits it into the primary key, vectors, typed
//! scalar fields, and `extra`, the dynamic field that keeps undeclared keys
//! when the schema enables dynamic fields. [`Record::to_json`] flattens a
//! record back into the same shape.
//!
//! In a validated [`Record`], absent and null scalar fields are the same
//! thing, so `fields` never holds [`Value::Null`]. In a [`PartialUpdate`],
//! a null means "set this field to null" and an absent key means "leave it
//! unchanged".

mod error;
mod parse;
mod validate;

pub use error::RecordError;

use crate::{
    schema::{CollectionSchema, PrimaryKeyType},
    value::Value,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use std::{collections::BTreeMap, fmt};

/// Longest allowed string primary key, in bytes.
pub const MAX_STRING_PRIMARY_KEY_BYTES: usize = 1_024;

/// A primary key value. Serialized untagged, as a JSON number or string.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PrimaryKey {
    /// A signed 64-bit integer key.
    Int64(i64),
    /// A non-empty UTF-8 string key of at most
    /// [`MAX_STRING_PRIMARY_KEY_BYTES`] bytes.
    String(String),
}

impl PrimaryKey {
    /// The type of this key.
    #[must_use]
    pub fn key_type(&self) -> PrimaryKeyType {
        match self {
            Self::Int64(_) => PrimaryKeyType::Int64,
            Self::String(_) => PrimaryKeyType::String,
        }
    }

    /// The key as user-facing JSON.
    #[must_use]
    pub fn to_json(&self) -> JsonValue {
        match self {
            Self::Int64(value) => JsonValue::from(*value),
            Self::String(value) => JsonValue::from(value.as_str()),
        }
    }
}

impl fmt::Display for PrimaryKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int64(value) => write!(formatter, "{value}"),
            Self::String(value) => write!(formatter, "{value:?}"),
        }
    }
}

impl From<i64> for PrimaryKey {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<&str> for PrimaryKey {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for PrimaryKey {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

/// One row of a collection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    /// The primary key.
    pub pk: PrimaryKey,
    /// Vectors by vector field name. A valid record has every vector field.
    #[serde(default)]
    pub vectors: BTreeMap<String, Vec<f32>>,
    /// Scalar values by field name. Absent means null.
    #[serde(default)]
    pub fields: BTreeMap<String, Value>,
    /// Undeclared keys kept by the dynamic `$extra` field.
    #[serde(default)]
    pub extra: Map<String, JsonValue>,
}

impl Record {
    /// An empty record with only a primary key.
    #[must_use]
    pub fn new(pk: impl Into<PrimaryKey>) -> Self {
        Self {
            pk: pk.into(),
            vectors: BTreeMap::new(),
            fields: BTreeMap::new(),
            extra: Map::new(),
        }
    }

    /// Set a vector, builder style.
    #[must_use]
    pub fn with_vector(mut self, name: impl Into<String>, vector: Vec<f32>) -> Self {
        self.vectors.insert(name.into(), vector);
        self
    }

    /// Set a scalar field, builder style.
    #[must_use]
    pub fn with_field(mut self, name: impl Into<String>, value: Value) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    /// Parse a user JSON document into a validated record.
    ///
    /// Keys are routed by the schema: the primary key, vector fields (arrays
    /// of numbers), declared scalar fields (converted with
    /// [`Value::from_json`]), and everything else into `extra` when dynamic
    /// fields are enabled. The `$extra` key itself is reserved.
    ///
    /// # Errors
    ///
    /// Returns a [`RecordError`] when the document does not fit the schema.
    pub fn from_json(schema: &CollectionSchema, json: JsonValue) -> Result<Self, RecordError> {
        let document = parse::parse_document(schema, json)?;
        let pk = document.pk.ok_or_else(|| RecordError::MissingPrimaryKey {
            field: schema.primary_key().name.clone(),
        })?;
        schema.validate_record(Self {
            pk,
            vectors: document.vectors,
            fields: document.fields,
            extra: document.extra,
        })
    }

    /// Flatten into a user JSON document; the inverse of
    /// [`Record::from_json`] for a validated record.
    #[must_use]
    pub fn to_json(&self, schema: &CollectionSchema) -> JsonValue {
        let mut object = self.extra.clone();
        object.insert(schema.primary_key().name.clone(), self.pk.to_json());
        for (name, vector) in &self.vectors {
            object.insert(name.clone(), vector_to_json(vector));
        }
        for (name, value) in &self.fields {
            object.insert(name.clone(), value.to_json());
        }
        JsonValue::Object(object)
    }
}

/// A change to some fields of one record, addressed by primary key, that
/// does not have to resend the vectors.
///
/// - `vectors`: each entry replaces that vector
/// - `fields`: each entry replaces that field; [`Value::Null`] clears it
/// - `extra`: each entry replaces that top-level dynamic key; JSON `null`
///   removes it, as in a JSON merge patch
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PartialUpdate {
    /// The primary key of the record to change.
    pub pk: PrimaryKey,
    /// Vectors to replace.
    #[serde(default)]
    pub vectors: BTreeMap<String, Vec<f32>>,
    /// Scalar fields to replace or clear.
    #[serde(default)]
    pub fields: BTreeMap<String, Value>,
    /// Dynamic keys to replace or remove.
    #[serde(default)]
    pub extra: Map<String, JsonValue>,
}

impl PartialUpdate {
    /// An update that changes nothing yet.
    #[must_use]
    pub fn new(pk: impl Into<PrimaryKey>) -> Self {
        Self {
            pk: pk.into(),
            vectors: BTreeMap::new(),
            fields: BTreeMap::new(),
            extra: Map::new(),
        }
    }

    /// Whether the update changes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty() && self.fields.is_empty() && self.extra.is_empty()
    }

    /// Parse a user JSON document with the primary key and the keys to
    /// change, using the same routing as [`Record::from_json`].
    ///
    /// # Errors
    ///
    /// Returns a [`RecordError`] when the update does not fit the schema.
    pub fn from_json(schema: &CollectionSchema, json: JsonValue) -> Result<Self, RecordError> {
        let document = parse::parse_document(schema, json)?;
        let pk = document.pk.ok_or_else(|| RecordError::MissingPrimaryKey {
            field: schema.primary_key().name.clone(),
        })?;
        schema.validate_update(Self {
            pk,
            vectors: document.vectors,
            fields: document.fields,
            extra: document.extra,
        })
    }

    /// Apply a validated update to a validated record of the same schema.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::PrimaryKeyMismatch`] when the keys differ; the
    /// record is unchanged.
    pub fn apply_to(self, record: &mut Record) -> Result<(), RecordError> {
        if self.pk != record.pk {
            return Err(RecordError::PrimaryKeyMismatch {
                update: self.pk.to_string(),
                record: record.pk.to_string(),
            });
        }
        record.vectors.extend(self.vectors);
        for (name, value) in self.fields {
            if value.is_null() {
                record.fields.remove(&name);
            } else {
                record.fields.insert(name, value);
            }
        }
        for (key, value) in self.extra {
            if value.is_null() {
                record.extra.remove(&key);
            } else {
                record.extra.insert(key, value);
            }
        }
        Ok(())
    }
}

/// Emit each component as the shortest decimal that reads back as the same
/// `f32` (so `0.1_f32` becomes `0.1`, not `0.10000000149011612`).
fn vector_to_json(vector: &[f32]) -> JsonValue {
    JsonValue::Array(
        vector
            .iter()
            .map(|component| {
                let shortest = component
                    .to_string()
                    .parse::<f64>()
                    .unwrap_or(f64::from(*component));
                serde_json::Number::from_f64(shortest).map_or(JsonValue::Null, JsonValue::Number)
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests;
