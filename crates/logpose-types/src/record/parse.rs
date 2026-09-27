//! Splitting a flat user JSON document into typed parts.

use super::{PrimaryKey, RecordError};
use crate::{
    schema::{CollectionSchema, DYNAMIC_FIELD_NAME, FieldRef, PrimaryKeyField, PrimaryKeyType},
    value::{Value, json_kind, number_to_i64},
};
use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;

/// A document split by field kind, before schema-level checks.
pub(super) struct Document {
    pub(super) pk: Option<PrimaryKey>,
    pub(super) vectors: BTreeMap<String, Vec<f32>>,
    pub(super) fields: BTreeMap<String, Value>,
    pub(super) extra: Map<String, JsonValue>,
}

/// Route every key of a JSON object to the primary key, a vector, a typed
/// scalar field, or `extra`. Whether `extra` is allowed, and whether required
/// parts are present, is decided by the validator.
pub(super) fn parse_document(
    schema: &CollectionSchema,
    json: JsonValue,
) -> Result<Document, RecordError> {
    let JsonValue::Object(object) = json else {
        return Err(RecordError::NotAnObject {
            found: json_kind(&json),
        });
    };
    let mut document = Document {
        pk: None,
        vectors: BTreeMap::new(),
        fields: BTreeMap::new(),
        extra: Map::new(),
    };
    for (key, value) in object {
        if key == DYNAMIC_FIELD_NAME {
            return Err(RecordError::ReservedKey { key });
        }
        match schema.field(&key) {
            Some(FieldRef::PrimaryKey(field)) => {
                document.pk = Some(parse_primary_key(field, value)?);
            }
            Some(FieldRef::Vector(_)) => {
                let vector = parse_vector(&key, value)?;
                document.vectors.insert(key, vector);
            }
            Some(FieldRef::Scalar(field)) => {
                let value = Value::from_json(value, field.field_type).map_err(|source| {
                    RecordError::InvalidField {
                        field: key.clone(),
                        source,
                    }
                })?;
                document.fields.insert(key, value);
            }
            None => {
                document.extra.insert(key, value);
            }
        }
    }
    Ok(document)
}

/// Convert JSON to a primary key. String keys accept only strings; integer
/// keys accept the same numbers as an `int64` field. No other coercion.
fn parse_primary_key(field: &PrimaryKeyField, json: JsonValue) -> Result<PrimaryKey, RecordError> {
    match (field.key_type, json) {
        (PrimaryKeyType::String, JsonValue::String(value)) => Ok(PrimaryKey::String(value)),
        (PrimaryKeyType::Int64, JsonValue::Number(number)) => number_to_i64(&number)
            .map(PrimaryKey::Int64)
            .map_err(|source| RecordError::InvalidPrimaryKey {
                field: field.name.clone(),
                source,
            }),
        (expected, other) => Err(RecordError::PrimaryKeyType {
            field: field.name.clone(),
            expected,
            found: json_kind(&other),
        }),
    }
}

/// Convert a JSON array of numbers to `f32` components. Finiteness and
/// dimension are checked by the validator.
fn parse_vector(field: &str, json: JsonValue) -> Result<Vec<f32>, RecordError> {
    let JsonValue::Array(items) = json else {
        return Err(RecordError::VectorNotArray {
            field: field.to_owned(),
            found: json_kind(&json),
        });
    };
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            item.as_f64()
                // Narrowing is intended; out-of-range values become infinite
                // and are rejected by the validator.
                .map(|component| component as f32)
                .ok_or_else(|| RecordError::VectorElementNotNumber {
                    field: field.to_owned(),
                    index,
                    found: json_kind(item),
                })
        })
        .collect()
}
