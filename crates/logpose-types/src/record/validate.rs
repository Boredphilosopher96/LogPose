//! Schema checks for records and partial updates.

use super::{MAX_STRING_PRIMARY_KEY_BYTES, PartialUpdate, PrimaryKey, Record, RecordError};
use crate::{
    schema::{CollectionSchema, DYNAMIC_FIELD_NAME, VectorField},
    value::Value,
};
use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;

impl CollectionSchema {
    /// Validate a record and return it in canonical form.
    ///
    /// Checks, in order:
    ///
    /// - the primary key has the schema's type; string keys are non-empty
    ///   and at most [`MAX_STRING_PRIMARY_KEY_BYTES`] bytes
    /// - every vector field is present with the declared dimension and only
    ///   finite components; no other vector names appear
    /// - every declared scalar value conforms to its type, and non-nullable
    ///   fields are present and not null
    /// - undeclared scalar keys move into `extra` when dynamic fields are
    ///   enabled, and are rejected otherwise
    /// - `extra` is empty unless dynamic fields are enabled, and its keys do
    ///   not collide with declared fields, retired names, or `$extra`
    ///
    /// The canonical form drops null scalar values, since absent means null.
    ///
    /// # Errors
    ///
    /// Returns the first [`RecordError`] found.
    pub fn validate_record(&self, record: Record) -> Result<Record, RecordError> {
        self.validate_primary_key(&record.pk)?;
        self.check_vectors(&record.vectors)?;
        if let Some(missing) = self
            .vectors()
            .iter()
            .find(|field| !record.vectors.contains_key(&field.name))
        {
            return Err(RecordError::MissingVector {
                field: missing.name.clone(),
            });
        }
        let (fields, extra) = self.check_fields(record.fields, record.extra)?;
        let fields: BTreeMap<String, Value> = fields
            .into_iter()
            .filter(|(_, value)| !value.is_null())
            .collect();
        if let Some(missing) = self
            .fields()
            .iter()
            .find(|field| !field.nullable && !fields.contains_key(&field.name))
        {
            return Err(RecordError::RequiredField {
                field: missing.name.clone(),
            });
        }
        Ok(Record {
            pk: record.pk,
            vectors: record.vectors,
            fields,
            extra,
        })
    }

    /// Validate a partial update and return it in canonical form.
    ///
    /// Applies the same per-value checks as
    /// [`validate_record`](Self::validate_record), without requiring every
    /// vector or non-nullable field. Setting a non-nullable field to null is
    /// rejected. Nulls on nullable fields are kept, because they clear the
    /// field.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::EmptyUpdate`] when nothing changes, or the
    /// first other [`RecordError`] found.
    pub fn validate_update(&self, update: PartialUpdate) -> Result<PartialUpdate, RecordError> {
        if update.is_empty() {
            return Err(RecordError::EmptyUpdate);
        }
        self.validate_primary_key(&update.pk)?;
        self.check_vectors(&update.vectors)?;
        let (fields, extra) = self.check_fields(update.fields, update.extra)?;
        for (name, value) in &fields {
            let nullable = self.scalar_field(name).is_none_or(|field| field.nullable);
            if value.is_null() && !nullable {
                return Err(RecordError::RequiredField {
                    field: name.clone(),
                });
            }
        }
        Ok(PartialUpdate {
            pk: update.pk,
            vectors: update.vectors,
            fields,
            extra,
        })
    }

    /// Check that a primary key has the schema's key type and, for string
    /// keys, is non-empty and at most [`MAX_STRING_PRIMARY_KEY_BYTES`] bytes.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::PrimaryKeyType`],
    /// [`RecordError::EmptyPrimaryKey`], or
    /// [`RecordError::PrimaryKeyTooLong`].
    pub fn validate_primary_key(&self, pk: &PrimaryKey) -> Result<(), RecordError> {
        let field = self.primary_key();
        if pk.key_type() != field.key_type {
            return Err(RecordError::PrimaryKeyType {
                field: field.name.clone(),
                expected: field.key_type,
                found: match pk {
                    PrimaryKey::Int64(_) => "int64",
                    PrimaryKey::String(_) => "string",
                },
            });
        }
        if let PrimaryKey::String(value) = pk {
            if value.is_empty() {
                return Err(RecordError::EmptyPrimaryKey {
                    field: field.name.clone(),
                });
            }
            if value.len() > MAX_STRING_PRIMARY_KEY_BYTES {
                return Err(RecordError::PrimaryKeyTooLong {
                    field: field.name.clone(),
                    len: value.len(),
                    max: MAX_STRING_PRIMARY_KEY_BYTES,
                });
            }
        }
        Ok(())
    }

    /// Check that every supplied vector is declared, has the right dimension,
    /// and is finite. Presence of all vectors is checked by the caller.
    fn check_vectors(&self, vectors: &BTreeMap<String, Vec<f32>>) -> Result<(), RecordError> {
        for (name, vector) in vectors {
            let field = self
                .vector_field(name)
                .ok_or_else(|| RecordError::UnknownVectorField {
                    field: name.clone(),
                })?;
            check_vector(field, vector)?;
        }
        Ok(())
    }

    /// Conform declared scalar values, route undeclared keys into `extra`,
    /// and check `extra` against the schema. Nulls are kept.
    fn check_fields(
        &self,
        fields: BTreeMap<String, Value>,
        mut extra: Map<String, JsonValue>,
    ) -> Result<(BTreeMap<String, Value>, Map<String, JsonValue>), RecordError> {
        for key in extra.keys() {
            self.check_dynamic_key(key)?;
        }
        let mut typed = BTreeMap::new();
        for (name, value) in fields {
            if let Some(field) = self.scalar_field(&name) {
                let value = value.conform(field.field_type).map_err(|source| {
                    RecordError::InvalidField {
                        field: name.clone(),
                        source,
                    }
                })?;
                typed.insert(name, value);
                continue;
            }
            if self.field(&name).is_some() {
                return Err(RecordError::NotAScalarField { field: name });
            }
            self.check_dynamic_key(&name)?;
            if extra.contains_key(&name) {
                return Err(RecordError::ExtraKeyConflict { key: name });
            }
            extra.insert(name, value.into_json());
        }
        Ok((typed, extra))
    }

    fn check_dynamic_key(&self, key: &str) -> Result<(), RecordError> {
        if key == DYNAMIC_FIELD_NAME {
            return Err(RecordError::ReservedKey {
                key: key.to_owned(),
            });
        }
        if self.field(key).is_some() {
            return Err(RecordError::ExtraKeyConflict {
                key: key.to_owned(),
            });
        }
        if self.is_retired(key) {
            return Err(RecordError::RetiredKey {
                key: key.to_owned(),
            });
        }
        if !self.dynamic_fields() {
            return Err(RecordError::UnknownField {
                field: key.to_owned(),
            });
        }
        Ok(())
    }
}

fn check_vector(field: &VectorField, vector: &[f32]) -> Result<(), RecordError> {
    let dimensions_match =
        usize::try_from(field.dimensions).is_ok_and(|expected| expected == vector.len());
    if !dimensions_match {
        return Err(RecordError::VectorDimensionMismatch {
            field: field.name.clone(),
            expected: field.dimensions,
            actual: vector.len(),
        });
    }
    if let Some(index) = vector.iter().position(|component| !component.is_finite()) {
        return Err(RecordError::NonFiniteVectorElement {
            field: field.name.clone(),
            index,
        });
    }
    Ok(())
}
