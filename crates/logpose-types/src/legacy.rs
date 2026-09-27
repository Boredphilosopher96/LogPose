//! Mapping between the v1 data model and the v2 schema model.
//!
//! A v1 collection is `(dimensions, metric)` plus records of the shape
//! `PutRecord { id, vector, metadata }`. In the v2 model it is a collection
//! whose schema is a string primary key `id`, one vector field `vector`, no
//! scalar fields, and dynamic fields on, which is what a v1
//! create-collection request produces. A v1 record becomes a [`Record`]
//! whose metadata keys live in the dynamic `$extra` field, so v1 filters on
//! top-level metadata keys resolve as undeclared names.
//!
//! This module exists only while the v1 `StorageEngine` API is served on
//! top of the v2 engine, and is deleted with it.

use crate::{
    DistanceMetric, PutRecord, RecordId, WriteOperation,
    record::{ClientOp, PrimaryKey, Record, RecordError},
    schema::{
        CollectionSchema, MAX_VECTOR_DIMENSIONS, MIN_VECTOR_DIMENSIONS, PrimaryKeySpec,
        PrimaryKeyType, SchemaError, VectorFieldSpec,
    },
    value::json_kind,
};
use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;
use thiserror::Error;

/// Name of the primary key field of a legacy collection.
pub const LEGACY_PRIMARY_KEY_FIELD: &str = "id";
/// Name of the single vector field of a legacy collection.
pub const LEGACY_VECTOR_FIELD: &str = "vector";

/// Reasons a v1 record or operation does not map to the v2 model.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum LegacyError {
    /// v1 metadata must be a JSON object or null to become dynamic fields.
    #[error("record metadata must be a JSON object or null, found {found}")]
    MetadataNotObject {
        /// The kind of JSON that was supplied.
        found: &'static str,
    },
    /// The mapped record does not fit the collection schema. This includes
    /// metadata keys named `id` or `vector`, which collide with the legacy
    /// primary key and vector field, and the reserved key `$extra`.
    #[error(transparent)]
    Record(#[from] RecordError),
    /// A v2 record cannot be expressed as a v1 record: it has an integer
    /// key, typed scalar fields, or no `vector`.
    #[error("record {pk} has no v1 form: {reason}")]
    NotLegacyShaped {
        /// The record's primary key.
        pk: String,
        /// What does not fit.
        reason: &'static str,
    },
}

/// The schema of a legacy collection: string primary key `id`, one vector
/// field `vector` with the given dimensions and metric, and dynamic fields
/// on.
///
/// # Errors
///
/// Returns [`SchemaError::InvalidDimensions`] when `dimensions` is outside
/// 1 to 65,536.
pub fn legacy_schema(
    dimensions: usize,
    metric: DistanceMetric,
) -> Result<CollectionSchema, SchemaError> {
    let dimensions = u32::try_from(dimensions).map_err(|_| SchemaError::InvalidDimensions {
        field: LEGACY_VECTOR_FIELD.to_owned(),
        dimensions: u32::MAX,
        min: MIN_VECTOR_DIMENSIONS,
        max: MAX_VECTOR_DIMENSIONS,
    })?;
    CollectionSchema::new(
        PrimaryKeySpec {
            name: LEGACY_PRIMARY_KEY_FIELD.to_owned(),
            key_type: PrimaryKeyType::String,
        },
        vec![VectorFieldSpec {
            name: LEGACY_VECTOR_FIELD.to_owned(),
            dimensions,
            metric,
        }],
        Vec::new(),
        true,
    )
}

/// Map a v1 record to a v2 record, without schema validation.
///
/// # Errors
///
/// Returns [`LegacyError::MetadataNotObject`] when the metadata is neither
/// an object nor null.
pub fn record_from_put(put: PutRecord) -> Result<Record, LegacyError> {
    let extra = match put.metadata {
        JsonValue::Null => Map::new(),
        JsonValue::Object(object) => object,
        other => {
            return Err(LegacyError::MetadataNotObject {
                found: json_kind(&other),
            });
        }
    };
    Ok(Record {
        pk: PrimaryKey::String(put.id.0),
        vectors: BTreeMap::from([(LEGACY_VECTOR_FIELD.to_owned(), put.vector)]),
        fields: BTreeMap::new(),
        extra,
    })
}

/// Map a v1 write operation to a v2 client operation, without schema
/// validation.
///
/// # Errors
///
/// As [`record_from_put`].
pub fn client_op_from_write(operation: WriteOperation) -> Result<ClientOp, LegacyError> {
    match operation {
        WriteOperation::Put(put) => record_from_put(put).map(ClientOp::Upsert),
        WriteOperation::Delete(delete) => Ok(ClientOp::Delete(PrimaryKey::String(delete.id.0))),
    }
}

/// Map a v1 write operation to a v2 client operation and validate it
/// against `schema`, returning the operation in canonical form.
///
/// # Errors
///
/// Returns [`LegacyError::MetadataNotObject`] or the first
/// [`RecordError`] from schema validation.
pub fn validate_write(
    schema: &CollectionSchema,
    operation: WriteOperation,
) -> Result<ClientOp, LegacyError> {
    match client_op_from_write(operation)? {
        ClientOp::Upsert(record) => Ok(ClientOp::Upsert(schema.validate_record(record)?)),
        ClientOp::Update(update) => Ok(ClientOp::Update(schema.validate_update(update)?)),
        ClientOp::Delete(pk) => {
            schema.validate_primary_key(&pk)?;
            Ok(ClientOp::Delete(pk))
        }
    }
}

/// Map a v2 record of a legacy collection back to a v1 record. The dynamic
/// keys become the metadata object; a record without dynamic keys maps to
/// an empty object, so v1 `null` metadata reads back as `{}`.
///
/// # Errors
///
/// Returns [`LegacyError::NotLegacyShaped`] for an integer key, typed
/// scalar fields, or a missing `vector`.
pub fn put_from_record(mut record: Record) -> Result<PutRecord, LegacyError> {
    let not_legacy = |pk: &PrimaryKey, reason| LegacyError::NotLegacyShaped {
        pk: pk.to_string(),
        reason,
    };
    let id = match &record.pk {
        PrimaryKey::String(id) => RecordId::new(id.clone()),
        PrimaryKey::Int64(_) => return Err(not_legacy(&record.pk, "the key is an integer")),
    };
    if !record.fields.is_empty() {
        return Err(not_legacy(&record.pk, "it has typed scalar fields"));
    }
    let vector = record
        .vectors
        .remove(LEGACY_VECTOR_FIELD)
        .ok_or_else(|| not_legacy(&record.pk, "it has no 'vector' field"))?;
    if !record.vectors.is_empty() {
        return Err(not_legacy(&record.pk, "it has more than one vector field"));
    }
    Ok(PutRecord {
        id,
        vector,
        metadata: JsonValue::Object(record.extra),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeleteRecord;
    use serde_json::json;

    fn put(metadata: JsonValue) -> PutRecord {
        PutRecord {
            id: RecordId::new("alpha"),
            vector: vec![1.0, 0.0],
            metadata,
        }
    }

    #[test]
    fn legacy_schema_has_string_id_one_vector_and_dynamic_fields() {
        let schema = legacy_schema(2, DistanceMetric::Dot).expect("schema should build");
        assert_eq!(schema.primary_key().name, "id");
        assert_eq!(schema.primary_key_type(), PrimaryKeyType::String);
        assert_eq!(schema.vectors().len(), 1);
        assert_eq!(schema.vectors()[0].name, "vector");
        assert_eq!(schema.vectors()[0].dimensions, 2);
        assert_eq!(schema.vectors()[0].metric, DistanceMetric::Dot);
        assert!(schema.fields().is_empty());
        assert!(schema.dynamic_fields());
        assert_eq!(schema.schema_version(), 1);
    }

    #[test]
    fn legacy_schema_rejects_out_of_range_dimensions() {
        for dimensions in [0, 65_537, usize::MAX] {
            assert!(matches!(
                legacy_schema(dimensions, DistanceMetric::Cosine),
                Err(SchemaError::InvalidDimensions { .. })
            ));
        }
    }

    #[test]
    fn put_maps_metadata_to_dynamic_fields_and_back() {
        let original = put(json!({ "color": "red", "size": 3, "nested": { "a": [1] } }));
        let record = record_from_put(original.clone()).expect("put should map");
        assert_eq!(record.pk, PrimaryKey::from("alpha"));
        assert_eq!(record.vectors["vector"], vec![1.0, 0.0]);
        assert!(record.fields.is_empty());
        assert_eq!(record.extra["color"], json!("red"));
        assert_eq!(put_from_record(record), Ok(original));
    }

    #[test]
    fn null_metadata_maps_to_no_dynamic_fields() {
        let record = record_from_put(put(JsonValue::Null)).expect("put should map");
        assert!(record.extra.is_empty());
        assert_eq!(
            put_from_record(record).map(|put| put.metadata),
            Ok(json!({}))
        );
    }

    #[test]
    fn non_object_metadata_is_rejected() {
        assert_eq!(
            record_from_put(put(json!([1, 2]))),
            Err(LegacyError::MetadataNotObject { found: "array" })
        );
    }

    #[test]
    fn validate_write_checks_records_against_the_schema() {
        let schema = legacy_schema(2, DistanceMetric::Cosine).expect("schema should build");
        let valid = validate_write(&schema, WriteOperation::Put(put(json!({ "k": 1 }))))
            .expect("valid put");
        assert!(matches!(valid, ClientOp::Upsert(_)));

        let mut wrong_dimensions = put(json!({}));
        wrong_dimensions.vector = vec![1.0];
        assert!(matches!(
            validate_write(&schema, WriteOperation::Put(wrong_dimensions)),
            Err(LegacyError::Record(
                RecordError::VectorDimensionMismatch { .. }
            ))
        ));

        assert_eq!(
            validate_write(&schema, WriteOperation::Put(put(json!({ "id": "x" })))),
            Err(LegacyError::Record(RecordError::ExtraKeyConflict {
                key: "id".to_owned()
            }))
        );

        assert_eq!(
            validate_write(
                &schema,
                WriteOperation::Delete(DeleteRecord {
                    id: RecordId::new("")
                })
            ),
            Err(LegacyError::Record(RecordError::EmptyPrimaryKey {
                field: "id".to_owned()
            }))
        );
        assert_eq!(
            validate_write(
                &schema,
                WriteOperation::Delete(DeleteRecord {
                    id: RecordId::new("alpha")
                })
            ),
            Ok(ClientOp::Delete(PrimaryKey::from("alpha")))
        );
    }

    #[test]
    fn records_outside_the_legacy_shape_have_no_v1_form() {
        let integer = Record::new(1).with_vector("vector", vec![1.0]);
        assert!(matches!(
            put_from_record(integer),
            Err(LegacyError::NotLegacyShaped { .. })
        ));
        let typed = Record::new("a")
            .with_vector("vector", vec![1.0])
            .with_field("price", crate::value::Value::Int64(1));
        assert!(matches!(
            put_from_record(typed),
            Err(LegacyError::NotLegacyShaped { .. })
        ));
        assert!(matches!(
            put_from_record(Record::new("a")),
            Err(LegacyError::NotLegacyShaped { .. })
        ));
    }
}
