//! The v1 view of v2 rows, for the legacy `StorageEngine` read paths.
//!
//! The mutable delta holds `FieldId`-keyed row images. The legacy read paths (exact scan,
//! latest-visible lookups, statistics, v1 segment flushes) still speak `PutRecord { id, vector,
//! metadata }`. A row is read with the reading `Version`'s schema ([`RowImage::to_record`]:
//! dropped fields skipped, dynamic keys shadowed), then flattened: the key becomes the id, the
//! collection's vector field the vector, and the visible dynamic keys plus the typed scalar
//! fields (under their current names) the metadata object. Deleted with the legacy trait
//! (PR 14).

use crate::{
    segment_v1::SegmentRecord,
    version::{DeltaOp, DeltaRecord},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    DeleteRecord, LogPoseError, PutRecord, RecordId, Result, WriteOperation,
    legacy::client_op_from_write, record::ClientOp, schema::CollectionSchema,
};
use logpose_wal::codec::{RowImage, WirePk};
use serde_json::Value as JsonValue;

/// Map a v1 batch to client operations for the writer, with the v1 error messages for the
/// checks only the v1 shape has (an empty batch, the configured dimensions, non-object
/// metadata). Schema validation, duplicate ids and cosine normalization happen in the writer.
pub(crate) fn legacy_ops(
    descriptor: &CollectionDescriptor,
    operations: Vec<WriteOperation>,
) -> Result<Vec<ClientOp>> {
    if operations.is_empty() {
        return Err(LogPoseError::invalid_field(
            "operations",
            "write batch must include at least one operation",
        ));
    }
    operations
        .into_iter()
        .enumerate()
        .map(|(index, operation)| {
            let prefix = format!("operations[{index}]");
            descriptor
                .validate_operation(&operation)
                .map_err(|error| error.with_field_prefix(&prefix))?;
            let id = operation.id().clone();
            client_op_from_write(operation).map_err(|error| {
                LogPoseError::invalid_field(prefix, format!("record '{id}' is invalid: {error}"))
            })
        })
        .collect()
}

/// The v1 record id of a key.
pub(crate) fn legacy_id(pk: &WirePk) -> RecordId {
    match pk {
        WirePk::String(value) => RecordId::new(value.clone()),
        WirePk::Int64(value) => RecordId::new(value.to_string()),
    }
}

/// The v1 put of `image` as a reader of `schema` sees it.
pub(crate) fn legacy_put(schema: &CollectionSchema, image: &RowImage) -> Result<PutRecord> {
    let mut record = image.to_record(schema).map_err(|error| {
        LogPoseError::internal(format!(
            "row '{}' cannot be read with schema version {}: {error}",
            legacy_id(&image.pk),
            schema.schema_version()
        ))
    })?;
    let vector = schema
        .vectors()
        .first()
        .and_then(|field| record.vectors.remove(&field.name))
        .unwrap_or_default();
    let mut metadata = std::mem::take(&mut record.extra);
    for (name, value) in record.fields {
        metadata.insert(name, value.into_json());
    }
    Ok(PutRecord {
        id: legacy_id(&image.pk),
        vector,
        metadata: JsonValue::Object(metadata),
    })
}

/// The v1 record of one delta operation, or `None` for a schema change, which changes no row.
pub(crate) fn legacy_record(
    schema: &CollectionSchema,
    record: &DeltaRecord,
) -> Result<Option<SegmentRecord>> {
    let op = match &record.op {
        DeltaOp::Put(image) => WriteOperation::Put(legacy_put(schema, image)?),
        DeltaOp::Delete(pk) => WriteOperation::Delete(DeleteRecord { id: legacy_id(pk) }),
        DeltaOp::SchemaChange { .. } => return Ok(None),
    };
    Ok(Some(SegmentRecord {
        seq_no: record.seq_no,
        op,
    }))
}

/// The key a delta operation writes, if it writes one.
pub(crate) fn delta_key(op: &DeltaOp) -> Option<&WirePk> {
    match op {
        DeltaOp::Put(image) => Some(&image.pk),
        DeltaOp::Delete(pk) => Some(pk),
        DeltaOp::SchemaChange { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_types::{
        DistanceMetric,
        legacy::legacy_schema,
        record::Record,
        schema::{FieldType, ScalarFieldSpec},
        value::Value,
    };
    use serde_json::json;

    #[test]
    fn typed_fields_join_the_metadata_and_dropped_ones_disappear() {
        let mut schema = legacy_schema(2, DistanceMetric::Dot).expect("schema");
        schema
            .add_field(ScalarFieldSpec::new("price", FieldType::Int64))
            .expect("add");
        let mut record = Record::new("a")
            .with_vector("vector", vec![1.0, 2.0])
            .with_field("price", Value::Int64(5));
        record.extra.insert("color".to_owned(), json!("red"));
        let image = RowImage::from_record(&schema, record).expect("image");

        let put = legacy_put(&schema, &image).expect("put");
        assert_eq!(put.id, RecordId::new("a"));
        assert_eq!(put.vector, vec![1.0, 2.0]);
        assert_eq!(put.metadata, json!({"color": "red", "price": 5}));

        schema.drop_field("price").expect("drop");
        let put = legacy_put(&schema, &image).expect("put");
        assert_eq!(put.metadata, json!({"color": "red"}));

        schema.rename_field("vector", "embedding").expect("rename");
        assert_eq!(
            legacy_put(&schema, &image).expect("put").vector,
            vec![1.0, 2.0],
            "the vector is found by field id, whatever its name"
        );
    }
}
