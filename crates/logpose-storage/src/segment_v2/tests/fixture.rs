//! A small, fully deterministic segment that exercises every section kind.
//! It is the input of the committed golden file, so changing anything here
//! changes the golden bytes.

use crate::segment_v2::{IndexSection, IndexSectionKind, SegmentBuilder, SegmentIdentity};
use logpose_types::{
    CollectionId, DistanceMetric,
    record::{PrimaryKey, Record},
    schema::{
        CollectionSchema, ElementType, FieldIndex, FieldType, PrimaryKeySpec, PrimaryKeyType,
        ScalarFieldSpec, VectorFieldSpec,
    },
    value::{Timestamp, Value},
};
use logpose_wal::codec::RowImage;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

pub(super) const GOLDEN_UNIT: u32 = 42;

pub(super) fn golden_collection() -> CollectionId {
    CollectionId(Uuid::from_bytes([
        0x6c, 0x6f, 0x67, 0x70, 0x6f, 0x73, 0x65, 0x2d, 0x73, 0x65, 0x67, 0x2d, 0x76, 0x32, 0x00,
        0x01,
    ]))
}

fn scalar(name: &str, field_type: FieldType) -> ScalarFieldSpec {
    ScalarFieldSpec::new(name, field_type)
}

/// The golden schema: a string key, two vector fields, every scalar type,
/// dynamic fields, and one dropped field (so field ids have a gap and the
/// schema has a retired name).
pub(super) fn golden_schema() -> CollectionSchema {
    let mut schema = CollectionSchema::new(
        PrimaryKeySpec {
            name: "sku".to_owned(),
            key_type: PrimaryKeyType::String,
        },
        vec![
            VectorFieldSpec {
                name: "embedding".to_owned(),
                dimensions: 4,
                metric: DistanceMetric::Cosine,
            },
            VectorFieldSpec {
                name: "image".to_owned(),
                dimensions: 3,
                metric: DistanceMetric::L2,
            },
        ],
        vec![
            scalar("in_stock", FieldType::Bool),
            scalar("count", FieldType::Int64),
            scalar("legacy", FieldType::Int64),
            scalar("price", FieldType::Float64),
            scalar("color", FieldType::String),
            scalar("title", FieldType::String),
            scalar("added_at", FieldType::Timestamp),
            scalar("tags", FieldType::Array(ElementType::String)),
            scalar("sizes", FieldType::Array(ElementType::Int64)),
            scalar("weights", FieldType::Array(ElementType::Float64)),
            scalar("flags", FieldType::Array(ElementType::Bool)),
            scalar("seen_at", FieldType::Array(ElementType::Timestamp)),
            ScalarFieldSpec {
                index: FieldIndex::None,
                ..scalar("details", FieldType::Json)
            },
        ],
        true,
    )
    .expect("golden schema is valid");
    schema.drop_field("legacy").expect("drop succeeds");
    schema
}

fn ts(micros: i64) -> Value {
    Value::Timestamp(Timestamp::from_micros(micros).expect("timestamp in range"))
}

fn strings(values: &[&str]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|value| Value::String((*value).to_owned()))
            .collect(),
    )
}

/// The golden rows as `(seq_no, record)`, with the vector fields to null
/// out after conversion.
fn golden_records() -> Vec<(u64, Record, &'static [&'static str])> {
    let mut rows = Vec::new();

    let mut record = Record::new("A-1");
    record
        .vectors
        .insert("embedding".to_owned(), vec![1.0, 0.0, 0.0, 0.0]);
    record
        .vectors
        .insert("image".to_owned(), vec![0.5, -0.25, 2.0]);
    record
        .fields
        .insert("in_stock".to_owned(), Value::Bool(true));
    record.fields.insert("count".to_owned(), Value::Int64(7));
    record
        .fields
        .insert("price".to_owned(), Value::Float64(12.5));
    record
        .fields
        .insert("color".to_owned(), Value::String("red".to_owned()));
    record
        .fields
        .insert("title".to_owned(), Value::String("Red mug".to_owned()));
    record
        .fields
        .insert("added_at".to_owned(), ts(1_790_000_000_000_000));
    record
        .fields
        .insert("tags".to_owned(), strings(&["kitchen", "mug"]));
    record.fields.insert(
        "sizes".to_owned(),
        Value::Array(vec![Value::Int64(1), Value::Int64(-2)]),
    );
    record.fields.insert(
        "weights".to_owned(),
        Value::Array(vec![Value::Float64(0.25)]),
    );
    record.fields.insert(
        "flags".to_owned(),
        Value::Array(vec![Value::Bool(true), Value::Bool(false)]),
    );
    record
        .fields
        .insert("seen_at".to_owned(), Value::Array(vec![ts(0), ts(-1)]));
    record.fields.insert(
        "details".to_owned(),
        Value::Json(json!({"origin": "PT", "dims": [8, 9.5], "ok": true, "none": null})),
    );
    record.extra.insert("material".to_owned(), json!("ceramic"));
    rows.push((10, record, &[][..]));

    let mut record = Record::new("B-2");
    record
        .vectors
        .insert("embedding".to_owned(), vec![0.0, 3.0, 4.0, 0.0]);
    record
        .vectors
        .insert("image".to_owned(), vec![0.0, 0.0, 0.0]);
    record
        .fields
        .insert("in_stock".to_owned(), Value::Bool(false));
    record
        .fields
        .insert("color".to_owned(), Value::String("blue".to_owned()));
    record
        .fields
        .insert("title".to_owned(), Value::String(String::new()));
    record.fields.insert("tags".to_owned(), strings(&[]));
    record
        .fields
        .insert("price".to_owned(), Value::Float64(-0.0));
    rows.push((11, record, &["image"][..]));

    let mut record = Record::new("C-3 \u{65e5}");
    record
        .vectors
        .insert("embedding".to_owned(), vec![-1.0, 1.0, -1.0, 1.0]);
    record
        .vectors
        .insert("image".to_owned(), vec![1.0, 2.0, 3.0]);
    record
        .fields
        .insert("count".to_owned(), Value::Int64(i64::MIN));
    record
        .fields
        .insert("color".to_owned(), Value::String("red".to_owned()));
    record
        .fields
        .insert("added_at".to_owned(), ts(Timestamp::MAX.as_micros()));
    record.fields.insert(
        "sizes".to_owned(),
        Value::Array(vec![Value::Int64(i64::MAX)]),
    );
    record.fields.insert(
        "details".to_owned(),
        Value::Json(json!([1, "two", {"three": 3}])),
    );
    record.extra.insert(
        "zeta".to_owned(),
        json!({"nested": [null, false, 18446744073709551615_u64]}),
    );
    record.extra.insert("alpha".to_owned(), json!(-7));
    rows.push((12, record, &[][..]));

    let mut record = Record::new("D-4");
    record
        .vectors
        .insert("embedding".to_owned(), vec![0.0, 0.0, 0.0, 2.0]);
    record
        .vectors
        .insert("image".to_owned(), vec![-9.0, 0.125, 7.0]);
    record
        .fields
        .insert("color".to_owned(), Value::String("blue".to_owned()));
    record.fields.insert(
        "weights".to_owned(),
        Value::Array(vec![Value::Float64(1e-300), Value::Float64(-3.5)]),
    );
    rows.push((20, record, &[][..]));

    let mut record = Record::new("E-5");
    record
        .vectors
        .insert("embedding".to_owned(), vec![0.1, 0.2, 0.3, 0.4]);
    record
        .vectors
        .insert("image".to_owned(), vec![0.0, 1.0, 0.0]);
    record
        .fields
        .insert("in_stock".to_owned(), Value::Bool(true));
    record.fields.insert("count".to_owned(), Value::Int64(7));
    record
        .fields
        .insert("color".to_owned(), Value::String("red".to_owned()));
    record
        .fields
        .insert("title".to_owned(), Value::String("Blue bowl".to_owned()));
    record.extra.insert("material".to_owned(), json!("glass"));
    rows.push((13, record, &["embedding", "image"][..]));

    rows
}

/// The golden rows as the builder receives them.
pub(super) fn golden_rows(schema: &CollectionSchema) -> Vec<(u64, RowImage)> {
    golden_records()
        .into_iter()
        .map(|(seq_no, record, null_vectors)| {
            let mut image = RowImage::from_record(schema, record).expect("golden row is valid");
            image.vectors.retain(|(field, _)| {
                !null_vectors.iter().any(|name| {
                    schema
                        .vector_field(name)
                        .is_some_and(|vector| vector.id == *field)
                })
            });
            (seq_no, image)
        })
        .collect()
}

/// Opaque index payloads attached to the golden segment.
pub(super) fn golden_index_sections(schema: &CollectionSchema) -> Vec<IndexSection> {
    let embedding = schema.vector_field("embedding").expect("declared").id;
    let color = schema.scalar_field("color").expect("declared").id;
    vec![
        IndexSection {
            kind: IndexSectionKind::VectorSq8,
            field: embedding,
            encoding: 1,
            aux32: 4,
            aux64: 5,
            payload: b"sq8 payload owned by logpose-index".to_vec(),
        },
        IndexSection {
            kind: IndexSectionKind::ScalarInverted,
            field: color,
            encoding: 1,
            aux32: 0,
            aux64: 3,
            payload: vec![0xab; 13],
        },
    ]
}

/// Build the golden segment.
pub(super) fn golden_bytes() -> Vec<u8> {
    let schema = Arc::new(golden_schema());
    let mut builder = SegmentBuilder::new(
        Arc::clone(&schema),
        SegmentIdentity {
            collection_id: golden_collection(),
            unit_id: GOLDEN_UNIT,
        },
    )
    .expect("builder");
    for (seq_no, image) in golden_rows(&schema) {
        builder
            .push_row_image(seq_no, &image)
            .expect("row accepted");
    }
    for section in golden_index_sections(&schema) {
        builder
            .add_index_section(section)
            .expect("index section accepted");
    }
    builder.finish_to_vec().expect("golden segment builds").0
}

/// A key of the golden segment.
pub(super) fn golden_pk(row: usize) -> PrimaryKey {
    ["A-1", "B-2", "C-3 \u{65e5}", "D-4", "E-5"]
        .get(row)
        .map(|key| PrimaryKey::from(*key))
        .expect("golden row exists")
}
