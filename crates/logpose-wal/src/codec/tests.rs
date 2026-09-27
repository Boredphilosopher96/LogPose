use super::*;
use logpose_types::{
    schema::{ElementType, FieldIndex, PrimaryKeySpec, ScalarFieldSpec, VectorFieldSpec},
    value::Timestamp,
};
use serde_json::json;

fn schema(metric: DistanceMetric) -> CollectionSchema {
    CollectionSchema::new(
        PrimaryKeySpec {
            name: "id".to_owned(),
            key_type: PrimaryKeyType::String,
        },
        vec![VectorFieldSpec {
            name: "v".to_owned(),
            dimensions: 2,
            metric,
        }],
        vec![ScalarFieldSpec::new("n", FieldType::Int64)],
        true,
    )
    .expect("schema should be valid")
}

/// The schema used by the golden bytes: version 3 after adding and dropping
/// `tmp`, so `next_field_id` is 4 and `tmp` is retired.
fn evolved_schema() -> CollectionSchema {
    let mut schema = schema(DistanceMetric::Cosine);
    schema
        .add_field(ScalarFieldSpec::new("tmp", FieldType::Bool))
        .expect("add should succeed");
    schema.drop_field("tmp").expect("drop should succeed");
    schema
}

fn row(pk: &str) -> RowImage {
    RowImage {
        pk: WirePk::String(pk.to_owned()),
        vectors: vec![(FieldId(1), F32Bytes::from_f32s(&[1.0, 0.0]))],
        scalars: vec![(
            FieldId(2),
            ValueBytes::encode(&Value::Int64(5)).expect("value should encode"),
        )],
        dynamic: Some(ValueBytes::encode_json(&json!({ "x": true })).expect("json should encode")),
    }
}

fn round_trip(payload: &WalPayload) -> Vec<u8> {
    let bytes = payload.encode().expect("payload should encode");
    assert_eq!(WalPayload::decode(&bytes).as_ref(), Ok(payload));
    bytes
}

/// Golden bytes pin the WAL payload encoding. If one of these fails, the
/// encoding changed and existing WAL files would be misread: change it only
/// on purpose, together with the frame `format_version`.
#[test]
fn golden_bytes_for_checkpoint_payload() {
    let payload = WalPayload::Checkpoint(CheckpointPayload {
        manifest_generation: 7,
        checkpoint_seq_no: 300,
    });
    assert_eq!(round_trip(&payload), vec![0x02, 0x07, 0xac, 0x02]);
}

#[test]
fn golden_bytes_for_delete_ops() {
    let payload = WalPayload::WriteBatch(WriteBatchPayload {
        schema_version: 1,
        ops: vec![
            RowOp::Delete(WirePk::Int64(-2)),
            RowOp::Delete(WirePk::String("ab".to_owned())),
        ],
    });
    assert_eq!(
        round_trip(&payload),
        vec![
            0x00, // WriteBatch
            0x01, // schema_version
            0x02, // two ops
            0x01, 0x00, 0x03, // Delete(Int64(-2)), zigzag
            0x01, 0x01, 0x02, 0x61, 0x62, // Delete(String("ab"))
        ]
    );
}

#[test]
fn golden_bytes_for_put_op() {
    let payload = WalPayload::WriteBatch(WriteBatchPayload {
        schema_version: 1,
        ops: vec![RowOp::Put(row("k"))],
    });
    assert_eq!(
        round_trip(&payload),
        vec![
            0x00, 0x01, 0x01, // WriteBatch, schema_version 1, one op
            0x00, // Put
            0x01, 0x01, 0x6b, // pk String("k")
            0x01, // one vector
            0x01, 0x08, 0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x00, // field 1: [1.0, 0.0]
            0x01, // one scalar
            0x02, 0x02, 0x03, 0x0a, // field 2: Int64(5)
            0x01, 0x05, 0x18, 0x01, 0x01, 0x78, 0x12, // Some({"x": true})
        ]
    );
}

#[test]
fn golden_bytes_for_schema_change_payload() {
    let payload = WalPayload::SchemaChange(SchemaChangePayload {
        schema: evolved_schema(),
    });
    assert_eq!(
        round_trip(&payload),
        vec![
            0x01, // SchemaChange
            0x03, // schema_version
            0x04, // next_field_id
            0x00, 0x02, 0x69, 0x64, 0x00, // primary key: id 0, "id", string
            0x01, 0x01, 0x01, 0x76, 0x02, 0x00, // vectors: id 1, "v", 2 dims, cosine
            0x01, 0x02, 0x01, 0x6e, // fields: id 2, "n"
            0x05, 0x69, 0x6e, 0x74, 0x36, 0x34, // type "int64"
            0x04, 0x01, // index inverted_and_sorted, nullable
            0x01, // dynamic_fields
            0x01, 0x03, 0x74, 0x6d, 0x70, // retired_names: {"tmp"}
        ]
    );
}

#[test]
fn collection_schema_round_trips_through_postcard() {
    let mut schemas = vec![schema(DistanceMetric::L2), evolved_schema()];
    let mut rich = CollectionSchema::new(
        PrimaryKeySpec {
            name: "pk".to_owned(),
            key_type: PrimaryKeyType::Int64,
        },
        vec![
            VectorFieldSpec {
                name: "a".to_owned(),
                dimensions: 768,
                metric: DistanceMetric::Cosine,
            },
            VectorFieldSpec {
                name: "b".to_owned(),
                dimensions: 3,
                metric: DistanceMetric::Dot,
            },
        ],
        vec![
            ScalarFieldSpec::new("flag", FieldType::Bool),
            ScalarFieldSpec::new("price", FieldType::Float64),
            ScalarFieldSpec {
                index: FieldIndex::None,
                nullable: false,
                ..ScalarFieldSpec::new("title", FieldType::String)
            },
            ScalarFieldSpec::new("at", FieldType::Timestamp),
            ScalarFieldSpec::new("tags", FieldType::Array(ElementType::String)),
            ScalarFieldSpec::new("doc", FieldType::Json),
        ],
        false,
    )
    .expect("schema should be valid");
    schemas.push(rich.clone());
    rich.rename_field("title", "name").expect("rename");
    rich.drop_field("b").expect("drop");
    rich.rename_field("pk", "key").expect("rename key");
    schemas.push(rich);

    for schema in schemas {
        let bytes = postcard::to_allocvec(&schema).expect("schema should encode");
        let decoded: CollectionSchema = postcard::from_bytes(&bytes).expect("schema should decode");
        assert_eq!(decoded, schema);
        let payload = WalPayload::SchemaChange(SchemaChangePayload { schema });
        round_trip(&payload);
    }
}

#[test]
fn invalid_stored_schema_fails_to_decode() {
    let mut bytes = WalPayload::SchemaChange(SchemaChangePayload {
        schema: evolved_schema(),
    })
    .encode()
    .expect("payload should encode");
    // next_field_id is the third byte; 2 is not above every field id.
    bytes[2] = 0x02;
    assert!(matches!(
        WalPayload::decode(&bytes),
        Err(PayloadError::Decode(_))
    ));
}

#[test]
fn decode_rejects_trailing_bytes_truncation_and_wrong_kind() {
    let payload = WalPayload::Checkpoint(CheckpointPayload {
        manifest_generation: 1,
        checkpoint_seq_no: 2,
    });
    let mut bytes = payload.encode().expect("payload should encode");
    assert_eq!(
        WalPayload::decode_as(&bytes, PayloadKind::Checkpoint),
        Ok(payload)
    );
    assert_eq!(
        WalPayload::decode_as(&bytes, PayloadKind::WriteBatch),
        Err(PayloadError::KindMismatch {
            expected: PayloadKind::WriteBatch,
            found: PayloadKind::Checkpoint
        })
    );
    bytes.push(0);
    assert_eq!(
        WalPayload::decode(&bytes),
        Err(PayloadError::TrailingBytes { count: 1 })
    );
    assert!(matches!(
        WalPayload::decode(&bytes[..2]),
        Err(PayloadError::Decode(_))
    ));
    assert!(matches!(
        WalPayload::decode(&[0x07]),
        Err(PayloadError::Decode(_))
    ));
}

#[test]
fn f32_bytes_must_hold_whole_floats() {
    // Put op whose vector byte string is 3 bytes long.
    let bytes = [
        0x00, 0x01, 0x01, 0x00, 0x01, 0x01, 0x6b, 0x01, 0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    assert!(matches!(
        WalPayload::decode(&bytes),
        Err(PayloadError::Decode(_))
    ));
    assert_eq!(F32Bytes::from_le_bytes(vec![0; 3]), None);
    let vector = F32Bytes::from_f32s(&[1.5, -2.0, f32::MIN_POSITIVE]);
    assert_eq!(vector.dimensions(), 3);
    assert_eq!(vector.to_f32s(), vec![1.5, -2.0, f32::MIN_POSITIVE]);
    assert_eq!(
        F32Bytes::from_le_bytes(vector.as_bytes().to_vec()),
        Some(vector)
    );
}

#[test]
fn payload_kinds_map_to_frame_types() {
    for kind in [
        PayloadKind::WriteBatch,
        PayloadKind::SchemaChange,
        PayloadKind::Checkpoint,
    ] {
        assert_eq!(PayloadKind::from_frame_type(kind.frame_type()), Some(kind));
    }
    assert_eq!(PayloadKind::WriteBatch.frame_type(), 1);
    assert_eq!(PayloadKind::SchemaChange.frame_type(), 2);
    assert_eq!(PayloadKind::Checkpoint.frame_type(), 3);
    assert_eq!(PayloadKind::from_frame_type(0), None);
    assert_eq!(PayloadKind::from_frame_type(4), None);
}

#[test]
fn wire_pk_mirrors_primary_key() {
    for pk in [PrimaryKey::Int64(-9), PrimaryKey::from("key")] {
        let wire = WirePk::from(pk.clone());
        assert_eq!(wire.key_type(), pk.key_type());
        assert_eq!(PrimaryKey::from(wire), pk);
    }
}

#[test]
fn record_converts_to_row_image_keyed_by_field_id() {
    let schema = schema(DistanceMetric::Cosine);
    let record = Record::new("k")
        .with_vector("v", vec![3.0, 4.0])
        .with_field("n", Value::Int64(5));
    let mut record = record;
    record.extra.insert("x".to_owned(), json!(true));

    let image = RowImage::from_record(&schema, record).expect("record should convert");
    assert_eq!(image.pk, WirePk::String("k".to_owned()));
    assert_eq!(image.vectors.len(), 1);
    assert_eq!(image.vectors[0].0, FieldId(1));
    assert_eq!(image.vectors[0].1.to_f32s(), vec![0.6, 0.8]);
    assert_eq!(
        image.scalars,
        vec![(
            FieldId(2),
            ValueBytes::encode(&Value::Int64(5)).expect("encode")
        )]
    );
    assert_eq!(
        image.dynamic,
        Some(ValueBytes::encode_json(&json!({ "x": true })).expect("encode"))
    );

    let back = image.to_record(&schema).expect("row should read back");
    assert_eq!(back.pk, PrimaryKey::from("k"));
    assert_eq!(back.vectors["v"], vec![0.6, 0.8]);
    assert_eq!(back.fields["n"], Value::Int64(5));
    assert_eq!(back.extra["x"], json!(true));
}

#[test]
fn rename_keeps_values_because_rows_are_keyed_by_id() {
    let mut schema = schema(DistanceMetric::L2);
    let record = Record::new("k")
        .with_vector("v", vec![1.0, 2.0])
        .with_field("n", Value::Int64(5));
    let image = RowImage::from_record(&schema, record).expect("record should convert");
    schema.rename_field("n", "count").expect("rename");
    schema.rename_field("v", "embedding").expect("rename");
    let back = image.to_record(&schema).expect("row should read back");
    assert_eq!(back.fields["count"], Value::Int64(5));
    assert_eq!(back.vectors["embedding"], vec![1.0, 2.0]);
}

#[test]
fn dropped_and_re_added_fields_do_not_resurrect_old_values() {
    let mut schema = schema(DistanceMetric::L2);
    let record = Record::new("k")
        .with_vector("v", vec![1.0, 2.0])
        .with_field("n", Value::Int64(5));
    let image = RowImage::from_record(&schema, record).expect("record should convert");

    schema.drop_field("n").expect("drop");
    let dropped = image.to_record(&schema).expect("row should read back");
    assert!(dropped.fields.is_empty());

    schema
        .add_field(ScalarFieldSpec::new("n", FieldType::String))
        .expect("re-add with another type");
    let re_added = image.to_record(&schema).expect("row should read back");
    assert!(
        re_added.fields.is_empty(),
        "the new field has a new id, so old rows read null"
    );
}

#[test]
fn dynamic_keys_are_shadowed_by_declared_and_retired_names() {
    let mut schema = schema(DistanceMetric::L2);
    let mut record = Record::new("k").with_vector("v", vec![1.0, 2.0]);
    record.extra.insert("color".to_owned(), json!("red"));
    record.extra.insert("size".to_owned(), json!(3));
    let image = RowImage::from_record(&schema, record).expect("record should convert");

    schema
        .add_field(ScalarFieldSpec::new("color", FieldType::String))
        .expect("add");
    let added = image.to_record(&schema).expect("row should read back");
    assert!(
        !added.fields.contains_key("color"),
        "an added field reads null on old rows"
    );
    assert!(!added.extra.contains_key("color"), "and hides the old key");
    assert_eq!(added.extra["size"], json!(3));

    schema.drop_field("color").expect("drop");
    let dropped = image.to_record(&schema).expect("row should read back");
    assert!(
        !dropped.extra.contains_key("color"),
        "a retired name stays shadowed"
    );
    assert_eq!(dropped.extra["size"], json!(3));
}

#[test]
fn conversion_rejects_invalid_records_and_zero_cosine_vectors() {
    let schema = schema(DistanceMetric::Cosine);
    let missing_vector = Record::new("k");
    assert!(matches!(
        RowImage::from_record(&schema, missing_vector),
        Err(RowImageError::InvalidRecord(
            RecordError::MissingVector { .. }
        ))
    ));
    let zero = Record::new("k").with_vector("v", vec![0.0, 0.0]);
    assert_eq!(
        RowImage::from_record(&schema, zero),
        Err(RowImageError::ZeroNormVector {
            field: "v".to_owned()
        })
    );
    let l2 = self::schema(DistanceMetric::L2);
    let zero = Record::new("k").with_vector("v", vec![0.0, 0.0]);
    assert!(
        RowImage::from_record(&l2, zero).is_ok(),
        "zero vectors are fine without cosine"
    );
}

#[test]
fn to_record_rejects_corrupt_rows() {
    let schema = schema(DistanceMetric::L2);
    let good = RowImage::from_record(&schema, Record::new("k").with_vector("v", vec![1.0, 2.0]))
        .expect("record should convert");

    let mut wrong_key = good.clone();
    wrong_key.pk = WirePk::Int64(1);
    assert_eq!(
        wrong_key.to_record(&schema),
        Err(RowImageError::PrimaryKeyType {
            expected: PrimaryKeyType::String,
            found: PrimaryKeyType::Int64
        })
    );

    let mut short = good.clone();
    short.vectors[0].1 = F32Bytes::from_f32s(&[1.0]);
    assert!(matches!(
        short.to_record(&schema),
        Err(RowImageError::VectorDimensions { actual: 1, .. })
    ));

    let mut unsorted = good.clone();
    let five = ValueBytes::encode(&Value::Int64(5)).expect("encode");
    unsorted.scalars = vec![(FieldId(2), five.clone()), (FieldId(2), five)];
    assert_eq!(
        unsorted.to_record(&schema),
        Err(RowImageError::UnsortedFieldIds { field: FieldId(2) })
    );

    let mut wrong_type = good.clone();
    wrong_type.scalars = vec![(
        FieldId(2),
        ValueBytes::encode(&Value::String("x".to_owned())).expect("encode"),
    )];
    assert!(matches!(
        wrong_type.to_record(&schema),
        Err(RowImageError::Decode {
            field: FieldId(2),
            ..
        })
    ));

    let mut not_object = good;
    not_object.dynamic = Some(ValueBytes::encode_json(&json!([1])).expect("encode"));
    assert!(matches!(
        not_object.to_record(&schema),
        Err(RowImageError::Dynamic(_))
    ));
}

/// Small deterministic generator so the property tests need no extra
/// dependency and every failure reproduces from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn string(&mut self) -> String {
        (0..self.below(5))
            .map(|_| ["a", "b", "é", "\u{1f600}"][self.below(4) as usize])
            .collect()
    }

    fn value(&mut self, field_type: FieldType) -> Value {
        match field_type {
            FieldType::Bool => Value::Bool(self.below(2) == 1),
            FieldType::Int64 => Value::Int64(self.next() as i64),
            FieldType::Float64 => Value::Float64((self.below(1000) as f64 - 500.0) / 4.0),
            FieldType::String => Value::String(self.string()),
            FieldType::Timestamp => Value::Timestamp(
                Timestamp::from_micros(self.below(1 << 50) as i64).expect("in range"),
            ),
            FieldType::Array(element) => Value::Array(
                (0..self.below(4))
                    .map(|_| self.value(element.into()))
                    .collect(),
            ),
            FieldType::Json => Value::Json(json!({ "k": self.string(), "n": [1, 2.5, null] })),
        }
    }
}

fn property_schema() -> CollectionSchema {
    CollectionSchema::new(
        PrimaryKeySpec {
            name: "id".to_owned(),
            key_type: PrimaryKeyType::Int64,
        },
        vec![
            VectorFieldSpec {
                name: "a".to_owned(),
                dimensions: 4,
                metric: DistanceMetric::L2,
            },
            VectorFieldSpec {
                name: "b".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Dot,
            },
        ],
        vec![
            ScalarFieldSpec::new("flag", FieldType::Bool),
            ScalarFieldSpec::new("count", FieldType::Int64),
            ScalarFieldSpec::new("price", FieldType::Float64),
            ScalarFieldSpec::new("title", FieldType::String),
            ScalarFieldSpec::new("at", FieldType::Timestamp),
            ScalarFieldSpec::new("tags", FieldType::Array(ElementType::String)),
            ScalarFieldSpec::new("nums", FieldType::Array(ElementType::Int64)),
            ScalarFieldSpec::new("doc", FieldType::Json),
        ],
        true,
    )
    .expect("schema should be valid")
}

/// Random records survive record -> row image -> postcard -> row image ->
/// record unchanged (non-cosine metrics, so vectors are not normalized).
#[test]
fn random_records_round_trip_through_row_images_and_payloads() {
    let schema = property_schema();
    let mut rng = Rng(0x5eed_0101);
    for case in 0..500 {
        let mut ops = Vec::new();
        let mut records = Vec::new();
        for index in 0..=rng.below(4) {
            let mut record = Record::new(case * 10 + index as i64)
                .with_vector("a", (0..4).map(|_| rng.below(100) as f32 / 8.0).collect())
                .with_vector("b", vec![rng.below(9) as f32, -1.5]);
            for field in schema.fields() {
                if rng.below(3) != 0 {
                    let value = rng.value(field.field_type);
                    record.fields.insert(field.name.clone(), value);
                }
            }
            for _ in 0..rng.below(3) {
                let key = format!("extra_{}", rng.below(5));
                record.extra.insert(key, json!({ "v": rng.string() }));
            }
            let record = schema
                .validate_record(record)
                .expect("record should be valid");
            let image = RowImage::from_record(&schema, record.clone()).expect("convert");
            assert_eq!(
                image.to_record(&schema).as_ref(),
                Ok(&record),
                "case {case}"
            );
            records.push(record);
            ops.push(RowOp::Put(image));
            if rng.below(4) == 0 {
                ops.push(RowOp::Delete(WirePk::Int64(rng.next() as i64)));
            }
        }
        let payload = WalPayload::WriteBatch(WriteBatchPayload {
            schema_version: schema.schema_version(),
            ops,
        });
        let bytes = round_trip(&payload);
        assert_eq!(
            payload.encode().expect("payload should encode"),
            bytes,
            "encoding is deterministic"
        );
        let WalPayload::WriteBatch(decoded) =
            WalPayload::decode_as(&bytes, PayloadKind::WriteBatch).expect("decode")
        else {
            unreachable!("decode_as checked the kind");
        };
        let puts: Vec<Record> = decoded
            .ops
            .iter()
            .filter_map(|op| match op {
                RowOp::Put(image) => Some(image.to_record(&schema).expect("read back")),
                RowOp::Delete(_) => None,
            })
            .collect();
        assert_eq!(puts, records, "case {case}");
    }
}

/// Truncated or corrupted payload bytes decode to an error, never a panic.
#[test]
fn mutated_payloads_never_panic() {
    let schema = property_schema();
    let mut rng = Rng(0x5eed_0102);
    let record = schema
        .validate_record(
            Record::new(1)
                .with_vector("a", vec![1.0, 2.0, 3.0, 4.0])
                .with_vector("b", vec![1.0, 2.0])
                .with_field("title", Value::String("t".to_owned())),
        )
        .expect("record should be valid");
    let image = RowImage::from_record(&schema, record).expect("convert");
    let payloads = [
        WalPayload::WriteBatch(WriteBatchPayload {
            schema_version: 1,
            ops: vec![RowOp::Put(image), RowOp::Delete(WirePk::Int64(3))],
        }),
        WalPayload::SchemaChange(SchemaChangePayload {
            schema: schema.clone(),
        }),
    ];
    for payload in &payloads {
        let bytes = payload.encode().expect("payload should encode");
        for _ in 0..2_000 {
            let mut mutated = bytes.clone();
            let index = rng.below(mutated.len() as u64) as usize;
            if rng.below(2) == 0 {
                mutated[index] = rng.next() as u8;
            } else {
                mutated.truncate(index);
            }
            if let Ok(WalPayload::WriteBatch(batch)) = WalPayload::decode(&mutated) {
                for op in batch.ops {
                    if let RowOp::Put(image) = op {
                        let _ = image.to_record(&schema);
                    }
                }
            }
        }
    }
}
