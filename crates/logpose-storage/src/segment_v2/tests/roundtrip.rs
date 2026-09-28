//! Round trips of random and hand-built segments, and builder validation.

use super::{Patcher, fixture, open_verified, random, rows_of};
use crate::segment_v2::{
    ColumnEncoding, FileSource, IndexSection, IndexSectionKind, MemorySource, SectionKind,
    SegmentBuilder, SegmentError, SegmentIdentity, SegmentReader, StatValue, format::SECTION_ALIGN,
};
use crate::test_support::unique_temp_dir;
use logpose_types::{
    CollectionId, DistanceMetric,
    record::PrimaryKey,
    schema::{
        CollectionSchema, ElementType, FieldId, FieldType, PrimaryKeySpec, PrimaryKeyType,
        ScalarFieldSpec, VectorFieldSpec,
    },
    value::{Value, codec},
};
use logpose_wal::codec::RowImage;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::json;
use std::sync::Arc;

fn identity() -> SegmentIdentity {
    SegmentIdentity {
        collection_id: CollectionId::default(),
        unit_id: 7,
    }
}

fn build(schema: &Arc<CollectionSchema>, rows: &[(u64, RowImage)]) -> Vec<u8> {
    let mut builder = SegmentBuilder::new(Arc::clone(schema), identity()).expect("builder");
    for (seq_no, image) in rows {
        builder
            .push_row_image(*seq_no, image)
            .expect("row accepted");
    }
    builder.finish_to_vec().expect("segment builds").0
}

/// Build a random segment for `seed`: rows plus opaque index sections.
pub(super) fn random_segment(
    seed: u64,
    max_rows: usize,
) -> (
    Arc<CollectionSchema>,
    Vec<(u64, RowImage)>,
    Vec<IndexSection>,
    Vec<u8>,
) {
    let mut rng = StdRng::seed_from_u64(seed);
    let schema = Arc::new(random::schema(&mut rng));
    let count = rng.random_range(0..=max_rows);
    let keys = random::keys(&mut rng, schema.primary_key_type(), count);
    let mut seq = rng.random_range(1..1_000_u64);
    let wide = rng.random_bool(0.2);
    let rows: Vec<(u64, RowImage)> = keys
        .into_iter()
        .map(|pk| {
            seq += if wide && rng.random_bool(0.1) {
                u64::from(u32::MAX) + 1
            } else {
                rng.random_range(1..5)
            };
            (seq, random::row(&mut rng, &schema, pk))
        })
        .collect();
    let mut sections = Vec::new();
    for field in schema.vectors() {
        if rng.random_bool(0.5) {
            sections.push(IndexSection {
                kind: IndexSectionKind::VectorGraph,
                field: field.id,
                encoding: 3,
                aux32: field.dimensions,
                aux64: count as u64,
                payload: (0..rng.random_range(0..200))
                    .map(|_| rng.random())
                    .collect(),
            });
        }
    }
    for field in schema.fields() {
        if rng.random_bool(0.3) {
            sections.push(IndexSection {
                kind: IndexSectionKind::ScalarSorted,
                field: field.id,
                encoding: 1,
                aux32: 0,
                aux64: 0,
                payload: vec![rng.random(); rng.random_range(0..70)],
            });
        }
    }
    let mut builder = SegmentBuilder::new(Arc::clone(&schema), identity()).expect("builder");
    for (seq_no, image) in &rows {
        builder
            .push_row_image(*seq_no, image)
            .expect("row accepted");
    }
    for section in &sections {
        builder
            .add_index_section(section.clone())
            .expect("index section accepted");
    }
    let bytes = builder.finish_to_vec().expect("segment builds").0;
    (schema, rows, sections, bytes)
}

#[test]
fn random_segments_round_trip_rows_keys_stats_and_index_sections() {
    for seed in 0..48 {
        let (schema, rows, sections, bytes) = random_segment(seed, 160);
        let reader = open_verified(&bytes);
        assert_eq!(reader.schema().as_ref(), schema.as_ref(), "seed {seed}");
        assert_eq!(reader.row_count() as usize, rows.len(), "seed {seed}");
        assert_eq!(rows_of(&reader), rows, "seed {seed}");

        for (row, (_, image)) in rows.iter().enumerate() {
            let pk = PrimaryKey::from(image.pk.clone());
            assert_eq!(
                reader.find_row(&pk).expect("lookup"),
                Some(u32::try_from(row).expect("row fits")),
                "seed {seed} row {row}"
            );
        }
        let absent = match schema.primary_key_type() {
            PrimaryKeyType::Int64 => PrimaryKey::Int64(4_000_000_000),
            PrimaryKeyType::String => PrimaryKey::from("absent key!"),
        };
        assert_eq!(reader.find_row(&absent).expect("lookup"), None);

        for section in &sections {
            let (entry, payload) = reader
                .index_section(section.kind, section.field)
                .expect("index section reads")
                .expect("index section exists");
            assert_eq!(payload, section.payload);
            assert_eq!(
                (entry.encoding, entry.aux32, entry.aux64),
                (section.encoding, section.aux32, section.aux64)
            );
        }

        check_stats(&reader, &schema, &rows);
        check_lazy_accessors(&reader, &schema, &rows);
    }
}

fn check_stats(
    reader: &SegmentReader<MemorySource>,
    schema: &CollectionSchema,
    rows: &[(u64, RowImage)],
) {
    let stats = reader.stats().expect("stats");
    assert_eq!(stats.row_count as usize, rows.len());
    assert_eq!(
        stats.min_seq_no,
        rows.iter().map(|(seq, _)| *seq).min().unwrap_or(0)
    );
    assert_eq!(
        stats.max_seq_no,
        rows.iter().map(|(seq, _)| *seq).max().unwrap_or(0)
    );
    assert_eq!(
        stats.dynamic_rows as usize,
        rows.iter()
            .filter(|(_, image)| image.dynamic.is_some())
            .count()
    );
    for field in schema.fields() {
        let values: Vec<Value> = rows
            .iter()
            .filter_map(|(_, image)| {
                image
                    .scalars
                    .iter()
                    .find(|(id, _)| *id == field.id)
                    .map(|(_, bytes)| bytes.decode(field.field_type).expect("decodes"))
            })
            .collect();
        let field_stats = stats.field(field.id).expect("field stats");
        assert_eq!(field_stats.null_count as usize, rows.len() - values.len());
        if field.field_type == FieldType::Int64 {
            let ints: Vec<i64> = values
                .iter()
                .filter_map(|value| match value {
                    Value::Int64(value) => Some(*value),
                    _ => None,
                })
                .collect();
            assert_eq!(ints.len(), values.len());
            assert_eq!(
                field_stats.min,
                ints.iter().min().copied().map(StatValue::Int64)
            );
            assert_eq!(
                field_stats.max,
                ints.iter().max().copied().map(StatValue::Int64)
            );
            let histogram_total: u64 = field_stats
                .histogram
                .iter()
                .map(|bucket| bucket.count)
                .sum();
            assert_eq!(histogram_total, ints.len() as u64);
            assert!(field_stats.histogram.len() <= 32);
            assert!(field_stats.top.len() <= 16);
        }
    }
    for field in schema.vectors() {
        let non_null = rows
            .iter()
            .filter(|(_, image)| image.vectors.iter().any(|(id, _)| *id == field.id))
            .count();
        assert_eq!(
            stats.field(field.id).expect("vector stats").value_count,
            non_null as u64
        );
    }
}

fn check_lazy_accessors(
    reader: &SegmentReader<MemorySource>,
    schema: &CollectionSchema,
    rows: &[(u64, RowImage)],
) {
    for field in schema.vectors() {
        let handle = reader
            .vector(field.id)
            .expect("vector section")
            .expect("present");
        assert_eq!(handle.prefix().dim(), field.dimensions);
        for (row, (_, image)) in rows.iter().enumerate().step_by(7) {
            let expected = image
                .vectors
                .iter()
                .find(|(id, _)| *id == field.id)
                .map(|(_, vector)| vector.to_f32s());
            let row = u32::try_from(row).expect("row fits");
            assert_eq!(
                reader.vector_row(&handle, row).expect("vector row"),
                expected
            );
        }
    }
    for field in schema.fields() {
        let column = reader
            .scalar_column(field.id)
            .expect("column")
            .expect("present");
        for (row, (_, image)) in rows.iter().enumerate().step_by(5) {
            let expected = image
                .scalars
                .iter()
                .find(|(id, _)| *id == field.id)
                .map_or(Value::Null, |(_, bytes)| {
                    bytes.decode(field.field_type).expect("decodes")
                });
            assert_eq!(column.value(row).expect("value"), expected);
        }
    }
}

#[test]
fn large_segment_spans_many_pages_and_dynamic_blocks() {
    let schema = Arc::new(
        CollectionSchema::new(
            PrimaryKeySpec {
                name: "id".to_owned(),
                key_type: PrimaryKeyType::Int64,
            },
            vec![
                VectorFieldSpec {
                    name: "tiny".to_owned(),
                    dimensions: 1,
                    metric: DistanceMetric::Dot,
                },
                VectorFieldSpec {
                    name: "wide".to_owned(),
                    dimensions: 700,
                    metric: DistanceMetric::L2,
                },
            ],
            vec![ScalarFieldSpec::new("label", FieldType::String)],
            true,
        )
        .expect("schema"),
    );
    let tiny = schema.vector_field("tiny").expect("tiny").id;
    let wide = schema.vector_field("wide").expect("wide").id;
    let label = schema.scalar_field("label").expect("label").id;
    let mut builder = SegmentBuilder::new(Arc::clone(&schema), identity()).expect("builder");
    let rows = 9_000_u32;
    for row in 0..rows {
        let mut writer = builder
            .push_row(u64::from(row) + 1, &PrimaryKey::Int64(i64::from(row) * 3))
            .expect("row");
        writer
            .vector(tiny, &[row as f32])
            .expect("tiny")
            .scalar(label, &Value::String(format!("label-{}", row % 10)))
            .expect("label");
        if row % 64 == 0 {
            let wide_vector: Vec<f32> = (0..700).map(|index| (row + index) as f32).collect();
            writer.vector(wide, &wide_vector).expect("wide");
        }
        if row % 3 == 0 {
            let object = codec::encode_json(&json!({"row": row})).expect("encodes");
            writer.dynamic(&object).expect("dynamic");
        }
    }
    let (bytes, written) = builder.finish_to_vec().expect("builds");
    assert_eq!(written.file_len, bytes.len() as u64);
    let reader = open_verified(&bytes);
    assert_eq!(written.sections, reader.sections());

    let tiny_handle = reader.vector(tiny).expect("tiny").expect("present");
    assert_eq!(tiny_handle.prefix().page_rows(), 2048);
    assert_eq!(tiny_handle.prefix().page_count(), 5);
    assert_eq!(
        reader.vector_row(&tiny_handle, 8_999).expect("row"),
        Some(vec![8_999.0])
    );
    let wide_handle = reader.vector(wide).expect("wide").expect("present");
    assert_eq!(wide_handle.prefix().page_rows(), 2);
    assert_eq!(wide_handle.prefix().page_count(), 4_500);
    assert_eq!(
        wide_handle.prefix().nulls().len(),
        u64::from(rows - rows.div_ceil(64))
    );
    assert_eq!(reader.vector_row(&wide_handle, 65).expect("row"), None);
    let expected: Vec<f32> = (0..700).map(|index| (128 + index) as f32).collect();
    assert_eq!(
        reader.vector_row(&wide_handle, 128).expect("row"),
        Some(expected)
    );

    let dynamic = reader.dynamic().expect("dynamic").expect("present");
    assert_eq!(dynamic.blocks().block_count(), 3);
    let block = reader.dynamic_block(&dynamic, 2).expect("block");
    assert_eq!(block.rows(), 8_192..9_000);
    assert!(block.raw(8_194).is_none());
    assert_eq!(
        codec::decode_json(block.raw(8_196).expect("value")).expect("decodes"),
        json!({"row": 8_196})
    );

    let label_index = reader
        .find_section(SectionKind::ScalarColumn, Some(label))
        .expect("label column");
    assert_eq!(
        reader.sections()[label_index].encoding,
        u16::from(ColumnEncoding::StringDict.code())
    );
    assert_eq!(
        reader
            .find_row(&PrimaryKey::Int64(3 * 4_321))
            .expect("find"),
        Some(4_321)
    );
    assert_eq!(
        reader
            .find_row(&PrimaryKey::Int64(3 * 4_321 + 1))
            .expect("find"),
        None
    );
    assert_eq!(rows_of(&reader).len(), rows as usize);
}

#[test]
fn file_source_reads_what_the_builder_wrote() {
    let (_, rows, _, bytes) = random_segment(99, 200);
    let dir = unique_temp_dir("segment-v2");
    let path = dir.path().join("00000007.seg");
    std::fs::write(&path, &bytes).expect("segment written");
    let reader = SegmentReader::open(FileSource::open(&path).expect("file opens")).expect("opens");
    reader.verify().expect("verifies");
    let read: Vec<(u64, RowImage)> = reader
        .read_rows()
        .expect("rows")
        .into_iter()
        .map(|row| (row.seq_no, row.image))
        .collect();
    assert_eq!(read, rows);
}

/// `for_each_row` visits exactly the rows it is asked for, in row order, each equal to what
/// `read_rows` returns for it, and hands back the first error the visitor returns.
#[test]
fn for_each_row_visits_the_wanted_rows_in_order_and_stops_at_a_visit_error() {
    let (_, _, _, bytes) = random_segment(17, 120);
    let reader = open_verified(&bytes);
    let all = reader.read_rows().expect("rows");
    let mut visited = Vec::new();
    reader
        .for_each_row(
            |row| row % 3 != 0,
            |row, stored| {
                visited.push((row, stored));
                Ok::<_, SegmentError>(())
            },
        )
        .expect("visit");
    let expected = (0_u32..)
        .zip(all)
        .filter(|(row, _)| row % 3 != 0)
        .collect::<Vec<_>>();
    assert_eq!(visited, expected);

    let mut seen = 0;
    let stopped = reader.for_each_row(
        |_| true,
        |row, _| {
            seen += 1;
            if row == 4 {
                Err(SegmentError::Encode("stop".to_owned()))
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(stopped, Err(SegmentError::Encode(message)) if message == "stop"));
    assert_eq!(seen, 5, "nothing is visited after the error");
}

#[test]
fn every_section_is_64_byte_aligned_and_sections_are_ordered() {
    let bytes = fixture::golden_bytes();
    let reader = open_verified(&bytes);
    let kinds: Vec<u16> = reader.sections().iter().map(|entry| entry.kind).collect();
    assert_eq!(&kinds[..6], &[1, 2, 3, 4, 5, 6]);
    for entry in reader.sections() {
        assert_eq!(entry.offset % SECTION_ALIGN, 0);
    }
    assert_eq!(reader.footer().table_offset % SECTION_ALIGN, 0);
}

#[test]
fn empty_segment_round_trips() {
    let mut rng = StdRng::seed_from_u64(5);
    let schema = Arc::new(random::schema(&mut rng));
    let bytes = build(&schema, &[]);
    let reader = open_verified(&bytes);
    assert_eq!(reader.row_count(), 0);
    assert!(rows_of(&reader).is_empty());
    assert_eq!(
        (reader.header().min_seq_no, reader.header().max_seq_no),
        (0, 0)
    );
    assert_eq!(reader.find_row(&PrimaryKey::Int64(1)).expect("find"), None);
    assert!(reader.dynamic().expect("dynamic").is_none());
}

#[test]
fn row_meta_switches_to_u64_when_deltas_do_not_fit() {
    let mut rng = StdRng::seed_from_u64(11);
    let schema = Arc::new(random::schema(&mut rng));
    let keys = random::keys(&mut rng, schema.primary_key_type(), 3);
    let rows: Vec<(u64, RowImage)> = keys
        .into_iter()
        .zip([5, 5 + u64::from(u32::MAX), u64::MAX])
        .map(|(pk, seq)| (seq, random::row(&mut rng, &schema, pk)))
        .collect();
    let reader = open_verified(&build(&schema, &rows));
    let index = reader
        .find_section(SectionKind::RowMeta, None)
        .expect("row meta");
    assert_eq!(reader.sections()[index].encoding, 1);
    assert_eq!(
        reader.row_meta().expect("row meta"),
        vec![5, 5 + u64::from(u32::MAX), u64::MAX]
    );

    let narrow: Vec<(u64, RowImage)> = rows
        .into_iter()
        .enumerate()
        .map(|(index, (_, image))| (100 + index as u64, image))
        .collect();
    let reader = open_verified(&build(&schema, &narrow));
    let index = reader
        .find_section(SectionKind::RowMeta, None)
        .expect("row meta");
    assert_eq!(
        (
            reader.sections()[index].encoding,
            reader.sections()[index].aux64
        ),
        (2, 100)
    );
}

#[test]
fn string_columns_choose_dictionary_or_plain_by_cardinality() {
    let schema = Arc::new(fixture::golden_schema());
    let reader = open_verified(&fixture::golden_bytes());
    let encoding_of = |name: &str| {
        let field = schema.scalar_field(name).expect("declared").id;
        let index = reader
            .find_section(SectionKind::ScalarColumn, Some(field))
            .expect("column exists");
        ColumnEncoding::from_code(u8::try_from(reader.sections()[index].encoding).expect("code"))
    };
    assert_eq!(encoding_of("color"), Some(ColumnEncoding::StringDict));
    assert_eq!(encoding_of("title"), Some(ColumnEncoding::StringPlain));
    assert_eq!(encoding_of("tags"), Some(ColumnEncoding::Array));
    assert_eq!(encoding_of("details"), Some(ColumnEncoding::JsonValue));
    assert_eq!(encoding_of("in_stock"), Some(ColumnEncoding::BoolBitmap));
    assert_eq!(encoding_of("added_at"), Some(ColumnEncoding::Int64Plain));
    assert_eq!(encoding_of("price"), Some(ColumnEncoding::Float64Plain));
}

#[test]
fn pk_filter_has_no_false_negatives_and_few_false_positives() {
    let schema = Arc::new(
        CollectionSchema::new(
            PrimaryKeySpec {
                name: "id".to_owned(),
                key_type: PrimaryKeyType::Int64,
            },
            vec![VectorFieldSpec {
                name: "v".to_owned(),
                dimensions: 1,
                metric: DistanceMetric::Dot,
            }],
            Vec::new(),
            false,
        )
        .expect("schema"),
    );
    let mut builder = SegmentBuilder::new(Arc::clone(&schema), identity()).expect("builder");
    for key in 0..20_000_i64 {
        builder
            .push_row(1, &PrimaryKey::Int64(key * 2))
            .expect("row");
    }
    let reader = open_verified(&builder.finish_to_vec().expect("builds").0);
    let filter = reader.pk_filter().expect("filter");
    assert_eq!(filter.key_count(), 20_000);
    for key in 0..20_000_i64 {
        assert!(filter.may_contain(&PrimaryKey::Int64(key * 2)));
    }
    let false_positives = (0..20_000_i64)
        .filter(|key| filter.may_contain(&PrimaryKey::Int64(key * 2 + 1)))
        .count();
    assert!(
        false_positives < 200,
        "{false_positives} false positives in 20000"
    );
}

#[test]
fn segments_without_a_fields_section_read_it_as_absent() {
    let mut schema = fixture::golden_schema();
    let reader = open_verified(&fixture::golden_bytes());
    let added = schema
        .add_field(ScalarFieldSpec::new("added_later", FieldType::Bool))
        .expect("add field");
    assert!(reader.scalar_column(added).expect("lookup").is_none());
    assert!(reader.vector(FieldId(9_999)).expect("lookup").is_none());
    assert!(
        reader
            .index_section(IndexSectionKind::VectorGraph, FieldId(1))
            .expect("lookup")
            .is_none()
    );
}

#[test]
fn builder_rejects_invalid_input() {
    let schema = Arc::new(fixture::golden_schema());
    let field = |name: &str| {
        schema
            .field(name)
            .map(|field| field.id())
            .expect("declared")
    };
    let (embedding, count, tags, price) = (
        field("embedding"),
        field("count"),
        field("tags"),
        field("price"),
    );
    let mut builder = SegmentBuilder::new(Arc::clone(&schema), identity()).expect("builder");

    assert!(matches!(
        builder.push_row(0, &PrimaryKey::from("a")),
        Err(SegmentError::InvalidSeqNo)
    ));
    assert!(matches!(
        builder.push_row(1, &PrimaryKey::Int64(1)),
        Err(SegmentError::PrimaryKeyType { .. })
    ));
    let mut row = builder.push_row(1, &PrimaryKey::from("a")).expect("row");
    assert!(matches!(
        row.vector(embedding, &[1.0]),
        Err(SegmentError::VectorDimensions { .. })
    ));
    assert!(matches!(
        row.vector(count, &[1.0]),
        Err(SegmentError::FieldKind { .. })
    ));
    assert!(matches!(
        row.scalar(embedding, &Value::Int64(1)),
        Err(SegmentError::FieldKind { .. })
    ));
    assert!(matches!(
        row.scalar(FieldId(500), &Value::Int64(1)),
        Err(SegmentError::UnknownField { .. })
    ));
    assert!(matches!(
        row.scalar(count, &Value::Bool(true)),
        Err(SegmentError::ValueType { .. })
    ));
    assert!(matches!(
        row.scalar(price, &Value::Float64(f64::NAN)),
        Err(SegmentError::ValueType { .. })
    ));
    assert!(matches!(
        row.scalar(tags, &Value::Array(vec![Value::Null])),
        Err(SegmentError::ValueType { .. })
    ));
    assert!(matches!(
        row.scalar(tags, &Value::Array(vec![Value::Int64(1)])),
        Err(SegmentError::ValueType { .. })
    ));
    row.scalar(count, &Value::Int64(3)).expect("first set");
    assert!(matches!(
        row.scalar(count, &Value::Int64(4)),
        Err(SegmentError::FieldAlreadySet { .. })
    ));
    row.vector(embedding, &[1.0, 0.0, 0.0, 0.0])
        .expect("first set");
    assert!(matches!(
        row.vector(embedding, &[1.0, 0.0, 0.0, 0.0]),
        Err(SegmentError::FieldAlreadySet { .. })
    ));
    assert!(matches!(
        row.dynamic(&[0x13, 0x02]),
        Err(SegmentError::InvalidDynamic(_))
    ));
    assert!(matches!(
        row.dynamic(&[0xff]),
        Err(SegmentError::InvalidDynamic(_))
    ));
    let object = codec::encode_json(&json!({"k": 1})).expect("encodes");
    row.dynamic(&object).expect("first set");
    assert!(matches!(
        row.dynamic(&object),
        Err(SegmentError::DynamicAlreadySet)
    ));

    let sq8 = |field: FieldId| IndexSection {
        kind: IndexSectionKind::VectorSq8,
        field,
        encoding: 1,
        aux32: 0,
        aux64: 0,
        payload: Vec::new(),
    };
    assert!(matches!(
        builder.add_index_section(sq8(count)),
        Err(SegmentError::FieldKind { .. })
    ));
    assert!(matches!(
        builder.add_index_section(sq8(FieldId(500))),
        Err(SegmentError::UnknownField { .. })
    ));
    builder
        .add_index_section(sq8(embedding))
        .expect("first index section");
    assert!(matches!(
        builder.add_index_section(sq8(embedding)),
        Err(SegmentError::DuplicateIndexSection { .. })
    ));

    builder.push_row(2, &PrimaryKey::from("a")).expect("row");
    assert!(matches!(
        builder.finish_to_vec(),
        Err(SegmentError::DuplicatePrimaryKey { .. })
    ));
}

#[test]
fn push_row_image_is_all_or_nothing_and_skips_dropped_fields() {
    let mut old = fixture::golden_schema();
    let rows = fixture::golden_rows(&old);
    old.drop_field("color").expect("drop color");
    let color_dropped = Arc::new(old);
    let mut builder = SegmentBuilder::new(Arc::clone(&color_dropped), identity()).expect("builder");
    for (seq_no, image) in &rows {
        builder
            .push_row_image(*seq_no, image)
            .expect("row accepted");
    }
    let mut bad = rows[0].1.clone();
    bad.pk = logpose_wal::codec::WirePk::String("fresh".to_owned());
    bad.vectors[0].1 = logpose_wal::codec::F32Bytes::from_f32s(&[1.0]);
    assert!(matches!(
        builder.push_row_image(99, &bad),
        Err(SegmentError::VectorDimensions { .. })
    ));
    assert_eq!(builder.row_count(), 5);
    let reader = open_verified(&builder.finish_to_vec().expect("builds").0);
    let color = fixture::golden_schema()
        .scalar_field("color")
        .expect("declared")
        .id;
    assert!(reader.scalar_column(color).expect("lookup").is_none());
    for (read, (_, original)) in rows_of(&reader).iter().zip(&rows) {
        let mut expected = original.clone();
        expected.scalars.retain(|(field, _)| *field != color);
        assert_eq!(read.1, expected);
    }
    assert!(reader.schema().field_by_id(color).is_none());
}

#[test]
fn unknown_section_kinds_are_ignored() {
    let bytes = fixture::golden_bytes();
    let mut patcher = Patcher::new(&bytes);
    let index = patcher.find(SectionKind::ScalarInverted.code());
    let at = patcher.entry_at(index);
    patcher.set_u16(at, 999).seal_table();
    let reader = open_verified(&patcher.bytes);
    assert!(
        reader
            .index_section(
                IndexSectionKind::ScalarInverted,
                fixture::golden_schema()
                    .scalar_field("color")
                    .expect("color")
                    .id
            )
            .expect("lookup")
            .is_none()
    );
    assert_eq!(reader.sections()[index].kind, 999);
    assert_eq!(rows_of(&reader).len(), 5);
}

#[test]
fn array_of_every_element_type_round_trips() {
    let schema = Arc::new(
        CollectionSchema::new(
            PrimaryKeySpec {
                name: "id".to_owned(),
                key_type: PrimaryKeyType::Int64,
            },
            vec![VectorFieldSpec {
                name: "v".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::L2,
            }],
            [
                ElementType::Bool,
                ElementType::Int64,
                ElementType::Float64,
                ElementType::String,
                ElementType::Timestamp,
            ]
            .iter()
            .enumerate()
            .map(|(index, element)| {
                ScalarFieldSpec::new(format!("a{index}"), FieldType::Array(*element))
            })
            .collect(),
            false,
        )
        .expect("schema"),
    );
    let mut rng = StdRng::seed_from_u64(3);
    let keys = random::keys(&mut rng, PrimaryKeyType::Int64, 300);
    let rows: Vec<(u64, RowImage)> = keys
        .into_iter()
        .enumerate()
        .map(|(index, pk)| (index as u64 + 1, random::row(&mut rng, &schema, pk)))
        .collect();
    let reader = open_verified(&build(&schema, &rows));
    assert_eq!(rows_of(&reader), rows);
}

#[test]
fn find_row_finds_extreme_keys_and_sorted_order_matches_primary_key_order() {
    let string_keys = [
        "z",
        "",
        "\u{e9}",
        "a\u{0}",
        "A",
        "\u{10ffff}",
        "a",
        "\u{ff}",
        "ab",
    ]
    .map(|key| PrimaryKey::String(key.to_owned()));
    let int_keys = [7, i64::MIN, -1, i64::MAX, 0, 1, i64::MIN + 1].map(PrimaryKey::Int64);
    let string_absent = ["b", "\u{e8}", "a\u{1}"].map(|key| PrimaryKey::String(key.to_owned()));
    let int_absent = [2, i64::MAX - 1, -2].map(PrimaryKey::Int64);
    for (key_type, keys, absent) in [
        (PrimaryKeyType::String, &string_keys[..], &string_absent[..]),
        (PrimaryKeyType::Int64, &int_keys[..], &int_absent[..]),
    ] {
        let schema = Arc::new(
            CollectionSchema::new(
                PrimaryKeySpec {
                    name: "id".to_owned(),
                    key_type,
                },
                vec![VectorFieldSpec {
                    name: "v".to_owned(),
                    dimensions: 1,
                    metric: DistanceMetric::Dot,
                }],
                Vec::new(),
                false,
            )
            .expect("schema"),
        );
        let mut builder = SegmentBuilder::new(Arc::clone(&schema), identity()).expect("builder");
        for key in keys {
            builder.push_row(1, key).expect("row");
        }
        let reader = open_verified(&builder.finish_to_vec().expect("builds").0);
        let filter = reader.pk_filter().expect("filter");
        for (row, key) in keys.iter().enumerate() {
            assert!(filter.may_contain(key), "filter rejects {key:?}");
            let row = u32::try_from(row).expect("row fits");
            assert_eq!(reader.find_row(key).expect("lookup"), Some(row), "{key:?}");
        }
        for key in absent {
            assert_eq!(reader.find_row(key).expect("lookup"), None, "{key:?}");
        }
        // Byte order of the sorted section is `PrimaryKey`'s order.
        let mut expected: Vec<u32> = (0..u32::try_from(keys.len()).expect("fits")).collect();
        expected.sort_by(|left, right| keys[*left as usize].cmp(&keys[*right as usize]));
        assert_eq!(reader.pk_sorted().expect("sorted").rows(), &expected[..]);
    }
}
