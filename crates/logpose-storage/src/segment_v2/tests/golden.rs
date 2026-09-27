//! The committed golden segment pins the byte layout.
//!
//! If `golden_file_matches_the_encoder` fails, the encoder's output changed.
//! That is a format change: bump `FORMAT_VERSION`, update the layout in
//! `docs/src/engine-core-design.md`, and regenerate the file with
//! `LOGPOSE_UPDATE_GOLDEN=1 cargo test -p logpose-storage golden`.

use super::{fixture, open_verified, rows_of};
use crate::segment_v2::{
    FORMAT_VERSION, IndexSectionKind, MemorySource, SectionKind, SegmentReader, StatValue,
    format::SEGMENT_MAGIC,
};
use std::path::PathBuf;
use twox_hash::XxHash3_64;

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/segment_v2/golden.seg")
}

fn committed() -> Vec<u8> {
    std::fs::read(golden_path()).expect("the golden segment is committed under testdata/segment_v2")
}

#[test]
fn golden_file_matches_the_encoder() {
    let encoded = fixture::golden_bytes();
    if std::env::var_os("LOGPOSE_UPDATE_GOLDEN").is_some() {
        std::fs::write(golden_path(), &encoded).expect("golden segment written");
    }
    let committed = committed();
    if encoded != committed {
        let first_difference = encoded
            .iter()
            .zip(&committed)
            .position(|(left, right)| left != right)
            .unwrap_or(encoded.len().min(committed.len()));
        unreachable!(
            "SEGMENT V2 ENCODING CHANGED: the encoder now writes {} bytes, the committed golden \
             file has {} bytes, first difference at byte {first_difference}. Any change to the \
             bytes is a format change: bump FORMAT_VERSION, update the segment layout in \
             docs/src/engine-core-design.md, and regenerate with LOGPOSE_UPDATE_GOLDEN=1.",
            encoded.len(),
            committed.len(),
        );
    }
}

#[test]
fn golden_file_decodes_to_the_fixture_rows() {
    let committed = committed();
    let reader = open_verified(&committed);
    let schema = fixture::golden_schema();
    assert_eq!(&committed[..8], &SEGMENT_MAGIC);
    assert_eq!(FORMAT_VERSION, 2);
    let header = reader.header();
    assert_eq!(header.collection_id, fixture::golden_collection());
    assert_eq!(header.unit_id, fixture::GOLDEN_UNIT);
    assert_eq!(header.row_count, 5);
    assert_eq!((header.min_seq_no, header.max_seq_no), (10, 20));
    assert_eq!(header.schema_version, schema.schema_version());
    assert_eq!(reader.schema().as_ref(), &schema);
    let snapshot = reader
        .read_section(
            reader
                .find_section(SectionKind::SchemaSnapshot, None)
                .expect("snapshot"),
        )
        .expect("snapshot reads");
    assert_eq!(header.schema_hash, XxHash3_64::oneshot(&snapshot));

    assert_eq!(rows_of(&reader), fixture::golden_rows(&schema));
    for row in 0..5 {
        let pk = fixture::golden_pk(row);
        assert_eq!(reader.find_row(&pk).expect("lookup"), Some(row as u32));
    }
    for section in fixture::golden_index_sections(&schema) {
        let (_, payload) = reader
            .index_section(section.kind, section.field)
            .expect("reads")
            .expect("present");
        assert_eq!(payload, section.payload);
    }
    let embedding = schema.vector_field("embedding").expect("embedding").id;
    assert!(
        reader
            .index_section(IndexSectionKind::VectorGraph, embedding)
            .expect("lookup")
            .is_none()
    );

    let stats = reader.stats().expect("stats");
    assert_eq!(stats.pk_min, Some(StatValue::String("A-1".to_owned())));
    assert_eq!(stats.pk_max, Some(StatValue::String("E-5".to_owned())));
    assert_eq!(stats.dynamic_rows, 3);
    let count = stats
        .field(schema.scalar_field("count").expect("count").id)
        .expect("count stats");
    assert_eq!(count.null_count, 2);
    assert_eq!(count.min, Some(StatValue::Int64(i64::MIN)));
    assert_eq!(count.max, Some(StatValue::Int64(7)));
    assert_eq!(count.distinct, Some(2));
    assert_eq!(count.top.first(), Some(&(StatValue::Int64(7), 2)));
    let image = stats
        .field(schema.vector_field("image").expect("image").id)
        .expect("image stats");
    assert_eq!((image.null_count, image.value_count), (2, 3));
}

#[test]
fn golden_file_reads_the_same_through_a_file_source() {
    let reader = SegmentReader::open(
        crate::segment_v2::FileSource::open(&golden_path()).expect("golden file opens"),
    )
    .expect("golden segment opens");
    reader.verify().expect("golden segment verifies");
    let from_memory = SegmentReader::open(MemorySource::new(committed())).expect("opens");
    assert_eq!(
        reader.read_rows().expect("rows"),
        from_memory.read_rows().expect("rows")
    );
}
