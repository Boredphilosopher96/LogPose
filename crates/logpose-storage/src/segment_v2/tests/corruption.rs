//! Every single-byte change and every truncation of the golden segment is
//! detected, and no declared length can make the reader allocate more than
//! the file holds.

use super::{Patcher, fixture};
use crate::segment_v2::{
    MemorySource, SectionKind, SectionSource, SegmentBuilder, SegmentError, SegmentIdentity,
    SegmentReader,
};
use logpose_types::{
    CollectionId, DistanceMetric,
    record::PrimaryKey,
    schema::{
        CollectionSchema, ElementType, FieldType, PrimaryKeySpec, PrimaryKeyType, ScalarFieldSpec,
        VectorFieldSpec,
    },
    value::Value,
};
use std::sync::Arc;
use std::{
    io,
    sync::atomic::{AtomicU64, Ordering},
};

/// Open and fully verify; the error if either fails.
fn open_and_verify(bytes: &[u8]) -> Result<(), SegmentError> {
    let reader = SegmentReader::open(MemorySource::new(bytes.to_vec()))?;
    reader.verify()?;
    reader.read_rows().map(|_| ())
}

fn assert_detected(bytes: &[u8], what: &str) {
    match open_and_verify(bytes) {
        Ok(()) => unreachable!("{what} was not detected"),
        Err(error) => assert!(
            error.is_corruption(),
            "{what}: not a corruption error: {error}"
        ),
    }
}

#[test]
fn every_single_byte_change_is_detected() {
    let golden = fixture::golden_bytes();
    open_and_verify(&golden).expect("golden segment is valid");
    let mut bytes = golden.clone();
    for index in 0..golden.len() {
        for mask in [0x01_u8, 0x80, 0xff] {
            bytes[index] ^= mask;
            assert_detected(&bytes, &format!("xor {mask:#04x} at byte {index}"));
            bytes[index] = golden[index];
        }
    }
}

#[test]
fn every_truncation_and_extension_is_detected() {
    let golden = fixture::golden_bytes();
    for len in 0..golden.len() {
        assert_detected(&golden[..len], &format!("truncation to {len} bytes"));
    }
    let mut longer = golden.clone();
    longer.push(0);
    assert_detected(&longer, "one extra byte");
    let mut padded = golden.clone();
    padded.splice(0..0, [0_u8; 64]);
    assert_detected(&padded, "64 leading bytes");
}

/// A source that records the largest read and refuses reads past the end.
struct TrackingSource {
    inner: MemorySource,
    largest: AtomicU64,
}

impl SectionSource for TrackingSource {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.largest.fetch_max(buf.len() as u64, Ordering::Relaxed);
        self.inner.read_exact_at(buf, offset)
    }
}

/// Run every reader entry point on `bytes`. Returns whether all of them
/// failed (or open failed), and asserts no read exceeded the file length.
fn every_accessor_fails(bytes: &[u8]) -> bool {
    let source = TrackingSource {
        inner: MemorySource::new(bytes.to_vec()),
        largest: AtomicU64::new(0),
    };
    let all_failed = match SegmentReader::open(&source) {
        Err(_) => true,
        Ok(reader) => {
            let schema = reader.schema().clone();
            let mut results = vec![reader.verify().is_err(), reader.read_rows().is_err()];
            results.push(reader.row_meta().is_err() || reader.pk_column().is_err());
            for field in schema.vectors() {
                results.push(reader.vector(field.id).is_err());
            }
            results.iter().all(|failed| *failed)
        }
    };
    assert!(source.largest.load(Ordering::Relaxed) <= bytes.len() as u64);
    all_failed
}

#[test]
fn declared_counts_and_lengths_are_bounded_by_the_file_size() {
    let golden = fixture::golden_bytes();
    let base = Patcher::new(&golden);
    let footer = base.footer_at();

    let mut patch = Patcher::new(&golden);
    patch.set_u32(footer + 8, u32::MAX).seal_footer();
    assert!(every_accessor_fails(&patch.bytes), "huge section count");

    let mut patch = Patcher::new(&golden);
    patch.set_u64(footer, u64::MAX - 63).seal_footer();
    assert!(every_accessor_fails(&patch.bytes), "huge table offset");

    let stats = base.find(SectionKind::Stats.code());
    let mut patch = Patcher::new(&golden);
    let at = patch.entry_at(stats);
    patch.set_u64(at + 16, u64::MAX / 2).seal_table();
    assert!(every_accessor_fails(&patch.bytes), "huge section length");

    // A header that claims u32::MAX rows, with every row-shaped section
    // agreeing, so the lie gets past open.
    let mut patch = Patcher::new(&golden);
    patch.set_u32(36, u32::MAX).seal_header();
    for index in 0..patch.section_count() {
        let kind = patch.entry(index).0;
        let row_shaped = [
            SectionKind::PkColumn,
            SectionKind::PkSorted,
            SectionKind::ScalarColumn,
            SectionKind::DynamicJson,
            SectionKind::VectorF32,
        ]
        .iter()
        .any(|shaped| shaped.code() == kind);
        if row_shaped {
            let at = patch.entry_at(index);
            patch.set_u64(at + 32, u64::from(u32::MAX));
        }
    }
    patch.seal_table();
    assert!(every_accessor_fails(&patch.bytes), "huge row count");
}

#[test]
fn inner_lengths_are_bounded_by_the_section() {
    let golden = fixture::golden_bytes();
    let reader = SegmentReader::open(MemorySource::new(golden.clone())).expect("opens");
    let schema = reader.schema().clone();
    let base = Patcher::new(&golden);

    let title = schema.scalar_field("title").expect("title").id;
    let column = reader
        .find_section(SectionKind::ScalarColumn, Some(title))
        .expect("title column");
    let embedding = schema.vector_field("embedding").expect("embedding").id;
    let vector = reader
        .find_section(SectionKind::VectorF32, Some(embedding))
        .expect("embedding vectors");
    let dynamic = reader
        .find_section(SectionKind::DynamicJson, None)
        .expect("dynamic section");
    let pk = base.find(SectionKind::PkColumn.code());

    // (section, offset in payload, value, width, accessor)
    type Access =
        fn(&SegmentReader<MemorySource>, &logpose_types::schema::CollectionSchema) -> bool;
    let title_fails: Access = |reader, schema| {
        let title = schema.scalar_field("title").map(|field| field.id);
        title.is_some_and(|title| reader.scalar_column(title).is_err())
    };
    let vector_fails: Access = |reader, schema| {
        let field = schema.vector_field("embedding").map(|field| field.id);
        field.is_some_and(|field| reader.vector(field).is_err())
    };
    let dynamic_fails: Access = |reader, _| reader.dynamic().is_err();
    let pk_fails: Access = |reader, _| reader.pk_column().is_err();
    let cases: [(usize, usize, u64, usize, Access, &str); 9] = [
        (column, 8, u64::MAX / 2, 8, title_fails, "column nulls_len"),
        (column, 16, u64::MAX / 2, 8, title_fails, "column dict_len"),
        (column, 24, u64::MAX / 2, 8, title_fails, "column data_len"),
        (
            column,
            4,
            u64::from(u32::MAX),
            4,
            title_fails,
            "column row_count",
        ),
        (
            vector,
            16,
            u64::MAX - 7,
            8,
            vector_fails,
            "vector nulls_len",
        ),
        (
            vector,
            12,
            u64::from(u32::MAX),
            4,
            vector_fails,
            "vector page_count",
        ),
        (
            vector,
            4,
            u64::from(u32::MAX),
            4,
            vector_fails,
            "vector row_count",
        ),
        (
            dynamic,
            8,
            u64::from(u32::MAX),
            4,
            dynamic_fails,
            "dynamic block_count",
        ),
        (pk, 4, u64::from(u32::MAX), 4, pk_fails, "pk offset"),
    ];
    for (section, offset, value, width, fails, what) in cases {
        let mut patch = Patcher::new(&golden);
        let at = patch.entry(section).1 + offset;
        if width == 8 {
            patch.set_u64(at, value);
        } else {
            patch.set_u32(at, u32::try_from(value).expect("fits"));
        }
        patch.seal_section(section);
        every_accessor_fails(&patch.bytes);
        let reader = SegmentReader::open(MemorySource::new(patch.bytes.clone()))
            .expect("open does not read these sections");
        assert!(fails(&reader, &schema), "{what} was accepted");
        assert!(reader.verify().is_err(), "{what} passed verify");
    }
}

/// An `array<bool>` cell of about 2^32 elements fits in a few bytes, because
/// its child block is a bitmap of the true elements. That is a valid file,
/// and `verify` must check it without materializing the cell (which would
/// need over 100 GiB).
#[test]
fn verify_does_not_materialize_compressed_cells() {
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
            vec![ScalarFieldSpec::new(
                "flags",
                FieldType::Array(ElementType::Bool),
            )],
            false,
        )
        .expect("schema"),
    );
    let flags = schema.scalar_field("flags").expect("flags").id;
    let identity = SegmentIdentity {
        collection_id: CollectionId::default(),
        unit_id: 1,
    };
    let mut builder = SegmentBuilder::new(Arc::clone(&schema), identity).expect("builder");
    builder
        .push_row(1, &PrimaryKey::Int64(1))
        .expect("row")
        .scalar(flags, &Value::Array(vec![Value::Bool(false)]))
        .expect("flags");
    let (bytes, _) = builder.finish_to_vec().expect("builds");
    let mut patch = Patcher::new(&bytes);
    let section = (0..patch.section_count())
        .find(|index| patch.entry(*index).0 == SectionKind::ScalarColumn.code())
        .expect("flags column");
    // Column header (64 bytes), then `u32 offsets[2]`, then the child
    // block, whose row count is at byte 4 of its header.
    let data = patch.entry(section).1 + 64;
    let elements = u32::MAX - 1;
    patch
        .set_u32(data + 4, elements)
        .set_u32(data + 8 + 4, elements)
        .seal_section(section);
    let reader = SegmentReader::open(MemorySource::new(patch.bytes.clone())).expect("opens");
    reader.verify().expect("a long array of false verifies");
    let column = reader
        .scalar_column(flags)
        .expect("decodes")
        .expect("present");
    assert_eq!(column.len(), 1);
}
