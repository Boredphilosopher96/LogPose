//! Segment reads through the buffer cache: lazy units, CRC failures,
//! bypassing scans, invalidation, async fetches, warm-up, and the Vfs
//! source.

use super::{fixture, open_verified};
use crate::cache::{
    AlignedBytes, ArtifactClass, BufferCache, CacheConfig, CacheUnit, FetchReport, Fetched,
    InlineExecutor, LoadExecutor, LoadJob, PinSet, charge_for,
};
use crate::segment_v2::{
    MemorySource, Region, SectionKind, SegmentBuilder, SegmentError, SegmentIdentity,
    SegmentReader, VfsSource,
};
use logpose_types::{
    CollectionId, DistanceMetric,
    record::PrimaryKey,
    schema::{CollectionSchema, FieldId, PrimaryKeySpec, PrimaryKeyType, VectorFieldSpec},
    value::codec,
};
use logpose_vfs::{FaultVfs, OpenMode};
use serde_json::json;
use std::{
    future::Future,
    io::IoSlice,
    path::Path,
    pin::pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

fn cache() -> BufferCache {
    BufferCache::new(CacheConfig::with_budget(64 << 20))
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park_timeout(Duration::from_millis(10));
    }
}

/// Runs each job on a new thread. `join` waits for them: a job thread holds
/// its flight (whose result pins the bytes) until it returns, a moment
/// after its waiters wake, so tests that assert on eviction join first.
#[derive(Default)]
struct SpawnExecutor {
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl LoadExecutor for SpawnExecutor {
    fn execute(&self, job: LoadJob) {
        let thread = thread::spawn(move || job.run());
        self.threads.lock().expect("threads").push(thread);
    }
}

impl SpawnExecutor {
    fn join(&self) {
        let threads = std::mem::take(&mut *self.threads.lock().expect("threads"));
        for thread in threads {
            thread.join().expect("job thread");
        }
    }
}

/// A segment with one vector field of `dim` dimensions, `rows` rows (every
/// row's vector is `[row; dim]`), and a dynamic object on every row.
fn vector_segment(dim: u32, rows: u32) -> (Vec<u8>, FieldId) {
    let schema = Arc::new(
        CollectionSchema::new(
            PrimaryKeySpec {
                name: "id".to_owned(),
                key_type: PrimaryKeyType::Int64,
            },
            vec![VectorFieldSpec {
                name: "embedding".to_owned(),
                dimensions: dim,
                metric: DistanceMetric::L2,
            }],
            Vec::new(),
            true,
        )
        .expect("schema"),
    );
    let field = schema.vector_field("embedding").expect("declared").id;
    let mut builder = SegmentBuilder::new(
        Arc::clone(&schema),
        SegmentIdentity {
            collection_id: CollectionId::default(),
            unit_id: 3,
        },
    )
    .expect("builder");
    for row in 0..rows {
        let object = codec::encode_json(&json!({ "row": row })).expect("encodes");
        builder
            .push_row(u64::from(row) + 1, &PrimaryKey::Int64(i64::from(row)))
            .expect("row")
            .vector(field, &vec![row as f32; dim as usize])
            .expect("vector")
            .dynamic(&object)
            .expect("dynamic");
    }
    (builder.finish_to_vec().expect("builds").0, field)
}

fn page_key(
    reader: &SegmentReader<MemorySource>,
    index: usize,
    page: u32,
) -> crate::cache::CacheKey {
    crate::cache::CacheKey {
        file: reader.cache_file().expect("cache attached"),
        section: u32::try_from(index).expect("fits"),
        unit: CacheUnit::Page(page),
    }
}

#[test]
fn cached_reads_match_uncached_reads_and_hit_the_second_time() {
    let bytes = fixture::golden_bytes();
    let plain = open_verified(&bytes);
    let cache = cache();
    let cached = SegmentReader::open(MemorySource::new(bytes.clone()))
        .expect("opens")
        .with_cache(&cache);
    let schema = fixture::golden_schema();
    let color = schema.scalar_field("color").expect("declared").id;
    let embedding = schema.vector_field("embedding").expect("declared").id;
    for _ in 0..2 {
        assert_eq!(
            cached.pk_column().expect("pk"),
            plain.pk_column().expect("pk")
        );
        assert_eq!(
            cached.stats().expect("stats"),
            plain.stats().expect("stats")
        );
        assert_eq!(
            cached.scalar_column(color).expect("column"),
            plain.scalar_column(color).expect("column")
        );
        let handle = cached.vector(embedding).expect("vector").expect("present");
        assert_eq!(
            &handle,
            &plain.vector(embedding).expect("vector").expect("present")
        );
        for row in 0..5 {
            assert_eq!(
                cached.vector_row(&handle, row).expect("row"),
                plain.vector_row(&handle, row).expect("row")
            );
        }
        let dynamic = cached.dynamic().expect("dynamic").expect("present");
        assert_eq!(
            cached.dynamic_block(&dynamic, 0).expect("block"),
            plain.dynamic_block(&dynamic, 0).expect("block")
        );
        for pk in (0..5).map(fixture::golden_pk) {
            assert_eq!(
                cached.find_row(&pk).expect("find"),
                plain.find_row(&pk).expect("find")
            );
        }
    }
    let stats = cache.stats();
    // pk column, pk sorted, pk filter, stats, column, vector prefix, one
    // vector page, dynamic index, one dynamic block.
    assert_eq!(stats.entries, 9, "{stats:?}");
    assert_eq!(stats.misses, 9, "every unit was read once");
    assert!(stats.hits > stats.misses);
    assert_eq!(
        stats.used_by(ArtifactClass::PkIndex),
        [
            SectionKind::PkColumn,
            SectionKind::PkSorted,
            SectionKind::PkFilter
        ]
        .into_iter()
        .map(|kind| {
            let index = cached.find_section(kind, None).expect("present");
            charge_for(usize::try_from(cached.sections()[index].length).expect("fits"))
        })
        .sum::<u64>()
    );
    assert_eq!(rows_of_both(&cached), rows_of_both(&plain));
}

fn rows_of_both(reader: &SegmentReader<MemorySource>) -> usize {
    reader.read_rows().expect("rows").len()
}

#[test]
fn vector_pages_are_cache_units_sized_by_dimension() {
    // 8 KiB pages at 128 dimensions, 6 KiB at 768, one row past 2048.
    for (dim, page_bytes, page_rows) in [
        (128_u32, 8192_u64, 16_u32),
        (768, 6144, 2),
        (4096, 16384, 1),
    ] {
        let (bytes, field) = vector_segment(dim, page_rows * 4 + 1);
        let cache = cache();
        let reader = SegmentReader::open(MemorySource::new(bytes))
            .expect("opens")
            .with_cache(&cache);
        let handle = reader.vector(field).expect("vector").expect("present");
        assert_eq!(handle.prefix().page_rows(), page_rows, "dim {dim}");
        let row = page_rows * 2 + page_rows / 2;
        assert_eq!(
            reader.vector_row(&handle, row).expect("row"),
            Some(vec![row as f32; dim as usize])
        );
        let index = handle.section_index();
        assert!(cache.residency(&page_key(&reader, index, 2)));
        assert!(!cache.residency(&page_key(&reader, index, 0)));
        assert!(!cache.residency(&page_key(&reader, index, 3)));
        let unit = handle.page_unit(2).expect("page");
        assert_eq!(unit.len_hint(), Some(page_bytes));
        let prefix = reader.vector_prefix_unit(field).expect("prefix");
        let prefix_len = reader.load(&prefix).expect("prefix").0.len();
        assert_eq!(
            cache.stats().used_by(ArtifactClass::RawVectors),
            charge_for(prefix_len) + charge_for(page_bytes as usize),
            "dim {dim}: only the prefix and one page are resident"
        );
    }
}

#[test]
fn a_corrupt_page_is_a_typed_error_and_is_not_cached() {
    let (mut bytes, field) = vector_segment(128, 64);
    let clean = open_verified(&bytes);
    let handle = clean.vector(field).expect("vector").expect("present");
    let index = handle.section_index();
    let section_start = usize::try_from(clean.sections()[index].offset).expect("fits");
    let page_one = handle.prefix().page_byte_range(1).expect("page 1");
    bytes[section_start + usize::try_from(page_one.start).expect("fits") + 5] ^= 0x40;

    let cache = cache();
    let reader = SegmentReader::open(MemorySource::new(bytes))
        .expect("opens")
        .with_cache(&cache);
    let handle = reader.vector(field).expect("vector").expect("present");
    for _ in 0..2 {
        let error = reader
            .vector_page(&handle, 1)
            .expect_err("page 1 is corrupt");
        assert!(
            matches!(error, SegmentError::Checksum { region: Region::VectorPage { index: found, page: 1 } } if found == index),
            "{error}"
        );
        assert!(!cache.residency(&page_key(&reader, index, 1)));
    }
    assert_eq!(cache.stats().failed_loads, 2, "a failed page is read again");
    assert_eq!(
        reader.vector_page(&handle, 0).expect("page 0").len(),
        16 * 128
    );
    assert!(cache.residency(&page_key(&reader, index, 0)));
}

#[test]
fn a_corrupt_section_is_a_typed_error_and_is_not_cached() {
    let bytes = fixture::golden_bytes();
    let clean = open_verified(&bytes);
    let index = clean
        .find_section(SectionKind::PkSorted, None)
        .expect("present");
    let mut corrupt = bytes.clone();
    corrupt[usize::try_from(clean.sections()[index].offset).expect("fits")] ^= 1;
    let cache = cache();
    let reader = SegmentReader::open(MemorySource::new(corrupt))
        .expect("opens")
        .with_cache(&cache);
    let error = reader.pk_sorted().expect_err("corrupt");
    assert!(error.is_corruption());
    assert!(
        matches!(error, SegmentError::Checksum { region: Region::Section { index: found, .. } } if found == index)
    );
    let unit = reader.section_unit(index).expect("unit");
    assert!(!reader.residency(&unit));
    assert_eq!(cache.used(), 0);
    reader.pk_column().expect("other sections still load");
    assert_eq!(cache.stats().entries, 1);
}

#[test]
fn dynamic_blocks_load_one_block_at_a_time() {
    let (bytes, _) = vector_segment(4, 3 * 4096 + 5);
    let cache = cache();
    let reader = SegmentReader::open(MemorySource::new(bytes))
        .expect("opens")
        .with_cache(&cache);
    let handle = reader.dynamic().expect("dynamic").expect("present");
    assert_eq!(handle.blocks().block_count(), 4);
    let block = reader.dynamic_block(&handle, 2).expect("block");
    assert_eq!(block.rows(), 8192..12288);
    let index = handle.section_index();
    assert!(cache.residency(&page_key(&reader, index, 2)));
    assert!(
        (0..4)
            .filter(|block| *block != 2)
            .all(|block| !cache.residency(&page_key(&reader, index, block)))
    );
    let stats = cache.stats();
    assert_eq!(stats.entries, 2, "the block index and one block");
    assert_eq!(
        stats.used_by(ArtifactClass::DynamicJson),
        stats.used_total()
    );
}

#[test]
fn full_scans_and_verify_leave_the_cache_alone() {
    let bytes = fixture::golden_bytes();
    let cache = cache();
    let reader = SegmentReader::open(MemorySource::new(bytes))
        .expect("opens")
        .with_cache(&cache);
    reader.read_rows().expect("rows");
    reader.verify().expect("verifies");
    let stats = cache.stats();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.used_total(), 0);
    // A scan still uses what is resident.
    reader.pk_column().expect("pk");
    let before = cache.stats().hits;
    reader.read_rows().expect("rows");
    assert_eq!(cache.stats().hits, before + 1);
}

#[test]
fn dropping_the_reader_of_an_obsolete_segment_invalidates_its_entries() {
    let bytes = fixture::golden_bytes();
    let cache = cache();
    let kept = SegmentReader::open(MemorySource::new(bytes.clone()))
        .expect("opens")
        .with_cache(&cache);
    let obsolete = SegmentReader::open(MemorySource::new(bytes))
        .expect("opens")
        .with_cache(&cache);
    assert_ne!(kept.cache_file(), obsolete.cache_file());
    kept.pk_column().expect("pk");
    obsolete.pk_column().expect("pk");
    obsolete.stats().expect("stats");
    let kept_used = cache.used() - {
        let stats_unit = obsolete
            .section_unit(
                obsolete
                    .find_section(SectionKind::Stats, None)
                    .expect("present"),
            )
            .expect("unit");
        let pk_unit = obsolete
            .section_unit(
                obsolete
                    .find_section(SectionKind::PkColumn, None)
                    .expect("present"),
            )
            .expect("unit");
        [stats_unit, pk_unit]
            .iter()
            .map(|unit| charge_for(usize::try_from(unit.len_hint().expect("known")).expect("fits")))
            .sum::<u64>()
    };
    drop(obsolete);
    assert_eq!(cache.used(), kept_used);
    assert_eq!(cache.stats().entries, 1);
    assert_eq!(cache.stats().invalidated, 2);
}

#[test]
fn async_fetches_pin_units_for_the_compute_stage() {
    let (bytes, field) = vector_segment(128, 100);
    let cache = cache();
    let reader = SegmentReader::open(MemorySource::new(bytes))
        .expect("opens")
        .with_cache(&cache);
    let executor = SpawnExecutor::default();
    let mut pins = PinSet::new();
    let mut report = FetchReport::default();

    // Stage 1: the prefix.
    let prefix = reader.vector_prefix_unit(field).expect("prefix");
    let (bytes, fetched) = block_on(reader.fetch(&prefix, &executor)).expect("prefix");
    report.record(prefix.class(), fetched);
    let handle = reader.vector_handle(&prefix, &bytes).expect("decodes");
    pins.insert(reader.unit_key(&prefix).expect("key"), bytes);

    // Stage 2: three pages, fetched concurrently.
    let units: Vec<_> = [0, 3, 6]
        .iter()
        .map(|page| handle.page_unit(*page).expect("page"))
        .collect();
    let fetches: Vec<_> = units
        .iter()
        .map(|unit| reader.fetch(unit, &executor))
        .collect();
    for (unit, fetch) in units.iter().zip(fetches) {
        let (bytes, fetched) = block_on(fetch).expect("page");
        report.record(unit.class(), fetched);
        pins.insert(reader.unit_key(unit).expect("key"), bytes);
    }
    assert_eq!(report.misses, 4);
    assert_eq!(report.by_class[ArtifactClass::RawVectors.index()].misses, 4);
    assert_eq!(report.bytes_read, pins.bytes());

    // Everything is pinned: shrinking the budget cannot evict it.
    cache.set_budget(1);
    assert_eq!(cache.stats().entries, 4);
    let page = pins
        .get(&reader.unit_key(&units[1]).expect("key"))
        .expect("pinned");
    let floats: &[f32] = bytemuck::try_cast_slice(page).expect("aligned f32 view");
    assert_eq!(floats[0], 48.0, "page 3 starts at row 48");
    drop(pins);
    drop(handle);
    executor.join();
    cache.trim();
    assert_eq!(cache.stats().entries, 0);

    // A reader without a cache still fetches through the executor.
    let (bytes, _) = vector_segment(4, 3);
    let plain = SegmentReader::open(MemorySource::new(bytes)).expect("opens");
    let unit = plain.section_unit(0).expect("unit");
    let (loaded, fetched) = block_on(plain.fetch(&unit, &InlineExecutor)).expect("loads");
    assert_eq!(loaded.len() as u64, unit.len_hint().expect("whole section"));
    assert!(matches!(fetched, Fetched::Loaded { .. }));
}

/// A read view fetches the key sections decoded: the loader attaches the decoded column,
/// order, and filter before the insert, so the cache charges them with the bytes and every
/// hit reuses them (decoding on first use attached them after the charge).
#[test]
fn key_sections_fetched_decoded_are_charged_with_their_decoded_form() {
    let cache = cache();
    let reader = SegmentReader::open(MemorySource::new(fixture::golden_bytes()))
        .expect("opens")
        .with_cache(&cache);
    let mut charged = 0;
    for kind in [
        SectionKind::PkColumn,
        SectionKind::PkSorted,
        SectionKind::PkFilter,
    ] {
        let index = reader.find_section(kind, None).expect("present");
        let unit = reader.section_unit(index).expect("unit");
        let (bytes, _) = block_on(reader.fetch_decoded(&unit, &InlineExecutor)).expect("loads");
        let decoded = match kind {
            SectionKind::PkColumn => bytes.attached::<crate::segment_v2::PkColumn>().is_some(),
            SectionKind::PkSorted => bytes.attached::<crate::segment_v2::PkSorted>().is_some(),
            _ => bytes.attached::<crate::segment_v2::PkFilter>().is_some(),
        };
        assert!(decoded, "{kind:?} carries its decoded form");
        let length = usize::try_from(reader.sections()[index].length).expect("fits");
        charged += charge_for(length) + bytes.len() as u64;
    }
    assert_eq!(cache.stats().used_by(ArtifactClass::PkIndex), charged);
}

#[test]
fn warm_up_loads_the_hot_sections_of_a_segment() {
    let bytes = fixture::golden_bytes();
    let cache = cache();
    let reader = SegmentReader::open(MemorySource::new(bytes))
        .expect("opens")
        .with_cache(&cache);
    let items = reader.warm_up_items();
    let mut classes: Vec<_> = items.iter().map(|item| item.class).collect();
    classes.dedup();
    assert!(classes.iter().all(|class| matches!(
        class,
        ArtifactClass::GraphAndCodes | ArtifactClass::PkIndex | ArtifactClass::ScalarIndex
    )));
    // SQ8, three pk sections, stats, and the inverted index.
    assert_eq!(items.len(), 6);
    let report = block_on(cache.warm_up(items, &SpawnExecutor::default()));
    assert_eq!(report.loaded, 6);
    let misses = cache.stats().misses;
    reader.pk_column().expect("pk");
    reader.stats().expect("stats");
    assert_eq!(cache.stats().misses, misses, "warmed sections are hits");
    let plain = SegmentReader::open(MemorySource::new(fixture::golden_bytes())).expect("opens");
    assert!(
        plain.warm_up_items().is_empty(),
        "no cache, nothing to warm"
    );
}

#[test]
fn a_vfs_source_reads_segments_through_the_vfs() {
    let vfs = FaultVfs::new(7);
    let process = vfs.process();
    let dir = Path::new("/segments");
    process.create_dir_all(dir).expect("dir");
    let path = dir.join("00000003.seg");
    let (bytes, field) = vector_segment(128, 64);
    let file = process.open(&path, OpenMode::CreateNew).expect("create");
    file.append(&[IoSlice::new(&bytes)]).expect("append");
    file.sync_all().expect("sync");

    let cache = cache();
    let source = VfsSource(process.open(&path, OpenMode::Read).expect("open"));
    let reader = SegmentReader::open(source)
        .expect("opens")
        .with_cache(&cache);
    reader.verify().expect("verifies");
    let handle = reader.vector(field).expect("vector").expect("present");
    assert_eq!(
        reader.vector_row(&handle, 3).expect("row"),
        Some(vec![3.0; 128])
    );
    // Corrupt page 2 on "disk": the cached page 0 is still served, page 2
    // fails with a typed error.
    let index = handle.section_index();
    let offset =
        reader.sections()[index].offset + handle.prefix().page_byte_range(2).expect("page").start;
    vfs.corrupt(&path, offset, &[0xff]).expect("corrupt");
    assert_eq!(
        reader.vector_row(&handle, 3).expect("cached"),
        Some(vec![3.0; 128])
    );
    assert!(matches!(
        reader.vector_page(&handle, 2),
        Err(SegmentError::Checksum {
            region: Region::VectorPage { page: 2, .. }
        })
    ));
    let pinned: Arc<AlignedBytes> = reader
        .load(&handle.page_unit(0).expect("page"))
        .expect("hit")
        .0;
    assert_eq!(pinned.len(), 8192);
}

/// Every accessor of `cached` returns what the same accessor of `plain` (no
/// cache) returns.
fn assert_same_reads(
    plain: &SegmentReader<MemorySource>,
    cached: &SegmentReader<MemorySource>,
    executor: &dyn LoadExecutor,
    what: &str,
) {
    assert_eq!(
        cached.row_meta().expect("seqs"),
        plain.row_meta().expect("seqs"),
        "{what}"
    );
    let pks = plain.pk_column().expect("pk");
    assert_eq!(cached.pk_column().expect("pk"), pks, "{what}");
    assert_eq!(
        cached.pk_sorted().expect("sorted"),
        plain.pk_sorted().expect("sorted"),
        "{what}"
    );
    assert_eq!(
        cached.pk_filter().expect("filter"),
        plain.pk_filter().expect("filter"),
        "{what}"
    );
    assert_eq!(
        cached.stats().expect("stats"),
        plain.stats().expect("stats"),
        "{what}"
    );
    let stride = pks.len() / 40 + 1;
    for row in (0..pks.len()).step_by(stride) {
        let pk = pks.get(row).expect("pk");
        assert_eq!(
            cached.find_row(&pk).expect("find"),
            plain.find_row(&pk).expect("find"),
            "{what}"
        );
    }
    let schema = Arc::clone(plain.schema());
    for field in schema.fields() {
        assert_eq!(
            cached.scalar_column(field.id).expect("column"),
            plain.scalar_column(field.id).expect("column"),
            "{what} field {}",
            field.id
        );
    }
    for field in schema.vectors() {
        let expected = plain.vector(field.id).expect("vector").expect("present");
        let handle = cached.vector(field.id).expect("vector").expect("present");
        assert_eq!(handle, expected, "{what}");
        for page in 0..expected.prefix().page_count() {
            assert_eq!(
                cached.vector_page(&handle, page).expect("page"),
                plain.vector_page(&expected, page).expect("page"),
                "{what} page {page}"
            );
            let unit = handle.page_unit(page).expect("unit");
            let (fetched, _) = block_on(cached.fetch(&unit, executor)).expect("fetch");
            assert_eq!(&**fetched, &**plain.load(&unit).expect("load").0, "{what}");
        }
        for row in (0..plain.row_count()).step_by(stride) {
            assert_eq!(
                cached.vector_row(&handle, row).expect("row"),
                plain.vector_row(&expected, row).expect("row"),
                "{what} row {row}"
            );
        }
    }
    let expected = plain.dynamic().expect("dynamic");
    let handle = cached.dynamic().expect("dynamic");
    assert_eq!(handle, expected, "{what}");
    if let (Some(handle), Some(expected)) = (handle, expected) {
        for block in 0..expected.blocks().block_count() {
            assert_eq!(
                cached.dynamic_block(&handle, block).expect("block"),
                plain.dynamic_block(&expected, block).expect("block"),
                "{what} block {block}"
            );
            let unit = handle.block_unit(block).expect("unit");
            let (fetched, _) = block_on(cached.fetch(&unit, executor)).expect("fetch");
            assert_eq!(&**fetched, &**plain.load(&unit).expect("load").0, "{what}");
        }
    }
    for index in 0..plain.sections().len() {
        let section = plain.read_section(index).expect("section");
        assert_eq!(
            &**cached.read_section(index).expect("section"),
            &**section,
            "{what} section {index}"
        );
        let unit = plain.section_unit(index).expect("unit");
        let (fetched, _) = block_on(cached.fetch(&unit, executor)).expect("fetch");
        assert_eq!(&**fetched, &**section, "{what} section {index}");
    }
    assert_eq!(
        cached.read_rows().expect("rows"),
        plain.read_rows().expect("rows"),
        "{what}"
    );
    cached.verify().expect("verifies");
}

#[test]
fn random_segments_read_the_same_through_a_thrashing_cache() {
    let executor = SpawnExecutor::default();
    let mut segments: Vec<Vec<u8>> = (0..24)
        .map(|seed| super::roundtrip::random_segment(seed, 200).3)
        .collect();
    segments.push(fixture::golden_bytes());
    segments.push(vector_segment(128, 300).0);
    segments.push(vector_segment(3, 2 * 4096 + 7).0);
    for (number, bytes) in segments.iter().enumerate() {
        let plain = open_verified(bytes);
        // From nothing, through a few units, to everything.
        for budget in [0, 600, 4096, 64 << 20] {
            let cache = BufferCache::new(CacheConfig::with_budget(budget));
            let cached = SegmentReader::open(MemorySource::new(bytes.clone()))
                .expect("opens")
                .with_cache(&cache);
            for pass in 0..2 {
                let what = format!("segment {number} budget {budget} pass {pass}");
                assert_same_reads(&plain, &cached, &executor, &what);
                assert_same_reads(&plain, &cached, &InlineExecutor, &what);
            }
            executor.join();
            cache.trim();
            assert!(cache.used() <= budget, "segment {number} budget {budget}");
        }
    }
}
