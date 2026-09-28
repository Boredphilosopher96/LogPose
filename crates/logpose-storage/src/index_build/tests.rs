//! Index builds: a flush writes no graph and the index build adds it in a sidecar that the
//! manifest names; searches see the graph once the build commits and use the SQ8 codes until
//! then; a crash at every operation of a build recovers the same rows with no orphan, and the
//! build is planned again; a compaction that takes the segment, a drop, and an engine shutdown
//! cancel a running build; a retired segment's sidecar is removed with it, only once no version
//! reads it; an explicit compaction converges and indexes; a quiet collection merges its small
//! segments and indexes what is left.

use crate::{
    CollectionHandle, CompactionConfig, CreateCollectionRequest, Engine, EngineConfig, IndexPolicy,
    JobKind, ManualClock, RuntimeConfig,
    dv::parse_dv_file_name,
    manifest::parse_manifest_file_name,
    paths::{index_path, parse_index_file_name, parse_segment_file_name, segment_path},
    read::{FetchPlan, ReadOptions, SectionNeed},
    test_support::{live_records, put},
};
use logpose_index::graph::{F32Metric, F32Vectors, SearchScratch};
use logpose_types::{CollectionRef, DistanceMetric, SeqNo, UnitId, record::Record};
use logpose_vfs::{CrashPoint, FaultPlan, FaultVfs, TearMode, Vfs};
use logpose_wal::BootId;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const ROOT: &str = "/storage";
const NAME: &str = "graphs";
const DIM: u32 = 8;

/// Graphs from 64 rows, codes from 16.
fn policy() -> IndexPolicy {
    IndexPolicy {
        graph_min_rows: 64,
        sq8_min_rows: 16,
        ..IndexPolicy::default()
    }
}

fn config() -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new("boot")),
        index: policy(),
        runtime: RuntimeConfig {
            io_threads: 2,
            query_threads: 1,
            maintenance_threads: 1,
            writer_threads: 1,
            ..RuntimeConfig::default()
        },
        ..EngineConfig::default()
    }
}

fn open(vfs: Arc<dyn Vfs>, config: EngineConfig) -> Engine {
    Engine::open(vfs, ROOT, config).expect("engine should open")
}

/// Create the collection. Flushes are explicit; `background` turns background compaction and
/// index builds on.
fn create(engine: &Engine, background: bool) -> Arc<CollectionHandle> {
    let mut descriptor = engine
        .core()
        .plan_collection_descriptor(&CreateCollectionRequest::new(
            NAME,
            DIM as usize,
            DistanceMetric::L2,
        ))
        .expect("descriptor should plan");
    descriptor.flush_threshold_ops = usize::MAX;
    descriptor.flush_threshold_bytes = usize::MAX;
    descriptor.compaction_threshold_segments = if background { 4 } else { usize::MAX };
    engine
        .create_collection_blocking(descriptor, None)
        .expect("collection should be created")
}

fn reference() -> CollectionRef {
    CollectionRef::new_default(NAME)
}

fn open_handle(engine: &Engine) -> Arc<CollectionHandle> {
    engine
        .collection(&reference())
        .expect("collection should be open")
}

/// Row `row`'s vector: distinct for rows below 1,000.
#[allow(clippy::cast_precision_loss)]
fn vector(row: u32) -> Vec<f32> {
    (0..DIM)
        .map(|dim| ((row * 919 + dim * 337 + dim * dim * 71) % 1_000) as f32 / 1_000.0)
        .collect()
}

/// Write rows `from..from + count`, keyed `r<row>`, in batches of 50.
fn fill(handle: &CollectionHandle, from: u32, count: u32) {
    let rows = (from..from + count).collect::<Vec<_>>();
    for batch in rows.chunks(50) {
        handle
            .write_blocking(
                batch
                    .iter()
                    .map(|row| put(&format!("r{row:04}"), vector(*row)))
                    .collect(),
            )
            .expect("write");
    }
}

fn rows(handle: &CollectionHandle) -> Vec<(SeqNo, Record)> {
    let version = handle.current();
    version.check_invariants().expect("invariants hold");
    live_records(&version).expect("scan")
}

/// Whether each segment of the current version has a graph, as a search fetches it.
fn graphs(engine: &Engine) -> Vec<(UnitId, bool)> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let view = engine
        .read_view_blocking(&reference(), &ReadOptions::default())
        .expect("view");
    let field = view.schema().vectors()[0].id;
    let mut plan = FetchPlan::default();
    for unit in view.units() {
        if !unit.is_memtable() {
            plan.push(unit.id(), SectionNeed::VectorIndex(field));
        }
    }
    let (pins, _) = runtime.block_on(view.fetch(&plan)).expect("fetch");
    view.units()
        .into_iter()
        .filter(|unit| !unit.is_memtable())
        .map(|unit| {
            let index = unit.vector_index(field, &pins).expect("vector index");
            assert!(index.sq8.is_some(), "every segment keeps its SQ8 codes");
            assert_eq!(
                index.graph.is_some(),
                unit.has_vector_index(field, true),
                "the fetched graph and the unit's report agree"
            );
            (unit.id(), index.graph.is_some())
        })
        .collect()
}

/// Walk the one segment's graph for rows `0..count`'s own vectors: the graph finds (almost)
/// every row as its own nearest neighbour.
fn graph_finds_rows(engine: &Engine, count: u32) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let view = engine
        .read_view_blocking(&reference(), &ReadOptions::default())
        .expect("view");
    let field = view.schema().vectors()[0].id;
    let unit = view
        .units()
        .into_iter()
        .find(|unit| !unit.is_memtable())
        .expect("a segment");
    let mut plan = FetchPlan::default();
    plan.push(unit.id(), SectionNeed::VectorIndex(field));
    let (pins, _) = runtime.block_on(view.fetch(&plan)).expect("fetch");
    let graph = unit
        .vector_index(field, &pins)
        .expect("vector index")
        .graph
        .expect("a graph");
    let data = (0..unit.row_count()).flat_map(vector).collect::<Vec<_>>();
    let vectors = F32Vectors::new(DIM as usize, data, F32Metric::L2Squared).expect("vectors");
    let mut scratch = SearchScratch::new();
    let found = (0..count)
        .filter(|row| {
            let query = vector(*row);
            let distance = vectors.query(&query).expect("query");
            graph
                .graph
                .search(&distance, 1, 32, &mut scratch)
                .neighbors
                .first()
                .and_then(|hit| graph.nodes.first_row(hit.row))
                == Some(*row)
        })
        .count();
    assert!(
        found * 100 >= count as usize * 95,
        "the graph found {found} of {count} rows"
    );
}

fn exists(vfs: &dyn Vfs, path: &std::path::Path) -> bool {
    logpose_vfs::exists(vfs, path).expect("exists")
}

/// Every file in `segments/` is one the current manifest names (a segment, its DV file, or its
/// index sidecar), every named file exists, and the manifests are the current generation and
/// at most the one below it.
fn assert_no_orphans(vfs: &dyn Vfs, handle: &CollectionHandle, context: &str) {
    let version = handle.current();
    let manifest = &version.manifest;
    let dir = &handle.meta().dir;
    for entry in &manifest.segments {
        assert!(exists(vfs, &segment_path(dir, entry.unit)), "{context}");
        if let Some(index) = entry.index {
            let path = index_path(dir, entry.unit, index.unit);
            assert!(exists(vfs, &path), "{context}: live {}", path.display());
        }
    }
    for entry in vfs.list(&dir.join("segments")).expect("list") {
        let named = parse_segment_file_name(&entry.name)
            .is_some_and(|unit| manifest.units().any(|live| live == unit))
            || parse_dv_file_name(&entry.name).is_some_and(|(unit, generation)| {
                manifest.segments.iter().any(|segment| {
                    segment.unit == unit && segment.dv.is_some_and(|dv| dv.generation == generation)
                })
            })
            || parse_index_file_name(&entry.name).is_some_and(|(unit, sidecar)| {
                manifest.segments.iter().any(|segment| {
                    segment.unit == unit && segment.index.is_some_and(|index| index.unit == sidecar)
                })
            });
        assert!(named, "{context}: orphan segments/{}", entry.name);
    }
    let generations = vfs
        .list(&dir.join("manifests"))
        .expect("list")
        .into_iter()
        .filter_map(|entry| parse_manifest_file_name(&entry.name))
        .collect::<Vec<_>>();
    assert!(generations.len() <= 2, "{context}: {generations:?}");
}

fn wait_for(what: &str, timeout: Duration, done: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Run the index build of the largest segment without a sidecar by hand.
fn index_by_hand(engine: &Engine, handle: &Arc<CollectionHandle>) {
    let job = engine
        .begin_job(handle, JobKind::Index)
        .expect("begin index build");
    assert!(job.has_work(), "a segment needs its graph");
    job.commit().expect("commit index build");
}

/// A flush writes the segment with SQ8 codes and no graph, and searches use the codes. The
/// index build then adds the graph in a sidecar: the manifest names it, the new version's
/// segment walks it, the rows are unchanged, a version pinned before still reads without it,
/// and a reopen opens the sidecar with the segment.
#[test]
fn an_index_build_adds_the_graph_a_flush_no_longer_builds() {
    let fault = FaultVfs::new(7);
    let engine = open(fault.process(), config());
    let handle = create(&engine, false);
    fill(&handle, 0, 300);
    let started = Instant::now();
    handle.flush_blocking().expect("flush");
    let flushed = started.elapsed();
    let expected = rows(&handle);
    assert_eq!(graphs(&engine).len(), 1);
    assert!(!graphs(&engine)[0].1, "a flush builds no graph");
    assert_eq!(handle.current().indexed_segments(), (0, 0));

    let before = handle.current();
    let pinned = handle.pin_snapshot().expect("pin");
    index_by_hand(&engine, &handle);
    assert!(graphs(&engine)[0].1, "the index build published the graph");
    assert_eq!(handle.current().indexed_segments(), (1, 1));
    assert_eq!(rows(&handle), expected);
    assert!(
        handle.current().manifest_generation > before.manifest_generation,
        "the graph lands with a manifest"
    );
    graph_finds_rows(&engine, 100);
    // The version pinned before the build still reads the segment without its graph.
    assert_eq!(before.indexed_segments(), (0, 0));
    assert!(handle.release_snapshot(&pinned));
    drop(before);
    tracing::debug!(?flushed, "flush time");

    // Nothing left to build.
    let job = engine
        .begin_job(&handle, JobKind::Index)
        .expect("begin index build");
    assert!(!job.has_work());
    drop(job);

    drop(handle);
    drop(engine);
    let engine = open(fault.process(), config());
    let handle = open_handle(&engine);
    assert_eq!(rows(&handle), expected);
    assert!(
        graphs(&engine)[0].1,
        "the sidecar is opened with its segment"
    );
    engine.wait_for_gc();
    assert_no_orphans(fault.process().as_ref(), &handle, "after reopen");
}

/// With background maintenance on, the writer plans the build as soon as a flush commits a
/// segment of at least `graph_min_rows` rows, and a smaller segment gets none while the
/// collection takes writes.
#[test]
fn background_index_builds_follow_flushes_of_large_enough_segments() {
    let fault = FaultVfs::new(8);
    let engine = open(fault.process(), config());
    let handle = create(&engine, true);
    fill(&handle, 0, 40);
    handle.flush_blocking().expect("flush");
    fill(&handle, 40, 200);
    handle.flush_blocking().expect("flush");
    wait_for("the index build", Duration::from_secs(30), || {
        handle.current().indexed_segments() == (1, 1)
    });
    let graphs = graphs(&engine);
    assert_eq!(graphs.len(), 2);
    assert!(!graphs[0].1, "40 rows are below graph_min_rows");
    assert!(graphs[1].1);
    assert!(engine.scheduler().stats().index_builds_granted >= 1);
    assert!(handle.maintenance_written().index_bytes > 0);
}

/// Index build crash analysis, exhaustively: a crash at every mutating operation of an index
/// build (the sidecar's writes and syncs, the directory sync, the manifest publish, and the
/// removals after it), under every tear mode, recovers the same rows with no orphan file, the
/// segment has its graph exactly when the manifest that names the sidecar became durable, and
/// a reopen with background maintenance plans the build again when it is missing.
#[test]
fn a_crash_at_every_op_of_an_index_build_recovers_the_same_rows_without_orphans() {
    let prepare = |fault: &Arc<FaultVfs>| {
        let engine = open(fault.process(), config());
        let handle = create(&engine, false);
        fill(&handle, 0, 120);
        handle.flush_blocking().expect("flush");
        engine.wait_for_gc();
        (engine, handle)
    };
    let clean = FaultVfs::new(40);
    let (engine, handle) = prepare(&clean);
    let expected = rows(&handle);
    let before = clean.mutating_ops();
    index_by_hand(&engine, &handle);
    engine.wait_for_gc();
    let ops = clean.mutating_ops() - before;
    assert!(
        ops >= 8,
        "an index build writes, syncs, and publishes: {ops} ops"
    );
    drop(handle);
    drop(engine);

    for tear in TearMode::ALL {
        for k in 0..ops {
            let context = format!("{tear:?}, crash after {k} of {ops} index build ops");
            let fault = FaultVfs::new(2_000 + k);
            let (engine, handle) = prepare(&fault);
            let generation = handle.current().manifest_generation;
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let committed = engine
                .begin_job(&handle, JobKind::Index)
                .and_then(crate::SteppedJob::commit)
                .is_ok();
            drop(handle);
            drop(engine);
            fault.crash();
            fault.set_plan(FaultPlan::default());

            let engine = open(fault.process(), config());
            let handle = open_handle(&engine);
            assert_eq!(rows(&handle), expected, "{context}");
            let recovered = handle.current();
            let indexed = recovered.manifest_generation > generation;
            if committed {
                assert!(indexed, "{context}: an acknowledged build is durable");
            }
            assert_eq!(
                recovered.indexed_segments(),
                if indexed { (1, 1) } else { (0, 0) },
                "{context}"
            );
            drop(recovered);
            engine.wait_for_gc();
            assert_no_orphans(fault.process().as_ref(), &handle, &context);
            if !indexed {
                // The build is planned again: by hand here, as the background writer would.
                index_by_hand(&engine, &handle);
                assert_eq!(rows(&handle), expected, "{context}");
                assert_eq!(handle.current().indexed_segments(), (1, 1), "{context}");
            }
        }
    }
}

/// The two named crash points of an index build leave a sidecar no manifest names: recovery
/// removes it, and with background maintenance on, the reopened collection builds it again.
#[test]
fn named_index_crash_points_leave_the_segment_without_a_graph_until_the_build_reruns() {
    for (seed, point) in [
        CrashPoint::IndexAfterSidecarSync,
        CrashPoint::IndexAfterSegmentsDirSync,
    ]
    .into_iter()
    .enumerate()
    {
        let context = format!("{point:?}");
        let fault = FaultVfs::new(60 + seed as u64);
        let engine = open(fault.process(), config());
        let handle = create(&engine, false);
        fill(&handle, 0, 120);
        handle.flush_blocking().expect("flush");
        let expected = rows(&handle);
        fault.set_plan(FaultPlan {
            crash_at: Some(point),
            ..FaultPlan::default()
        });
        let built = engine
            .begin_job(&handle, JobKind::Index)
            .and_then(crate::SteppedJob::commit);
        assert!(built.is_err(), "{context}: the build crashed");
        drop(handle);
        drop(engine);
        fault.crash();
        fault.set_plan(FaultPlan::default());

        // Reopen with background maintenance on: the collection plans the build again at its
        // first data-plane access.
        let engine = open(fault.process(), config());
        let handle = open_handle(&engine);
        assert_eq!(handle.current().indexed_segments(), (0, 0), "{context}");
        engine.wait_for_gc();
        assert_no_orphans(fault.process().as_ref(), &handle, &context);
        index_by_hand(&engine, &handle);
        assert_eq!(rows(&handle), expected, "{context}");
        assert_eq!(handle.current().indexed_segments(), (1, 1), "{context}");
    }
}

/// A recovered collection whose segment lost its build to a crash builds it in the background
/// once it is used again.
#[test]
fn a_reopened_collection_plans_the_missing_index_build() {
    let fault = FaultVfs::new(70);
    let engine = open(fault.process(), config());
    let handle = create(&engine, false);
    fill(&handle, 0, 120);
    handle.flush_blocking().expect("flush");
    let mut descriptor = handle.descriptor().clone();
    drop(handle);
    drop(engine);
    // Turn background maintenance on in the stored descriptor, as an operator would.
    descriptor.compaction_threshold_segments = 4;
    let path = descriptor.root_path.join("descriptor.json");
    let bytes = serde_json::to_vec(&descriptor).expect("descriptor encodes");
    let vfs = fault.process();
    vfs.remove_file(&path).expect("remove descriptor");
    let file = vfs
        .open(&path, logpose_vfs::OpenMode::CreateNew)
        .expect("create descriptor");
    file.append(&[std::io::IoSlice::new(&bytes)])
        .expect("write descriptor");
    file.sync_all().expect("sync descriptor");

    let engine = open(fault.process(), config());
    let handle = open_handle(&engine);
    assert_eq!(handle.current().indexed_segments(), (0, 0));
    // The first data-plane access arms maintenance.
    let _ = graphs(&engine);
    wait_for("the index build", Duration::from_secs(30), || {
        handle.current().indexed_segments() == (1, 1)
    });
}

/// A compaction that takes a segment cancels its running index build: the build ends without
/// committing and its sidecar is removed; the compaction's output is indexed afterwards.
#[test]
fn a_compaction_cancels_the_index_build_of_its_input() {
    let fault = FaultVfs::new(80);
    let engine = open(fault.process(), config());
    let handle = create(&engine, false);
    fill(&handle, 0, 100);
    handle.flush_blocking().expect("flush");
    fill(&handle, 100, 100);
    handle.flush_blocking().expect("flush");
    let expected = rows(&handle);

    let mut index = engine
        .begin_job(&handle, JobKind::Index)
        .expect("begin index build");
    index.build().expect("build index");
    // The compaction takes both segments, the one being indexed included.
    let compaction = engine
        .begin_job(&handle, JobKind::Compact)
        .expect("begin compaction");
    assert!(compaction.has_work());
    // The cancelled build ends without a change.
    index
        .commit()
        .expect("a cancelled build answers with the current state");
    assert_eq!(handle.current().indexed_segments(), (0, 0));
    compaction.commit().expect("commit compaction");
    assert_eq!(rows(&handle), expected);
    assert_eq!(handle.current().counters.segment_count, 1);
    engine.wait_for_gc();
    assert_no_orphans(
        fault.process().as_ref(),
        &handle,
        "after the cancelled build",
    );
    index_by_hand(&engine, &handle);
    assert_eq!(handle.current().indexed_segments(), (1, 1));
    graph_finds_rows(&engine, 200);
}

/// A build whose segment a compaction merged away before it committed commits nothing and
/// leaves no file; and a compaction of an indexed segment retires its sidecar with it, removed
/// only once the last version that reads it is released.
#[test]
fn a_retired_segment_takes_its_sidecar_along_once_no_version_reads_it() {
    let fault = FaultVfs::new(90);
    let engine = open(fault.process(), config());
    let handle = create(&engine, false);
    fill(&handle, 0, 100);
    handle.flush_blocking().expect("flush");
    index_by_hand(&engine, &handle);
    let sidecar = {
        let version = handle.current();
        let entry = &version.manifest.segments[0];
        index_path(
            &handle.meta().dir,
            entry.unit,
            entry.index.expect("indexed").unit,
        )
    };
    let vfs = fault.process();
    fill(&handle, 100, 100);
    handle.flush_blocking().expect("flush");
    let pinned = handle.pin_snapshot().expect("pin");
    let job = engine
        .begin_job(&handle, JobKind::Compact)
        .expect("begin compaction");
    job.commit().expect("commit compaction");
    engine.wait_for_gc();
    assert!(
        exists(vfs.as_ref(), &sidecar),
        "a pinned version still reads the retired segment's graph"
    );
    assert!(handle.release_snapshot(&pinned));
    engine.reap_snapshots();
    engine.wait_for_gc();
    assert!(
        !exists(vfs.as_ref(), &sidecar),
        "the sidecar left with its segment"
    );
    assert_no_orphans(vfs.as_ref(), &handle, "after the compaction");
}

/// Dropping a collection, and closing the engine, cancel a running index build instead of
/// waiting for it to finish.
#[test]
fn a_drop_and_a_shutdown_cancel_a_running_index_build() {
    // A graph build of this size and effort takes far longer than the bounds below.
    let slow = || EngineConfig {
        index: IndexPolicy {
            hnsw: logpose_index::graph::HnswParams {
                m: 48,
                ef_construction: 4_000,
                ..logpose_index::graph::HnswParams::default()
            },
            ..policy()
        },
        ..config()
    };
    for drop_collection in [true, false] {
        let fault = FaultVfs::new(100);
        let engine = open(fault.process(), slow());
        let handle = create(&engine, true);
        fill(&handle, 0, 1_000);
        handle.flush_blocking().expect("flush");
        wait_for("the index build to start", Duration::from_secs(30), || {
            handle.maintenance_status().in_progress.as_deref() == Some("index")
        });
        std::thread::sleep(Duration::from_millis(50));
        let started = Instant::now();
        if drop_collection {
            drop(handle);
            engine
                .drop_collection_blocking(&reference())
                .expect("drop collection");
        } else {
            drop(handle);
            drop(engine);
        }
        let took = started.elapsed();
        assert!(
            took < Duration::from_secs(10),
            "drop_collection={drop_collection}: the build was not cancelled ({took:?})"
        );
    }
}

/// An explicit compaction merges smallest first as far as the maintenance-memory pool allows
/// (more than one job when one cannot hold every segment), then builds the graphs of what it
/// settled on before it answers.
#[test]
fn an_explicit_compaction_converges_and_indexes_before_it_answers() {
    let fault = FaultVfs::new(110);
    // A 200,000-byte pool: the graph of a 300-row output (551 bytes a row at 8 dimensions)
    // fits it, and a 400-row one does not, so one job merges three of the six 100-row
    // segments, the next job the other three, and the two outputs cannot merge.
    let engine = open(
        fault.process(),
        EngineConfig {
            memory_limit: 1_000_000,
            maintenance_fraction: 0.2,
            ..config()
        },
    );
    let handle = create(&engine, false);
    for segment in 0..6 {
        fill(&handle, segment * 100, 100);
        handle.flush_blocking().expect("flush");
    }
    let expected = rows(&handle);
    handle.compact_blocking().expect("compact");
    let version = handle.current();
    let segments = version.counters.segment_count;
    assert_eq!(
        segments, 2,
        "the compaction merged as far as the pool allows"
    );
    assert_eq!(
        engine.scheduler().stats().compactions_granted,
        2,
        "one job could not hold every segment, so it took two"
    );
    assert_eq!(
        version.indexed_segments().0,
        segments as usize,
        "every settled segment got its index build before the compaction answered"
    );
    drop(version);
    assert_eq!(rows(&handle), expected);
    assert!(graphs(&engine).iter().all(|(_, graph)| *graph));
}

/// Once the collection is quiet, its segments below the graph threshold merge together, and a
/// lone small one gets a graph anyway.
#[test]
fn a_quiet_collection_merges_small_segments_and_indexes_the_rest() {
    let fault = FaultVfs::new(120);
    let clock = Arc::new(ManualClock::new());
    let engine = open(
        fault.process(),
        EngineConfig {
            clock: Some(clock.clone()),
            compaction: CompactionConfig {
                quiet_after: Duration::from_secs(10),
                ..CompactionConfig::default()
            },
            ..config()
        },
    );
    let handle = create(&engine, true);
    for segment in 0..3 {
        fill(&handle, segment * 30, 30);
        handle.flush_blocking().expect("flush");
    }
    let expected = rows(&handle);
    // Busy: three 30-row segments are below graph_min_rows (64) and below min_merge (4).
    engine
        .tick_writer(&handle, Duration::from_secs(30))
        .expect("tick");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(handle.current().counters.segment_count, 3);
    assert_eq!(handle.current().indexed_segments(), (0, 0));

    clock.advance(Duration::from_secs(11));
    engine
        .tick_writer(&handle, Duration::from_secs(30))
        .expect("tick");
    wait_for(
        "the quiet merge and index build",
        Duration::from_secs(30),
        || {
            let version = handle.current();
            version.counters.segment_count == 1 && version.indexed_segments() == (1, 1)
        },
    );
    assert_eq!(rows(&handle), expected);
    engine.wait_for_gc();
    assert_no_orphans(fault.process().as_ref(), &handle, "after the quiet merge");
}
