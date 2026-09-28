//! Recovery tests for manifest v2: crashes at every operation of a flush's publish, failed
//! publishes and in-process reopens settled by the durability barrier, id burning, and orphan
//! cleanup at open.

use crate::{
    CollectionHandle, CreateCollectionRequest, Engine, EngineConfig,
    dv::{dv_path, parse_dv_file_name},
    manifest::{CURRENT_FILE, manifest_path},
    paths::{parse_segment_file_name, segment_path},
    test_support::{ControlledVfs, put},
};
use logpose_types::{
    CollectionRef, CorruptionKind, DeleteRecord, DistanceMetric, LogPoseError, RecordId, UnitId,
    VisibleRecord, WriteOperation,
};
use logpose_vfs::{CrashPoint, FaultPlan, FaultVfs, OpenMode, TearMode, Vfs};
use logpose_wal::BootId;
use std::{io::IoSlice, path::Path, sync::Arc};

const ROOT: &str = "/storage";
const NAME: &str = "docs";

fn config() -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new("boot")),
        ..EngineConfig::default()
    }
}

fn open(vfs: Arc<dyn Vfs>) -> Engine {
    Engine::open(vfs, ROOT, config()).expect("engine should open")
}

/// Create the collection with background maintenance off.
fn create(engine: &Engine) -> Arc<CollectionHandle> {
    let mut descriptor = engine
        .core()
        .plan_collection_descriptor(&CreateCollectionRequest::new(NAME, 2, DistanceMetric::Dot))
        .expect("descriptor should plan");
    descriptor.flush_threshold_ops = usize::MAX;
    descriptor.flush_threshold_bytes = usize::MAX;
    descriptor.compaction_threshold_segments = usize::MAX;
    engine
        .create_collection(descriptor, None)
        .expect("collection should be created")
}

fn open_handle(engine: &Engine) -> Arc<CollectionHandle> {
    engine
        .collection(&CollectionRef::new_default(NAME))
        .expect("collection should be open")
}

fn write(engine: &Engine, handle: &Arc<CollectionHandle>, id: &str, x: f32) {
    engine
        .core()
        .write(handle, vec![put(id, vec![x, 1.0])])
        .expect("write");
}

fn rows(engine: &Engine) -> Vec<VisibleRecord> {
    let handle = open_handle(engine);
    handle
        .current()
        .check_invariants()
        .expect("invariants hold");
    engine
        .core()
        .scan_exact_internal(&handle, None, true, None)
        .expect("scan")
}

fn exists(vfs: &dyn Vfs, path: &Path) -> bool {
    logpose_vfs::exists(vfs, path).expect("exists")
}

/// Every segment and DV file in the collection is one the current manifest names, and every
/// manifest file is the current generation or the one kept below it.
fn assert_no_orphans(vfs: &dyn Vfs, handle: &CollectionHandle, context: &str) {
    let version = handle.current();
    let live = version.manifest.units().collect::<Vec<_>>();
    let dvs = version
        .manifest
        .segments
        .iter()
        .filter_map(|segment| segment.dv.map(|dv| (segment.unit, dv.generation)))
        .collect::<Vec<_>>();
    let dir = &handle.meta().dir;
    for unit in &live {
        let path = segment_path(dir, *unit);
        assert!(exists(vfs, &path), "{context}: live {}", path.display());
    }
    for (unit, generation) in &dvs {
        let path = dv_path(dir, *unit, *generation);
        assert!(exists(vfs, &path), "{context}: live {}", path.display());
    }
    for entry in vfs.list(&dir.join("segments")).expect("list") {
        let named = parse_segment_file_name(&entry.name).is_some_and(|unit| live.contains(&unit))
            || parse_dv_file_name(&entry.name).is_some_and(|dv| dvs.contains(&dv));
        assert!(named, "{context}: orphan segments/{}", entry.name);
    }
    let generations = vfs
        .list(&dir.join("manifests"))
        .expect("list")
        .into_iter()
        .filter_map(|entry| crate::manifest::parse_manifest_file_name(&entry.name))
        .collect::<Vec<_>>();
    assert!(
        generations.contains(&version.manifest_generation),
        "{context}"
    );
    assert!(generations.len() <= 2, "{context}: {generations:?}");
    assert!(
        generations
            .iter()
            .all(|generation| *generation <= version.manifest_generation),
        "{context}: {generations:?}"
    );
}

/// Flush crash analysis, exhaustively: a crash at every mutating operation of a flush (WAL
/// rotation, segment writes, the manifest publish, WAL cleanup), under every tear mode,
/// recovers the same rows, from either the old or the new manifest, with no orphan left.
#[test]
fn a_crash_at_every_op_of_a_flush_recovers_the_same_rows_without_orphans() {
    let prepare = |fault: &Arc<FaultVfs>| {
        let engine = open(fault.process());
        let handle = create(&engine);
        write(&engine, &handle, "a", 1.0);
        write(&engine, &handle, "b", 2.0);
        (engine, handle)
    };
    let clean = FaultVfs::new(40);
    let (engine, handle) = prepare(&clean);
    let expected = rows(&engine);
    let before = clean.mutating_ops();
    engine
        .core()
        .flush_collection(&handle)
        .expect("clean flush");
    engine.wait_for_gc();
    let ops = clean.mutating_ops() - before;
    assert_eq!(rows(&engine), expected);
    drop(handle);
    drop(engine);

    for tear in TearMode::ALL {
        for k in 0..ops {
            let context = format!("{tear:?}, crash after {k} of {ops} flush ops");
            let fault = FaultVfs::new(1000 + k);
            let (engine, handle) = prepare(&fault);
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let flushed = engine.core().flush_collection(&handle).is_ok();
            drop(handle);
            drop(engine);
            fault.crash();
            fault.set_plan(FaultPlan::default());

            let engine = open(fault.process());
            assert_eq!(rows(&engine), expected, "{context}");
            let handle = open_handle(&engine);
            let generation = handle.current().manifest_generation;
            assert!(generation <= 1, "{context}: generation {generation}");
            if flushed {
                assert_eq!(generation, 1, "{context}: an acknowledged flush is durable");
            }
            assert_no_orphans(fault.process().as_ref(), &handle, &context);
            // The collection keeps working: a new flush gets fresh names.
            write(&engine, &handle, "c", 3.0);
            engine
                .core()
                .flush_collection(&handle)
                .map_err(|error| format!("{context}: {error}"))
                .expect("a flush after recovery succeeds");
            assert_eq!(rows(&engine).len(), 3, "{context}");
        }
    }
}

/// I11: a crash at every operation of a recovery that has orphans to clean up, then a clean
/// recovery, ends where one uninterrupted recovery does.
#[test]
fn recovery_interrupted_at_any_op_converges() {
    let setup = |fault: &Arc<FaultVfs>| {
        let engine = open(fault.process());
        let handle = create(&engine);
        write(&engine, &handle, "a", 1.0);
        engine.core().flush_collection(&handle).expect("flush");
        write(&engine, &handle, "b", 2.0);
        // Stop a flush right after its CURRENT rename, before the directory sync.
        fault.set_plan(FaultPlan {
            crash_at: Some(CrashPoint::CurrentAfterRename),
            tear: TearMode::KeepRandomPrefix,
            ..FaultPlan::default()
        });
        let _ = engine.core().flush_collection(&handle);
        drop(handle);
        drop(engine);
        fault.crash();
        fault.set_plan(FaultPlan::default());
    };
    let reference = FaultVfs::new(50);
    setup(&reference);
    let before = reference.mutating_ops();
    let engine = open(reference.process());
    let expected = rows(&engine);
    let recovery_ops = reference.mutating_ops() - before;
    let expected_generation = open_handle(&engine).current().manifest_generation;
    drop(engine);

    for k in 0..recovery_ops {
        let fault = FaultVfs::new(50);
        setup(&fault);
        fault.set_plan(FaultPlan {
            crash_after_ops: Some(fault.mutating_ops() + k),
            ..FaultPlan::default()
        });
        drop(Engine::open(fault.process(), ROOT, config()));
        fault.crash();
        fault.set_plan(FaultPlan::default());
        let engine = open(fault.process());
        assert_eq!(rows(&engine), expected, "crash after {k} recovery ops");
        let handle = open_handle(&engine);
        assert_eq!(handle.current().manifest_generation, expected_generation);
        assert_no_orphans(fault.process().as_ref(), &handle, &format!("after {k} ops"));
    }
}

/// A failed `CURRENT` rename leaves the durable `CURRENT` unknown, so the collection is
/// poisoned. An in-process reopen settles it through the barrier, a crash after that changes
/// nothing, and no id the failed attempt used is ever issued again.
#[test]
fn a_failed_current_rename_poisons_then_reopens_agree_and_ids_are_never_reused() {
    let fault = FaultVfs::new(60);
    let controlled = ControlledVfs::wrap(fault.process());
    let engine = open(controlled.clone());
    let handle = create(&engine);
    write(&engine, &handle, "a", 1.0);
    engine
        .core()
        .flush_collection(&handle)
        .expect("flush: generation 1, unit 2");
    write(&engine, &handle, "b", 2.0);

    controlled.fail_renames_to(CURRENT_FILE, 1);
    engine
        .core()
        .flush_collection(&handle)
        .expect_err("the rename fails");
    assert!(handle.is_poisoned());
    let error = engine
        .core()
        .write(&handle, vec![put("c", vec![1.0, 1.0])])
        .expect_err("poisoned");
    assert!(
        matches!(error, LogPoseError::CollectionPoisoned { .. }),
        "{error}"
    );
    let dir = handle.meta().dir.clone();
    let vfs = fault.process();
    assert!(
        exists(vfs.as_ref(), &manifest_path(&dir, 2)),
        "the failed attempt wrote generation 2"
    );
    // The failed flush's output was unit 4 (the freeze gave the new memtable unit 3).
    assert!(exists(vfs.as_ref(), &segment_path(&dir, UnitId(4))));
    let seen = rows(&engine);
    drop(handle);
    drop(engine);

    // In-process reopen: CURRENT still names generation 1, and generation 2 and unit 4 are
    // orphans.
    let engine = open(controlled.clone());
    let first = rows(&engine);
    assert_eq!(first, seen);
    let handle = open_handle(&engine);
    assert_eq!(handle.current().manifest_generation, 1);
    assert!(!exists(vfs.as_ref(), &manifest_path(&dir, 2)));
    assert!(!exists(vfs.as_ref(), &segment_path(&dir, UnitId(4))));
    engine.core().flush_collection(&handle).expect("retry");
    let retried = handle.current();
    assert_eq!(retried.manifest_generation, 3, "generation 2 is burned");
    assert_eq!(
        retried.manifest.units().collect::<Vec<_>>(),
        [UnitId(2), UnitId(7)],
        "unit 4 is burned; the recovered memtable took unit 5, the next one unit 6"
    );
    let after_retry = rows(&engine);
    drop((handle, retried));
    drop(engine);

    fault.crash();
    let engine = open(fault.process());
    assert_eq!(rows(&engine), after_retry);
    assert_eq!(after_retry, first);
    assert_eq!(open_handle(&engine).current().manifest_generation, 3);
}

/// Fail a sync, reopen in-process, crash, reopen again: both opens agree. The failed sync is the
/// collection directory's, after the `CURRENT` rename, so the first reopen reads a `CURRENT`
/// that is only in the page cache until its barrier makes it durable.
#[test]
fn a_failed_sync_then_in_process_reopen_then_crash_reopens_agree() {
    for (step, fail) in [
        ("collection directory sync", 0),
        ("CURRENT.tmp sync", 1),
        ("manifest file sync", 2),
    ] {
        let fault = FaultVfs::new(70);
        let controlled = ControlledVfs::wrap(fault.process());
        let engine = open(controlled.clone());
        let handle = create(&engine);
        write(&engine, &handle, "a", 1.0);
        write(&engine, &handle, "b", 2.0);
        let dir = handle.meta().dir.clone();
        match fail {
            0 => controlled.fail_dir_syncs(&dir, 1),
            1 => controlled.fail_file_syncs_containing("CURRENT.tmp", 1),
            _ => controlled.fail_file_syncs_containing(".mf", 1),
        }
        engine
            .core()
            .flush_collection(&handle)
            .expect_err("the sync fails");
        assert_eq!(handle.is_poisoned(), fail == 0, "{step}");
        drop(handle);
        drop(engine);

        let engine = open(controlled.clone());
        let first = rows(&engine);
        let first_generation = open_handle(&engine).current().manifest_generation;
        assert_eq!(first.len(), 2, "{step}");
        drop(engine);

        fault.crash();
        let engine = open(fault.process());
        assert_eq!(rows(&engine), first, "{step}");
        assert_eq!(
            open_handle(&engine).current().manifest_generation,
            first_generation,
            "{step}: both opens chose the same manifest"
        );
    }
}

/// Files no durable manifest references are removed at open; live ones never are.
#[test]
fn orphans_left_in_a_collection_are_removed_at_open_and_live_files_kept() {
    let fault = FaultVfs::new(80);
    let engine = open(fault.process());
    let handle = create(&engine);
    write(&engine, &handle, "a", 1.0);
    engine.core().flush_collection(&handle).expect("flush");
    let expected = rows(&engine);
    let dir = handle.meta().dir.clone();
    drop(handle);
    drop(engine);

    let vfs = fault.process();
    let planted = [
        segment_path(&dir, UnitId(7)),
        dv_path(&dir, UnitId(7), 4),
        dir.join("segments").join("00000008.seg.tmp"),
        manifest_path(&dir, 9),
        dir.join("CURRENT.tmp"),
    ];
    for path in &planted {
        let file = vfs.open(path, OpenMode::CreateNew).expect("plant");
        file.append(&[IoSlice::new(b"orphan")]).expect("write");
        file.sync_all().expect("sync");
    }
    let engine = open(fault.process());
    assert_eq!(rows(&engine), expected);
    let handle = open_handle(&engine);
    for path in &planted {
        assert!(!exists(vfs.as_ref(), path), "{}", path.display());
    }
    assert_no_orphans(vfs.as_ref(), &handle, "planted");
    // Unit 8, DV generation 4, and manifest generation 9 were seen, so they are never issued:
    // the recovered memtable took unit 9, the one the flush froze it for unit 10, and the
    // flush output unit 11.
    write(&engine, &handle, "b", 2.0);
    engine.core().flush_collection(&handle).expect("flush");
    let version = handle.current();
    assert_eq!(version.manifest_generation, 10);
    assert_eq!(version.manifest.units().last(), Some(UnitId(11)));
    assert!(version.manifest.next_dv_gen >= 5);
}

/// A leftover whose name uses the last manifest generation leaves no generation to issue after
/// it: a flush then fails cleanly instead of overflowing the counter.
#[test]
fn a_leftover_at_the_last_generation_fails_the_next_publish_cleanly() {
    let fault = FaultVfs::new(85);
    let engine = open(fault.process());
    let handle = create(&engine);
    let dir = handle.meta().dir.clone();
    drop(handle);
    drop(engine);
    let vfs = fault.process();
    let file = vfs
        .open(&manifest_path(&dir, u64::MAX), OpenMode::CreateNew)
        .expect("plant");
    file.append(&[IoSlice::new(b"orphan")]).expect("write");
    file.sync_all().expect("sync");

    let engine = open(fault.process());
    let handle = open_handle(&engine);
    write(&engine, &handle, "a", 1.0);
    let error = engine
        .core()
        .flush_collection(&handle)
        .expect_err("no manifest generation is left");
    assert!(matches!(error, LogPoseError::Internal { .. }), "{error}");
    assert!(!handle.is_poisoned());
    assert_eq!(rows(&engine).len(), 1, "the write stays readable");
}

/// A version 1 layout (a `CURRENT` that is not a 21-byte pointer) fails the collection's open
/// with manifest corruption and changes nothing.
#[test]
fn a_version_1_current_fails_the_open_as_manifest_corruption() {
    let fault = FaultVfs::new(90);
    let engine = open(fault.process());
    let dir = create(&engine).meta().dir.clone();
    drop(engine);
    let vfs = fault.process();
    let current = dir.join(CURRENT_FILE);
    vfs.remove_file(&current).expect("remove");
    let file = vfs.open(&current, OpenMode::CreateNew).expect("create");
    file.append(&[IoSlice::new(b"0")]).expect("write");
    let engine = open(fault.process());
    let error = engine
        .collection(&CollectionRef::new_default(NAME))
        .expect_err("refused");
    assert!(error.to_string().contains("manifest v2 pointer"), "{error}");
    assert!(
        exists(vfs.as_ref(), &manifest_path(&dir, 0)),
        "nothing was removed"
    );
}

/// Named crash points of garbage collection and orphan cleanup: a crash while removing the
/// files a compaction retired leaves the compaction durable, and one right after orphan
/// cleanup at open leaves nothing a rerun would change.
#[test]
fn named_gc_and_orphan_cleanup_crash_points_have_the_documented_outcome() {
    let fault = FaultVfs::new(95);
    let engine = open(fault.process());
    let handle = create(&engine);
    write(&engine, &handle, "a", 1.0);
    engine.core().flush_collection(&handle).expect("flush");
    write(&engine, &handle, "b", 2.0);
    engine.core().flush_collection(&handle).expect("flush");
    let expected = rows(&engine);
    fault.set_plan(FaultPlan {
        crash_at: Some(CrashPoint::GcAfterRemove),
        tear: TearMode::KeepRandomPrefix,
        ..FaultPlan::default()
    });
    engine
        .core()
        .compact_collection(&handle)
        .expect("the compaction commits before its inputs are collected");
    let compacted = handle.current().manifest_generation;
    engine.wait_for_gc();
    assert!(
        fault
            .crash_points_hit()
            .contains(&CrashPoint::GcAfterRemove)
    );
    drop(handle);
    drop(engine);
    fault.crash();

    // The next open is interrupted right after its orphan cleanup.
    fault.set_plan(FaultPlan {
        crash_at: Some(CrashPoint::RecoveryAfterOrphanCleanup),
        ..FaultPlan::default()
    });
    drop(Engine::open(fault.process(), ROOT, config()));
    assert!(
        fault
            .crash_points_hit()
            .contains(&CrashPoint::RecoveryAfterOrphanCleanup)
    );
    fault.crash();
    fault.set_plan(FaultPlan::default());

    let engine = open(fault.process());
    assert_eq!(rows(&engine), expected);
    let handle = open_handle(&engine);
    assert_eq!(handle.current().manifest_generation, compacted);
    assert_eq!(handle.current().manifest.segments.len(), 1);
    assert_no_orphans(
        fault.process().as_ref(),
        &handle,
        "after GC and cleanup crashes",
    );
}

/// A collection with one segment `{a, b, c}` and, above the checkpoint, an upsert of `a`, a
/// delete of `b`, and a new `d`: its next flush writes a DV file for the segment.
fn with_segment_deletions(fault: &Arc<FaultVfs>) -> (Engine, Arc<CollectionHandle>) {
    let engine = open(fault.process());
    let handle = create(&engine);
    write(&engine, &handle, "a", 1.0);
    write(&engine, &handle, "b", 2.0);
    write(&engine, &handle, "c", 3.0);
    engine.core().flush_collection(&handle).expect("flush");
    write(&engine, &handle, "a", 10.0);
    engine
        .core()
        .write(
            &handle,
            vec![WriteOperation::Delete(DeleteRecord {
                id: RecordId::new("b"),
            })],
        )
        .expect("delete");
    write(&engine, &handle, "d", 4.0);
    (engine, handle)
}

/// The primary-key index a recovery rebuilt resolves every key to its live row: an upsert of
/// every live key leaves exactly one live row per key (checked by the version invariants,
/// before and after a flush) and changes no row count.
fn assert_index_resolves_every_key(engine: &Engine, context: &str) {
    let before = rows(engine);
    let handle = open_handle(engine);
    for record in &before {
        write(engine, &handle, record.id.as_str(), 100.0);
    }
    let after = rows(engine);
    assert_eq!(
        after.iter().map(|record| &record.id).collect::<Vec<_>>(),
        before.iter().map(|record| &record.id).collect::<Vec<_>>(),
        "{context}"
    );
    assert!(
        after.iter().all(|record| record.vector == [100.0, 1.0]),
        "{context}: {after:?}"
    );
    engine
        .core()
        .flush_collection(&handle)
        .map_err(|error| format!("{context}: {error}"))
        .expect("flush after the upserts");
    assert_eq!(rows(engine), after, "{context}");
}

/// Flush crash analysis with deletion vectors, exhaustively: a crash at every mutating
/// operation of a flush that writes a segment and a DV file (including right after the DV
/// file's sync), under every tear mode, recovers the same rows with the segment's deletions
/// (from the DV file or replayed from the WAL), no orphan, and a primary-key index that
/// resolves every key.
#[test]
fn a_crash_at_every_op_of_a_flush_that_writes_a_dv_file_recovers_the_same_state() {
    let clean = FaultVfs::new(70);
    let (engine, handle) = with_segment_deletions(&clean);
    let expected = rows(&engine);
    let segment = handle.current().manifest.segments[0].unit;
    let before = clean.mutating_ops();
    engine
        .core()
        .flush_collection(&handle)
        .expect("clean flush");
    engine.wait_for_gc();
    let ops = clean.mutating_ops() - before;
    assert_eq!(rows(&engine), expected);
    let version = handle.current();
    assert_eq!(
        version.manifest.segments[0].dv.map(|dv| dv.cardinality),
        Some(2),
        "the flush wrote the segment's DV file"
    );
    drop((version, handle));
    drop(engine);

    for tear in TearMode::ALL {
        for k in 0..ops {
            let context = format!("{tear:?}, crash after {k} of {ops} flush ops");
            let fault = FaultVfs::new(2000 + k);
            let (engine, handle) = with_segment_deletions(&fault);
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let flushed = engine.core().flush_collection(&handle).is_ok();
            drop(handle);
            drop(engine);
            fault.crash();
            fault.set_plan(FaultPlan::default());

            let engine = open(fault.process());
            assert_eq!(rows(&engine), expected, "{context}");
            let handle = open_handle(&engine);
            let version = handle.current();
            assert!(version.manifest_generation <= 2, "{context}");
            if flushed {
                assert_eq!(version.manifest_generation, 2, "{context}: durable");
            }
            assert_eq!(
                version.deletes.len_of(segment),
                2,
                "{context}: a's and b's rows stay deleted"
            );
            drop(version);
            assert_no_orphans(fault.process().as_ref(), &handle, &context);
            drop(handle);
            assert_index_resolves_every_key(&engine, &context);
        }
    }
}

/// Three segments `{a, b}`, `{c, d}`, `{e, f}`, and a compaction of all three that the test
/// steps by hand: begun, then, while it builds, an upsert of `a`, a delete of `c`, and a new
/// `g`, so the commit reconciles two deletions onto the output and writes its DV file.
fn compaction_in_progress(
    fault: &Arc<FaultVfs>,
) -> (
    Engine,
    Arc<CollectionHandle>,
    crate::handle::JobTicket,
    crate::writer::JobStart,
) {
    let engine = open(fault.process());
    let handle = create(&engine);
    for pair in [["a", "b"], ["c", "d"], ["e", "f"]] {
        for id in pair {
            write(&engine, &handle, id, 1.0);
        }
        engine.core().flush_collection(&handle).expect("flush");
    }
    let (ticket, start) = handle
        .begin_job(crate::writer::JobKind::Compact)
        .expect("the compaction begins");
    write(&engine, &handle, "a", 10.0);
    engine
        .core()
        .write(
            &handle,
            vec![WriteOperation::Delete(DeleteRecord {
                id: RecordId::new("c"),
            })],
        )
        .expect("delete");
    write(&engine, &handle, "g", 7.0);
    (engine, handle, ticket, start)
}

/// Build and commit the compaction [`compaction_in_progress`] began. Returns whether it
/// committed.
fn finish_compaction(
    engine: &Engine,
    handle: &Arc<CollectionHandle>,
    mut ticket: crate::handle::JobTicket,
    start: crate::writer::JobStart,
) -> bool {
    let crate::writer::JobWork::Compact(work) = &start.work else {
        unreachable!("three segments to compact");
    };
    let built =
        engine
            .core()
            .build_compaction(handle, &start.version, start.unit, work, &mut ticket);
    drop(start);
    match built {
        Ok(commit) => ticket.commit(commit).is_ok(),
        Err(_) => false,
    }
}

/// Compaction crash analysis, exhaustively: a crash at every mutating operation of a
/// compaction whose commit reconciles deletions onto its output (the output segment, its sync
/// and directory sync, the output's DV file, the manifest publish, and the removal of the
/// inputs), under every tear mode, recovers the same rows from either the old or the new
/// manifest, with no orphan, the reconciled deletions in force, and a primary-key index that
/// resolves every key.
#[test]
fn a_crash_at_every_op_of_a_compaction_recovers_the_same_state() {
    let clean = FaultVfs::new(90);
    let (engine, handle, ticket, start) = compaction_in_progress(&clean);
    let expected = rows(&engine);
    let before = clean.mutating_ops();
    assert!(finish_compaction(&engine, &handle, ticket, start));
    engine.wait_for_gc();
    let ops = clean.mutating_ops() - before;
    assert_eq!(rows(&engine), expected);
    let version = handle.current();
    assert_eq!(version.manifest_generation, 4);
    assert_eq!(version.manifest.segments.len(), 1);
    assert_eq!(
        version.manifest.segments[0].dv.map(|dv| dv.cardinality),
        Some(2),
        "a's and c's copies are deleted in the output's DV file"
    );
    drop((version, handle));
    drop(engine);

    for tear in TearMode::ALL {
        for k in 0..=ops {
            let context = format!("{tear:?}, crash after {k} of {ops} compaction ops");
            let fault = FaultVfs::new(3000 + k);
            let (engine, handle, ticket, start) = compaction_in_progress(&fault);
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let committed = finish_compaction(&engine, &handle, ticket, start);
            engine.wait_for_gc();
            drop(handle);
            drop(engine);
            fault.crash();
            fault.set_plan(FaultPlan::default());

            let engine = open(fault.process());
            assert_eq!(rows(&engine), expected, "{context}");
            let handle = open_handle(&engine);
            let version = handle.current();
            assert!(version.manifest_generation <= 4, "{context}");
            if committed {
                assert_eq!(version.manifest_generation, 4, "{context}: durable");
            }
            match version.manifest_generation {
                4 => assert_eq!(version.segments.len(), 1, "{context}"),
                _ => assert_eq!(version.segments.len(), 3, "{context}"),
            }
            drop(version);
            assert_no_orphans(fault.process().as_ref(), &handle, &context);
            drop(handle);
            assert_index_resolves_every_key(&engine, &context);
        }
    }
}

/// The named compaction crash points: after the output's sync nothing is published, so the
/// output is an orphan; after the output DV file's sync the same holds for both files. Either
/// way recovery keeps the inputs and replays the deletions that landed during the job.
#[test]
fn named_compaction_crash_points_leave_the_inputs_in_force() {
    for (seed, point) in [
        (91, CrashPoint::CompactionAfterOutputSync),
        (92, CrashPoint::CompactionAfterDvSync),
    ] {
        let fault = FaultVfs::new(seed);
        let (engine, handle, ticket, start) = compaction_in_progress(&fault);
        let expected = rows(&engine);
        fault.set_plan(FaultPlan {
            crash_at: Some(point),
            ..FaultPlan::default()
        });
        assert!(
            !finish_compaction(&engine, &handle, ticket, start),
            "{point:?}"
        );
        drop(handle);
        drop(engine);
        fault.crash();
        fault.set_plan(FaultPlan::default());
        let engine = open(fault.process());
        assert_eq!(rows(&engine), expected, "{point:?}");
        let handle = open_handle(&engine);
        assert_eq!(handle.current().manifest_generation, 3, "{point:?}");
        assert_eq!(handle.current().segments.len(), 3, "{point:?}");
        assert_no_orphans(fault.process().as_ref(), &handle, &format!("{point:?}"));
        drop(handle);
        assert_index_resolves_every_key(&engine, &format!("{point:?}"));
    }
}

/// The named DV crash point: the segment and the DV file are synced, the manifest is not
/// published, so recovery keeps the previous manifest, replays the deletions from the WAL, and
/// removes both files as orphans.
#[test]
fn a_crash_after_the_flush_dv_sync_replays_the_deletions() {
    let fault = FaultVfs::new(71);
    let (engine, handle) = with_segment_deletions(&fault);
    let expected = rows(&engine);
    fault.set_plan(FaultPlan {
        crash_at: Some(CrashPoint::FlushAfterDvSync),
        ..FaultPlan::default()
    });
    engine
        .core()
        .flush_collection(&handle)
        .expect_err("the crash stops the flush");
    drop(handle);
    drop(engine);
    fault.crash();
    fault.set_plan(FaultPlan::default());
    let engine = open(fault.process());
    assert_eq!(rows(&engine), expected);
    let handle = open_handle(&engine);
    let version = handle.current();
    assert_eq!(version.manifest_generation, 1);
    assert!(version.manifest.segments[0].dv.is_none());
    assert_eq!(version.deletes.len_of(version.manifest.segments[0].unit), 2);
    drop(version);
    assert_no_orphans(fault.process().as_ref(), &handle, "FlushAfterDvSync");
    drop(handle);
    assert_index_resolves_every_key(&engine, "FlushAfterDvSync");
}

/// Rewrite `path` with the byte at `offset` flipped, durably.
fn flip_byte(vfs: &dyn Vfs, path: &Path, offset: usize) {
    let mut bytes = logpose_vfs::read_file(vfs, path).expect("read");
    bytes[offset] ^= 0x10;
    rewrite(vfs, path, &bytes);
}

fn rewrite(vfs: &dyn Vfs, path: &Path, bytes: &[u8]) {
    vfs.remove_file(path).expect("remove");
    let file = vfs.open(path, OpenMode::CreateNew).expect("create");
    file.append(&[IoSlice::new(bytes)]).expect("append");
    file.sync_all().expect("sync");
    vfs.sync_dir(logpose_vfs::parent_dir(path))
        .expect("sync dir");
}

fn truncate_to_half(vfs: &dyn Vfs, path: &Path) {
    let bytes = logpose_vfs::read_file(vfs, path).expect("read");
    rewrite(vfs, path, &bytes[..bytes.len() / 2]);
}

fn drop_last_byte(vfs: &dyn Vfs, path: &Path) {
    let bytes = logpose_vfs::read_file(vfs, path).expect("read");
    rewrite(vfs, path, &bytes[..bytes.len() - 1]);
}

fn flip_last_byte(vfs: &dyn Vfs, path: &Path) {
    let len = logpose_vfs::read_file(vfs, path).expect("read").len();
    flip_byte(vfs, path, len - 1);
}

/// A damaged segment or DV file fails the collection's open with typed corruption of its own
/// kind (other collections are unaffected; see the engine tests), never with wrong rows. A
/// segment's header, footer, and length are checked at open, its sections when read.
#[test]
fn a_damaged_segment_or_dv_file_fails_the_open_with_its_corruption_kind() {
    type Damage = fn(&dyn Vfs, &Path);
    let cases: [(&str, bool, Damage, CorruptionKind); 7] = [
        (
            "dv magic",
            true,
            |vfs, path| flip_byte(vfs, path, 0),
            CorruptionKind::DeletionVector,
        ),
        (
            "dv unit",
            true,
            |vfs, path| flip_byte(vfs, path, 9),
            CorruptionKind::DeletionVector,
        ),
        (
            "dv bitmap",
            true,
            |vfs, path| flip_byte(vfs, path, 44),
            CorruptionKind::DeletionVector,
        ),
        (
            "dv checksum",
            true,
            flip_last_byte,
            CorruptionKind::DeletionVector,
        ),
        (
            "dv truncated",
            true,
            truncate_to_half,
            CorruptionKind::DeletionVector,
        ),
        (
            "segment header",
            false,
            |vfs, path| flip_byte(vfs, path, 12),
            CorruptionKind::Segment,
        ),
        (
            "segment truncated",
            false,
            drop_last_byte,
            CorruptionKind::Segment,
        ),
    ];
    for (index, (name, dv_file, damage, kind)) in (0_u64..).zip(cases) {
        let fault = FaultVfs::new(3000 + index);
        let (engine, handle) = with_segment_deletions(&fault);
        engine.core().flush_collection(&handle).expect("flush");
        let entry = handle.current().manifest.segments[0].clone();
        let dv = entry.dv.expect("the flush wrote a DV file");
        let dir = handle.meta().dir.clone();
        drop(handle);
        drop(engine);
        let path = if dv_file {
            dv_path(&dir, entry.unit, dv.generation)
        } else {
            segment_path(&dir, entry.unit)
        };
        damage(fault.process().as_ref(), &path);

        let engine = open(fault.process());
        let error = engine
            .collection(&CollectionRef::new_default(NAME))
            .expect_err("the damaged collection fails its open");
        assert!(
            matches!(&error, LogPoseError::Corrupt { kind: found, .. } if *found == kind),
            "{name}: {error:?}"
        );
    }
}
