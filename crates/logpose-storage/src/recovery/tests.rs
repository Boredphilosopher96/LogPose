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
use logpose_types::{CollectionRef, DistanceMetric, LogPoseError, UnitId, VisibleRecord};
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
        .expect("flush: generation 1, unit 1");
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
    // The failed flush's output was unit 3 (unit 2 was the memtable it froze).
    assert!(exists(vfs.as_ref(), &segment_path(&dir, UnitId(3))));
    let seen = rows(&engine);
    drop(handle);
    drop(engine);

    // In-process reopen: CURRENT still names generation 1, and generation 2 and unit 3 are
    // orphans.
    let engine = open(controlled.clone());
    let first = rows(&engine);
    assert_eq!(first, seen);
    let handle = open_handle(&engine);
    assert_eq!(handle.current().manifest_generation, 1);
    assert!(!exists(vfs.as_ref(), &manifest_path(&dir, 2)));
    assert!(!exists(vfs.as_ref(), &segment_path(&dir, UnitId(3))));
    engine.core().flush_collection(&handle).expect("retry");
    let retried = handle.current();
    assert_eq!(retried.manifest_generation, 3, "generation 2 is burned");
    assert_eq!(
        retried.manifest.units().collect::<Vec<_>>(),
        [UnitId(1), UnitId(5)],
        "unit 3 is burned; the recovered memtable took unit 4"
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
    // the recovered memtable took unit 9, the flush output unit 10.
    write(&engine, &handle, "b", 2.0);
    engine.core().flush_collection(&handle).expect("flush");
    let version = handle.current();
    assert_eq!(version.manifest_generation, 10);
    assert_eq!(version.manifest.units().last(), Some(UnitId(10)));
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
