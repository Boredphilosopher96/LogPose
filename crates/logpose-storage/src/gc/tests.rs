//! Garbage collection tests: orphan cleanup on `FaultVfs`, and version-refcounted removal of
//! segment files, superseded manifests, and checkpointed WAL files through the engine.

use super::*;
use crate::{
    CollectionHandle, CreateCollectionRequest, Engine, EngineConfig,
    manifest::{Manifest, manifest_path, publish_manifest},
    paths::UnitFiles,
    test_support::{ControlledVfs, put},
};
use logpose_types::{CollectionId, CollectionRef, DistanceMetric, legacy::legacy_schema};
use logpose_vfs::{FaultPlan, FaultVfs, OpenMode, TearMode};
use logpose_wal::BootId;
use std::io::IoSlice;
use uuid::Uuid;

const DIR: &str = "/c";

fn touch(vfs: &dyn Vfs, path: &Path) {
    vfs.create_dir_all(parent_dir(path)).expect("parent");
    let file = vfs.open(path, OpenMode::CreateNew).expect("create");
    file.append(&[IoSlice::new(b"x")]).expect("append");
    file.sync_all().expect("sync");
    vfs.sync_dir(parent_dir(path)).expect("sync dir");
}

fn exists_at(vfs: &dyn Vfs, path: &Path) -> bool {
    logpose_vfs::exists(vfs, path).expect("exists")
}

/// A manifest at `generation` holding the (empty-detail) segments `units`.
fn manifest(generation: u64, units: &[u32]) -> Manifest {
    let schema = legacy_schema(2, DistanceMetric::Dot).expect("schema");
    let mut manifest = Manifest::empty(CollectionId(Uuid::from_u128(7)), schema);
    manifest.generation = generation;
    manifest.next_unit_id = units.iter().max().map_or(0, |unit| unit + 1);
    manifest.segments = units
        .iter()
        .map(|unit| crate::manifest::ManifestSegment {
            unit: UnitId(*unit),
            file_len: 1,
            footer_crc: 0,
            row_count: 1,
            schema_version: 1,
            min_seq_no: 1,
            max_seq_no: 1,
            origin: crate::manifest::SegmentOrigin::Flush {
                first_seq_no: 1,
                last_seq_no: 1,
            },
            tier: 0,
            dv: None,
            vectors: Vec::new(),
            zones: Vec::new(),
            legacy: None,
        })
        .collect();
    manifest
}

/// A collection directory holding the live unit 1, orphans of units 2 and 9, staged files,
/// and manifest generations 1, 3, 4 (current) and 6.
fn populated(vfs: &dyn Vfs) -> (Manifest, Vec<PathBuf>, Vec<PathBuf>) {
    let dir = Path::new(DIR);
    vfs.create_dir_all(&manifests_dir(dir)).expect("manifests");
    vfs.sync_dir(Path::new("/")).expect("sync /");
    vfs.sync_dir(dir).expect("sync dir");
    for generation in [1, 3, 6] {
        publish_manifest(vfs, dir, &manifest(generation, &[1])).expect("publish");
    }
    let current = manifest(4, &[1]);
    publish_manifest(vfs, dir, &current).expect("publish current");
    let live = UnitFiles::new(dir, UnitId(1)).published();
    let mut orphans = UnitFiles::new(dir, UnitId(2)).published();
    orphans.extend(UnitFiles::new(dir, UnitId(9)).all());
    orphans.extend([
        dir.join(SEGMENTS_DIR).join("00000001.lps.tmp"),
        dir.join(TMP_DIR).join("anything"),
        dir.join(CURRENT_TEMP_FILE),
        manifest_path(dir, 1),
        manifest_path(dir, 6),
    ]);
    for path in live.iter().chain(&orphans) {
        if !exists_at(vfs, path) {
            touch(vfs, path);
        }
    }
    let foreign = dir.join(SEGMENTS_DIR).join("README");
    touch(vfs, &foreign);
    let mut kept = live;
    kept.extend([foreign, manifest_path(dir, 3), manifest_path(dir, 4)]);
    (current, kept, orphans)
}

#[test]
fn orphan_cleanup_removes_exactly_the_files_the_manifest_does_not_reference() {
    let vfs = FaultVfs::new(1);
    let (current, kept, orphans) = populated(vfs.as_ref());
    let cleanup = remove_orphans(vfs.as_ref(), Path::new(DIR), &current).expect("cleanup");
    for path in &kept {
        assert!(exists_at(vfs.as_ref(), path), "kept {}", path.display());
    }
    for path in &orphans {
        assert!(!exists_at(vfs.as_ref(), path), "removed {}", path.display());
    }
    assert_eq!(cleanup.previous_generation, Some(3));
    assert_eq!(
        cleanup.next_manifest_gen, 7,
        "above generation 6 that was on disk"
    );
    assert_eq!(cleanup.next_unit_id, 10, "above unit 9 that was on disk");
    assert!(
        vfs.crash_points_hit()
            .contains(&CrashPoint::RecoveryAfterOrphanCleanup)
    );

    // The removals are durable, and a second pass changes nothing.
    vfs.crash();
    for path in &orphans {
        assert!(
            !exists_at(vfs.as_ref(), path),
            "stays removed {}",
            path.display()
        );
    }
    let again = remove_orphans(vfs.as_ref(), Path::new(DIR), &current).expect("again");
    assert!(again.removed.is_empty());
}

/// I11: orphan cleanup that crashes at any operation and runs again ends in the same state as
/// one uninterrupted run, and never removes a live file.
#[test]
fn orphan_cleanup_interrupted_at_any_op_converges_and_never_removes_a_live_file() {
    let clean = FaultVfs::new(2);
    let (current, kept, orphans) = populated(clean.as_ref());
    let before = clean.mutating_ops();
    remove_orphans(clean.as_ref(), Path::new(DIR), &current).expect("clean run");
    let ops = clean.mutating_ops() - before;
    for tear in TearMode::ALL {
        for k in 0..ops {
            let vfs = FaultVfs::new(100 + k);
            populated(vfs.as_ref());
            vfs.set_plan(FaultPlan {
                crash_after_ops: Some(vfs.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            assert!(remove_orphans(vfs.as_ref(), Path::new(DIR), &current).is_err());
            vfs.crash();
            for path in &kept {
                assert!(
                    exists_at(vfs.as_ref(), path),
                    "{tear:?} {k}: {}",
                    path.display()
                );
            }
            remove_orphans(vfs.as_ref(), Path::new(DIR), &current).expect("rerun");
            for path in &orphans {
                assert!(
                    !exists_at(vfs.as_ref(), path),
                    "{tear:?} {k}: {}",
                    path.display()
                );
            }
        }
    }
}

/// The barrier makes a `CURRENT` rename that only reached the page cache durable, so what
/// recovery reads before a crash is what it reads after one.
#[test]
fn the_durability_barrier_makes_an_unsynced_current_durable() {
    let vfs = FaultVfs::new(3);
    let dir = Path::new(DIR);
    vfs.create_dir_all(&manifests_dir(dir)).expect("manifests");
    vfs.sync_dir(Path::new("/")).expect("sync /");
    vfs.sync_dir(dir).expect("sync");
    publish_manifest(vfs.as_ref(), dir, &manifest(0, &[])).expect("publish 0");
    // Generation 1 up to its rename, without the directory sync.
    vfs.set_plan(FaultPlan {
        crash_at: Some(CrashPoint::CurrentAfterRename),
        ..FaultPlan::default()
    });
    publish_manifest(vfs.as_ref(), dir, &manifest(1, &[])).expect_err("stopped at the rename");
    vfs.crash();
    assert_eq!(
        crate::manifest::read_current(vfs.as_ref(), dir).expect("CURRENT"),
        0
    );

    let with_barrier = FaultVfs::new(3);
    with_barrier
        .create_dir_all(&manifests_dir(dir))
        .expect("manifests");
    with_barrier.sync_dir(Path::new("/")).expect("sync /");
    with_barrier.sync_dir(dir).expect("sync");
    publish_manifest(with_barrier.as_ref(), dir, &manifest(0, &[])).expect("publish 0");
    let controlled = ControlledVfs::wrap(with_barrier.process());
    controlled.fail_dir_syncs(dir, 1);
    let failure = publish_manifest(controlled.as_ref(), dir, &manifest(1, &[]))
        .expect_err("the directory sync fails");
    assert!(failure.current_unknown);
    let seen = crate::manifest::read_current(with_barrier.as_ref(), dir).expect("CURRENT");
    durability_barrier(with_barrier.as_ref(), dir).expect("barrier");
    with_barrier.crash();
    assert_eq!(
        crate::manifest::read_current(with_barrier.as_ref(), dir).expect("CURRENT"),
        seen,
        "the barrier made what recovery saw durable"
    );
}

// Engine-level collection.

const ROOT: &str = "/storage";

fn config() -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new("boot")),
        ..EngineConfig::default()
    }
}

/// Create `name` with background maintenance off.
fn create(engine: &Engine, name: &str) -> Arc<CollectionHandle> {
    let mut descriptor = engine
        .core()
        .plan_collection_descriptor(&CreateCollectionRequest::new(name, 2, DistanceMetric::Dot))
        .expect("descriptor should plan");
    descriptor.flush_threshold_ops = usize::MAX;
    descriptor.flush_threshold_bytes = usize::MAX;
    descriptor.compaction_threshold_segments = usize::MAX;
    engine
        .create_collection(descriptor, None)
        .expect("collection should be created")
}

fn write(engine: &Engine, handle: &Arc<CollectionHandle>, id: &str) {
    engine
        .core()
        .write(handle, vec![put(id, vec![1.0, 0.0])])
        .expect("write");
}

fn unit_files_exist(vfs: &dyn Vfs, handle: &CollectionHandle, unit: UnitId) -> bool {
    let files = UnitFiles::new(&handle.meta().dir, unit).published();
    let present = files.iter().filter(|path| exists_at(vfs, path)).count();
    assert!(
        present == 0 || present == files.len(),
        "a segment is whole or gone"
    );
    present == files.len()
}

fn wal_files(vfs: &dyn Vfs, handle: &CollectionHandle) -> Vec<String> {
    let mut names = vfs
        .list(&handle.meta().dir.join("wal"))
        .expect("list wal")
        .into_iter()
        .map(|entry| entry.name)
        .filter(|name| name.ends_with(".wal"))
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// I7: a compacted-away segment stays on disk while any `Version` holding it lives, a
/// token-pinned one included, and is removed once the last one is released.
#[test]
fn a_segment_is_removed_only_after_the_last_version_and_token_holding_it_are_released() {
    let fault = FaultVfs::new(31);
    let engine = Engine::open(fault.process(), ROOT, config()).expect("engine should open");
    let handle = create(&engine, "gc");
    let core = engine.core();
    write(&engine, &handle, "a");
    core.flush_collection(&handle).expect("flush");
    write(&engine, &handle, "b");
    core.flush_collection(&handle).expect("flush");
    let inputs = handle.current().manifest.units().collect::<Vec<_>>();
    assert_eq!(inputs.len(), 2);

    let held = handle.current();
    let token = handle.pin_snapshot().expect("pin");
    core.compact_collection(&handle).expect("compact");
    let output = handle.current().manifest.units().collect::<Vec<_>>();
    assert_eq!(output.len(), 1);
    assert!(
        !inputs.contains(&output[0]),
        "a compaction output gets a fresh unit"
    );

    engine.wait_for_gc();
    let vfs = fault.process();
    for unit in &inputs {
        assert!(
            unit_files_exist(vfs.as_ref(), &handle, *unit),
            "held by both"
        );
    }
    drop(held);
    engine.wait_for_gc();
    for unit in &inputs {
        assert!(
            unit_files_exist(vfs.as_ref(), &handle, *unit),
            "held by the token"
        );
    }
    assert!(handle.release_snapshot(&token));
    engine.wait_for_gc();
    for unit in &inputs {
        assert!(!unit_files_exist(vfs.as_ref(), &handle, *unit), "released");
    }
    assert!(unit_files_exist(vfs.as_ref(), &handle, output[0]));
    assert!(engine.gc_removed_files() >= 6);
    drop((handle, core));
    drop(engine);

    // Nothing live was removed: the collection reopens with its rows.
    fault.crash();
    let engine = Engine::open(fault.process(), ROOT, config()).expect("engine should reopen");
    let handle = engine
        .collection(&CollectionRef::new_default("gc"))
        .expect("collection");
    let records = engine
        .core()
        .scan_exact_internal(&handle, None, true, None)
        .expect("scan");
    assert_eq!(records.len(), 2);
}

/// Checkpointed WAL files are deleted only once the manifest with that checkpoint is durable:
/// not when its publish fails, and right after it succeeds. Superseded manifests go too, the
/// previous generation stays for inspection.
#[test]
fn wal_files_and_old_manifests_are_removed_only_after_a_durable_checkpoint() {
    let fault = FaultVfs::new(32);
    let controlled = ControlledVfs::wrap(fault.process());
    let engine = Engine::open(controlled.clone(), ROOT, config()).expect("engine should open");
    let handle = create(&engine, "wal");
    let core = engine.core();
    let vfs = fault.process();
    write(&engine, &handle, "a");
    let before = wal_files(vfs.as_ref(), &handle);
    assert_eq!(before.len(), 1);

    // The flush rotates the WAL, then fails before its CURRENT rename.
    controlled.fail_file_syncs_containing("CURRENT.tmp", 1);
    core.flush_collection(&handle)
        .expect_err("the publish fails before the rename");
    assert!(
        !handle.is_poisoned(),
        "a failure before the rename abandons the job"
    );
    engine.wait_for_gc();
    let rotated = wal_files(vfs.as_ref(), &handle);
    assert_eq!(
        rotated.len(),
        2,
        "no checkpoint is durable, so every WAL file stays"
    );
    assert!(rotated.contains(&before[0]));

    write(&engine, &handle, "b");
    core.flush_collection(&handle).expect("flush");
    let after = wal_files(vfs.as_ref(), &handle);
    let checkpoint = handle.current().checkpoint_seq_no;
    assert_eq!(checkpoint, 2);
    assert_eq!(
        after.len(),
        1,
        "files at or below the checkpoint are gone: {after:?}"
    );
    assert!(
        logpose_wal::parse_wal_file_name(&after[0]).is_some_and(|first| first > checkpoint),
        "{after:?}"
    );

    // Generations: 0, 1 (burned), 2 (durable). The next flush supersedes 2 and removes 0.
    let dir = handle.meta().dir.clone();
    assert_eq!(handle.current().manifest_generation, 2);
    engine.wait_for_gc();
    assert!(!exists_at(vfs.as_ref(), &manifest_path(&dir, 1)), "burned");
    assert!(exists_at(vfs.as_ref(), &manifest_path(&dir, 0)));
    write(&engine, &handle, "c");
    core.flush_collection(&handle).expect("flush");
    engine.wait_for_gc();
    assert!(!exists_at(vfs.as_ref(), &manifest_path(&dir, 0)));
    assert!(
        exists_at(vfs.as_ref(), &manifest_path(&dir, 2)),
        "previous is kept"
    );
    assert!(exists_at(vfs.as_ref(), &manifest_path(&dir, 3)));
}

/// A job that fails before it commits has its files removed right away: no durable manifest
/// names them, and its unit is never issued again.
#[test]
fn an_abandoned_job_leaves_no_files_behind() {
    let fault = FaultVfs::new(33);
    let controlled = ControlledVfs::wrap(fault.process());
    let engine = Engine::open(controlled.clone(), ROOT, config()).expect("engine should open");
    let handle = create(&engine, "abandoned");
    let core = engine.core();
    write(&engine, &handle, "a");
    controlled.fail_file_syncs_containing(".mf", 1);
    core.flush_collection(&handle)
        .expect_err("the manifest file sync fails");
    engine.wait_for_gc();
    let vfs = fault.process();
    assert!(!unit_files_exist(vfs.as_ref(), &handle, UnitId(0)));
    for child in [SEGMENTS_DIR, INDEXES_DIR, TMP_DIR] {
        let dir = handle.meta().dir.join(child);
        let names = vfs.list(&dir).expect("list");
        assert!(names.is_empty(), "{child}: {names:?}");
    }
    core.flush_collection(&handle).expect("retry");
    assert_eq!(
        handle.current().manifest.units().collect::<Vec<_>>(),
        [UnitId(1)],
        "the retry got a fresh unit"
    );
}
