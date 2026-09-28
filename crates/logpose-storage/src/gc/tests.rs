//! Garbage collection tests: orphan cleanup on `FaultVfs`, and version-refcounted removal of
//! segment files, superseded manifests, and checkpointed WAL files through the engine.

use super::*;
use crate::{
    CollectionHandle, CreateCollectionRequest, Engine, EngineConfig,
    dv::dv_path,
    manifest::{DvRef, Manifest, manifest_path, publish_manifest},
    paths::segment_path,
    test_support::{ControlledVfs, delete, put, scan, vector_schema},
};
use logpose_types::{CollectionId, CollectionRef, DistanceMetric};
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
    let schema = vector_schema(2, DistanceMetric::Dot);
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
        })
        .collect();
    manifest
}

/// A collection directory holding the live unit 1 with its DV generation 2, orphan segments
/// of units 2 and 9, orphan DV files (an older generation of unit 1, and one of unit 9), staged
/// files, and manifest generations 1, 3, 4 (current) and 6.
fn populated(vfs: &dyn Vfs) -> (Manifest, Vec<PathBuf>, Vec<PathBuf>) {
    let dir = Path::new(DIR);
    vfs.create_dir_all(&manifests_dir(dir)).expect("manifests");
    vfs.sync_dir(Path::new("/")).expect("sync /");
    vfs.sync_dir(dir).expect("sync dir");
    for generation in [1, 3, 6] {
        publish_manifest(vfs, dir, &manifest(generation, &[1])).expect("publish");
    }
    let mut current = manifest(4, &[1]);
    current.segments[0].dv = Some(DvRef {
        generation: 2,
        cardinality: 1,
        covered_seq_no: 1,
    });
    current.next_dv_gen = 3;
    publish_manifest(vfs, dir, &current).expect("publish current");
    let live = vec![segment_path(dir, UnitId(1)), dv_path(dir, UnitId(1), 2)];
    let orphans = vec![
        segment_path(dir, UnitId(2)),
        segment_path(dir, UnitId(9)),
        dv_path(dir, UnitId(1), 1),
        dv_path(dir, UnitId(9), 5),
        dir.join(SEGMENTS_DIR).join("00000001.seg.tmp"),
        dir.join(CURRENT_TEMP_FILE),
        manifest_path(dir, 1),
        manifest_path(dir, 6),
    ];
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
    assert_eq!(
        cleanup.next_dv_gen, 6,
        "above DV generation 5 that was on disk"
    );
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
        .create_collection_blocking(descriptor, None)
        .expect("collection should be created")
}

fn write(handle: &Arc<CollectionHandle>, id: &str) {
    handle
        .write_blocking(vec![put(id, vec![1.0, 0.0])])
        .expect("write");
}

fn unit_files_exist(vfs: &dyn Vfs, handle: &CollectionHandle, unit: UnitId) -> bool {
    exists_at(vfs, &segment_path(&handle.meta().dir, unit))
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
    write(&handle, "a");
    handle.flush_blocking().expect("flush");
    write(&handle, "b");
    handle.flush_blocking().expect("flush");
    let inputs = handle.current().manifest.units().collect::<Vec<_>>();
    assert_eq!(inputs.len(), 2);

    let held = handle.current();
    let token = handle.pin_snapshot().expect("pin");
    handle.compact_blocking().expect("compact");
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
    assert!(engine.gc_removed_files() >= 2);
    drop((handle, core));
    drop(engine);

    // Nothing live was removed: the collection reopens with its rows.
    fault.crash();
    let engine = Engine::open(fault.process(), ROOT, config()).expect("engine should reopen");
    let handle = engine
        .collection(&CollectionRef::new_default("gc"))
        .expect("collection");
    let records = scan(&handle, None).expect("scan");
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
    let vfs = fault.process();
    write(&handle, "a");
    let before = wal_files(vfs.as_ref(), &handle);
    assert_eq!(before.len(), 1);

    // The flush rotates the WAL, then fails before its CURRENT rename.
    controlled.fail_file_syncs_containing("CURRENT.tmp", 1);
    handle
        .flush_blocking()
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

    // The retry flushes the memtable the failed flush froze, then the one "b" went to.
    write(&handle, "b");
    handle.flush_blocking().expect("flush");
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

    // Generations: 0, 1 (burned), 2 (checkpoint 1), 3 (checkpoint 2). Committing 3 removed 0;
    // the previous generation stays for inspection.
    let dir = handle.meta().dir.clone();
    assert_eq!(handle.current().manifest_generation, 3);
    engine.wait_for_gc();
    assert!(!exists_at(vfs.as_ref(), &manifest_path(&dir, 1)), "burned");
    assert!(!exists_at(vfs.as_ref(), &manifest_path(&dir, 0)));
    assert!(exists_at(vfs.as_ref(), &manifest_path(&dir, 2)));
    write(&handle, "c");
    handle.flush_blocking().expect("flush");
    engine.wait_for_gc();
    assert!(!exists_at(vfs.as_ref(), &manifest_path(&dir, 2)));
    assert!(
        exists_at(vfs.as_ref(), &manifest_path(&dir, 3)),
        "previous is kept"
    );
    assert!(exists_at(vfs.as_ref(), &manifest_path(&dir, 4)));
}

/// A job that fails before it commits has its files removed right away: no durable manifest
/// names them, and its unit is never issued again.
#[test]
fn an_abandoned_job_leaves_no_files_behind() {
    let fault = FaultVfs::new(33);
    let controlled = ControlledVfs::wrap(fault.process());
    let engine = Engine::open(controlled.clone(), ROOT, config()).expect("engine should open");
    let handle = create(&engine, "abandoned");
    write(&handle, "a");
    controlled.fail_file_syncs_containing(".mf", 1);
    handle
        .flush_blocking()
        .expect_err("the manifest file sync fails");
    engine.wait_for_gc();
    let vfs = fault.process();
    // The job's output was unit 1 (unit 0 is the memtable it froze, unit 2 the new active one).
    assert!(!unit_files_exist(vfs.as_ref(), &handle, UnitId(1)));
    let names = vfs
        .list(&handle.meta().dir.join(SEGMENTS_DIR))
        .expect("list");
    assert!(names.is_empty(), "{names:?}");
    assert_eq!(
        handle.current().frozen.len(),
        1,
        "the memtable stays frozen"
    );
    handle.flush_blocking().expect("retry");
    assert_eq!(
        handle.current().manifest.units().collect::<Vec<_>>(),
        [UnitId(3)],
        "the retry got a fresh unit"
    );
    assert!(handle.current().frozen.is_empty());
}

/// Dropping the last reference to a file handle removes its files only when the writer marked
/// it obsolete: a handle of a live segment can be dropped (at shutdown, or by a stray `Version`)
/// without touching the files the durable manifest still references.
#[test]
fn only_the_last_reference_to_an_obsolete_handle_removes_its_files() {
    let fault = FaultVfs::new(36);
    let engine = Engine::open(fault.process(), ROOT, config()).expect("engine should open");
    let vfs = fault.process();
    let dir = Path::new(ROOT).join("handles");
    let live = segment_path(&dir, UnitId(1));
    let obsolete = segment_path(&dir, UnitId(2));
    for path in [&live, &obsolete] {
        touch(vfs.as_ref(), path);
    }
    let gc = engine.core().gc.clone();
    let live_handle = Arc::new(FileHandle::new(UnitId(1), live.clone(), gc.clone()));
    let obsolete_handle = Arc::new(FileHandle::new(UnitId(2), obsolete.clone(), gc));
    let reader = Arc::clone(&obsolete_handle);
    obsolete_handle.mark_obsolete();
    drop(obsolete_handle);
    engine.wait_for_gc();
    assert!(
        exists_at(vfs.as_ref(), &obsolete),
        "a reader still holds the obsolete handle"
    );
    drop(reader);
    drop(live_handle);
    engine.wait_for_gc();
    assert!(!exists_at(vfs.as_ref(), &obsolete));
    assert!(
        exists_at(vfs.as_ref(), &live),
        "a handle that was never marked obsolete leaves its file"
    );
}

/// The state a read runs against holds its `Version`, so the segment files the read opens by
/// path stay on disk until the read ends, even when a compaction retires them and nothing else
/// holds that `Version` any more.
#[test]
fn a_read_in_progress_keeps_the_files_of_its_version() {
    let fault = FaultVfs::new(37);
    let engine = Engine::open(fault.process(), ROOT, config()).expect("engine should open");
    let handle = create(&engine, "reading");
    write(&handle, "a");
    handle.flush_blocking().expect("flush");
    write(&handle, "b");
    handle.flush_blocking().expect("flush");
    let inputs = handle.current().manifest.units().collect::<Vec<_>>();

    let (state, _) = handle.read_state(None).expect("read state");
    handle.compact_blocking().expect("compact");
    engine.wait_for_gc();
    let vfs = fault.process();
    for unit in &inputs {
        assert!(
            unit_files_exist(vfs.as_ref(), &handle, *unit),
            "the read still needs {unit}"
        );
    }
    drop(state);
    engine.wait_for_gc();
    for unit in &inputs {
        assert!(!unit_files_exist(vfs.as_ref(), &handle, *unit), "released");
    }
}

/// The reaper releases the versions of the pins it drops on the maintenance pool, off the
/// caller's thread. Waiting for the collector waits for those releases too, so the files a
/// reaped pin alone held are gone once it returns.
#[test]
fn waiting_for_gc_covers_the_versions_the_reaper_releases_in_the_background() {
    use crate::{RuntimeConfig, clock::ManualClock};
    use std::time::Duration;

    let fault = FaultVfs::new(35);
    let clock = Arc::new(ManualClock::new());
    let engine = Engine::open(
        fault.process(),
        ROOT,
        EngineConfig {
            clock: Some(Arc::clone(&clock) as Arc<dyn crate::Clock>),
            runtime: RuntimeConfig {
                maintenance_threads: 1,
                ..RuntimeConfig::default()
            },
            ..config()
        },
    )
    .expect("engine should open");
    let handle = create(&engine, "reaped");
    let core = engine.core();
    write(&handle, "a");
    handle.flush_blocking().expect("flush");
    write(&handle, "b");
    handle.flush_blocking().expect("flush");
    let inputs = handle.current().manifest.units().collect::<Vec<_>>();
    let _token = handle.pin_snapshot().expect("pin");
    handle.compact_blocking().expect("compact");

    // Occupy the maintenance pool, so the reaper's release waits behind this job.
    let (unblock, blocked) = std::sync::mpsc::channel::<()>();
    core.runtime().maintenance.spawn(move || {
        let _ = blocked.recv();
    });
    clock.advance(Duration::from_secs(3600));
    assert_eq!(engine.reap_snapshots(), 1);
    let unblocker = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(unblock);
    });
    engine.wait_for_gc();
    let vfs = fault.process();
    for unit in &inputs {
        assert!(
            !unit_files_exist(vfs.as_ref(), &handle, *unit),
            "the reaped pin was the last holder of {unit}"
        );
    }
    unblocker.join().expect("unblocker should join");
}

/// Whether `result` failed only because the snapshot it read is no longer retained.
fn expired<T>(result: &Result<T>) -> bool {
    matches!(result, Err(LogPoseError::SnapshotExpired { .. }))
}

/// I7 under load: readers pin tokens and read through them (and through the exact snapshots
/// they name) while a writer and a maintenance thread flush and compact without pause, the
/// reaper expires idle pins, and the collector removes what they release. No read ever fails
/// with anything but `SnapshotExpired`, every file of every `Version` a reader holds is on
/// disk, two reads through one token agree (I12), and once everything is released the files
/// on disk are exactly those the durable manifest references.
#[test]
fn readers_pinning_tokens_never_lose_a_file_while_flushes_compactions_and_gc_run() {
    use crate::{TokenConfig, clock::ManualClock, manifest::MANIFESTS_DIR};
    use std::{
        sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
        time::Duration,
    };

    let fault = FaultVfs::new(34);
    let clock = Arc::new(ManualClock::new());
    let engine = Engine::open(
        fault.process(),
        ROOT,
        EngineConfig {
            clock: Some(Arc::clone(&clock) as Arc<dyn crate::Clock>),
            tokens: TokenConfig {
                ttl: Duration::from_secs(10),
                max_per_collection: 16,
                ..TokenConfig::default()
            },
            ..config()
        },
    )
    .expect("engine should open");
    let handle = create(&engine, "stress");
    let done = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));

    let writer = {
        let handle = Arc::clone(&handle);
        std::thread::spawn(move || {
            for round in 0..400_u32 {
                let id = format!("k{}", round % 37);
                let operation = if round % 5 == 4 {
                    delete(&id)
                } else {
                    put(&id, vec![round as f32, 1.0])
                };
                handle.write_blocking(vec![operation]).expect("write");
            }
        })
    };
    let maintenance = {
        let handle = Arc::clone(&handle);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut round = 0_u32;
            while !done.load(AtomicOrdering::Acquire) {
                handle.flush_blocking().expect("flush");
                if round % 3 == 2 {
                    handle.compact_blocking().expect("compact");
                }
                round += 1;
            }
        })
    };
    let reaper = {
        let engine = engine.clone();
        let clock = Arc::clone(&clock);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            while !done.load(AtomicOrdering::Acquire) {
                clock.advance(Duration::from_secs(1));
                engine.reap_snapshots();
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };
    let readers = (0..3)
        .map(|reader| {
            let handle = Arc::clone(&handle);
            let done = Arc::clone(&done);
            let reads = Arc::clone(&reads);
            let vfs = fault.process();
            std::thread::spawn(move || {
                let mut held = Vec::new();
                let mut round = 0_usize;
                while !done.load(AtomicOrdering::Acquire) {
                    round += 1;
                    let current = handle.current();
                    let pinned = handle.pin_version(Arc::clone(&current));
                    assert!(
                        pinned.is_ok()
                            || matches!(pinned, Err(LogPoseError::TooManySnapshots { .. })),
                        "pin: {:?}",
                        pinned.as_ref().err()
                    );
                    if let Ok(token) = pinned {
                        held.push((token, current.snapshot(), None));
                    }
                    drop(current);
                    for (token, snapshot, first) in &mut held {
                        let resolved = handle.snapshot_version(token);
                        assert!(
                            resolved.is_ok() || expired(&resolved),
                            "resolve: {:?}",
                            resolved.as_ref().err()
                        );
                        let Ok(version) = resolved else {
                            continue;
                        };
                        // Deletion vectors live in the version, so only its segment files must
                        // stay on disk while it is held.
                        for segment in version.segments.iter() {
                            assert!(
                                exists_at(vfs.as_ref(), segment.path()),
                                "{} is gone while a version holds it",
                                segment.path().display()
                            );
                        }
                        // The reaper may expire the token between two uses.
                        let records = scan(&handle, token.clone());
                        assert!(
                            records.is_ok() || expired(&records),
                            "read through a token: {:?}",
                            records.as_ref().err()
                        );
                        if let Ok(records) = records {
                            match first {
                                Some(first) => {
                                    assert_eq!(*first, records, "reads through one token agree");
                                }
                                None => *first = Some(records),
                            }
                        }
                        let exact = scan(&handle, Some(snapshot.clone()));
                        assert!(
                            exact.is_ok() || expired(&exact),
                            "read at an exact snapshot: {:?}",
                            exact.as_ref().err()
                        );
                        drop(version);
                        reads.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    scan(&handle, None).expect("a read of the current state never fails");
                    if (held.len() > 4 || round % 7 == reader) && !held.is_empty() {
                        let (token, _, _) = held.remove(0);
                        handle.release_snapshot(&token);
                    }
                }
                for (token, _, _) in held {
                    handle.release_snapshot(&token);
                }
            })
        })
        .collect::<Vec<_>>();

    writer.join().expect("writer should join");
    done.store(true, AtomicOrdering::Release);
    maintenance.join().expect("maintenance should join");
    reaper.join().expect("reaper should join");
    for reader in readers {
        reader.join().expect("reader should join");
    }
    assert!(reads.load(AtomicOrdering::Relaxed) > 0, "the readers read");
    clock.advance(Duration::from_secs(3600));
    engine.reap_snapshots();
    assert_eq!(handle.pinned_snapshots(), 0);
    handle.flush_blocking().expect("flush");
    handle.compact_blocking().expect("compact");
    engine.wait_for_gc();

    // Exactly the durable manifest's files are left, with it and the previous generation.
    let vfs = fault.process();
    let dir = handle.meta().dir.clone();
    let current = handle.current();
    let mut expected = current
        .manifest
        .segments
        .iter()
        .flat_map(|segment| {
            [
                Some(segment_path(&dir, segment.unit)),
                segment
                    .dv
                    .map(|dv| dv_path(&dir, segment.unit, dv.generation)),
            ]
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let generation = current.manifest_generation;
    drop(current);
    expected.insert(manifest_path(&dir, generation));
    // No publish failed, so generations are consecutive.
    if let Some(previous) = generation.checked_sub(1) {
        expected.insert(manifest_path(&dir, previous));
    }
    let mut found = BTreeSet::new();
    for child in [SEGMENTS_DIR, MANIFESTS_DIR] {
        for name in files_in(vfs.as_ref(), &dir.join(child)).expect("list") {
            found.insert(dir.join(child).join(name));
        }
    }
    assert_eq!(found, expected, "no leaked and no missing files");
    assert!(engine.gc_removed_files() > 0);
}
