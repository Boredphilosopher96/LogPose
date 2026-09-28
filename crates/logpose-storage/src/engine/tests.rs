//! Tests for the engine shell: root exclusivity, shutdown, the collection map, create and drop
//! races, and readers pinning versions while the writer publishes.

use super::*;
use crate::runtime::RuntimeConfig;
use crate::{
    CreateCollectionRequest,
    test_support::{put, scan, unique_temp_dir},
};
use logpose_types::{DistanceMetric, record::ClientOp};
use logpose_vfs::{FaultVfs, std_vfs};
use std::{
    fs,
    sync::Barrier,
    thread,
    time::{Duration, Instant},
};

fn open(root: &Path) -> Result<Engine> {
    Engine::open(std_vfs(), root, EngineConfig::default())
}

fn request(name: &str) -> CreateCollectionRequest {
    CreateCollectionRequest::new(name, 2, DistanceMetric::Dot)
}

fn create(engine: &Engine, name: &str) -> Arc<CollectionHandle> {
    let descriptor = engine
        .core()
        .plan_collection_descriptor(&request(name))
        .expect("descriptor should plan");
    engine
        .create_collection_blocking(descriptor, None)
        .expect("collection should be created")
}

fn write(handle: &Arc<CollectionHandle>, ops: Vec<ClientOp>) {
    handle.write_blocking(ops).expect("write should succeed");
}

fn reference(name: &str) -> CollectionRef {
    CollectionRef::new_default(name)
}

fn collection_dirs(root: &Path) -> usize {
    fs::read_dir(root.join("collections"))
        .expect("collections should list")
        .count()
}

#[test]
fn a_second_engine_on_the_same_root_fails_until_every_clone_is_dropped() {
    let root = unique_temp_dir("engine-root-exclusive");
    let first = open(&root).expect("first engine should open");
    let error = open(&root).expect_err("a second engine must not open the same root");
    assert!(
        matches!(error, LogPoseError::StorageRootLocked { .. }),
        "{error}"
    );

    let clone = first.clone();
    drop(first);
    assert!(open(&root).is_err(), "a clone still owns the root");
    drop(clone);

    // An independent handle on LOCK is what another process looks like to the OS.
    let foreign = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("LOCK"))
        .expect("the engine should have created the lock file");
    foreign
        .try_lock()
        .expect("the root is released once every clone is dropped");
    let error = open(&root).expect_err("an engine must not open a root another process holds");
    assert!(
        error
            .to_string()
            .contains("is already in use by another engine"),
        "{error}"
    );
    drop(foreign);
    open(&root).expect("the root is free once the other holder exits");
}

#[test]
fn engines_on_one_fault_vfs_process_exclude_each_other() {
    let fault = FaultVfs::new(7);
    let process = fault.process();
    let first = Engine::open(Arc::clone(&process), "/storage", EngineConfig::default())
        .expect("first engine should open");
    assert!(
        Engine::open(Arc::clone(&process), "/storage", EngineConfig::default()).is_err(),
        "the same process must not open the root twice"
    );
    drop(first);
    Engine::open(process, "/storage", EngineConfig::default())
        .expect("the root is free after the first engine is dropped");
}

#[test]
fn dropping_the_engine_waits_for_background_maintenance_before_releasing_the_root() {
    let root = unique_temp_dir("engine-drop-waits");
    for round in 0..3 {
        let engine = open(&root).expect("engine should reopen right after the last drop");
        let handle = if round == 0 {
            let mut descriptor = engine
                .core()
                .plan_collection_descriptor(&CreateCollectionRequest::new(
                    "events",
                    4096,
                    DistanceMetric::Dot,
                ))
                .expect("descriptor should plan");
            descriptor.flush_threshold_ops = 1;
            engine
                .create_collection_blocking(descriptor, None)
                .expect("collection should be created")
        } else {
            engine
                .collection(&reference("events"))
                .expect("collection should be recovered")
        };
        // Each write crosses the flush threshold and queues a background flush; the engine is
        // dropped while it may still run, and the next open must not find the root locked.
        write(&handle, vec![put(&format!("id-{round}"), vec![1.0; 4096])]);
    }
    let engine = open(&root).expect("engine should reopen");
    let handle = engine
        .collection(&reference("events"))
        .expect("collection should be recovered");
    let version = handle.current();
    version.check_invariants().expect("invariants should hold");
    assert_eq!(
        version.visible_seq_no, 3,
        "every acknowledged write survives"
    );
}

#[test]
fn concurrent_creates_of_one_name_have_exactly_one_winner() {
    let root = unique_temp_dir("engine-create-race");
    let engine = open(&root).expect("engine should open");
    let barrier = Arc::new(Barrier::new(8));
    let threads = (0..8)
        .map(|_| {
            let engine = engine.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                // Planned before anyone creates, so every thread holds a distinct id.
                let descriptor = engine
                    .core()
                    .plan_collection_descriptor(&request("documents"))
                    .expect("descriptor should plan");
                barrier.wait();
                engine
                    .create_collection_blocking(descriptor, None)
                    .map(|_| ())
            })
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|thread| thread.join().expect("thread should join"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    for error in results.into_iter().filter_map(Result::err) {
        assert!(error.to_string().contains("already exists"), "{error}");
    }
    drop(engine);

    let engine = open(&root).expect("engine should reopen");
    assert_eq!(engine.collections().len(), 1);
    assert_eq!(
        collection_dirs(&root),
        1,
        "losing creates must not leave directories behind"
    );
}

#[test]
fn create_and_drop_races_leave_the_map_and_the_disk_in_agreement() {
    let root = unique_temp_dir("engine-create-drop-race");
    let engine = open(&root).expect("engine should open");
    let threads = (0..4)
        .map(|index| {
            let engine = engine.clone();
            thread::spawn(move || {
                for step in 0..20 {
                    if (index + step) % 2 == 0 {
                        if let Ok(descriptor) =
                            engine.core().plan_collection_descriptor(&request("churn"))
                        {
                            let _ = engine.create_collection_blocking(descriptor, None);
                        }
                    } else {
                        let _ = engine.drop_collection_blocking(&reference("churn"));
                    }
                    if let Ok(handle) = engine.collection(&reference("churn")) {
                        let _ = handle
                            .write_blocking(vec![put(&format!("{index}-{step}"), vec![1.0, 0.0])]);
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().expect("thread should join");
    }
    let visible = engine
        .collection(&reference("churn"))
        .map(|handle| handle.visible_seq_no())
        .ok();
    drop(engine);

    let engine = open(&root).expect("engine should reopen");
    let reopened = engine
        .collection(&reference("churn"))
        .map(|handle| handle.visible_seq_no())
        .ok();
    assert_eq!(reopened, visible, "the disk must agree with the map");
    assert_eq!(
        collection_dirs(&root),
        usize::from(visible.is_some()),
        "no retired or losing directory is left behind"
    );
}

#[test]
fn a_dropped_collection_refuses_calls_but_pinned_versions_keep_their_data() {
    let root = unique_temp_dir("engine-drop");
    let engine = open(&root).expect("engine should open");
    let handle = create(&engine, "documents");
    write(&handle, vec![put("alpha", vec![1.0, 0.0])]);
    let pinned = handle.current();

    engine
        .drop_collection_blocking(&reference("documents"))
        .expect("drop should succeed");
    assert!(handle.is_dropped());
    let error = handle
        .write_blocking(vec![put("beta", vec![0.0, 1.0])])
        .expect_err("a dropped handle must refuse writes");
    assert!(error.to_string().contains("does not exist"), "{error}");
    assert!(engine.collection(&reference("documents")).is_err());
    assert_eq!(pinned.visible_seq_no, 1);
    assert_eq!(
        pinned.active.slot_count(),
        1,
        "a pinned version keeps its resident state"
    );
    assert!(
        engine
            .drop_collection_blocking(&reference("documents"))
            .is_err(),
        "a second drop finds nothing"
    );
    assert_eq!(collection_dirs(&root), 0, "the drop removed the directory");

    let recreated = create(&engine, "documents");
    assert_eq!(
        recreated.visible_seq_no(),
        0,
        "a recreated collection is empty"
    );
    drop((handle, recreated, pinned));
    drop(engine);
    let engine = open(&root).expect("engine should reopen");
    let recovered = engine
        .collection(&reference("documents"))
        .expect("the recreated collection survives");
    assert_eq!(recovered.visible_seq_no(), 0);
}

#[test]
fn a_drop_whose_rename_fails_leaves_the_collection_serving() {
    let root = unique_temp_dir("engine-drop-rename-fails");
    let engine = open(&root).expect("engine should open");
    let handle = create(&engine, "documents");
    write(&handle, vec![put("alpha", vec![1.0, 0.0])]);

    // A non-empty directory in the way makes the retiring rename fail before anything changed.
    let mut blocker = handle.meta().dir.clone().into_os_string();
    blocker.push(DROPPED_DIR_SUFFIX);
    let blocker = PathBuf::from(blocker);
    fs::create_dir_all(blocker.join("in-the-way")).expect("blocker should be created");
    engine
        .drop_collection_blocking(&reference("documents"))
        .expect_err("the rename fails");

    assert!(
        !handle.is_dropped(),
        "a drop that changed nothing is undone"
    );
    let served = engine
        .collection(&reference("documents"))
        .expect("the collection is still served");
    assert!(Arc::ptr_eq(&served, &handle));
    write(&handle, vec![put("beta", vec![0.0, 1.0])]);
    assert_eq!(handle.visible_seq_no(), 2);

    fs::remove_dir_all(&blocker).expect("blocker should be removed");
    drop((handle, served));
    drop(engine);
    let engine = open(&root).expect("engine should reopen");
    let recovered = engine
        .collection(&reference("documents"))
        .expect("the collection survives");
    assert_eq!(recovered.visible_seq_no(), 2);
}

#[test]
fn open_removes_retired_and_unfinished_collection_directories() {
    let root = unique_temp_dir("engine-abandoned-dirs");
    let engine = open(&root).expect("engine should open");
    let kept = create(&engine, "kept");
    let retired = create(&engine, "retired");
    let retired_dir = retired.meta().dir.clone();
    drop((kept, retired));
    drop(engine);

    // A drop that crashed after its rename, and a create that crashed before its descriptor
    // was written.
    let mut dropped = retired_dir.clone().into_os_string();
    dropped.push(DROPPED_DIR_SUFFIX);
    let dropped = PathBuf::from(dropped);
    fs::rename(&retired_dir, &dropped).expect("rename should succeed");
    let unfinished = root.join("collections").join("unfinished");
    fs::create_dir_all(unfinished.join("wal")).expect("directory should be created");

    let engine = open(&root).expect("engine should reopen");
    assert!(engine.collection(&reference("kept")).is_ok());
    assert!(engine.collection(&reference("retired")).is_err());
    assert!(!dropped.exists());
    assert!(!unfinished.exists());
}

#[test]
fn a_collection_that_fails_recovery_does_not_fail_the_engine() {
    let root = unique_temp_dir("engine-failed-collection");
    let engine = open(&root).expect("engine should open");
    let broken = create(&engine, "broken");
    let healthy = create(&engine, "healthy");
    write(&healthy, vec![put("alpha", vec![1.0, 0.0])]);
    let broken_dir = broken.meta().dir.clone();
    drop((broken, healthy));
    drop(engine);

    fs::write(broken_dir.join("CURRENT"), b"not a generation").expect("CURRENT is corrupted");
    let engine = open(&root).expect("the engine opens despite one broken collection");
    let error = engine
        .collection(&reference("broken"))
        .expect_err("the broken collection reports its recovery error");
    assert!(error.to_string().contains("CURRENT"), "{error}");
    let healthy = engine
        .collection(&reference("healthy"))
        .expect("the healthy collection is served");
    assert_eq!(healthy.visible_seq_no(), 1);
    let listed = engine
        .core()
        .list_descriptors()
        .expect("listing includes the failed collection");
    assert_eq!(listed.len(), 2);
    engine
        .drop_collection_blocking(&reference("broken"))
        .expect("a failed collection can be dropped");
    assert_eq!(engine.core().list_descriptors().expect("listing").len(), 1);
}

#[test]
fn version_ids_increase_and_invariants_hold_across_writes_flush_and_compaction() {
    let root = unique_temp_dir("engine-version-ids");
    let engine = open(&root).expect("engine should open");
    let handle = create(&engine, "documents");
    let mut last = handle.current();
    last.check_invariants().expect("initial version is valid");
    for round in 0..3 {
        write(
            &handle,
            vec![
                put(&format!("a{round}"), vec![1.0, 0.0]),
                put(&format!("b{round}"), vec![0.0, 1.0]),
            ],
        );
        let written = handle.current();
        assert!(written.id > last.id);
        assert_eq!(written.counters.memtable_rows, 2);
        written
            .check_invariants()
            .expect("invariants hold after a write");
        handle.flush_blocking().expect("flush should succeed");
        let flushed = handle.current();
        assert!(flushed.id > written.id);
        assert_eq!(flushed.counters.memtable_rows, 0);
        assert_eq!(flushed.visible_seq_no, written.visible_seq_no);
        flushed
            .check_invariants()
            .expect("invariants hold after a flush");
        last = flushed;
    }
    handle
        .compact_blocking()
        .expect("compaction should succeed");
    let compacted = handle.current();
    assert!(compacted.id > last.id);
    assert_eq!(compacted.counters.segment_count, 1);
    assert_eq!(compacted.visible_seq_no, last.visible_seq_no);
    compacted
        .check_invariants()
        .expect("invariants hold after compaction");
}

#[test]
fn readers_pin_a_version_while_writes_and_flushes_publish() {
    let root = unique_temp_dir("engine-pinned-readers");
    let engine = open(&root).expect("engine should open");
    let handle = create(&engine, "documents");
    let deadline = Instant::now() + Duration::from_secs(60);
    let done = Arc::new(AtomicBool::new(false));

    let readers = (0..4)
        .map(|_| {
            let handle = Arc::clone(&handle);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                let mut last_seen = 0;
                let mut pinned = Vec::new();
                while !done.load(Ordering::Acquire) {
                    let version = handle.current();
                    assert!(
                        version.visible_seq_no >= last_seen,
                        "visible_seq_no never goes backwards for one reader"
                    );
                    last_seen = version.visible_seq_no;
                    version
                        .check_invariants()
                        .expect("a pinned version is consistent");
                    let summary = (
                        version.id,
                        version.visible_seq_no,
                        version.manifest_generation,
                        version.counters,
                        version.active.slot_count(),
                    );
                    pinned.push((version, summary));
                    if pinned.len() > 64 {
                        pinned.remove(0);
                    }
                }
                // Every later publication left what this reader pinned unchanged.
                for (version, summary) in pinned {
                    let now = (
                        version.id,
                        version.visible_seq_no,
                        version.manifest_generation,
                        version.counters,
                        version.active.slot_count(),
                    );
                    assert_eq!(now, summary);
                    version.check_invariants().expect("still consistent");
                }
                last_seen
            })
        })
        .collect::<Vec<_>>();

    let mut acked = 0;
    for index in 0..200 {
        let ack = handle
            .write_blocking(vec![put(&format!("id-{index}"), vec![1.0, 0.0])])
            .expect("write should succeed");
        acked = ack.last_seq_no;
        // I1: a read that starts after the acknowledgement sees the write.
        assert!(handle.current().visible_seq_no >= acked);
        if index % 50 == 49 {
            handle.flush_blocking().expect("flush should succeed");
        }
        assert!(Instant::now() < deadline, "the test ran too long");
    }
    done.store(true, Ordering::Release);
    for reader in readers {
        let seen = reader.join().expect("reader should join");
        assert!(seen <= acked);
    }
}

/// Compaction builds its output holding only the maintenance slot, so writes land while it
/// runs. Its publication must keep every one of them (no lost update) without duplicating or
/// reordering sequence numbers.
#[test]
fn compaction_keeps_every_write_that_lands_while_it_builds() {
    let root = unique_temp_dir("engine-compaction-vs-writes");
    let engine = open(&root).expect("engine should open");
    let handle = create(&engine, "documents");
    let core = engine.core();
    for index in 0..3 {
        write(&handle, vec![put(&format!("seed-{index}"), vec![1.0, 0.0])]);
        handle.flush_blocking().expect("flush should succeed");
    }

    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let handle = Arc::clone(&handle);
        let done = Arc::clone(&done);
        thread::spawn(move || {
            let mut acked = Vec::new();
            let mut index = 0;
            while !done.load(Ordering::Acquire) || acked.len() < 50 {
                let id = format!("write-{index}");
                let ack = handle
                    .write_blocking(vec![put(&id, vec![0.0, 1.0])])
                    .expect("write should succeed");
                assert!(
                    handle.current().visible_seq_no >= ack.last_seq_no,
                    "an acknowledged write is visible (I1)"
                );
                acked.push((id, ack.last_seq_no));
                index += 1;
                // The writer slot is a plain mutex, which a thread that relocks it at once can
                // hold indefinitely; pause so that flush and compaction get it too.
                thread::sleep(Duration::from_millis(1));
            }
            acked
        })
    };

    let mut last_seen = handle.visible_seq_no();
    for _ in 0..6 {
        // Flush adds a segment while holding the writer slot; compaction then runs while
        // writes continue.
        handle.flush_blocking().expect("flush should succeed");
        handle
            .compact_blocking()
            .expect("compaction should succeed");
        let version = handle.current();
        version
            .check_invariants()
            .expect("the version compaction published is consistent");
        assert!(
            version.visible_seq_no >= last_seen,
            "visibility never regresses"
        );
        last_seen = version.visible_seq_no;
    }
    done.store(true, Ordering::Release);
    let acked = writer.join().expect("writer should join");

    let check = |engine: &Engine, context: &str| {
        let handle = engine
            .collection(&reference("documents"))
            .expect("collection should be open");
        let version = handle.current();
        version.check_invariants().expect("invariants should hold");
        let last = acked.last().map_or(0, |(_, seq_no)| *seq_no);
        assert_eq!(
            version.visible_seq_no, last,
            "{context}: every ack is visible"
        );
        let visible = scan(&handle, None)
            .expect("scan should succeed")
            .into_iter()
            .map(|(seq_no, record)| (record.pk.label(), seq_no))
            .collect::<BTreeMap<_, _>>();
        for (id, seq_no) in &acked {
            assert_eq!(
                visible.get(id),
                Some(seq_no),
                "{context}: acknowledged write '{id}' is visible at its own seq"
            );
        }
        assert_eq!(visible.len(), acked.len() + 3, "{context}: nothing is lost");
    };
    check(&engine, "in memory");
    drop((handle, core));
    drop(engine);
    let engine = open(&root).expect("engine should reopen");
    check(&engine, "after reopen");
}

/// The job threads are shared by every collection. A collection whose writes keep crossing its
/// flush threshold must not monopolize them: other collections' jobs still run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_busy_collection_does_not_starve_other_collections_of_job_threads() {
    let root = unique_temp_dir("engine-job-fairness");
    let config = EngineConfig {
        runtime: RuntimeConfig {
            maintenance_threads: 1,
            ..RuntimeConfig::default()
        },
        ..EngineConfig::default()
    };
    let engine = Engine::open(std_vfs(), &root, config).expect("engine should open");
    let mut descriptor = engine
        .core()
        .plan_collection_descriptor(&request("busy"))
        .expect("descriptor should plan");
    descriptor.flush_threshold_ops = 1;
    descriptor.compaction_threshold_segments = usize::MAX;
    let busy = engine
        .create_collection_blocking(descriptor, None)
        .expect("collection should be created");
    let quiet = create(&engine, "quiet");
    quiet
        .write(vec![put("alpha", vec![1.0, 0.0])])
        .await
        .expect("write should succeed");

    // Every write of the busy collection queues a flush for it.
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let done = Arc::clone(&done);
        thread::spawn(move || {
            let mut index = 0;
            while !done.load(Ordering::Acquire) {
                busy.write_blocking(vec![put(&format!("id-{index}"), vec![1.0, 0.0])])
                    .expect("write should succeed");
                index += 1;
                // Let the busy collection's flushes take the writer slot between writes.
                thread::sleep(Duration::from_millis(1));
            }
        })
    };
    // Let the busy collection's job loop start.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let flushed = tokio::time::timeout(Duration::from_secs(5), quiet.flush()).await;
    done.store(true, Ordering::Release);
    writer.join().expect("writer should join");
    flushed
        .expect("the quiet collection's flush must get a job thread")
        .expect("the flush should succeed");
}

/// Drop `engine` on another thread and fail, instead of hanging, when that takes longer than
/// `limit`. The failure names where the tasks the drop still waits for were created.
fn drop_within(engine: Engine, limit: Duration, context: &str) {
    let tasks = Arc::clone(&engine.shared.core.tasks);
    let (done, dropped) = mpsc::channel();
    thread::spawn(move || {
        drop(engine);
        let _ = done.send(());
    });
    let finished = dropped.recv_timeout(limit).is_ok();
    assert!(
        finished,
        "{context}: dropping the engine hung; it still waits for {}",
        tasks.outstanding()
    );
}

/// A crash at any point of a collection drop (before, at, or after its commit point) must not
/// leave a writer task, a quiesce or a job behind that the engine's drop then waits for forever,
/// neither in the engine that saw the crash nor in the one that recovers after it.
#[test]
fn the_engine_drops_promptly_after_a_crash_at_any_point_of_a_collection_drop() {
    for k in 0..8 {
        let context = format!("crash after {k} operations of the drop");
        let fault = FaultVfs::new(40 + k);
        let engine = Engine::open(fault.process(), "/storage", EngineConfig::default())
            .expect("engine should open");
        let handle = create(&engine, "documents");
        write(&handle, vec![put("alpha", vec![1.0, 0.0])]);
        drop(handle);
        fault.set_plan(logpose_vfs::FaultPlan {
            crash_after_ops: Some(fault.mutating_ops() + k),
            ..logpose_vfs::FaultPlan::default()
        });
        let _ = engine.drop_collection_blocking(&reference("documents"));
        drop_within(engine, Duration::from_secs(60), &context);

        fault.crash();
        let engine = Engine::open(fault.process(), "/storage", EngineConfig::default())
            .expect("engine should reopen");
        if let Ok(handle) = engine.collection(&reference("documents")) {
            write(&handle, vec![put("beta", vec![0.0, 1.0])]);
            handle.flush_blocking().expect("flush should succeed");
        }
        drop_within(engine, Duration::from_secs(60), &context);
    }
}

/// Two directories that hold the same collection: the second is not served, but recovery
/// already started its writer task. The engine's drop must stop that writer too.
#[test]
fn the_engine_drop_stops_the_writer_of_a_collection_it_does_not_serve() {
    fn copy_dir(from: &Path, to: &Path) {
        fs::create_dir_all(to).expect("directory should be created");
        for entry in fs::read_dir(from).expect("directory should list") {
            let entry = entry.expect("entry should read");
            let target = to.join(entry.file_name());
            if entry.file_type().expect("type should read").is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), &target).expect("file should copy");
            }
        }
    }

    let root = unique_temp_dir("engine-duplicate-collection");
    let engine = open(&root).expect("engine should open");
    let handle = create(&engine, "documents");
    write(&handle, vec![put("alpha", vec![1.0, 0.0])]);
    let dir = handle.meta().dir.clone();
    drop(handle);
    drop(engine);
    let mut copy = dir.clone().into_os_string();
    copy.push("-copy");
    copy_dir(&dir, Path::new(&copy));

    let engine = open(&root).expect("engine should open");
    let served = engine
        .collection(&reference("documents"))
        .expect("one of the two directories is served");
    assert_eq!(served.visible_seq_no(), 1);
    drop(served);
    drop_within(
        engine,
        Duration::from_secs(60),
        "a duplicate collection directory",
    );
    open(&root).expect("the root is released");
}

/// A create that an engine task (such as an async create on the I/O pool) finishes after the
/// last engine clone was dropped starts its writer after shutdown already stopped the others.
/// That writer must stop too, or the engine's drop waits for it forever.
#[test]
fn a_create_that_finishes_during_shutdown_does_not_hang_the_engine_drop() {
    let fault = FaultVfs::new(50);
    let vfs = crate::test_support::ControlledVfs::wrap(fault.process());
    let engine =
        Engine::open(vfs.clone(), "/storage", EngineConfig::default()).expect("engine should open");
    let descriptor = engine
        .core()
        .plan_collection_descriptor(&request("late"))
        .expect("descriptor should plan");

    // Hold the create inside its first fsync, on an engine task.
    vfs.hold_syncs();
    let (created, create_result) = mpsc::channel();
    {
        let core = engine.core();
        thread::spawn(move || {
            let result = core.create_collection(descriptor, None).map(|_| ());
            let _ = created.send(result);
        });
    }
    assert!(vfs.wait_for_held_sync(Duration::from_secs(10)));

    // Drop the last engine clone; it waits for the create's engine reference.
    let core = Arc::clone(&engine.shared.core);
    let tasks = Arc::clone(&core.tasks);
    let (done, dropped) = mpsc::channel();
    thread::spawn(move || {
        drop(engine);
        let _ = done.send(());
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while !core.is_shutting_down() {
        assert!(Instant::now() < deadline, "the engine drop never started");
        thread::sleep(Duration::from_millis(1));
    }
    // Let the drop stop the writers it knows about before the create registers its own.
    thread::sleep(Duration::from_millis(100));
    vfs.release_syncs();

    create_result
        .recv_timeout(Duration::from_secs(30))
        .expect("the create finishes")
        .expect("the create succeeds");
    let finished = dropped.recv_timeout(Duration::from_secs(60)).is_ok();
    assert!(
        finished,
        "dropping the engine hung; it still waits for {}",
        tasks.outstanding()
    );
}

/// The shutdown diagnostics name the creation site of every live engine task.
#[test]
fn the_task_tracker_names_where_live_tasks_were_created() {
    let root = unique_temp_dir("engine-task-sites");
    let engine = open(&root).expect("engine should open");
    let core = engine.core();
    let report = engine.shared.core.tasks.outstanding();
    assert!(report.contains("engine/tests.rs"), "{report}");
    drop(core);
    assert_eq!(engine.shared.core.tasks.outstanding(), "");
}
