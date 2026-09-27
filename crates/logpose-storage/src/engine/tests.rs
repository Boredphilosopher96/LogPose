//! Tests for the engine shell: root exclusivity, shutdown, the collection map, create and drop
//! races, and readers pinning versions while the writer publishes.

use super::*;
use crate::{
    CreateCollectionRequest,
    test_support::{put, unique_temp_dir},
};
use logpose_types::{DistanceMetric, WriteOperation};
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
        .create_collection(descriptor, None)
        .expect("collection should be created")
}

fn write(engine: &Engine, handle: &Arc<CollectionHandle>, ops: Vec<WriteOperation>) {
    engine
        .core()
        .write(handle, ops)
        .expect("write should succeed");
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
    open(&root).expect("the root is free once every clone is dropped");
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
                .create_collection(descriptor, None)
                .expect("collection should be created")
        } else {
            engine
                .collection(&reference("events"))
                .expect("collection should be recovered")
        };
        // Each write crosses the flush threshold and queues a background flush; the engine is
        // dropped while it may still run, and the next open must not find the root locked.
        write(
            &engine,
            &handle,
            vec![put(&format!("id-{round}"), vec![1.0; 4096])],
        );
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
                engine.create_collection(descriptor, None).map(|_| ())
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
                            let _ = engine.create_collection(descriptor, None);
                        }
                    } else {
                        let _ = engine.drop_collection(&reference("churn"));
                    }
                    if let Ok(handle) = engine.collection(&reference("churn")) {
                        let _ = engine.core().write(
                            &handle,
                            vec![put(&format!("{index}-{step}"), vec![1.0, 0.0])],
                        );
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
    write(&engine, &handle, vec![put("alpha", vec![1.0, 0.0])]);
    let pinned = handle.current();

    engine
        .drop_collection(&reference("documents"))
        .expect("drop should succeed");
    assert!(handle.is_dropped());
    let error = engine
        .core()
        .write(&handle, vec![put("beta", vec![0.0, 1.0])])
        .expect_err("a dropped handle must refuse writes");
    assert!(error.to_string().contains("does not exist"), "{error}");
    assert!(engine.collection(&reference("documents")).is_err());
    assert_eq!(pinned.visible_seq_no, 1);
    assert_eq!(
        pinned.delta.len(),
        1,
        "a pinned version keeps its resident state"
    );
    assert!(
        engine.drop_collection(&reference("documents")).is_err(),
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
    write(&engine, &healthy, vec![put("alpha", vec![1.0, 0.0])]);
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
        .drop_collection(&reference("broken"))
        .expect("a failed collection can be dropped");
    assert_eq!(engine.core().list_descriptors().expect("listing").len(), 1);
}

#[test]
fn version_ids_increase_and_invariants_hold_across_writes_flush_and_compaction() {
    let root = unique_temp_dir("engine-version-ids");
    let engine = open(&root).expect("engine should open");
    let handle = create(&engine, "documents");
    let core = engine.core();
    let mut last = handle.current();
    last.check_invariants().expect("initial version is valid");
    for round in 0..3 {
        write(
            &engine,
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
        core.flush_collection(&handle)
            .expect("flush should succeed");
        let flushed = handle.current();
        assert!(flushed.id > written.id);
        assert_eq!(flushed.counters.memtable_rows, 0);
        assert_eq!(flushed.visible_seq_no, written.visible_seq_no);
        flushed
            .check_invariants()
            .expect("invariants hold after a flush");
        last = flushed;
    }
    core.compact_collection(&handle)
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
                        version.delta.to_vec().len(),
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
                        version.delta.to_vec().len(),
                    );
                    assert_eq!(now, summary);
                    version.check_invariants().expect("still consistent");
                }
                last_seen
            })
        })
        .collect::<Vec<_>>();

    let core = engine.core();
    let mut acked = 0;
    for index in 0..200 {
        let ack = core
            .write(&handle, vec![put(&format!("id-{index}"), vec![1.0, 0.0])])
            .expect("write should succeed");
        acked = ack.last_seq_no;
        // I1: a read that starts after the acknowledgement sees the write.
        assert!(handle.current().visible_seq_no >= acked);
        if index % 50 == 49 {
            core.flush_collection(&handle)
                .expect("flush should succeed");
        }
        assert!(Instant::now() < deadline, "the test ran too long");
    }
    done.store(true, Ordering::Release);
    for reader in readers {
        let seen = reader.join().expect("reader should join");
        assert!(seen <= acked);
    }
}
