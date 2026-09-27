//! Writer task tests: group commit, visibility (I1, I14), poisoning after a failed WAL write,
//! schema changes across a crash, and fairness for maintenance under a continuous writer.

use super::*;
use crate::{
    CreateCollectionRequest, Engine, EngineConfig,
    legacy_view::{legacy_ops, legacy_put},
    test_support::{ControlledVfs, put},
    version::DeltaOp,
};
use logpose_types::{
    CollectionRef, DistanceMetric, WriteOperation,
    record::Record,
    schema::{FieldType, ScalarFieldSpec},
    value::Value,
};
use logpose_vfs::{FaultPlan, FaultVfs, TearMode};
use logpose_wal::{BootId, codec::WirePk};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

const ROOT: &str = "/storage";

fn config(boot: &str) -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new(boot)),
        ..EngineConfig::default()
    }
}

fn reference(name: &str) -> CollectionRef {
    CollectionRef::new_default(name)
}

/// Create `name` with background maintenance off, so every job comes from the test.
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

fn ops(handle: &CollectionHandle, operations: Vec<WriteOperation>) -> Vec<ClientOp> {
    legacy_ops(handle.descriptor(), operations).expect("legacy ops should map")
}

/// Every record the published version shows, as `id -> (seq_no, metadata)`.
fn visible(handle: &CollectionHandle) -> BTreeMap<String, (SeqNo, serde_json::Value)> {
    let version = handle.current();
    version.check_invariants().expect("invariants hold");
    let mut latest = BTreeMap::new();
    for record in version.delta.iter() {
        match &record.op {
            DeltaOp::Put(image) => {
                let put = legacy_put(&version.schema, image).expect("row should read");
                latest.insert(
                    put.id.as_str().to_owned(),
                    Some((record.seq_no, put.metadata)),
                );
            }
            DeltaOp::Delete(pk) => {
                latest.insert(crate::legacy_view::legacy_id(pk).as_str().to_owned(), None);
            }
            DeltaOp::SchemaChange { .. } => {}
        }
    }
    latest
        .into_iter()
        .filter_map(|(id, value)| value.map(|value| (id, value)))
        .collect()
}

async fn write(handle: &Arc<CollectionHandle>, id: &str) -> Result<CommitAck> {
    handle
        .write(ops(handle, vec![put(id, vec![1.0, 0.0])]))
        .await
}

fn spawn_write(
    handle: &Arc<CollectionHandle>,
    id: String,
) -> tokio::task::JoinHandle<Result<CommitAck>> {
    let handle = Arc::clone(handle);
    tokio::spawn(async move { write(&handle, &id).await })
}

/// Many writers queued behind one fsync share the next one: 64 concurrent batches cost at
/// most three fsyncs, and every acknowledgement is already visible when it arrives (I1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_share_group_fsyncs() {
    let fault = FaultVfs::new(1);
    let vfs = ControlledVfs::wrap(fault.process());
    let engine = Engine::open(vfs.clone(), ROOT, config("boot")).expect("engine should open");
    let handle = create(&engine, "grouped");
    let before = vfs.file_syncs();

    // Hold the first group's fsync so that the other writers queue up behind it.
    vfs.hold_syncs();
    let first = spawn_write(&handle, "w-0".to_owned());
    assert!(vfs.wait_for_held_sync(Duration::from_secs(10)));
    let rest = (1..64)
        .map(|index| spawn_write(&handle, format!("w-{index}")))
        .collect::<Vec<_>>();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        handle.visible_seq_no(),
        0,
        "nothing is visible while the first group's fsync is held (I14)"
    );
    vfs.release_syncs();

    let mut last_seq_nos = Vec::new();
    for task in std::iter::once(first).chain(rest) {
        let ack = task
            .await
            .expect("task should join")
            .expect("write should succeed");
        assert!(
            handle.visible_seq_no() >= ack.last_seq_no,
            "an acknowledged write is visible (I1)"
        );
        assert!(ack.snapshot.visible_seq_no >= ack.last_seq_no);
        last_seq_nos.push(ack.last_seq_no);
    }
    last_seq_nos.sort_unstable();
    assert_eq!(last_seq_nos, (1..=64).collect::<Vec<_>>());
    let syncs = vfs.file_syncs() - before;
    assert!(
        syncs <= 3,
        "64 concurrent writers took {syncs} fsyncs; group commit should need at most 3"
    );
    assert_eq!(visible(&handle).len(), 64);
}

/// Linearizable acknowledgements: a client that got its ack and immediately reads, on any
/// thread, sees its write (I1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acknowledged_write_is_immediately_readable_from_another_thread() {
    let fault = FaultVfs::new(2);
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should open");
    let handle = create(&engine, "linearizable");
    let clients = (0..8)
        .map(|client| {
            let handle = Arc::clone(&handle);
            tokio::spawn(async move {
                for index in 0..50 {
                    let id = format!("c{client}-{index}");
                    let ack = write(&handle, &id).await.expect("write should succeed");
                    let reader = Arc::clone(&handle);
                    let seen = std::thread::spawn(move || {
                        let version = reader.current();
                        let image = version.delta_image(&WirePk::String(id.clone()));
                        (version.visible_seq_no, image.map(|(seq_no, _)| seq_no))
                    })
                    .join()
                    .expect("reader should join");
                    assert!(seen.0 >= ack.last_seq_no, "visible_seq_no after the ack");
                    assert_eq!(seen.1, Some(ack.last_seq_no), "the write is readable");
                }
            })
        })
        .collect::<Vec<_>>();
    for client in clients {
        client.await.expect("client should finish");
    }
    assert_eq!(handle.visible_seq_no(), 400);
}

/// A failed WAL fsync whose rollback succeeds: the group's writes fail with a typed
/// `NotApplied` error (never an ack), so does the group prepared behind it, and the collection
/// turns read-only while still serving its last published version. Reopening in process is safe
/// and shows neither write, before and after a crash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_fsync_poisons_the_collection_and_fails_the_group_as_not_applied() {
    let fault = FaultVfs::new(3);
    let vfs = ControlledVfs::wrap(fault.process());
    let engine = Engine::open(vfs.clone(), ROOT, config("boot")).expect("engine should open");
    let handle = create(&engine, "poisoned");
    write(&handle, "kept").await.expect("write should succeed");

    vfs.hold_syncs();
    vfs.fail_next_syncs(1);
    let failed = spawn_write(&handle, "failed".to_owned());
    assert!(vfs.wait_for_held_sync(Duration::from_secs(10)));
    let behind = spawn_write(&handle, "behind".to_owned());
    tokio::time::sleep(Duration::from_millis(200)).await;
    vfs.release_syncs();

    for (name, task) in [("failed", failed), ("behind", behind)] {
        let error = task
            .await
            .expect("task should join")
            .expect_err("a write in or behind a failed group must not be acknowledged");
        assert!(
            matches!(
                error,
                LogPoseError::WalWriteFailed {
                    outcome: WriteOutcome::NotApplied,
                    ..
                }
            ),
            "{name}: {error}"
        );
    }
    assert!(handle.is_poisoned());
    let refused = write(&handle, "later")
        .await
        .expect_err("a poisoned collection refuses writes");
    assert!(
        matches!(refused, LogPoseError::CollectionPoisoned { .. }),
        "{refused}"
    );
    assert!(
        refused
            .to_string()
            .contains("read-only until it is reopened"),
        "{refused}"
    );
    assert_eq!(
        visible(&handle).keys().collect::<Vec<_>>(),
        vec!["kept"],
        "the last published version keeps serving reads"
    );
    let begin = std::thread::scope(|scope| {
        scope
            .spawn(|| handle.begin_job(JobKind::Flush).map(|_| ()))
            .join()
            .expect("thread should join")
    });
    assert!(begin.is_err(), "a poisoned collection refuses maintenance");

    // Reopen in process: the rollback was durable, so the WAL holds exactly what was published.
    drop(handle);
    drop(engine);
    let engine = Engine::open(vfs.clone(), ROOT, config("boot")).expect("engine should reopen");
    let handle = engine
        .collection(&reference("poisoned"))
        .expect("collection should reopen");
    assert_eq!(visible(&handle).keys().collect::<Vec<_>>(), vec!["kept"]);
    let ack = write(&handle, "after").await.expect("write should succeed");
    assert_eq!(
        ack.last_seq_no, 2,
        "the failed group's sequence numbers are reused"
    );

    drop(handle);
    drop(engine);
    fault.crash();
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should reopen");
    let handle = engine
        .collection(&reference("poisoned"))
        .expect("collection should reopen");
    assert_eq!(
        visible(&handle).keys().collect::<Vec<_>>(),
        vec!["after", "kept"]
    );
}

/// A failed fsync whose rollback fails too: the write's outcome is unknown, the WAL is fenced,
/// and the collection is failed for this process. Reopening in the same boot is refused;
/// after a reboot the on-disk bytes are the truth and the batch is there whole or not at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_rollback_fences_the_wal_and_fails_the_collection() {
    let fault = FaultVfs::new(4);
    let vfs = ControlledVfs::wrap(fault.process());
    let engine = Engine::open(vfs.clone(), ROOT, config("boot-a")).expect("engine should open");
    let handle = create(&engine, "fenced");
    write(&handle, "kept").await.expect("write should succeed");

    // The group's sync_data and the rollback's sync_all.
    vfs.fail_next_syncs(2);
    let error = write(&handle, "unknown")
        .await
        .expect_err("the write must not be acknowledged");
    assert!(
        matches!(
            error,
            LogPoseError::WalWriteFailed {
                outcome: WriteOutcome::Unknown { fenced: true },
                ..
            }
        ),
        "{error}"
    );
    let refused = write(&handle, "later").await.expect_err("refused");
    assert!(
        matches!(refused, LogPoseError::CollectionPoisoned { .. })
            && refused.to_string().contains("rollback failed")
            && refused.to_string().contains("restart the process"),
        "{refused}"
    );

    drop(handle);
    drop(engine);
    let engine = Engine::open(vfs.clone(), ROOT, config("boot-a")).expect("engine should open");
    let error = engine
        .collection(&reference("fenced"))
        .expect_err("the same boot must not reopen a fenced collection");
    assert!(error.to_string().contains("is fenced"), "{error}");
    drop(engine);

    fault.crash();
    let engine =
        Engine::open(fault.process(), ROOT, config("boot-b")).expect("engine should reopen");
    let handle = engine
        .collection(&reference("fenced"))
        .expect("a reboot recovers the collection");
    let ids = visible(&handle).into_keys().collect::<Vec<_>>();
    assert!(
        ids == ["kept"] || ids == ["kept", "unknown"],
        "the failed batch is present whole or not at all: {ids:?}"
    );
    write(&handle, "later").await.expect("the collection works");
}

/// When the fence marker cannot be written either, nothing on disk records the hazard, so the
/// engine stops the process through its fatal handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unfenced_rollback_failure_stops_the_process() {
    let fault = FaultVfs::new(5);
    let vfs = ControlledVfs::wrap(fault.process());
    let fatal = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&fatal);
    let engine = Engine::open(
        vfs.clone(),
        ROOT,
        EngineConfig {
            on_fatal: Some(Arc::new(move |error: &LogPoseError| {
                recorded
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(error.to_string());
            })),
            ..config("boot")
        },
    )
    .expect("engine should open");
    let handle = create(&engine, "unfenced");
    vfs.fail_next_syncs(2);
    vfs.fail_creates_containing("FSYNC_FAILED");
    let error = write(&handle, "unknown")
        .await
        .expect_err("not acknowledged");
    assert!(
        matches!(
            error,
            LogPoseError::WalWriteFailed {
                outcome: WriteOutcome::Unknown { fenced: false },
                ..
            }
        ),
        "{error}"
    );
    let calls = fatal.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert!(calls[0].contains("must stop"), "{calls:?}");
}

use std::sync::PoisonError;

/// Visibility never runs ahead of durability (I14): while a group's fsync has not returned,
/// no reader sees it, and a crash at that moment takes back nothing any reader saw.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_visible_before_its_fsync_and_a_crash_takes_back_nothing_seen() {
    let fault = FaultVfs::new(6);
    let vfs = ControlledVfs::wrap(fault.process());
    let engine = Engine::open(vfs.clone(), ROOT, config("boot")).expect("engine should open");
    let handle = create(&engine, "durable");
    write(&handle, "a").await.expect("write should succeed");

    vfs.hold_syncs();
    let pending = spawn_write(&handle, "b".to_owned());
    assert!(vfs.wait_for_held_sync(Duration::from_secs(10)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(handle.visible_seq_no(), 1);
    assert_eq!(visible(&handle).keys().collect::<Vec<_>>(), vec!["a"]);

    // Power fails while the fsync is outstanding.
    fault.set_plan(FaultPlan {
        crash_after_ops: Some(0),
        ..FaultPlan::default()
    });
    vfs.release_syncs();
    let outcome = pending.await.expect("task should join");
    assert!(outcome.is_err(), "the write was never acknowledged");
    assert_eq!(visible(&handle).keys().collect::<Vec<_>>(), vec!["a"]);

    drop(handle);
    drop(engine);
    fault.crash();
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should reopen");
    let handle = engine
        .collection(&reference("durable"))
        .expect("collection should reopen");
    assert_eq!(visible(&handle).keys().collect::<Vec<_>>(), vec!["a"]);
}

/// Randomized I14: readers record the highest `visible_seq_no` they ever observe while writers
/// run into a crash at a random operation; recovery keeps at least everything that was seen,
/// and every acknowledged write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_keeps_everything_any_reader_saw() {
    for seed in 0..24_u64 {
        let fault = FaultVfs::new(seed);
        let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine opens");
        let handle = create(&engine, "observed");
        let setup = fault.mutating_ops();
        fault.set_plan(FaultPlan {
            crash_after_ops: Some(setup + 5 + seed * 3),
            tear: TearMode::ALL[(seed % 4) as usize],
            ..FaultPlan::default()
        });
        let seen = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let reader = {
            let handle = Arc::clone(&handle);
            let seen = Arc::clone(&seen);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    seen.fetch_max(handle.visible_seq_no(), Ordering::AcqRel);
                }
            })
        };
        let writers = (0..4)
            .map(|client| {
                let handle = Arc::clone(&handle);
                tokio::spawn(async move {
                    let mut acked = 0;
                    for index in 0..20 {
                        match write(&handle, &format!("{client}-{index}")).await {
                            Ok(ack) => acked = acked.max(ack.last_seq_no),
                            Err(_) => break,
                        }
                    }
                    acked
                })
            })
            .collect::<Vec<_>>();
        let mut acked = 0;
        for writer in writers {
            acked = acked.max(writer.await.expect("writer should join"));
        }
        done.store(true, Ordering::Release);
        reader.join().expect("reader should join");
        let observed = seen.load(Ordering::Acquire).max(handle.visible_seq_no());
        drop(handle);
        drop(engine);
        fault.crash();
        let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine reopens");
        let recovered = engine
            .collection(&reference("observed"))
            .expect("collection recovers")
            .visible_seq_no();
        assert!(
            recovered >= observed && recovered >= acked,
            "seed {seed}: recovered {recovered}, observed {observed}, acked {acked}"
        );
    }
}

/// Schema changes are in the request stream and the WAL: a batch is validated against the
/// schema at its point in the stream, and recovery replays each batch with that schema,
/// starting from a manifest whose schema is newer than its checkpoint.
#[test]
fn schema_changes_replay_with_the_schema_of_each_record_across_a_crash() {
    let fault = FaultVfs::new(7);
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should open");
    let handle = create(&engine, "altered");
    let core = engine.core();
    let price = || ScalarFieldSpec::new("price", FieldType::Int64);
    let record = |id: &str, price: Option<i64>, extra: serde_json::Value| {
        let mut record = Record::new(id).with_vector("vector", vec![1.0, 0.0]);
        if let Some(price) = price {
            record = record.with_field("price", Value::Int64(price));
        }
        if let serde_json::Value::Object(extra) = extra {
            record.extra = extra;
        }
        ClientOp::Upsert(record)
    };

    // Seq 1: `price` is a dynamic key, since the schema does not declare it.
    handle
        .write_blocking(vec![record("a", None, json!({"price": 1, "color": "red"}))])
        .expect("write a");
    // A typed value for an undeclared field is refused... unless it can be dynamic: it moves to
    // `$extra`, which is the v1 behavior for undeclared keys.
    let (mut ticket, start) = handle.begin_job(JobKind::Flush).expect("flush begins");
    let frozen = start.version;
    assert_eq!(frozen.visible_seq_no, 1);

    // Seq 2..=4, above the flush checkpoint: add `price`, write `b` with a typed price, drop it.
    let added = handle
        .alter_schema_blocking(SchemaChange::AddField(price()))
        .expect("add price");
    assert_eq!(added.last_seq_no, 2);
    handle
        .write_blocking(vec![record("b", Some(7), json!({}))])
        .expect("write b");
    assert_eq!(
        visible(&handle)["b"].1,
        json!({"price": 7}),
        "b is validated against the schema that declares price"
    );
    assert_eq!(
        visible(&handle)["a"].1,
        json!({"color": "red"}),
        "a's dynamic price is shadowed once price is declared"
    );
    handle
        .alter_schema_blocking(SchemaChange::DropField {
            name: "price".to_owned(),
        })
        .expect("drop price");
    // Seq 5: `cost` is a new field; `price` stays retired.
    handle
        .alter_schema_blocking(SchemaChange::AddField(ScalarFieldSpec::new(
            "cost",
            FieldType::Int64,
        )))
        .expect("add cost");
    let mut c = Record::new("c").with_vector("vector", vec![0.0, 1.0]);
    c = c.with_field("cost", Value::Int64(9));
    handle
        .write_blocking(vec![ClientOp::Upsert(c)])
        .expect("write c");
    let invalid = handle
        .alter_schema_blocking(SchemaChange::DropField {
            name: "missing".to_owned(),
        })
        .expect_err("an invalid change fails");
    assert!(
        invalid.to_string().contains("invalid schema change"),
        "{invalid}"
    );

    // Commit the flush now: the manifest records schema version 4 with checkpoint 1.
    let segment = core
        .write_segment_file(
            handle.descriptor(),
            &frozen
                .delta
                .iter()
                .filter_map(|record| {
                    crate::legacy_view::legacy_record(&frozen.schema, record).expect("legacy")
                })
                .collect::<Vec<_>>(),
            crate::segment_v1::SegmentBuild {
                unit: start.unit,
                purpose: crate::segment_v1::SegmentPurpose::Flush,
                origin: crate::manifest::SegmentOrigin::Flush {
                    first_seq_no: 1,
                    last_seq_no: 1,
                },
                schema_version: frozen.schema.schema_version(),
            },
        )
        .expect("segment should write");
    ticket.writing_files();
    ticket
        .commit(JobCommit::Flush {
            checkpoint_seq_no: 1,
            segment: Some(segment),
        })
        .expect("flush should commit");
    let before = handle.current();
    assert_eq!(before.schema.schema_version(), 4);
    assert_eq!(before.checkpoint_seq_no, 1);
    assert_eq!(before.visible_seq_no, 6);
    let before_rows = visible(&handle);
    assert_eq!(before_rows["b"].1, json!({}), "b's price was dropped");
    assert_eq!(before_rows["c"].1, json!({"cost": 9}));

    drop((ticket_guard(), before, handle, core));
    drop(engine);
    fault.crash();
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should reopen");
    let handle = engine
        .collection(&reference("altered"))
        .expect("collection should reopen");
    let after = handle.current();
    assert_eq!(after.schema.schema_version(), 4);
    assert_eq!(after.visible_seq_no, 6);
    assert_eq!(visible(&handle), before_rows);
    // Replay skipped the schema changes the manifest already reflects and read `b` (written
    // under version 2) with version 4: the dropped field's value is gone from the row itself.
    let b = after
        .delta_image(&WirePk::String("b".to_owned()))
        .expect("b is in the delta");
    assert!(b.1.scalars.is_empty(), "{:?}", b.1);
    let kinds = after
        .delta
        .iter()
        .map(|record| match record.op {
            DeltaOp::Put(_) => "put",
            DeltaOp::Delete(_) => "delete",
            DeltaOp::SchemaChange { .. } => "schema",
        })
        .collect::<Vec<_>>();
    assert_eq!(kinds, ["schema", "put", "schema", "schema", "put"]);
    handle
        .write_blocking(vec![ClientOp::Upsert(
            Record::new("d")
                .with_vector("vector", vec![1.0, 1.0])
                .with_field("cost", Value::Int64(3)),
        )])
        .expect("writes continue with the recovered schema");
}

fn ticket_guard() {}

/// A continuous stream of writes must not starve maintenance: flushes, a compaction, and a
/// drop all finish promptly while several clients keep writing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_and_drops_make_progress_under_continuous_writes() {
    let fault = FaultVfs::new(8);
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should open");
    let handle = create(&engine, "busy");
    let stop = Arc::new(AtomicBool::new(false));
    let clients = (0..4)
        .map(|client| {
            let handle = Arc::clone(&handle);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut index = 0;
                while !stop.load(Ordering::Acquire) {
                    if write(&handle, &format!("{client}-{index}")).await.is_err() {
                        break;
                    }
                    index += 1;
                    // Keep the delta small enough that a debug-build v1 flush (which builds an
                    // HNSW sidecar) stays fast; the writer still never goes idle for long.
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                index
            })
        })
        .collect::<Vec<_>>();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for round in 0..3 {
        let flushed = tokio::time::timeout(Duration::from_secs(20), {
            let handle = Arc::clone(&handle);
            engine.job(move |core| core.flush_collection(&handle))
        })
        .await;
        let snapshot = flushed
            .expect("a flush starved behind the writers")
            .expect("flush should succeed");
        assert!(snapshot.manifest_generation > round);
    }
    let compacted = tokio::time::timeout(Duration::from_secs(20), {
        let handle = Arc::clone(&handle);
        engine.job(move |core| core.compact_collection(&handle))
    })
    .await;
    compacted
        .expect("compaction starved behind the writers")
        .expect("compaction should succeed");
    assert_eq!(handle.current().manifest.segments.len(), 1);

    let started = Instant::now();
    let dropped = tokio::time::timeout(Duration::from_secs(20), {
        let engine = engine.clone();
        tokio::task::spawn_blocking(move || engine.drop_collection(&reference("busy")))
    })
    .await;
    dropped
        .expect("drop starved behind the writers")
        .expect("drop task should join")
        .expect("drop should succeed");
    assert!(started.elapsed() < Duration::from_secs(20));
    stop.store(true, Ordering::Release);
    let mut writes = 0;
    for client in clients {
        writes += client.await.expect("client should finish");
    }
    assert!(writes > 0, "the clients kept writing");
    assert!(engine.collection(&reference("busy")).is_err());
}

/// Recovery refuses a log that `CURRENT` went backwards on: the WAL files below the newer
/// checkpoint are gone once its manifest is durable, and a checkpoint frame ahead of the
/// manifest would fail the replay's cross-check if they were not.
#[test]
fn a_checkpoint_frame_above_the_manifest_checkpoint_fails_recovery() {
    let fault = FaultVfs::new(9);
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should open");
    let handle = create(&engine, "rewound");
    let core = engine.core();
    core.write(&handle, vec![put("a", vec![1.0, 0.0])])
        .expect("write a");
    core.flush_collection(&handle).expect("flush");
    // The flush's checkpoint frame rides along with the next group.
    core.write(&handle, vec![put("b", vec![1.0, 0.0])])
        .expect("write b");
    let current = handle.meta().dir.join(crate::manifest::CURRENT_FILE);
    drop((handle, core));
    drop(engine);

    // Point CURRENT back at generation 0, whose checkpoint is 0.
    let vfs = fault.process();
    vfs.remove_file(&current).expect("remove CURRENT");
    let file = vfs
        .open(&current, logpose_vfs::OpenMode::CreateNew)
        .expect("create CURRENT");
    file.append(&[std::io::IoSlice::new(b"00000000000000000000\n")])
        .expect("write CURRENT");
    file.sync_all().expect("sync CURRENT");

    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should open");
    let error = engine
        .collection(&reference("rewound"))
        .expect_err("recovery must refuse");
    assert!(error.to_string().contains("checkpoint"), "{error}");
}

/// A directory with the version 1 WAL is rejected with a typed format error.
#[test]
fn a_version_1_wal_is_an_unsupported_format() {
    let fault = FaultVfs::new(10);
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should open");
    let handle = create(&engine, "legacy");
    let wal_dir = crate::engine::EngineCore::wal_dir(handle.descriptor());
    drop(handle);
    drop(engine);
    let vfs = fault.process();
    vfs.open(
        &wal_dir.join("active.wal"),
        logpose_vfs::OpenMode::CreateNew,
    )
    .expect("create active.wal");
    let engine = Engine::open(fault.process(), ROOT, config("boot")).expect("engine should open");
    let error = engine
        .collection(&reference("legacy"))
        .expect_err("a v1 WAL is refused");
    assert!(error.to_string().contains("unexpected WAL file"), "{error}");
}
