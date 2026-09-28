//! Crash-recovery tests on `FaultVfs`: crashes while creating and dropping a collection, one
//! test per named crash point with its documented outcome, and failed WAL fsyncs. The
//! exhaustive crash enumeration (every operation of a scenario under every tear mode, with
//! recovery idempotence against a clean recovery) is harness v2's (`tests/harness/crash.rs`).
//!
//! Every test checks the recovery contract: every acknowledged batch is present, an
//! unacknowledged batch is either entirely present or entirely absent, and the engine keeps
//! working (writes, flushes and compactions succeed and survive another crash) afterwards.

// Assertion helpers panic with the crash context on failure.
#![allow(clippy::panic)]

use arc_swap as _;
use bytemuck as _;
use crc32c as _;
use imbl as _;
use logpose_auth as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query as _;
use logpose_wal as _;
use postcard as _;
use rand as _;
use rayon as _;
use roaring as _;
use serde as _;
use thiserror as _;
use tracing as _;
use twox_hash as _;
use uuid as _;

#[path = "support/engine.rs"]
mod db;

use db::{Row, create, handle, put_with, scan};
use logpose_storage::{CreateCollectionRequest, Engine, EngineConfig};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, CollectionAssignment, CollectionRef, DistanceMetric, LogPoseError,
    NodeRole, SeqNo, Snapshot, WriteOutcome,
    record::{ClientOp, PrimaryKey},
};
use logpose_vfs::{CrashPoint, FaultPlan, FaultVfs, OpenMode, TearMode, Vfs};
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::IoSlice,
    path::{Path, PathBuf},
    sync::Arc,
};

const ROOT: &str = "/storage";
const COLLECTION: &str = "crashy";

#[derive(Clone, Debug)]
enum Step {
    Write(Vec<ClientOp>),
    Flush,
    Compact,
}

fn put(id: &str, x: f32) -> ClientOp {
    put_with(id, vec![x, 1.0], json!({"key": id, "x": x}))
}

fn delete(id: &str) -> ClientOp {
    db::delete(id)
}

/// Two flushes (the second rolls a WAL file with data and writes a DV file for the first
/// segment, whose `b` and `c` rows the writes between them supersede), a compaction of the two
/// segments, and writes before, between and after them.
fn scenario() -> Vec<Step> {
    vec![
        Step::Write(vec![put("a", 1.0), put("b", 2.0)]),
        Step::Write(vec![put("c", 3.0), delete("a")]),
        Step::Flush,
        Step::Write(vec![put("a", 4.0)]),
        Step::Write(vec![put("d", 5.0), put("b", 6.0), delete("c")]),
        Step::Flush,
        Step::Compact,
        Step::Write(vec![put("e", 7.0)]),
    ]
}

/// What a (possibly crashed) scenario run achieved.
#[derive(Debug, Default)]
struct Outcome {
    /// Batches whose write returned `Ok`, in order.
    acked: Vec<Vec<ClientOp>>,
    /// The batch whose write failed, if the run stopped in a write.
    in_flight: Option<Vec<ClientOp>>,
    /// The step that failed, if any.
    failed_step: Option<usize>,
    /// Snapshots taken after each completed step, with the number of batches acked by then.
    snapshots: Vec<(Snapshot, usize)>,
}

struct Harness {
    fault: Arc<FaultVfs>,
    engine: Option<Engine>,
}

/// Open an engine on `fault`'s filesystem.
fn open(fault: &FaultVfs) -> logpose_types::Result<Engine> {
    Engine::open(fault.process(), ROOT, EngineConfig::default())
}

impl Harness {
    /// A fresh filesystem with a created collection whose background maintenance is disabled,
    /// so every mutating operation comes from the test and runs are deterministic.
    async fn new(seed: u64) -> Self {
        let fault = FaultVfs::new(seed);
        let engine = open(&fault).expect("engine should open on a fresh filesystem");
        let mut descriptor = engine
            .plan_collection_descriptor(&CreateCollectionRequest::new(
                COLLECTION,
                2,
                DistanceMetric::Dot,
            ))
            .expect("descriptor should plan");
        descriptor.flush_threshold_ops = usize::MAX;
        descriptor.flush_threshold_bytes = usize::MAX;
        descriptor.compaction_threshold_segments = usize::MAX;
        engine
            .create_collection(
                descriptor,
                Some(CollectionAssignment {
                    assigned_node: ANONYMOUS_LOCAL_NODE_NAME.to_owned(),
                    assigned_role: NodeRole::Data,
                }),
            )
            .await
            .expect("collection should be created");
        Self {
            fault,
            engine: Some(engine),
        }
    }

    fn engine(&self) -> &Engine {
        self.engine
            .as_ref()
            .expect("engine should be open between crashes")
    }

    async fn run(&self, steps: &[Step]) -> Outcome {
        let engine = self.engine();
        let mut outcome = Outcome::default();
        for (index, step) in steps.iter().enumerate() {
            let result = match step {
                Step::Write(ops) => match handle(engine, COLLECTION).write(ops.clone()).await {
                    Ok(_) => {
                        outcome.acked.push(ops.clone());
                        Ok(())
                    }
                    Err(error) => {
                        outcome.in_flight = Some(ops.clone());
                        Err(error)
                    }
                },
                Step::Flush => handle(engine, COLLECTION).flush().await.map(|_| ()),
                Step::Compact => handle(engine, COLLECTION).compact().await.map(|_| ()),
            };
            if result.is_err() {
                outcome.failed_step = Some(index);
                break;
            }
            // Snapshot reads do no I/O that mutates, so they do not shift crash op counts.
            match handle(engine, COLLECTION).snapshot() {
                Ok(snapshot) => outcome.snapshots.push((snapshot, outcome.acked.len())),
                Err(_) => {
                    outcome.failed_step = Some(index);
                    break;
                }
            }
        }
        outcome
    }

    /// Drop the engine (the crashed process), apply the crash model, and reopen.
    fn crash_and_reopen(&mut self) {
        self.engine = None;
        self.fault.crash();
        self.engine = Some(open(&self.fault).expect("engine should reopen after a crash"));
    }
}

/// The rows visible after applying `batches` (upserts and deletes) in order, numbering
/// operations from 1.
fn expected_visible(batches: &[Vec<ClientOp>]) -> Vec<Row> {
    let mut latest = BTreeMap::<PrimaryKey, Option<Row>>::new();
    for (op, seq_no) in batches.iter().flatten().zip(1..) {
        let visible = match op {
            ClientOp::Upsert(record) => Some(Row {
                id: record.pk.label(),
                vector: record.vectors["vector"].clone(),
                metadata: serde_json::Value::Object(record.extra.clone()),
                seq_no,
            }),
            ClientOp::Delete(_) => None,
            ClientOp::Update(_) => unreachable!("the scenarios upsert and delete"),
        };
        latest.insert(op.pk().clone(), visible);
    }
    latest.into_values().flatten().collect()
}

fn op_count(batches: &[Vec<ClientOp>]) -> SeqNo {
    batches.iter().map(|batch| batch.len() as SeqNo).sum()
}

/// Check the recovery contract after a crash and return the batches recovery kept.
async fn assert_recovered(
    harness: &Harness,
    outcome: &Outcome,
    context: &str,
) -> Vec<Vec<ClientOp>> {
    let engine = harness.engine();
    let recovered = scan(engine, COLLECTION, None)
        .await
        .unwrap_or_else(|error| panic!("{context}: recovery failed: {error}"));
    let snapshot = handle(engine, COLLECTION)
        .snapshot()
        .unwrap_or_else(|error| panic!("{context}: snapshot failed: {error}"));

    let mut kept = outcome.acked.clone();
    if recovered != expected_visible(&kept) {
        let Some(in_flight) = &outcome.in_flight else {
            panic!(
                "{context}: recovered state is not the acknowledged state\nexpected: {:#?}\nrecovered: {recovered:#?}",
                expected_visible(&kept)
            );
        };
        kept.push(in_flight.clone());
        assert_eq!(
            recovered,
            expected_visible(&kept),
            "{context}: recovered state is neither the acknowledged state nor the acknowledged \
             state plus the whole in-flight batch"
        );
    }
    assert_eq!(
        snapshot.visible_seq_no,
        op_count(&kept),
        "{context}: visible sequence number must end exactly at a batch boundary"
    );
    let stats = handle(engine, COLLECTION)
        .stats(None)
        .unwrap_or_else(|error| panic!("{context}: stats failed: {error}"));
    assert_eq!(stats.live_record_count, recovered.len(), "{context}");
    let current = Snapshot {
        manifest_generation: stats.manifest_generation,
        visible_seq_no: stats.visible_seq_no,
    };

    // A snapshot handed out before the crash reads exactly the state it named when that state
    // is the recovered one; any other is expired (recovery retains only its first version, and
    // tokens, which could pin another, do not survive a restart). It never reads a different
    // state.
    for (snapshot, acked) in &outcome.snapshots {
        let read = scan(engine, COLLECTION, Some(snapshot.clone())).await;
        if *snapshot == current {
            let read = read.unwrap_or_else(|error| {
                panic!("{context}: snapshot {snapshot:?} is unreadable after recovery: {error}")
            });
            assert_eq!(
                read,
                expected_visible(&outcome.acked[..*acked]),
                "{context}: snapshot {snapshot:?} changed across the crash"
            );
        } else {
            let error = read.expect_err("a snapshot of another state is not retained");
            assert!(
                matches!(error, LogPoseError::SnapshotExpired { .. }),
                "{context}: snapshot {snapshot:?}: {error}"
            );
        }
    }
    kept
}

/// After recovery the engine must keep working: a write, a flush and a compaction succeed and
/// survive a further crash.
async fn assert_engine_keeps_working(
    harness: &mut Harness,
    mut kept: Vec<Vec<ClientOp>>,
    context: &str,
) {
    let follow_up = vec![put("z", 9.0), delete("b")];
    let handle = handle(harness.engine(), COLLECTION);
    handle
        .write(follow_up.clone())
        .await
        .unwrap_or_else(|error| panic!("{context}: write after recovery failed: {error}"));
    kept.push(follow_up);
    handle
        .flush()
        .await
        .unwrap_or_else(|error| panic!("{context}: flush after recovery failed: {error}"));
    handle
        .compact()
        .await
        .unwrap_or_else(|error| panic!("{context}: compact after recovery failed: {error}"));

    drop(handle);
    harness.crash_and_reopen();
    let recovered = scan(harness.engine(), COLLECTION, None)
        .await
        .unwrap_or_else(|error| panic!("{context}: second recovery failed: {error}"));
    assert_eq!(
        recovered,
        expected_visible(&kept),
        "{context}: state after recovery, more work and a second crash"
    );
}

/// Crash before every mutating operation of creating a collection: afterwards the collection
/// either does not exist and can be created again, or exists, is empty, and works.
#[tokio::test]
async fn a_crash_while_creating_a_collection_leaves_it_absent_or_usable() {
    let create_ops = {
        let fault = FaultVfs::new(0);
        let engine = open(&fault).expect("engine should open on a fresh filesystem");
        let before = fault.mutating_ops();
        create(
            &engine,
            CreateCollectionRequest::new(COLLECTION, 2, DistanceMetric::Dot),
        )
        .await
        .expect("clean create should succeed");
        fault.mutating_ops() - before
    };
    assert!(
        create_ops > 10,
        "creating a collection should take many operations"
    );

    for tear in TearMode::ALL {
        for k in 0..=create_ops {
            let context = format!("tear={tear:?} crash_after_ops={k} during create");
            let fault = FaultVfs::new(k * 4 + tear as u64);
            let engine = open(&fault).expect("engine should open on a fresh filesystem");
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let created = create(
                &engine,
                CreateCollectionRequest::new(COLLECTION, 2, DistanceMetric::Dot),
            )
            .await
            .is_ok();
            drop(engine);
            fault.crash();

            let engine =
                open(&fault).unwrap_or_else(|error| panic!("{context}: reopen failed: {error}"));
            let listed = engine
                .list_collections()
                .unwrap_or_else(|error| panic!("{context}: listing failed: {error}"));
            if created {
                assert_eq!(
                    listed.len(),
                    1,
                    "{context}: an acknowledged create must survive"
                );
            }
            if listed.is_empty() {
                create(
                    &engine,
                    CreateCollectionRequest::new(COLLECTION, 2, DistanceMetric::Dot),
                )
                .await
                .unwrap_or_else(|error| panic!("{context}: create again failed: {error}"));
            } else {
                assert_eq!(listed.len(), 1, "{context}");
                let visible = scan(&engine, COLLECTION, None)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: scan failed: {error}"));
                assert!(visible.is_empty(), "{context}: a new collection is empty");
            }
            handle(&engine, COLLECTION)
                .write(vec![put("a", 1.0)])
                .await
                .unwrap_or_else(|error| panic!("{context}: write failed: {error}"));
            handle(&engine, COLLECTION)
                .flush()
                .await
                .unwrap_or_else(|error| panic!("{context}: flush failed: {error}"));
            drop(engine);
            fault.crash();
            let engine = open(&fault)
                .unwrap_or_else(|error| panic!("{context}: second reopen failed: {error}"));
            assert_eq!(
                scan(&engine, COLLECTION, None)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: second scan failed: {error}")),
                expected_visible(&[vec![put("a", 1.0)]]),
                "{context}"
            );
        }
    }
}

/// Collection directories left under the storage root, retired ones included.
fn collection_dir_entries(harness: &Harness) -> Vec<String> {
    harness
        .fault
        .process()
        .list(&Path::new(ROOT).join("collections"))
        .expect("collections should list")
        .into_iter()
        .map(|entry| entry.name)
        .collect()
}

/// A drop that returned `Ok` survives any crash; a drop interrupted by a crash leaves the
/// collection either gone or whole, and the root usable either way.
#[tokio::test]
async fn a_crash_while_dropping_a_collection_leaves_it_gone_or_whole() {
    let steps = &scenario()[..3];
    let drop_ops = {
        let harness = Harness::new(0).await;
        harness.run(steps).await;
        let before = harness.fault.mutating_ops();
        harness
            .engine()
            .drop_collection_blocking(&CollectionRef::new_default(COLLECTION))
            .expect("clean drop should succeed");
        harness.fault.mutating_ops() - before
    };
    assert!(drop_ops >= 3, "a drop renames, syncs and removes");

    for tear in TearMode::ALL {
        for k in 0..=drop_ops {
            let context = format!("tear={tear:?} crash_after_ops={k} during drop");
            let mut harness = Harness::new(k * 4 + tear as u64).await;
            let outcome = harness.run(steps).await;
            assert!(outcome.failed_step.is_none(), "{context}: setup failed");
            harness.fault.set_plan(FaultPlan {
                crash_after_ops: Some(harness.fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let dropped = harness
                .engine()
                .drop_collection_blocking(&CollectionRef::new_default(COLLECTION))
                .is_ok();
            harness.crash_and_reopen();

            let listed = harness
                .engine()
                .list_collections()
                .unwrap_or_else(|error| panic!("{context}: listing failed: {error}"));
            if dropped || listed.is_empty() {
                assert!(
                    listed.is_empty(),
                    "{context}: an acknowledged drop must survive the crash"
                );
                assert!(
                    collection_dir_entries(&harness).is_empty(),
                    "{context}: open removes what the drop left behind"
                );
                create(
                    harness.engine(),
                    CreateCollectionRequest::new(COLLECTION, 2, DistanceMetric::Dot),
                )
                .await
                .unwrap_or_else(|error| panic!("{context}: create again failed: {error}"));
                handle(harness.engine(), COLLECTION)
                    .write(vec![put("a", 1.0)])
                    .await
                    .unwrap_or_else(|error| panic!("{context}: write failed: {error}"));
            } else {
                let kept = assert_recovered(&harness, &outcome, &context).await;
                assert_engine_keeps_working(&mut harness, kept, &context).await;
            }
        }
    }
}

async fn run_to_crash_point(point: CrashPoint, seed: u64) -> (Harness, Outcome) {
    let mut harness = Harness::new(seed).await;
    harness.fault.set_plan(FaultPlan {
        crash_at: Some(point),
        ..FaultPlan::default()
    });
    let outcome = harness.run(&scenario()).await;
    assert!(
        outcome.failed_step.is_some(),
        "{point:?} should be reached by the scenario"
    );
    harness.engine = None;
    let report = harness.fault.crash();
    assert_eq!(report.triggered_at, Some(point));
    harness.engine = Some(open(&harness.fault).expect("engine should reopen"));
    (harness, outcome)
}

fn manifest_generation(harness: &Harness) -> u64 {
    handle(harness.engine(), COLLECTION)
        .stats(None)
        .expect("stats should succeed")
        .manifest_generation
}

/// One test per crash point a sequential scenario reaches, asserting the outcome from the crash
/// analysis: before `CURRENT` is durably renamed the flush or compaction is invisible; after, it
/// is complete; a WAL frame is durable exactly when its fsync returned.
#[tokio::test]
async fn named_crash_points_have_the_documented_outcome() {
    let scenario = scenario();
    let first_flush = scenario
        .iter()
        .position(|step| matches!(step, Step::Flush))
        .expect("scenario has a flush");
    let compact = scenario
        .iter()
        .position(|step| matches!(step, Step::Compact))
        .expect("scenario has a compaction");

    // A WAL frame appended but not synced is lost; once synced it survives even unacknowledged.
    let (harness, outcome) = run_to_crash_point(CrashPoint::WalAfterAppend, 10).await;
    assert_eq!(outcome.failed_step, Some(0));
    let kept = assert_recovered(&harness, &outcome, "WalAfterAppend").await;
    assert!(
        kept.is_empty(),
        "an unsynced frame must not survive: {kept:?}"
    );

    let (harness, outcome) = run_to_crash_point(CrashPoint::WalAfterSync, 11).await;
    let kept = assert_recovered(&harness, &outcome, "WalAfterSync").await;
    assert_eq!(kept.len(), 1, "a synced frame must survive");

    // Every step of the first flush before the durable `CURRENT` rename leaves generation 0:
    // the WAL rotation at the flush's begin, the segment, and the manifest.
    for (seed, point) in [
        CrashPoint::WalAfterRotateCreate,
        CrashPoint::FlushAfterSegmentSync,
        CrashPoint::FlushAfterSegmentsDirSync,
        CrashPoint::ManifestAfterFileSync,
        CrashPoint::ManifestAfterDirSync,
        CrashPoint::CurrentAfterTempSync,
        CrashPoint::CurrentAfterRename,
    ]
    .into_iter()
    .enumerate()
    {
        let (mut harness, outcome) = run_to_crash_point(point, 20 + seed as u64).await;
        let context = format!("{point:?}");
        assert_eq!(outcome.failed_step, Some(first_flush), "{context}");
        assert_eq!(manifest_generation(&harness), 0, "{context}");
        let kept = assert_recovered(&harness, &outcome, &context).await;
        assert_engine_keeps_working(&mut harness, kept, &context).await;
    }

    // Once the directory holding `CURRENT` is synced, the flush is published: recovery replays
    // nothing below its checkpoint.
    for (seed, point) in [CrashPoint::CurrentAfterDirSync].into_iter().enumerate() {
        let (mut harness, outcome) = run_to_crash_point(point, 30 + seed as u64).await;
        let context = format!("{point:?}");
        assert_eq!(outcome.failed_step, Some(first_flush), "{context}");
        assert_eq!(manifest_generation(&harness), 1, "{context}");
        let stats = handle(harness.engine(), COLLECTION)
            .stats(None)
            .expect("stats");
        assert_eq!(
            stats.mutable_op_count, 0,
            "{context}: the flush checkpointed everything"
        );
        let kept = assert_recovered(&harness, &outcome, &context).await;
        assert_engine_keeps_working(&mut harness, kept, &context).await;
    }

    // The second flush writes a DV file for the first segment; until its manifest is published
    // the file is an orphan and recovery replays the deletions from the WAL.
    let second_flush = scenario
        .iter()
        .enumerate()
        .skip(first_flush + 1)
        .find(|(_, step)| matches!(step, Step::Flush))
        .map(|(index, _)| index)
        .expect("scenario has a second flush");
    let (mut harness, outcome) = run_to_crash_point(CrashPoint::FlushAfterDvSync, 35).await;
    assert_eq!(outcome.failed_step, Some(second_flush));
    assert_eq!(manifest_generation(&harness), 1);
    let kept = assert_recovered(&harness, &outcome, "FlushAfterDvSync").await;
    assert_engine_keeps_working(&mut harness, kept, "FlushAfterDvSync").await;

    // A compaction whose output is synced but whose manifest is not published changes nothing.
    let (mut harness, outcome) =
        run_to_crash_point(CrashPoint::CompactionAfterOutputSync, 40).await;
    assert_eq!(outcome.failed_step, Some(compact));
    assert_eq!(manifest_generation(&harness), 2);
    let stats = handle(harness.engine(), COLLECTION)
        .stats(None)
        .expect("stats");
    assert_eq!(stats.segment_count, 2);
    let kept = assert_recovered(&harness, &outcome, "CompactionAfterOutputSync").await;
    assert_engine_keeps_working(&mut harness, kept, "CompactionAfterOutputSync").await;
}

/// A failed WAL fsync is rolled back before the write reports failure, so the batch never
/// reappears, even when the process crashes right after the rollback. The failed sync may have
/// written the whole frame before its error, so this fails unless the rollback is itself synced.
#[tokio::test]
async fn failed_wal_fsync_is_rolled_back_and_the_batch_never_reappears() {
    for tear in TearMode::ALL {
        for seed in 0..16 {
            let context = format!("WalAfterRollback tear={tear:?} seed={seed}");
            let mut harness = Harness::new(50 + seed).await;
            let outcome = harness.run(&scenario()[..1]).await;
            assert_eq!(outcome.acked.len(), 1, "{context}");

            // A write performs exactly one file sync: the WAL fsync. Fail the next one.
            harness.fault.set_plan(FaultPlan {
                fail_sync: Some(harness.fault.file_syncs()),
                crash_at: Some(CrashPoint::WalAfterRollback),
                tear,
                ..FaultPlan::default()
            });
            let error = handle(harness.engine(), COLLECTION)
                .write(vec![put("lost", 8.0)])
                .await
                .expect_err("the fsync failure should fail the write");
            assert!(
                matches!(
                    error,
                    LogPoseError::WalWriteFailed {
                        outcome: WriteOutcome::NotApplied,
                        ..
                    }
                ),
                "{context}: {error}"
            );
            let outcome = Outcome {
                acked: outcome.acked,
                in_flight: None,
                failed_step: Some(1),
                snapshots: outcome.snapshots,
            };
            harness.crash_and_reopen();
            let kept = assert_recovered(&harness, &outcome, &context).await;
            assert_engine_keeps_working(&mut harness, kept, &context).await;
        }
    }
}

/// A failed WAL fsync poisons the collection; reopening it in the same process (no crash) is
/// safe because the rollback was durable. Then more work and a crash: the failed batch is
/// invisible throughout, and every batch acknowledged after the reopen survives. Under every
/// tear mode the failed sync may have written part or all of its frame before the error, so
/// only a durable rollback keeps the batch from coming back.
#[tokio::test]
async fn failed_wal_fsync_then_more_writes_then_crash_keeps_exactly_the_acked_batches() {
    for tear in TearMode::ALL {
        for seed in 0..8 {
            let context = format!("tear={tear:?} seed={seed}");
            let mut harness = Harness::new(100 + seed).await;
            let mut outcome = harness.run(&scenario()[..2]).await;
            assert_eq!(outcome.acked.len(), 2, "{context}");

            harness.fault.set_plan(FaultPlan {
                fail_sync: Some(harness.fault.file_syncs()),
                tear,
                ..FaultPlan::default()
            });
            let error = handle(harness.engine(), COLLECTION)
                .write(vec![put("lost", 8.0)])
                .await
                .expect_err("the fsync failure should fail the write");
            assert!(
                matches!(
                    error,
                    LogPoseError::WalWriteFailed {
                        outcome: WriteOutcome::NotApplied,
                        ..
                    }
                ),
                "{context}: {error}"
            );
            let refused = handle(harness.engine(), COLLECTION)
                .write(vec![put("refused", 8.0)])
                .await
                .expect_err("the poisoned collection refuses writes");
            assert!(
                matches!(refused, LogPoseError::CollectionPoisoned { .. }),
                "{context}: {refused}"
            );
            let visible = scan(harness.engine(), COLLECTION, None)
                .await
                .unwrap_or_else(|error| panic!("{context}: scan after the failure: {error}"));
            assert_eq!(
                visible,
                expected_visible(&outcome.acked),
                "{context}: the failed batch must stay invisible in the same process"
            );

            // Reopen in the same process, without a crash.
            harness.engine = None;
            harness.engine = Some(open(&harness.fault).expect("engine should reopen in process"));
            let visible = scan(harness.engine(), COLLECTION, None)
                .await
                .unwrap_or_else(|error| panic!("{context}: scan after the reopen: {error}"));
            assert_eq!(
                visible,
                expected_visible(&outcome.acked),
                "{context}: the in-process reopen shows exactly the acknowledged batches"
            );

            let after = vec![put("after", 3.0), delete("b")];
            handle(harness.engine(), COLLECTION)
                .write(after.clone())
                .await
                .unwrap_or_else(|error| panic!("{context}: write after the failure: {error}"));
            outcome.acked.push(after);

            // `crash` applies the plan's tear mode to everything not yet synced.
            harness.crash_and_reopen();
            let kept = assert_recovered(&harness, &outcome, &context).await;
            assert_engine_keeps_working(&mut harness, kept, &context).await;
        }
    }
}

/// A torn WAL tail is repaired when the collection is recovered; a crash right after the repair
/// keeps it.
#[tokio::test]
async fn crash_after_wal_tail_repair_keeps_the_repair() {
    let mut harness = Harness::new(60).await;
    let outcome = harness.run(&scenario()[..2]).await;
    assert_eq!(outcome.acked.len(), 2);
    harness.engine = None;

    let active = active_wal_path(&harness);
    let file = harness
        .fault
        .open(&active, OpenMode::Append)
        .expect("active WAL should open");
    file.append(&[IoSlice::new(b"torn frame")])
        .expect("garbage append");
    file.sync_data().expect("garbage sync");
    drop(file);

    harness.fault.set_plan(FaultPlan {
        crash_at: Some(CrashPoint::RecoveryAfterTailRepair),
        ..FaultPlan::default()
    });
    let engine =
        open(&harness.fault).expect("the engine opens even when a collection fails to recover");
    let error = engine
        .collection(&CollectionRef::new_default(COLLECTION))
        .expect_err("recovery should crash right after repairing the tail");
    assert!(error.to_string().contains("tail repair"), "{error}");
    drop(engine);
    assert_eq!(
        harness.fault.crash().triggered_at,
        Some(CrashPoint::RecoveryAfterTailRepair)
    );
    harness.engine = Some(open(&harness.fault).expect("engine should reopen"));
    let kept = assert_recovered(&harness, &outcome, "RecoveryAfterTailRepair").await;
    assert_eq!(kept.len(), 2, "no write was in flight");
    assert_engine_keeps_working(&mut harness, kept, "RecoveryAfterTailRepair").await;
}

/// Every crash point a sequential run can reach is reached by a clean run, so the named tests
/// above cannot silently stop testing anything. `CompactionAfterDvSync` needs a deletion that
/// lands while a compaction runs (the storage unit tests step it); the GC, recovery, and
/// rollback points have tests of their own.
#[tokio::test]
async fn clean_run_reaches_every_implemented_crash_point() {
    let harness = Harness::new(70).await;
    let outcome = harness.run(&scenario()).await;
    assert!(outcome.failed_step.is_none());
    let hit = harness.fault.crash_points_hit();
    for point in [
        CrashPoint::WalAfterAppend,
        CrashPoint::WalAfterSync,
        CrashPoint::WalAfterRotateCreate,
        CrashPoint::FlushAfterSegmentSync,
        CrashPoint::FlushAfterDvSync,
        CrashPoint::FlushAfterSegmentsDirSync,
        CrashPoint::ManifestAfterFileSync,
        CrashPoint::ManifestAfterDirSync,
        CrashPoint::CurrentAfterTempSync,
        CrashPoint::CurrentAfterRename,
        CrashPoint::CurrentAfterDirSync,
        CrashPoint::CompactionAfterOutputSync,
    ] {
        assert!(hit.contains(&point), "{point:?} was not reached: {hit:?}");
    }
}

fn active_wal_path(harness: &Harness) -> PathBuf {
    let list = |dir: &Path| {
        harness
            .fault
            .list(dir)
            .expect("directory should list")
            .into_iter()
            .map(|entry| dir.join(entry.name))
            .collect::<Vec<_>>()
    };
    let collections = list(&Path::new(ROOT).join("collections"));
    assert_eq!(collections.len(), 1);
    let mut wal_files = list(&collections[0].join("wal"))
        .into_iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .collect::<Vec<_>>();
    wal_files.sort();
    wal_files.pop().expect("the collection has a WAL file")
}
