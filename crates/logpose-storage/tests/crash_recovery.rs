//! Crash-recovery tests on `FaultVfs`: exhaustive crash enumeration over a small scenario that
//! covers writes, flush (segment write, manifest publish, WAL rotation) and compaction, plus one
//! test per crash point the legacy engine implements.
//!
//! Every test checks the recovery contract: every acknowledged batch is present, an
//! unacknowledged batch is either entirely present or entirely absent, and the engine keeps
//! working (writes, flushes and compactions succeed and survive another crash) afterwards.

// Assertion helpers panic with the crash context on failure.
#![allow(clippy::panic)]

use async_trait as _;
use crc32c as _;
use crc32fast as _;
use logpose_auth as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query as _;
use logpose_wal as _;
use postcard as _;
use rand as _;
use roaring as _;
use serde as _;
use thiserror as _;
use twox_hash as _;
use uuid as _;

use logpose_storage::{CreateCollectionRequest, LocalStorageEngine, StorageEngine};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, CollectionAssignment, DeleteRecord, DistanceMetric, NodeRole,
    PutRecord, RecordId, SeqNo, Snapshot, VisibleRecord, WriteOperation,
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
    Write(Vec<WriteOperation>),
    Flush,
    Compact,
}

fn put(id: &str, x: f32) -> WriteOperation {
    WriteOperation::Put(PutRecord {
        id: RecordId::new(id),
        vector: vec![x, 1.0],
        metadata: json!({"key": id, "x": x}),
    })
}

fn delete(id: &str) -> WriteOperation {
    WriteOperation::Delete(DeleteRecord {
        id: RecordId::new(id),
    })
}

/// Two flushes (the second rolls a WAL file with data), a compaction of the two segments, and
/// writes before, between and after them.
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
    acked: Vec<Vec<WriteOperation>>,
    /// The batch whose write failed, if the run stopped in a write.
    in_flight: Option<Vec<WriteOperation>>,
    /// The step that failed, if any.
    failed_step: Option<usize>,
    /// Snapshots taken after each completed step, with the number of batches acked by then.
    snapshots: Vec<(Snapshot, usize)>,
}

struct Harness {
    fault: Arc<FaultVfs>,
    engine: Option<LocalStorageEngine>,
}

impl Harness {
    /// A fresh filesystem with a created collection whose background maintenance is disabled,
    /// so every mutating operation comes from the test and runs are deterministic.
    async fn new(seed: u64) -> Self {
        let fault = FaultVfs::new(seed);
        let engine = LocalStorageEngine::with_vfs(fault.process(), ROOT, None)
            .expect("engine should open on a fresh filesystem");
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
            .create_collection_from_descriptor(
                descriptor,
                Some(&CollectionAssignment {
                    assigned_node: ANONYMOUS_LOCAL_NODE_NAME.to_owned(),
                    assigned_role: NodeRole::Data,
                }),
            )
            .expect("collection should be created");
        Self {
            fault,
            engine: Some(engine),
        }
    }

    fn engine(&self) -> &LocalStorageEngine {
        self.engine
            .as_ref()
            .expect("engine should be open between crashes")
    }

    async fn run(&self, steps: &[Step]) -> Outcome {
        let engine = self.engine();
        let mut outcome = Outcome::default();
        for (index, step) in steps.iter().enumerate() {
            let result = match step {
                Step::Write(ops) => match engine.write(COLLECTION, ops.clone()).await {
                    Ok(_) => {
                        outcome.acked.push(ops.clone());
                        Ok(())
                    }
                    Err(error) => {
                        outcome.in_flight = Some(ops.clone());
                        Err(error)
                    }
                },
                Step::Flush => engine.flush(COLLECTION).await.map(|_| ()),
                Step::Compact => engine.compact(COLLECTION).await.map(|_| ()),
            };
            if result.is_err() {
                outcome.failed_step = Some(index);
                break;
            }
            // Snapshot reads do no I/O that mutates, so they do not shift crash op counts.
            match engine.snapshot(COLLECTION).await {
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
        self.engine = Some(
            LocalStorageEngine::with_vfs(self.fault.process(), ROOT, None)
                .expect("engine should reopen after a crash"),
        );
    }
}

/// The records visible after applying `batches` in order, numbering operations from 1.
fn expected_visible(batches: &[Vec<WriteOperation>]) -> Vec<VisibleRecord> {
    let mut latest = BTreeMap::<RecordId, Option<VisibleRecord>>::new();
    for (op, seq_no) in batches.iter().flatten().zip(1..) {
        let visible = match op {
            WriteOperation::Put(put) => Some(VisibleRecord {
                id: put.id.clone(),
                vector: put.vector.clone(),
                metadata: put.metadata.clone(),
                seq_no,
            }),
            WriteOperation::Delete(_) => None,
        };
        latest.insert(op.id().clone(), visible);
    }
    latest.into_values().flatten().collect()
}

fn op_count(batches: &[Vec<WriteOperation>]) -> SeqNo {
    batches.iter().map(|batch| batch.len() as SeqNo).sum()
}

/// Check the recovery contract after a crash and return the batches recovery kept.
async fn assert_recovered(
    harness: &Harness,
    outcome: &Outcome,
    context: &str,
) -> Vec<Vec<WriteOperation>> {
    let engine = harness.engine();
    let recovered = engine
        .scan_exact(COLLECTION, None)
        .await
        .unwrap_or_else(|error| panic!("{context}: recovery failed: {error}"));
    let snapshot = engine
        .snapshot(COLLECTION)
        .await
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
    let stats = engine
        .stats(COLLECTION)
        .await
        .unwrap_or_else(|error| panic!("{context}: stats failed: {error}"));
    assert_eq!(stats.live_record_count, recovered.len(), "{context}");

    // Every snapshot handed out before the crash still reads exactly the state it named.
    for (snapshot, acked) in &outcome.snapshots {
        let read = engine
            .scan_exact(COLLECTION, Some(snapshot.clone()))
            .await
            .unwrap_or_else(|error| {
                panic!("{context}: snapshot {snapshot:?} is unreadable after recovery: {error}")
            });
        assert_eq!(
            read,
            expected_visible(&outcome.acked[..*acked]),
            "{context}: snapshot {snapshot:?} changed across the crash"
        );
    }
    kept
}

/// After recovery the engine must keep working: a write, a flush and a compaction succeed and
/// survive a further crash.
async fn assert_engine_keeps_working(
    harness: &mut Harness,
    mut kept: Vec<Vec<WriteOperation>>,
    context: &str,
) {
    let follow_up = vec![put("z", 9.0), delete("b")];
    let engine = harness.engine();
    engine
        .write(COLLECTION, follow_up.clone())
        .await
        .unwrap_or_else(|error| panic!("{context}: write after recovery failed: {error}"));
    kept.push(follow_up);
    engine
        .flush(COLLECTION)
        .await
        .unwrap_or_else(|error| panic!("{context}: flush after recovery failed: {error}"));
    engine
        .compact(COLLECTION)
        .await
        .unwrap_or_else(|error| panic!("{context}: compact after recovery failed: {error}"));

    harness.crash_and_reopen();
    let recovered = harness
        .engine()
        .scan_exact(COLLECTION, None)
        .await
        .unwrap_or_else(|error| panic!("{context}: second recovery failed: {error}"));
    assert_eq!(
        recovered,
        expected_visible(&kept),
        "{context}: state after recovery, more work and a second crash"
    );
}

async fn ops_after_setup(seed: u64, steps: &[Step]) -> (u64, u64) {
    let harness = Harness::new(seed).await;
    let setup_ops = harness.fault.mutating_ops();
    let outcome = harness.run(steps).await;
    assert!(
        outcome.failed_step.is_none(),
        "clean run failed: {outcome:?}"
    );
    (setup_ops, harness.fault.mutating_ops() - setup_ops)
}

/// Crash before every mutating operation of the scenario, under every tear mode.
#[tokio::test]
async fn every_crash_point_of_writes_flush_rotation_and_compaction_recovers_acked_batches() {
    let steps = scenario();
    let (setup_ops, scenario_ops) = ops_after_setup(0, &steps).await;
    assert!(
        scenario_ops > 50,
        "scenario should exercise many operations"
    );

    for tear in TearMode::ALL {
        for k in 0..=scenario_ops {
            let seed = k * 4 + tear as u64;
            let mut harness = Harness::new(seed).await;
            harness.fault.set_plan(FaultPlan {
                crash_after_ops: Some(setup_ops + k),
                tear,
                ..FaultPlan::default()
            });
            let outcome = harness.run(&steps).await;
            let context = format!(
                "tear={tear:?} crash_after_ops={k} failed_step={:?}",
                outcome.failed_step
            );
            harness.crash_and_reopen();
            let kept = assert_recovered(&harness, &outcome, &context).await;
            assert_engine_keeps_working(&mut harness, kept, &context).await;
        }
    }
}

/// Crash during the scenario, then crash again at every operation of the recovery that follows,
/// then recover cleanly (I11: a crash during recovery is recovered like any other).
#[tokio::test]
async fn a_crash_during_recovery_is_recovered() {
    let steps = scenario();
    let (setup_ops, scenario_ops) = ops_after_setup(1, &steps).await;

    // Crash points inside the flushes, where recovery has the most to do.
    for tear in TearMode::ALL {
        for k in (0..=scenario_ops).step_by(3) {
            let seed = k * 4 + tear as u64;
            let plan = FaultPlan {
                crash_after_ops: Some(setup_ops + k),
                tear,
                ..FaultPlan::default()
            };
            let mut harness = Harness::new(seed).await;
            harness.fault.set_plan(plan.clone());
            let outcome = harness.run(&steps).await;
            harness.crash_and_reopen();

            // Count what recovery does on this state, on a throwaway copy of the same run.
            let recovery_ops = {
                let mut probe = Harness::new(seed).await;
                probe.fault.set_plan(plan);
                probe.run(&steps).await;
                probe.crash_and_reopen();
                let _ = probe.engine().stats(COLLECTION).await;
                probe.fault.mutating_ops()
            };

            for recovery_crash in 0..recovery_ops {
                harness.fault.set_plan(FaultPlan {
                    crash_after_ops: Some(recovery_crash),
                    tear,
                    ..FaultPlan::default()
                });
                let _ = harness.engine().stats(COLLECTION).await;
                harness.crash_and_reopen();
            }
            let context = format!("tear={tear:?} crash_after_ops={k} then recovery crashes");
            let kept = assert_recovered(&harness, &outcome, &context).await;
            assert_engine_keeps_working(&mut harness, kept, &context).await;
        }
    }
}

/// Crash before every mutating operation of creating a collection: afterwards the collection
/// either does not exist and can be created again, or exists, is empty, and works.
#[tokio::test]
async fn a_crash_while_creating_a_collection_leaves_it_absent_or_usable() {
    let create_ops = {
        let fault = FaultVfs::new(0);
        let engine = LocalStorageEngine::with_vfs(fault.process(), ROOT, None)
            .expect("engine should open on a fresh filesystem");
        let before = fault.mutating_ops();
        engine
            .create_collection(CreateCollectionRequest::new(
                COLLECTION,
                2,
                DistanceMetric::Dot,
            ))
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
            let engine = LocalStorageEngine::with_vfs(fault.process(), ROOT, None)
                .expect("engine should open on a fresh filesystem");
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let created = engine
                .create_collection(CreateCollectionRequest::new(
                    COLLECTION,
                    2,
                    DistanceMetric::Dot,
                ))
                .await
                .is_ok();
            drop(engine);
            fault.crash();

            let engine = LocalStorageEngine::with_vfs(fault.process(), ROOT, None)
                .unwrap_or_else(|error| panic!("{context}: reopen failed: {error}"));
            let listed = engine
                .list_collections()
                .await
                .unwrap_or_else(|error| panic!("{context}: listing failed: {error}"));
            if created {
                assert_eq!(
                    listed.len(),
                    1,
                    "{context}: an acknowledged create must survive"
                );
            }
            if listed.is_empty() {
                engine
                    .create_collection(CreateCollectionRequest::new(
                        COLLECTION,
                        2,
                        DistanceMetric::Dot,
                    ))
                    .await
                    .unwrap_or_else(|error| panic!("{context}: create again failed: {error}"));
            } else {
                assert_eq!(listed.len(), 1, "{context}");
                let visible = engine
                    .scan_exact(COLLECTION, None)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: scan failed: {error}"));
                assert!(visible.is_empty(), "{context}: a new collection is empty");
            }
            engine
                .write(COLLECTION, vec![put("a", 1.0)])
                .await
                .unwrap_or_else(|error| panic!("{context}: write failed: {error}"));
            engine
                .flush(COLLECTION)
                .await
                .unwrap_or_else(|error| panic!("{context}: flush failed: {error}"));
            drop(engine);
            fault.crash();
            let engine = LocalStorageEngine::with_vfs(fault.process(), ROOT, None)
                .unwrap_or_else(|error| panic!("{context}: second reopen failed: {error}"));
            assert_eq!(
                engine
                    .scan_exact(COLLECTION, None)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: second scan failed: {error}")),
                expected_visible(&[vec![put("a", 1.0)]]),
                "{context}"
            );
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
    harness.engine = Some(
        LocalStorageEngine::with_vfs(harness.fault.process(), ROOT, None)
            .expect("engine should reopen"),
    );
    (harness, outcome)
}

async fn manifest_generation(harness: &Harness) -> u64 {
    harness
        .engine()
        .stats(COLLECTION)
        .await
        .expect("stats should succeed")
        .manifest_generation
}

/// One test per crash point the legacy engine implements, asserting the outcome from the crash
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

    // Every step of the first flush before the durable `CURRENT` rename leaves generation 0.
    for (seed, point) in [
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
        assert_eq!(manifest_generation(&harness).await, 0, "{context}");
        let kept = assert_recovered(&harness, &outcome, &context).await;
        assert_engine_keeps_working(&mut harness, kept, &context).await;
    }

    // Once the directory holding `CURRENT` is synced, the flush is published; recovery finishes
    // the WAL rotation.
    for (seed, point) in [
        CrashPoint::CurrentAfterDirSync,
        CrashPoint::WalAfterRotateCreate,
    ]
    .into_iter()
    .enumerate()
    {
        let (mut harness, outcome) = run_to_crash_point(point, 30 + seed as u64).await;
        let context = format!("{point:?}");
        assert_eq!(outcome.failed_step, Some(first_flush), "{context}");
        assert_eq!(manifest_generation(&harness).await, 1, "{context}");
        let stats = harness.engine().stats(COLLECTION).await.expect("stats");
        assert_eq!(
            stats.mutable_op_count, 0,
            "{context}: the flush checkpointed everything"
        );
        let kept = assert_recovered(&harness, &outcome, &context).await;
        assert_engine_keeps_working(&mut harness, kept, &context).await;
    }

    // A compaction whose output is synced but whose manifest is not published changes nothing.
    let (mut harness, outcome) =
        run_to_crash_point(CrashPoint::CompactionAfterOutputSync, 40).await;
    assert_eq!(outcome.failed_step, Some(compact));
    assert_eq!(manifest_generation(&harness).await, 2);
    let stats = harness.engine().stats(COLLECTION).await.expect("stats");
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
            let error = harness
                .engine()
                .write(COLLECTION, vec![put("lost", 8.0)])
                .await
                .expect_err("the fsync failure should fail the write");
            assert!(error.to_string().contains("fsync"), "{context}: {error}");
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

/// A failed WAL fsync followed by more work in the same process, then a crash: the failed batch
/// is invisible before and after the crash, and every batch acknowledged after the failure
/// survives. Under every tear mode the failed sync may have written part or all of its frame
/// before the error, so only a durable rollback keeps the batch from coming back.
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
            harness
                .engine()
                .write(COLLECTION, vec![put("lost", 8.0)])
                .await
                .expect_err("the fsync failure should fail the write");
            let visible = harness
                .engine()
                .scan_exact(COLLECTION, None)
                .await
                .unwrap_or_else(|error| panic!("{context}: scan after the failure: {error}"));
            assert_eq!(
                visible,
                expected_visible(&outcome.acked),
                "{context}: the failed batch must stay invisible in the same process"
            );

            let after = vec![put("after", 3.0), delete("b")];
            harness
                .engine()
                .write(COLLECTION, after.clone())
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

/// A torn WAL tail is repaired on the next write; a crash right after the repair keeps it.
#[tokio::test]
async fn crash_after_wal_tail_repair_keeps_the_repair() {
    let mut harness = Harness::new(60).await;
    let outcome = harness.run(&scenario()[..2]).await;
    assert_eq!(outcome.acked.len(), 2);

    let active = active_wal_path(&harness);
    let file = harness
        .fault
        .open(&active, OpenMode::Append)
        .expect("active WAL should open");
    file.append(&[IoSlice::new(b"torn frame")])
        .expect("garbage append");
    file.sync_data().expect("garbage sync");

    harness.fault.set_plan(FaultPlan {
        crash_at: Some(CrashPoint::RecoveryAfterTailRepair),
        ..FaultPlan::default()
    });
    harness
        .engine()
        .write(COLLECTION, vec![put("f", 1.5)])
        .await
        .expect_err("the write should crash after repairing the tail");
    let outcome = Outcome {
        acked: outcome.acked,
        in_flight: Some(vec![put("f", 1.5)]),
        failed_step: Some(2),
        snapshots: outcome.snapshots,
    };
    harness.crash_and_reopen();
    let kept = assert_recovered(&harness, &outcome, "RecoveryAfterTailRepair").await;
    assert_eq!(kept.len(), 2, "the crashed write never reached the WAL");
    assert_engine_keeps_working(&mut harness, kept, "RecoveryAfterTailRepair").await;
}

/// Every crash point the legacy engine implements is reached by a clean run, so the named tests
/// above cannot silently stop testing anything. The points for deletion vectors, GC and orphan
/// cleanup belong to engine code that does not exist yet.
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
    collections[0].join("wal").join("active.wal")
}
