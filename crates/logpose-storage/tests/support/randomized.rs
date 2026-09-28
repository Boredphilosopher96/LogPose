use crate::scan::ScanExt;
use logpose_query::{ExplainMode, QueryMatch, QueryPlanKind, QueryRequest, QueryResponse, query};
use logpose_storage::{
    CreateCollectionRequest, InspectTarget, LocalStorageEngine, SnapshotToken, StorageEngine,
};
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, CollectionAssignment, CollectionId, CollectionStats, CommitAck,
    DEFAULT_DATABASE_NAME, DeleteRecord, DistanceMetric, LogPoseError, NodeRole, PutRecord,
    RecordId, SeqNo, Snapshot, VisibleRecord, WriteOperation,
};
use logpose_vfs::{FaultPlan, FaultVfs, TearMode, Vfs};
use rand::{RngExt, SeedableRng, rng, rngs::StdRng};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

#[path = "fs.rs"]
mod fs_support;

const COLLECTION_NAME: &str = "randomized";
const DEFAULT_SCENARIO_STEPS: usize = 40;
/// Storage root on the in-memory `FaultVfs` backend.
const FAULT_ROOT: &str = "/storage";
/// Chance, in percent, that a step on the `FaultVfs` backend is a crash.
const CRASH_PERCENT: u32 = 10;
/// Upper bound for the crash position inside a flush or compaction, in mutating operations.
/// A flush performs a few dozen; larger values let the operation finish before power is lost.
const MAX_MAINTENANCE_CRASH_OPS: u64 = 48;
/// Upper bound for the crash position inside a write, in mutating operations.
const MAX_WRITE_CRASH_OPS: u64 = 3;
/// Upper bound for the crash position inside the recovery that follows a crash.
const MAX_RECOVERY_CRASH_OPS: u64 = 10;

/// Where a scenario's storage lives.
enum Backend {
    /// The real filesystem through `StdVfs`.
    Std { root: PathBuf },
    /// An in-memory `FaultVfs` that crash actions power-cycle. `process` is the handle of the
    /// current simulated process; it dies with each crash.
    Fault {
        fault: Arc<FaultVfs>,
        process: Arc<dyn Vfs>,
    },
}

impl Backend {
    fn open_engine(&self) -> LocalStorageEngine {
        self.try_open_engine().expect("storage engine should open")
    }

    fn try_open_engine(&self) -> logpose_types::Result<LocalStorageEngine> {
        match self {
            Self::Std { root } => LocalStorageEngine::new(root),
            Self::Fault { process, .. } => {
                LocalStorageEngine::with_vfs(Arc::clone(process), FAULT_ROOT, None)
            }
        }
    }

    fn allows_crashes(&self) -> bool {
        matches!(self, Self::Fault { .. })
    }
}

/// Which backend a seeded scenario runs on.
#[derive(Clone, Copy, Debug)]
pub enum BackendKind {
    /// The real filesystem; no crash actions.
    Std,
    /// `FaultVfs`, with crash-and-reopen actions.
    Fault,
}
const DEFAULT_RANDOM_SCENARIOS: usize = 5;
const RECORD_DIMENSIONS: usize = 2;
const RECORD_ID_POOL: usize = 8;
const EXACT_QUERY_TOP_K: usize = 3;
const EXACT_QUERY_VECTORS: [[f32; RECORD_DIMENSIONS]; 3] = [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]];

#[derive(Clone, Debug)]
pub enum StorageAction {
    CreateCollection,
    PutBatch(Vec<TestRecord>),
    Delete {
        id: String,
    },
    Snapshot,
    ScanCurrent,
    ScanSnapshot {
        snapshot_index: usize,
    },
    Flush,
    Compact,
    Stats,
    InspectManifest,
    InspectWal,
    InspectSegment,
    Reopen,
    /// Power-cycle the `FaultVfs` while `during` runs: crash before its `crash_after_ops`-th
    /// mutating operation (or right after it finishes, if it performs fewer), tear unsynced data
    /// per `tear`, optionally crash again inside the recovery that follows, then reopen.
    Crash {
        during: CrashDuring,
        crash_after_ops: u64,
        tear: TearMode,
        recovery_crash_after_ops: Option<u64>,
    },
}

/// The operation a crash interrupts.
#[derive(Clone, Debug)]
pub enum CrashDuring {
    PutBatch(Vec<TestRecord>),
    Delete { id: String },
    Flush,
    Compact,
}

#[derive(Clone, Debug)]
pub struct TestRecord {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: Value,
}

#[derive(Debug)]
enum ExpectedState {
    Visible(VisibleRecord),
    Deleted,
}

#[derive(Clone, Copy, Debug)]
struct ExpectedGenerationState {
    checkpoint_seq_no: SeqNo,
    segment_count: usize,
}

/// One row the engine stores: a put, in a memtable until a flush moves it into a segment, and
/// gone once a flush (for a memtable row) or a compaction (for a segment row) that ran after it
/// was superseded dropped it.
#[derive(Clone, Debug)]
struct PhysicalRow {
    id: RecordId,
    seq_no: SeqNo,
    in_segment: bool,
    /// The manifest generation whose flush or compaction dropped the row.
    dropped_at: Option<u64>,
}

#[derive(Debug)]
struct ExpectedModel {
    collection_id: Option<CollectionId>,
    metric: Option<DistanceMetric>,
    manifest_generation: u64,
    checkpoint_seq_no: SeqNo,
    next_seq_no: SeqNo,
    segment_count: usize,
    generation_states: BTreeMap<u64, ExpectedGenerationState>,
    history: Vec<(SeqNo, WriteOperation)>,
    rows: Vec<PhysicalRow>,
}

impl ExpectedModel {
    fn new() -> Self {
        let mut generation_states = BTreeMap::new();
        generation_states.insert(
            0,
            ExpectedGenerationState {
                checkpoint_seq_no: 0,
                segment_count: 0,
            },
        );
        Self {
            collection_id: None,
            metric: None,
            manifest_generation: 0,
            checkpoint_seq_no: 0,
            next_seq_no: 0,
            segment_count: 0,
            generation_states,
            history: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Whether an operation on `row`'s key after the row, at or below `visible_seq_no`,
    /// superseded it.
    fn superseded(&self, row: &PhysicalRow, visible_seq_no: SeqNo) -> bool {
        self.history.iter().any(|(seq_no, operation)| {
            *seq_no > row.seq_no && *seq_no <= visible_seq_no && operation.id() == &row.id
        })
    }

    /// Puts above the checkpoint: the memtable's slots.
    fn memtable_put_count(&self) -> usize {
        self.history
            .iter()
            .filter(|(seq_no, operation)| {
                *seq_no > self.checkpoint_seq_no && matches!(operation, WriteOperation::Put(_))
            })
            .count()
    }

    fn register_collection(&mut self, collection_id: CollectionId, metric: DistanceMetric) {
        self.collection_id = Some(collection_id);
        self.metric = Some(metric);
    }

    fn record_write(&mut self, operations: &[WriteOperation]) {
        for operation in operations {
            self.next_seq_no += 1;
            let mut operation = operation.clone();
            // The engine stores cosine vectors normalized to unit length (the WAL logs the row
            // image the reader will see), so the model does too.
            if self.metric == Some(DistanceMetric::Cosine)
                && let WriteOperation::Put(put) = &mut operation
            {
                normalize(&mut put.vector);
            }
            if let WriteOperation::Put(put) = &operation {
                self.rows.push(PhysicalRow {
                    id: put.id.clone(),
                    seq_no: self.next_seq_no,
                    in_segment: false,
                    dropped_at: None,
                });
            }
            self.history.push((self.next_seq_no, operation));
        }
    }

    /// A flush that the engine published as `generation`. Generations only grow, but can skip
    /// numbers a failed or interrupted publish burned.
    fn record_flush(&mut self, generation: u64) {
        if self.mutable_op_count() == 0 {
            assert_eq!(
                generation, self.manifest_generation,
                "an empty flush publishes nothing"
            );
            return;
        }
        assert!(
            generation > self.manifest_generation,
            "flush generation {generation} must follow {}",
            self.manifest_generation
        );
        self.manifest_generation = generation;
        self.checkpoint_seq_no = self.next_seq_no;
        // The memtable's dead slots are dropped; its live ones become a segment, if any.
        let now = self.next_seq_no;
        let mut live = false;
        for index in 0..self.rows.len() {
            let row = &self.rows[index];
            if row.in_segment || row.dropped_at.is_some() {
                continue;
            }
            if self.superseded(row, now) {
                self.rows[index].dropped_at = Some(generation);
            } else {
                self.rows[index].in_segment = true;
                live = true;
            }
        }
        if live {
            self.segment_count += 1;
        }
        self.generation_states.insert(
            self.manifest_generation,
            ExpectedGenerationState {
                checkpoint_seq_no: self.checkpoint_seq_no,
                segment_count: self.segment_count,
            },
        );
    }

    /// A compaction that the engine published as `generation`. It merges two or more segments,
    /// or rewrites a lone segment that has deleted rows.
    fn record_compact(&mut self, generation: u64) {
        let now = self.next_seq_no;
        let reclaims = self
            .rows
            .iter()
            .any(|row| row.in_segment && row.dropped_at.is_none() && self.superseded(row, now));
        if self.segment_count == 0 || (self.segment_count == 1 && !reclaims) {
            assert_eq!(generation, self.manifest_generation, "nothing to compact");
            return;
        }
        assert!(
            generation > self.manifest_generation,
            "compaction generation {generation} must follow {}",
            self.manifest_generation
        );
        self.manifest_generation = generation;
        // The segments' deleted rows are dropped; the live ones become one segment, if any.
        let mut live = false;
        for index in 0..self.rows.len() {
            let row = &self.rows[index];
            if !row.in_segment || row.dropped_at.is_some() {
                continue;
            }
            if self.superseded(row, now) {
                self.rows[index].dropped_at = Some(generation);
            } else {
                live = true;
            }
        }
        self.segment_count = usize::from(live);
        self.generation_states.insert(
            self.manifest_generation,
            ExpectedGenerationState {
                checkpoint_seq_no: self.checkpoint_seq_no,
                segment_count: self.segment_count,
            },
        );
    }

    fn current_snapshot(&self) -> Snapshot {
        Snapshot {
            manifest_generation: self.manifest_generation,
            visible_seq_no: self.next_seq_no,
        }
    }

    fn mutable_op_count(&self) -> usize {
        self.history
            .iter()
            .filter(|(seq_no, _)| *seq_no > self.checkpoint_seq_no)
            .count()
    }

    fn generation_state(&self, manifest_generation: u64) -> ExpectedGenerationState {
        *self
            .generation_states
            .get(&manifest_generation)
            .expect("snapshot generation should be tracked")
    }

    fn expected_stats(&self, snapshot: Snapshot) -> CollectionStats {
        let generation_state = self.generation_state(snapshot.manifest_generation);
        let resolved = self.resolve_latest(snapshot.visible_seq_no);
        let live_record_count = resolved
            .values()
            .filter(|state| matches!(state, ExpectedState::Visible(_)))
            .count();
        // Rows the engine still stores at that state (not yet dropped by a flush or
        // compaction of a later generation) that an operation up to it superseded.
        let deleted_record_count = self
            .rows
            .iter()
            .filter(|row| {
                row.seq_no <= snapshot.visible_seq_no
                    && row
                        .dropped_at
                        .is_none_or(|generation| generation > snapshot.manifest_generation)
                    && self.superseded(row, snapshot.visible_seq_no)
            })
            .count();
        let mutable_op_count = self
            .history
            .iter()
            .filter(|(seq_no, _)| {
                *seq_no > generation_state.checkpoint_seq_no && *seq_no <= snapshot.visible_seq_no
            })
            .count();

        CollectionStats {
            collection_id: self
                .collection_id
                .clone()
                .expect("collection id should be registered"),
            database_name: DEFAULT_DATABASE_NAME.to_owned(),
            collection_name: COLLECTION_NAME.to_owned(),
            manifest_generation: snapshot.manifest_generation,
            visible_seq_no: snapshot.visible_seq_no,
            mutable_op_count,
            segment_count: generation_state.segment_count,
            live_record_count,
            deleted_record_count,
            maintenance: Default::default(),
            query_units: Vec::new(),
        }
    }

    fn expected_visible(&self, visible_seq_no: SeqNo) -> Vec<VisibleRecord> {
        self.resolve_latest(visible_seq_no)
            .into_values()
            .filter_map(|state| match state {
                ExpectedState::Visible(record) => Some(record),
                ExpectedState::Deleted => None,
            })
            .collect()
    }

    fn resolve_latest(&self, visible_seq_no: SeqNo) -> BTreeMap<RecordId, ExpectedState> {
        let mut resolved = BTreeMap::new();

        for (seq_no, operation) in self
            .history
            .iter()
            .rev()
            .filter(|(seq_no, _)| *seq_no <= visible_seq_no)
        {
            let id = operation.id().clone();
            if resolved.contains_key(&id) {
                continue;
            }

            let state = match operation {
                WriteOperation::Put(put) => ExpectedState::Visible(VisibleRecord {
                    id: put.id.clone(),
                    vector: put.vector.clone(),
                    metadata: put.metadata.clone(),
                    seq_no: *seq_no,
                }),
                WriteOperation::Delete(_) => ExpectedState::Deleted,
            };
            resolved.insert(id, state);
        }

        resolved
    }
}

/// Scale a vector to unit length the way the engine does: the norm in `f64`, each component
/// divided in `f64` and rounded to `f32`.
fn normalize(vector: &mut [f32]) {
    let norm = vector
        .iter()
        .map(|component| f64::from(*component) * f64::from(*component))
        .sum::<f64>()
        .sqrt();
    if norm == 0.0 || !norm.is_finite() {
        return;
    }
    for component in vector.iter_mut() {
        *component = (f64::from(*component) / norm) as f32;
    }
}

pub async fn run_storage_scenarios(kind: BackendKind) {
    let seeds = scenario_seeds();
    for seed in seeds {
        run_seeded_storage_scenario(seed, DEFAULT_SCENARIO_STEPS, kind).await;
    }
}

pub fn current_exact_query_request_for_test(vector: Vec<f32>) -> QueryRequest {
    current_exact_query_request(vector)
}

async fn run_seeded_storage_scenario(seed: u64, steps: usize, kind: BackendKind) {
    let mut backend = match kind {
        BackendKind::Std => Backend::Std {
            root: fs_support::unique_temp_dir(&format!("storage-random-{seed}")),
        },
        BackendKind::Fault => {
            let fault = FaultVfs::new(seed);
            let process = fault.process();
            Backend::Fault { fault, process }
        }
    };
    let mut engine = backend.open_engine();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut model = ExpectedModel::new();
    let mut trace = Vec::new();
    let mut snapshots: Vec<PinnedSnapshot> = Vec::new();

    trace.push(StorageAction::CreateCollection);
    let descriptor =
        create_collection_without_background_maintenance(&engine).unwrap_or_else(|error| {
            panic_with_context(seed, &trace, format!("create failed: {error}"))
        });
    model.register_collection(descriptor.collection_id.clone(), descriptor.metric);
    assert_stats_match(&engine, &model, None, seed, &trace).await;
    assert_current_scan_matches(&engine, &model, seed, &trace).await;
    assert_current_exact_queries_match(&engine, &model, seed, &trace).await;

    for _ in 0..steps {
        let action = if backend.allows_crashes() && rng.random_range(0..100) < CRASH_PERCENT {
            next_crash_action(&mut rng, snapshots.len(), model.segment_count)
        } else {
            next_action(&mut rng, snapshots.len(), model.segment_count)
        };
        trace.push(action.clone());

        match action {
            StorageAction::CreateCollection => {
                panic_with_context(seed, &trace, "duplicate create action".to_owned());
            }
            StorageAction::PutBatch(records) => {
                let operations = records.iter().map(put_operation).collect::<Vec<_>>();
                let ack = engine
                    .write(COLLECTION_NAME, operations.clone())
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("write failed: {error}"))
                    });
                model.record_write(&operations);
                assert_ack_matches(&ack, operations.len(), &model, seed, &trace);
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, None, seed, &trace).await;
            }
            StorageAction::Delete { id } => {
                let operations = vec![WriteOperation::Delete(DeleteRecord {
                    id: RecordId::new(id),
                })];
                let ack = engine
                    .write(COLLECTION_NAME, operations.clone())
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("delete failed: {error}"))
                    });
                model.record_write(&operations);
                assert_ack_matches(&ack, operations.len(), &model, seed, &trace);
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, None, seed, &trace).await;
            }
            StorageAction::Snapshot => {
                let snapshot = engine
                    .snapshot(COLLECTION_NAME)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("snapshot failed: {error}"))
                    });
                let expected = model.current_snapshot();
                assert_eq_with_context(seed, &trace, "snapshot mismatch", &expected, &snapshot);
                // Pin every snapshot while at most `MAX_PINNED` are pinned; the oldest pin is
                // released to make room, and its snapshot then lives only as long as its
                // generation is current.
                let (token, pinned) =
                    engine
                        .pin_snapshot(COLLECTION_NAME)
                        .unwrap_or_else(|error| {
                            panic_with_context(seed, &trace, format!("pin failed: {error}"))
                        });
                assert_eq_with_context(seed, &trace, "pinned snapshot", &snapshot, &pinned);
                let pinned_count = snapshots
                    .iter()
                    .filter(|entry| entry.token.is_some())
                    .count();
                if pinned_count >= MAX_PINNED
                    && let Some(oldest) = snapshots.iter_mut().find(|entry| entry.token.is_some())
                    && let Some(token) = oldest.token.take()
                {
                    let released = engine
                        .release_snapshot(COLLECTION_NAME, &token)
                        .unwrap_or_else(|error| {
                            panic_with_context(seed, &trace, format!("release failed: {error}"))
                        });
                    assert!(released, "seed={seed}: the oldest pin was still pinned");
                }
                snapshots.push(PinnedSnapshot {
                    snapshot: snapshot.clone(),
                    token: Some(token),
                });
                assert_scan_matches_snapshot(&engine, &model, &snapshot, seed, &trace).await;
                assert_exact_queries_match_snapshot(&engine, &model, &snapshot, seed, &trace).await;
                assert_stats_match(&engine, &model, Some(snapshot), seed, &trace).await;
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
            }
            StorageAction::ScanCurrent => {
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
            }
            StorageAction::ScanSnapshot { snapshot_index } => {
                if snapshot_index >= snapshots.len() {
                    panic_with_context(
                        seed,
                        &trace,
                        format!("missing snapshot index {snapshot_index}"),
                    );
                }
                assert_snapshot_reads(&engine, &model, &snapshots, snapshot_index, seed, &trace)
                    .await;
            }
            StorageAction::Flush => {
                let snapshot = engine.flush(COLLECTION_NAME).await.unwrap_or_else(|error| {
                    panic_with_context(seed, &trace, format!("flush failed: {error}"))
                });
                model.record_flush(snapshot.manifest_generation);
                let expected = model.current_snapshot();
                assert_eq_with_context(
                    seed,
                    &trace,
                    "flush snapshot mismatch",
                    &expected,
                    &snapshot,
                );
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, None, seed, &trace).await;
            }
            StorageAction::Compact => {
                let before = engine
                    .scan_exact(COLLECTION_NAME, None)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(
                            seed,
                            &trace,
                            format!("pre-compact scan failed: {error}"),
                        )
                    });
                let snapshot = engine
                    .compact(COLLECTION_NAME)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("compact failed: {error}"))
                    });
                model.record_compact(snapshot.manifest_generation);
                let expected = model.current_snapshot();
                assert_eq_with_context(
                    seed,
                    &trace,
                    "compact snapshot mismatch",
                    &expected,
                    &snapshot,
                );
                let after = engine
                    .scan_exact(COLLECTION_NAME, None)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(
                            seed,
                            &trace,
                            format!("post-compact scan failed: {error}"),
                        )
                    });
                assert_eq_with_context(
                    seed,
                    &trace,
                    "compaction changed visible state",
                    &before,
                    &after,
                );
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, None, seed, &trace).await;
            }
            StorageAction::Stats => {
                assert_stats_match(&engine, &model, None, seed, &trace).await;
            }
            StorageAction::InspectManifest => {
                assert_manifest_inspect_matches(&engine, &model, seed, &trace).await;
            }
            StorageAction::InspectWal => {
                assert_wal_inspect_matches(&engine, &model, seed, &trace).await;
            }
            StorageAction::InspectSegment => {
                assert_segment_inspect_matches(&engine, &model, seed, &trace).await;
            }
            StorageAction::Reopen => {
                // One engine owns a root at a time: the old one releases it before the reopen.
                drop(engine);
                engine = backend.open_engine();
                // Pins live in memory: a restart ends them.
                for entry in &mut snapshots {
                    entry.token = None;
                }
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, None, seed, &trace).await;
            }
            StorageAction::Crash {
                during,
                crash_after_ops,
                tear,
                recovery_crash_after_ops,
            } => {
                let crash = PlannedCrash {
                    during,
                    crash_after_ops,
                    tear,
                    recovery_crash_after_ops,
                };
                engine =
                    crash_and_recover(&mut backend, engine, &mut model, crash, seed, &trace).await;
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, None, seed, &trace).await;
                // Every snapshot handed out before the crash reads what it named while its
                // generation is current, and is expired otherwise: pins end with the process.
                for entry in &mut snapshots {
                    entry.token = None;
                }
                for index in 0..snapshots.len() {
                    assert_snapshot_reads(&engine, &model, &snapshots, index, seed, &trace).await;
                }
            }
        }
    }
}

/// A snapshot the harness handed out, and the token that pins it, if one still does.
#[derive(Clone, Debug)]
struct PinnedSnapshot {
    snapshot: Snapshot,
    token: Option<SnapshotToken>,
}

/// Most snapshots the harness keeps pinned at once.
const MAX_PINNED: usize = 16;

/// A snapshot reads exactly the state it named while that state is retained: it is the current
/// one, or a live token pins exactly it. Otherwise the engine may still retain it among the
/// latest versions of the current generation (then the read must be exact) or report it
/// expired; it never reads a different state.
async fn assert_snapshot_reads(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    snapshots: &[PinnedSnapshot],
    index: usize,
    seed: u64,
    trace: &[StorageAction],
) {
    let entry = &snapshots[index];
    let snapshot = &entry.snapshot;
    let pinned = snapshots
        .iter()
        .any(|other| other.token.is_some() && other.snapshot == *snapshot);
    if let Some(token) = &entry.token {
        let actual = engine
            .scan_exact_at_token(COLLECTION_NAME, token.clone())
            .await
            .unwrap_or_else(|error| {
                panic_with_context(seed, trace, format!("token scan failed: {error}"))
            });
        let expected = model.expected_visible(snapshot.visible_seq_no);
        assert_eq_with_context(seed, trace, "token scan mismatch", &expected, &actual);
    }
    if pinned || *snapshot == model.current_snapshot() {
        assert_scan_matches_snapshot(engine, model, snapshot, seed, trace).await;
        assert_exact_queries_match_snapshot(engine, model, snapshot, seed, trace).await;
        assert_stats_match(engine, model, Some(snapshot.clone()), seed, trace).await;
        return;
    }
    let scan = engine
        .scan_exact(COLLECTION_NAME, Some(snapshot.clone()))
        .await;
    let stats = engine
        .stats_snapshot(COLLECTION_NAME, Some(snapshot.clone()))
        .await;
    match scan {
        Ok(actual) => {
            let recent = snapshot.manifest_generation == model.manifest_generation;
            assert!(
                recent,
                "seed={seed}: only versions of the current generation are retained unpinned"
            );
            let expected = model.expected_visible(snapshot.visible_seq_no);
            assert_eq_with_context(seed, trace, "recent snapshot scan", &expected, &actual);
        }
        Err(LogPoseError::SnapshotExpired { .. }) => {}
        Err(other) => panic_with_context(
            seed,
            trace,
            format!(
                "scan of unpinned snapshot {snapshot:?} (current generation {}) must read \
                 exactly or expire, got {other:?}",
                model.manifest_generation
            ),
        ),
    }
    match stats {
        Ok(actual) => {
            let expected = model.expected_stats(snapshot.clone());
            assert_eq_with_context(
                seed,
                trace,
                "recent snapshot stats",
                &(expected.visible_seq_no, expected.live_record_count),
                &(actual.visible_seq_no, actual.live_record_count),
            );
        }
        Err(LogPoseError::SnapshotExpired { .. }) => {}
        Err(other) => panic_with_context(
            seed,
            trace,
            format!("stats of unpinned snapshot {snapshot:?} must be exact or expire: {other:?}"),
        ),
    }
}

struct PlannedCrash {
    during: CrashDuring,
    crash_after_ops: u64,
    tear: TearMode,
    recovery_crash_after_ops: Option<u64>,
}

/// What a crashed operation got done before power was lost.
enum Interrupted {
    /// A write batch that was not acknowledged: after recovery it must be entirely present or
    /// entirely absent.
    Write(Vec<WriteOperation>),
    /// A flush or compaction that did not report success: after recovery it is either
    /// published (one more manifest generation) or not at all.
    Maintenance(MaintenanceKind),
    /// The operation finished; nothing is uncertain.
    Nothing,
}

#[derive(Clone, Copy)]
enum MaintenanceKind {
    Flush,
    Compact,
}

/// Run `crash.during` with a crash planned inside it, power-cycle the `FaultVfs`, optionally
/// crash again inside recovery, reopen, and fold the outcome of the interrupted operation into
/// the model. Acknowledged operations were already folded in when they returned.
async fn crash_and_recover(
    backend: &mut Backend,
    engine: LocalStorageEngine,
    model: &mut ExpectedModel,
    crash: PlannedCrash,
    seed: u64,
    trace: &[StorageAction],
) -> LocalStorageEngine {
    let Backend::Fault { fault, process } = backend else {
        panic_with_context(seed, trace, "crash action on a real filesystem".to_owned());
    };
    fault.set_plan(FaultPlan {
        crash_after_ops: Some(fault.mutating_ops() + crash.crash_after_ops),
        tear: crash.tear,
        ..FaultPlan::default()
    });

    let interrupted = match crash.during {
        CrashDuring::PutBatch(records) => {
            let operations = records.iter().map(put_operation).collect::<Vec<_>>();
            interrupted_write(&engine, model, operations, seed, trace).await
        }
        CrashDuring::Delete { id } => {
            let operations = vec![WriteOperation::Delete(DeleteRecord {
                id: RecordId::new(id),
            })];
            interrupted_write(&engine, model, operations, seed, trace).await
        }
        CrashDuring::Flush => match engine.flush(COLLECTION_NAME).await {
            Ok(snapshot) => {
                model.record_flush(snapshot.manifest_generation);
                Interrupted::Nothing
            }
            Err(_) => Interrupted::Maintenance(MaintenanceKind::Flush),
        },
        CrashDuring::Compact => match engine.compact(COLLECTION_NAME).await {
            Ok(snapshot) => {
                model.record_compact(snapshot.manifest_generation);
                Interrupted::Nothing
            }
            Err(_) => Interrupted::Maintenance(MaintenanceKind::Compact),
        },
    };

    // The crashed process is gone: its handle and every file it opened stay dead.
    drop(engine);
    fault.crash();
    *process = fault.process();
    if let Some(recovery_crash_after_ops) = crash.recovery_crash_after_ops {
        fault.set_plan(FaultPlan {
            crash_after_ops: Some(recovery_crash_after_ops),
            tear: crash.tear,
            ..FaultPlan::default()
        });
        // Recovery runs inside `open`; it may crash part-way, failing the open or leaving the
        // collection registered as failed.
        drop(backend.try_open_engine());
        let Backend::Fault { fault, process } = backend else {
            panic_with_context(seed, trace, "backend changed during a crash".to_owned());
        };
        fault.crash();
        *process = fault.process();
    }
    let engine = backend.open_engine();

    match interrupted {
        Interrupted::Nothing => {}
        Interrupted::Write(operations) => {
            let recovered = engine
                .snapshot(COLLECTION_NAME)
                .await
                .unwrap_or_else(|error| {
                    panic_with_context(seed, trace, format!("recovery failed: {error}"))
                })
                .visible_seq_no;
            let absent = model.next_seq_no;
            let present = absent + operations.len() as SeqNo;
            if recovered == present {
                model.record_write(&operations);
            } else if recovered != absent {
                panic_with_context(
                    seed,
                    trace,
                    format!(
                        "recovered visible_seq_no {recovered}: the interrupted batch must be \
                         wholly absent ({absent}) or wholly present ({present}), and \
                         acknowledged batches must survive"
                    ),
                );
            }
        }
        Interrupted::Maintenance(kind) => {
            let generation = engine
                .stats(COLLECTION_NAME)
                .await
                .unwrap_or_else(|error| {
                    panic_with_context(seed, trace, format!("recovery failed: {error}"))
                })
                .manifest_generation;
            if generation > model.manifest_generation {
                match kind {
                    MaintenanceKind::Flush => model.record_flush(generation),
                    MaintenanceKind::Compact => model.record_compact(generation),
                }
            }
            if generation != model.manifest_generation {
                panic_with_context(
                    seed,
                    trace,
                    format!(
                        "recovered manifest generation {generation}, but the interrupted \
                         operation leaves {} (not published) or a newer one (published)",
                        model.manifest_generation
                    ),
                );
            }
        }
    }
    engine
}

/// Write `operations`; an acknowledged batch goes into the model now, a failed one is uncertain
/// until recovery.
async fn interrupted_write(
    engine: &LocalStorageEngine,
    model: &mut ExpectedModel,
    operations: Vec<WriteOperation>,
    seed: u64,
    trace: &[StorageAction],
) -> Interrupted {
    match engine.write(COLLECTION_NAME, operations.clone()).await {
        Ok(ack) => {
            model.record_write(&operations);
            assert_ack_matches(&ack, operations.len(), model, seed, trace);
            Interrupted::Nothing
        }
        Err(_) => Interrupted::Write(operations),
    }
}

fn put_operation(record: &TestRecord) -> WriteOperation {
    WriteOperation::Put(PutRecord {
        id: RecordId::new(record.id.clone()),
        vector: record.vector.clone(),
        metadata: record.metadata.clone(),
    })
}

/// Create the scenario's collection with flush and compaction thresholds that never trigger, so
/// background maintenance threads never race the scenario (and crash positions are
/// deterministic).
fn create_collection_without_background_maintenance(
    engine: &LocalStorageEngine,
) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
    let mut descriptor = engine.plan_collection_descriptor(&CreateCollectionRequest::new(
        COLLECTION_NAME,
        RECORD_DIMENSIONS,
        DistanceMetric::Cosine,
    ))?;
    descriptor.flush_threshold_ops = usize::MAX;
    descriptor.flush_threshold_bytes = usize::MAX;
    descriptor.compaction_threshold_segments = usize::MAX;
    engine.create_collection_from_descriptor(
        descriptor,
        Some(&CollectionAssignment {
            assigned_node: ANONYMOUS_LOCAL_NODE_NAME.to_owned(),
            assigned_role: NodeRole::Data,
        }),
    )
}

fn next_crash_action(
    rng: &mut StdRng,
    snapshot_count: usize,
    segment_count: usize,
) -> StorageAction {
    let (during, max_ops) = match rng.random_range(0..4) {
        0 => (
            CrashDuring::PutBatch(generate_put_batch(rng)),
            MAX_WRITE_CRASH_OPS,
        ),
        1 => (
            CrashDuring::Delete {
                id: format!("id-{}", rng.random_range(0..RECORD_ID_POOL)),
            },
            MAX_WRITE_CRASH_OPS,
        ),
        2 => (CrashDuring::Flush, MAX_MAINTENANCE_CRASH_OPS),
        _ => (CrashDuring::Compact, MAX_MAINTENANCE_CRASH_OPS),
    };
    let _ = (snapshot_count, segment_count);
    StorageAction::Crash {
        during,
        crash_after_ops: rng.random_range(0..=max_ops),
        tear: TearMode::ALL[rng.random_range(0..TearMode::ALL.len())],
        recovery_crash_after_ops: rng
            .random_bool(0.25)
            .then(|| rng.random_range(0..MAX_RECOVERY_CRASH_OPS)),
    }
}

fn scenario_seeds() -> Vec<u64> {
    match std::env::var("LOGPOSE_STORAGE_RANDOM_SEED") {
        Ok(value) if !value.trim().is_empty() => {
            value.split(',').map(str::trim).map(parse_seed).collect()
        }
        _ => {
            let mut random = rng();
            (0..DEFAULT_RANDOM_SCENARIOS)
                .map(|_| random.random::<u64>())
                .collect()
        }
    }
}

#[allow(clippy::panic)]
fn parse_seed(seed: &str) -> u64 {
    match seed.parse::<u64>() {
        Ok(value) => value,
        Err(error) => panic!("invalid LOGPOSE_STORAGE_RANDOM_SEED '{seed}': {error}"),
    }
}

fn next_action(rng: &mut StdRng, snapshot_count: usize, segment_count: usize) -> StorageAction {
    let roll = rng.random_range(0..100);
    match roll {
        0..=34 => StorageAction::PutBatch(generate_put_batch(rng)),
        35..=49 => StorageAction::Delete {
            id: format!("id-{}", rng.random_range(0..RECORD_ID_POOL)),
        },
        50..=59 => StorageAction::Snapshot,
        60..=69 => StorageAction::ScanCurrent,
        70..=77 if snapshot_count > 0 => StorageAction::ScanSnapshot {
            snapshot_index: rng.random_range(0..snapshot_count),
        },
        78..=82 => StorageAction::Flush,
        83..=87 => StorageAction::Compact,
        88..=90 => StorageAction::Stats,
        91..=93 => StorageAction::InspectManifest,
        94..=96 => StorageAction::InspectWal,
        97..=98 if segment_count > 0 => StorageAction::InspectSegment,
        _ => StorageAction::Reopen,
    }
}

fn generate_put_batch(rng: &mut StdRng) -> Vec<TestRecord> {
    let batch_size = rng.random_range(1..=3);
    let mut selected = BTreeSet::new();
    while selected.len() < batch_size {
        selected.insert(rng.random_range(0..RECORD_ID_POOL));
    }

    selected
        .into_iter()
        .map(|slot| {
            let version = rng.random_range(0..=999u64);
            TestRecord {
                id: format!("id-{slot}"),
                vector: vec![
                    // Never zero, so no vector is all zeros (cosine collections reject those).
                    rng.random_range(1..=10u64) as f32 + (slot as f32 / 10.0),
                    rng.random_range(0..=10u64) as f32 + (version as f32 / 1000.0),
                ],
                metadata: json!({
                    "slot": slot,
                    "version": version,
                }),
            }
        })
        .collect()
}

async fn assert_current_scan_matches(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    seed: u64,
    trace: &[StorageAction],
) {
    let actual = engine
        .scan_exact(COLLECTION_NAME, None)
        .await
        .unwrap_or_else(|error| panic_with_context(seed, trace, format!("scan failed: {error}")));
    let expected = model.expected_visible(model.current_snapshot().visible_seq_no);
    assert_eq_with_context(seed, trace, "current scan mismatch", &expected, &actual);
}

async fn assert_scan_matches_snapshot(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    snapshot: &Snapshot,
    seed: u64,
    trace: &[StorageAction],
) {
    let actual = engine
        .scan_exact(COLLECTION_NAME, Some(snapshot.clone()))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("snapshot scan failed: {error}"))
        });
    let expected = model.expected_visible(snapshot.visible_seq_no);
    assert_eq_with_context(seed, trace, "snapshot scan mismatch", &expected, &actual);
}

async fn assert_current_exact_queries_match(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    seed: u64,
    trace: &[StorageAction],
) {
    assert_exact_queries_match(engine, model, None, seed, trace).await;
}

async fn assert_exact_queries_match_snapshot(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    snapshot: &Snapshot,
    seed: u64,
    trace: &[StorageAction],
) {
    assert_exact_queries_match(engine, model, Some(snapshot.clone()), seed, trace).await;
}

async fn assert_exact_queries_match(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    snapshot: Option<Snapshot>,
    seed: u64,
    trace: &[StorageAction],
) {
    for vector in EXACT_QUERY_VECTORS {
        let request = match snapshot.clone() {
            Some(snapshot) => snapshot_exact_query_request(vector.to_vec(), snapshot),
            None => current_exact_query_request(vector.to_vec()),
        };
        let actual = query(engine, request.clone())
            .await
            .unwrap_or_else(|error| {
                panic_with_context(seed, trace, format!("query failed: {error}"))
            });
        let expected = model.expected_query_response(request.clone());
        let exact_ranking = model.expected_query_ranking(&request);

        let profiled = query(engine, profiled_request(&request))
            .await
            .unwrap_or_else(|error| {
                panic_with_context(seed, trace, format!("profile query failed: {error}"))
            });
        let diagnostics = profiled.diagnostics.as_ref().unwrap_or_else(|| {
            panic_with_context(seed, trace, "profile query missing diagnostics".to_owned())
        });
        assert_query_response_matches_oracle(
            seed,
            trace,
            diagnostics.chosen_plan,
            &expected,
            &exact_ranking,
            &actual,
        );
        assert_query_response_matches_oracle(
            seed,
            trace,
            diagnostics.chosen_plan,
            &expected,
            &exact_ranking,
            &profiled,
        );
        let timings = diagnostics.stage_timings.as_ref().unwrap_or_else(|| {
            panic_with_context(
                seed,
                trace,
                "profile query missing stage timings".to_owned(),
            )
        });
        assert!(
            timings.planning_micros
                + timings.candidate_generation_micros
                + timings.rerank_micros
                + timings.postfilter_micros
                > 0,
            "seed={seed} trace={trace:?} diagnostics={diagnostics:#?}"
        );
        assert!(
            diagnostics.units_scanned == 0 || !diagnostics.unit_scan_mix.is_empty(),
            "seed={seed} trace={trace:?} diagnostics={diagnostics:#?}"
        );
    }
}

async fn assert_stats_match(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    snapshot: Option<Snapshot>,
    seed: u64,
    trace: &[StorageAction],
) {
    let actual = match snapshot.clone() {
        Some(snapshot) => engine
            .stats_snapshot(COLLECTION_NAME, Some(snapshot))
            .await
            .unwrap_or_else(|error| {
                panic_with_context(seed, trace, format!("snapshot stats failed: {error}"))
            }),
        None => engine.stats(COLLECTION_NAME).await.unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("stats failed: {error}"))
        }),
    };
    let expected_snapshot = snapshot.unwrap_or_else(|| model.current_snapshot());
    let expected = model.expected_stats(expected_snapshot);
    assert_eq_with_context(
        seed,
        trace,
        "stats mismatch",
        &expected.collection_id,
        &actual.collection_id,
    );
    assert_eq_with_context(
        seed,
        trace,
        "stats mismatch",
        &expected.collection_name,
        &actual.collection_name,
    );
    assert_eq_with_context(
        seed,
        trace,
        "stats mismatch",
        &expected.manifest_generation,
        &actual.manifest_generation,
    );
    assert_eq_with_context(
        seed,
        trace,
        "stats mismatch",
        &expected.visible_seq_no,
        &actual.visible_seq_no,
    );
    assert_eq_with_context(
        seed,
        trace,
        "stats mismatch",
        &expected.mutable_op_count,
        &actual.mutable_op_count,
    );
    assert_eq_with_context(
        seed,
        trace,
        "stats mismatch",
        &expected.segment_count,
        &actual.segment_count,
    );
    assert_eq_with_context(
        seed,
        trace,
        "stats mismatch",
        &expected.live_record_count,
        &actual.live_record_count,
    );
    assert_eq_with_context(
        seed,
        trace,
        "stats mismatch",
        &expected.deleted_record_count,
        &actual.deleted_record_count,
    );
    assert!(
        !actual.query_units.is_empty(),
        "seed={seed} trace={trace:?} query_units={:?}",
        actual.query_units
    );
    assert_eq!(
        actual.query_units[0].tier, "mutable",
        "seed={seed} trace={trace:?} query_units={:?}",
        actual.query_units
    );
    if expected.segment_count > 0 {
        let immutable = actual
            .query_units
            .iter()
            .find(|unit| unit.tier == "immutable")
            .unwrap_or_else(|| {
                panic_with_context(
                    seed,
                    trace,
                    format!("missing immutable unit in stats: {:?}", actual.query_units),
                )
            });
        assert!(
            immutable
                .artifact_stats
                .iter()
                .any(|artifact| artifact.file_name.ends_with(".seg")),
            "seed={seed} trace={trace:?} immutable={immutable:?}"
        );
        assert!(
            immutable.component_bytes.contains_key("segment"),
            "seed={seed} trace={trace:?} immutable={immutable:?}"
        );
    }
}

async fn assert_manifest_inspect_matches(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    seed: u64,
    trace: &[StorageAction],
) {
    let report = engine
        .inspect(COLLECTION_NAME, InspectTarget::Manifest)
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("manifest inspect failed: {error}"))
        });
    assert_eq!(report.target, "manifest");
    let segments = report
        .payload
        .get("segments")
        .and_then(Value::as_array)
        .expect("manifest segments should be an array");
    assert_eq!(segments.len(), model.segment_count);
}

async fn assert_wal_inspect_matches(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    seed: u64,
    trace: &[StorageAction],
) {
    let report = engine
        .inspect(COLLECTION_NAME, InspectTarget::Wal)
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("wal inspect failed: {error}"))
        });
    assert_eq!(report.target, "wal");
    let records = report
        .payload
        .get("records")
        .and_then(Value::as_array)
        .expect("wal records should be an array");
    assert_eq!(records.len(), model.memtable_put_count());
    let operations = report.payload["visible_seq_no"]
        .as_u64()
        .zip(report.payload["checkpoint_seq_no"].as_u64())
        .map(|(visible, checkpoint)| visible - checkpoint);
    assert_eq!(operations, Some(model.mutable_op_count() as u64));
}

async fn assert_segment_inspect_matches(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    seed: u64,
    trace: &[StorageAction],
) {
    let manifest = engine
        .inspect(COLLECTION_NAME, InspectTarget::Manifest)
        .await
        .unwrap_or_else(|error| {
            panic_with_context(
                seed,
                trace,
                format!("segment manifest inspect failed: {error}"),
            )
        });
    let segment_id = manifest
        .payload
        .get("segments")
        .and_then(Value::as_array)
        .and_then(|segments| segments.first())
        .and_then(|segment| segment.get("segment_id"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic_with_context(
                seed,
                trace,
                "segment inspect requested without a segment".to_owned(),
            )
        })
        .to_owned();
    let report = engine
        .inspect(COLLECTION_NAME, InspectTarget::Segment(segment_id.clone()))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("segment inspect failed: {error}"))
        });
    assert_eq!(report.target, format!("segment:{segment_id}"));
    assert_eq!(
        report
            .payload
            .get("segment")
            .and_then(Value::as_object)
            .and_then(|segment| segment.get("segment_id"))
            .and_then(Value::as_str),
        Some(segment_id.as_str())
    );
    assert!(
        report
            .payload
            .get("records")
            .and_then(Value::as_array)
            .is_some_and(|records| !records.is_empty()),
        "seed={seed} trace={trace:?} expected a non-empty segment payload with {segment_id}"
    );
    assert!(
        model.segment_count > 0,
        "segment inspect should only run when segments exist"
    );
}

fn assert_ack_matches(
    ack: &CommitAck,
    applied_ops: usize,
    model: &ExpectedModel,
    seed: u64,
    trace: &[StorageAction],
) {
    let expected = CommitAck {
        last_seq_no: model.current_snapshot().visible_seq_no,
        applied_ops,
        snapshot: model.current_snapshot(),
    };
    assert_eq_with_context(seed, trace, "commit ack mismatch", &expected, ack);
}

impl ExpectedModel {
    fn expected_query_response(&self, request: QueryRequest) -> QueryResponse {
        let metric = self.metric.expect("collection metric should be registered");
        let snapshot = request.snapshot.unwrap_or_else(|| self.current_snapshot());
        let matches = self.expected_query_matches(
            metric,
            request.vector.as_slice(),
            request.top_k,
            snapshot.visible_seq_no,
        );
        QueryResponse {
            metric,
            top_k: request.top_k,
            returned: matches.len(),
            snapshot,
            matches,
            diagnostics: None,
            snapshot_token: None,
        }
    }

    fn expected_query_ranking(&self, request: &QueryRequest) -> Vec<QueryMatch> {
        let metric = self.metric.expect("collection metric should be registered");
        let snapshot = request
            .snapshot
            .clone()
            .unwrap_or_else(|| self.current_snapshot());
        self.expected_query_matches(
            metric,
            request.vector.as_slice(),
            self.expected_visible(snapshot.visible_seq_no).len(),
            snapshot.visible_seq_no,
        )
    }

    fn expected_query_matches(
        &self,
        metric: DistanceMetric,
        query: &[f32],
        top_k: usize,
        visible_seq_no: SeqNo,
    ) -> Vec<QueryMatch> {
        let mut matches = self
            .expected_visible(visible_seq_no)
            .into_iter()
            .map(|record| {
                let value = expected_match_value(metric, query, &record.vector);
                QueryMatch {
                    id: record.id,
                    value,
                    metadata: record.metadata,
                }
            })
            .collect::<Vec<_>>();

        matches.sort_by(|left, right| compare_query_matches(metric, left, right));
        matches.truncate(top_k);
        matches
    }
}

fn current_exact_query_request(vector: Vec<f32>) -> QueryRequest {
    QueryRequest {
        collection_name: COLLECTION_NAME.to_owned(),
        vector,
        top_k: EXACT_QUERY_TOP_K,
        snapshot: None,
        read_barrier: None,
        filters: Vec::new(),
        predicate: None,
        explain: logpose_query::ExplainMode::None,
        snapshot_token: None,
        pin: false,
    }
}

fn snapshot_exact_query_request(vector: Vec<f32>, snapshot: Snapshot) -> QueryRequest {
    QueryRequest {
        collection_name: COLLECTION_NAME.to_owned(),
        vector,
        top_k: EXACT_QUERY_TOP_K,
        snapshot: Some(snapshot),
        read_barrier: None,
        filters: Vec::new(),
        predicate: None,
        explain: logpose_query::ExplainMode::None,
        snapshot_token: None,
        pin: false,
    }
}

fn profiled_request(request: &QueryRequest) -> QueryRequest {
    let mut profiled = request.clone();
    profiled.explain = ExplainMode::Profile;
    profiled
}

fn expected_match_value(metric: DistanceMetric, query: &[f32], candidate: &[f32]) -> f32 {
    match metric {
        DistanceMetric::Dot => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| lhs * rhs)
            .sum(),
        DistanceMetric::Cosine => {
            let dot: f32 = query
                .iter()
                .zip(candidate)
                .map(|(lhs, rhs)| lhs * rhs)
                .sum();
            let query_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
            let candidate_norm = candidate
                .iter()
                .map(|value| value * value)
                .sum::<f32>()
                .sqrt();

            if query_norm == 0.0 || candidate_norm == 0.0 {
                0.0
            } else {
                dot / (query_norm * candidate_norm)
            }
        }
        DistanceMetric::L2 => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| {
                let delta = lhs - rhs;
                delta * delta
            })
            .sum::<f32>()
            .sqrt(),
    }
}

fn compare_query_matches(
    metric: DistanceMetric,
    left: &QueryMatch,
    right: &QueryMatch,
) -> std::cmp::Ordering {
    let value_order = match metric {
        DistanceMetric::Cosine | DistanceMetric::Dot => right.value.total_cmp(&left.value),
        DistanceMetric::L2 => left.value.total_cmp(&right.value),
    };

    value_order.then_with(|| left.id.cmp(&right.id))
}

fn assert_query_response_matches_oracle(
    seed: u64,
    trace: &[StorageAction],
    plan: QueryPlanKind,
    expected: &QueryResponse,
    exact_ranking: &[QueryMatch],
    actual: &QueryResponse,
) {
    assert_eq_with_context(
        seed,
        trace,
        "query metric mismatch",
        &expected.metric,
        &actual.metric,
    );
    assert_eq_with_context(
        seed,
        trace,
        "query top_k mismatch",
        &expected.top_k,
        &actual.top_k,
    );
    assert_eq_with_context(
        seed,
        trace,
        "query snapshot mismatch",
        &expected.snapshot,
        &actual.snapshot,
    );
    assert_eq_with_context(
        seed,
        trace,
        "query returned count mismatch",
        &actual.matches.len(),
        &actual.returned,
    );

    if !uses_ann(plan) {
        assert_eq_with_context(
            seed,
            trace,
            "exact query matches mismatch",
            &expected.matches,
            &actual.matches,
        );
        return;
    }

    let expected_top_ids = expected
        .matches
        .iter()
        .map(|candidate| candidate.id.as_str().to_owned())
        .collect::<Vec<_>>();
    let actual_ids = actual
        .matches
        .iter()
        .map(|candidate| candidate.id.as_str().to_owned())
        .collect::<Vec<_>>();
    let hits = expected_top_ids
        .iter()
        .filter(|id| actual_ids.contains(*id))
        .count();
    assert!(
        hits >= minimum_required_hits(expected_top_ids.len()),
        "seed={seed} trace={trace:?} plan={plan:?} expected_top={expected_top_ids:?} actual={actual_ids:?}"
    );
    if let Some(first_expected) = expected_top_ids.first() {
        assert_eq!(
            actual_ids.first(),
            Some(first_expected),
            "seed={seed} trace={trace:?} plan={plan:?} expected_top={expected_top_ids:?} actual={actual_ids:?}"
        );
    }

    let exact_lookup = exact_ranking
        .iter()
        .enumerate()
        .map(|(rank, candidate)| {
            (
                candidate.id.as_str().to_owned(),
                (rank, candidate.value, candidate.metadata.clone()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut observed_ranks = Vec::with_capacity(actual.matches.len());
    for candidate in &actual.matches {
        let Some((rank, exact_value, exact_metadata)) =
            exact_lookup.get(candidate.id.as_str()).cloned()
        else {
            panic_with_context(
                seed,
                trace,
                format!("ann query returned unknown id '{}'", candidate.id),
            );
        };
        observed_ranks.push(rank);
        assert!(
            (candidate.value - exact_value).abs() <= f32::EPSILON,
            "seed={seed} trace={trace:?} id={} expected_value={exact_value} actual_value={}",
            candidate.id,
            candidate.value
        );
        assert_eq_with_context(
            seed,
            trace,
            "ann query metadata mismatch",
            &exact_metadata,
            &candidate.metadata,
        );
    }
    assert!(
        observed_ranks.windows(2).all(|pair| pair[0] <= pair[1]),
        "seed={seed} trace={trace:?} plan={plan:?} exact_ranks={observed_ranks:?}"
    );
}

fn uses_ann(plan: QueryPlanKind) -> bool {
    matches!(
        plan,
        QueryPlanKind::VectorFirstAnn
            | QueryPlanKind::CooperativeFilteredAnn
            | QueryPlanKind::HybridExactAnnMerge
    )
}

fn minimum_required_hits(top_k: usize) -> usize {
    if top_k == 0 {
        0
    } else {
        (top_k * 2).div_ceil(3)
    }
}

fn assert_eq_with_context<T>(
    seed: u64,
    trace: &[StorageAction],
    message: &str,
    expected: &T,
    actual: &T,
) where
    T: std::fmt::Debug + PartialEq,
{
    if expected != actual {
        panic_with_context(
            seed,
            trace,
            format!("{message}\nexpected: {expected:#?}\nactual: {actual:#?}"),
        );
    }
}

#[allow(clippy::panic)]
fn panic_with_context(seed: u64, trace: &[StorageAction], message: String) -> ! {
    panic!("seed={seed}\ntrace={trace:#?}\n{message}");
}
