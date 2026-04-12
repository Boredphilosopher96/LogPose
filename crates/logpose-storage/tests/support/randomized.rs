use logpose_storage::{CreateCollectionRequest, LocalStorageEngine, StorageEngine};
use logpose_types::{
    CollectionId, CollectionStats, CommitAck, DeleteRecord, DistanceMetric, PutRecord, RecordId,
    SeqNo, Snapshot, VisibleRecord, WriteOperation,
};
use rand::{RngExt, SeedableRng, rng, rngs::StdRng};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[path = "fs.rs"]
mod fs_support;

const COLLECTION_NAME: &str = "randomized";
const DEFAULT_SCENARIO_STEPS: usize = 40;
const DEFAULT_RANDOM_SCENARIOS: usize = 5;
const RECORD_DIMENSIONS: usize = 2;
const RECORD_ID_POOL: usize = 8;

#[derive(Clone, Debug)]
pub enum StorageAction {
    CreateCollection,
    PutBatch(Vec<TestRecord>),
    Delete { id: String },
    Snapshot,
    ScanCurrent,
    ScanSnapshot { snapshot_index: usize },
    Flush,
    Compact,
    Stats,
    Reopen,
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

#[derive(Debug)]
struct ExpectedModel {
    collection_id: Option<CollectionId>,
    manifest_generation: u64,
    checkpoint_seq_no: SeqNo,
    next_seq_no: SeqNo,
    segment_count: usize,
    history: Vec<(SeqNo, WriteOperation)>,
}

impl ExpectedModel {
    fn new() -> Self {
        Self {
            collection_id: None,
            manifest_generation: 0,
            checkpoint_seq_no: 0,
            next_seq_no: 0,
            segment_count: 0,
            history: Vec::new(),
        }
    }

    fn register_collection(&mut self, collection_id: CollectionId) {
        self.collection_id = Some(collection_id);
    }

    fn record_write(&mut self, operations: &[WriteOperation]) {
        for operation in operations {
            self.next_seq_no += 1;
            self.history.push((self.next_seq_no, operation.clone()));
        }
    }

    fn record_flush(&mut self) {
        if self.mutable_op_count() == 0 {
            return;
        }
        self.manifest_generation += 1;
        self.checkpoint_seq_no = self.next_seq_no;
        self.segment_count += 1;
    }

    fn record_compact(&mut self) {
        if self.segment_count <= 1 {
            return;
        }
        self.manifest_generation += 1;
        self.segment_count = 1;
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

    fn expected_stats(&self) -> CollectionStats {
        let resolved = self.resolve_latest(self.next_seq_no);
        let live_record_count = resolved
            .values()
            .filter(|state| matches!(state, ExpectedState::Visible(_)))
            .count();
        let deleted_record_count = resolved
            .values()
            .filter(|state| matches!(state, ExpectedState::Deleted))
            .count();

        CollectionStats {
            collection_id: self
                .collection_id
                .clone()
                .expect("collection id should be registered"),
            collection_name: COLLECTION_NAME.to_owned(),
            manifest_generation: self.manifest_generation,
            visible_seq_no: self.next_seq_no,
            mutable_op_count: self.mutable_op_count(),
            segment_count: self.segment_count,
            live_record_count,
            deleted_record_count,
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

pub async fn run_storage_scenarios() {
    let seeds = scenario_seeds();
    for seed in seeds {
        run_seeded_storage_scenario(seed, DEFAULT_SCENARIO_STEPS).await;
    }
}

async fn run_seeded_storage_scenario(seed: u64, steps: usize) {
    let root = fs_support::unique_temp_dir(&format!("storage-random-{seed}"));
    let mut engine = LocalStorageEngine::new(&root);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut model = ExpectedModel::new();
    let mut trace = Vec::new();
    let mut snapshots = Vec::new();

    trace.push(StorageAction::CreateCollection);
    let descriptor = engine
        .create_collection(CreateCollectionRequest {
            name: COLLECTION_NAME.to_owned(),
            dimensions: RECORD_DIMENSIONS,
            metric: DistanceMetric::Cosine,
        })
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, &trace, format!("create failed: {error}"))
        });
    model.register_collection(descriptor.collection_id.clone());
    assert_stats_match(&engine, &model, seed, &trace).await;
    assert_current_scan_matches(&engine, &model, seed, &trace).await;

    for _ in 0..steps {
        let action = next_action(&mut rng, snapshots.len());
        trace.push(action.clone());

        match action {
            StorageAction::CreateCollection => {
                panic_with_context(seed, &trace, "duplicate create action".to_owned());
            }
            StorageAction::PutBatch(records) => {
                let operations = records
                    .iter()
                    .map(|record| {
                        WriteOperation::Put(PutRecord {
                            id: RecordId::new(record.id.clone()),
                            vector: record.vector.clone(),
                            metadata: record.metadata.clone(),
                        })
                    })
                    .collect::<Vec<_>>();
                let ack = engine
                    .write(COLLECTION_NAME, operations.clone())
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("write failed: {error}"))
                    });
                model.record_write(&operations);
                assert_ack_matches(&ack, operations.len(), &model, seed, &trace);
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, seed, &trace).await;
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
                assert_stats_match(&engine, &model, seed, &trace).await;
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
                snapshots.push(snapshot.clone());
                assert_scan_matches_snapshot(&engine, &model, &snapshot, seed, &trace).await;
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
            }
            StorageAction::ScanCurrent => {
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
            }
            StorageAction::ScanSnapshot { snapshot_index } => {
                let snapshot = snapshots.get(snapshot_index).cloned().unwrap_or_else(|| {
                    panic_with_context(
                        seed,
                        &trace,
                        format!("missing snapshot index {snapshot_index}"),
                    )
                });
                assert_scan_matches_snapshot(&engine, &model, &snapshot, seed, &trace).await;
            }
            StorageAction::Flush => {
                let snapshot = engine.flush(COLLECTION_NAME).await.unwrap_or_else(|error| {
                    panic_with_context(seed, &trace, format!("flush failed: {error}"))
                });
                model.record_flush();
                let expected = model.current_snapshot();
                assert_eq_with_context(
                    seed,
                    &trace,
                    "flush snapshot mismatch",
                    &expected,
                    &snapshot,
                );
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, seed, &trace).await;
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
                model.record_compact();
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
                assert_stats_match(&engine, &model, seed, &trace).await;
            }
            StorageAction::Stats => {
                assert_stats_match(&engine, &model, seed, &trace).await;
            }
            StorageAction::Reopen => {
                engine = LocalStorageEngine::new(&root);
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, seed, &trace).await;
            }
        }
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

fn next_action(rng: &mut StdRng, snapshot_count: usize) -> StorageAction {
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
        78..=85 => StorageAction::Flush,
        86..=92 => StorageAction::Compact,
        93..=96 => StorageAction::Stats,
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
                    rng.random_range(0..=10u64) as f32 + (slot as f32 / 10.0),
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

async fn assert_stats_match(
    engine: &LocalStorageEngine,
    model: &ExpectedModel,
    seed: u64,
    trace: &[StorageAction],
) {
    let actual = engine
        .stats(COLLECTION_NAME)
        .await
        .unwrap_or_else(|error| panic_with_context(seed, trace, format!("stats failed: {error}")));
    let expected = model.expected_stats();
    assert_eq_with_context(seed, trace, "stats mismatch", &expected, &actual);
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
    };
    assert_eq_with_context(seed, trace, "commit ack mismatch", &expected, ack);
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
