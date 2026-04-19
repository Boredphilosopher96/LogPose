use logpose_query::{
    ExplainMode, QueryMatch, QueryPlanKind, QueryRequest, QueryResponse, query_exact,
};
use logpose_storage::{CreateCollectionRequest, InspectTarget, LocalStorageEngine, StorageEngine};
use logpose_types::{
    CollectionId, CollectionStats, CommitAck, DEFAULT_DATABASE_NAME, DeleteRecord, DistanceMetric,
    PutRecord, RecordId, SeqNo, Snapshot, VisibleRecord, WriteOperation,
};
use rand::{RngExt, SeedableRng, rng, rngs::StdRng};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

#[path = "fs.rs"]
mod fs_support;

const COLLECTION_NAME: &str = "randomized";
const DEFAULT_SCENARIO_STEPS: usize = 40;
const DEFAULT_RANDOM_SCENARIOS: usize = 5;
const RECORD_DIMENSIONS: usize = 2;
const RECORD_ID_POOL: usize = 8;
const EXACT_QUERY_TOP_K: usize = 3;
const EXACT_QUERY_VECTORS: [[f32; RECORD_DIMENSIONS]; 3] = [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]];

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
    InspectManifest,
    InspectWal,
    InspectSegment,
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

#[derive(Clone, Copy, Debug)]
struct ExpectedGenerationState {
    checkpoint_seq_no: SeqNo,
    segment_count: usize,
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
        }
    }

    fn register_collection(&mut self, collection_id: CollectionId, metric: DistanceMetric) {
        self.collection_id = Some(collection_id);
        self.metric = Some(metric);
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
        self.generation_states.insert(
            self.manifest_generation,
            ExpectedGenerationState {
                checkpoint_seq_no: self.checkpoint_seq_no,
                segment_count: self.segment_count,
            },
        );
    }

    fn record_compact(&mut self) {
        if self.segment_count <= 1 {
            return;
        }
        self.manifest_generation += 1;
        self.segment_count = 1;
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
        let deleted_record_count = resolved
            .values()
            .filter(|state| matches!(state, ExpectedState::Deleted))
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

pub async fn run_storage_scenarios() {
    let seeds = scenario_seeds();
    for seed in seeds {
        run_seeded_storage_scenario(seed, DEFAULT_SCENARIO_STEPS).await;
    }
}

pub fn current_exact_query_request_for_test(vector: Vec<f32>) -> QueryRequest {
    current_exact_query_request(vector)
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
        .create_collection(CreateCollectionRequest::new(
            COLLECTION_NAME,
            RECORD_DIMENSIONS,
            DistanceMetric::Cosine,
        ))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, &trace, format!("create failed: {error}"))
        });
    disable_background_maintenance(&descriptor.root_path);
    model.register_collection(descriptor.collection_id.clone(), descriptor.metric);
    assert_stats_match(&engine, &model, None, seed, &trace).await;
    assert_current_scan_matches(&engine, &model, seed, &trace).await;
    assert_current_exact_queries_match(&engine, &model, seed, &trace).await;

    for _ in 0..steps {
        let action = next_action(&mut rng, snapshots.len(), model.segment_count);
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
                snapshots.push(snapshot.clone());
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
                let snapshot = snapshots.get(snapshot_index).cloned().unwrap_or_else(|| {
                    panic_with_context(
                        seed,
                        &trace,
                        format!("missing snapshot index {snapshot_index}"),
                    )
                });
                assert_scan_matches_snapshot(&engine, &model, &snapshot, seed, &trace).await;
                assert_exact_queries_match_snapshot(&engine, &model, &snapshot, seed, &trace).await;
                assert_stats_match(&engine, &model, Some(snapshot), seed, &trace).await;
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
                engine = LocalStorageEngine::new(&root);
                assert_current_scan_matches(&engine, &model, seed, &trace).await;
                assert_current_exact_queries_match(&engine, &model, seed, &trace).await;
                assert_stats_match(&engine, &model, None, seed, &trace).await;
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
        let actual = query_exact(engine, request.clone())
            .await
            .unwrap_or_else(|error| {
                panic_with_context(seed, trace, format!("query failed: {error}"))
            });
        let expected = model.expected_query_response(request.clone());
        let exact_ranking = model.expected_query_ranking(&request);

        let profiled = query_exact(engine, profiled_request(&request))
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
            timings.planning_micros > 0,
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
                .any(|artifact| artifact.file_name.ends_with(".flat.json")),
            "seed={seed} trace={trace:?} immutable={immutable:?}"
        );
        assert!(
            immutable
                .artifact_stats
                .iter()
                .any(|artifact| artifact.file_name.ends_with(".hnsw.bin")),
            "seed={seed} trace={trace:?} immutable={immutable:?}"
        );
        assert!(
            immutable.component_bytes.contains_key("ann_graph"),
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
    assert_eq!(records.len(), model.mutable_op_count());
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
        filters: Vec::new(),
        predicate: None,
        explain: logpose_query::ExplainMode::None,
    }
}

fn disable_background_maintenance(root_path: &Path) {
    let descriptor_path = root_path.join("descriptor.json");
    let mut descriptor = serde_json::from_slice::<Value>(
        &fs::read(&descriptor_path).expect("descriptor should exist"),
    )
    .expect("descriptor should parse");
    descriptor["flush_threshold_ops"] = json!(usize::MAX);
    descriptor["flush_threshold_bytes"] = json!(usize::MAX);
    descriptor["compaction_threshold_segments"] = json!(usize::MAX);
    fs::write(
        &descriptor_path,
        serde_json::to_vec_pretty(&descriptor).expect("descriptor should serialize"),
    )
    .expect("descriptor should be updated");
}

fn snapshot_exact_query_request(vector: Vec<f32>, snapshot: Snapshot) -> QueryRequest {
    QueryRequest {
        collection_name: COLLECTION_NAME.to_owned(),
        vector,
        top_k: EXACT_QUERY_TOP_K,
        snapshot: Some(snapshot),
        filters: Vec::new(),
        predicate: None,
        explain: logpose_query::ExplainMode::None,
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
