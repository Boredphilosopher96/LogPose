use axum::body::Body;
use http_body_util::BodyExt;
use logpose_api_grpc::proto::log_pose_service_server::LogPoseService;
use logpose_api_grpc::{GrpcLogPoseService, proto};
use logpose_core::AppState;
use logpose_query::{MetadataFilter, QueryMatch, QueryRequest, QueryResponse, ScalarMetadataValue};
use logpose_service::LogPoseDataService;
use logpose_storage::CreateCollectionRequest;
use logpose_types::{
    CollectionStats, CommitAck, DeleteRecord, DistanceMetric, PutRecord, RecordId, SeqNo, Snapshot,
    VisibleRecord, WriteOperation,
};
use rand::{RngExt, SeedableRng, rng, rngs::StdRng};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tonic::Request;
use tower::util::ServiceExt;

const COLLECTION_NAME: &str = "randomized";
const DEFAULT_SCENARIO_STEPS: usize = 30;
const DEFAULT_RANDOM_SCENARIOS: usize = 4;
const RECORD_DIMENSIONS: usize = 2;
const RECORD_ID_POOL: usize = 6;
const EXACT_QUERY_TOP_K: usize = 3;
const EXACT_QUERY_VECTORS: [[f32; RECORD_DIMENSIONS]; 3] = [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]];

#[derive(Clone, Debug)]
enum ServiceAction {
    PutBatch(Vec<TestRecord>),
    Delete {
        id: String,
    },
    Snapshot,
    QueryCurrent {
        vector_index: usize,
        keep_only: bool,
    },
    QuerySnapshot {
        snapshot_index: usize,
        vector_index: usize,
        keep_only: bool,
    },
    Flush,
    Compact,
    Stats,
    InspectManifest,
}

#[derive(Clone, Debug)]
struct TestRecord {
    id: String,
    vector: Vec<f32>,
    metadata: Value,
}

#[derive(Debug)]
enum ExpectedState {
    Visible(VisibleRecord),
    Deleted,
}

#[derive(Debug)]
struct ExpectedModel {
    collection_id: Option<String>,
    metric: Option<DistanceMetric>,
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
            metric: None,
            manifest_generation: 0,
            checkpoint_seq_no: 0,
            next_seq_no: 0,
            segment_count: 0,
            history: Vec::new(),
        }
    }

    fn register_collection(&mut self, collection_id: String, metric: DistanceMetric) {
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
            collection_id: logpose_types::CollectionId(
                self.collection_id
                    .clone()
                    .expect("collection should exist")
                    .parse()
                    .expect("collection id should be valid uuid"),
            ),
            collection_name: COLLECTION_NAME.to_owned(),
            manifest_generation: self.manifest_generation,
            visible_seq_no: self.next_seq_no,
            mutable_op_count: self.mutable_op_count(),
            segment_count: self.segment_count,
            live_record_count,
            deleted_record_count,
        }
    }

    fn expected_query_response(
        &self,
        vector: &[f32],
        snapshot: Snapshot,
        keep_only: bool,
    ) -> QueryResponse {
        let metric = self.metric.expect("metric should be set");
        let filters = if keep_only {
            vec![MetadataFilter {
                field: "kind".to_owned(),
                value: ScalarMetadataValue::String("keep".to_owned()),
            }]
        } else {
            Vec::new()
        };

        let mut matches = self
            .resolve_latest(snapshot.visible_seq_no)
            .into_values()
            .filter_map(|state| match state {
                ExpectedState::Visible(record) => Some(record),
                ExpectedState::Deleted => None,
            })
            .filter(|record| record_matches_filters(record, &filters))
            .map(|record| QueryMatch {
                id: record.id,
                value: expected_match_value(metric, vector, &record.vector),
                metadata: record.metadata,
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| compare_query_matches(metric, left, right));
        matches.truncate(EXACT_QUERY_TOP_K);

        QueryResponse {
            metric,
            top_k: EXACT_QUERY_TOP_K,
            returned: matches.len(),
            snapshot,
            matches,
        }
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

pub async fn run_service_scenarios() {
    for seed in scenario_seeds() {
        run_seeded_service_scenario(seed, DEFAULT_SCENARIO_STEPS).await;
    }
}

async fn run_seeded_service_scenario(seed: u64, steps: usize) {
    let root = unique_temp_dir(&format!("service-random-{seed}"));
    let state = Arc::new(AppState::new(test_config(&root)));
    let service = Arc::clone(&state.service);
    let rest = logpose_api_rest::router(Arc::clone(&state));
    let grpc = GrpcLogPoseService::new(state);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut model = ExpectedModel::new();
    let mut trace = Vec::new();
    let mut snapshots = Vec::new();

    let descriptor = service
        .create_collection(CreateCollectionRequest {
            name: COLLECTION_NAME.to_owned(),
            dimensions: RECORD_DIMENSIONS,
            metric: DistanceMetric::Cosine,
        })
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, &trace, format!("create failed: {error}"))
        });
    model.register_collection(descriptor.collection_id.to_string(), descriptor.metric);

    for _ in 0..steps {
        let action = next_action(&mut rng, snapshots.len());
        trace.push(action.clone());

        match action {
            ServiceAction::PutBatch(records) => {
                let operations = records
                    .into_iter()
                    .map(|record| {
                        WriteOperation::Put(PutRecord {
                            id: RecordId::new(record.id),
                            vector: record.vector,
                            metadata: record.metadata,
                        })
                    })
                    .collect::<Vec<_>>();
                let ack = service
                    .write(COLLECTION_NAME, operations.clone())
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("write failed: {error}"))
                    });
                model.record_write(&operations);
                assert_ack_matches(&ack, operations.len(), &model, seed, &trace);
            }
            ServiceAction::Delete { id } => {
                let operations = vec![WriteOperation::Delete(DeleteRecord {
                    id: RecordId::new(id),
                })];
                let ack = service
                    .write(COLLECTION_NAME, operations.clone())
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("delete failed: {error}"))
                    });
                model.record_write(&operations);
                assert_ack_matches(&ack, operations.len(), &model, seed, &trace);
            }
            ServiceAction::Snapshot => {
                let snapshot = service
                    .snapshot(COLLECTION_NAME)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("snapshot failed: {error}"))
                    });
                let expected = model.current_snapshot();
                assert_eq_with_context(seed, &trace, "snapshot mismatch", &expected, &snapshot);
                snapshots.push(snapshot);
            }
            ServiceAction::QueryCurrent {
                vector_index,
                keep_only,
            } => {
                assert_query_parity(
                    &service,
                    &rest,
                    &grpc,
                    &model,
                    None,
                    vector_index,
                    keep_only,
                    seed,
                    &trace,
                )
                .await;
            }
            ServiceAction::QuerySnapshot {
                snapshot_index,
                vector_index,
                keep_only,
            } => {
                let snapshot = snapshots.get(snapshot_index).cloned().unwrap_or_else(|| {
                    panic_with_context(
                        seed,
                        &trace,
                        format!("missing snapshot index {snapshot_index}"),
                    )
                });
                assert_query_parity(
                    &service,
                    &rest,
                    &grpc,
                    &model,
                    Some(snapshot),
                    vector_index,
                    keep_only,
                    seed,
                    &trace,
                )
                .await;
            }
            ServiceAction::Flush => {
                let snapshot = service
                    .flush(COLLECTION_NAME)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("flush failed: {error}"))
                    });
                model.record_flush();
                assert_eq_with_context(
                    seed,
                    &trace,
                    "flush mismatch",
                    &model.current_snapshot(),
                    &snapshot,
                );
            }
            ServiceAction::Compact => {
                let snapshot = service
                    .compact(COLLECTION_NAME)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("compact failed: {error}"))
                    });
                model.record_compact();
                assert_eq_with_context(
                    seed,
                    &trace,
                    "compact mismatch",
                    &model.current_snapshot(),
                    &snapshot,
                );
            }
            ServiceAction::Stats => {
                assert_stats_parity(&service, &rest, &grpc, &model, seed, &trace).await;
            }
            ServiceAction::InspectManifest => {
                assert_inspect_parity(&service, &rest, &grpc, seed, &trace).await;
            }
        }
    }
}

fn scenario_seeds() -> Vec<u64> {
    match std::env::var("LOGPOSE_SERVICE_RANDOM_SEED") {
        Ok(value) if !value.trim().is_empty() => value
            .split(',')
            .map(str::trim)
            .map(|seed| seed.parse::<u64>().expect("seed should parse"))
            .collect(),
        _ => {
            let mut random = rng();
            (0..DEFAULT_RANDOM_SCENARIOS)
                .map(|_| random.random::<u64>())
                .collect()
        }
    }
}

fn next_action(rng: &mut StdRng, snapshot_count: usize) -> ServiceAction {
    let roll = rng.random_range(0..100);
    match roll {
        0..=34 => ServiceAction::PutBatch(generate_put_batch(rng)),
        35..=48 => ServiceAction::Delete {
            id: format!("id-{}", rng.random_range(0..RECORD_ID_POOL)),
        },
        49..=57 => ServiceAction::Snapshot,
        58..=69 => ServiceAction::QueryCurrent {
            vector_index: rng.random_range(0..EXACT_QUERY_VECTORS.len()),
            keep_only: rng.random_bool(0.5),
        },
        70..=77 if snapshot_count > 0 => ServiceAction::QuerySnapshot {
            snapshot_index: rng.random_range(0..snapshot_count),
            vector_index: rng.random_range(0..EXACT_QUERY_VECTORS.len()),
            keep_only: rng.random_bool(0.5),
        },
        78..=85 => ServiceAction::Flush,
        86..=91 => ServiceAction::Compact,
        92..=96 => ServiceAction::Stats,
        _ => ServiceAction::InspectManifest,
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
            let keep = rng.random_bool(0.5);
            TestRecord {
                id: format!("id-{slot}"),
                vector: vec![
                    rng.random_range(0..=10u64) as f32 + (slot as f32 / 10.0),
                    rng.random_range(0..=10u64) as f32 + (version as f32 / 1000.0),
                ],
                metadata: json!({
                    "slot": slot,
                    "version": version,
                    "kind": if keep { "keep" } else { "drop" },
                }),
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn assert_query_parity(
    service: &LogPoseDataService,
    rest: &axum::Router,
    grpc: &GrpcLogPoseService,
    model: &ExpectedModel,
    snapshot: Option<Snapshot>,
    vector_index: usize,
    keep_only: bool,
    seed: u64,
    trace: &[ServiceAction],
) {
    let vector = EXACT_QUERY_VECTORS[vector_index].to_vec();
    let request = QueryRequest {
        collection_name: COLLECTION_NAME.to_owned(),
        vector: vector.clone(),
        top_k: EXACT_QUERY_TOP_K,
        snapshot: snapshot.clone(),
        filters: if keep_only {
            vec![MetadataFilter {
                field: "kind".to_owned(),
                value: ScalarMetadataValue::String("keep".to_owned()),
            }]
        } else {
            Vec::new()
        },
    };
    let actual = service
        .query(request.clone())
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("service query failed: {error}"))
        });
    let expected = model.expected_query_response(
        &vector,
        snapshot.unwrap_or_else(|| model.current_snapshot()),
        keep_only,
    );
    assert_eq_with_context(seed, trace, "service query mismatch", &expected, &actual);

    let rest_response = rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v1/collections/{COLLECTION_NAME}/query"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "vector": request.vector,
                        "top_k": request.top_k,
                        "snapshot": request.snapshot,
                        "filters": if keep_only { json!({"kind":"keep"}) } else { json!({}) }
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("rest query should respond");
    let rest_body = json_body(rest_response).await;
    assert_eq!(
        rest_body["matches"]
            .as_array()
            .expect("matches should be an array")
            .iter()
            .map(|candidate| candidate["id"].as_str().expect("id should be a string"))
            .collect::<Vec<_>>(),
        expected
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        "seed={seed} trace={trace:?}"
    );

    let grpc_response = grpc
        .query_collection(Request::new(proto::QueryCollectionRequest {
            collection_name: COLLECTION_NAME.to_owned(),
            vector,
            top_k: EXACT_QUERY_TOP_K as u64,
            snapshot: request.snapshot.map(|snapshot| proto::Snapshot {
                manifest_generation: snapshot.manifest_generation,
                visible_seq_no: snapshot.visible_seq_no,
            }),
            filters: if keep_only {
                vec![proto::MetadataFilter {
                    field: "kind".to_owned(),
                    value: Some(proto::ScalarValue {
                        kind: Some(proto::scalar_value::Kind::StringValue("keep".to_owned())),
                    }),
                }]
            } else {
                Vec::new()
            },
        }))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("grpc query failed: {error}"))
        })
        .into_inner();
    assert_eq!(
        grpc_response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        expected
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        "seed={seed} trace={trace:?}"
    );
}

async fn assert_stats_parity(
    service: &LogPoseDataService,
    rest: &axum::Router,
    grpc: &GrpcLogPoseService,
    model: &ExpectedModel,
    seed: u64,
    trace: &[ServiceAction],
) {
    let actual = service
        .stats(COLLECTION_NAME)
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("service stats failed: {error}"))
        });
    let expected = model.expected_stats();
    assert_eq_with_context(seed, trace, "service stats mismatch", &expected, &actual);

    let rest_response = rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/v1/collections/{COLLECTION_NAME}/stats"))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("rest stats should respond");
    let rest_body = json_body(rest_response).await;
    assert_eq!(
        rest_body["live_record_count"], expected.live_record_count,
        "seed={seed} trace={trace:?}"
    );

    let grpc_stats = grpc
        .get_collection_stats(Request::new(proto::GetCollectionStatsRequest {
            collection_name: COLLECTION_NAME.to_owned(),
        }))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("grpc stats failed: {error}"))
        })
        .into_inner();
    assert_eq!(
        grpc_stats.live_record_count as usize, expected.live_record_count,
        "seed={seed} trace={trace:?}"
    );
}

async fn assert_inspect_parity(
    service: &LogPoseDataService,
    rest: &axum::Router,
    grpc: &GrpcLogPoseService,
    seed: u64,
    trace: &[ServiceAction],
) {
    let actual = service
        .inspect_manifest(COLLECTION_NAME)
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("service inspect failed: {error}"))
        });
    assert_eq!(actual.target, "manifest");

    let rest_response = rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(format!(
                    "/v1/collections/{COLLECTION_NAME}/inspect?target=manifest"
                ))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("rest inspect should respond");
    assert_eq!(json_body(rest_response).await["target"], "manifest");

    let grpc_response = grpc
        .inspect_collection(Request::new(proto::InspectCollectionRequest {
            collection_name: COLLECTION_NAME.to_owned(),
            target: proto::InspectTarget::Manifest as i32,
            segment_id: String::new(),
        }))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("grpc inspect failed: {error}"))
        })
        .into_inner();
    assert_eq!(grpc_response.target, "manifest");
}

fn assert_ack_matches(
    ack: &CommitAck,
    applied_ops: usize,
    model: &ExpectedModel,
    seed: u64,
    trace: &[ServiceAction],
) {
    let expected = CommitAck {
        last_seq_no: model.current_snapshot().visible_seq_no,
        applied_ops,
    };
    assert_eq_with_context(seed, trace, "commit ack mismatch", &expected, ack);
}

fn record_matches_filters(record: &VisibleRecord, filters: &[MetadataFilter]) -> bool {
    filters.iter().all(|filter| {
        record
            .metadata
            .get(&filter.field)
            .is_some_and(|value| scalar_matches_value(&filter.value, value))
    })
}

fn scalar_matches_value(expected: &ScalarMetadataValue, actual: &Value) -> bool {
    match (expected, actual) {
        (ScalarMetadataValue::String(expected), Value::String(actual)) => expected == actual,
        (ScalarMetadataValue::Number(expected), Value::Number(actual)) => {
            actual.as_f64().is_some_and(|actual| actual == *expected)
        }
        (ScalarMetadataValue::Bool(expected), Value::Bool(actual)) => expected == actual,
        (ScalarMetadataValue::Null, Value::Null) => true,
        _ => false,
    }
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

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body should be readable")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("body should be valid json")
}

fn test_config(root: &Path) -> logpose_config::LogPoseConfig {
    logpose_config::LogPoseConfig {
        storage_root: root.to_path_buf(),
        ..logpose_config::LogPoseConfig::default()
    }
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-service-{label}-{suffix}"));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}

fn assert_eq_with_context<T>(
    seed: u64,
    trace: &[ServiceAction],
    label: &str,
    expected: &T,
    actual: &T,
) where
    T: std::fmt::Debug + PartialEq,
{
    assert_eq!(
        expected, actual,
        "{label}\nseed={seed}\ntrace={trace:?}\nexpected={expected:#?}\nactual={actual:#?}"
    );
}

#[allow(clippy::panic)]
fn panic_with_context(seed: u64, trace: &[ServiceAction], message: String) -> ! {
    panic!("seed={seed}\ntrace={trace:#?}\n{message}");
}
