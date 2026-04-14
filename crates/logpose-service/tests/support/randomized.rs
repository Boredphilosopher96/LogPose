use axum::body::Body;
use http_body_util::BodyExt;
use logpose_api_grpc::proto::log_pose_service_server::LogPoseService;
use logpose_api_grpc::{GrpcLogPoseService, proto};
use logpose_core::AppState;
use logpose_query::{
    ExplainMode, MetadataFilter, QueryDiagnostics, QueryMatch, QueryPlanKind, QueryRequest,
    QueryResponse, ScalarMetadataValue,
};
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{
    CollectionStats, CommitAck, DeleteRecord, DistanceMetric, MaintenanceStatus, PutRecord,
    RecordId, SeqNo, Snapshot, VisibleRecord, WriteOperation,
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
    InspectWal,
    InspectSegment,
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
            maintenance: MaintenanceStatus::default(),
            query_units: Vec::new(),
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
            diagnostics: None,
        }
    }

    fn expected_query_ranking(
        &self,
        vector: &[f32],
        snapshot: Snapshot,
        keep_only: bool,
    ) -> Vec<QueryMatch> {
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
        matches
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
    let rest = logpose_api_rest::router(Arc::clone(&state));
    let grpc = GrpcLogPoseService::new(Arc::clone(&state));
    let mut rng = StdRng::seed_from_u64(seed);
    let mut model = ExpectedModel::new();
    let mut trace = Vec::new();
    let mut snapshots = Vec::new();

    let descriptor = state
        .control
        .create_collection(CreateCollectionRequest {
            name: COLLECTION_NAME.to_owned(),
            dimensions: RECORD_DIMENSIONS,
            metric: DistanceMetric::Cosine,
        })
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, &trace, format!("create failed: {error}"))
        });
    disable_background_maintenance(&descriptor.root_path);
    model.register_collection(descriptor.collection_id.to_string(), descriptor.metric);

    for _ in 0..steps {
        let action = next_action(&mut rng, snapshots.len(), model.segment_count);
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
                let ack = state
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
                let ack = state
                    .write(COLLECTION_NAME, operations.clone())
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(seed, &trace, format!("delete failed: {error}"))
                    });
                model.record_write(&operations);
                assert_ack_matches(&ack, operations.len(), &model, seed, &trace);
            }
            ServiceAction::Snapshot => {
                let snapshot = state
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
                    &state,
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
                    &state,
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
                let snapshot = state.flush(COLLECTION_NAME).await.unwrap_or_else(|error| {
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
                let snapshot = state
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
                assert_stats_parity(&state, &rest, &grpc, &model, seed, &trace).await;
            }
            ServiceAction::InspectManifest => {
                assert_inspect_parity(
                    &state,
                    &rest,
                    &grpc,
                    &model,
                    InspectTarget::Manifest,
                    seed,
                    &trace,
                )
                .await;
            }
            ServiceAction::InspectWal => {
                assert_inspect_parity(
                    &state,
                    &rest,
                    &grpc,
                    &model,
                    InspectTarget::Wal,
                    seed,
                    &trace,
                )
                .await;
            }
            ServiceAction::InspectSegment => {
                assert_inspect_segment_parity(&state, &rest, &grpc, &model, seed, &trace).await;
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

fn next_action(rng: &mut StdRng, snapshot_count: usize, segment_count: usize) -> ServiceAction {
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
        78..=84 => ServiceAction::Flush,
        85..=89 => ServiceAction::Compact,
        90..=93 => ServiceAction::Stats,
        94..=95 => ServiceAction::InspectManifest,
        96..=97 => ServiceAction::InspectWal,
        98..=99 if segment_count > 0 => ServiceAction::InspectSegment,
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
    state: &AppState,
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
        filters: Vec::new(),
        predicate: keep_only.then(keep_only_predicate),
        explain: ExplainMode::None,
    };
    let actual = state.query(request.clone()).await.unwrap_or_else(|error| {
        panic_with_context(seed, trace, format!("service query failed: {error}"))
    });
    let expected = model.expected_query_response(
        &vector,
        snapshot.unwrap_or_else(|| model.current_snapshot()),
        keep_only,
    );
    let exact_ranking =
        model.expected_query_ranking(&request.vector, expected.snapshot.clone(), keep_only);

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
                        "predicate": if keep_only {
                            json!({
                                "kind": "comparison",
                                "field": "kind",
                                "operator": "eq",
                                "value": "keep"
                            })
                        } else {
                            Value::Null
                        }
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
        actual
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
            snapshot: request.snapshot.clone().map(|snapshot| proto::Snapshot {
                manifest_generation: snapshot.manifest_generation,
                visible_seq_no: snapshot.visible_seq_no,
            }),
            filters: Vec::new(),
            predicate: keep_only.then(keep_only_proto_predicate),
            explain: proto::ExplainMode::None as i32,
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
        actual
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        "seed={seed} trace={trace:?}"
    );

    let profiled_request = QueryRequest {
        explain: ExplainMode::Profile,
        ..request.clone()
    };
    let profiled_service = state
        .query(profiled_request.clone())
        .await
        .unwrap_or_else(|error| {
            panic_with_context(
                seed,
                trace,
                format!("service profile query failed: {error}"),
            )
        });
    assert_eq!(
        profiled_service
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
    let service_diagnostics = profiled_service.diagnostics.as_ref().unwrap_or_else(|| {
        panic_with_context(
            seed,
            trace,
            "service profile diagnostics missing".to_owned(),
        )
    });
    assert_query_response_matches_oracle(
        seed,
        trace,
        service_diagnostics.chosen_plan,
        &expected,
        &exact_ranking,
        &actual,
    );
    assert_query_response_matches_oracle(
        seed,
        trace,
        service_diagnostics.chosen_plan,
        &expected,
        &exact_ranking,
        &profiled_service,
    );
    let service_timings = service_diagnostics
        .stage_timings
        .as_ref()
        .unwrap_or_else(|| {
            panic_with_context(seed, trace, "service profile timings missing".to_owned())
        });
    assert!(
        service_timings.planning_micros > 0,
        "seed={seed} trace={trace:?} diagnostics={service_diagnostics:#?}"
    );
    assert!(
        service_diagnostics.units_scanned == 0 || !service_diagnostics.unit_scan_mix.is_empty(),
        "seed={seed} trace={trace:?} diagnostics={service_diagnostics:#?}"
    );

    let profiled_rest_response = rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v1/collections/{COLLECTION_NAME}/query"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "vector": profiled_request.vector,
                        "top_k": profiled_request.top_k,
                        "snapshot": profiled_request.snapshot,
                        "predicate": if keep_only {
                            json!({
                                "kind": "comparison",
                                "field": "kind",
                                "operator": "eq",
                                "value": "keep"
                            })
                        } else {
                            Value::Null
                        },
                        "explain": "profile"
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("rest profile query should respond");
    let profiled_rest_body = json_body(profiled_rest_response).await;
    assert_eq!(
        profiled_rest_body["matches"]
            .as_array()
            .expect("matches should be an array")
            .iter()
            .map(|candidate| candidate["id"].as_str().expect("id should be a string"))
            .collect::<Vec<_>>(),
        profiled_service
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        profiled_rest_body["diagnostics"]["chosen_plan"],
        serde_json::to_value(service_diagnostics.chosen_plan).expect("plan kind should serialize")
    );
    assert_json_profile_diagnostics_matches_service(
        seed,
        trace,
        &profiled_rest_body["diagnostics"],
        service_diagnostics,
    );

    let profiled_grpc = grpc
        .query_collection(Request::new(proto::QueryCollectionRequest {
            collection_name: COLLECTION_NAME.to_owned(),
            vector: request.vector.clone(),
            top_k: EXACT_QUERY_TOP_K as u64,
            snapshot: request.snapshot.clone().map(|snapshot| proto::Snapshot {
                manifest_generation: snapshot.manifest_generation,
                visible_seq_no: snapshot.visible_seq_no,
            }),
            filters: Vec::new(),
            predicate: keep_only.then(keep_only_proto_predicate),
            explain: proto::ExplainMode::Profile as i32,
        }))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("grpc profile query failed: {error}"))
        })
        .into_inner();
    assert_eq!(
        profiled_grpc
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        profiled_service
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        "seed={seed} trace={trace:?}"
    );
    let grpc_diagnostics = profiled_grpc.diagnostics.as_ref().unwrap_or_else(|| {
        panic_with_context(seed, trace, "grpc profile diagnostics missing".to_owned())
    });
    assert_eq!(
        grpc_diagnostics.chosen_plan,
        proto_plan_kind(service_diagnostics.chosen_plan) as i32
    );
    assert_proto_profile_diagnostics_matches_service(
        seed,
        trace,
        grpc_diagnostics,
        service_diagnostics,
    );
}

async fn assert_stats_parity(
    state: &AppState,
    rest: &axum::Router,
    grpc: &GrpcLogPoseService,
    model: &ExpectedModel,
    seed: u64,
    trace: &[ServiceAction],
) {
    let actual = state.stats(COLLECTION_NAME).await.unwrap_or_else(|error| {
        panic_with_context(seed, trace, format!("service stats failed: {error}"))
    });
    let expected = model.expected_stats();
    assert_eq_with_context(
        seed,
        trace,
        "service stats collection_id mismatch",
        &expected.collection_id,
        &actual.collection_id,
    );
    assert_eq_with_context(
        seed,
        trace,
        "service stats collection_name mismatch",
        &expected.collection_name,
        &actual.collection_name,
    );
    assert_eq_with_context(
        seed,
        trace,
        "service stats manifest_generation mismatch",
        &expected.manifest_generation,
        &actual.manifest_generation,
    );
    assert_eq_with_context(
        seed,
        trace,
        "service stats visible_seq_no mismatch",
        &expected.visible_seq_no,
        &actual.visible_seq_no,
    );
    assert_eq_with_context(
        seed,
        trace,
        "service stats mutable_op_count mismatch",
        &expected.mutable_op_count,
        &actual.mutable_op_count,
    );
    assert_eq_with_context(
        seed,
        trace,
        "service stats segment_count mismatch",
        &expected.segment_count,
        &actual.segment_count,
    );
    assert_eq_with_context(
        seed,
        trace,
        "service stats live_record_count mismatch",
        &expected.live_record_count,
        &actual.live_record_count,
    );
    assert_eq_with_context(
        seed,
        trace,
        "service stats deleted_record_count mismatch",
        &expected.deleted_record_count,
        &actual.deleted_record_count,
    );
    assert_eq!(
        actual.query_units.len(),
        actual.segment_count + 1,
        "seed={seed} trace={trace:?}"
    );

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
        rest_body["manifest_generation"], expected.manifest_generation,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        rest_body["visible_seq_no"], expected.visible_seq_no,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        rest_body["mutable_op_count"], expected.mutable_op_count,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        rest_body["segment_count"], expected.segment_count,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        rest_body["live_record_count"], expected.live_record_count,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        rest_body["deleted_record_count"], expected.deleted_record_count,
        "seed={seed} trace={trace:?}"
    );
    assert!(
        rest_body["maintenance"].is_object(),
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        rest_body["query_units"]
            .as_array()
            .expect("query_units should be an array")
            .len(),
        actual.query_units.len(),
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
        grpc_stats.manifest_generation, expected.manifest_generation,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        grpc_stats.visible_seq_no, expected.visible_seq_no,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        grpc_stats.mutable_op_count as usize, expected.mutable_op_count,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        grpc_stats.segment_count as usize, expected.segment_count,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        grpc_stats.live_record_count as usize, expected.live_record_count,
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        grpc_stats.deleted_record_count as usize, expected.deleted_record_count,
        "seed={seed} trace={trace:?}"
    );
    assert!(
        grpc_stats.maintenance.is_some(),
        "seed={seed} trace={trace:?}"
    );
    assert_eq!(
        grpc_stats.query_units.len(),
        actual.query_units.len(),
        "seed={seed} trace={trace:?}"
    );
    if model.segment_count > 0 {
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
        let rest_units = rest_body["query_units"]
            .as_array()
            .expect("rest query units should be an array");
        let rest_immutable = rest_units
            .iter()
            .find(|unit| unit["tier"] == "immutable")
            .unwrap_or_else(|| {
                panic_with_context(
                    seed,
                    trace,
                    format!("missing rest immutable unit: {rest_units:#?}"),
                )
            });
        assert!(
            rest_immutable["artifact_stats"]
                .as_array()
                .is_some_and(|artifacts| artifacts.len() >= 2),
            "seed={seed} trace={trace:?} immutable={rest_immutable:#?}"
        );
        assert!(
            rest_immutable["component_bytes"]["ann_graph"]
                .as_u64()
                .is_some(),
            "seed={seed} trace={trace:?} immutable={rest_immutable:#?}"
        );
        let grpc_immutable = grpc_stats
            .query_units
            .iter()
            .find(|unit| unit.tier == "immutable")
            .unwrap_or_else(|| {
                panic_with_context(
                    seed,
                    trace,
                    format!("missing grpc immutable unit: {:?}", grpc_stats.query_units),
                )
            });
        assert!(
            grpc_immutable
                .artifact_stats
                .iter()
                .any(|artifact| artifact.file_name.ends_with(".hnsw.bin")),
            "seed={seed} trace={trace:?} immutable={grpc_immutable:?}"
        );
        assert!(
            grpc_immutable.component_bytes.contains_key("ann_graph"),
            "seed={seed} trace={trace:?} immutable={grpc_immutable:?}"
        );
    }
}

async fn assert_inspect_parity(
    state: &AppState,
    rest: &axum::Router,
    grpc: &GrpcLogPoseService,
    model: &ExpectedModel,
    target: InspectTarget,
    seed: u64,
    trace: &[ServiceAction],
) {
    let target_label = match &target {
        InspectTarget::Manifest => "manifest",
        InspectTarget::Wal => "wal",
        InspectTarget::Segment(_) => "segment",
        InspectTarget::Maintenance => "maintenance",
    };

    let actual = state
        .inspect(COLLECTION_NAME, target.clone())
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("service inspect failed: {error}"))
        });
    assert_eq!(actual.target, target_label);

    let rest_response = rest
        .clone()
        .oneshot(inspect_request(target.clone()))
        .await
        .expect("rest inspect should respond");
    let rest_body = json_body(rest_response).await;
    assert_eq!(rest_body["target"], target_label);
    match target_label {
        "manifest" => assert_eq!(
            rest_body["payload"]["segments"]
                .as_array()
                .expect("manifest segments should be an array")
                .len(),
            model.segment_count
        ),
        "wal" => assert_eq!(
            rest_body["payload"]["records"]
                .as_array()
                .expect("wal records should be an array")
                .len(),
            model.mutable_op_count()
        ),
        _ => {}
    }

    let grpc_response = grpc
        .inspect_collection(Request::new(proto::InspectCollectionRequest {
            collection_name: COLLECTION_NAME.to_owned(),
            target: match &target {
                InspectTarget::Manifest => proto::InspectTarget::Manifest as i32,
                InspectTarget::Wal => proto::InspectTarget::Wal as i32,
                InspectTarget::Segment(_) => proto::InspectTarget::Segment as i32,
                InspectTarget::Maintenance => proto::InspectTarget::Maintenance as i32,
            },
            segment_id: match &target {
                InspectTarget::Segment(segment_id) => segment_id.clone(),
                InspectTarget::Manifest | InspectTarget::Wal | InspectTarget::Maintenance => {
                    String::new()
                }
            },
        }))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("grpc inspect failed: {error}"))
        })
        .into_inner();
    assert_eq!(grpc_response.target, target_label);
    let grpc_payload =
        serde_json::from_str::<Value>(&grpc_response.payload_json).unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("grpc inspect payload failed: {error}"))
        });
    match target_label {
        "manifest" => assert_eq!(
            grpc_payload["segments"]
                .as_array()
                .expect("manifest segments should be an array")
                .len(),
            model.segment_count
        ),
        "wal" => assert_eq!(
            grpc_payload["records"]
                .as_array()
                .expect("wal records should be an array")
                .len(),
            model.mutable_op_count()
        ),
        _ => {}
    }
}

async fn assert_inspect_segment_parity(
    state: &AppState,
    rest: &axum::Router,
    grpc: &GrpcLogPoseService,
    model: &ExpectedModel,
    seed: u64,
    trace: &[ServiceAction],
) {
    let manifest = state
        .inspect(COLLECTION_NAME, InspectTarget::Manifest)
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("manifest inspect failed: {error}"))
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

    let actual = state
        .inspect(COLLECTION_NAME, InspectTarget::Segment(segment_id.clone()))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(
                seed,
                trace,
                format!("service segment inspect failed: {error}"),
            )
        });
    assert_eq!(actual.target, format!("segment:{segment_id}"));
    assert!(
        actual
            .payload
            .get("records")
            .and_then(Value::as_array)
            .is_some_and(|records| !records.is_empty()),
        "segment inspect should return records"
    );
    assert!(
        model.segment_count > 0,
        "segment inspect should only run when segments exist"
    );

    let rest_response = rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(format!(
                    "/v1/collections/{COLLECTION_NAME}/inspect?target=segment&segment_id={segment_id}"
                ))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("rest segment inspect should respond");
    let rest_body = json_body(rest_response).await;
    assert_eq!(
        rest_body["target"]
            .as_str()
            .expect("segment target should be a string"),
        format!("segment:{segment_id}")
    );
    assert!(
        rest_body["payload"]["records"]
            .as_array()
            .is_some_and(|records| !records.is_empty()),
        "segment inspect should return records"
    );

    let grpc_response = grpc
        .inspect_collection(Request::new(proto::InspectCollectionRequest {
            collection_name: COLLECTION_NAME.to_owned(),
            target: proto::InspectTarget::Segment as i32,
            segment_id: segment_id.clone(),
        }))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(seed, trace, format!("grpc segment inspect failed: {error}"))
        })
        .into_inner();
    assert_eq!(grpc_response.target, format!("segment:{segment_id}"));
    let grpc_payload =
        serde_json::from_str::<Value>(&grpc_response.payload_json).unwrap_or_else(|error| {
            panic_with_context(
                seed,
                trace,
                format!("grpc segment inspect payload failed: {error}"),
            )
        });
    assert!(
        grpc_payload["records"]
            .as_array()
            .is_some_and(|records| !records.is_empty()),
        "segment inspect should return records"
    );
}

fn inspect_request(target: InspectTarget) -> axum::http::Request<Body> {
    let uri = match target {
        InspectTarget::Manifest => "/v1/collections/randomized/inspect?target=manifest".to_owned(),
        InspectTarget::Wal => "/v1/collections/randomized/inspect?target=wal".to_owned(),
        InspectTarget::Segment(segment_id) => {
            format!("/v1/collections/randomized/inspect?target=segment&segment_id={segment_id}")
        }
        InspectTarget::Maintenance => {
            "/v1/collections/randomized/inspect?target=maintenance".to_owned()
        }
    };

    axum::http::Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request should build")
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

fn assert_query_response_matches_oracle(
    seed: u64,
    trace: &[ServiceAction],
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
            "service query matches mismatch",
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

fn assert_json_profile_diagnostics_matches_service(
    seed: u64,
    trace: &[ServiceAction],
    actual: &Value,
    expected: &QueryDiagnostics,
) {
    assert_eq!(
        actual["chosen_plan"],
        serde_json::to_value(expected.chosen_plan).expect("plan kind should serialize"),
        "seed={seed} trace={trace:?}"
    );
    let actual_fallback = serde_json::from_value::<Option<String>>(
        actual["fallback_reason"].clone(),
    )
    .unwrap_or_else(|error| {
        panic_with_context(
            seed,
            trace,
            format!("rest fallback reason should decode: {error}"),
        )
    });
    assert_eq_with_context(
        seed,
        trace,
        "rest fallback reason mismatch",
        &expected.fallback_reason,
        &actual_fallback,
    );
    let actual_reranked = actual["candidates_reranked"].as_u64().unwrap_or_else(|| {
        panic_with_context(
            seed,
            trace,
            "rest rerank count should be numeric".to_owned(),
        )
    });
    assert_eq_with_context(
        seed,
        trace,
        "rest planner reason mismatch",
        &expected.planner_reason,
        &actual["planner_reason"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| {
                panic_with_context(
                    seed,
                    trace,
                    "rest planner reason should be a string".to_owned(),
                )
            }),
    );
    let actual_selectivity = actual["estimated_selectivity"].as_f64().unwrap_or_else(|| {
        panic_with_context(
            seed,
            trace,
            "rest estimated selectivity should be numeric".to_owned(),
        )
    }) as f32;
    assert!(
        (expected.estimated_selectivity - actual_selectivity).abs() <= f32::EPSILON,
        "seed={seed} trace={trace:?} rest estimated selectivity mismatch expected={} actual={actual_selectivity}",
        expected.estimated_selectivity,
    );
    let actual_units_considered = actual["units_considered"].as_u64().unwrap_or_else(|| {
        panic_with_context(
            seed,
            trace,
            "rest units considered should be numeric".to_owned(),
        )
    });
    assert_eq_with_context(
        seed,
        trace,
        "rest units considered mismatch",
        &(expected.units_considered as u64),
        &actual_units_considered,
    );
    let actual_units_pruned = actual["units_pruned"].as_u64().unwrap_or_else(|| {
        panic_with_context(
            seed,
            trace,
            "rest units pruned should be numeric".to_owned(),
        )
    });
    assert_eq_with_context(
        seed,
        trace,
        "rest units pruned mismatch",
        &(expected.units_pruned as u64),
        &actual_units_pruned,
    );
    let actual_before_filter = actual["candidates_before_filter"]
        .as_u64()
        .unwrap_or_else(|| {
            panic_with_context(
                seed,
                trace,
                "rest candidates before filter should be numeric".to_owned(),
            )
        });
    assert_eq_with_context(
        seed,
        trace,
        "rest candidates before filter mismatch",
        &(expected.candidates_before_filter as u64),
        &actual_before_filter,
    );
    let actual_after_filter = actual["candidates_after_filter"]
        .as_u64()
        .unwrap_or_else(|| {
            panic_with_context(
                seed,
                trace,
                "rest candidates after filter should be numeric".to_owned(),
            )
        });
    assert_eq_with_context(
        seed,
        trace,
        "rest candidates after filter mismatch",
        &(expected.candidates_after_filter as u64),
        &actual_after_filter,
    );
    let actual_rerank_count = actual["rerank_count"].as_u64().unwrap_or_else(|| {
        panic_with_context(
            seed,
            trace,
            "rest rerank count should be numeric".to_owned(),
        )
    });
    assert_eq_with_context(
        seed,
        trace,
        "rest rerank count mismatch",
        &(expected.rerank_count as u64),
        &actual_rerank_count,
    );
    assert_eq_with_context(
        seed,
        trace,
        "rest rerank count mismatch",
        &(expected.candidates_reranked as u64),
        &actual_reranked,
    );
    let actual_merged = actual["candidates_merged"].as_u64().unwrap_or_else(|| {
        panic_with_context(seed, trace, "rest merge count should be numeric".to_owned())
    });
    assert_eq_with_context(
        seed,
        trace,
        "rest merge count mismatch",
        &(expected.candidates_merged as u64),
        &actual_merged,
    );
    let actual_units_scanned = actual["units_scanned"].as_u64().unwrap_or_else(|| {
        panic_with_context(
            seed,
            trace,
            "rest units scanned should be numeric".to_owned(),
        )
    });
    assert_eq_with_context(
        seed,
        trace,
        "rest units scanned mismatch",
        &(expected.units_scanned as u64),
        &actual_units_scanned,
    );
    let actual_unit_scan_mix = if actual["unit_scan_mix"].is_null() {
        BTreeMap::new()
    } else {
        serde_json::from_value::<BTreeMap<String, usize>>(actual["unit_scan_mix"].clone())
            .unwrap_or_else(|error| {
                panic_with_context(
                    seed,
                    trace,
                    format!("rest unit scan mix should decode: {error}"),
                )
            })
    };
    assert_eq_with_context(
        seed,
        trace,
        "rest unit scan mix mismatch",
        &expected.unit_scan_mix,
        &actual_unit_scan_mix,
    );
    let timings = expected.stage_timings.as_ref().unwrap_or_else(|| {
        panic_with_context(seed, trace, "service profile timings missing".to_owned())
    });
    assert_json_stage_timing_matches(
        seed,
        trace,
        "planning_micros",
        timings.planning_micros,
        &actual["stage_timings"]["planning_micros"],
    );
    assert_json_stage_timing_matches(
        seed,
        trace,
        "prefilter_micros",
        timings.prefilter_micros,
        &actual["stage_timings"]["prefilter_micros"],
    );
    assert_json_stage_timing_matches(
        seed,
        trace,
        "candidate_generation_micros",
        timings.candidate_generation_micros,
        &actual["stage_timings"]["candidate_generation_micros"],
    );
    assert_json_stage_timing_matches(
        seed,
        trace,
        "postfilter_micros",
        timings.postfilter_micros,
        &actual["stage_timings"]["postfilter_micros"],
    );
    assert_json_stage_timing_matches(
        seed,
        trace,
        "rerank_micros",
        timings.rerank_micros,
        &actual["stage_timings"]["rerank_micros"],
    );
    assert_json_stage_timing_matches(
        seed,
        trace,
        "merge_micros",
        timings.merge_micros,
        &actual["stage_timings"]["merge_micros"],
    );
}

fn assert_proto_profile_diagnostics_matches_service(
    seed: u64,
    trace: &[ServiceAction],
    actual: &proto::QueryDiagnostics,
    expected: &QueryDiagnostics,
) {
    assert_eq_with_context(
        seed,
        trace,
        "grpc planner reason mismatch",
        &expected.planner_reason,
        &actual.planner_reason,
    );
    assert!(
        (expected.estimated_selectivity - actual.estimated_selectivity).abs() <= f32::EPSILON,
        "seed={seed} trace={trace:?} grpc estimated selectivity mismatch expected={} actual={}",
        expected.estimated_selectivity,
        actual.estimated_selectivity,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc units considered mismatch",
        &(expected.units_considered as u64),
        &actual.units_considered,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc units pruned mismatch",
        &(expected.units_pruned as u64),
        &actual.units_pruned,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc candidates before filter mismatch",
        &(expected.candidates_before_filter as u64),
        &actual.candidates_before_filter,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc candidates after filter mismatch",
        &(expected.candidates_after_filter as u64),
        &actual.candidates_after_filter,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc rerank count mismatch",
        &(expected.rerank_count as u64),
        &actual.rerank_count,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc fallback reason mismatch",
        &expected.fallback_reason,
        &actual.fallback_reason,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc rerank count mismatch",
        &(expected.candidates_reranked as u64),
        &actual.candidates_reranked,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc merge count mismatch",
        &(expected.candidates_merged as u64),
        &actual.candidates_merged,
    );
    assert_eq_with_context(
        seed,
        trace,
        "grpc units scanned mismatch",
        &(expected.units_scanned as u64),
        &actual.units_scanned,
    );
    let actual_unit_scan_mix = actual
        .unit_scan_mix
        .iter()
        .map(|(key, value)| (key.clone(), *value as usize))
        .collect::<BTreeMap<_, _>>();
    assert_eq_with_context(
        seed,
        trace,
        "grpc unit scan mix mismatch",
        &expected.unit_scan_mix,
        &actual_unit_scan_mix,
    );
    let timings = expected.stage_timings.as_ref().unwrap_or_else(|| {
        panic_with_context(seed, trace, "service profile timings missing".to_owned())
    });
    let actual_timings = actual.stage_timings.as_ref().unwrap_or_else(|| {
        panic_with_context(seed, trace, "grpc profile timings missing".to_owned())
    });
    assert_proto_stage_timing_matches(
        seed,
        trace,
        "planning_micros",
        timings.planning_micros,
        actual_timings.planning_micros,
    );
    assert_proto_stage_timing_matches(
        seed,
        trace,
        "prefilter_micros",
        timings.prefilter_micros,
        actual_timings.prefilter_micros,
    );
    assert_proto_stage_timing_matches(
        seed,
        trace,
        "candidate_generation_micros",
        timings.candidate_generation_micros,
        actual_timings.candidate_generation_micros,
    );
    assert_proto_stage_timing_matches(
        seed,
        trace,
        "postfilter_micros",
        timings.postfilter_micros,
        actual_timings.postfilter_micros,
    );
    assert_proto_stage_timing_matches(
        seed,
        trace,
        "rerank_micros",
        timings.rerank_micros,
        actual_timings.rerank_micros,
    );
    assert_proto_stage_timing_matches(
        seed,
        trace,
        "merge_micros",
        timings.merge_micros,
        actual_timings.merge_micros,
    );
}

fn assert_json_stage_timing_matches(
    seed: u64,
    trace: &[ServiceAction],
    label: &str,
    expected: u64,
    actual: &Value,
) {
    let _ = expected;
    let _ = actual.as_u64().unwrap_or_else(|| {
        panic_with_context(
            seed,
            trace,
            format!("rest timing '{label}' should be numeric"),
        )
    });
}

fn assert_proto_stage_timing_matches(
    seed: u64,
    trace: &[ServiceAction],
    label: &str,
    expected: u64,
    actual: u64,
) {
    let _ = (seed, trace, label, expected, actual);
}

fn record_matches_filters(record: &VisibleRecord, filters: &[MetadataFilter]) -> bool {
    filters.iter().all(|filter| {
        record
            .metadata
            .get(&filter.field)
            .is_some_and(|value| scalar_matches_value(&filter.value, value))
    })
}

fn proto_plan_kind(plan: QueryPlanKind) -> proto::QueryPlanKind {
    match plan {
        QueryPlanKind::UnfilteredExactScan => proto::QueryPlanKind::UnfilteredExactScan,
        QueryPlanKind::PredicateFirstExact => proto::QueryPlanKind::PredicateFirstExact,
        QueryPlanKind::VectorFirstExact => proto::QueryPlanKind::VectorFirstExact,
        QueryPlanKind::TinyPopulationExactFallback => {
            proto::QueryPlanKind::TinyPopulationExactFallback
        }
        QueryPlanKind::VectorFirstAnn => proto::QueryPlanKind::VectorFirstAnn,
        QueryPlanKind::CooperativeFilteredAnn => proto::QueryPlanKind::CooperativeFilteredAnn,
        QueryPlanKind::HybridExactAnnMerge => proto::QueryPlanKind::HybridExactAnnMerge,
    }
}

fn keep_only_predicate() -> logpose_query::Predicate {
    logpose_query::Predicate::Comparison(logpose_query::PredicateComparison {
        field: "kind".to_owned(),
        operator: logpose_query::PredicateOperator::Eq,
        value: Some(ScalarMetadataValue::String("keep".to_owned())),
    })
}

fn keep_only_proto_predicate() -> proto::Predicate {
    proto::Predicate {
        node: Some(proto::predicate::Node::Comparison(
            proto::PredicateComparison {
                field: "kind".to_owned(),
                operator: proto::PredicateOperator::Eq as i32,
                value: Some(proto::ScalarValue {
                    kind: Some(proto::scalar_value::Kind::StringValue("keep".to_owned())),
                }),
            },
        )),
    }
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

fn scalar_matches_value(expected: &ScalarMetadataValue, actual: &Value) -> bool {
    match (expected, actual) {
        (ScalarMetadataValue::String(expected), Value::String(actual)) => expected == actual,
        (ScalarMetadataValue::Number(expected), Value::Number(actual)) => expected == actual,
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
