use axum::body::Body;
use http_body_util::BodyExt;
use logpose_api_grpc::proto::log_pose_service_server::LogPoseService;
use logpose_api_grpc::{GrpcLogPoseService, proto};
use logpose_auth as _;
use logpose_core::AppState;
use logpose_query::{ExplainMode, QueryRequest};
use logpose_storage::CreateCollectionRequest;
use logpose_types::{DistanceMetric, NodeRole, PutRecord, RecordId, WriteOperation};
use serde as _;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tonic::Request;
use tower::util::ServiceExt;

#[derive(Clone, Debug)]
enum Step {
    ReadStatus,
    CreateCollection(&'static str),
    WriteBatch(&'static str, usize),
    Flush(&'static str),
    ReadPlacement(&'static str),
    ReadData(&'static str),
    ExpectWriteRejected(&'static str),
    Restart,
    RestartAs(NodeRole),
    RestartAsNamed(&'static str, NodeRole),
}

#[derive(Debug)]
struct ExpectedCollection {
    assigned_node: String,
    assigned_role: NodeRole,
    next_record_index: usize,
    record_ids: Vec<String>,
    mutable_op_count: usize,
    segment_count: usize,
}

#[derive(Debug)]
struct ExpectedModel {
    node_name: String,
    current_role: NodeRole,
    collections: BTreeMap<String, ExpectedCollection>,
}

impl ExpectedModel {
    fn new(node_name: &str, current_role: NodeRole) -> Self {
        Self {
            node_name: node_name.to_owned(),
            current_role,
            collections: BTreeMap::new(),
        }
    }

    fn register_collection(&mut self, collection_name: &str) {
        let collection_name = canonical_collection_name(collection_name);
        self.collections
            .entry(collection_name.clone())
            .or_insert_with(|| ExpectedCollection {
                assigned_node: self.node_name.clone(),
                assigned_role: NodeRole::Data,
                next_record_index: 0,
                record_ids: Vec::new(),
                mutable_op_count: 0,
                segment_count: 0,
            });
    }

    fn set_role(&mut self, role: NodeRole) {
        self.current_role = role;
    }

    fn set_identity(&mut self, node_name: &str, role: NodeRole) {
        self.node_name = node_name.to_owned();
        self.current_role = role;
    }

    fn record_write_batch(
        &mut self,
        scenario_name: &str,
        collection_name: &str,
        records: usize,
    ) -> Vec<WriteOperation> {
        let collection_name = canonical_collection_name(collection_name);
        let collection = self
            .collections
            .get_mut(&collection_name)
            .expect("collection should be registered before writes");
        let start_index = collection.next_record_index;
        collection.next_record_index += records;
        collection.mutable_op_count += records;

        (0..records)
            .map(|offset| {
                let index = start_index + offset;
                let record_id = format!("{collection_name}-{index}");
                collection.record_ids.push(record_id.clone());
                WriteOperation::Put(PutRecord {
                    id: RecordId::new(record_id),
                    vector: vec![index as f32 + 1.0, 0.0],
                    metadata: json!({"scenario": scenario_name, "index": index}),
                })
            })
            .collect()
    }

    fn record_flush(&mut self, collection_name: &str) {
        let collection_name = canonical_collection_name(collection_name);
        let collection = self
            .collections
            .get_mut(&collection_name)
            .expect("collection should be registered before flush");
        if collection.mutable_op_count > 0 {
            collection.segment_count += 1;
            collection.mutable_op_count = 0;
        }
    }

    fn collection_names(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    fn contains_collection(&self, collection_name: &str) -> bool {
        self.collections
            .contains_key(&canonical_collection_name(collection_name))
    }

    fn control_plane_ready(&self) -> bool {
        matches!(self.current_role, NodeRole::Combined | NodeRole::Control)
    }

    fn data_plane_ready(&self) -> bool {
        matches!(self.current_role, NodeRole::Combined | NodeRole::Data)
    }

    fn expected_route_kind(&self, collection_name: &str) -> &'static str {
        if self.serves_local_assignment(collection_name) {
            "local"
        } else {
            "recorded"
        }
    }

    fn expected_route_reason_fragment(&self, collection_name: &str) -> &'static str {
        let collection = self.collection(collection_name);
        match (
            self.serves_local_assignment(collection_name),
            collection.assigned_node == self.node_name,
            self.current_role,
        ) {
            (true, _, NodeRole::Combined) => "single-node",
            (true, _, NodeRole::Data) => "data-plane assignment",
            (true, _, _) => "persisted local",
            (false, true, NodeRole::Control) => "control-only",
            (false, true, _) => "persisted local",
            (false, false, _) => "targets node",
        }
    }

    fn local_collection_count(&self) -> usize {
        self.collection_names()
            .into_iter()
            .filter(|collection_name| self.expected_route_kind(collection_name) == "local")
            .count()
    }

    fn expected_query_ids(&self, collection_name: &str) -> Vec<String> {
        self.collection(collection_name)
            .record_ids
            .iter()
            .rev()
            .cloned()
            .collect()
    }

    fn expected_record_count(&self, collection_name: &str) -> usize {
        self.collection(collection_name).record_ids.len()
    }

    fn expected_mutable_op_count(&self, collection_name: &str) -> usize {
        self.collection(collection_name).mutable_op_count
    }

    fn expected_segment_count(&self, collection_name: &str) -> usize {
        self.collection(collection_name).segment_count
    }

    fn collection(&self, collection_name: &str) -> &ExpectedCollection {
        self.collections
            .get(&canonical_collection_name(collection_name))
            .expect("collection should be tracked by the simulation model")
    }

    fn serves_local_assignment(&self, collection_name: &str) -> bool {
        let collection = self.collection(collection_name);
        collection.assigned_node == self.node_name
            && match collection.assigned_role {
                NodeRole::Combined => self.current_role == NodeRole::Combined,
                NodeRole::Control => {
                    matches!(self.current_role, NodeRole::Combined | NodeRole::Control)
                }
                NodeRole::Data => {
                    matches!(self.current_role, NodeRole::Combined | NodeRole::Data)
                }
            }
    }
}

struct Harness {
    config: logpose_config::LogPoseConfig,
    state: Arc<AppState>,
    rest: axum::Router,
    grpc: GrpcLogPoseService,
}

impl Harness {
    fn new(config: logpose_config::LogPoseConfig) -> Self {
        let state = Arc::new(AppState::new(config.clone()));
        Self {
            rest: logpose_api_rest::router(Arc::clone(&state)),
            grpc: GrpcLogPoseService::new(Arc::clone(&state)),
            state,
            config,
        }
    }

    fn restart(&mut self) {
        *self = Self::new(self.config.clone());
    }

    fn restart_with_role(&mut self, node_role: NodeRole) {
        self.config.node_role = node_role;
        self.restart();
    }

    fn restart_with_identity(&mut self, node_name: &str, node_role: NodeRole) {
        self.config.node_name = node_name.to_owned();
        self.config.node_role = node_role;
        self.restart();
    }
}

pub async fn run_control_plane_scenarios() {
    for (name, steps) in scenarios() {
        run_scenario(name, steps).await;
    }
}

async fn run_scenario(name: &str, steps: Vec<Step>) {
    let config = test_config(name);
    let mut harness = Harness::new(config.clone());
    let mut model = ExpectedModel::new(&config.node_name, config.node_role);
    let mut trace = Vec::new();

    for step in steps {
        trace.push(format!("{step:?}"));
        match step {
            Step::ReadStatus => assert_status_matches(&harness, &model, &trace).await,
            Step::CreateCollection(collection_name) => {
                let (database_name, bare_collection_name) = request_database_parts(collection_name);
                harness
                    .state
                    .control
                    .create_collection(CreateCollectionRequest::in_database(
                        database_name,
                        bare_collection_name,
                        2,
                        DistanceMetric::Dot,
                    ))
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(&trace, format!("create collection failed: {error}"))
                    });
                model.register_collection(collection_name);
            }
            Step::WriteBatch(collection_name, records) => {
                let operations = model.record_write_batch(name, collection_name, records);
                harness
                    .state
                    .write(collection_name, operations)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(&trace, format!("write failed: {error}"))
                    });
            }
            Step::Flush(collection_name) => {
                harness
                    .state
                    .flush(collection_name)
                    .await
                    .unwrap_or_else(|error| {
                        panic_with_context(&trace, format!("flush failed: {error}"))
                    });
                model.record_flush(collection_name);
            }
            Step::ReadPlacement(collection_name) => {
                assert_placement_matches(&harness, &model, collection_name, &trace).await;
            }
            Step::ReadData(collection_name) => {
                assert_data_matches(&harness, &model, collection_name, &trace).await;
            }
            Step::ExpectWriteRejected(collection_name) => {
                let error = harness
                    .state
                    .write(
                        collection_name,
                        vec![WriteOperation::Put(PutRecord {
                            id: RecordId::new(format!("{collection_name}-rejected")),
                            vector: vec![1.0, 0.0],
                            metadata: json!({"scenario": name}),
                        })],
                    )
                    .await
                    .expect_err("data-plane write should be rejected");
                assert!(
                    error.to_string().contains("data-plane operations")
                        || error.to_string().contains("not locally served"),
                    "trace: {trace:?}, error: {error}"
                );
            }
            Step::Restart => harness.restart(),
            Step::RestartAs(node_role) => {
                harness.restart_with_role(node_role);
                model.set_role(node_role);
            }
            Step::RestartAsNamed(node_name, node_role) => {
                harness.restart_with_identity(node_name, node_role);
                model.set_identity(node_name, node_role);
            }
        }
    }
}

async fn assert_status_matches(harness: &Harness, model: &ExpectedModel, trace: &[String]) {
    let service_status = harness
        .state
        .control
        .runtime_status()
        .await
        .unwrap_or_else(|error| {
            panic_with_context(trace, format!("service status failed: {error}"))
        });
    let rest_status = rest_runtime_status(harness)
        .await
        .unwrap_or_else(|error| panic_with_context(trace, error));
    let grpc_status = harness
        .grpc
        .get_runtime_status(Request::new(proto::GetRuntimeStatusRequest {}))
        .await
        .unwrap_or_else(|error| panic_with_context(trace, format!("grpc status failed: {error}")))
        .into_inner();

    let expected_collections = model.collection_names();
    let actual_collections = service_status
        .collections
        .iter()
        .map(|placement| {
            runtime_collection_identity(&placement.database_name, &placement.collection_name)
        })
        .collect::<Vec<_>>();

    assert_eq!(
        service_status.metadata.node_name, model.node_name,
        "trace: {trace:?}"
    );
    assert_eq!(service_status.role, model.current_role, "trace: {trace:?}");
    assert_eq!(
        service_status.collection_count,
        model.local_collection_count(),
        "trace: {trace:?}"
    );
    assert_eq!(actual_collections, expected_collections, "trace: {trace:?}");
    for placement in &service_status.collections {
        assert_eq!(
            placement.assigned_node,
            model.collection(&placement.collection_name).assigned_node,
            "trace: {trace:?}"
        );
        assert_eq!(
            placement.assigned_role.as_str(),
            model
                .collection(&placement.collection_name)
                .assigned_role
                .as_str(),
            "trace: {trace:?}"
        );
        assert_eq!(
            placement.route_kind,
            model.expected_route_kind(&placement.collection_name),
            "trace: {trace:?}"
        );
        assert!(
            placement
                .route_reason
                .contains(model.expected_route_reason_fragment(&placement.collection_name)),
            "trace: {trace:?}"
        );
    }
    assert_eq!(
        service_status.rest_endpoint,
        format!(
            "http://{}:{}",
            harness.config.rest_host, harness.config.rest_port
        ),
        "trace: {trace:?}"
    );
    assert_eq!(
        service_status.grpc_endpoint,
        format!(
            "http://{}:{}",
            harness.config.grpc_host, harness.config.grpc_port
        ),
        "trace: {trace:?}"
    );

    assert_eq!(
        rest_status["role"],
        model.current_role.as_str(),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_status["control_plane_ready"],
        Value::Bool(model.control_plane_ready()),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_status["data_plane_ready"],
        Value::Bool(model.data_plane_ready()),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_status["collection_count"].as_u64(),
        Some(model.local_collection_count() as u64),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_status["collections"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        Some(runtime_collection_identity(
                            item["database_name"].as_str()?,
                            item["collection_name"].as_str()?,
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        expected_collections,
        "trace: {trace:?}"
    );
    if let Some(items) = rest_status["collections"].as_array() {
        for item in items {
            let collection_name = runtime_collection_identity(
                item["database_name"]
                    .as_str()
                    .expect("database_name should be present"),
                item["collection_name"]
                    .as_str()
                    .expect("collection_name should be present"),
            );
            assert!(item.get("tenant_name").is_none(), "trace: {trace:?}");
            assert_eq!(
                item["assigned_node"],
                model.collection(&collection_name).assigned_node,
                "trace: {trace:?}"
            );
            assert_eq!(
                item["assigned_role"],
                model.collection(&collection_name).assigned_role.as_str(),
                "trace: {trace:?}"
            );
            assert_eq!(
                item["route_kind"],
                model.expected_route_kind(&collection_name),
                "trace: {trace:?}"
            );
            assert!(
                item["route_reason"].as_str().is_some_and(|reason| {
                    reason.contains(model.expected_route_reason_fragment(&collection_name))
                }),
                "trace: {trace:?}"
            );
        }
    }
    assert_eq!(
        rest_status["maintenance"]["pending_operations"].as_u64(),
        Some(0),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_status["maintenance"]["collections_with_pending"].as_u64(),
        Some(0),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_status["maintenance"]["collections_in_progress"].as_u64(),
        Some(0),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_status["maintenance"]["collections_with_errors"].as_u64(),
        Some(0),
        "trace: {trace:?}"
    );

    assert_eq!(
        grpc_status.role,
        proto_node_role(model.current_role) as i32,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_status.control_plane_ready,
        model.control_plane_ready(),
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_status.data_plane_ready,
        model.data_plane_ready(),
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_status.collection_count,
        model.local_collection_count() as u64,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_status
            .collections
            .iter()
            .map(|placement| {
                runtime_collection_identity(&placement.database_name, &placement.collection_name)
            })
            .collect::<Vec<_>>(),
        expected_collections,
        "trace: {trace:?}"
    );
    for placement in &grpc_status.collections {
        let collection_name =
            runtime_collection_identity(&placement.database_name, &placement.collection_name);
        assert_eq!(
            placement.assigned_node,
            model.collection(&collection_name).assigned_node,
            "trace: {trace:?}"
        );
        assert_eq!(
            placement.assigned_role,
            proto_node_role(model.collection(&collection_name).assigned_role) as i32,
            "trace: {trace:?}"
        );
        assert_eq!(
            placement.route_kind,
            model.expected_route_kind(&collection_name),
            "trace: {trace:?}"
        );
        assert!(
            placement
                .route_reason
                .contains(model.expected_route_reason_fragment(&collection_name)),
            "trace: {trace:?}"
        );
    }
    let grpc_maintenance = grpc_status
        .maintenance
        .as_ref()
        .expect("maintenance should be present");
    assert_eq!(grpc_maintenance.pending_operations, 0, "trace: {trace:?}");
    assert_eq!(
        grpc_maintenance.collections_with_pending, 0,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_maintenance.collections_in_progress, 0,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_maintenance.collections_with_errors, 0,
        "trace: {trace:?}"
    );
}

async fn assert_placement_matches(
    harness: &Harness,
    model: &ExpectedModel,
    collection_name: &str,
    trace: &[String],
) {
    let service_placement = harness
        .state
        .control
        .collection_placement(collection_name)
        .await
        .unwrap_or_else(|error| {
            panic_with_context(trace, format!("service placement failed: {error}"))
        });
    let rest_placement = rest_collection_placement(harness, collection_name)
        .await
        .unwrap_or_else(|error| panic_with_context(trace, error));
    let (database_name, bare_collection_name) = request_database_parts(collection_name);
    let grpc_placement = harness
        .grpc
        .get_collection_placement(Request::new(proto::GetCollectionPlacementRequest {
            collection_name: bare_collection_name.clone(),
            database_name: database_name.clone(),
        }))
        .await
        .unwrap_or_else(|error| {
            panic_with_context(trace, format!("grpc placement failed: {error}"))
        })
        .into_inner();

    assert!(
        model.contains_collection(collection_name),
        "placement requested for unknown collection with trace: {trace:?}"
    );
    assert_eq!(
        service_placement.database_name, database_name,
        "trace: {trace:?}"
    );
    assert_eq!(
        service_placement.collection_name, bare_collection_name,
        "trace: {trace:?}"
    );
    assert_eq!(
        service_placement.assigned_node,
        model.collection(collection_name).assigned_node,
        "trace: {trace:?}"
    );
    assert_eq!(
        service_placement.assigned_role.as_str(),
        model.collection(collection_name).assigned_role.as_str(),
        "trace: {trace:?}"
    );
    assert_eq!(
        service_placement.route_kind,
        model.expected_route_kind(collection_name),
        "trace: {trace:?}"
    );
    assert!(
        service_placement
            .route_reason
            .contains(model.expected_route_reason_fragment(collection_name)),
        "trace: {trace:?}"
    );

    assert!(
        rest_placement.get("tenant_name").is_none(),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_placement["database_name"], database_name,
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_placement["collection_name"], bare_collection_name,
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_placement["assigned_node"],
        model.collection(collection_name).assigned_node,
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_placement["assigned_role"],
        model.collection(collection_name).assigned_role.as_str(),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_placement["route_kind"],
        model.expected_route_kind(collection_name),
        "trace: {trace:?}"
    );
    assert!(
        rest_placement["route_reason"]
            .as_str()
            .is_some_and(|reason| {
                reason.contains(model.expected_route_reason_fragment(collection_name))
            }),
        "trace: {trace:?}"
    );

    assert_eq!(
        grpc_placement.database_name, database_name,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_placement.collection_name, bare_collection_name,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_placement.assigned_node,
        model.collection(collection_name).assigned_node,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_placement.assigned_role,
        proto_node_role(model.collection(collection_name).assigned_role) as i32,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_placement.route_kind,
        model.expected_route_kind(collection_name),
        "trace: {trace:?}"
    );
    assert!(
        grpc_placement
            .route_reason
            .contains(model.expected_route_reason_fragment(collection_name)),
        "trace: {trace:?}"
    );
}

async fn assert_data_matches(
    harness: &Harness,
    model: &ExpectedModel,
    collection_name: &str,
    trace: &[String],
) {
    let expected_ids = model.expected_query_ids(collection_name);
    let expected_record_count = model.expected_record_count(collection_name);
    let expected_mutable_op_count = model.expected_mutable_op_count(collection_name);
    let expected_segment_count = model.expected_segment_count(collection_name);

    let service_stats = harness
        .state
        .stats(collection_name)
        .await
        .unwrap_or_else(|error| {
            panic_with_context(trace, format!("service stats failed: {error}"))
        });
    let rest_stats = rest_collection_stats(harness, collection_name)
        .await
        .unwrap_or_else(|error| panic_with_context(trace, error));
    let (database_name, bare_collection_name) = request_database_parts(collection_name);
    let grpc_stats = harness
        .grpc
        .get_collection_stats(Request::new(proto::GetCollectionStatsRequest {
            collection_name: bare_collection_name,
            database_name,
        }))
        .await
        .unwrap_or_else(|error| panic_with_context(trace, format!("grpc stats failed: {error}")))
        .into_inner();

    assert_eq!(
        service_stats.live_record_count, expected_record_count,
        "trace: {trace:?}"
    );
    assert_eq!(service_stats.deleted_record_count, 0, "trace: {trace:?}");
    assert_eq!(
        service_stats.mutable_op_count, expected_mutable_op_count,
        "trace: {trace:?}"
    );
    assert_eq!(
        service_stats.segment_count, expected_segment_count,
        "trace: {trace:?}"
    );

    assert_eq!(
        rest_stats["live_record_count"].as_u64(),
        Some(expected_record_count as u64),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_stats["deleted_record_count"].as_u64(),
        Some(0),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_stats["mutable_op_count"].as_u64(),
        Some(expected_mutable_op_count as u64),
        "trace: {trace:?}"
    );
    assert_eq!(
        rest_stats["segment_count"].as_u64(),
        Some(expected_segment_count as u64),
        "trace: {trace:?}"
    );

    assert_eq!(
        grpc_stats.live_record_count, expected_record_count as u64,
        "trace: {trace:?}"
    );
    assert_eq!(grpc_stats.deleted_record_count, 0, "trace: {trace:?}");
    assert_eq!(
        grpc_stats.mutable_op_count, expected_mutable_op_count as u64,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_stats.segment_count, expected_segment_count as u64,
        "trace: {trace:?}"
    );

    if expected_record_count == 0 {
        return;
    }

    let service_query = harness
        .state
        .query(QueryRequest {
            collection_name: collection_name.to_owned(),
            vector: vec![1.0, 0.0],
            top_k: expected_record_count,
            snapshot: None,
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
        })
        .await
        .unwrap_or_else(|error| {
            panic_with_context(trace, format!("service query failed: {error}"))
        });
    let rest_query = rest_collection_query(harness, collection_name, expected_record_count)
        .await
        .unwrap_or_else(|error| panic_with_context(trace, error));
    let (database_name, bare_collection_name) = request_database_parts(collection_name);
    let grpc_query = harness
        .grpc
        .query_collection(Request::new(proto::QueryCollectionRequest {
            collection_name: bare_collection_name,
            vector: vec![1.0, 0.0],
            top_k: expected_record_count as u64,
            snapshot: None,
            filters: Vec::new(),
            predicate: None,
            explain: proto::ExplainMode::None as i32,
            database_name,
        }))
        .await
        .unwrap_or_else(|error| panic_with_context(trace, format!("grpc query failed: {error}")))
        .into_inner();

    assert_eq!(
        service_query.returned, expected_record_count,
        "trace: {trace:?}"
    );
    assert_eq!(
        service_query
            .matches
            .iter()
            .map(|candidate| candidate.id.to_string())
            .collect::<Vec<_>>(),
        expected_ids,
        "trace: {trace:?}"
    );
    assert_eq!(
        query_match_ids_from_json(&rest_query),
        expected_ids,
        "trace: {trace:?}"
    );
    assert_eq!(
        grpc_query
            .matches
            .iter()
            .map(|candidate| candidate.id.clone())
            .collect::<Vec<_>>(),
        expected_ids,
        "trace: {trace:?}"
    );
}

async fn rest_runtime_status(harness: &Harness) -> Result<Value, String> {
    let response = harness
        .rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/v1/runtime/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .map_err(|error| format!("rest status request failed: {error}"))?;
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("rest status body failed: {error}"))?
        .to_bytes();
    serde_json::from_slice(&body).map_err(|error| format!("rest status json failed: {error}"))
}

async fn rest_collection_placement(
    harness: &Harness,
    collection_name: &str,
) -> Result<Value, String> {
    let (database_name, bare_collection_name) = request_database_parts(collection_name);
    let response = harness
        .rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(format!(
                    "/v1/collections/{}/placement?database={}",
                    encode_collection_path_segment(&bare_collection_name),
                    encode_collection_query_value(&database_name)
                ))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .map_err(|error| format!("rest placement request failed: {error}"))?;
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("rest placement body failed: {error}"))?
        .to_bytes();
    serde_json::from_slice(&body).map_err(|error| format!("rest placement json failed: {error}"))
}

async fn rest_collection_stats(harness: &Harness, collection_name: &str) -> Result<Value, String> {
    let (database_name, bare_collection_name) = request_database_parts(collection_name);
    let response = harness
        .rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(format!(
                    "/v1/collections/{}/stats?database={}",
                    encode_collection_path_segment(&bare_collection_name),
                    encode_collection_query_value(&database_name)
                ))
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .map_err(|error| format!("rest stats request failed: {error}"))?;
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("rest stats body failed: {error}"))?
        .to_bytes();
    serde_json::from_slice(&body).map_err(|error| format!("rest stats json failed: {error}"))
}

async fn rest_collection_query(
    harness: &Harness,
    collection_name: &str,
    top_k: usize,
) -> Result<Value, String> {
    let (database_name, bare_collection_name) = request_database_parts(collection_name);
    let response = harness
        .rest
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!(
                    "/v1/collections/{}/query",
                    encode_collection_path_segment(&bare_collection_name)
                ))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "database_name": database_name,
                        "vector": [1.0, 0.0],
                        "top_k": top_k
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .map_err(|error| format!("rest query request failed: {error}"))?;
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("rest query body failed: {error}"))?
        .to_bytes();
    serde_json::from_slice(&body).map_err(|error| format!("rest query json failed: {error}"))
}

fn query_match_ids_from_json(response: &Value) -> Vec<String> {
    response["matches"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item["id"].as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn scenarios() -> Vec<(&'static str, Vec<Step>)> {
    vec![
        (
            "phase5-control-restart",
            vec![
                Step::ReadStatus,
                Step::CreateCollection("documents"),
                Step::ReadStatus,
                Step::ReadPlacement("documents"),
                Step::WriteBatch("documents", 3),
                Step::ReadData("documents"),
                Step::Flush("documents"),
                Step::ReadData("documents"),
                Step::ReadStatus,
                Step::Restart,
                Step::ReadStatus,
                Step::ReadPlacement("documents"),
                Step::ReadData("documents"),
            ],
        ),
        (
            "phase5-control-only-recorded-route",
            vec![
                Step::CreateCollection("documents"),
                Step::WriteBatch("documents", 1),
                Step::ReadStatus,
                Step::RestartAs(NodeRole::Control),
                Step::ReadStatus,
                Step::ReadPlacement("documents"),
                Step::ExpectWriteRejected("documents"),
            ],
        ),
        (
            "phase5-node-name-recorded-route",
            vec![
                Step::CreateCollection("documents"),
                Step::WriteBatch("documents", 1),
                Step::ReadStatus,
                Step::RestartAsNamed("phase5-node-name-recorded-route-moved", NodeRole::Combined),
                Step::ReadStatus,
                Step::ReadPlacement("documents"),
                Step::ExpectWriteRejected("documents"),
            ],
        ),
        (
            "phase5-multi-collection",
            vec![
                Step::CreateCollection("events"),
                Step::CreateCollection("metrics"),
                Step::WriteBatch("events", 2),
                Step::WriteBatch("metrics", 1),
                Step::ReadData("events"),
                Step::ReadData("metrics"),
                Step::ReadStatus,
                Step::ReadPlacement("metrics"),
                Step::Restart,
                Step::ReadStatus,
                Step::ReadPlacement("events"),
                Step::ReadPlacement("metrics"),
                Step::ReadData("events"),
                Step::ReadData("metrics"),
            ],
        ),
        (
            "phase5-namespace-collision",
            vec![
                Step::CreateCollection("documents"),
                Step::CreateCollection("analytics/documents"),
                Step::WriteBatch("documents", 1),
                Step::WriteBatch("analytics/documents", 1),
                Step::ReadStatus,
                Step::ReadPlacement("documents"),
                Step::ReadPlacement("analytics/documents"),
                Step::ReadData("documents"),
                Step::ReadData("analytics/documents"),
                Step::Restart,
                Step::ReadStatus,
                Step::ReadPlacement("documents"),
                Step::ReadPlacement("analytics/documents"),
                Step::ReadData("documents"),
                Step::ReadData("analytics/documents"),
            ],
        ),
    ]
}

fn canonical_collection_name(collection_name: &str) -> String {
    let mut parts = collection_name.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("default"), Some(name), None) => name.to_owned(),
        _ => collection_name.to_owned(),
    }
}

fn request_database_parts(collection_name: &str) -> (String, String) {
    let mut parts = collection_name.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(database_name), Some(name), None) => (database_name.to_owned(), name.to_owned()),
        _ => ("default".to_owned(), collection_name.to_owned()),
    }
}

fn runtime_collection_identity(database_name: &str, collection_name: &str) -> String {
    canonical_collection_name(&format!("{database_name}/{collection_name}"))
}

fn encode_collection_path_segment(collection_name: &str) -> String {
    collection_name
        .replace('%', "%25")
        .replace('/', "%2F")
        .replace(' ', "%20")
}

fn encode_collection_query_value(value: &str) -> String {
    value.replace('%', "%25").replace(' ', "%20")
}

#[allow(clippy::panic)]
fn panic_with_context(trace: &[String], message: String) -> ! {
    panic!("{message}\ntrace:\n{}", trace.join("\n"));
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-control-sim-{label}-{suffix}"));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}

fn test_config(label: &str) -> logpose_config::LogPoseConfig {
    logpose_config::LogPoseConfig {
        node_name: label.to_owned(),
        storage_root: unique_temp_dir(label),
        ..logpose_config::LogPoseConfig::default()
    }
}

fn proto_node_role(role: NodeRole) -> proto::NodeRole {
    match role {
        NodeRole::Combined => proto::NodeRole::Combined,
        NodeRole::Control => proto::NodeRole::Control,
        NodeRole::Data => proto::NodeRole::Data,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_database_parts_defaults_database_for_bare_collection() {
        let (database_name, collection_name) = request_database_parts("documents");

        assert_eq!(database_name, "default");
        assert_eq!(collection_name, "documents");
    }

    #[test]
    fn request_database_parts_preserves_explicit_database() {
        let (database_name, collection_name) = request_database_parts("analytics/documents");

        assert_eq!(database_name, "analytics");
        assert_eq!(collection_name, "documents");
    }

    #[test]
    fn runtime_collection_identity_collapses_default_database_prefix() {
        assert_eq!(canonical_collection_name("documents"), "documents");
        assert_eq!(canonical_collection_name("default/documents"), "documents");
        assert_eq!(
            runtime_collection_identity("default", "documents"),
            "documents"
        );
        assert_eq!(
            runtime_collection_identity("analytics", "documents"),
            "analytics/documents"
        );
    }
}
