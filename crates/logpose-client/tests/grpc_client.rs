//! Integration tests for the gRPC-backed LogPose client.

use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy as AuthDatabaseAccessPolicy, DatabaseRole,
    DatabaseRoleBinding as AuthDatabaseRoleBinding, Principal, PrincipalKind,
};
use logpose_catalog as _;
use logpose_client::{
    ClientError, CreateCollectionRequest, DatabaseAccessPolicy, DatabaseDescriptor,
    DatabaseRoleBinding, LogPoseClient,
};
use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
use logpose_core::AppState;
use logpose_query::{
    ExplainMode, Predicate, PredicateComparison, PredicateOperator, QueryPlanKind, QueryRequest,
    ScalarMetadataValue,
};
use logpose_storage::{CreateCollectionRequest as StorageCreateCollectionRequest, InspectTarget};
use logpose_types::{DeleteRecord, DistanceMetric, PutRecord, RecordId, WriteOperation};
use serde as _;
use serde_json::{Value, json};
use std::{
    fs,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror as _;
use tonic as _;

#[tokio::test]
async fn grpc_client_runs_metadata_and_collection_workflows() {
    let temp_root = unique_temp_dir("client-grpc");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config(&temp_root, rest_addr, grpc_addr)));

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let endpoint = format!("http://{grpc_addr}");
    let client = LogPoseClient::connect(endpoint.clone())
        .await
        .expect("client should connect");

    let metadata = client.metadata().await.expect("metadata should load");
    assert_eq!(metadata.product, "LogPose");
    assert_eq!(metadata.node_name, "client-grpc");
    assert_eq!(metadata.profile, "debug");
    assert!(!metadata.version.is_empty(), "version should be non-empty");
    assert!(!metadata.git_sha.is_empty(), "git sha should be non-empty");

    let descriptor = client
        .create_collection(CreateCollectionRequest::in_database(
            "default",
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");
    let qualified = descriptor.lookup_name();
    assert_eq!(descriptor.database_name, "default");
    assert_eq!(descriptor.name, "documents");

    let read_back = client
        .get_collection(&qualified)
        .await
        .expect("collection should load");
    assert_eq!(read_back.database_name, "default");
    assert_eq!(read_back.collection_id, descriptor.collection_id);

    client
        .write(
            &qualified,
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep","color":"red"}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.5, 0.0],
                    metadata: json!({"kind":"drop","color":"blue"}),
                }),
            ],
        )
        .await
        .expect("write should succeed");

    let query = client
        .query(QueryRequest {
            collection_name: qualified.clone(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Profile,
        })
        .await
        .expect("query should succeed");
    assert_eq!(query.matches[0].id.as_str(), "alpha");
    assert_eq!(
        query
            .diagnostics
            .as_ref()
            .expect("diagnostics should be present")
            .chosen_plan,
        QueryPlanKind::TinyPopulationExactFallback
    );
    let diagnostics = query
        .diagnostics
        .as_ref()
        .expect("diagnostics should be present");
    assert!(diagnostics.fallback_reason.is_some());
    let timings = diagnostics
        .stage_timings
        .as_ref()
        .expect("profile mode should include timings");
    assert!(timings.planning_micros > 0);
    assert!(timings.prefilter_micros > 0);
    assert!(timings.rerank_micros > 0);
    assert!(diagnostics.candidates_merged >= 1);
    assert!(diagnostics.candidates_reranked >= 1);
    assert_eq!(
        diagnostics.unit_scan_mix.get("mutable_exact").copied(),
        Some(1)
    );

    let stats = client.stats(&qualified).await.expect("stats should load");
    assert_eq!(stats.database_name, "default");
    assert_eq!(stats.collection_name, "documents");
    assert_eq!(stats.live_record_count, 2);
    assert_eq!(stats.deleted_record_count, 0);
    assert_eq!(stats.mutable_op_count, 2);
    assert_eq!(stats.segment_count, 0);
    assert_eq!(stats.maintenance.completed_runs, 0);
    assert_eq!(stats.query_units.len(), 1);
    assert_eq!(stats.query_units[0].artifact_stats.len(), 1);
    assert_eq!(stats.query_units[0].artifact_stats[0].kind, "mutable_delta");
    assert!(
        stats.query_units[0]
            .component_bytes
            .get("mutable_delta")
            .copied()
            .unwrap_or_default()
            > 0
    );

    let flush = client
        .flush(&qualified)
        .await
        .expect("flush should succeed");
    assert!(flush.manifest_generation >= 1);

    client
        .write(
            &qualified,
            vec![WriteOperation::Delete(DeleteRecord {
                id: RecordId::new("beta"),
            })],
        )
        .await
        .expect("delete should succeed");

    let compact = client
        .compact(&qualified)
        .await
        .expect("compact should succeed");
    assert!(compact.manifest_generation >= flush.manifest_generation);

    let stats = client.stats(&qualified).await.expect("stats should reload");
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(stats.deleted_record_count, 1);
    assert_eq!(stats.mutable_op_count, 1);
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.maintenance.completed_runs, 0);
    assert!(stats.maintenance.in_progress.is_none());
    assert_eq!(stats.query_units.len(), 2);
    let immutable = stats
        .query_units
        .iter()
        .find(|unit| unit.tier == "immutable")
        .expect("immutable unit should be present");
    assert_eq!(immutable.index_kind, "hnsw");
    assert!(
        immutable
            .artifact_stats
            .iter()
            .any(|artifact| artifact.file_name.ends_with(".flat.json"))
    );
    assert!(
        immutable
            .artifact_stats
            .iter()
            .any(|artifact| artifact.file_name.ends_with(".hnsw.bin"))
    );
    assert!(
        immutable
            .component_bytes
            .get("ann_graph")
            .copied()
            .unwrap_or_default()
            > 0
    );

    let hybrid_query = client
        .query(QueryRequest {
            collection_name: qualified.clone(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::Profile,
        })
        .await
        .expect("hybrid query should succeed");
    assert_eq!(hybrid_query.matches[0].id.as_str(), "alpha");
    let hybrid_diagnostics = hybrid_query
        .diagnostics
        .as_ref()
        .expect("hybrid query should include diagnostics");
    assert_eq!(
        hybrid_diagnostics.chosen_plan,
        QueryPlanKind::HybridExactAnnMerge
    );
    assert!(hybrid_diagnostics.candidates_merged >= 1);
    assert!(hybrid_diagnostics.candidates_reranked >= 1);
    assert_eq!(
        hybrid_diagnostics
            .unit_scan_mix
            .get("immutable_ann")
            .copied(),
        Some(1)
    );
    assert_eq!(
        hybrid_diagnostics
            .unit_scan_mix
            .get("mutable_exact")
            .copied(),
        Some(1)
    );
    let hybrid_timings = hybrid_diagnostics
        .stage_timings
        .as_ref()
        .expect("hybrid profile should include timings");
    assert!(hybrid_timings.candidate_generation_micros > 0);
    assert!(hybrid_timings.merge_micros > 0);
    assert!(hybrid_timings.rerank_micros > 0);

    let inspect = client
        .inspect(&qualified, InspectTarget::Manifest)
        .await
        .expect("inspect should succeed");
    assert_eq!(inspect.target, "manifest");
    let manifest_segments = inspect
        .payload
        .get("segments")
        .and_then(Value::as_array)
        .expect("manifest segments should be an array");
    assert_eq!(manifest_segments.len(), 1);
    let segment_id = manifest_segments[0]["segment_id"]
        .as_str()
        .expect("segment id should be a string")
        .to_owned();

    let wal = client
        .inspect(&qualified, InspectTarget::Wal)
        .await
        .expect("wal inspect should succeed");
    assert_eq!(wal.target, "wal");
    assert_eq!(
        wal.payload
            .get("records")
            .and_then(Value::as_array)
            .expect("wal records should be an array")
            .len(),
        1
    );

    let segment = client
        .inspect(&qualified, InspectTarget::Segment(segment_id.clone()))
        .await
        .expect("segment inspect should succeed");
    assert_eq!(segment.target, format!("segment:{segment_id}"));
    assert_eq!(
        segment
            .payload
            .get("segment")
            .and_then(Value::as_object)
            .and_then(|segment| segment.get("segment_id"))
            .and_then(Value::as_str),
        Some(segment_id.as_str())
    );

    let maintenance = client
        .inspect(&qualified, InspectTarget::Maintenance)
        .await
        .expect("maintenance inspect should succeed");
    assert_eq!(maintenance.target, "maintenance");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_round_trips_database_policy_over_grpc() {
    let temp_root = unique_temp_dir("client-grpc-policy");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config(&temp_root, rest_addr, grpc_addr)));

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let endpoint = format!("http://{grpc_addr}");
    let client = LogPoseClient::connect(endpoint)
        .await
        .expect("client should connect");

    let policy = DatabaseAccessPolicy {
        database_name: "default".to_owned(),
        authentication_mode: AuthenticationMode::Password,
        role_bindings: vec![
            DatabaseRoleBinding {
                database_name: "default".to_owned(),
                principal_name: "writer".to_owned(),
                role: DatabaseRole::ReadWrite,
            },
            DatabaseRoleBinding {
                database_name: "default".to_owned(),
                principal_name: "reader".to_owned(),
                role: DatabaseRole::ReadOnly,
            },
        ],
    };

    let stored = client
        .set_database_policy(policy.clone())
        .await
        .expect("policy should be written");
    assert_eq!(stored, policy);

    let read_back = client
        .database_policy("default")
        .await
        .expect("policy should be read");
    assert_eq!(read_back, policy);

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_round_trips_database_descriptors_over_grpc() {
    let temp_root = unique_temp_dir("client-grpc-namespace");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config(&temp_root, rest_addr, grpc_addr)));

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let client = LogPoseClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("client should connect");

    let database = client
        .set_database(DatabaseDescriptor::new("analytics"))
        .await
        .expect("database should be written");
    assert_eq!(database.name, "analytics");

    let read_back = client
        .database("analytics")
        .await
        .expect("database should be read");
    assert_eq!(read_back.name, "analytics");

    let databases = client
        .databases()
        .await
        .expect("databases should be listed");
    assert_eq!(databases.len(), 2);
    assert!(
        databases
            .iter()
            .any(|database| database.name == "default" && database.is_default),
        "default database should be bootstrapped lazily"
    );
    assert!(
        databases
            .iter()
            .any(|database| database.name == "analytics" && !database.is_default),
        "explicit database should still be listed"
    );

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_reads_runtime_status_and_collection_placement() {
    let temp_root = unique_temp_dir("client-runtime-status");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config(&temp_root, rest_addr, grpc_addr)));

    state
        .control
        .create_collection(StorageCreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let client = LogPoseClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("client should connect");

    let status = client
        .runtime_status()
        .await
        .expect("runtime status should load");
    assert_eq!(status.role.as_str(), "combined");
    assert_eq!(status.storage_engine, "local");
    assert_eq!(status.collection_count, 1);
    assert_eq!(status.collections[0].database_name, "default");
    assert_eq!(status.collections[0].collection_name, "documents");
    assert_eq!(status.collections[0].assigned_role.as_str(), "data");

    let placement = client
        .collection_placement("default/documents")
        .await
        .expect("placement should load");
    assert_eq!(placement.database_name, "default");
    assert_eq!(placement.collection_name, "documents");
    assert_eq!(placement.assigned_node, "client-grpc");
    assert_eq!(placement.assigned_role.as_str(), "data");
    assert_eq!(placement.route_kind, "local");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_requires_auth_token_for_runtime_status_when_server_auth_is_enabled() {
    let temp_root = unique_temp_dir("client-runtime-auth");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(auth_test_config(
        &temp_root, rest_addr, grpc_addr,
    )));

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let endpoint = format!("http://{grpc_addr}");
    let unauthenticated = LogPoseClient::connect(endpoint.clone())
        .await
        .expect("client should connect")
        .runtime_status()
        .await
        .expect_err("runtime status should require auth");
    assert!(matches!(
        unauthenticated,
        ClientError::Status(status) if status.code() == tonic::Code::Unauthenticated
    ));

    let status = LogPoseClient::connect_with_auth(endpoint, Some("operator-secret"))
        .await
        .expect("client should connect with auth")
        .runtime_status()
        .await
        .expect("operator token should load runtime status");
    assert_eq!(status.metadata.node_name, "client-grpc");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_enforces_read_only_token_permissions() {
    let temp_root = unique_temp_dir("client-readonly-auth");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(auth_test_config(
        &temp_root, rest_addr, grpc_addr,
    )));

    state
        .control
        .set_database_access_policy(read_only_policy("default", "reader"))
        .await
        .expect("database policy should persist");
    state
        .control
        .create_collection(StorageCreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let client =
        LogPoseClient::connect_with_auth(format!("http://{grpc_addr}"), Some("reader-secret"))
            .await
            .expect("client should connect with read-only auth");

    client
        .stats("documents")
        .await
        .expect("read-only token should read stats");

    let write_error = client
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({}),
            })],
        )
        .await
        .expect_err("read-only token should not write");
    assert!(matches!(
        write_error,
        ClientError::Status(status) if status.code() == tonic::Code::PermissionDenied
    ));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_surfaces_data_only_collection_creation_failures() {
    let temp_root = unique_temp_dir("client-grpc-data-only");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config_with_role(
        &temp_root,
        rest_addr,
        grpc_addr,
        logpose_types::NodeRole::Data,
    )));

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let client = LogPoseClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("client should connect");

    let error = client
        .create_collection(CreateCollectionRequest::in_database(
            "default",
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect_err("data-only node should reject collection creation");

    assert!(
        matches!(error, ClientError::Status(_)),
        "expected status error, got {error:?}"
    );
    let ClientError::Status(status) = error else {
        return;
    };
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("data-only nodes cannot accept control-plane collection lifecycle mutations")
    );

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_round_trips_cooperative_filtered_ann() {
    let temp_root = unique_temp_dir("client-grpc-cooperative");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config(&temp_root, rest_addr, grpc_addr)));

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let client = LogPoseClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("client should connect");
    client
        .create_collection(CreateCollectionRequest::in_database(
            "default",
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let operations = (0..12)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            WriteOperation::Put(PutRecord {
                id: RecordId::new(format!("doc-{index}")),
                vector: vec![index as f32 + 1.0, 0.0],
                metadata: json!({"kind":kind,"version":index}),
            })
        })
        .collect::<Vec<_>>();
    client
        .write("default/documents", operations)
        .await
        .expect("write should succeed");
    client
        .flush("default/documents")
        .await
        .expect("flush should succeed");

    let response = client
        .query(QueryRequest {
            collection_name: "default/documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Profile,
        })
        .await
        .expect("query should succeed");

    assert_eq!(
        response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["doc-8", "doc-4"]
    );
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(
        diagnostics.chosen_plan,
        QueryPlanKind::CooperativeFilteredAnn
    );
    assert_eq!(
        diagnostics.planner_reason,
        "filtered ann traversal is cheaper than exact scan for this selectivity"
    );
    assert!((diagnostics.estimated_selectivity - 0.25).abs() <= f32::EPSILON);
    assert_eq!(diagnostics.units_considered, 2);
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 1);
    assert!(diagnostics.candidates_before_filter >= response.returned);
    assert!(diagnostics.candidates_after_filter >= response.returned);
    assert!(diagnostics.candidates_after_filter <= diagnostics.candidates_before_filter);
    assert_eq!(
        diagnostics.candidates_merged,
        diagnostics.candidates_reranked
    );
    assert!(diagnostics.candidates_merged >= response.returned);
    assert_eq!(diagnostics.rerank_count, 1);
    assert_eq!(
        diagnostics.unit_scan_mix.get("immutable_ann").copied(),
        Some(1)
    );
    assert!(diagnostics.fallback_reason.is_none());
    let timings = diagnostics
        .stage_timings
        .as_ref()
        .expect("profile mode should include timings");
    assert!(timings.planning_micros > 0);
    assert_eq!(timings.prefilter_micros, 0);
    assert!(timings.candidate_generation_micros > 0);
    assert!(timings.postfilter_micros > 0);
    assert!(timings.rerank_micros > 0);
    assert!(timings.merge_micros > 0);

    server.abort();
    let _ = server.await;
}

fn test_config(root: &Path, rest_addr: SocketAddr, grpc_addr: SocketAddr) -> LogPoseConfig {
    test_config_with_role(
        root,
        rest_addr,
        grpc_addr,
        logpose_types::NodeRole::Combined,
    )
}

fn auth_test_config(root: &Path, rest_addr: SocketAddr, grpc_addr: SocketAddr) -> LogPoseConfig {
    let mut config = test_config(root, rest_addr, grpc_addr);
    config.auth.bootstrap_tokens = vec![
        BootstrapTokenConfig {
            token: "operator-secret".to_owned(),
            principal: Principal::new_with_access_tier(
                "ops-admin",
                PrincipalKind::User,
                AccessTier::Operator,
            ),
        },
        BootstrapTokenConfig {
            token: "reader-secret".to_owned(),
            principal: Principal::new_with_access_tier(
                "reader",
                PrincipalKind::User,
                AccessTier::Service,
            ),
        },
    ];
    config
}

fn read_only_policy(database_name: &str, principal_name: &str) -> AuthDatabaseAccessPolicy {
    AuthDatabaseAccessPolicy {
        database_name: database_name.to_owned(),
        authentication_mode: AuthenticationMode::ExternalToken,
        role_bindings: vec![AuthDatabaseRoleBinding {
            database_name: database_name.to_owned(),
            principal_name: principal_name.to_owned(),
            role: DatabaseRole::ReadOnly,
        }],
    }
}

fn test_config_with_role(
    root: &Path,
    rest_addr: SocketAddr,
    grpc_addr: SocketAddr,
    node_role: logpose_types::NodeRole,
) -> LogPoseConfig {
    LogPoseConfig {
        node_name: "client-grpc".to_owned(),
        node_role,
        rest_host: rest_addr.ip().to_string(),
        rest_port: rest_addr.port(),
        grpc_host: grpc_addr.ip().to_string(),
        grpc_port: grpc_addr.port(),
        log_filter: "info".to_owned(),
        storage_root: root.join("data"),
        metadata: Default::default(),
        auth: Default::default(),
    }
}

fn reserve_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener.local_addr().expect("listener should expose addr");
    drop(listener);
    address
}

async fn wait_for_port(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(address).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        TcpStream::connect(address).is_ok(),
        "timed out waiting for server at {address}"
    );
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}
