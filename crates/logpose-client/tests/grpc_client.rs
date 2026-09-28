//! Integration tests for the gRPC-backed LogPose client.

use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy as AuthDatabaseAccessPolicy, DatabaseRole,
    DatabaseRoleBinding as AuthDatabaseRoleBinding, Principal, PrincipalKind,
};
use logpose_catalog as _;
use logpose_client::{
    ClientError, CollectionRef, CountRecordsRequest, CreateCollectionRequest, DatabaseAccessPolicy,
    DatabaseRoleBinding, ErrorCode, ErrorReason, LogPoseClient, OrderBy, PartialUpdate, PrimaryKey,
    ReadConsistency, Record, RecordPatch, SchemaChange, ScrollRecordsRequest, ServerError,
    SortDirection,
};
use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
use logpose_core::AppState;
use logpose_query::{ExplainMode, FilterExpr, QueryPlanKind, QueryRequest, VectorQuery};
use logpose_storage::{CreateCollectionRequest as StorageCreateCollectionRequest, InspectTarget};
use logpose_types::{
    DistanceMetric,
    schema::{
        CreateCollectionSpec, FieldIndex, FieldType, PrimaryKeySpec, PrimaryKeyType,
        ScalarFieldSpec, VectorFieldSpec,
    },
    value::Value as RecordValue,
};
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
use tokio_stream as _;
use tonic_types as _;

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
    let collection = descriptor.collection_ref();
    assert_eq!(descriptor.database_name, "default");
    assert_eq!(descriptor.name, "documents");

    let read_back = client
        .collection(&collection)
        .await
        .expect("collection should load");
    assert_eq!(read_back.database_name, "default");
    assert_eq!(read_back.collection_id, descriptor.collection_id);

    client
        .upsert(
            &collection,
            vec![
                put(
                    "alpha",
                    vec![1.0, 0.0],
                    json!({"kind":"keep","color":"red"}),
                ),
                put(
                    "beta",
                    vec![0.5, 0.0],
                    json!({"kind":"drop","color":"blue"}),
                ),
            ],
        )
        .await
        .expect("write should succeed");

    let query = client
        .query(
            &CollectionRef::parse(&qualified.clone()).expect("name"),
            QueryRequest {
                vector: Some(VectorQuery {
                    field: None,
                    values: vec![1.0, 0.0],
                }),
                top_k: 2,
                filter: Some(FilterExpr::eq("kind", "keep")),
                output_fields: vec!["$extra".to_owned()],
                explain: ExplainMode::Profile,
                ..QueryRequest::default()
            },
        )
        .await
        .expect("query should succeed");
    assert_eq!(query.hits[0].record.pk.label(), "alpha");
    assert_eq!(
        query
            .diagnostics
            .as_ref()
            .expect("diagnostics should be present")
            .chosen_plan,
        QueryPlanKind::PredicateFirstExact
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
    assert_eq!(timings.prefilter_micros, 0);
    assert!(diagnostics.candidates_merged >= 1);
    assert_eq!(
        diagnostics.unit_scan_mix.get("memtable_scan").copied(),
        Some(1)
    );

    let stats = client
        .stats(&collection, None, None)
        .await
        .expect("stats should load");
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
        .flush(&collection)
        .await
        .expect("flush should succeed");
    assert!(flush.manifest_generation >= 1);

    client
        .delete(&collection, vec![PrimaryKey::String("beta".to_owned())])
        .await
        .expect("delete should succeed");
    // A live memtable row keeps a mutable unit for the hybrid plan below: the delete alone only
    // sets the segment row's deletion bit.
    client
        .upsert(
            &collection,
            vec![put("gamma", vec![0.0, 1.0], json!({"kind": "keep"}))],
        )
        .await
        .expect("write should succeed");

    let compact = client
        .compact(&collection)
        .await
        .expect("compact should succeed");
    assert!(compact.manifest_generation >= flush.manifest_generation);

    let stats = client
        .stats(&collection, None, None)
        .await
        .expect("stats should reload");
    assert_eq!(stats.live_record_count, 2);
    // The compaction rewrote the lone segment without its deleted row.
    assert_eq!(stats.deleted_record_count, 0);
    assert_eq!(stats.mutable_op_count, 2);
    assert_eq!(stats.segment_count, 1);
    // The explicit flush and the compaction are completed jobs.
    assert_eq!(stats.maintenance.completed_runs, 2);
    assert!(stats.maintenance.in_progress.is_none());
    assert_eq!(stats.query_units.len(), 2);
    let immutable = stats
        .query_units
        .iter()
        .find(|unit| unit.tier == "immutable")
        .expect("immutable unit should be present");
    // Two rows are too few for SQ8 codes or a graph.
    assert_eq!(immutable.index_kind, "flat");
    assert!(
        immutable
            .artifact_stats
            .iter()
            .any(|artifact| artifact.file_name.ends_with(".seg"))
    );
    assert!(
        immutable
            .component_bytes
            .get("segment")
            .copied()
            .unwrap_or_default()
            > 0
    );

    let hybrid_query = client
        .query(
            &CollectionRef::parse(&qualified.clone()).expect("name"),
            QueryRequest {
                vector: Some(VectorQuery {
                    field: None,
                    values: vec![1.0, 0.0],
                }),
                top_k: 2,
                filter: None,
                output_fields: vec!["$extra".to_owned()],
                explain: ExplainMode::Profile,
                ..QueryRequest::default()
            },
        )
        .await
        .expect("hybrid query should succeed");
    assert_eq!(hybrid_query.hits[0].record.pk.label(), "alpha");
    let hybrid_diagnostics = hybrid_query
        .diagnostics
        .as_ref()
        .expect("hybrid query should include diagnostics");
    // Without a segment graph, every unit is scanned exactly.
    assert_eq!(
        hybrid_diagnostics.chosen_plan,
        QueryPlanKind::UnfilteredExactScan
    );
    assert!(hybrid_diagnostics.candidates_merged >= 1);
    assert!(hybrid_diagnostics.candidates_reranked >= 1);
    assert_eq!(
        hybrid_diagnostics.unit_scan_mix.get("exact_f32").copied(),
        Some(1)
    );
    assert_eq!(
        hybrid_diagnostics
            .unit_scan_mix
            .get("memtable_scan")
            .copied(),
        Some(1)
    );
    let hybrid_timings = hybrid_diagnostics
        .stage_timings
        .as_ref()
        .expect("hybrid profile should include timings");
    assert_eq!(hybrid_timings.merge_micros, 0);

    let inspect = client
        .inspect(&collection, InspectTarget::Manifest)
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
        .inspect(&collection, InspectTarget::Wal)
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
        .inspect(&collection, InspectTarget::Segment(segment_id.clone()))
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
        .inspect(&collection, InspectTarget::Maintenance)
        .await
        .expect("maintenance inspect should succeed");
    assert_eq!(maintenance.target, "maintenance");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_manages_typed_schemas_and_records() {
    let temp_root = unique_temp_dir("client-grpc-typed");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config(&temp_root, rest_addr, grpc_addr)));

    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;

    let client = LogPoseClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("client should connect");
    client
        .put_database("shop")
        .await
        .expect("database should be created");

    let created = client
        .create_collection(CreateCollectionRequest::from_spec(
            "shop",
            CreateCollectionSpec {
                name: "products".to_owned(),
                primary_key: PrimaryKeySpec {
                    name: "sku".to_owned(),
                    key_type: PrimaryKeyType::Int64,
                },
                vectors: vec![VectorFieldSpec {
                    name: "embedding".to_owned(),
                    dimensions: 2,
                    metric: DistanceMetric::Cosine,
                }],
                fields: vec![ScalarFieldSpec {
                    name: "title".to_owned(),
                    field_type: FieldType::String,
                    index: FieldIndex::Auto,
                    nullable: false,
                }],
                dynamic_fields: true,
            },
        ))
        .await
        .expect("typed collection should be created");
    let collection = created.collection_ref();
    assert_eq!(created.schema.schema_version(), 1);
    assert_eq!(created.schema.fields()[0].index, FieldIndex::Inverted);

    let listed = client
        .collections("shop")
        .await
        .expect("collections should be listed");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].schema, created.schema);

    let altered = client
        .alter_collection(
            &collection,
            SchemaChange::AddField(ScalarFieldSpec {
                name: "price".to_owned(),
                field_type: FieldType::Float64,
                index: FieldIndex::Auto,
                nullable: true,
            }),
        )
        .await
        .expect("field should be added");
    assert_eq!(altered.schema.schema_version(), 2);
    assert!(altered.schema.field("price").is_some());

    let record = Record {
        pk: PrimaryKey::Int64(7),
        vectors: [("embedding".to_owned(), vec![3.0, 4.0])].into(),
        fields: [
            ("title".to_owned(), RecordValue::String("lamp".to_owned())),
            ("price".to_owned(), RecordValue::Float64(12.5)),
        ]
        .into(),
        extra: json!({"color": "red"})
            .as_object()
            .cloned()
            .expect("extra is an object"),
    };
    client
        .upsert(&collection, vec![record])
        .await
        .expect("typed record should be written");

    let fetched = client
        .get(
            &collection,
            vec![PrimaryKey::Int64(7), PrimaryKey::Int64(8)],
            Vec::new(),
        )
        .await
        .expect("records should be read");
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.missing_keys, vec![PrimaryKey::Int64(8)]);
    assert!(
        fetched.records[0].vectors.is_empty(),
        "no vectors by default"
    );
    assert_eq!(fetched.records[0].extra["color"], json!("red"));
    let with_vector = client
        .get(
            &collection,
            vec![PrimaryKey::Int64(7)],
            vec!["embedding".to_owned()],
        )
        .await
        .expect("records should be read");
    // Cosine vectors are normalized when written.
    assert_eq!(with_vector.records[0].vectors["embedding"], vec![0.6, 0.8]);

    let update = PartialUpdate {
        pk: PrimaryKey::Int64(7),
        vectors: Default::default(),
        fields: [("price".to_owned(), RecordValue::Null)].into(),
        extra: Default::default(),
    };
    client
        .update(&collection, vec![update])
        .await
        .expect("record should be updated");
    let projected = client
        .get(
            &collection,
            vec![PrimaryKey::Int64(7)],
            vec!["title".to_owned(), "price".to_owned()],
        )
        .await
        .expect("projected record should be read");
    let record = &projected.records[0];
    assert!(record.vectors.is_empty());
    assert!(record.extra.is_empty());
    assert_eq!(
        record.fields.get("title"),
        Some(&RecordValue::String("lamp".to_owned()))
    );
    assert_eq!(record.fields.get("price"), None);

    let missing = client
        .update(
            &collection,
            vec![PartialUpdate {
                pk: PrimaryKey::Int64(9),
                vectors: Default::default(),
                fields: [("price".to_owned(), RecordValue::Float64(1.0))].into(),
                extra: Default::default(),
            }],
        )
        .await
        .expect_err("updating a missing record should fail");
    assert_eq!(missing.reason(), Some(ErrorReason::ResourceNotFound));

    let invalid = client
        .upsert(
            &collection,
            vec![Record {
                pk: PrimaryKey::Int64(10),
                vectors: [("embedding".to_owned(), vec![1.0, 0.0])].into(),
                fields: Default::default(),
                extra: Default::default(),
            }],
        )
        .await
        .expect_err("a record without its required field should fail");
    let ClientError::Server(invalid) = invalid else {
        unreachable!("expected a typed server error, got {invalid:?}");
    };
    assert_eq!(invalid.reason(), Some(ErrorReason::InvalidArgument));
    assert_eq!(
        invalid
            .field_violations()
            .iter()
            .map(|violation| violation.field.as_str())
            .collect::<Vec<_>>(),
        vec!["records[0].title"]
    );

    client
        .delete(&collection, vec![PrimaryKey::Int64(7)])
        .await
        .expect("record should be deleted");
    let after_delete = client
        .get(&collection, vec![PrimaryKey::Int64(7)], Vec::new())
        .await
        .expect("records should be read");
    assert!(after_delete.records.is_empty());

    let refused = client
        .drop_database("shop")
        .await
        .expect_err("a database with collections should not be dropped");
    assert_eq!(refused.reason(), Some(ErrorReason::FailedPrecondition));
    client
        .drop_collection(&collection)
        .await
        .expect("collection should be dropped");
    let gone = client
        .collection(&collection)
        .await
        .expect_err("a dropped collection should not load");
    assert_eq!(gone.reason(), Some(ErrorReason::ResourceNotFound));
    client
        .drop_database("shop")
        .await
        .expect("empty database should be dropped");
    let databases = client.databases().await.expect("databases should list");
    assert!(databases.iter().all(|database| database.name != "shop"));

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
        authentication_mode: AuthenticationMode::ExternalToken,
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
        .put_database("analytics")
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
        .collection_placement(&CollectionRef::new("default", "documents"))
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
    assert_eq!(unauthenticated.reason(), Some(ErrorReason::Unauthenticated));
    assert_eq!(
        unauthenticated.status().map(tonic::Status::code),
        Some(tonic::Code::Unauthenticated)
    );

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

    let collection = CollectionRef::new("default", "documents");
    client
        .stats(&collection, None, None)
        .await
        .expect("read-only token should read stats");

    let write_error = client
        .upsert(&collection, vec![put("alpha", vec![1.0, 0.0], json!({}))])
        .await
        .expect_err("read-only token should not write");
    assert_eq!(write_error.reason(), Some(ErrorReason::PermissionDenied));
    assert_eq!(
        write_error.server_error().and_then(ServerError::error_code),
        Some(ErrorCode::PermissionDenied)
    );

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

    let ClientError::Server(error) = error else {
        unreachable!("expected a typed server error, got {error:?}");
    };
    assert_eq!(error.reason(), Some(ErrorReason::WrongNodeRole));
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        error.metadata().get("node_role").map(String::as_str),
        Some("data")
    );
    assert!(!error.is_retryable());
    assert!(error.message().contains(
        "is running as 'data' and cannot accept control-plane collection lifecycle mutations"
    ));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_round_trips_filtered_segment_scan_diagnostics() {
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

    let collection = CollectionRef::new("default", "documents");
    let records = (0..12)
        .map(|index| {
            let kind = if index % 4 == 0 { "keep" } else { "drop" };
            put(
                &format!("doc-{index}"),
                vec![index as f32 + 1.0, 0.0],
                json!({"kind":kind,"version":index}),
            )
        })
        .collect::<Vec<_>>();
    client
        .upsert(&collection, records)
        .await
        .expect("write should succeed");
    client
        .flush(&collection)
        .await
        .expect("flush should succeed");

    let response = client
        .query(
            &CollectionRef::parse("default/documents").expect("name"),
            QueryRequest {
                vector: Some(VectorQuery {
                    field: None,
                    values: vec![1.0, 0.0],
                }),
                top_k: 2,
                filter: Some(FilterExpr::eq("kind", "keep")),
                output_fields: vec!["$extra".to_owned()],
                explain: ExplainMode::Profile,
                ..QueryRequest::default()
            },
        )
        .await
        .expect("query should succeed");

    assert_eq!(
        response
            .hits
            .iter()
            .map(|hit| hit.record.pk.label())
            .collect::<Vec<_>>(),
        vec!["doc-8", "doc-4"]
    );
    let diagnostics = response
        .diagnostics
        .clone()
        .expect("diagnostics should be present");
    // Twelve rows make a segment without SQ8 codes or a graph: an exact f32 scan of the
    // three rows the filter matches.
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::PredicateFirstExact);
    assert!(diagnostics.planner_reason.contains("exact_f32"));
    assert!((diagnostics.estimated_selectivity - 0.25).abs() <= f32::EPSILON);
    assert!(diagnostics.units_considered >= 1);
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 1);
    assert!(diagnostics.candidates_before_filter >= response.hits.len());
    assert!(diagnostics.candidates_after_filter >= response.hits.len());
    assert!(diagnostics.candidates_after_filter <= diagnostics.candidates_before_filter);
    assert!(diagnostics.candidates_merged >= response.hits.len());
    assert_eq!(diagnostics.rerank_count, 1);
    assert_eq!(diagnostics.unit_scan_mix.get("exact_f32").copied(), Some(1));
    assert!(diagnostics.fallback_reason.is_some());
    let timings = diagnostics
        .stage_timings
        .as_ref()
        .expect("profile mode should include timings");
    assert_eq!(timings.prefilter_micros, 0);
    assert_eq!(timings.merge_micros, 0);

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn grpc_client_counts_scrolls_scans_and_writes_by_filter() {
    let temp_root = unique_temp_dir("client-grpc-scroll");
    let grpc_addr = reserve_local_addr();
    let rest_addr = reserve_local_addr();
    let state = Arc::new(AppState::new(test_config(&temp_root, rest_addr, grpc_addr)));
    let server = tokio::spawn(logpose_api_grpc::serve(state));
    wait_for_port(grpc_addr).await;
    let client = LogPoseClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("client should connect");

    let created = client
        .create_collection(CreateCollectionRequest::from_spec(
            "default",
            CreateCollectionSpec {
                name: "items".to_owned(),
                primary_key: PrimaryKeySpec {
                    name: "sku".to_owned(),
                    key_type: PrimaryKeyType::Int64,
                },
                vectors: vec![VectorFieldSpec {
                    name: "embedding".to_owned(),
                    dimensions: 2,
                    metric: DistanceMetric::Dot,
                }],
                fields: vec![ScalarFieldSpec {
                    name: "rank".to_owned(),
                    field_type: FieldType::Int64,
                    index: FieldIndex::Auto,
                    nullable: true,
                }],
                dynamic_fields: false,
            },
        ))
        .await
        .expect("collection should be created");
    let collection = created.collection_ref();
    let records = (1..=12_i64)
        .map(|sku| {
            Record::new(sku)
                .with_vector("embedding", vec![sku as f32, 0.0])
                .with_field("rank", RecordValue::Int64(sku % 4))
        })
        .collect();
    client
        .upsert(&collection, records)
        .await
        .expect("records should be written");

    let low_rank = FilterExpr::lt("rank", 2);
    let counted = client
        .count(
            &collection,
            CountRecordsRequest {
                filter: Some(low_rank.clone()),
                read: ReadConsistency {
                    pin: true,
                    ..ReadConsistency::default()
                },
            },
        )
        .await
        .expect("count should succeed")
        .response;
    assert_eq!(counted.count, 6);
    let token = counted
        .snapshot_token
        .expect("a pinned count returns a token");

    // Scroll the pinned state in pages of four, deleting matches between pages.
    let mut request = ScrollRecordsRequest {
        filter: Some(low_rank),
        page_size: Some(4),
        output_fields: vec!["rank".to_owned()],
        snapshot_token: Some(token),
        ..ScrollRecordsRequest::default()
    };
    let mut skus = Vec::new();
    loop {
        let page = client
            .scroll(&collection, request.clone())
            .await
            .expect("scroll page should succeed")
            .response;
        skus.extend(page.records.iter().map(|record| record.pk.clone()));
        assert!(page.records.iter().all(|record| record.vectors.is_empty()));
        let Some(cursor) = page.next_cursor else {
            break;
        };
        if skus.len() == 4 {
            let ack = client
                .delete_by_filter(&collection, FilterExpr::eq("rank", 0))
                .await
                .expect("delete by filter should succeed")
                .response;
            assert_eq!(ack.applied_ops, 3);
        }
        request.cursor = Some(cursor);
        request.snapshot_token = None;
    }
    assert_eq!(skus, [1_i64, 4, 5, 8, 9, 12].map(PrimaryKey::Int64));

    let patch = RecordPatch {
        fields: [("rank".to_owned(), RecordValue::Int64(9))].into(),
        ..RecordPatch::default()
    };
    let ack = client
        .update_by_filter(&collection, FilterExpr::eq("rank", 1), patch)
        .await
        .expect("update by filter should succeed")
        .response;
    assert_eq!(ack.applied_ops, 3);

    // A scan without a vector, ordered by rank descending.
    let scanned = client
        .query(
            &collection,
            QueryRequest {
                order_by: vec![OrderBy {
                    field: "rank".to_owned(),
                    direction: SortDirection::Desc,
                }],
                top_k: 3,
                output_fields: vec!["sku".to_owned()],
                ..QueryRequest::default()
            },
        )
        .await
        .expect("scan should succeed")
        .response;
    assert_eq!(
        scanned
            .hits
            .iter()
            .map(|hit| hit.record.pk.clone())
            .collect::<Vec<_>>(),
        [1_i64, 5, 9].map(PrimaryKey::Int64)
    );
    assert!(scanned.hits.iter().all(|hit| hit.score.is_none()));

    let error = client
        .delete_by_filter(&collection, FilterExpr::contains("rank", 1))
        .await
        .expect_err("contains on a scalar field should fail");
    let ClientError::Server(error) = error else {
        unreachable!("expected a typed server error, got {error:?}");
    };
    assert_eq!(error.reason(), Some(ErrorReason::InvalidArgument));
    assert_eq!(
        error
            .field_violations()
            .iter()
            .map(|violation| violation.field.as_str())
            .collect::<Vec<_>>(),
        vec!["filter.contains.rank"]
    );

    server.abort();
    let _ = server.await;
}

/// A record with key `id`, the `vector` field, and `metadata` as its `$extra` object.
fn put(id: &str, vector: Vec<f32>, metadata: Value) -> Record {
    let mut record = Record::new(id).with_vector("vector", vector);
    if let Value::Object(extra) = metadata {
        record.extra = extra;
    }
    record
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
        limits: Default::default(),
        snapshots: Default::default(),
        index: Default::default(),
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
