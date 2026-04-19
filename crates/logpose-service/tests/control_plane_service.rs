//! Integration tests for the Phase 5 control-plane surface.

use async_trait as _;
use axum as _;
use http_body_util as _;
use logpose_api_grpc as _;
use logpose_api_rest as _;
use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
    Principal, PrincipalKind,
};
use logpose_catalog as _;
use logpose_core::AppState;
use logpose_query::{ExplainMode, QueryRequest};
use logpose_service as _;
use logpose_storage::{
    CreateCollectionRequest, InspectTarget, LocalStorageEngine, StorageEngine as _,
};
use logpose_storage_etcd as _;
use logpose_types::{
    CollectionAssignment, DistanceMetric, MaintenanceStatus, PutRecord, RecordId, WriteOperation,
};
use rand as _;
use serde as _;
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror as _;
use tokio as _;
use tonic as _;
use tower as _;

#[tokio::test]
async fn control_plane_reports_runtime_status_and_local_placement() {
    let config = test_config("control-runtime");
    let state = Arc::new(AppState::new(config.clone()));

    let descriptor = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    state
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: serde_json::json!({"kind":"keep"}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: serde_json::json!({"kind":"keep"}),
                }),
            ],
        )
        .await
        .expect("write should succeed");

    let status = state
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");

    assert_eq!(status.metadata.node_name, config.node_name);
    assert_eq!(status.role.as_str(), "combined");
    assert_eq!(
        status.rest_endpoint,
        format!("http://{}:{}", config.rest_host, config.rest_port)
    );
    assert_eq!(
        status.grpc_endpoint,
        format!("http://{}:{}", config.grpc_host, config.grpc_port)
    );
    assert_eq!(status.storage_engine, "local");
    assert!(status.control_plane_ready);
    assert!(status.data_plane_ready);
    assert_eq!(status.collection_count, 1);
    assert_eq!(status.collections.len(), 1);
    assert_eq!(
        status.collections[0].collection_id,
        descriptor.collection_id
    );
    assert_eq!(status.collections[0].collection_name, "documents");
    assert_eq!(status.collections[0].assigned_node, config.node_name);
    assert_eq!(
        status.collections[0].assigned_role,
        logpose_types::NodeRole::Data
    );
    assert_eq!(status.collections[0].route_kind, "local");
    assert!(status.collections[0].route_reason.contains("single-node"));
    assert_eq!(status.maintenance.pending_operations, 0);
    assert_eq!(status.maintenance.collections_with_pending, 0);
    assert_eq!(status.maintenance.collections_in_progress, 0);
    assert_eq!(status.maintenance.collections_with_errors, 0);
}

#[tokio::test]
async fn control_plane_reconstructs_runtime_status_after_restart() {
    let config = test_config("control-restart");
    let state = Arc::new(AppState::new(config.clone()));

    state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Cosine,
        })
        .await
        .expect("collection should be created");
    state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    drop(state);

    let restarted = Arc::new(AppState::new(config.clone()));
    let status = restarted
        .control
        .runtime_status()
        .await
        .expect("runtime status should load after restart");

    assert_eq!(status.collection_count, 2);
    assert_eq!(
        status
            .collections
            .iter()
            .map(|placement| placement.collection_name.as_str())
            .collect::<Vec<_>>(),
        vec!["documents", "events"]
    );
}

#[tokio::test]
async fn control_plane_rejects_missing_collection_placement_requests() {
    let state = Arc::new(AppState::new(test_config("control-missing")));

    let error = state
        .control
        .collection_placement("missing")
        .await
        .expect_err("missing collection should fail");

    assert!(
        error.to_string().contains("does not exist"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn control_plane_reports_non_default_database_collection_identity() {
    let state = Arc::new(AppState::new(test_config("control-qualified-database")));

    let descriptor = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "analytics".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    let status = state
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");
    let bare_error = state
        .control
        .collection_placement("documents")
        .await
        .expect_err("bare placement lookup should be rejected for non-default databases");

    assert_eq!(descriptor.database_name, "analytics");
    assert_eq!(descriptor.name, "documents");
    assert_eq!(status.collection_count, 1);
    assert_eq!(status.collections.len(), 1);
    assert_eq!(status.collections[0].database_name, "analytics");
    assert_eq!(status.collections[0].collection_name, "documents");
    assert_eq!(status.collections[0].route_kind, "local");
    assert!(
        bare_error.to_string().contains("does not exist"),
        "unexpected error: {bare_error}"
    );
}

#[tokio::test]
async fn control_plane_distinguishes_duplicate_collection_names_across_databases() {
    let state = Arc::new(AppState::new(test_config("control-namespace-collision")));

    let default_descriptor = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("default namespace collection should be created");
    let analytics_descriptor = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "analytics".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("database namespace collection should be created");

    state
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("default-alpha"),
                vector: vec![1.0, 0.0],
                metadata: serde_json::json!({"namespace":"default"}),
            })],
        )
        .await
        .expect("default namespace write should succeed");
    state
        .write(
            "analytics/documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("analytics-alpha"),
                vector: vec![0.0, 1.0],
                metadata: serde_json::json!({"namespace":"analytics"}),
            })],
        )
        .await
        .expect("database namespace write should succeed");

    let status = state
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");
    let default_placement = state
        .control
        .collection_placement("documents")
        .await
        .expect("default placement should load");
    let analytics_placement = state
        .control
        .collection_placement("analytics/documents")
        .await
        .expect("database placement should load");
    let default_stats = state
        .stats("documents")
        .await
        .expect("default stats should load");
    let analytics_stats = state
        .stats("analytics/documents")
        .await
        .expect("database stats should load");

    assert_eq!(status.collection_count, 2);
    assert_eq!(status.collections.len(), 2);
    assert!(status.collections.iter().any(|placement| {
        placement.database_name == "default" && placement.collection_name == "documents"
    }));
    assert!(status.collections.iter().any(|placement| {
        placement.database_name == "analytics" && placement.collection_name == "documents"
    }));

    assert_eq!(
        default_placement.collection_id,
        default_descriptor.collection_id
    );
    assert_eq!(default_placement.database_name, "default");
    assert_eq!(
        analytics_placement.collection_id,
        analytics_descriptor.collection_id
    );
    assert_eq!(analytics_placement.database_name, "analytics");

    assert_eq!(default_stats.database_name, "default");
    assert_eq!(default_stats.live_record_count, 1);
    assert_eq!(analytics_stats.database_name, "analytics");
    assert_eq!(analytics_stats.live_record_count, 1);
}

#[tokio::test]
async fn data_only_nodes_reject_control_plane_collection_creation() {
    let state = Arc::new(AppState::new(test_config_with_role(
        "control-data-only",
        logpose_types::NodeRole::Data,
    )));

    let error = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect_err("data-only nodes should reject collection creation");

    assert!(
        error
            .to_string()
            .contains("control-plane collection lifecycle")
    );
}

#[tokio::test]
async fn control_only_nodes_reject_app_state_data_plane_operations() {
    let state = Arc::new(AppState::new(test_config_with_role(
        "control-appstate-gate",
        logpose_types::NodeRole::Control,
    )));

    let error = state
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: serde_json::json!({"kind":"keep"}),
            })],
        )
        .await
        .expect_err("control-only nodes should reject direct data-plane writes");

    assert!(error.to_string().contains("data-plane operations"));
}

#[tokio::test]
async fn control_only_nodes_reject_control_plane_collection_creation() {
    let state = Arc::new(AppState::new(test_config_with_role(
        "control-assignment",
        logpose_types::NodeRole::Control,
    )));

    let error = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect_err("control-only nodes should reject collection creation");

    assert!(
        error.to_string().contains("without a local data plane"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn control_only_restarts_preserve_persisted_data_assignment() {
    let root = unique_temp_dir("control-role-restart");
    let combined = logpose_config::LogPoseConfig {
        node_name: "control-role-node".to_owned(),
        node_role: logpose_types::NodeRole::Combined,
        storage_root: root.clone(),
        ..logpose_config::LogPoseConfig::default()
    };
    let control = logpose_config::LogPoseConfig {
        node_name: "control-role-node".to_owned(),
        node_role: logpose_types::NodeRole::Control,
        storage_root: root,
        ..logpose_config::LogPoseConfig::default()
    };

    let initial = Arc::new(AppState::new(combined));
    initial
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");
    drop(initial);

    let restarted = Arc::new(AppState::new(control));
    let status = restarted
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");
    let placement = restarted
        .control
        .collection_placement("documents")
        .await
        .expect("placement should load");

    assert_eq!(status.role, logpose_types::NodeRole::Control);
    assert!(status.control_plane_ready);
    assert!(!status.data_plane_ready);
    assert_eq!(status.collection_count, 0);
    assert_eq!(status.collections.len(), 1);
    assert_eq!(
        status.collections[0].assigned_role,
        logpose_types::NodeRole::Data
    );
    assert_eq!(status.collections[0].route_kind, "recorded");
    assert!(status.collections[0].route_reason.contains("control-only"));
    assert_eq!(placement.assigned_role, logpose_types::NodeRole::Data);
    assert_eq!(placement.route_kind, "recorded");
    assert!(placement.route_reason.contains("control-only"));
}

#[tokio::test]
async fn data_only_restarts_preserve_persisted_local_data_assignment() {
    let root = unique_temp_dir("data-role-restart");
    let combined = test_config_with_root(
        "data-role-node",
        logpose_types::NodeRole::Combined,
        root.clone(),
    );
    let data = test_config_with_root("data-role-node", logpose_types::NodeRole::Data, root);

    let initial = Arc::new(AppState::new(combined));
    initial
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");
    initial
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: serde_json::json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("write should succeed");
    drop(initial);

    let restarted = Arc::new(AppState::new(data));
    let status = restarted
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");
    let placement = restarted
        .control
        .collection_placement("documents")
        .await
        .expect("placement should load");
    restarted
        .stats("documents")
        .await
        .expect("data-only node should still serve local data assignments");

    assert_eq!(status.role, logpose_types::NodeRole::Data);
    assert!(status.data_plane_ready);
    assert_eq!(status.collection_count, 1);
    assert_eq!(status.collections.len(), 1);
    assert_eq!(
        status.collections[0].assigned_role,
        logpose_types::NodeRole::Data
    );
    assert_eq!(status.collections[0].route_kind, "local");
    assert!(status.collections[0].route_reason.contains("data-plane"));
    assert_eq!(placement.assigned_role, logpose_types::NodeRole::Data);
    assert_eq!(placement.route_kind, "local");
    assert!(placement.route_reason.contains("data-plane"));
}

#[tokio::test]
async fn control_plane_status_reads_do_not_resume_persisted_maintenance() {
    let root = unique_temp_dir("control-status-maintenance");
    let combined = test_config_with_root(
        "control-status-maintenance",
        logpose_types::NodeRole::Combined,
        root.clone(),
    );
    let control = test_config_with_root(
        "control-status-maintenance",
        logpose_types::NodeRole::Control,
        root,
    );
    let initial = Arc::new(AppState::new(combined));

    let descriptor = initial
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");
    initial
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: serde_json::json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("write should succeed");

    fs::write(
        descriptor.root_path.join("maintenance.json"),
        serde_json::to_vec_pretty(&MaintenanceStatus {
            pending: vec!["flush".to_owned()],
            in_progress: None,
            last_error: None,
            completed_runs: 0,
        })
        .expect("maintenance json should serialize"),
    )
    .expect("maintenance json should be written");

    drop(initial);

    let restarted = Arc::new(AppState::new(control));
    restarted
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");

    tokio::time::sleep(Duration::from_millis(250)).await;

    let persisted: MaintenanceStatus = serde_json::from_slice(
        &fs::read(descriptor.root_path.join("maintenance.json"))
            .expect("maintenance json should still exist"),
    )
    .expect("maintenance json should parse");
    let segment_count = fs::read_dir(descriptor.root_path.join("segments"))
        .expect("segments directory should exist")
        .count();

    assert_eq!(persisted.pending, vec!["flush"]);
    assert!(persisted.in_progress.is_none());
    assert_eq!(segment_count, 0);
}

#[tokio::test]
async fn combined_runtime_status_reads_persisted_maintenance_without_resuming_it() {
    let root = unique_temp_dir("combined-status-maintenance");
    let combined = test_config_with_root(
        "combined-status-maintenance",
        logpose_types::NodeRole::Combined,
        root.clone(),
    );
    let initial = Arc::new(AppState::new(combined.clone()));

    let descriptor = initial
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");
    initial
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: serde_json::json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("write should succeed");

    fs::write(
        descriptor.root_path.join("maintenance.json"),
        serde_json::to_vec_pretty(&MaintenanceStatus {
            pending: vec!["flush".to_owned()],
            in_progress: None,
            last_error: None,
            completed_runs: 0,
        })
        .expect("maintenance json should serialize"),
    )
    .expect("maintenance json should be written");

    drop(initial);

    let restarted = Arc::new(AppState::new(combined));
    let status = restarted
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");

    tokio::time::sleep(Duration::from_millis(250)).await;

    let persisted: MaintenanceStatus = serde_json::from_slice(
        &fs::read(descriptor.root_path.join("maintenance.json"))
            .expect("maintenance json should still exist"),
    )
    .expect("maintenance json should parse");
    let segment_count = fs::read_dir(descriptor.root_path.join("segments"))
        .expect("segments directory should exist")
        .count();

    assert_eq!(status.maintenance.collections_with_pending, 1);
    assert_eq!(status.maintenance.pending_operations, 1);
    assert_eq!(persisted.pending, vec!["flush"]);
    assert!(persisted.in_progress.is_none());
    assert_eq!(segment_count, 0);
}

#[tokio::test]
async fn renamed_nodes_record_remote_assignment_and_reject_data_plane_operations() {
    let root = unique_temp_dir("recorded-node-restart");
    let initial = Arc::new(AppState::new(test_config_with_root(
        "recorded-node-a",
        logpose_types::NodeRole::Combined,
        root.clone(),
    )));
    initial
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");
    initial
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: serde_json::json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("write should succeed");
    drop(initial);

    let restarted = Arc::new(AppState::new(test_config_with_root(
        "recorded-node-b",
        logpose_types::NodeRole::Combined,
        root,
    )));
    let status = restarted
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");
    let placement = restarted
        .control
        .collection_placement("documents")
        .await
        .expect("placement should load");

    assert_eq!(status.collection_count, 0);
    assert_eq!(status.collections.len(), 1);
    assert_eq!(status.collections[0].assigned_node, "recorded-node-a");
    assert_eq!(status.collections[0].route_kind, "recorded");
    assert!(status.collections[0].route_reason.contains("targets node"));
    assert_eq!(placement.assigned_node, "recorded-node-a");
    assert_eq!(placement.route_kind, "recorded");

    let errors = vec![
        restarted
            .write(
                "documents",
                vec![WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: serde_json::json!({"kind":"keep"}),
                })],
            )
            .await
            .expect_err("write should be rejected")
            .to_string(),
        restarted
            .query(QueryRequest {
                collection_name: "documents".to_owned(),
                vector: vec![1.0, 0.0],
                top_k: 1,
                snapshot: None,
                read_barrier: None,
                filters: Vec::new(),
                predicate: None,
                explain: ExplainMode::None,
            })
            .await
            .expect_err("query should be rejected")
            .to_string(),
        restarted
            .snapshot("documents")
            .await
            .expect_err("snapshot should be rejected")
            .to_string(),
        restarted
            .stats("documents")
            .await
            .expect_err("stats should be rejected")
            .to_string(),
        restarted
            .flush("documents")
            .await
            .expect_err("flush should be rejected")
            .to_string(),
        restarted
            .compact("documents")
            .await
            .expect_err("compact should be rejected")
            .to_string(),
        restarted
            .inspect("documents", InspectTarget::Manifest)
            .await
            .expect_err("inspect should be rejected")
            .to_string(),
    ];

    for error in errors {
        assert!(
            error.contains("not locally served"),
            "unexpected error: {error}"
        );
    }
}

#[tokio::test]
async fn raw_local_storage_creates_surface_local_runtime_status() {
    let root = unique_temp_dir("raw-local-status");
    let engine = LocalStorageEngine::new(&root);
    engine
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    let state = Arc::new(AppState::new(test_config_with_root(
        "raw-local-status",
        logpose_types::NodeRole::Combined,
        root,
    )));
    let status = state
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");
    let placement = state
        .control
        .collection_placement("documents")
        .await
        .expect("placement should load");

    assert_eq!(status.collection_count, 1);
    assert_eq!(status.collections.len(), 1);
    assert_eq!(status.collections[0].assigned_node, "local");
    assert_eq!(
        status.collections[0].assigned_role,
        logpose_types::NodeRole::Data
    );
    assert_eq!(status.collections[0].route_kind, "local");
    assert_eq!(placement.assigned_node, "local");
    assert_eq!(placement.assigned_role, logpose_types::NodeRole::Data);
    assert_eq!(placement.route_kind, "local");
}

#[tokio::test]
async fn local_control_assignments_still_reject_data_plane_operations() {
    let root = unique_temp_dir("local-control-assignment");
    let engine = LocalStorageEngine::new(&root);
    engine
        .create_collection_with_assignment(
            CreateCollectionRequest {
                database_name: "default".to_owned(),
                name: "documents".to_owned(),
                dimensions: 2,
                metric: DistanceMetric::Dot,
            },
            CollectionAssignment {
                assigned_node: "local-control-assignment".to_owned(),
                assigned_role: logpose_types::NodeRole::Control,
            },
        )
        .await
        .expect("collection should be created");

    let state = Arc::new(AppState::new(test_config_with_root(
        "local-control-assignment",
        logpose_types::NodeRole::Combined,
        root,
    )));
    let placement = state
        .control
        .collection_placement("documents")
        .await
        .expect("placement should load");

    assert_eq!(placement.assigned_role, logpose_types::NodeRole::Control);
    assert_eq!(placement.route_kind, "local");

    let errors = vec![
        state
            .write(
                "documents",
                vec![WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: serde_json::json!({"kind":"keep"}),
                })],
            )
            .await
            .expect_err("write should be rejected")
            .to_string(),
        state
            .stats("documents")
            .await
            .expect_err("stats should be rejected")
            .to_string(),
        state
            .inspect("documents", InspectTarget::Manifest)
            .await
            .expect_err("inspect should be rejected")
            .to_string(),
    ];

    for error in errors {
        assert!(
            error.contains("not locally served"),
            "unexpected error: {error}"
        );
    }
}

#[tokio::test]
async fn runtime_status_aggregates_pending_and_error_maintenance_counts() {
    let state = Arc::new(AppState::new(test_config("control-maintenance")));
    let descriptor = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("collection should be created");

    fs::write(
        descriptor.root_path.join("maintenance.json"),
        serde_json::to_vec_pretty(&MaintenanceStatus {
            pending: vec!["flush".to_owned(), "compact".to_owned()],
            in_progress: None,
            last_error: Some("disk full".to_owned()),
            completed_runs: 3,
        })
        .expect("maintenance json should serialize"),
    )
    .expect("maintenance file should be written");

    let status = state
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");

    assert_eq!(status.maintenance.collections_with_pending, 1);
    assert_eq!(status.maintenance.pending_operations, 2);
    assert_eq!(status.maintenance.collections_in_progress, 0);
    assert_eq!(status.maintenance.collections_with_errors, 1);
}

#[tokio::test]
async fn runtime_status_distinguishes_duplicate_namespaced_maintenance_backlogs() {
    let state = Arc::new(AppState::new(test_config("control-maintenance-namespace")));
    let default_descriptor = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "default".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("default namespace collection should be created");
    let analytics_descriptor = state
        .control
        .create_collection(CreateCollectionRequest {
            database_name: "analytics".to_owned(),
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
        .await
        .expect("database namespace collection should be created");

    fs::write(
        default_descriptor.root_path.join("maintenance.json"),
        serde_json::to_vec_pretty(&MaintenanceStatus {
            pending: vec!["flush".to_owned()],
            in_progress: None,
            last_error: None,
            completed_runs: 0,
        })
        .expect("default maintenance json should serialize"),
    )
    .expect("default maintenance file should be written");
    fs::write(
        analytics_descriptor.root_path.join("maintenance.json"),
        serde_json::to_vec_pretty(&MaintenanceStatus {
            pending: vec!["compact".to_owned()],
            in_progress: None,
            last_error: Some("disk full".to_owned()),
            completed_runs: 0,
        })
        .expect("database maintenance json should serialize"),
    )
    .expect("database maintenance file should be written");

    let status = state
        .control
        .runtime_status()
        .await
        .expect("runtime status should load");

    assert_eq!(status.collection_count, 2);
    assert_eq!(status.maintenance.collections_with_pending, 2);
    assert_eq!(status.maintenance.pending_operations, 2);
    assert_eq!(status.maintenance.collections_in_progress, 0);
    assert_eq!(status.maintenance.collections_with_errors, 1);
}

#[tokio::test]
async fn control_plane_round_trips_default_database_access_policy_after_restart() {
    let root = unique_temp_dir("control-policy-restart");
    let initial = Arc::new(AppState::new(test_config_with_root(
        "control-policy-restart",
        logpose_types::NodeRole::Combined,
        root.clone(),
    )));
    let policy = sample_policy("default");

    initial
        .control
        .set_database_access_policy(policy.clone())
        .await
        .expect("policy should be written");

    let before_restart = initial
        .control
        .database_access_policy("default")
        .await
        .expect("policy should load");
    drop(initial);

    let restarted = Arc::new(AppState::new(test_config_with_root(
        "control-policy-restart",
        logpose_types::NodeRole::Combined,
        root,
    )));
    let after_restart = restarted
        .control
        .database_access_policy("default")
        .await
        .expect("policy should survive restart");

    assert_eq!(before_restart, policy);
    assert_eq!(after_restart, policy);
}

#[tokio::test]
async fn control_plane_reads_database_access_policies_by_database_name() {
    let state = Arc::new(AppState::new(test_config("control-policy-namespace")));
    let default_policy = sample_policy("default");
    let analytics_policy = sample_policy("analytics");

    state
        .control
        .set_database_access_policy(default_policy.clone())
        .await
        .expect("default policy should be written");
    state
        .control
        .set_database_access_policy(analytics_policy.clone())
        .await
        .expect("analytics policy should be written");

    let read_default = state
        .control
        .database_access_policy("default")
        .await
        .expect("default policy should load");
    let read_analytics = state
        .control
        .database_access_policy("analytics")
        .await
        .expect("analytics policy should load by database name");

    assert_eq!(read_default, default_policy);
    assert_eq!(read_analytics, analytics_policy);
    assert_ne!(read_default, read_analytics);
}

#[tokio::test]
async fn data_only_nodes_reject_database_policy_mutation_while_control_only_nodes_accept_it() {
    let policy = sample_policy("default");
    let data_state = Arc::new(AppState::new(test_config_with_role(
        "control-policy-data-only",
        logpose_types::NodeRole::Data,
    )));

    let data_error = data_state
        .control
        .set_database_access_policy(policy.clone())
        .await
        .expect_err("data-only node should reject policy mutation");

    assert!(
        data_error
            .to_string()
            .contains("data-only nodes cannot accept control-plane database policy mutations"),
        "unexpected error: {data_error}"
    );

    let root = unique_temp_dir("control-policy-control-only");
    let combined = Arc::new(AppState::new(test_config_with_root(
        "control-policy-control-only",
        logpose_types::NodeRole::Combined,
        root.clone(),
    )));
    combined
        .control
        .set_database_access_policy(policy.clone())
        .await
        .expect("combined node should seed policy");
    drop(combined);

    let control_state = Arc::new(AppState::new(test_config_with_root(
        "control-policy-control-only",
        logpose_types::NodeRole::Control,
        root,
    )));
    let read_policy = control_state
        .control
        .database_access_policy("default")
        .await
        .expect("control-only node should read policy");

    assert_eq!(read_policy, policy);
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-control-{label}-{suffix}"));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}

fn test_config(label: &str) -> logpose_config::LogPoseConfig {
    test_config_with_role(label, logpose_types::NodeRole::Combined)
}

fn test_config_with_role(
    label: &str,
    node_role: logpose_types::NodeRole,
) -> logpose_config::LogPoseConfig {
    test_config_with_root(label, node_role, unique_temp_dir(label))
}

fn test_config_with_root(
    label: &str,
    node_role: logpose_types::NodeRole,
    storage_root: PathBuf,
) -> logpose_config::LogPoseConfig {
    logpose_config::LogPoseConfig {
        node_name: label.to_owned(),
        node_role,
        storage_root,
        ..logpose_config::LogPoseConfig::default()
    }
}

fn sample_policy(database_name: &str) -> DatabaseAccessPolicy {
    DatabaseAccessPolicy {
        database_name: database_name.to_owned(),
        authentication_mode: AuthenticationMode::ExternalToken,
        role_bindings: vec![
            DatabaseRoleBinding {
                database_name: database_name.to_owned(),
                principal_name: Principal::new_with_access_tier(
                    "ops-admin",
                    PrincipalKind::User,
                    AccessTier::Operator,
                )
                .name,
                role: DatabaseRole::Owner,
            },
            DatabaseRoleBinding {
                database_name: database_name.to_owned(),
                principal_name: Principal::new_with_access_tier(
                    "reader-service",
                    PrincipalKind::Service,
                    AccessTier::Service,
                )
                .name,
                role: DatabaseRole::ReadOnly,
            },
        ],
    }
}
