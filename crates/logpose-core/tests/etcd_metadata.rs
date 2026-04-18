//! End-to-end etcd metadata integration coverage for `AppState`.

use etcd_client::{Client, DeleteOptions};
use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
    Principal, PrincipalKind,
};
use logpose_catalog as _;
use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
use logpose_core::{AppState, RequestAuth};
use logpose_query as _;
use logpose_service::ServiceError;
use logpose_storage::CreateCollectionRequest;
use logpose_storage_etcd::EtcdCatalogStore;
use logpose_types::{
    DistanceMetric, EtcdMetadataConfig, MetadataBackend, MetadataConfig, PutRecord, RecordId,
    WriteOperation,
};
use serde as _;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[tokio::test]
async fn etcd_metadata_backend_surfaces_remote_collections_across_nodes() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("remote-discovery");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let root_a = unique_temp_dir("etcd-node-a");
    let root_b = unique_temp_dir("etcd-node-b");
    let cluster_name = "core-etcd-metadata";

    let state_a = Arc::new(AppState::new(test_config(
        "node-a",
        root_a,
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    let descriptor = state_a
        .control
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created through authoritative metadata");

    let state_b = Arc::new(AppState::new(test_config(
        "node-b",
        root_b,
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    state_a
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("authoritative owner should serve local writes");
    let local_stats = state_a
        .stats("documents")
        .await
        .expect("authoritative owner should serve local stats");
    let local_runtime = state_a
        .control
        .runtime_status()
        .await
        .expect("healthy etcd-backed owner should report runtime status");
    let remote_descriptor = state_b
        .get_collection("documents")
        .await
        .expect("remote node should resolve the authoritative descriptor");
    let placement = state_b
        .control
        .collection_placement("documents")
        .await
        .expect("remote node should resolve recorded placement");
    let runtime = state_b
        .control
        .runtime_status()
        .await
        .expect("runtime status should list authoritative metadata");
    let stats_error = state_b
        .stats("documents")
        .await
        .expect_err("remote node must reject non-local data-plane operations");

    assert_eq!(remote_descriptor.collection_id, descriptor.collection_id);
    assert_eq!(remote_descriptor.lookup_name(), "default/documents");
    assert_eq!(local_stats.live_record_count, 1);
    assert!(local_runtime.control_plane_ready);
    assert!(local_runtime.data_plane_ready);
    assert_eq!(placement.collection_id, descriptor.collection_id);
    assert_eq!(placement.assigned_node, "node-a");
    assert_eq!(placement.route_kind, "recorded");
    assert!(runtime.control_plane_ready);
    assert!(runtime.data_plane_ready);
    assert_eq!(runtime.collections.len(), 1);
    assert_eq!(runtime.collections[0].collection_name, "documents");
    assert_eq!(runtime.collections[0].assigned_node, "node-a");
    assert_eq!(runtime.collections[0].route_kind, "recorded");
    assert!(matches!(
        stats_error,
        ServiceError::InvalidArgument(message) if message.contains("not locally served")
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_metadata_backend_shares_database_policies_across_nodes() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("shared-database-policies");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let root_a = unique_temp_dir("etcd-policy-node-a");
    let root_b = unique_temp_dir("etcd-policy-node-b");
    let cluster_name = "core-etcd-auth-metadata";
    let bootstrap_tokens = vec![
        BootstrapTokenConfig {
            token: "operator-token".to_owned(),
            principal: Principal::new_with_access_tier(
                "ops-admin",
                PrincipalKind::User,
                AccessTier::Operator,
            ),
        },
        BootstrapTokenConfig {
            token: "reader-token".to_owned(),
            principal: Principal::new_with_access_tier(
                "reader-service",
                PrincipalKind::Service,
                AccessTier::Service,
            ),
        },
    ];

    let state_a = Arc::new(AppState::new(test_config_with_auth(
        "policy-node-a",
        root_a,
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens.clone(),
    )));
    state_a
        .put_database_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            logpose_catalog::DatabaseDescriptor::new("analytics"),
        )
        .await
        .expect("database descriptor should persist through shared metadata");
    state_a
        .set_database_access_policy_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            DatabaseAccessPolicy {
                database_name: "analytics".to_owned(),
                authentication_mode: AuthenticationMode::ExternalToken,
                role_bindings: vec![DatabaseRoleBinding {
                    database_name: "analytics".to_owned(),
                    principal_name: "reader-service".to_owned(),
                    role: DatabaseRole::ReadOnly,
                }],
            },
        )
        .await
        .expect("database policy should persist through shared metadata");
    state_a
        .create_collection_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            CreateCollectionRequest::in_database("analytics", "documents", 2, DistanceMetric::Dot),
        )
        .await
        .expect("collection should be created through shared metadata");

    let state_b = Arc::new(AppState::new(test_config_with_auth(
        "policy-node-b",
        root_b,
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens,
    )));
    let descriptor = state_b
        .get_collection_with_auth(
            &RequestAuth::bearer_token("reader-token"),
            "analytics/documents",
        )
        .await
        .expect("reader token should resolve shared database policy on another node");
    let policy = state_b
        .database_access_policy_with_auth(&RequestAuth::bearer_token("operator-token"), "analytics")
        .await
        .expect("operator should read the shared database policy on another node");

    assert_eq!(descriptor.database_name, "analytics");
    assert_eq!(descriptor.name, "documents");
    assert_eq!(policy.database_name, "analytics");
    assert_eq!(policy.role_bindings.len(), 1);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_metadata_backend_reads_shared_principal_overrides_across_nodes() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("shared-principal-overrides");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let root_a = unique_temp_dir("etcd-principal-node-a");
    let root_b = unique_temp_dir("etcd-principal-node-b");
    let cluster_name = "core-etcd-shared-principals";
    let bootstrap_tokens = vec![BootstrapTokenConfig {
        token: "operator-token".to_owned(),
        principal: Principal::new_with_access_tier(
            "ops-admin",
            PrincipalKind::User,
            AccessTier::Operator,
        ),
    }];
    let config_a = test_config_with_auth(
        "principal-node-a",
        root_a,
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens.clone(),
    );
    let config_b = test_config_with_auth(
        "principal-node-b",
        root_b,
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens,
    );
    let shared_catalog = EtcdCatalogStore::new(config_a.metadata.etcd.clone())
        .expect("etcd catalog store should be constructed");

    let _state_a = Arc::new(AppState::new(config_a));
    let state_b = Arc::new(AppState::new(config_b));
    shared_catalog
        .put_principal(Principal::new_with_access_tier(
            "ops-admin",
            PrincipalKind::User,
            AccessTier::Observer,
        ))
        .await
        .expect("shared principal override should persist through etcd");

    let error = state_b
        .put_database_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            logpose_catalog::DatabaseDescriptor::new("analytics"),
        )
        .await
        .expect_err("shared persisted principal should override bootstrap operator tier");

    assert!(matches!(
        error,
        ServiceError::PermissionDenied(message)
            if message.contains("not allowed to perform operator actions")
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_collection_creation_seeds_shared_database_metadata() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("shared-database-seeding");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let root_a = unique_temp_dir("etcd-seeded-database-node-a");
    let root_b = unique_temp_dir("etcd-seeded-database-node-b");
    let cluster_name = "core-etcd-shared-database-seeding";
    let bootstrap_tokens = vec![BootstrapTokenConfig {
        token: "operator-token".to_owned(),
        principal: Principal::new_with_access_tier(
            "ops-admin",
            PrincipalKind::User,
            AccessTier::Operator,
        ),
    }];

    let state_a = Arc::new(AppState::new(test_config_with_auth(
        "seed-node-a",
        root_a,
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens.clone(),
    )));
    state_a
        .create_collection_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            CreateCollectionRequest::in_database("analytics", "documents", 2, DistanceMetric::Dot),
        )
        .await
        .expect("collection creation should seed shared database metadata");

    let state_b = Arc::new(AppState::new(test_config_with_auth(
        "seed-node-b",
        root_b,
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens,
    )));
    let database = state_b
        .database_with_auth(&RequestAuth::bearer_token("operator-token"), "analytics")
        .await
        .expect("shared database metadata should be readable from another node");
    let databases = state_b
        .databases_with_auth(&RequestAuth::bearer_token("operator-token"))
        .await
        .expect("shared database list should include seeded namespaces");

    assert_eq!(database.name, "analytics");
    assert!(
        databases
            .iter()
            .any(|descriptor| descriptor.name == "analytics")
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_data_only_nodes_reject_catalog_mutations() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("data-node-catalog-mutations");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-data-node-mutations";
    let bootstrap_tokens = vec![BootstrapTokenConfig {
        token: "operator-token".to_owned(),
        principal: Principal::new_with_access_tier(
            "ops-admin",
            PrincipalKind::User,
            AccessTier::Operator,
        ),
    }];
    let mut combined_config = test_config_with_auth(
        "combined-node",
        unique_temp_dir("etcd-combined-catalog-node"),
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens.clone(),
    );
    combined_config.node_role = logpose_types::NodeRole::Combined;
    let combined = Arc::new(AppState::new(combined_config));
    combined
        .put_database_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            logpose_catalog::DatabaseDescriptor::new("analytics"),
        )
        .await
        .expect("combined node should seed the shared database");

    let mut data_config = test_config_with_auth(
        "data-node",
        unique_temp_dir("etcd-data-catalog-node"),
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens,
    );
    data_config.node_role = logpose_types::NodeRole::Data;
    let data_node = Arc::new(AppState::new(data_config));

    let database_error = data_node
        .put_database_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            logpose_catalog::DatabaseDescriptor::new("events"),
        )
        .await
        .expect_err("data-only nodes must reject shared database mutations");
    let policy_error = data_node
        .set_database_access_policy_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            DatabaseAccessPolicy {
                database_name: "analytics".to_owned(),
                authentication_mode: AuthenticationMode::ExternalToken,
                role_bindings: Vec::new(),
            },
        )
        .await
        .expect_err("data-only nodes must reject shared policy mutations");

    assert!(matches!(
        database_error,
        ServiceError::InvalidArgument(message)
            if message.contains("data-only nodes cannot accept control-plane database mutations")
    ));
    assert!(matches!(
        policy_error,
        ServiceError::InvalidArgument(message)
            if message.contains("data-only nodes cannot accept control-plane database mutations")
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

fn test_config(
    node_name: &str,
    storage_root: PathBuf,
    endpoints: &[String],
    key_prefix: &str,
    cluster_name: &str,
) -> LogPoseConfig {
    LogPoseConfig {
        node_name: node_name.to_owned(),
        storage_root,
        metadata: MetadataConfig {
            backend: MetadataBackend::Etcd,
            etcd: EtcdMetadataConfig {
                endpoints: endpoints.to_vec(),
                key_prefix: key_prefix.to_owned(),
                timeout_ms: 1_500,
                membership_ttl_secs: 15,
                leadership_ttl_secs: 10,
                cluster_name: cluster_name.to_owned(),
            },
        },
        ..LogPoseConfig::default()
    }
}

fn test_config_with_auth(
    node_name: &str,
    storage_root: PathBuf,
    endpoints: &[String],
    key_prefix: &str,
    cluster_name: &str,
    bootstrap_tokens: Vec<BootstrapTokenConfig>,
) -> LogPoseConfig {
    let mut config = test_config(node_name, storage_root, endpoints, key_prefix, cluster_name);
    config.auth.bootstrap_tokens = bootstrap_tokens;
    config
}

fn test_etcd_endpoints() -> Vec<String> {
    std::env::var("LOGPOSE_TEST_ETCD_ENDPOINTS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|endpoints| !endpoints.is_empty())
        .unwrap_or_else(|| vec!["http://127.0.0.1:2379".to_owned()])
}

async fn cleanup_prefix(endpoints: &[String], key_prefix: &str) {
    if let Ok(mut client) = Client::connect(endpoints.to_vec(), None).await {
        let _ = client
            .delete(key_prefix, Some(DeleteOptions::new().with_prefix()))
            .await;
    }
}

fn unique_etcd_prefix(label: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    format!("/logpose/tests/{label}/{suffix}")
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("logpose-core-{label}-{suffix}"));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}
