//! End-to-end etcd metadata integration coverage for `AppState`.

use etcd_client::{Client, DeleteOptions};
use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
    Principal, PrincipalKind,
};
use logpose_catalog::CollectionDescriptor;
use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
use logpose_core::{AppState, RequestAuth};
use logpose_query as _;
use logpose_service::ServiceError;
use logpose_storage::CreateCollectionRequest;
use logpose_storage_etcd::{EtcdCatalogStore, EtcdCoordinationClient, PromotionResult};
use logpose_types::{
    CollectionAssignment, CollectionRef, DistanceMetric, EtcdMetadataConfig, MetadataBackend,
    MetadataConfig, NodeRole, PutRecord, RecordId, WriteOperation,
};
use serde as _;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{Instant, sleep};

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
    assert_eq!(placement.owner_node.as_deref(), Some("node-a"));
    assert_eq!(placement.ownership_epoch, Some(1));
    assert_eq!(placement.route_kind, "recorded");
    assert!(!runtime.control_plane_ready);
    assert!(runtime.data_plane_ready);
    assert_eq!(runtime.collections.len(), 1);
    assert_eq!(runtime.collections[0].collection_name, "documents");
    assert_eq!(runtime.collections[0].assigned_node, "node-a");
    assert_eq!(runtime.collections[0].owner_node.as_deref(), Some("node-a"));
    assert_eq!(runtime.collections[0].ownership_epoch, Some(1));
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

#[tokio::test]
async fn etcd_runtime_status_surfaces_membership_and_controller_leader() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("runtime-status-coordination");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-runtime-status";

    let mut combined_config = test_config(
        "coordinator-a",
        unique_temp_dir("etcd-runtime-status-coordinator"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    combined_config.node_role = NodeRole::Combined;
    let combined = Arc::new(AppState::new(combined_config));

    let mut data_config = test_config(
        "data-b",
        unique_temp_dir("etcd-runtime-status-data"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    data_config.node_role = NodeRole::Data;
    let data = Arc::new(AppState::new(data_config));

    let combined_status = wait_for_runtime_status(&combined, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("coordinator-a")
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "coordinator-a")
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "data-b")
        })
    })
    .await;
    let data_status = wait_for_runtime_status(&data, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("coordinator-a")
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "coordinator-a")
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "data-b")
        })
    })
    .await;

    assert!(combined_status.control_plane_ready);
    assert!(combined_status.data_plane_ready);
    assert!(data_status.data_plane_ready);
    assert!(!data_status.control_plane_ready);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_membership_leases_expire_after_state_drop() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("membership-expiry-after-drop");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-membership-expiry";

    let mut config = test_config(
        "coordinator-a",
        unique_temp_dir("etcd-membership-expiry"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    config.node_role = NodeRole::Combined;
    config.metadata.etcd.membership_ttl_secs = 2;
    config.metadata.etcd.leadership_ttl_secs = 2;
    let state = Arc::new(AppState::new(config.clone()));

    wait_for_runtime_status(&state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered && coordination.is_local_leader
        })
    })
    .await;

    drop(state);

    let coordination = EtcdCoordinationClient::new(config.metadata.etcd.clone())
        .expect("coordination client should build");
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let members = coordination
            .list_membership()
            .await
            .expect("membership list should stay readable");
        let leader = coordination
            .current_leader()
            .await
            .expect("leader lookup should stay readable");
        if members.is_empty() && leader.is_none() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for membership and leadership lease expiry after state drop: members={members:?} leader={leader:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_follower_nodes_reject_control_plane_mutations() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("follower-control-plane-gate");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-leader-gate";
    let bootstrap_tokens = vec![BootstrapTokenConfig {
        token: "operator-token".to_owned(),
        principal: Principal::new_with_access_tier(
            "ops-admin",
            PrincipalKind::User,
            AccessTier::Operator,
        ),
    }];

    let mut leader_config = test_config_with_auth(
        "leader-a",
        unique_temp_dir("etcd-leader-gate-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens.clone(),
    );
    leader_config.node_role = NodeRole::Combined;
    let leader = Arc::new(AppState::new(leader_config));
    wait_for_runtime_status(&leader, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.is_local_leader)
    })
    .await;

    let mut follower_config = test_config_with_auth(
        "follower-b",
        unique_temp_dir("etcd-leader-gate-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config));
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("leader-a")
        })
    })
    .await;
    let follower_status = follower
        .control
        .runtime_status()
        .await
        .expect("follower runtime status should load");

    let collection_error = follower
        .control
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect_err("follower should reject direct control-plane collection mutations");
    let database_error = follower
        .put_database_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            logpose_catalog::DatabaseDescriptor::new("analytics"),
        )
        .await
        .expect_err("follower should reject shared database mutations");

    assert!(matches!(
        collection_error,
        ServiceError::InvalidArgument(message)
            if message.contains("not the active control-plane leader")
    ));
    assert!(!follower_status.control_plane_ready);
    assert!(follower_status.data_plane_ready);
    assert!(matches!(
        database_error,
        ServiceError::InvalidArgument(message)
            if message.contains("not the active control-plane leader")
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_promotion_fences_the_old_owner() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("owner-promotion-fence");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-owner-promotion";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-owner-promotion-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    let owner = Arc::new(AppState::new(owner_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.is_local_leader)
    })
    .await;

    let mut follower_config = test_config(
        "owner-b",
        unique_temp_dir("etcd-owner-promotion-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config.clone()));
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-a")
        })
    })
    .await;

    owner
        .control
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created by the owner");

    owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("current owner should accept writes before promotion");

    let descriptor = owner
        .get_collection("documents")
        .await
        .expect("owner descriptor should load before promotion");
    mirror_collection_state(
        &owner_config.storage_root,
        &follower_config.storage_root,
        &descriptor.root_path,
    );

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let current = coordination
        .shard_owner(&CollectionRef::new_default("documents"), "0")
        .await
        .expect("owner lookup should succeed")
        .expect("owner record should be seeded");
    assert_eq!(current.owner_node_id, "owner-a");
    assert_eq!(current.epoch, 1);

    let promoted = coordination
        .promote_shard_owner(&current, "owner-b")
        .await
        .expect("promotion should succeed");
    assert!(
        matches!(promoted, PromotionResult::Applied(_)),
        "promotion should apply on the first attempt"
    );
    let promoted = match promoted {
        PromotionResult::Applied(promoted) => promoted,
        PromotionResult::Conflict => unreachable!("promotion was asserted to apply"),
    };
    assert_eq!(promoted.owner_node_id, "owner-b");
    assert_eq!(promoted.epoch, 2);
    let stale_attempt = coordination
        .promote_shard_owner(&current, "owner-c")
        .await
        .expect("stale promotion attempt should return a conflict result");
    assert!(matches!(stale_attempt, PromotionResult::Conflict));

    let owner_placement = owner
        .control
        .collection_placement("documents")
        .await
        .expect("owner placement should still load");
    let follower_placement = follower
        .control
        .collection_placement("documents")
        .await
        .expect("follower placement should load");
    let owner_status = owner
        .control
        .runtime_status()
        .await
        .expect("owner runtime status should load");
    let follower_status = follower
        .control
        .runtime_status()
        .await
        .expect("follower runtime status should load");
    let owner_error = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect_err("promoted old owner must reject writes");
    let follower_ack = follower
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("gamma"),
                vector: vec![0.5, 0.5],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("promoted owner with local state should accept writes");
    let owner_stats_error = owner
        .stats("documents")
        .await
        .expect_err("promoted old owner must reject reads");
    let follower_stats = follower
        .stats("documents")
        .await
        .expect("promoted owner should serve reads after promotion");

    assert_eq!(owner_placement.owner_node.as_deref(), Some("owner-b"));
    assert_eq!(owner_placement.ownership_epoch, Some(2));
    assert_eq!(owner_placement.route_kind, "recorded");
    assert_eq!(follower_placement.owner_node.as_deref(), Some("owner-b"));
    assert_eq!(follower_placement.ownership_epoch, Some(2));
    assert_eq!(follower_placement.route_kind, "local");
    assert_eq!(follower_ack.last_seq_no, 2);
    assert_eq!(follower_stats.live_record_count, 2);
    assert_eq!(
        owner_status.collections[0].owner_node.as_deref(),
        Some("owner-b")
    );
    assert_eq!(owner_status.collections[0].ownership_epoch, Some(2));
    assert_eq!(owner_status.collections[0].route_kind, "recorded");
    assert_eq!(
        follower_status.collections[0].owner_node.as_deref(),
        Some("owner-b")
    );
    assert_eq!(follower_status.collections[0].ownership_epoch, Some(2));
    assert_eq!(follower_status.collections[0].route_kind, "local");
    assert!(
        matches!(owner_error, ServiceError::InvalidArgument(ref message) if message.contains("not locally served")),
        "old owner should be fenced by ownership: {owner_error:?}"
    );
    assert!(
        matches!(owner_stats_error, ServiceError::InvalidArgument(ref message) if message.contains("not locally served")),
        "old owner should reject reads after promotion: {owner_stats_error:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_promotion_conflicts_while_descriptor_is_pending() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("owner-promotion-pending");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-metadata";
    let collection = CollectionRef::new_default("documents");
    let descriptor = CollectionDescriptor::new_in_database(
        "default",
        "documents",
        2,
        DistanceMetric::Dot,
        unique_temp_dir("etcd-owner-promotion-pending").as_path(),
    )
    .without_root_path();
    let assignment = CollectionAssignment {
        assigned_node: "owner-a".to_owned(),
        assigned_role: NodeRole::Data,
    };

    let assignment_key = format!(
        "{key_prefix}/clusters/{cluster_name}/collections/{}/assignment",
        collection.lookup_name()
    );
    let descriptor_key = format!(
        "{key_prefix}/clusters/{cluster_name}/collections/{}/descriptor",
        collection.lookup_name()
    );
    let owner_key = format!(
        "{key_prefix}/clusters/{cluster_name}/collections/{}/shards/0/owner",
        collection.lookup_name()
    );

    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("etcd client should connect");
    client
        .put(
            assignment_key,
            serde_json::to_string(&assignment).expect("assignment should serialize"),
            None,
        )
        .await
        .expect("assignment metadata should be seeded");
    client
        .put(
            descriptor_key,
            serde_json::json!({
                "descriptor": descriptor,
                "ready": false,
            })
            .to_string(),
            None,
        )
        .await
        .expect("pending descriptor metadata should be seeded");
    client
        .put(
            owner_key,
            serde_json::json!({
                "database_name": "default",
                "collection_name": "documents",
                "shard_id": "0",
                "owner_node_id": "owner-a",
                "epoch": 1,
            })
            .to_string(),
            None,
        )
        .await
        .expect("owner metadata should be seeded");

    let coordination = EtcdCoordinationClient::new(EtcdMetadataConfig {
        endpoints: endpoints.clone(),
        key_prefix: key_prefix.clone(),
        timeout_ms: 1_500,
        membership_ttl_secs: 15,
        leadership_ttl_secs: 10,
        cluster_name: cluster_name.to_owned(),
    })
    .expect("coordination client should build");
    let current = coordination
        .shard_owner(&collection, "0")
        .await
        .expect("owner lookup should succeed")
        .expect("owner record should exist");

    let promotion = coordination
        .promote_shard_owner(&current, "owner-b")
        .await
        .expect("pending descriptors should return a conflict result");

    assert!(matches!(promotion, PromotionResult::Conflict));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_runtime_status_surfaces_coordination_errors_when_etcd_is_unreachable() {
    let root = unique_temp_dir("etcd-runtime-status-error");
    let mut config = LogPoseConfig {
        node_name: "unreachable-node".to_owned(),
        storage_root: root,
        metadata: MetadataConfig {
            backend: MetadataBackend::Etcd,
            etcd: EtcdMetadataConfig {
                endpoints: vec!["http://127.0.0.1:1".to_owned()],
                key_prefix: unique_etcd_prefix("runtime-status-error"),
                timeout_ms: 50,
                membership_ttl_secs: 2,
                leadership_ttl_secs: 2,
                cluster_name: "core-etcd-runtime-status-error".to_owned(),
            },
        },
        ..LogPoseConfig::default()
    };
    config.node_role = NodeRole::Combined;
    let state = Arc::new(AppState::new(config));

    let status = wait_for_runtime_status(&state, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.last_error.is_some())
    })
    .await;
    let coordination = status
        .coordination
        .expect("coordination state should be present for etcd backend");

    assert!(!status.control_plane_ready);
    assert!(!status.data_plane_ready);
    assert!(!coordination.membership_registered);
    assert!(coordination.registered_members.is_empty());
    assert!(coordination.leader_node.is_none());
    assert!(
        coordination
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("etcd metadata operation failed"))
    );
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

async fn wait_for_runtime_status(
    state: &AppState,
    ready: impl Fn(&logpose_types::NodeRuntimeStatus) -> bool,
) -> logpose_types::NodeRuntimeStatus {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = state
            .control
            .runtime_status()
            .await
            .expect("runtime status should be readable");
        if ready(&status) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for coordination-ready runtime status: {status:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }
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

fn mirror_collection_state(from_root: &Path, to_root: &Path, collection_root: &Path) {
    let relative = collection_root
        .strip_prefix(from_root)
        .expect("collection root should live under the source storage root");
    copy_dir_recursive(collection_root, &to_root.join(relative));
}

fn copy_dir_recursive(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("destination dir should be created");
    for entry in fs::read_dir(source).expect("source directory should be readable") {
        let entry = entry.expect("directory entry should load");
        let entry_type = entry.file_type().expect("entry type should load");
        let target = destination.join(entry.file_name());
        if entry_type.is_dir() {
            copy_dir_recursive(entry.path().as_path(), target.as_path());
        } else {
            fs::copy(entry.path(), &target).expect("file copy should succeed");
        }
    }
}
