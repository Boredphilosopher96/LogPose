//! End-to-end etcd metadata integration coverage for `AppState`.

use etcd_client::{Client, DeleteOptions};
use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
    Principal, PrincipalKind,
};
use logpose_catalog::CollectionDescriptor;
use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
use logpose_core::{AppState, RequestAuth};
use logpose_query::{ExplainMode, QueryRequest};
use logpose_service::ServiceError;
use logpose_storage::CreateCollectionRequest;
use logpose_storage_etcd::{
    EtcdCatalogStore, EtcdCoordinationClient, LeadershipRecord, PromotionResult, ShardReplicaReport,
};
use logpose_types::{
    CollectionAssignment, CollectionRef, DistanceMetric, EtcdMetadataConfig, LeadershipFence,
    MetadataBackend, MetadataConfig, NodeRole, PutRecord, RecordId, WriteOperation,
};
use serde as _;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{Instant, sleep, timeout};

static NEXT_TEST_PORT: AtomicUsize = AtomicUsize::new(20_000);

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
    assert!(
        local_runtime
            .coordination
            .as_ref()
            .and_then(|coordination| coordination.metadata_revision)
            .is_some_and(|revision| revision > 0)
    );
    assert_eq!(
        local_runtime
            .coordination
            .as_ref()
            .and_then(|coordination| coordination.watch_lag),
        Some(0)
    );
    assert_eq!(placement.collection_id, descriptor.collection_id);
    assert_eq!(placement.assigned_node, "node-a");
    assert_eq!(placement.owner_node.as_deref(), Some("node-a"));
    assert_eq!(placement.ownership_epoch, Some(1));
    assert!(
        placement
            .metadata_revision
            .is_some_and(|revision| revision > 0)
    );
    assert_eq!(placement.route_kind, "recorded");
    assert!(!runtime.control_plane_ready);
    assert!(runtime.data_plane_ready);
    assert_eq!(runtime.collections.len(), 1);
    assert_eq!(runtime.collections[0].collection_name, "documents");
    assert_eq!(runtime.collections[0].assigned_node, "node-a");
    assert_eq!(runtime.collections[0].owner_node.as_deref(), Some("node-a"));
    assert_eq!(runtime.collections[0].ownership_epoch, Some(1));
    assert!(
        runtime.collections[0]
            .metadata_revision
            .is_some_and(|revision| revision > 0)
    );
    assert!(
        runtime
            .coordination
            .as_ref()
            .and_then(|coordination| coordination.metadata_revision)
            .is_some_and(|revision| revision > 0)
    );
    assert_eq!(
        runtime
            .coordination
            .as_ref()
            .and_then(|coordination| coordination.watch_lag),
        Some(0)
    );
    assert_eq!(runtime.collections[0].route_kind, "recorded");
    assert!(matches!(
        stats_error,
        ServiceError::InvalidArgument(message) if message.contains("not locally served")
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_metadata_backend_surfaces_replica_targets_from_replication_factor() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("replica-targets");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-replica-targets";

    let state_a = Arc::new(AppState::new(test_config(
        "node-a",
        unique_temp_dir("etcd-replica-targets-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    let state_b = Arc::new(AppState::new(test_config(
        "node-b",
        unique_temp_dir("etcd-replica-targets-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    )));

    wait_for_runtime_status(&state_a, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered && coordination.registered_members.len() == 2
        })
    })
    .await;
    wait_for_runtime_status(&state_b, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered && coordination.registered_members.len() == 2
        })
    })
    .await;

    let leader_name = state_a
        .control
        .runtime_status()
        .await
        .expect("runtime status should load after membership stabilizes")
        .coordination
        .and_then(|coordination| coordination.leader_node)
        .expect("one node should be elected leader");
    let (leader, replica, replica_node_id) = if leader_name == "node-a" {
        (&state_a, &state_b, "node-b")
    } else {
        (&state_b, &state_a, "node-a")
    };

    leader
        .control
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created with replication factor 2");

    let leader_placement = wait_for_collection_placement(leader, "documents", |placement| {
        placement.replicas.len() == 1
    })
    .await;
    let replica_placement = wait_for_collection_placement(replica, "documents", |placement| {
        placement.replicas.len() == 1
    })
    .await;

    assert_eq!(
        leader_placement.owner_node.as_deref(),
        Some(leader_name.as_str())
    );
    assert_eq!(leader_placement.replicas.len(), 1);
    assert_eq!(leader_placement.replicas[0].node_id, replica_node_id);
    assert_eq!(leader_placement.replicas[0].node_role, NodeRole::Combined);
    assert_eq!(leader_placement.replicas[0].state, "unknown");

    assert_eq!(replica_placement.replicas.len(), 1);
    assert_eq!(replica_placement.replicas[0].node_id, replica_node_id);
    assert_eq!(replica_placement.replicas[0].state, "absent");

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
async fn etcd_new_node_registration_updates_visible_membership() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("node-registration");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-node-registration";

    let mut leader_config = test_config(
        "node-a",
        unique_temp_dir("etcd-node-registration-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    leader_config.node_role = NodeRole::Combined;
    let leader = Arc::new(AppState::new(leader_config));
    wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members.len() == 1
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "node-a")
        })
    })
    .await;

    let mut follower_config = test_config(
        "node-b",
        unique_temp_dir("etcd-node-registration-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config));
    wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.is_local_leader
                && coordination.registered_members.len() == 2
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "node-a")
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "node-b")
        })
    })
    .await;
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("node-a")
                && coordination.registered_members.len() == 2
        })
    })
    .await;

    let mut joining_config = test_config(
        "node-c",
        unique_temp_dir("etcd-node-registration-c"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    joining_config.node_role = NodeRole::Data;
    let joining = Arc::new(AppState::new(joining_config));

    let leader_status = wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.is_local_leader
                && coordination.registered_members.len() == 3
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "node-c")
        })
    })
    .await;
    let joining_status = wait_for_runtime_status(&joining, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("node-a")
                && coordination.registered_members.len() == 3
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "node-c")
        })
    })
    .await;

    assert!(
        leader_status.control_plane_ready,
        "leader should stay control-plane ready after new node registration"
    );
    assert!(
        joining_status.data_plane_ready,
        "joining node should report a data-plane-ready runtime status after registration"
    );

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
async fn etcd_rejoining_node_re_registers_membership_after_restart() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("membership-rejoin");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-membership-rejoin";

    let mut leader_config = test_config(
        "node-a",
        unique_temp_dir("etcd-membership-rejoin-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    leader_config.node_role = NodeRole::Combined;
    leader_config.metadata.etcd.membership_ttl_secs = 2;
    leader_config.metadata.etcd.leadership_ttl_secs = 2;
    let leader = Arc::new(AppState::new(leader_config));
    wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members.len() == 1
        })
    })
    .await;

    let follower_root = unique_temp_dir("etcd-membership-rejoin-b");
    let mut follower_config = test_config(
        "node-b",
        follower_root.clone(),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Data;
    follower_config.metadata.etcd.membership_ttl_secs = 2;
    follower_config.metadata.etcd.leadership_ttl_secs = 2;
    let follower = Arc::new(AppState::new(follower_config.clone()));
    wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.is_local_leader
                && coordination.registered_members.len() == 2
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "node-b")
        })
    })
    .await;

    drop(follower);
    let leader_after_drop = wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members.len() == 1
                && coordination
                    .registered_members
                    .iter()
                    .all(|member| member == "node-a")
        })
    })
    .await;

    let rejoining = Arc::new(AppState::new(follower_config));
    let leader_after_rejoin = wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members.len() == 2
                && coordination
                    .registered_members
                    .iter()
                    .any(|member| member == "node-b")
        })
    })
    .await;
    let rejoining_status = wait_for_runtime_status(&rejoining, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("node-a")
                && coordination.registered_members.len() == 2
        })
    })
    .await;

    assert!(
        leader_after_drop
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.registered_members == vec!["node-a"]),
        "leader should observe the follower membership lease expiry before rejoin"
    );
    assert!(
        leader_after_rejoin
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination
                .registered_members
                .iter()
                .any(|member| member == "node-b")),
        "leader should observe the rejoining node in visible membership"
    );
    assert!(rejoining_status.data_plane_ready);

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
async fn etcd_catalog_transactions_reject_stale_leaders_after_leadership_moves() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("stale-leader-catalog-fence");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-stale-leader-catalog-fence";
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
        unique_temp_dir("etcd-stale-leader-catalog-fence"),
        &endpoints,
        &key_prefix,
        cluster_name,
        bootstrap_tokens,
    );
    leader_config.node_role = NodeRole::Combined;
    let leader = Arc::new(AppState::new(leader_config.clone()));
    let leader_status = wait_for_runtime_status(&leader, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.is_local_leader)
    })
    .await;
    let leader_lease_id = leader_status
        .coordination
        .as_ref()
        .and_then(|coordination| coordination.leadership_lease_id)
        .expect("leader lease id should be visible");

    leader
        .put_database_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            logpose_catalog::DatabaseDescriptor::new("analytics"),
        )
        .await
        .expect("leader should seed one shared database");

    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("raw etcd client should connect");
    client
        .put(
            format!("{key_prefix}/clusters/{cluster_name}/controllers/leader"),
            serde_json::to_string(&LeadershipRecord {
                node_id: "leader-b".to_owned(),
                lease_id: 9_999,
            })
            .expect("leadership record should encode"),
            None,
        )
        .await
        .expect("leadership key should be replaceable for the stale-leader test");
    let demoted_status = wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("leader-b")
        }) && !status.control_plane_ready
            && status.data_plane_ready
    })
    .await;
    let app_database_error = leader
        .put_database_with_auth(
            &RequestAuth::bearer_token("operator-token"),
            logpose_catalog::DatabaseDescriptor::new("warehouse-app"),
        )
        .await
        .expect_err("demoted leader should reject app-layer database mutations");
    let app_policy_error = leader
        .control
        .set_database_access_policy(DatabaseAccessPolicy {
            database_name: "analytics".to_owned(),
            authentication_mode: AuthenticationMode::ExternalToken,
            role_bindings: Vec::new(),
        })
        .await
        .expect_err("demoted leader should reject app-layer policy mutations");

    let catalog = EtcdCatalogStore::new(leader_config.metadata.etcd.clone())
        .expect("shared catalog should build");
    let stale_database_error = catalog
        .put_database(
            logpose_catalog::DatabaseDescriptor::new("warehouse"),
            "leader-a",
            leader_lease_id,
        )
        .await
        .expect_err("stale leader should be fenced by the database txn");
    let stale_policy_error = catalog
        .put_database_access_policy(
            DatabaseAccessPolicy {
                database_name: "analytics".to_owned(),
                authentication_mode: AuthenticationMode::ExternalToken,
                role_bindings: vec![DatabaseRoleBinding {
                    database_name: "analytics".to_owned(),
                    principal_name: "ops-admin".to_owned(),
                    role: DatabaseRole::Owner,
                }],
            },
            "leader-a",
            leader_lease_id,
        )
        .await
        .expect_err("stale leader should be fenced by the policy txn");

    assert!(
        stale_database_error
            .to_string()
            .contains("not the active control-plane leader")
    );
    assert!(matches!(
        app_database_error,
        ServiceError::InvalidArgument(ref message)
            if message.contains("not the active control-plane leader")
    ));
    assert!(matches!(
        app_policy_error,
        ServiceError::InvalidArgument(ref message)
            if message.contains("not the active control-plane leader")
    ));
    assert!(
        stale_policy_error
            .to_string()
            .contains("not the active control-plane leader")
    );
    assert!(demoted_status.data_plane_ready);
    assert!(!demoted_status.control_plane_ready);

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
    follower
        .control
        .sync_local_replica_report("documents", None)
        .await
        .expect("follower replica report should publish after mirroring");
    let leader_fence = wait_for_leadership_fence(&owner).await;

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let current = coordination
        .shard_owner(&CollectionRef::new_default("documents"), "0")
        .await
        .expect("owner lookup should succeed")
        .expect("owner record should be seeded");
    assert_eq!(current.owner_node_id, "owner-a");
    assert_eq!(current.epoch, 1);
    let non_member_attempt = coordination
        .promote_shard_owner(&current, "owner-z", &leader_fence)
        .await
        .expect("non-member promotion attempt should return a conflict result");
    assert!(matches!(non_member_attempt, PromotionResult::Conflict));

    owner
        .control
        .drain_node("owner-a")
        .await
        .expect("owner should be drainable before direct promotion");
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;
    let leader = coordination
        .current_leader()
        .await
        .expect("leader lookup should succeed")
        .expect("replacement leader should exist after draining the owner");
    let leader_membership = coordination
        .membership(&leader.node_id)
        .await
        .expect("leader membership lookup should succeed")
        .expect("replacement leader membership should exist after draining the owner");
    let leader_fence = LeadershipFence {
        node_id: leader.node_id,
        lease_id: leader.lease_id,
        membership_lease_id: leader_membership.lease_id,
    };

    let promoted = coordination
        .promote_shard_owner(&current, "owner-b", &leader_fence)
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
        .promote_shard_owner(&current, "owner-c", &leader_fence)
        .await
        .expect("stale promotion attempt should return a conflict result");
    assert!(matches!(stale_attempt, PromotionResult::Conflict));

    let owner_placement = wait_for_collection_placement(&owner, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-b") && placement.ownership_epoch == Some(2)
    })
    .await;
    let follower_placement = wait_for_collection_placement(&follower, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-b")
            && placement.ownership_epoch == Some(2)
            && placement.route_kind == "local"
    })
    .await;
    let owner_status = wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection.owner_node.as_deref() == Some("owner-b")
                && collection.ownership_epoch == Some(2)
        })
    })
    .await;
    let follower_status = wait_for_runtime_status(&follower, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection.owner_node.as_deref() == Some("owner-b")
                && collection.ownership_epoch == Some(2)
                && collection.route_kind == "local"
        })
    })
    .await;
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
async fn etcd_owner_promotion_rejects_read_barriers_without_freshness_metadata() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("owner-promotion-read-barrier");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-owner-promotion-barrier";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-owner-promotion-barrier-a"),
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
        unique_temp_dir("etcd-owner-promotion-barrier-b"),
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

    let pre_promotion_ack = owner
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
    follower
        .control
        .sync_local_replica_report("documents", None)
        .await
        .expect("follower replica report should publish after mirroring");

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let current = coordination
        .shard_owner(&CollectionRef::new_default("documents"), "0")
        .await
        .expect("owner lookup should succeed")
        .expect("owner record should be seeded");
    owner
        .control
        .drain_node("owner-a")
        .await
        .expect("owner should be drainable before direct promotion");
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;
    let leader = coordination
        .current_leader()
        .await
        .expect("leader lookup should succeed")
        .expect("replacement leader should exist after draining the owner");
    let leader_membership = coordination
        .membership(&leader.node_id)
        .await
        .expect("leader membership lookup should succeed")
        .expect("replacement leader membership should exist after draining the owner");
    let leader_fence = LeadershipFence {
        node_id: leader.node_id,
        lease_id: leader.lease_id,
        membership_lease_id: leader_membership.lease_id,
    };
    let promoted = coordination
        .promote_shard_owner(&current, "owner-b", &leader_fence)
        .await
        .expect("promotion should succeed");
    assert!(matches!(promoted, PromotionResult::Applied(_)));
    wait_for_collection_placement(&follower, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-b")
            && placement.ownership_epoch == Some(2)
            && placement.route_kind == "local"
    })
    .await;

    let post_promotion_ack = follower
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("promoted owner with mirrored local state should accept writes");

    let query = follower
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            read_barrier: Some(pre_promotion_ack.snapshot.clone()),
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
        })
        .await
        .expect_err("promoted owner should fail closed on pre-promotion read barriers");
    let stats = follower
        .stats_for_read("documents", None, Some(pre_promotion_ack.snapshot.clone()))
        .await
        .expect_err("promoted owner should fail closed on stats read barriers");
    let post_promotion_query = follower
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            read_barrier: Some(post_promotion_ack.snapshot.clone()),
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
        })
        .await
        .expect_err("promoted owner should fail closed on post-promotion read barriers too");
    let post_promotion_stats = follower
        .stats_for_read("documents", None, Some(post_promotion_ack.snapshot.clone()))
        .await
        .expect_err("promoted owner should fail closed on post-promotion stats barriers too");
    let exact_snapshot_query = follower
        .query(QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: Some(post_promotion_ack.snapshot.clone()),
            read_barrier: None,
            filters: Vec::new(),
            predicate: None,
            explain: ExplainMode::None,
        })
        .await
        .expect("exact snapshots should remain readable after promotion");

    assert!(
        matches!(query, ServiceError::FailedPrecondition(ref message) if message.contains("cannot safely satisfy read barriers after promotion")),
        "promoted owner should explain the fail-closed read-barrier behavior: {query:?}"
    );
    assert!(
        matches!(stats, ServiceError::FailedPrecondition(ref message) if message.contains("cannot safely satisfy read barriers after promotion")),
        "promoted owner should explain the fail-closed stats behavior: {stats:?}"
    );
    assert!(
        matches!(post_promotion_query, ServiceError::FailedPrecondition(ref message) if message.contains("cannot safely satisfy read barriers after promotion")),
        "promoted owner should reject barriers minted after promotion too: {post_promotion_query:?}"
    );
    assert!(
        matches!(post_promotion_stats, ServiceError::FailedPrecondition(ref message) if message.contains("cannot safely satisfy read barriers after promotion")),
        "promoted owner should reject stats barriers minted after promotion too: {post_promotion_stats:?}"
    );
    assert_eq!(exact_snapshot_query.snapshot, post_promotion_ack.snapshot);
    assert_eq!(
        exact_snapshot_query
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_public_drain_fences_local_serving_until_undrain() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("public-drain-fence");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-public-drain";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-public-drain-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    let owner = Arc::new(AppState::new(owner_config));
    wait_for_runtime_status(&owner, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.is_local_leader)
    })
    .await;

    let mut follower_config = test_config(
        "owner-b",
        unique_temp_dir("etcd-public-drain-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config));
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
        .expect("collection should be created before draining");
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
        .expect("owner should accept writes before draining");

    let drained = owner
        .control
        .drain_node("owner-a")
        .await
        .expect("leader should be able to drain the local node");
    let stale_leader_error =
        owner.control.drain_node("owner-b").await.expect_err(
            "self-drained leaders must lose control-plane mutation authority immediately",
        );
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_state.as_deref() == Some("draining")
                && !status.control_plane_ready
                && !status.data_plane_ready
        })
    })
    .await;

    let drained_write = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect_err("drained owner must reject writes");
    let drained_stats = owner
        .stats("documents")
        .await
        .expect_err("drained owner must reject reads");

    assert_eq!(drained.node_id, "owner-a");
    assert_eq!(drained.state, "draining");
    assert!(
        matches!(stale_leader_error, ServiceError::InvalidArgument(ref message) if message.contains("not the active control-plane leader")),
        "self-drained leaders must not retain leader mutations after the drain call returns: {stale_leader_error:?}"
    );
    assert!(
        matches!(drained_write, ServiceError::InvalidArgument(ref message) if message.contains("not locally served")),
        "drained owners must stop serving writes: {drained_write:?}"
    );
    assert!(
        matches!(drained_stats, ServiceError::InvalidArgument(ref message) if message.contains("not locally served")),
        "drained owners must stop serving reads: {drained_stats:?}"
    );

    let ready = follower
        .control
        .undrain_node("owner-a")
        .await
        .expect("active leader should be able to restore the drained node");
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_state.as_deref() == Some("ready")
                && status.data_plane_ready
                && !status.control_plane_ready
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;

    let recovered_ack = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("gamma"),
                vector: vec![0.5, 0.5],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("undrained owner should resume serving writes");

    assert_eq!(ready.state, "ready");
    assert_eq!(recovered_ack.last_seq_no, 2);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_public_promotion_updates_assignment_and_owner() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("public-promotion-api");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-public-promotion";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-public-promotion-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    let owner = Arc::new(AppState::new(owner_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members == vec!["owner-a".to_owned()]
        })
    })
    .await;

    let mut follower_config = test_config(
        "owner-b",
        unique_temp_dir("etcd-public-promotion-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config.clone()));
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
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
        .expect("collection should be created before public promotion");
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
        .expect("owner should accept writes before public promotion");

    let descriptor = owner
        .get_collection("documents")
        .await
        .expect("owner descriptor should load before public promotion");
    mirror_collection_state(
        &owner_config.storage_root,
        &follower_config.storage_root,
        &descriptor.root_path,
    );
    follower
        .control
        .sync_local_replica_report("documents", None)
        .await
        .expect("follower replica report should publish after mirroring");
    owner
        .control
        .drain_node("owner-a")
        .await
        .expect("current owner should be drainable before manual promotion");
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;

    follower
        .control
        .promote_collection_owner("documents", "owner-b")
        .await
        .expect("public promotion API should succeed");
    let owner_error = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("gamma"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect_err("demoted owner must reject writes immediately after public promotion");
    let placement = wait_for_collection_placement(&follower, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-b")
            && placement.ownership_epoch == Some(2)
            && placement.assigned_node == "owner-b"
            && placement.route_kind == "local"
    })
    .await;
    let follower_ack = follower
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("promoted owner should accept writes after public promotion");
    let owner_status = wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection.owner_node.as_deref() == Some("owner-b")
                && collection.failover_reason.as_deref()
                    == Some("manual promotion to node 'owner-b' from node 'owner-a'")
        })
    })
    .await;
    let owner_status_placement = owner_status
        .collections
        .iter()
        .find(|collection| collection.collection_name == "documents")
        .expect("promoted placement should appear in owner status");

    assert_eq!(placement.assigned_node, "owner-b");
    assert_eq!(placement.owner_node.as_deref(), Some("owner-b"));
    assert_eq!(placement.ownership_epoch, Some(2));
    assert!(placement.replicas.is_empty());
    assert_eq!(
        owner_status_placement.failover_reason.as_deref(),
        Some("manual promotion to node 'owner-b' from node 'owner-a'")
    );
    assert_eq!(placement.route_kind, "local");
    assert_eq!(follower_ack.last_seq_no, 2);
    assert!(
        matches!(owner_error, ServiceError::InvalidArgument(ref message) if message.contains("not locally served")),
        "public promotion should fence the old owner: {owner_error:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_missing_owner_metadata_rejects_reads_until_reconciliation() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("missing-owner-read-fence");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-missing-owner-read-fence";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-missing-owner-read-fence-a"),
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
        .expect("owner should serve writes before owner metadata is removed");

    let owner_key = format!(
        "{key_prefix}/clusters/{cluster_name}/collections/{}/shards/0/owner",
        CollectionRef::new_default("documents").lookup_name()
    );
    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("raw etcd client should connect");
    client
        .delete(owner_key, None)
        .await
        .expect("owner metadata should be removable for the test");

    let placement = wait_for_collection_placement(&owner, "documents", |placement| {
        placement.route_kind == "recorded"
            && placement
                .route_reason
                .contains("ownership metadata is missing")
    })
    .await;
    let stats_error = owner
        .stats("documents")
        .await
        .expect_err("reads should fail closed when owner metadata is missing");

    assert_eq!(placement.route_kind, "recorded");
    assert!(
        placement
            .route_reason
            .contains("ownership metadata is missing")
    );
    assert!(
        matches!(stats_error, ServiceError::InvalidArgument(ref message) if message.contains("not locally served")),
        "missing owner metadata should fence reads until reconciliation: {stats_error:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_direct_promotion_conflicts_while_current_owner_remains_ready() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("direct-promotion-live-owner");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-direct-promotion-live-owner";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-direct-promotion-live-owner-a"),
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
        unique_temp_dir("etcd-direct-promotion-live-owner-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered && coordination.registered_members.len() == 2
        })
    })
    .await;
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.registered_members.len() == 2
        })
    })
    .await;

    owner
        .control
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before direct promotion");
    owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept the seed write");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b")
        })
    })
    .await;
    wait_for_ready_replica(&follower, "documents", "owner-a", "owner-b", 1).await;

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let ownership = coordination
        .shard_owner(&CollectionRef::new_default("documents"), "0")
        .await
        .expect("owner metadata should load")
        .expect("owner metadata should exist");
    let leader = coordination
        .current_leader()
        .await
        .expect("leader should load")
        .expect("leader should exist");
    let leader_membership = coordination
        .membership(&leader.node_id)
        .await
        .expect("leader membership should load")
        .expect("leader membership should exist");

    let promotion = coordination
        .promote_shard_owner(
            &ownership,
            "owner-b",
            &LeadershipFence {
                node_id: leader.node_id,
                lease_id: leader.lease_id,
                membership_lease_id: leader_membership.lease_id,
            },
        )
        .await
        .expect("direct promotion attempt should complete");

    assert!(matches!(promotion, PromotionResult::Conflict));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_direct_promotion_waits_for_owner_readiness_loss_even_with_fail_closed_owner_report() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("direct-promotion-fail-closed-owner");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-direct-promotion-fail-closed-owner";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-direct-promotion-fail-closed-owner-a"),
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
        unique_temp_dir("etcd-direct-promotion-fail-closed-owner-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered && coordination.registered_members.len() == 2
        })
    })
    .await;
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.registered_members.len() == 2
        })
    })
    .await;

    owner
        .control
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before direct promotion");
    let seed_ack = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept the seed write");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b")
        })
    })
    .await;
    let descriptor = owner
        .get_collection("documents")
        .await
        .expect("owner descriptor should load before promotion");
    wait_for_ready_replica(&follower, "documents", "owner-a", "owner-b", 1).await;

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let ownership = coordination
        .shard_owner(&descriptor.collection_ref(), "0")
        .await
        .expect("owner metadata should load")
        .expect("owner metadata should exist");
    let leader = coordination
        .current_leader()
        .await
        .expect("leader should load")
        .expect("leader should exist");
    let leader_membership = coordination
        .membership(&leader.node_id)
        .await
        .expect("leader membership should load")
        .expect("leader membership should exist");
    let owner_membership = coordination
        .membership("owner-a")
        .await
        .expect("owner membership should load")
        .expect("owner membership should exist");
    coordination
        .publish_shard_replica_report(
            &descriptor.collection_ref(),
            "0",
            &ShardReplicaReport {
                node_id: "owner-a".to_owned(),
                node_role: NodeRole::Combined,
                materialized: false,
                snapshot: Some(seed_ack.snapshot.clone()),
                ownership_epoch: Some(ownership.epoch),
                membership_mod_revision: None,
                mod_revision: 0,
            },
            owner_membership.lease_id,
            None,
        )
        .await
        .expect("owner fail-closed report should publish while membership remains ready");

    let still_conflicts = coordination
        .promote_shard_owner(
            &ownership,
            "owner-b",
            &LeadershipFence {
                node_id: leader.node_id,
                lease_id: leader.lease_id,
                membership_lease_id: leader_membership.lease_id,
            },
        )
        .await
        .expect("promotion attempt while owner remains ready should complete");
    assert!(
        matches!(still_conflicts, PromotionResult::Conflict),
        "ready owners must not be bypassed by a fail-closed report alone"
    );

    owner
        .control
        .drain_node("owner-a")
        .await
        .expect("owner should be drainable before direct promotion");
    let promoted_placement = wait_for_collection_placement(&follower, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-b")
            && placement.ownership_epoch == Some(2)
            && placement.route_kind == "local"
    })
    .await;
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;
    let follower_ack = follower
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("promoted follower should accept writes after the owner drains");

    assert_eq!(promoted_placement.owner_node.as_deref(), Some("owner-b"));
    assert_eq!(promoted_placement.ownership_epoch, Some(2));
    assert_eq!(follower_ack.last_seq_no, 2);
    assert!(
        promoted_placement.route_kind == "local",
        "fail-closed owner reports with a preserved snapshot should still allow safe promotion after the owner drains"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_direct_promotion_conflicts_when_candidate_membership_changed_after_report() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("direct-promotion-stale-candidate-report");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-direct-promotion-stale-candidate-report";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-direct-promotion-stale-candidate-report-a"),
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
        unique_temp_dir("etcd-direct-promotion-stale-candidate-report-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered && coordination.registered_members.len() == 2
        })
    })
    .await;
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered && coordination.registered_members.len() == 2
        })
    })
    .await;

    owner
        .control
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before stale candidate promotion");
    owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept the seed write");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b")
        })
    })
    .await;
    let descriptor = owner
        .get_collection("documents")
        .await
        .expect("owner descriptor should load before promotion");
    wait_for_ready_replica(&follower, "documents", "owner-a", "owner-b", 1).await;
    drop(follower);
    owner
        .control
        .drain_node("owner-a")
        .await
        .expect("owner should be drainable before direct promotion");
    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let candidate_membership = coordination
        .register_membership("owner-b", NodeRole::Combined)
        .await
        .expect(
            "candidate membership should be re-registered without republishing the stale report",
        );
    let ownership = coordination
        .shard_owner(&descriptor.collection_ref(), "0")
        .await
        .expect("owner metadata should load")
        .expect("owner metadata should exist");
    let leader = coordination
        .try_acquire_leadership("owner-b", candidate_membership.lease_id)
        .await
        .expect("leadership acquisition should succeed")
        .expect("candidate should acquire leadership after the old owner drains");

    let promotion = coordination
        .promote_shard_owner(
            &ownership,
            "owner-b",
            &LeadershipFence {
                node_id: leader.node_id,
                lease_id: leader.lease_id,
                membership_lease_id: candidate_membership.lease_id,
            },
        )
        .await
        .expect("direct promotion attempt should complete");

    assert!(matches!(promotion, PromotionResult::Conflict));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_loss_auto_promotes_local_leader_with_materialized_replica() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("auto-owner-failover");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-auto-owner-failover";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-auto-owner-failover-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    owner_config.metadata.etcd.membership_ttl_secs = 3;
    owner_config.metadata.etcd.leadership_ttl_secs = 3;
    let owner = Arc::new(AppState::new(owner_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members == vec!["owner-a".to_owned()]
        })
    })
    .await;

    let mut follower_config = test_config(
        "owner-b",
        unique_temp_dir("etcd-auto-owner-failover-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    follower_config.metadata.etcd.membership_ttl_secs = 3;
    follower_config.metadata.etcd.leadership_ttl_secs = 3;
    let follower = Arc::new(AppState::new(follower_config.clone()));

    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members.len() == 2
        })
    })
    .await;
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.registered_members.len() == 2
        })
    })
    .await;

    owner
        .control
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before owner loss");
    let write_ack = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("owner should accept writes before failover");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b")
        })
    })
    .await;
    wait_for_ready_replica(&follower, "documents", "owner-a", "owner-b", 1).await;

    drop(owner);

    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;
    let promoted_placement = wait_for_collection_placement(&follower, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-b")
            && placement.ownership_epoch == Some(2)
            && placement.replicas.is_empty()
    })
    .await;

    follower
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"post-failover"}),
            })],
        )
        .await
        .expect("promoted leader should accept writes after automatic failover");

    assert_eq!(promoted_placement.assigned_node, "owner-b");
    assert_eq!(promoted_placement.owner_node.as_deref(), Some("owner-b"));
    assert_eq!(promoted_placement.ownership_epoch, Some(2));
    assert!(promoted_placement.replicas.is_empty());
    assert_eq!(
        promoted_placement.failover_reason.as_deref(),
        Some(
            "automatic promotion to node 'owner-b' by leader 'owner-b' after owner 'owner-a' lost readiness"
        )
    );
    assert_eq!(write_ack.snapshot.manifest_generation, 0);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_replica_target_automatically_materializes_and_failsover_without_manual_mirroring() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("auto-replica-materialization");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-auto-replica-materialization";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-auto-replica-materialization-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    owner_config.metadata.etcd.membership_ttl_secs = 3;
    owner_config.metadata.etcd.leadership_ttl_secs = 3;
    let owner = Arc::new(AppState::new(owner_config));
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members == vec!["owner-a".to_owned()]
        })
    })
    .await;

    let mut follower_config = test_config(
        "owner-b",
        unique_temp_dir("etcd-auto-replica-materialization-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    follower_config.metadata.etcd.membership_ttl_secs = 3;
    follower_config.metadata.etcd.leadership_ttl_secs = 3;
    let follower = Arc::new(AppState::new(follower_config));

    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members.len() == 2
        })
    })
    .await;
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.registered_members.len() == 2
        })
    })
    .await;

    owner
        .control
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before automatic replica repair");
    let write_ack = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("owner should accept writes before automatic failover");

    let follower_ready = wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.last_error.is_some()
                || status.collections.iter().any(|collection| {
                    collection.collection_name == "documents"
                        && collection.owner_node.as_deref() == Some("owner-a")
                        && collection.ownership_epoch == Some(1)
                        && collection
                            .replicas
                            .iter()
                            .any(|replica| replica.node_id == "owner-b" && replica.state == "ready")
                })
        })
    })
    .await;
    assert_eq!(
        follower_ready
            .coordination
            .as_ref()
            .and_then(|coordination| coordination.last_error.clone()),
        None,
        "automatic replica materialization should not leave the follower degraded: {follower_ready:?}"
    );
    assert!(follower_ready.collections.iter().any(|collection| {
        collection.collection_name == "documents"
            && collection.assigned_node == "owner-a"
            && collection.owner_node.as_deref() == Some("owner-a")
            && collection
                .replicas
                .iter()
                .any(|replica| replica.node_id == "owner-b" && replica.state == "ready")
    }));

    drop(owner);

    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;
    let promoted_placement = wait_for_collection_placement(&follower, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-b")
            && placement.ownership_epoch == Some(2)
            && placement.assigned_node == "owner-b"
            && placement.route_kind == "local"
    })
    .await;
    let replica_ack = follower
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"post-failover"}),
            })],
        )
        .await
        .expect("automatically repaired replica should accept writes after failover");

    assert_eq!(promoted_placement.assigned_node, "owner-b");
    assert_eq!(promoted_placement.owner_node.as_deref(), Some("owner-b"));
    assert_eq!(promoted_placement.ownership_epoch, Some(2));
    assert_eq!(replica_ack.last_seq_no, 2);
    assert_eq!(write_ack.snapshot.manifest_generation, 0);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_loss_auto_promotes_ready_replica_under_control_only_leader() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("auto-owner-failover-control-leader");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-auto-owner-failover-control-leader";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-auto-owner-failover-control-owner"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    owner_config.metadata.etcd.membership_ttl_secs = 3;
    owner_config.metadata.etcd.leadership_ttl_secs = 3;
    let owner = Arc::new(AppState::new(owner_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.is_local_leader)
    })
    .await;

    let mut replica_config = test_config(
        "owner-b",
        unique_temp_dir("etcd-auto-owner-failover-control-replica"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    replica_config.node_role = NodeRole::Data;
    replica_config.metadata.etcd.membership_ttl_secs = 3;
    replica_config.metadata.etcd.leadership_ttl_secs = 3;
    let replica = Arc::new(AppState::new(replica_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members.len() == 2
        })
    })
    .await;
    wait_for_runtime_status(&replica, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-a")
        })
    })
    .await;

    owner
        .control
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before control-led failover");
    let write_ack = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("owner should accept writes before failover");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b")
        })
    })
    .await;
    wait_for_ready_replica(&replica, "documents", "owner-a", "owner-b", 1).await;

    let mut control_config = test_config(
        "control-c",
        unique_temp_dir("etcd-auto-owner-failover-control-leader"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    control_config.node_role = NodeRole::Control;
    control_config.metadata.etcd.membership_ttl_secs = 3;
    control_config.metadata.etcd.leadership_ttl_secs = 3;
    let control = Arc::new(AppState::new(control_config));
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.registered_members.len() == 3
        })
    })
    .await;

    drop(owner);

    wait_for_runtime_status(&control, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("control-c")
        })
    })
    .await;
    wait_for_runtime_status(&replica, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("control-c")
        })
    })
    .await;
    let promoted_placement = wait_for_collection_placement(&replica, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-b")
            && placement.ownership_epoch == Some(2)
            && placement.assigned_node == "owner-b"
            && placement.route_kind == "local"
    })
    .await;
    let replica_ack = replica
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"post-failover"}),
            })],
        )
        .await
        .expect("promoted replica should accept writes after control-led failover");

    assert_eq!(promoted_placement.assigned_node, "owner-b");
    assert_eq!(promoted_placement.owner_node.as_deref(), Some("owner-b"));
    assert_eq!(promoted_placement.ownership_epoch, Some(2));
    assert_eq!(replica_ack.last_seq_no, 2);
    assert_eq!(write_ack.snapshot.manifest_generation, 0);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_loss_does_not_auto_promote_stale_replica() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("auto-owner-failover-stale");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-auto-owner-failover-stale";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-auto-owner-failover-stale-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    owner_config.metadata.etcd.membership_ttl_secs = 3;
    owner_config.metadata.etcd.leadership_ttl_secs = 3;
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
        unique_temp_dir("etcd-auto-owner-failover-stale-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    follower_config.metadata.etcd.membership_ttl_secs = 3;
    follower_config.metadata.etcd.leadership_ttl_secs = 3;
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
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before stale failover");
    owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept the seed write");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b")
        })
    })
    .await;
    wait_for_ready_replica(&follower, "documents", "owner-a", "owner-b", 1).await;
    let stale_ack = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"stale-after-mirror"}),
            })],
        )
        .await
        .expect("owner should accept a post-mirror write");
    assert_eq!(stale_ack.last_seq_no, 2);
    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    wait_for_cluster_metadata(&coordination, |snapshot| {
        snapshot.collections.iter().any(|collection| {
            collection.collection.lookup_name()
                == CollectionRef::new_default("documents").lookup_name()
                && collection.replica_reports.iter().any(|report| {
                    report.node_id == "owner-a"
                        && report
                            .snapshot
                            .as_ref()
                            .is_some_and(|snapshot| snapshot.visible_seq_no == 2)
                })
                && collection.replica_reports.iter().any(|report| {
                    report.node_id == "owner-b"
                        && report
                            .snapshot
                            .as_ref()
                            .is_some_and(|snapshot| snapshot.visible_seq_no == 1)
                })
        })
    })
    .await;

    drop(owner);

    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;
    let placement = wait_for_collection_placement(&follower, "documents", |placement| {
        placement.owner_node.as_deref() == Some("owner-a")
            && placement.ownership_epoch == Some(1)
            && placement
                .replicas
                .iter()
                .any(|replica| replica.state == "stale")
    })
    .await;
    sleep(Duration::from_millis(500)).await;
    let stable_placement = follower
        .control
        .collection_placement("documents")
        .await
        .expect("placement should remain readable after stale failover");
    let write_error = follower
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("gamma"),
                vector: vec![0.5, 0.5],
                metadata: json!({"kind":"should-fail"}),
            })],
        )
        .await
        .expect_err("stale replica must not auto-promote into ownership");

    assert_eq!(placement.owner_node.as_deref(), Some("owner-a"));
    assert_eq!(placement.ownership_epoch, Some(1));
    assert_eq!(placement.replicas.len(), 1);
    assert_eq!(placement.replicas[0].node_id, "owner-b");
    assert_eq!(placement.replicas[0].state, "stale");
    assert_eq!(stable_placement.owner_node.as_deref(), Some("owner-a"));
    assert_eq!(stable_placement.ownership_epoch, Some(1));
    assert!(
        matches!(write_error, ServiceError::InvalidArgument(ref message) if message.contains("not locally served")),
        "stale failover should fail closed instead of serving writes: {write_error:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_rebalance_rejects_absent_replica_targets() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("rebalance-absent-target");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-rebalance-absent-target";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-rebalance-absent-target-a"),
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
        unique_temp_dir("etcd-rebalance-absent-target-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config));
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
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before rebalance");
    owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept writes before rebalance");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b")
        })
    })
    .await;
    let descriptor = owner
        .get_collection("documents")
        .await
        .expect("owner descriptor should load before forcing an absent target");
    wait_for_ready_replica(&follower, "documents", "owner-a", "owner-b", 1).await;
    let follower_collection_root = follower
        .config
        .storage_root
        .join("collections")
        .join(descriptor.collection_id.to_string());
    fs::remove_dir_all(&follower_collection_root)
        .expect("follower local replica state should be removable before rebalance");
    follower
        .control
        .fail_closed_local_replica_report("documents", None)
        .await
        .expect("follower should publish an absent replica report before rebalance");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b" && replica.state == "absent")
        })
    })
    .await;
    owner
        .control
        .drain_node("owner-a")
        .await
        .expect("owner should be drainable before rebalance");
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;

    let rebalance = follower
        .control
        .rebalance_collection("documents", None)
        .await
        .expect_err("rebalance must reject absent replica targets");

    assert!(
        matches!(rebalance, ServiceError::FailedPrecondition(ref message) if message.contains("no fresh rebalance target")),
        "rebalance should explain the missing fresh target: {rebalance:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_rebalance_rejects_stale_membership_reports() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("rebalance-stale-report");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-rebalance-stale-report";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-rebalance-stale-report-a"),
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
        unique_temp_dir("etcd-rebalance-stale-report-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    let follower = Arc::new(AppState::new(follower_config));
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
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(2),
        )
        .await
        .expect("collection should be created before stale-report rebalance");
    owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept writes before stale-report rebalance");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection
                    .replicas
                    .iter()
                    .any(|replica| replica.node_id == "owner-b")
        })
    })
    .await;
    wait_for_ready_replica(&follower, "documents", "owner-a", "owner-b", 1).await;
    drop(follower);

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    coordination
        .register_membership("owner-b", NodeRole::Combined)
        .await
        .expect("candidate membership should be re-registered without republishing the old report");

    let placement = wait_for_collection_placement(&owner, "documents", |placement| {
        placement
            .replicas
            .iter()
            .any(|replica| replica.node_id == "owner-b" && replica.state == "stale")
    })
    .await;
    let rebalance = owner
        .control
        .rebalance_collection("documents", None)
        .await
        .expect_err("rebalance must reject stale replica reports");

    assert_eq!(placement.replicas.len(), 1);
    assert_eq!(placement.replicas[0].node_id, "owner-b");
    assert_eq!(placement.replicas[0].state, "stale");
    assert!(
        matches!(rebalance, ServiceError::FailedPrecondition(ref message) if message.contains("no fresh rebalance target")),
        "rebalance should not treat stale reports as ready: {rebalance:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_promotion_conflicts_with_stale_leader_fence() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("owner-promotion-stale-leader");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-owner-promotion-stale-leader";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-owner-promotion-stale-leader-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    owner_config.metadata.etcd.membership_ttl_secs = 3;
    owner_config.metadata.etcd.leadership_ttl_secs = 3;
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
        unique_temp_dir("etcd-owner-promotion-stale-leader-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    follower_config.metadata.etcd.membership_ttl_secs = 3;
    follower_config.metadata.etcd.leadership_ttl_secs = 3;
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
        .expect("collection should be created before stale-fence promotion");
    owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept the seed write");
    let descriptor = owner
        .get_collection("documents")
        .await
        .expect("descriptor should load before mirroring");
    mirror_collection_state(
        owner_config.storage_root.as_path(),
        follower_config.storage_root.as_path(),
        &descriptor.root_path,
    );
    follower
        .control
        .sync_local_replica_report("documents", None)
        .await
        .expect("follower replica report should publish after mirroring");
    let stale_leader_fence = wait_for_leadership_fence(&owner).await;

    drop(owner);

    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("owner-b")
        })
    })
    .await;

    let coordination = EtcdCoordinationClient::new(follower_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let current = coordination
        .shard_owner(&CollectionRef::new_default("documents"), "0")
        .await
        .expect("owner lookup should succeed after leader transfer")
        .expect("owner record should still exist");
    let promotion = coordination
        .promote_shard_owner(&current, "owner-b", &stale_leader_fence)
        .await
        .expect_err("stale leader fence should be rejected before promotion");

    assert!(
        promotion
            .to_string()
            .contains("not the active control-plane leader"),
        "stale leader fences must be fenced before promotion: {promotion:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_acknowledge_local_replica_update_degrades_to_fail_closed_after_preclear() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("acknowledge-fail-closed");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-acknowledge-fail-closed";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-acknowledge-fail-closed-owner"),
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

    owner
        .control
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created before the acknowledgement test");
    let write_ack = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept the seed write");

    let stale_report_already_cleared = owner
        .control
        .prepare_local_replica_update("documents")
        .await
        .expect("existing replica report should clear before degraded acknowledgement");
    assert!(stale_report_already_cleared);
    owner
        .control
        .drain_node("owner-a")
        .await
        .expect("owner should be drainable before testing the degraded acknowledgement path");
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.membership_state.as_deref() == Some("draining")
        })
    })
    .await;

    owner
        .control
        .acknowledge_local_replica_update(
            "documents",
            Some(write_ack.snapshot.clone()),
            stale_report_already_cleared,
        )
        .await
        .expect("a precleared post-commit acknowledgement should degrade instead of failing");

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let report = coordination
        .shard_replica_report(&CollectionRef::new_default("documents"), "0", "owner-a")
        .await
        .expect("replica report lookup should succeed")
        .expect("fail-closed report should remain published");
    assert!(!report.materialized);
    assert_eq!(report.snapshot, Some(write_ack.snapshot.clone()));
    assert_eq!(report.ownership_epoch, Some(1));

    let degraded_status = wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination
                .last_error
                .as_ref()
                .is_some_and(|message| message.contains("fail-closed report was recorded"))
        })
    })
    .await;
    assert!(
        degraded_status
            .coordination
            .as_ref()
            .and_then(|coordination| coordination.last_error.as_ref())
            .is_some_and(|message| message.contains("repair is pending")),
        "degraded acknowledgement should be visible in runtime status: {degraded_status:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_background_report_publisher_skips_in_flight_local_updates() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("background-report-skips-in-flight-update");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-background-report-skips-in-flight-update";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-background-report-skips-in-flight-update"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    owner_config.node_role = NodeRole::Combined;
    owner_config.metadata.etcd.membership_ttl_secs = 3;
    owner_config.metadata.etcd.leadership_ttl_secs = 3;
    let owner = Arc::new(AppState::new(owner_config.clone()));
    wait_for_runtime_status(&owner, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.is_local_leader)
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
        .expect("collection should be created before the background publisher test");
    let write_ack = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept the seed write");

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    wait_for_runtime_status(&owner, |status| {
        status.collections.iter().any(|collection| {
            collection.collection_name == "documents"
                && collection.owner_node.as_deref() == Some("owner-a")
        })
    })
    .await;
    let seeded_report = timeout(Duration::from_secs(5), async {
        loop {
            let report = coordination
                .shard_replica_report(&CollectionRef::new_default("documents"), "0", "owner-a")
                .await
                .expect("replica report lookup should succeed before preclear");
            if let Some(report) = report {
                break report;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("materialized owner report should appear before preclear");
    assert!(seeded_report.materialized);
    assert_eq!(seeded_report.snapshot, Some(write_ack.snapshot.clone()));

    owner
        .control
        .prepare_local_replica_update("documents")
        .await
        .expect("preclear should succeed before the background publisher tick");
    let cleared_report = coordination
        .shard_replica_report(&CollectionRef::new_default("documents"), "0", "owner-a")
        .await
        .expect("replica report lookup should succeed after preclear");
    assert!(
        cleared_report.is_none(),
        "preclear should remove the authoritative report immediately"
    );

    sleep(Duration::from_millis(1_500)).await;

    let after_tick_report = coordination
        .shard_replica_report(&CollectionRef::new_default("documents"), "0", "owner-a")
        .await
        .expect("replica report lookup should succeed after the coordination tick");
    assert!(
        after_tick_report.is_none(),
        "background publishing must not republish stale local state while a mutation is in flight"
    );

    owner
        .control
        .acknowledge_local_replica_update("documents", Some(write_ack.snapshot), true)
        .await
        .expect("acknowledgement should restore the fresh report after the in-flight window");

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
    let leader_key = format!("{key_prefix}/clusters/{cluster_name}/controllers/leader");
    let membership_key = format!("{key_prefix}/clusters/{cluster_name}/members/owner-b");
    client
        .put(
            membership_key,
            serde_json::json!({
                "node_id": "owner-b",
                "node_role": "data",
                "state": "ready",
            })
            .to_string(),
            None,
        )
        .await
        .expect("candidate membership should be seeded");
    let leader_membership_key = format!("{key_prefix}/clusters/{cluster_name}/members/owner-a");
    client
        .put(
            leader_membership_key,
            serde_json::json!({
                "node_id": "owner-a",
                "node_role": "combined",
                "state": "ready",
            })
            .to_string(),
            None,
        )
        .await
        .expect("leader membership should be seeded");
    client
        .put(
            leader_key,
            serde_json::json!({
                "node_id": "owner-a",
                "lease_id": 7,
            })
            .to_string(),
            None,
        )
        .await
        .expect("leader fence should be seeded");
    let leader_fence = LeadershipFence {
        node_id: "owner-a".to_owned(),
        lease_id: 7,
        membership_lease_id: 0,
    };

    let promotion = coordination
        .promote_shard_owner(&current, "owner-b", &leader_fence)
        .await
        .expect("pending descriptors should return a conflict result");

    assert!(matches!(promotion, PromotionResult::Conflict));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_promotion_conflicts_for_control_only_members() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("owner-promotion-control-only");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-owner-promotion-control-only";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-owner-promotion-control-only-a"),
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

    let mut control_config = test_config(
        "control-b",
        unique_temp_dir("etcd-owner-promotion-control-only-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    control_config.node_role = NodeRole::Control;
    let control = Arc::new(AppState::new(control_config));
    wait_for_runtime_status(&control, |status| {
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

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let current = coordination
        .shard_owner(&CollectionRef::new_default("documents"), "0")
        .await
        .expect("owner lookup should succeed")
        .expect("owner record should be seeded");
    let leader_fence = wait_for_leadership_fence(&owner).await;

    let promotion = coordination
        .promote_shard_owner(&current, "control-b", &leader_fence)
        .await
        .expect("control-only members should produce a conflict result");

    assert!(matches!(promotion, PromotionResult::Conflict));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_runtime_status_surfaces_coordination_errors_when_etcd_is_unreachable() {
    let root = unique_temp_dir("etcd-runtime-status-error");
    let mut config = LogPoseConfig {
        node_name: "unreachable-node".to_owned(),
        storage_root: root,
        rest_advertise_host: Some("127.0.0.1".to_owned()),
        grpc_advertise_host: Some("127.0.0.1".to_owned()),
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
        internal: logpose_config::InternalConfig {
            replica_token: Some("replica-secret".to_owned()),
            allow_non_routable_rest_advertise_host: true,
            ..logpose_config::InternalConfig::default()
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

#[tokio::test]
async fn etcd_runtime_status_drops_ready_flags_after_external_lease_revocation() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("runtime-status-lease-revocation");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-runtime-status-lease-revocation";

    let mut config = test_config(
        "coordinator-a",
        unique_temp_dir("etcd-runtime-status-lease-revocation"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    config.node_role = NodeRole::Combined;
    config.metadata.etcd.membership_ttl_secs = 15;
    config.metadata.etcd.leadership_ttl_secs = 15;
    let state = Arc::new(AppState::new(config.clone()));

    let ready = wait_for_runtime_status(&state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.membership_lease_id.is_some()
                && coordination.leadership_lease_id.is_some()
        }) && status.control_plane_ready
            && status.data_plane_ready
    })
    .await;
    let coordination = ready
        .coordination
        .as_ref()
        .expect("coordination state should be present");
    let revoker = EtcdCoordinationClient::new(config.metadata.etcd.clone())
        .expect("coordination client should build");
    revoker
        .revoke_lease(
            coordination
                .leadership_lease_id
                .expect("leadership lease id should be present"),
        )
        .await
        .expect("leadership lease should be revocable");
    revoker
        .revoke_lease(
            coordination
                .membership_lease_id
                .expect("membership lease id should be present"),
        )
        .await
        .expect("membership lease should be revocable");

    let degraded = wait_for_runtime_status(&state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            !coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.membership_lease_id.is_none()
                && coordination.leadership_lease_id.is_none()
        }) && !status.control_plane_ready
            && !status.data_plane_ready
    })
    .await;
    let degraded_coordination = degraded
        .coordination
        .expect("coordination state should remain present");

    assert!(!degraded_coordination.membership_registered);
    assert!(!degraded_coordination.is_local_leader);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_runtime_revokes_stale_leadership_when_membership_record_disappears() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("runtime-status-membership-loss-revokes-leader");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-runtime-status-membership-loss-revokes-leader";

    let mut leader_config = test_config(
        "leader-a",
        unique_temp_dir("etcd-runtime-status-membership-loss-leader-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    leader_config.node_role = NodeRole::Combined;
    leader_config.metadata.etcd.membership_ttl_secs = 3;
    leader_config.metadata.etcd.leadership_ttl_secs = 30;
    let leader = Arc::new(AppState::new(leader_config.clone()));

    wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.membership_lease_id.is_some()
                && coordination.leadership_lease_id.is_some()
        })
    })
    .await;

    let membership_key = format!(
        "{}/clusters/{}/members/{}",
        key_prefix, cluster_name, leader_config.node_name
    );
    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("etcd client should connect");
    client
        .delete(membership_key, None)
        .await
        .expect("membership record should be removable without revoking the lease");

    let degraded = wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            !coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.membership_lease_id.is_none()
                && coordination.leadership_lease_id.is_none()
        })
    })
    .await;
    assert!(!degraded.control_plane_ready);

    drop(leader);

    let mut follower_config = test_config(
        "leader-b",
        unique_temp_dir("etcd-runtime-status-membership-loss-leader-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Combined;
    follower_config.metadata.etcd.membership_ttl_secs = 3;
    follower_config.metadata.etcd.leadership_ttl_secs = 30;
    let follower = Arc::new(AppState::new(follower_config));

    let follower_status = wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("leader-b")
        })
    })
    .await;
    assert!(follower_status.control_plane_ready);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_membership_mutations_require_live_local_leadership() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("membership-mutations-require-live-leader");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-membership-mutations-require-live-leader";

    let mut leader_config = test_config(
        "leader-a",
        unique_temp_dir("etcd-membership-live-leader-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    leader_config.node_role = NodeRole::Combined;
    leader_config.metadata.etcd.membership_ttl_secs = 15;
    leader_config.metadata.etcd.leadership_ttl_secs = 30;
    let leader = Arc::new(AppState::new(leader_config.clone()));

    let mut follower_config = test_config(
        "follower-b",
        unique_temp_dir("etcd-membership-live-leader-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Data;
    let follower = Arc::new(AppState::new(follower_config.clone()));

    wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.membership_lease_id.is_some()
                && coordination.leadership_lease_id.is_some()
        })
    })
    .await;
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("leader-a")
        })
    })
    .await;

    let membership_key = format!(
        "{}/clusters/{}/members/{}",
        key_prefix, cluster_name, leader_config.node_name
    );
    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("etcd client should connect");
    client
        .delete(membership_key, None)
        .await
        .expect("leader membership record should be removable without revoking leadership");

    wait_for_runtime_status(&leader, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            !coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.membership_lease_id.is_none()
        })
    })
    .await;

    let mutation_error = leader
        .control
        .drain_node("follower-b")
        .await
        .expect_err("stale local leadership must not mutate other membership state");
    assert!(
        matches!(mutation_error, ServiceError::InvalidArgument(ref message) if message.contains("not the active control-plane leader")),
        "membership mutation should be fenced by live local leadership: {mutation_error:?}"
    );

    let follower_status = wait_for_runtime_status(&follower, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.membership_state.as_deref() == Some("ready"))
    })
    .await;
    assert_eq!(
        follower_status
            .coordination
            .and_then(|coordination| coordination.membership_state),
        Some("ready".to_owned())
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_membership_reregistration_preserves_draining_state() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("membership-reregistration-preserves-drain");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-membership-reregistration-preserves-drain";

    let mut leader_config = test_config(
        "leader-a",
        unique_temp_dir("etcd-membership-preserve-drain-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    leader_config.node_role = NodeRole::Combined;
    let leader = Arc::new(AppState::new(leader_config.clone()));
    wait_for_runtime_status(&leader, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.is_local_leader)
    })
    .await;

    let mut follower_config = test_config(
        "follower-b",
        unique_temp_dir("etcd-membership-preserve-drain-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    follower_config.node_role = NodeRole::Data;
    let follower = Arc::new(AppState::new(follower_config.clone()));
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.membership_state.as_deref() == Some("ready")
        })
    })
    .await;

    leader
        .control
        .drain_node("follower-b")
        .await
        .expect("leader should be able to drain the follower");
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.membership_state.as_deref() == Some("draining")
                && !status.data_plane_ready
        })
    })
    .await;

    drop(follower);

    let follower_restart = Arc::new(AppState::new(follower_config.clone()));
    let restarted_status = wait_for_runtime_status(&follower_restart, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.membership_state.as_deref() == Some("draining")
                && !status.data_plane_ready
        })
    })
    .await;

    assert_eq!(
        restarted_status
            .coordination
            .as_ref()
            .and_then(|coordination| coordination.membership_state.as_deref()),
        Some("draining")
    );
    assert!(
        !restarted_status.data_plane_ready,
        "re-registered draining members must not silently return to serving"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_duplicate_node_name_fences_the_old_runtime() {
    let endpoints = test_etcd_endpoints();
    let key_prefix = unique_etcd_prefix("duplicate-node-name-fence");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-duplicate-node-name-fence";

    let mut owner_config = test_config(
        "owner-a",
        unique_temp_dir("etcd-duplicate-node-name-primary"),
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

    owner
        .control
        .create_collection(
            CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot)
                .with_replication_factor(1),
        )
        .await
        .expect("collection should be created before duplicating the node identity");
    owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"seed"}),
            })],
        )
        .await
        .expect("owner should accept writes before duplicating the node identity");

    let descriptor = owner
        .get_collection("documents")
        .await
        .expect("owner descriptor should be readable before duplicating identity");
    let duplicate_root = unique_temp_dir("etcd-duplicate-node-name-shadow");
    mirror_collection_state(
        owner_config.storage_root.as_path(),
        duplicate_root.as_path(),
        &descriptor.root_path,
    );

    let mut duplicate_config = owner_config.clone();
    duplicate_config.storage_root = duplicate_root;
    let duplicate = Arc::new(AppState::new(duplicate_config));
    wait_for_runtime_status(&duplicate, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.membership_state.as_deref() == Some("ready")
                && status.data_plane_ready
        })
    })
    .await;
    wait_for_runtime_status(&owner, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            !coordination.membership_registered
                && !status.data_plane_ready
                && !status.control_plane_ready
        })
    })
    .await;

    let old_runtime_error = owner
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"kind":"duplicate"}),
            })],
        )
        .await
        .expect_err(
            "the displaced runtime must stop serving once its membership lease is replaced",
        );
    let new_runtime_ack = duplicate
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("gamma"),
                vector: vec![0.5, 0.5],
                metadata: json!({"kind":"replacement"}),
            })],
        )
        .await
        .expect("the replacement runtime should remain able to serve the logical node identity");

    assert!(
        matches!(old_runtime_error, ServiceError::InvalidArgument(ref message) if message.contains("not locally served")),
        "the displaced runtime must be fenced from local serving: {old_runtime_error:?}"
    );
    assert_eq!(new_runtime_ack.last_seq_no, 2);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

fn test_config(
    node_name: &str,
    storage_root: PathBuf,
    endpoints: &[String],
    key_prefix: &str,
    cluster_name: &str,
) -> LogPoseConfig {
    let (rest_port, grpc_port) = allocate_test_ports();
    LogPoseConfig {
        node_name: node_name.to_owned(),
        storage_root,
        rest_advertise_host: Some("127.0.0.1".to_owned()),
        rest_port,
        grpc_advertise_host: Some("127.0.0.1".to_owned()),
        grpc_port,
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
        internal: logpose_config::InternalConfig {
            replica_token: Some("replica-secret".to_owned()),
            allow_non_routable_rest_advertise_host: true,
            ..logpose_config::InternalConfig::default()
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

fn allocate_test_ports() -> (u16, u16) {
    let base = NEXT_TEST_PORT.fetch_add(2, Ordering::Relaxed);
    (base as u16, (base + 1) as u16)
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

async fn wait_for_leadership_fence(state: &AppState) -> LeadershipFence {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match state.control.require_local_control_plane_leader().await {
            Ok(Some(fence)) => return fence,
            Ok(None) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for local leadership fence"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn wait_for_collection_placement(
    state: &AppState,
    collection_name: &str,
    ready: impl Fn(&logpose_types::CollectionPlacement) -> bool,
) -> logpose_types::CollectionPlacement {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let placement = state
            .control
            .collection_placement(collection_name)
            .await
            .expect("collection placement should be readable");
        if ready(&placement) {
            return placement;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for collection placement: {placement:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_ready_replica(
    state: &AppState,
    collection_name: &str,
    owner_node_id: &str,
    replica_node_id: &str,
    ownership_epoch: u64,
) -> logpose_types::CollectionPlacement {
    wait_for_collection_placement(state, collection_name, |placement| {
        placement.owner_node.as_deref() == Some(owner_node_id)
            && placement.ownership_epoch == Some(ownership_epoch)
            && placement
                .replicas
                .iter()
                .any(|replica| replica.node_id == replica_node_id && replica.state == "ready")
    })
    .await
}

async fn wait_for_cluster_metadata(
    client: &EtcdCoordinationClient,
    ready: impl Fn(&logpose_storage_etcd::ClusterMetadataSnapshot) -> bool,
) -> logpose_storage_etcd::ClusterMetadataSnapshot {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = client
            .load_cluster_metadata()
            .await
            .expect("cluster metadata should load");
        if ready(&snapshot) {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for cluster metadata snapshot: {snapshot:?}"
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
