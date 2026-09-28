//! End-to-end etcd metadata integration coverage for `AppState`.
//!
//! Tests that need a live etcd run only when `LOGPOSE_TEST_ETCD_ENDPOINTS` is
//! set, for example to `http://127.0.0.1:2379`; otherwise they pass without
//! running (the skip message shows with `--nocapture`). When the variable is
//! set, an unreachable etcd is a failure, and under CI a missing variable is too.

use etcd_client::{Client, DeleteOptions, PutOptions};
use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
    Principal, PrincipalKind,
};
use logpose_catalog::CollectionDescriptor;
use logpose_config::{BootstrapTokenConfig, LogPoseConfig};
use logpose_core::{AppState, RequestAuth};
use logpose_query::{ExplainMode, QueryRequest};
use logpose_service as _;
use logpose_storage::CreateCollectionRequest;
use logpose_storage_etcd::{
    EtcdCatalogStore, EtcdCoordinationClient, LeadershipRecord, LeaseKeepAlive, PromotionResult,
};
use logpose_types::{
    CollectionAssignment, CollectionRef, CorruptionKind, DistanceMetric, EtcdMetadataConfig,
    LogPoseError, MetadataBackend, MetadataConfig, NodeRole, PutRecord, RecordId,
    legacy::record_from_put,
    schema::{FieldType, ScalarFieldSpec, SchemaChange},
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

/// Environment variable that enables the etcd integration tests.
const ETCD_ENDPOINTS_ENV: &str = "LOGPOSE_TEST_ETCD_ENDPOINTS";

#[tokio::test]
async fn etcd_metadata_backend_surfaces_remote_collections_across_nodes() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_metadata_backend_surfaces_remote_collections_across_nodes")
            .await
    else {
        return;
    };
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
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
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
    assert!(matches!(stats_error, LogPoseError::NotOwner { .. }));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_schema_changes_reach_the_catalog_other_nodes_describe() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_schema_changes_reach_the_catalog_other_nodes_describe").await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("alter-catalog");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-alter-catalog";
    let state_a = Arc::new(AppState::new(test_config(
        "alter-node-a",
        unique_temp_dir("etcd-alter-node-a"),
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    state_a
        .control
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created through authoritative metadata");
    let altered = state_a
        .alter_collection_with_auth(
            &RequestAuth::default(),
            "documents",
            SchemaChange::AddField(ScalarFieldSpec::new("color", FieldType::String)),
        )
        .await
        .expect("the owner should apply the schema change");
    assert!(altered.schema.scalar_field("color").is_some());

    let state_b = Arc::new(AppState::new(test_config(
        "alter-node-b",
        unique_temp_dir("etcd-alter-node-b"),
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    let remote = state_b
        .get_collection("documents")
        .await
        .expect("another node should describe the collection from the catalog");
    assert_eq!(
        remote.schema, altered.schema,
        "a node that does not own the collection describes its live schema"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_drop_database_refuses_while_any_collection_metadata_remains() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_drop_database_refuses_while_any_collection_metadata_remains")
            .await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("drop-database");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-drop-database";
    let state = Arc::new(AppState::new(test_config(
        "drop-db-node",
        unique_temp_dir("etcd-drop-db-node"),
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    let auth = RequestAuth::default();
    state
        .put_database_with_auth(&auth, logpose_catalog::DatabaseDescriptor::new("analytics"))
        .await
        .expect("database should be created");
    // A collection whose creation has written only its first metadata key, as a create racing
    // the drop has.
    let orphan =
        format!("{key_prefix}/clusters/{cluster_name}/collections/analytics/late/assignment");
    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("etcd should be reachable");
    client
        .put(orphan.clone(), "{}", Some(PutOptions::new()))
        .await
        .expect("raw metadata key should be written");

    let error = state
        .drop_database_with_auth(&auth, "analytics")
        .await
        .expect_err("a database with collection metadata must not be dropped");
    assert!(
        matches!(error, LogPoseError::FailedPrecondition { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains("late"), "{error}");
    state
        .database_with_auth(&auth, "analytics")
        .await
        .expect("the refused drop keeps the database");

    client
        .delete(orphan, None)
        .await
        .expect("raw metadata key should be removed");
    state
        .drop_database_with_auth(&auth, "analytics")
        .await
        .expect("an empty database is dropped");
    assert!(matches!(
        state.database_with_auth(&auth, "analytics").await,
        Err(LogPoseError::NotFound { .. })
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_metadata_backend_shares_database_policies_across_nodes() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_metadata_backend_shares_database_policies_across_nodes").await
    else {
        return;
    };
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
    let Some(endpoints) = etcd_endpoints_or_skip(
        "etcd_metadata_backend_reads_shared_principal_overrides_across_nodes",
    )
    .await
    else {
        return;
    };
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
        LogPoseError::PermissionDenied { message }
            if message.contains("not allowed to perform operator actions")
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_stored_principals_that_fail_validation_are_reported_as_corrupt() {
    let Some(endpoints) = etcd_endpoints_or_skip(
        "etcd_stored_principals_that_fail_validation_are_reported_as_corrupt",
    )
    .await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("invalid-stored-principal");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-invalid-principal";
    let config = test_config(
        "invalid-principal-node",
        unique_temp_dir("etcd-invalid-principal"),
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    // A record that decodes but fails validation: principal names may not contain '/'.
    let key = format!("{key_prefix}/clusters/{cluster_name}/principals/reader/descriptor");
    let mut record = serde_json::to_value(Principal::new_with_access_tier(
        "reader",
        PrincipalKind::User,
        AccessTier::Observer,
    ))
    .expect("principal serializes");
    record["name"] = json!("a/b");
    Client::connect(endpoints.clone(), None)
        .await
        .expect("etcd should connect")
        .put(key.clone(), record.to_string(), None)
        .await
        .expect("the damaged record should be written");

    let catalog =
        EtcdCatalogStore::new(config.metadata.etcd).expect("etcd catalog store should open");
    for error in [
        catalog
            .get_principal("reader")
            .await
            .expect_err("a damaged principal should fail"),
        catalog
            .list_principals()
            .await
            .expect_err("listing a damaged principal should fail"),
    ] {
        assert!(
            matches!(
                &error,
                LogPoseError::Corrupt {
                    kind: CorruptionKind::Metadata,
                    location: Some(location),
                    ..
                } if *location == key
            ),
            "{error:?}"
        );
    }

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_collection_creation_seeds_shared_database_metadata() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_collection_creation_seeds_shared_database_metadata").await
    else {
        return;
    };
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
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_data_only_nodes_reject_catalog_mutations").await
    else {
        return;
    };
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
        LogPoseError::WrongNodeRole { operation, .. }
            if operation == "control-plane database mutations"
    ));
    assert!(matches!(
        policy_error,
        LogPoseError::WrongNodeRole { operation, .. }
            if operation == "control-plane database mutations"
    ));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_runtime_status_surfaces_membership_and_controller_leader() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_runtime_status_surfaces_membership_and_controller_leader")
            .await
    else {
        return;
    };
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
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_new_node_registration_updates_visible_membership").await
    else {
        return;
    };
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
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_membership_leases_expire_after_state_drop").await
    else {
        return;
    };
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
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_rejoining_node_re_registers_membership_after_restart").await
    else {
        return;
    };
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
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_follower_nodes_reject_control_plane_mutations").await
    else {
        return;
    };
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

    assert!(matches!(collection_error, LogPoseError::NotLeader { .. }));
    assert!(!follower_status.control_plane_ready);
    assert!(follower_status.data_plane_ready);
    assert!(matches!(database_error, LogPoseError::NotLeader { .. }));

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_catalog_transactions_reject_stale_leaders_after_leadership_moves() {
    let Some(endpoints) = etcd_endpoints_or_skip(
        "etcd_catalog_transactions_reject_stale_leaders_after_leadership_moves",
    )
    .await
    else {
        return;
    };
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
    assert!(matches!(app_database_error, LogPoseError::NotLeader { .. }));
    assert!(matches!(app_policy_error, LogPoseError::NotLeader { .. }));
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
    let Some(endpoints) = etcd_endpoints_or_skip("etcd_owner_promotion_fences_the_old_owner").await
    else {
        return;
    };
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
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
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
    let follower = restart_with_mirrored_state(follower, &follower_config).await;

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
        .promote_shard_owner(&current, "owner-z")
        .await
        .expect("non-member promotion attempt should return a conflict result");
    assert!(matches!(non_member_attempt, PromotionResult::Conflict));

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
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
        )
        .await
        .expect_err("promoted old owner must reject writes");
    let follower_ack = follower
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![0.5, 0.5],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
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
        matches!(owner_error, LogPoseError::NotOwner { .. }),
        "old owner should be fenced by ownership: {owner_error:?}"
    );
    assert!(
        matches!(owner_stats_error, LogPoseError::NotOwner { .. }),
        "old owner should reject reads after promotion: {owner_stats_error:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_promotion_rejects_read_barriers_without_freshness_metadata() {
    let Some(endpoints) = etcd_endpoints_or_skip(
        "etcd_owner_promotion_rejects_read_barriers_without_freshness_metadata",
    )
    .await
    else {
        return;
    };
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
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
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
    let follower = restart_with_mirrored_state(follower, &follower_config).await;

    let coordination = EtcdCoordinationClient::new(owner_config.metadata.etcd.clone())
        .expect("coordination client should build");
    let current = coordination
        .shard_owner(&CollectionRef::new_default("documents"), "0")
        .await
        .expect("owner lookup should succeed")
        .expect("owner record should be seeded");
    let promoted = coordination
        .promote_shard_owner(&current, "owner-b")
        .await
        .expect("promotion should succeed");
    assert!(matches!(promoted, PromotionResult::Applied(_)));

    let post_promotion_ack = follower
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
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
            snapshot_token: None,
            pin: false,
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
            snapshot_token: None,
            pin: false,
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
            snapshot_token: None,
            pin: false,
        })
        .await
        .expect("exact snapshots should remain readable after promotion");

    assert!(
        matches!(query, LogPoseError::FailedPrecondition { ref message } if message.contains("cannot safely satisfy read barriers after promotion")),
        "promoted owner should explain the fail-closed read-barrier behavior: {query:?}"
    );
    assert!(
        matches!(stats, LogPoseError::FailedPrecondition { ref message } if message.contains("cannot safely satisfy read barriers after promotion")),
        "promoted owner should explain the fail-closed stats behavior: {stats:?}"
    );
    assert!(
        matches!(post_promotion_query, LogPoseError::FailedPrecondition { ref message } if message.contains("cannot safely satisfy read barriers after promotion")),
        "promoted owner should reject barriers minted after promotion too: {post_promotion_query:?}"
    );
    assert!(
        matches!(post_promotion_stats, LogPoseError::FailedPrecondition { ref message } if message.contains("cannot safely satisfy read barriers after promotion")),
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
async fn etcd_missing_owner_metadata_rejects_reads_until_reconciliation() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_missing_owner_metadata_rejects_reads_until_reconciliation")
            .await
    else {
        return;
    };
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
        .upsert_records_with_auth(
            &RequestAuth::default(),
            "documents",
            vec![
                record_from_put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                })
                .expect("record"),
            ],
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

    let placement = owner
        .control
        .collection_placement("documents")
        .await
        .expect("placement should still load when owner metadata is missing");
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
        matches!(stats_error, LogPoseError::NotOwner { .. }),
        "missing owner metadata should fence reads until reconciliation: {stats_error:?}"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_owner_promotion_conflicts_while_descriptor_is_pending() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_owner_promotion_conflicts_while_descriptor_is_pending").await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("owner-promotion-pending");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-metadata";
    let collection = CollectionRef::new_default("documents");
    let descriptor = CollectionDescriptor::new_in_database(
        "default",
        "documents",
        logpose_types::legacy::legacy_schema(2, DistanceMetric::Dot).expect("schema"),
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
async fn etcd_owner_promotion_conflicts_for_control_only_members() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_owner_promotion_conflicts_for_control_only_members").await
    else {
        return;
    };
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

    let promotion = coordination
        .promote_shard_owner(&current, "control-b")
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

#[tokio::test]
async fn etcd_keep_alive_reports_revoked_leases_as_expired() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_keep_alive_reports_revoked_leases_as_expired").await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("keep-alive-reports-expired");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let config = test_config(
        "keep-alive-node",
        unique_temp_dir("etcd-keep-alive-reports-expired"),
        &endpoints,
        &key_prefix,
        "core-etcd-keep-alive-reports-expired",
    );
    let coordination = EtcdCoordinationClient::new(config.metadata.etcd.clone())
        .expect("coordination client should build");
    let membership = coordination
        .register_membership("keep-alive-node", NodeRole::Combined)
        .await
        .expect("membership should register");
    let leadership = coordination
        .try_acquire_leadership("keep-alive-node")
        .await
        .expect("leadership campaign should run")
        .expect("an empty cluster should grant leadership");

    assert!(matches!(
        coordination.keep_alive(membership.lease_id).await,
        Ok(LeaseKeepAlive::Alive { ttl_secs }) if ttl_secs > 0
    ));

    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("raw etcd client should connect");
    client
        .lease_revoke(leadership.lease_id)
        .await
        .expect("leadership lease should be revocable out of band");
    client
        .lease_revoke(membership.lease_id)
        .await
        .expect("membership lease should be revocable out of band");

    assert_eq!(
        coordination
            .keep_alive(leadership.lease_id)
            .await
            .expect("keep-alive on a revoked lease should not be a transport error"),
        LeaseKeepAlive::Expired
    );
    assert_eq!(
        coordination
            .keep_alive(membership.lease_id)
            .await
            .expect("keep-alive on a revoked lease should not be a transport error"),
        LeaseKeepAlive::Expired
    );
    // With the session gone, a later keep-alive reopens the stream and still
    // reports the lease as dead.
    assert_eq!(
        coordination
            .keep_alive(membership.lease_id)
            .await
            .expect("keep-alive on an unknown lease should not be a transport error"),
        LeaseKeepAlive::Expired
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_node_recampaigns_after_leadership_lease_revocation() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_node_recampaigns_after_leadership_lease_revocation").await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("recampaign-after-leadership-revocation");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-recampaign-after-leadership-revocation";
    let config = short_ttl_config(
        "leader-a",
        "etcd-recampaign-after-leadership-revocation",
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    let state = Arc::new(AppState::new(config));
    let (membership_lease_id, leadership_lease_id) = wait_for_local_leadership(&state).await;

    revoke_lease_out_of_band(&endpoints, leadership_lease_id).await;

    let recovered = wait_for_runtime_status(&state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.is_local_leader
                && coordination
                    .leadership_lease_id
                    .is_some_and(|lease_id| lease_id != leadership_lease_id)
        }) && status.control_plane_ready
    })
    .await;
    let coordination = recovered
        .coordination
        .expect("coordination state should be present");

    assert_eq!(coordination.leader_node.as_deref(), Some("leader-a"));
    assert_eq!(
        coordination.membership_lease_id,
        Some(membership_lease_id),
        "losing leadership must not disturb a healthy membership lease"
    );
    assert_eq!(
        visible_leader(&endpoints, &key_prefix, cluster_name).await,
        Some(LeadershipRecord {
            node_id: "leader-a".to_owned(),
            lease_id: coordination
                .leadership_lease_id
                .expect("re-acquired leadership lease should be visible"),
        })
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_node_recampaigns_when_leader_key_disappears() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_node_recampaigns_when_leader_key_disappears").await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("recampaign-after-leader-key-delete");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-recampaign-after-leader-key-delete";
    let config = short_ttl_config(
        "leader-a",
        "etcd-recampaign-after-leader-key-delete",
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    let state = Arc::new(AppState::new(config));
    let (membership_lease_id, leadership_lease_id) = wait_for_local_leadership(&state).await;

    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("raw etcd client should connect");
    client
        .delete(
            format!("{key_prefix}/clusters/{cluster_name}/controllers/leader"),
            None,
        )
        .await
        .expect("leader key should be removable without revoking its lease");

    let recovered = wait_for_runtime_status(&state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.is_local_leader
                && coordination
                    .leadership_lease_id
                    .is_some_and(|lease_id| lease_id != leadership_lease_id)
        }) && status.control_plane_ready
    })
    .await;
    let coordination = recovered
        .coordination
        .expect("coordination state should be present");

    assert_eq!(coordination.membership_lease_id, Some(membership_lease_id));
    assert!(
        !lease_is_granted(&endpoints, leadership_lease_id).await,
        "the orphaned leadership lease should be revoked"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_node_re_registers_after_membership_lease_revocation() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_node_re_registers_after_membership_lease_revocation").await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("re-register-after-membership-revocation");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-re-register-after-membership-revocation";
    let config = short_ttl_config(
        "leader-a",
        "etcd-re-register-after-membership-revocation",
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    let state = Arc::new(AppState::new(config));
    let (membership_lease_id, leadership_lease_id) = wait_for_local_leadership(&state).await;

    revoke_lease_out_of_band(&endpoints, membership_lease_id).await;

    let recovered = wait_for_runtime_status(&state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination
                    .membership_lease_id
                    .is_some_and(|lease_id| lease_id != membership_lease_id)
                && coordination.is_local_leader
        }) && status.control_plane_ready
            && status.data_plane_ready
    })
    .await;
    let coordination = recovered
        .coordination
        .expect("coordination state should be present");

    assert_eq!(coordination.registered_members, vec!["leader-a".to_owned()]);
    assert_ne!(
        coordination.leadership_lease_id,
        Some(leadership_lease_id),
        "membership loss should give up the old leadership claim before re-campaigning"
    );
    assert!(!lease_is_granted(&endpoints, leadership_lease_id).await);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_node_re_registers_when_membership_record_disappears() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_node_re_registers_when_membership_record_disappears").await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("re-register-after-membership-key-delete");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-re-register-after-membership-key-delete";
    let config = short_ttl_config(
        "leader-a",
        "etcd-re-register-after-membership-key-delete",
        &endpoints,
        &key_prefix,
        cluster_name,
    );
    let state = Arc::new(AppState::new(config));
    let (membership_lease_id, leadership_lease_id) = wait_for_local_leadership(&state).await;

    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("raw etcd client should connect");
    client
        .delete(
            format!("{key_prefix}/clusters/{cluster_name}/members/leader-a"),
            None,
        )
        .await
        .expect("membership record should be removable without revoking the lease");

    let recovered = wait_for_runtime_status(&state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination
                    .membership_lease_id
                    .is_some_and(|lease_id| lease_id != membership_lease_id)
                && coordination.is_local_leader
        }) && status.control_plane_ready
    })
    .await;
    let coordination = recovered
        .coordination
        .expect("coordination state should be present");

    assert_eq!(coordination.registered_members, vec!["leader-a".to_owned()]);
    assert!(
        !lease_is_granted(&endpoints, membership_lease_id).await,
        "the membership lease without a record should be revoked"
    );
    assert!(
        !lease_is_granted(&endpoints, leadership_lease_id).await,
        "a node that lost membership should give up its old leadership lease"
    );

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_follower_takes_over_after_leader_loses_leadership_lease() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_follower_takes_over_after_leader_loses_leadership_lease")
            .await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("follower-takeover-after-leadership-revocation");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-follower-takeover-after-leadership-revocation";
    let leader = Arc::new(AppState::new(short_ttl_config(
        "leader-a",
        "etcd-follower-takeover-a",
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    let (_, leadership_lease_id) = wait_for_local_leadership(&leader).await;
    let follower = Arc::new(AppState::new(short_ttl_config(
        "leader-b",
        "etcd-follower-takeover-b",
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    wait_for_runtime_status(&follower, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && !coordination.is_local_leader
                && coordination.leader_node.as_deref() == Some("leader-a")
        })
    })
    .await;

    revoke_lease_out_of_band(&endpoints, leadership_lease_id).await;

    // Whichever node wins the new campaign, the cluster must converge on
    // exactly one leader backed by a fresh lease, and both nodes must agree.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let leader_status = leader
            .control
            .runtime_status()
            .await
            .expect("leader runtime status should load");
        let follower_status = follower
            .control
            .runtime_status()
            .await
            .expect("follower runtime status should load");
        let a = leader_status
            .coordination
            .as_ref()
            .expect("leader coordination state should be present");
        let b = follower_status
            .coordination
            .as_ref()
            .expect("follower coordination state should be present");
        let new_lease = a.leadership_lease_id.or(b.leadership_lease_id);
        if a.is_local_leader != b.is_local_leader
            && a.leader_node.is_some()
            && a.leader_node == b.leader_node
            && new_lease.is_some_and(|lease_id| lease_id != leadership_lease_id)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a new leader: leader={a:?} follower={b:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }

    cleanup_prefix(&endpoints, &key_prefix).await;
}

#[tokio::test]
async fn etcd_restarted_node_waits_out_its_stale_leader_key_then_leads() {
    let Some(endpoints) =
        etcd_endpoints_or_skip("etcd_restarted_node_waits_out_its_stale_leader_key_then_leads")
            .await
    else {
        return;
    };
    let key_prefix = unique_etcd_prefix("restart-waits-out-stale-leader-key");
    cleanup_prefix(&endpoints, &key_prefix).await;
    let cluster_name = "core-etcd-restart-waits-out-stale-leader-key";

    // Leave a leader key from a previous process of the same node, backed by a
    // lease that the new process does not hold.
    let mut client = Client::connect(endpoints.clone(), None)
        .await
        .expect("raw etcd client should connect");
    let stale_lease_id = client
        .lease_grant(60, None)
        .await
        .expect("stale leadership lease should be granted")
        .id();
    client
        .put(
            format!("{key_prefix}/clusters/{cluster_name}/controllers/leader"),
            serde_json::to_string(&LeadershipRecord {
                node_id: "leader-a".to_owned(),
                lease_id: stale_lease_id,
            })
            .expect("leader record should encode"),
            Some(PutOptions::new().with_lease(stale_lease_id)),
        )
        .await
        .expect("stale leader key should be written");

    let state = Arc::new(AppState::new(short_ttl_config(
        "leader-a",
        "etcd-restart-waits-out-stale-leader-key",
        &endpoints,
        &key_prefix,
        cluster_name,
    )));
    let waiting = wait_for_runtime_status(&state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered && coordination.leader_node.is_some()
        })
    })
    .await;
    let waiting = waiting
        .coordination
        .expect("coordination state should be present");
    assert!(
        !waiting.is_local_leader,
        "a leader key backed by another lease must not count as local leadership"
    );
    assert_eq!(waiting.leader_node.as_deref(), Some("leader-a"));

    revoke_lease_out_of_band(&endpoints, stale_lease_id).await;

    let (_, leadership_lease_id) = wait_for_local_leadership(&state).await;
    assert_ne!(leadership_lease_id, stale_lease_id);

    cleanup_prefix(&endpoints, &key_prefix).await;
}

/// Config with TTLs short enough that the coordination loop ticks every second.
fn short_ttl_config(
    node_name: &str,
    temp_label: &str,
    endpoints: &[String],
    key_prefix: &str,
    cluster_name: &str,
) -> LogPoseConfig {
    let mut config = test_config(
        node_name,
        unique_temp_dir(temp_label),
        endpoints,
        key_prefix,
        cluster_name,
    );
    config.node_role = NodeRole::Combined;
    config.metadata.etcd.membership_ttl_secs = 3;
    config.metadata.etcd.leadership_ttl_secs = 3;
    config
}

/// Wait until the node leads and return its (membership, leadership) lease ids.
async fn wait_for_local_leadership(state: &AppState) -> (i64, i64) {
    let status = wait_for_runtime_status(state, |status| {
        status.coordination.as_ref().is_some_and(|coordination| {
            coordination.membership_registered
                && coordination.is_local_leader
                && coordination.membership_lease_id.is_some()
                && coordination.leadership_lease_id.is_some()
        }) && status.control_plane_ready
    })
    .await;
    let coordination = status
        .coordination
        .expect("coordination state should be present");
    (
        coordination
            .membership_lease_id
            .expect("membership lease id should be present"),
        coordination
            .leadership_lease_id
            .expect("leadership lease id should be present"),
    )
}

async fn revoke_lease_out_of_band(endpoints: &[String], lease_id: i64) {
    let mut client = Client::connect(endpoints.to_vec(), None)
        .await
        .expect("raw etcd client should connect");
    client
        .lease_revoke(lease_id)
        .await
        .expect("lease should be revocable out of band");
}

async fn lease_is_granted(endpoints: &[String], lease_id: i64) -> bool {
    let mut client = Client::connect(endpoints.to_vec(), None)
        .await
        .expect("raw etcd client should connect");
    client
        .lease_time_to_live(lease_id, None)
        .await
        .is_ok_and(|response| response.ttl() > 0)
}

async fn visible_leader(
    endpoints: &[String],
    key_prefix: &str,
    cluster_name: &str,
) -> Option<LeadershipRecord> {
    let mut client = Client::connect(endpoints.to_vec(), None)
        .await
        .expect("raw etcd client should connect");
    let response = client
        .get(
            format!("{key_prefix}/clusters/{cluster_name}/controllers/leader"),
            None,
        )
        .await
        .expect("leader key should be readable");
    response
        .kvs()
        .first()
        .map(|kv| serde_json::from_slice(kv.value()).expect("leader record should decode"))
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

/// Returns the etcd endpoints for an integration test, or `None` to skip it.
///
/// The etcd tests run only when `LOGPOSE_TEST_ETCD_ENDPOINTS` names one or more
/// comma-separated endpoints. When it does, an unreachable etcd fails the test
/// instead of skipping it. Under CI (`CI` is set, as on GitHub Actions) a
/// missing variable also fails, so CI cannot silently lose this coverage.
async fn etcd_endpoints_or_skip(test_name: &str) -> Option<Vec<String>> {
    let endpoints = std::env::var(ETCD_ENDPOINTS_ENV)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if endpoints.is_empty() {
        assert!(
            std::env::var_os("CI").is_none(),
            "{test_name}: CI is set but {ETCD_ENDPOINTS_ENV} is not; set it so the etcd integration tests run in CI"
        );
        eprintln!(
            "skipping {test_name}: set {ETCD_ENDPOINTS_ENV} (for example http://127.0.0.1:2379) to run the etcd integration tests"
        );
        return None;
    }

    let probe = async {
        let mut client = Client::connect(endpoints.clone(), None).await?;
        client.status().await.map(|_| ())
    };
    let reachable = tokio::time::timeout(Duration::from_secs(5), probe).await;
    assert!(
        matches!(reachable, Ok(Ok(()))),
        "{ETCD_ENDPOINTS_ENV} is set to {endpoints:?} but etcd is unreachable: {reachable:?}; start etcd or unset {ETCD_ENDPOINTS_ENV} to skip the etcd tests"
    );
    Some(endpoints)
}

async fn wait_for_runtime_status(
    state: &AppState,
    ready: impl Fn(&logpose_types::NodeRuntimeStatus) -> bool,
) -> logpose_types::NodeRuntimeStatus {
    let deadline = Instant::now() + Duration::from_secs(15);
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

/// Restart a node so that its engine recovers the collection state mirrored into its storage
/// root, as a node that received a state transfer would. The engine keeps collection state
/// resident and owns its root exclusively, so the running instance is dropped first.
async fn restart_with_mirrored_state(node: Arc<AppState>, config: &LogPoseConfig) -> Arc<AppState> {
    drop(node);
    let node = Arc::new(AppState::new(config.clone()));
    wait_for_runtime_status(&node, |status| {
        status
            .coordination
            .as_ref()
            .is_some_and(|coordination| coordination.membership_registered)
    })
    .await;
    node
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
