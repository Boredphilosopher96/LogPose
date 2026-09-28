//! Integration tests for `logpose-storage` workflows.

use arc_swap as _;
use bytemuck as _;
use crc32c as _;
use imbl as _;
use logpose_auth as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query as _;
use logpose_vfs as _;
use logpose_wal as _;
use postcard as _;
use rand as _;
use rayon as _;
use roaring as _;
use serde as _;
use thiserror as _;
use tracing as _;
use twox_hash as _;
use uuid as _;

#[path = "support/engine.rs"]
mod db;
#[path = "support/fs.rs"]
mod support;

use db::{create, delete, describe, handle, open, pin, put_with, scan, scan_at_token};

use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
    Principal, PrincipalKind,
};
use logpose_catalog::{CatalogStore, DatabaseDescriptor};
use logpose_storage::{CreateCollectionRequest, Engine, InspectTarget};
use logpose_types::{
    CorruptionKind, DEFAULT_DATABASE_NAME, DistanceMetric, ErrorCode, LogPoseError, Snapshot,
};
use serde_json::{Value, json};
use std::{
    fs,
    time::{Duration, Instant},
};

#[tokio::test]
async fn create_write_scan_and_delete_records() {
    let root_dir = support::unique_temp_dir("storage-write-scan");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("colors", 2, DistanceMetric::Cosine),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "colors")
        .write(vec![
            put_with("alpha", vec![1.0, 0.0], json!({"color":"red"})),
            put_with("beta", vec![0.0, 1.0], json!({"color":"green"})),
        ])
        .await
        .expect("writes should succeed");

    let before_delete = scan(&engine, "colors", None)
        .await
        .expect("scan should succeed");
    assert_eq!(before_delete.len(), 2);

    handle(&engine, "colors")
        .write(vec![delete("alpha")])
        .await
        .expect("delete should succeed");

    let after_delete = scan(&engine, "colors", None)
        .await
        .expect("scan should succeed");
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0].id.as_str(), "beta");
}

#[tokio::test]
async fn create_collection_persists_default_database_descriptor() {
    let root_dir = support::unique_temp_dir("storage-default-database");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let descriptor = create(
        &engine,
        CreateCollectionRequest::new("colors", 2, DistanceMetric::Cosine),
    )
    .await
    .expect("collection should be created")
    .describe();

    let database_descriptor_path = root
        .join("databases")
        .join(DEFAULT_DATABASE_NAME)
        .join("descriptor.json");
    let database_descriptor: logpose_catalog::DatabaseDescriptor = serde_json::from_slice(
        &fs::read(&database_descriptor_path).expect("database descriptor should exist"),
    )
    .expect("database descriptor JSON should parse");

    assert_eq!(descriptor.database_name, DEFAULT_DATABASE_NAME);
    assert_eq!(database_descriptor.name, DEFAULT_DATABASE_NAME);
    assert!(database_descriptor.is_default);
}

#[test]
fn stored_descriptors_that_fail_validation_are_reported_as_corrupt() {
    let root_dir = support::unique_temp_dir("storage-catalog-invalid-stored");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);
    engine
        .put_database(DatabaseDescriptor::new("analytics"))
        .expect("database descriptor should persist");
    engine
        .put_principal(Principal::new_with_access_tier(
            "reader",
            PrincipalKind::User,
            AccessTier::Observer,
        ))
        .expect("principal descriptor should persist");

    // Damage the stored copies so they no longer pass validation: only the default database
    // may be the default, and a principal name may not contain '/'.
    let database_path = root.join("databases/analytics/descriptor.json");
    let mut database: Value =
        serde_json::from_slice(&fs::read(&database_path).expect("database descriptor exists"))
            .expect("database descriptor is JSON");
    database["is_default"] = json!(true);
    fs::write(&database_path, database.to_string()).expect("database descriptor rewrites");
    let principal_path = root.join("principals/reader/descriptor.json");
    let mut principal: Value =
        serde_json::from_slice(&fs::read(&principal_path).expect("principal descriptor exists"))
            .expect("principal descriptor is JSON");
    principal["name"] = json!("a/b");
    fs::write(&principal_path, principal.to_string()).expect("principal descriptor rewrites");

    for error in [
        engine
            .get_database("analytics")
            .expect_err("a damaged database descriptor should fail"),
        engine
            .list_databases()
            .expect_err("listing a damaged database descriptor should fail"),
        engine
            .get_principal("reader")
            .expect_err("a damaged principal descriptor should fail"),
        engine
            .list_principals()
            .expect_err("listing a damaged principal descriptor should fail"),
    ] {
        assert_eq!(error.code(), ErrorCode::DataLoss, "{error}");
        assert!(
            matches!(
                &error,
                LogPoseError::Corrupt {
                    kind: CorruptionKind::Descriptor,
                    location: Some(_),
                    ..
                }
            ),
            "{error:?}"
        );
    }
}

#[test]
fn catalog_store_round_trips_databases_principals_and_policies() {
    let root_dir = support::unique_temp_dir("storage-catalog-round-trip");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let database = engine
        .put_database(DatabaseDescriptor::new("analytics"))
        .expect("database descriptor should persist");
    let principal = engine
        .put_principal(Principal::new_with_access_tier(
            "reader",
            PrincipalKind::User,
            AccessTier::Observer,
        ))
        .expect("principal descriptor should persist");
    let policy = engine
        .put_database_access_policy(DatabaseAccessPolicy {
            database_name: "analytics".to_owned(),
            authentication_mode: AuthenticationMode::ExternalToken,
            role_bindings: vec![DatabaseRoleBinding {
                database_name: "analytics".to_owned(),
                principal_name: "reader".to_owned(),
                role: DatabaseRole::ReadOnly,
            }],
        })
        .expect("database policy should persist");

    assert_eq!(
        engine
            .get_database("analytics")
            .expect("database lookup should succeed"),
        database
    );
    assert_eq!(
        engine
            .get_principal("reader")
            .expect("principal lookup should succeed"),
        principal
    );
    assert_eq!(
        engine
            .get_database_access_policy("analytics")
            .expect("policy lookup should succeed"),
        policy
    );
    let databases = engine
        .list_databases()
        .expect("database listing should succeed");
    assert_eq!(databases.len(), 2);
    assert!(
        databases
            .iter()
            .any(|descriptor| descriptor.name == database.name && !descriptor.is_default)
    );
    assert!(
        databases
            .iter()
            .any(|descriptor| descriptor.name == "default" && descriptor.is_default)
    );
    assert_eq!(
        engine
            .list_principals()
            .expect("principal listing should succeed"),
        vec![principal.clone()]
    );

    let database_descriptor_path = root.join("databases/analytics/descriptor.json");
    let principal_descriptor_path = root.join("principals/reader/descriptor.json");
    let policy_path = root.join("databases/analytics/policy.json");

    assert_eq!(
        serde_json::from_slice::<DatabaseDescriptor>(
            &fs::read(&database_descriptor_path).expect("database descriptor file should exist")
        )
        .expect("database descriptor JSON should parse"),
        database
    );
    assert_eq!(
        serde_json::from_slice::<Principal>(
            &fs::read(&principal_descriptor_path).expect("principal descriptor file should exist")
        )
        .expect("principal descriptor JSON should parse"),
        principal
    );
    assert_eq!(
        serde_json::from_slice::<DatabaseAccessPolicy>(
            &fs::read(&policy_path).expect("policy file should exist")
        )
        .expect("policy JSON should parse"),
        policy
    );

    drop(engine);
    let reopened = open(&root);
    assert_eq!(
        reopened
            .get_database("analytics")
            .expect("reopened database lookup should succeed"),
        database
    );
    assert_eq!(
        reopened
            .get_principal("reader")
            .expect("reopened principal lookup should succeed"),
        principal
    );
    assert_eq!(
        reopened
            .get_database_access_policy("analytics")
            .expect("reopened policy lookup should succeed"),
        policy
    );
}

#[test]
fn catalog_store_overwrites_database_policy_by_database_name() {
    let root_dir = support::unique_temp_dir("storage-catalog-database-isolation");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let database = engine
        .put_database(DatabaseDescriptor::new("analytics"))
        .expect("database should persist");

    let owner_policy = engine
        .put_database_access_policy(DatabaseAccessPolicy {
            database_name: "analytics".to_owned(),
            authentication_mode: AuthenticationMode::ExternalToken,
            role_bindings: vec![DatabaseRoleBinding {
                database_name: "analytics".to_owned(),
                principal_name: "owner-reader".to_owned(),
                role: DatabaseRole::Owner,
            }],
        })
        .expect("owner policy should persist");
    let read_only_policy = engine
        .put_database_access_policy(DatabaseAccessPolicy {
            database_name: "analytics".to_owned(),
            authentication_mode: AuthenticationMode::ExternalToken,
            role_bindings: vec![DatabaseRoleBinding {
                database_name: "analytics".to_owned(),
                principal_name: "readonly-reader".to_owned(),
                role: DatabaseRole::ReadOnly,
            }],
        })
        .expect("read-only policy should replace the existing database policy");

    assert_eq!(
        engine
            .get_database_access_policy("analytics")
            .expect("database policy lookup should succeed"),
        read_only_policy
    );

    assert_eq!(
        engine
            .get_database("analytics")
            .expect("database descriptor should still load"),
        database
    );
    assert_ne!(owner_policy, read_only_policy);
}

#[test]
fn put_database_preserves_stable_database_identity_on_replace() {
    let root_dir = support::unique_temp_dir("storage-database-idempotence");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let first = engine
        .put_database(DatabaseDescriptor::new("analytics"))
        .expect("first database descriptor should persist");
    let replacement = engine
        .put_database(DatabaseDescriptor::new("analytics"))
        .expect("replacing a database descriptor should preserve its identity");

    assert_eq!(replacement.name, "analytics");
    assert_eq!(replacement.database_id, first.database_id);
}

#[tokio::test]
async fn duplicate_collection_names_can_exist_in_different_databases() {
    let root_dir = support::unique_temp_dir("storage-namespace-duplicates");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let default_descriptor = create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("default namespace collection should be created")
    .describe();
    let analytics_descriptor = create(
        &engine,
        CreateCollectionRequest::in_database("analytics", "documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("analytics database collection should be created")
    .describe();
    assert_ne!(
        default_descriptor.collection_id,
        analytics_descriptor.collection_id
    );

    let opened_default =
        describe(&engine, "documents").expect("default namespace lookup should work");
    let opened_analytics =
        describe(&engine, "analytics/documents").expect("database-qualified lookup should work");

    assert_eq!(opened_default.database_name, "default");
    assert_eq!(opened_analytics.database_name, "analytics");

    let explicit_analytics =
        describe(&engine, "analytics/documents").expect("explicit database lookup should work");
    assert_eq!(
        explicit_analytics.collection_id,
        analytics_descriptor.collection_id
    );
}

#[tokio::test]
async fn create_collection_allows_duplicate_names_in_distinct_databases() {
    let root_dir = support::unique_temp_dir("storage-duplicate-collection-namespaces");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let left = create(
        &engine,
        CreateCollectionRequest::in_database(
            DEFAULT_DATABASE_NAME,
            "events",
            2,
            DistanceMetric::Dot,
        ),
    )
    .await
    .expect("first collection should be created")
    .describe();

    let right = create(
        &engine,
        CreateCollectionRequest::in_database("analytics", "events", 2, DistanceMetric::Dot),
    )
    .await
    .expect("second collection in another database should be created")
    .describe();

    assert_eq!(left.name, "events");
    assert_eq!(right.name, "events");
    assert_ne!(left.collection_id, right.collection_id);
    assert_eq!(left.database_name, DEFAULT_DATABASE_NAME);
    assert_eq!(right.database_name, "analytics");

    let descriptors = engine
        .list_collections()
        .expect("collection listing should succeed");
    assert_eq!(
        descriptors
            .iter()
            .filter(|descriptor| descriptor.name == "events")
            .count(),
        2
    );
}

#[tokio::test]
async fn create_collection_rejects_reserved_namespace_separator() {
    let root_dir = support::unique_temp_dir("storage-reserved-separator");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let error = create(
        &engine,
        CreateCollectionRequest::in_database("analytics", "docs/v2", 2, DistanceMetric::Dot),
    )
    .await
    .expect_err("slash-containing collection names should fail");

    assert!(error.to_string().contains("collection_name"));
    assert!(error.to_string().contains("/"));
}

#[tokio::test]
async fn open_collection_resolves_database_collection_tuple() {
    let root_dir = support::unique_temp_dir("storage-open-collection-namespace");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let default_descriptor = create(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
    )
    .await
    .expect("default namespace collection should be created")
    .describe();
    let analytics_descriptor = create(
        &engine,
        CreateCollectionRequest::in_database("analytics", "events", 2, DistanceMetric::Dot),
    )
    .await
    .expect("analytics database collection should be created")
    .describe();

    let default_lookup =
        describe(&engine, "events").expect("default namespace lookup should succeed");
    assert_eq!(
        default_lookup.collection_id,
        default_descriptor.collection_id
    );

    let explicit_lookup =
        describe(&engine, "analytics/events").expect("explicit database lookup should succeed");
    assert_eq!(
        explicit_lookup.collection_id,
        analytics_descriptor.collection_id
    );
    assert_eq!(explicit_lookup.database_name, "analytics");

    let slash_lookup =
        describe(&engine, "analytics/events").expect("database-qualified lookup should succeed");
    assert_eq!(
        slash_lookup.collection_id,
        analytics_descriptor.collection_id
    );
}

#[tokio::test]
async fn flush_persists_visible_records_for_reopen() {
    let root_dir = support::unique_temp_dir("storage-flush");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("documents", 3, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "documents")
        .write(vec![put_with(
            "doc-1",
            vec![0.1, 0.2, 0.3],
            json!({"topic":"intro"}),
        )])
        .await
        .expect("write should succeed");

    handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");

    drop(engine);
    let reopened = open(&root);
    let visible = scan(&reopened, "documents", None)
        .await
        .expect("scan should succeed");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "doc-1");

    let stats = handle(&reopened, "documents")
        .stats(None)
        .expect("stats should succeed");
    assert_eq!(stats.manifest_generation, 1);
    assert_eq!(stats.visible_seq_no, 1);
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 0);
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(stats.deleted_record_count, 0);
    assert!(stats.maintenance.pending.is_empty());
    assert!(stats.maintenance.in_progress.is_none());
    assert_eq!(stats.maintenance.last_error, None);
    assert!(
        stats.query_units.iter().any(|unit| unit.tier == "mutable"),
        "mutable unit should still be reported for planner visibility"
    );

    let immutable = stats
        .query_units
        .iter()
        .find(|unit| unit.tier == "immutable")
        .expect("immutable unit should be reported");
    assert_eq!(
        immutable.index_kind, "flat",
        "a one-row segment has no graph or SQ8 codes, so it is searched exactly"
    );
    assert!(
        immutable
            .artifact_stats
            .iter()
            .any(|artifact| artifact.file_name == format!("{}.seg", immutable.unit_id))
    );
    assert!(
        immutable
            .component_bytes
            .get("segment")
            .copied()
            .unwrap_or_default()
            > 0
    );
    assert!(
        immutable.scalar_fields.is_empty(),
        "`$extra` keys have no zone maps"
    );
}

#[tokio::test]
async fn reopen_after_flush_and_new_write_only_replays_the_post_checkpoint_delta() {
    let root_dir = support::unique_temp_dir("storage-reopen-post-checkpoint-delta");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "documents")
        .write(vec![put_with(
            "alpha",
            vec![1.0, 0.0],
            json!({"version":1}),
        )])
        .await
        .expect("first write should succeed");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");
    handle(&engine, "documents")
        .write(vec![put_with("beta", vec![0.0, 1.0], json!({"version":2}))])
        .await
        .expect("second write should succeed");

    drop(engine);
    let reopened = open(&root);
    let visible = scan(&reopened, "documents", None)
        .await
        .expect("scan should succeed after reopen");
    assert_eq!(visible.len(), 2);
    assert_eq!(visible[0].id.as_str(), "alpha");
    assert_eq!(visible[1].id.as_str(), "beta");

    let stats = handle(&reopened, "documents")
        .stats(None)
        .expect("stats should succeed after reopen");
    assert_eq!(stats.visible_seq_no, 2);
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 1);
}

#[tokio::test]
async fn checkpointed_rolled_wal_corruption_does_not_block_recovery() {
    let root_dir = support::unique_temp_dir("storage-checkpointed-wal-corruption");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let descriptor = create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created")
    .describe();

    handle(&engine, "documents")
        .write(vec![put_with(
            "alpha",
            vec![1.0, 0.0],
            json!({"version":1}),
        )])
        .await
        .expect("write should succeed");
    let flushed = handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");

    let rolled_wal_path = descriptor
        .root_path
        .join("wal")
        .join(format!("{:020}.wal", flushed.visible_seq_no));
    fs::write(&rolled_wal_path, b"corrupt checkpointed wal")
        .expect("corrupted rolled wal should be written");

    drop(engine);
    let reopened = open(&root);
    let visible = scan(&reopened, "documents", None)
        .await
        .expect("checkpointed wal corruption should be ignored once the manifest covers it");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");

    let stats = handle(&reopened, "documents")
        .stats(None)
        .expect("stats should still load after reopen");
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 0);
    assert_eq!(stats.live_record_count, 1);
}

#[tokio::test]
async fn a_pinned_snapshot_reads_exactly_its_state_after_a_flush_until_a_restart() {
    let root_dir = support::unique_temp_dir("storage-old-snapshot-rotated-wal");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let descriptor = create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created")
    .describe();

    handle(&engine, "documents")
        .write(vec![put_with(
            "alpha",
            vec![1.0, 0.0],
            json!({"version":1}),
        )])
        .await
        .expect("write should succeed");
    let (token, pre_flush_snapshot) = pin(&engine, "documents").expect("pin");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");
    handle(&engine, "documents")
        .write(vec![put_with("beta", vec![0.0, 1.0], json!({"version":1}))])
        .await
        .expect("write after the flush should succeed");
    assert_eq!(
        wal_file_count(&descriptor.root_path),
        1,
        "the flush rotated the WAL and deleted the checkpointed file"
    );

    let old_snapshot_stats = handle(&engine, "documents")
        .stats_at_token(&token)
        .expect("the pinned state is readable");
    assert_eq!(old_snapshot_stats.live_record_count, 1);
    assert_eq!(old_snapshot_stats.mutable_op_count, 1);
    assert_eq!(
        old_snapshot_stats.manifest_generation,
        pre_flush_snapshot.manifest_generation
    );
    let visible = scan(&engine, "documents", Some(pre_flush_snapshot.clone()))
        .await
        .expect("the exact snapshot a token pins stays readable");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");

    // Pins live in memory: a restart ends them.
    drop(engine);
    let reopened = open(&root);
    let error = scan_at_token(&reopened, "documents", token)
        .await
        .expect_err("tokens do not survive a restart");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
    let error = handle(&reopened, "documents")
        .stats(Some(pre_flush_snapshot))
        .expect_err("an older generation is not retained");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn a_pinned_snapshot_preserves_pre_compaction_history() {
    let root_dir = support::unique_temp_dir("storage-old-snapshot-compaction-history");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "documents")
        .write(vec![put_with(
            "alpha",
            vec![1.0, 0.0],
            json!({"version":1}),
        )])
        .await
        .expect("first write should succeed");
    let (token, old_snapshot) = pin(&engine, "documents").expect("pin");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("first flush should succeed");

    handle(&engine, "documents")
        .write(vec![put_with(
            "alpha",
            vec![2.0, 0.0],
            json!({"version":2}),
        )])
        .await
        .expect("second write should succeed");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("second flush should succeed");
    handle(&engine, "documents")
        .compact()
        .await
        .expect("compaction should succeed");

    handle(&engine, "documents")
        .write(vec![put_with("beta", vec![0.0, 1.0], json!({"version":3}))])
        .await
        .expect("third write should succeed");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("third flush should succeed");

    let old_snapshot_stats = handle(&engine, "documents")
        .stats_at_token(&token)
        .expect("pinned stats stay readable after compaction");
    assert_eq!(
        old_snapshot_stats.manifest_generation,
        old_snapshot.manifest_generation
    );
    assert_eq!(old_snapshot_stats.live_record_count, 1);
    assert_eq!(old_snapshot_stats.mutable_op_count, 1);
    assert_eq!(old_snapshot_stats.segment_count, 0);

    let visible = scan_at_token(&engine, "documents", token.clone())
        .await
        .expect("the pinned state keeps the pre-compaction record");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");
    assert_eq!(visible[0].metadata["version"], json!(1));

    assert!(handle(&engine, "documents").release_snapshot(&token));
    let error = scan(&engine, "documents", Some(old_snapshot))
        .await
        .expect_err("released");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn compact_merges_segments_and_preserves_latest_versions() {
    let root_dir = support::unique_temp_dir("storage-compact");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("profiles", 2, DistanceMetric::L2),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "profiles")
        .write(vec![put_with(
            "alpha",
            vec![1.0, 1.0],
            json!({"version":1}),
        )])
        .await
        .expect("write should succeed");
    handle(&engine, "profiles")
        .flush()
        .await
        .expect("flush should succeed");

    handle(&engine, "profiles")
        .write(vec![
            put_with("alpha", vec![2.0, 2.0], json!({"version":2})),
            put_with("beta", vec![3.0, 3.0], json!({"version":1})),
        ])
        .await
        .expect("write should succeed");
    handle(&engine, "profiles")
        .flush()
        .await
        .expect("flush should succeed");

    let before = handle(&engine, "profiles")
        .stats(None)
        .expect("stats should succeed");
    assert_eq!(before.live_record_count, 2);
    assert_eq!(
        before.deleted_record_count, 1,
        "the first segment's alpha row is deleted by the upsert"
    );
    assert_eq!(before.segment_count, 2);

    handle(&engine, "profiles")
        .compact()
        .await
        .expect("compaction should succeed");

    let after = handle(&engine, "profiles")
        .stats(None)
        .expect("stats should succeed");
    assert_eq!(after.live_record_count, 2);
    assert_eq!(
        after.deleted_record_count, 0,
        "compaction drops the deleted row"
    );
    assert_eq!(after.segment_count, 1);

    let visible = scan(&engine, "profiles", None)
        .await
        .expect("scan should succeed");
    assert_eq!(visible.len(), 2);
    let alpha = visible
        .iter()
        .find(|record| record.id.as_str() == "alpha")
        .expect("alpha should be present");
    assert_eq!(alpha.vector, vec![2.0, 2.0]);
}

#[tokio::test]
async fn inspect_reports_manifest_wal_and_segment_targets() {
    let root_dir = support::unique_temp_dir("storage-inspect");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Cosine),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "documents")
        .write(vec![
            put_with("alpha", vec![1.0, 0.0], json!({"version":1})),
            put_with("beta", vec![0.0, 1.0], json!({"version":1})),
        ])
        .await
        .expect("write should succeed");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");
    handle(&engine, "documents")
        .write(vec![delete("alpha")])
        .await
        .expect("delete should succeed");

    let manifest = handle(&engine, "documents")
        .inspect(InspectTarget::Manifest)
        .await
        .expect("manifest inspect should succeed");
    assert_eq!(manifest.target, "manifest");

    let manifest_body = manifest
        .payload
        .as_object()
        .expect("manifest payload should be an object");
    let segments = manifest_body["segments"]
        .as_array()
        .expect("manifest segments should be an array");
    assert_eq!(segments.len(), 1);
    let segment_id = segments[0]["segment_id"]
        .as_str()
        .expect("segment id should be a string")
        .to_owned();

    let wal = handle(&engine, "documents")
        .inspect(InspectTarget::Wal)
        .await
        .expect("wal inspect should succeed");
    assert_eq!(wal.target, "wal");
    let wal_records = wal
        .payload
        .get("records")
        .and_then(Value::as_array)
        .expect("wal records should be an array");
    assert!(
        wal_records.is_empty(),
        "the memtable holds no row: the delete only set a deletion bit"
    );
    assert_eq!(
        wal.payload["visible_seq_no"].as_u64(),
        wal.payload["checkpoint_seq_no"].as_u64().map(|seq| seq + 1),
        "one operation above the checkpoint"
    );

    let segment = handle(&engine, "documents")
        .inspect(InspectTarget::Segment(segment_id.clone()))
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
    assert_eq!(
        segment.payload["segment"]["file_name"].as_str(),
        Some(format!("{segment_id}.seg").as_str())
    );
    assert_eq!(segment.payload["segment"]["deleted_rows"], 1);
    assert_eq!(segment.payload["segment"]["live_rows"], 1);
    let kinds = segment.payload["sections"]
        .as_array()
        .expect("segment sections should be an array")
        .iter()
        .filter_map(|section| section["kind"].as_str())
        .collect::<Vec<_>>();
    for kind in [
        "SchemaSnapshot",
        "RowMeta",
        "PkColumn",
        "VectorF32",
        "Stats",
    ] {
        assert!(kinds.contains(&kind), "{kind} in {kinds:?}");
    }
    let records = segment
        .payload
        .get("records")
        .and_then(Value::as_array)
        .expect("segment records should be an array");
    assert_eq!(records.len(), 2);
    assert_eq!(
        records
            .iter()
            .map(|record| (record["pk"].as_str(), record["deleted"].as_bool()))
            .collect::<Vec<_>>(),
        [(Some("alpha"), Some(true)), (Some("beta"), Some(false))]
    );

    let maintenance = handle(&engine, "documents")
        .inspect(InspectTarget::Maintenance)
        .await
        .expect("maintenance inspect should succeed");
    assert_eq!(maintenance.target, "maintenance");
    assert_eq!(
        maintenance
            .payload
            .get("pending")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );

    let stats = handle(&engine, "documents")
        .stats(None)
        .expect("stats should succeed");
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(stats.deleted_record_count, 1);
    assert_eq!(stats.mutable_op_count, 1);
    assert_eq!(stats.segment_count, 1);
}

#[tokio::test]
async fn background_maintenance_flushes_and_compacts_using_thresholds() {
    let root_dir = support::unique_temp_dir("storage-background-maintenance");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create_with_thresholds(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
        1,
        1024,
        2,
    )
    .expect("collection should be created");

    handle(&engine, "events")
        .write(vec![put_with(
            "evt-1",
            vec![1.0, 0.0],
            json!({"kind":"keep","shard":1}),
        )])
        .await
        .expect("first write should succeed");

    wait_for_condition(&engine, "events", |stats| {
        stats.segment_count == 1 && stats.mutable_op_count == 0
    })
    .await;

    handle(&engine, "events")
        .write(vec![put_with(
            "evt-2",
            vec![2.0, 0.0],
            json!({"kind":"keep","shard":2}),
        )])
        .await
        .expect("second write should succeed");

    wait_for_condition(&engine, "events", |stats| {
        stats.segment_count == 1
            && stats.mutable_op_count == 0
            && stats.maintenance.completed_runs >= 3
            && stats.maintenance.pending.is_empty()
            && stats.maintenance.in_progress.is_none()
    })
    .await;

    let stats = handle(&engine, "events")
        .stats(None)
        .expect("stats should succeed");
    assert_eq!(stats.live_record_count, 2);
    assert_eq!(stats.deleted_record_count, 0);
    assert_eq!(stats.segment_count, 1);
    assert!(stats.maintenance.pending.is_empty());
    assert!(stats.maintenance.in_progress.is_none());
    assert_eq!(stats.maintenance.last_error, None);
}

#[tokio::test]
async fn background_maintenance_preserves_namespace_for_duplicate_collection_names() {
    let root_dir = support::unique_temp_dir("storage-background-maintenance-namespace");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
    )
    .await
    .expect("default namespace collection should be created");
    create_with_thresholds(
        &engine,
        CreateCollectionRequest::in_database("analytics", "events", 2, DistanceMetric::Dot),
        1,
        1024,
        2,
    )
    .expect("database namespace collection should be created");

    handle(&engine, "analytics/events")
        .write(vec![put_with(
            "evt-1",
            vec![1.0, 0.0],
            json!({"namespace":"analytics","shard":1}),
        )])
        .await
        .expect("database write should succeed");

    wait_for_condition(&engine, "analytics/events", |stats| {
        stats.segment_count == 1 && stats.mutable_op_count == 0
    })
    .await;

    let default_stats = handle(&engine, "events")
        .stats(None)
        .expect("default namespace stats should succeed");
    assert_eq!(default_stats.segment_count, 0);
    assert_eq!(default_stats.mutable_op_count, 0);

    let analytics_stats = handle(&engine, "analytics/events")
        .stats(None)
        .expect("database namespace stats should succeed");
    assert_eq!(analytics_stats.segment_count, 1);
    assert_eq!(analytics_stats.mutable_op_count, 0);
    assert_eq!(analytics_stats.live_record_count, 1);
}

/// A byte flipped inside a segment's vector data leaves the segment openable (its header,
/// footer, and section table are intact) but fails every read of that data with typed segment
/// corruption, never a wrong answer or a panic.
#[tokio::test]
async fn queries_surface_a_corrupted_segment_section_as_typed_corruption() {
    let root_dir = support::unique_temp_dir("storage-segment-corruption");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);
    let descriptor = create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created")
    .describe();
    handle(&engine, "documents")
        .write(vec![
            put_with("alpha", vec![1.0, 0.0], json!({"kind":"keep"})),
            put_with("beta", vec![2.0, 0.0], json!({"kind":"keep"})),
        ])
        .await
        .expect("write should succeed");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");
    let segment_id = handle(&engine, "documents")
        .inspect(InspectTarget::Manifest)
        .await
        .expect("manifest inspect should succeed")
        .payload["segments"][0]["segment_id"]
        .as_str()
        .expect("segment id should exist")
        .to_owned();
    drop(engine);

    let path = descriptor
        .root_path
        .join("segments")
        .join(format!("{segment_id}.seg"));
    let (offset, length) = {
        let reader = logpose_storage::segment_v2::SegmentReader::open(
            logpose_storage::segment_v2::FileSource::open(&path).expect("segment opens"),
        )
        .expect("segment reads");
        let vectors = reader
            .sections()
            .iter()
            .find(|section| {
                section.section_kind() == Some(logpose_storage::segment_v2::SectionKind::VectorF32)
            })
            .expect("a vector section");
        (vectors.offset, vectors.length)
    };
    let mut bytes = fs::read(&path).expect("segment should read");
    let last = usize::try_from(offset + length - 1).expect("offset fits");
    bytes[last] ^= 0x40;
    fs::write(&path, &bytes).expect("corrupted segment should be written");

    let engine = open(&root);
    let error = logpose_query::query(
        &engine,
        &logpose_types::CollectionRef::parse("documents").expect("name"),
        logpose_query::QueryRequest {
            vector: Some(logpose_query::VectorQuery {
                field: None,
                values: vec![1.0, 0.0],
            }),
            top_k: 1,
            ..logpose_query::QueryRequest::default()
        },
    )
    .await
    .expect_err("a corrupted vector section fails the query");
    assert!(
        matches!(
            error,
            logpose_query::QueryError::Storage(LogPoseError::Corrupt {
                kind: CorruptionKind::Segment,
                ..
            })
        ),
        "unexpected error: {error:?}"
    );
    let error = scan(&engine, "documents", None)
        .await
        .expect_err("a scan reads the vector section too");
    assert!(
        matches!(
            error,
            LogPoseError::Corrupt {
                kind: CorruptionKind::Segment,
                ..
            }
        ),
        "{error:?}"
    );
}

/// ANN candidates over a segment come from an exact scan of its live rows: rows an upsert or a
/// delete superseded never appear, and filters apply before the budget.
#[tokio::test]
async fn ann_queries_over_segments_see_only_live_rows() {
    let root_dir = support::unique_temp_dir("storage-ann-live-rows");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);
    create(
        &engine,
        CreateCollectionRequest::new("documents", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created");
    handle(&engine, "documents")
        .write(
            (1..=12)
                .map(|index| {
                    put_with(
                        &format!("doc-{index:02}"),
                        vec![index as f32, 0.0],
                        json!({
                            "kind": if index % 3 == 0 { "drop" } else { "keep" }
                        }),
                    )
                })
                .collect(),
        )
        .await
        .expect("write should succeed");
    handle(&engine, "documents")
        .flush()
        .await
        .expect("flush should succeed");
    handle(&engine, "documents")
        .write(vec![
            put_with("doc-12", vec![0.5, 0.0], json!({"kind":"keep"})),
            delete("doc-11"),
        ])
        .await
        .expect("write should succeed");

    let collection = logpose_types::CollectionRef::parse("documents").expect("name");
    let query = |filter: Option<logpose_query::FilterExpr>| logpose_query::QueryRequest {
        vector: Some(logpose_query::VectorQuery {
            field: None,
            values: vec![1.0, 0.0],
        }),
        top_k: 2,
        filter,
        output_fields: vec!["id".to_owned()],
        ..logpose_query::QueryRequest::default()
    };
    let keep = || Some(logpose_query::FilterExpr::eq("kind", "keep"));
    let ids = |response: logpose_query::WithSchema<logpose_query::QueryResponse>| {
        response
            .value
            .hits
            .into_iter()
            .map(|hit| hit.record.pk.label())
            .collect::<Vec<_>>()
    };
    for round in 0..2 {
        let unfiltered = ids(logpose_query::query(&engine, &collection, query(None))
            .await
            .expect("query should succeed"));
        let filtered = ids(logpose_query::query(&engine, &collection, query(keep()))
            .await
            .expect("filtered query should succeed"));
        assert_eq!(unfiltered, ["doc-10", "doc-09"], "round {round}");
        assert_eq!(filtered, ["doc-10", "doc-08"], "round {round}");
        // The same answers once the upsert and the delete are flushed too.
        handle(&engine, "documents")
            .flush()
            .await
            .expect("flush should succeed");
    }
}

#[tokio::test]
async fn manual_flush_and_background_maintenance_do_not_race() {
    let root_dir = support::unique_temp_dir("storage-manual-background-race");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create_with_thresholds(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
        1,
        1024,
        2,
    )
    .expect("collection should be created");

    let writer = engine.clone();
    let manual = engine.clone();
    let write_task = tokio::spawn(async move {
        for index in 0..12 {
            handle(&writer, "events")
                .write(vec![put_with(
                    &format!("evt-{index}"),
                    vec![index as f32, 0.0],
                    json!({"kind":"keep","version":index}),
                )])
                .await?;
        }
        logpose_types::Result::<()>::Ok(())
    });
    let manual_task = tokio::spawn(async move {
        for _ in 0..12 {
            let _ = handle(&manual, "events").flush().await?;
        }
        logpose_types::Result::<()>::Ok(())
    });

    write_task
        .await
        .expect("write task should join")
        .expect("writes should succeed");
    manual_task
        .await
        .expect("manual maintenance task should join")
        .expect("manual flushes should succeed");

    wait_for_condition(&engine, "events", |stats| {
        stats.maintenance.in_progress.is_none() && stats.maintenance.pending.is_empty()
    })
    .await;

    let stats = handle(&engine, "events")
        .stats(None)
        .expect("stats should succeed");
    assert_eq!(stats.maintenance.last_error, None);
}

#[tokio::test]
async fn background_maintenance_handles_inflight_writes_without_losing_visibility() {
    let root_dir = support::unique_temp_dir("storage-follow-up-background-flush");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create_with_thresholds(
        &engine,
        CreateCollectionRequest::new("events", 65_536, DistanceMetric::Dot),
        1,
        usize::MAX,
        99,
    )
    .expect("collection should be created");

    handle(&engine, "events")
        .write(vec![put_with(
            "evt-1",
            vec![1.0; 65_536],
            json!({"kind":"keep","version":1}),
        )])
        .await
        .expect("first write should succeed");

    // Under load the first flush can start and finish between two polls, so a
    // completed run also counts; waiting for `in_progress` alone can miss it.
    wait_for_condition(&engine, "events", |stats| {
        stats.maintenance.in_progress.as_deref() == Some("flush")
            || stats.maintenance.completed_runs >= 1
    })
    .await;

    handle(&engine, "events")
        .write(vec![put_with(
            "evt-2",
            vec![2.0; 65_536],
            json!({"kind":"keep","version":2}),
        )])
        .await
        .expect("second write should succeed");

    wait_for_condition(&engine, "events", |stats| {
        stats.mutable_op_count == 0
            && stats.segment_count >= 1
            && stats.maintenance.pending.is_empty()
            && stats.maintenance.in_progress.is_none()
    })
    .await;

    let stats = handle(&engine, "events")
        .stats(None)
        .expect("stats should succeed");
    assert_eq!(stats.live_record_count, 2);
    assert_eq!(stats.mutable_op_count, 0);
    assert_eq!(stats.maintenance.last_error, None);
}

/// Maintenance status is runtime state: nothing about it is persisted. A reopened collection
/// plans whatever its recovered state is due (here, a replayed memtable over a flush trigger
/// lowered while the engine was down) once it has a data-plane access.
#[tokio::test]
async fn reopening_plans_the_maintenance_the_recovered_state_is_due() {
    let root_dir = support::unique_temp_dir("storage-resume-background-maintenance");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let descriptor = create(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created")
    .describe();

    handle(&engine, "events")
        .write(vec![put_with(
            "evt-1",
            vec![1.0, 0.0],
            json!({"kind":"keep"}),
        )])
        .await
        .expect("write should succeed");
    drop(engine);
    assert!(
        !descriptor.root_path.join("maintenance.json").exists(),
        "maintenance status is never persisted"
    );

    let descriptor_path = descriptor.root_path.join("descriptor.json");
    let mut stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&descriptor_path).expect("descriptor should read"))
            .expect("descriptor should parse");
    stored["flush_threshold_ops"] = json!(1);
    fs::write(
        &descriptor_path,
        serde_json::to_vec_pretty(&stored).expect("descriptor should serialize"),
    )
    .expect("descriptor should be written");

    let reopened = open(&root);
    let stats = handle(&reopened, "events")
        .stats(None)
        .expect("stats should succeed");
    assert_eq!(
        stats.maintenance,
        logpose_types::MaintenanceStatus::default()
    );
    wait_for_condition(&reopened, "events", |stats| {
        stats.mutable_op_count == 0
            && stats.segment_count == 1
            && stats.maintenance.pending.is_empty()
            && stats.maintenance.in_progress.is_none()
            && stats.maintenance.completed_runs >= 1
    })
    .await;
}

#[tokio::test]
async fn rejects_impossible_snapshots() {
    let root_dir = support::unique_temp_dir("storage-invalid-snapshot");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created");

    handle(&engine, "events")
        .write(vec![put_with(
            "evt-1",
            vec![1.0, 0.0],
            json!({"kind":"keep"}),
        )])
        .await
        .expect("write should succeed");

    let invalid_snapshot = Snapshot {
        manifest_generation: 0,
        visible_seq_no: 99,
    };

    let scan_error = scan(&engine, "events", Some(invalid_snapshot.clone()))
        .await
        .expect_err("invalid snapshot should fail");
    assert!(scan_error.to_string().contains("invalid snapshot"));

    let stats_error = handle(&engine, "events")
        .stats(Some(invalid_snapshot))
        .expect_err("invalid snapshot should fail");
    assert!(stats_error.to_string().contains("invalid snapshot"));
}

#[tokio::test]
async fn rejects_snapshots_below_manifest_checkpoint() {
    let root_dir = support::unique_temp_dir("storage-below-checkpoint-snapshot");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created");

    let flushed = handle(&engine, "events")
        .write(vec![put_with(
            "evt-1",
            vec![1.0, 0.0],
            json!({"kind":"keep"}),
        )])
        .await
        .expect("write should succeed");
    assert_eq!(flushed.last_seq_no, 1);

    let snapshot = handle(&engine, "events")
        .flush()
        .await
        .expect("flush should succeed");
    let invalid_snapshot = Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no - 1,
    };

    let scan_error = scan(&engine, "events", Some(invalid_snapshot.clone()))
        .await
        .expect_err("below-checkpoint snapshot should fail");
    assert!(scan_error.to_string().contains("invalid snapshot"));

    let stats_error = handle(&engine, "events")
        .stats(Some(invalid_snapshot))
        .expect_err("below-checkpoint snapshot should fail");
    assert!(stats_error.to_string().contains("invalid snapshot"));
}

#[tokio::test]
async fn rejects_invalid_maintenance_thresholds_in_descriptor() {
    let root_dir = support::unique_temp_dir("storage-invalid-thresholds");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let descriptor = create(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
    )
    .await
    .expect("collection should be created")
    .describe();

    let descriptor_path = descriptor.root_path.join("descriptor.json");
    let mut descriptor_json: Value =
        serde_json::from_slice(&fs::read(&descriptor_path).expect("descriptor should exist"))
            .expect("descriptor JSON should parse");
    descriptor_json["flush_threshold_ops"] = json!(0);
    descriptor_json["flush_threshold_bytes"] = json!(0);
    descriptor_json["compaction_threshold_segments"] = json!(1);
    fs::write(
        &descriptor_path,
        serde_json::to_vec_pretty(&descriptor_json).expect("descriptor JSON should serialize"),
    )
    .expect("descriptor should be rewritten");

    drop(engine);
    let reopened = open(&root);
    let error = describe(&reopened, "events").expect_err("invalid thresholds should be rejected");
    assert!(error.to_string().contains("threshold"));
}

#[tokio::test]
async fn a_pinned_snapshot_remains_readable_after_flush_and_an_unpinned_one_expires() {
    let root_dir = support::unique_temp_dir("storage-snapshot-flush");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    let descriptor = create(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Cosine),
    )
    .await
    .expect("collection should be created")
    .describe();

    handle(&engine, "events")
        .write(vec![put_with(
            "evt-1",
            vec![1.0, 2.0],
            json!({"kind":"login"}),
        )])
        .await
        .expect("write should succeed");

    let unpinned = handle(&engine, "events")
        .snapshot()
        .expect("snapshot should succeed");
    let (token, snapshot) = pin(&engine, "events").expect("pin");
    assert_eq!(snapshot, unpinned);
    handle(&engine, "events")
        .flush()
        .await
        .expect("flush should succeed");

    let visible = scan_at_token(&engine, "events", token.clone())
        .await
        .expect("the pinned snapshot still scans");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "evt-1");

    // Reads never go back to the WAL: the checkpointed file is gone once the flush is durable.
    let wal_dir = descriptor.root_path.join("wal");
    let wal_files = fs::read_dir(wal_dir)
        .expect("wal dir should exist")
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .count();
    assert_eq!(wal_files, 1);

    handle(&engine, "events").release_snapshot(&token);
    let error = scan(&engine, "events", Some(unpinned))
        .await
        .expect_err("nothing pins the old generation");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn duplicate_id_batch_rejects_without_committing_anything() {
    let root_dir = support::unique_temp_dir("storage-duplicate-batch");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("items", 2, DistanceMetric::Cosine),
    )
    .await
    .expect("collection should be created");

    let error = handle(&engine, "items")
        .write(vec![
            put_with("dup", vec![1.0, 0.0], json!({"version":1})),
            put_with("dup", vec![2.0, 0.0], json!({"version":2})),
        ])
        .await
        .expect_err("duplicate batch should fail");
    assert!(error.to_string().contains("more than once"), "{error}");
    assert_eq!(
        error.details().field_violations[0].field,
        "[1]",
        "the second operation repeats the key"
    );

    let visible = scan(&engine, "items", None)
        .await
        .expect("scan should succeed");
    assert!(visible.is_empty(), "invalid batch should commit nothing");
}

#[tokio::test]
async fn dimension_error_batch_rejects_without_committing_anything() {
    let root_dir = support::unique_temp_dir("storage-dimension-batch");
    let root = root_dir.path().to_path_buf();
    let engine = open(&root);

    create(
        &engine,
        CreateCollectionRequest::new("embeddings", 2, DistanceMetric::Cosine),
    )
    .await
    .expect("collection should be created");

    let error = handle(&engine, "embeddings")
        .write(vec![
            put_with("ok", vec![1.0, 1.0], json!({"kind":"valid"})),
            put_with("bad", vec![1.0, 1.0, 1.0], json!({"kind":"invalid"})),
        ])
        .await
        .expect_err("dimension mismatch batch should fail");
    assert!(error.to_string().contains("expected 2 dimensions"));

    let visible = scan(&engine, "embeddings", None)
        .await
        .expect("scan should succeed");
    assert!(visible.is_empty(), "invalid batch should commit nothing");
}

/// Create a collection whose maintenance thresholds are set before it is created.
fn create_with_thresholds(
    engine: &Engine,
    request: CreateCollectionRequest,
    flush_ops: usize,
    flush_bytes: usize,
    compact_segments: usize,
) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
    let mut descriptor = engine.plan_collection_descriptor(&request)?;
    descriptor.flush_threshold_ops = flush_ops;
    descriptor.flush_threshold_bytes = flush_bytes;
    descriptor.compaction_threshold_segments = compact_segments;
    engine
        .create_collection_blocking(descriptor, None)
        .map(|handle| handle.describe())
}

async fn wait_for_condition<F>(engine: &Engine, collection_name: &str, predicate: F)
where
    F: Fn(&logpose_types::CollectionStats) -> bool,
{
    // Generous because a flush of large vectors takes seconds on a loaded machine. Passing
    // tests return as soon as the predicate holds.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats = handle(engine, collection_name)
            .stats(None)
            .expect("stats should succeed");
        if predicate(&stats) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for background maintenance: {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// WAL files of the collection rooted at `root`.
fn wal_file_count(root: &std::path::Path) -> usize {
    fs::read_dir(root.join("wal"))
        .expect("wal dir should exist")
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .count()
}

/// A segment is searchable before its graph lands (its SQ8 codes are scanned and reranked in
/// f32) and after (its graph is walked), with the same top hits: the index build changes how a
/// search runs, never what it returns beyond approximation.
#[test]
fn a_segment_answers_the_same_before_and_after_its_graph_lands() {
    use logpose_storage::{EngineConfig, IndexPolicy, JobKind};

    const DIM: usize = 16;
    const ROWS: u32 = 3_000;
    let root_dir = support::unique_temp_dir("storage-graph-lands");
    let engine = Engine::open_local(
        root_dir.path(),
        EngineConfig {
            index: IndexPolicy {
                graph_min_rows: 256,
                sq8_min_rows: 64,
                ..IndexPolicy::default()
            },
            ..EngineConfig::default()
        },
    )
    .expect("engine should open");
    let mut descriptor = engine
        .plan_collection_descriptor(&CreateCollectionRequest::new(
            "graphs",
            DIM,
            DistanceMetric::L2,
        ))
        .expect("descriptor");
    descriptor.flush_threshold_ops = usize::MAX;
    descriptor.flush_threshold_bytes = usize::MAX;
    descriptor.compaction_threshold_segments = usize::MAX;
    let collection = engine
        .create_collection_blocking(descriptor, None)
        .expect("collection");
    // 30 clusters of 100 rows with a deterministic spread.
    let vector = |row: u32| -> Vec<f32> {
        let cluster = row % 30;
        (0..DIM as u32)
            .map(|dim| {
                let center = ((cluster * 37 + dim * 11) % 29) as f32;
                let noise = ((row * 7_919 + dim * 104_729) % 1_000) as f32 / 1_000.0;
                center + noise
            })
            .collect()
    };
    let rows = (0..ROWS).collect::<Vec<_>>();
    for batch in rows.chunks(500) {
        collection
            .write_blocking(
                batch
                    .iter()
                    .map(|row| db::put(&format!("r{row:05}"), vector(*row)))
                    .collect(),
            )
            .expect("write");
    }
    collection.flush_blocking().expect("flush");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let reference = logpose_types::CollectionRef::parse("graphs").expect("name");
    let search = |query: &[f32]| -> (Vec<String>, Vec<String>) {
        let request = logpose_query::QueryRequest {
            vector: Some(logpose_query::VectorQuery {
                field: None,
                values: query.to_vec(),
            }),
            top_k: 10,
            ef: Some(128),
            explain: logpose_query::ExplainMode::Plan,
            output_fields: vec!["id".to_owned()],
            ..logpose_query::QueryRequest::default()
        };
        let response = runtime
            .block_on(logpose_query::query(&engine, &reference, request))
            .expect("query")
            .value;
        let strategies = response
            .diagnostics
            .map(|diagnostics| diagnostics.unit_scan_mix.into_keys().collect())
            .unwrap_or_default();
        let hits = response
            .hits
            .into_iter()
            .map(|hit| hit.record.pk.label())
            .collect();
        (hits, strategies)
    };
    let exact = |query: &[f32]| -> Vec<String> {
        let mut scored = (0..ROWS)
            .map(|row| {
                let distance = vector(row)
                    .iter()
                    .zip(query)
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum::<f32>();
                (distance, format!("r{row:05}"))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        scored.into_iter().take(10).map(|(_, id)| id).collect()
    };
    let queries = (0..20_u32)
        .map(|index| {
            vector(index * 149 + 7)
                .into_iter()
                .map(|value| value + 0.013)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let recall = |hits: &[Vec<String>]| -> f64 {
        let found = queries
            .iter()
            .zip(hits)
            .map(|(query, hits)| {
                let truth = exact(query);
                hits.iter().filter(|id| truth.contains(id)).count()
            })
            .sum::<usize>();
        found as f64 / (queries.len() * 10) as f64
    };

    let mut before = Vec::new();
    for query in &queries {
        let (hits, strategies) = search(query);
        // The empty active memtable reports `empty`.
        assert_eq!(
            strategies,
            ["empty", "exact_sq8"],
            "no graph yet: the codes are scanned"
        );
        before.push(hits);
    }
    let job = engine
        .begin_job(&collection, JobKind::Index)
        .expect("begin index build");
    assert!(job.has_work());
    job.commit().expect("commit index build");
    let mut after = Vec::new();
    for query in &queries {
        let (hits, strategies) = search(query);
        assert_eq!(
            strategies,
            ["empty", "graph_admit"],
            "the graph landed and is walked"
        );
        after.push(hits);
    }
    let (recall_before, recall_after) = (recall(&before), recall(&after));
    assert!(
        recall_before >= 0.95,
        "recall without the graph {recall_before}"
    );
    assert!(recall_after >= 0.95, "recall with the graph {recall_after}");
    drop(collection);
    drop(engine);
}
