//! Integration tests for `logpose-storage` workflows.

use arc_swap as _;
use async_trait as _;
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

#[path = "support/scan.rs"]
mod scan;
#[path = "support/fs.rs"]
mod support;

use scan::ScanExt;

use logpose_auth::{
    AccessTier, AuthenticationMode, DatabaseAccessPolicy, DatabaseRole, DatabaseRoleBinding,
    Principal, PrincipalKind,
};
use logpose_catalog::{CatalogStore, DatabaseDescriptor};
use logpose_storage::{CreateCollectionRequest, InspectTarget, LocalStorageEngine, StorageEngine};
use logpose_types::{
    CorruptionKind, DEFAULT_DATABASE_NAME, DeleteRecord, DistanceMetric, ErrorCode, LogPoseError,
    PutRecord, RecordId, Snapshot, WriteOperation,
};
use serde_json::{Value, json};
use std::{
    fs,
    time::{Duration, Instant},
};

#[tokio::test]
async fn create_write_scan_and_delete_records() {
    let root = support::unique_temp_dir("storage-write-scan");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "colors",
            2,
            DistanceMetric::Cosine,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "colors",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"color":"red"}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: json!({"color":"green"}),
                }),
            ],
        )
        .await
        .expect("writes should succeed");

    let before_delete = engine
        .scan_exact("colors", None)
        .await
        .expect("scan should succeed");
    assert_eq!(before_delete.len(), 2);

    engine
        .write(
            "colors",
            vec![WriteOperation::Delete(DeleteRecord {
                id: RecordId::new("alpha"),
            })],
        )
        .await
        .expect("delete should succeed");

    let after_delete = engine
        .scan_exact("colors", None)
        .await
        .expect("scan should succeed");
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0].id.as_str(), "beta");
}

#[tokio::test]
async fn create_collection_persists_default_database_descriptor() {
    let root = support::unique_temp_dir("storage-default-database");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "colors",
            2,
            DistanceMetric::Cosine,
        ))
        .await
        .expect("collection should be created");

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
    let root = support::unique_temp_dir("storage-catalog-invalid-stored");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
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
    let root = support::unique_temp_dir("storage-catalog-round-trip");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

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
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
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
    let root = support::unique_temp_dir("storage-catalog-database-isolation");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

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
    let root = support::unique_temp_dir("storage-database-idempotence");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

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
    let root = support::unique_temp_dir("storage-namespace-duplicates");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let default_descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("default namespace collection should be created");
    let analytics_descriptor = engine
        .create_collection(CreateCollectionRequest::in_database(
            "analytics",
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("analytics database collection should be created");
    assert_ne!(
        default_descriptor.collection_id,
        analytics_descriptor.collection_id
    );

    let opened_default = engine
        .open_collection("documents")
        .await
        .expect("default namespace lookup should work");
    let opened_analytics = engine
        .open_collection("analytics/documents")
        .await
        .expect("database-qualified lookup should work");

    assert_eq!(opened_default.database_name, "default");
    assert_eq!(opened_analytics.database_name, "analytics");

    let explicit_analytics = engine
        .open_collection_in_database("analytics", "documents")
        .await
        .expect("explicit database lookup should work");
    assert_eq!(
        explicit_analytics.collection_id,
        analytics_descriptor.collection_id
    );
}

#[tokio::test]
async fn create_collection_allows_duplicate_names_in_distinct_databases() {
    let root = support::unique_temp_dir("storage-duplicate-collection-namespaces");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let left = engine
        .create_collection(CreateCollectionRequest::in_database(
            DEFAULT_DATABASE_NAME,
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("first collection should be created");

    let right = engine
        .create_collection(CreateCollectionRequest::in_database(
            "analytics",
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("second collection in another database should be created");

    assert_eq!(left.name, "events");
    assert_eq!(right.name, "events");
    assert_ne!(left.collection_id, right.collection_id);
    assert_eq!(left.database_name, DEFAULT_DATABASE_NAME);
    assert_eq!(right.database_name, "analytics");

    let descriptors = engine
        .list_collections()
        .await
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
    let root = support::unique_temp_dir("storage-reserved-separator");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let error = engine
        .create_collection(CreateCollectionRequest::in_database(
            "analytics",
            "docs/v2",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect_err("slash-containing collection names should fail");

    assert!(error.to_string().contains("collection_name"));
    assert!(error.to_string().contains("/"));
}

#[tokio::test]
async fn open_collection_resolves_database_collection_tuple() {
    let root = support::unique_temp_dir("storage-open-collection-namespace");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let default_descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("default namespace collection should be created");
    let analytics_descriptor = engine
        .create_collection(CreateCollectionRequest::in_database(
            "analytics",
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("analytics database collection should be created");

    let default_lookup = engine
        .open_collection("events")
        .await
        .expect("default namespace lookup should succeed");
    assert_eq!(
        default_lookup.collection_id,
        default_descriptor.collection_id
    );

    let explicit_lookup = engine
        .open_collection_in_database("analytics", "events")
        .await
        .expect("explicit database lookup should succeed");
    assert_eq!(
        explicit_lookup.collection_id,
        analytics_descriptor.collection_id
    );
    assert_eq!(explicit_lookup.database_name, "analytics");

    let slash_lookup = engine
        .open_collection("analytics/events")
        .await
        .expect("database-qualified lookup should succeed");
    assert_eq!(
        slash_lookup.collection_id,
        analytics_descriptor.collection_id
    );
}

#[tokio::test]
async fn flush_persists_visible_records_for_reopen() {
    let root = support::unique_temp_dir("storage-flush");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            3,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("doc-1"),
                vector: vec![0.1, 0.2, 0.3],
                metadata: json!({"topic":"intro"}),
            })],
        )
        .await
        .expect("write should succeed");

    engine
        .flush("documents")
        .await
        .expect("flush should succeed");

    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    let visible = reopened
        .scan_exact("documents", None)
        .await
        .expect("scan should succeed");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "doc-1");

    let stats = reopened
        .stats("documents")
        .await
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
    let root = support::unique_temp_dir("storage-reopen-post-checkpoint-delta");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"version":1}),
            })],
        )
        .await
        .expect("first write should succeed");
    engine
        .flush("documents")
        .await
        .expect("flush should succeed");
    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"version":2}),
            })],
        )
        .await
        .expect("second write should succeed");

    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    let visible = reopened
        .scan_exact("documents", None)
        .await
        .expect("scan should succeed after reopen");
    assert_eq!(visible.len(), 2);
    assert_eq!(visible[0].id.as_str(), "alpha");
    assert_eq!(visible[1].id.as_str(), "beta");

    let stats = reopened
        .stats("documents")
        .await
        .expect("stats should succeed after reopen");
    assert_eq!(stats.visible_seq_no, 2);
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 1);
}

#[tokio::test]
async fn checkpointed_rolled_wal_corruption_does_not_block_recovery() {
    let root = support::unique_temp_dir("storage-checkpointed-wal-corruption");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"version":1}),
            })],
        )
        .await
        .expect("write should succeed");
    let flushed = engine
        .flush("documents")
        .await
        .expect("flush should succeed");

    let rolled_wal_path = descriptor
        .root_path
        .join("wal")
        .join(format!("{:020}.wal", flushed.visible_seq_no));
    fs::write(&rolled_wal_path, b"corrupt checkpointed wal")
        .expect("corrupted rolled wal should be written");

    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    let visible = reopened
        .scan_exact("documents", None)
        .await
        .expect("checkpointed wal corruption should be ignored once the manifest covers it");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");

    let stats = reopened
        .stats("documents")
        .await
        .expect("stats should still load after reopen");
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 0);
    assert_eq!(stats.live_record_count, 1);
}

#[tokio::test]
async fn a_pinned_snapshot_reads_exactly_its_state_after_a_flush_until_a_restart() {
    let root = support::unique_temp_dir("storage-old-snapshot-rotated-wal");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"version":1}),
            })],
        )
        .await
        .expect("write should succeed");
    let (token, pre_flush_snapshot) = engine.pin_snapshot("documents").expect("pin");
    engine
        .flush("documents")
        .await
        .expect("flush should succeed");
    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"version":1}),
            })],
        )
        .await
        .expect("write after the flush should succeed");
    assert_eq!(
        wal_file_count(&descriptor.root_path),
        1,
        "the flush rotated the WAL and deleted the checkpointed file"
    );

    let old_snapshot_stats = engine
        .stats_at_token("documents", token.clone())
        .await
        .expect("the pinned state is readable");
    assert_eq!(old_snapshot_stats.live_record_count, 1);
    assert_eq!(old_snapshot_stats.mutable_op_count, 1);
    assert_eq!(
        old_snapshot_stats.manifest_generation,
        pre_flush_snapshot.manifest_generation
    );
    let visible = engine
        .scan_exact("documents", Some(pre_flush_snapshot.clone()))
        .await
        .expect("the exact snapshot a token pins stays readable");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");

    // Pins live in memory: a restart ends them.
    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    let error = reopened
        .scan_exact_at_token("documents", token)
        .await
        .expect_err("tokens do not survive a restart");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
    let error = reopened
        .stats_snapshot("documents", Some(pre_flush_snapshot))
        .await
        .expect_err("an older generation is not retained");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn a_pinned_snapshot_preserves_pre_compaction_history() {
    let root = support::unique_temp_dir("storage-old-snapshot-compaction-history");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 0.0],
                metadata: json!({"version":1}),
            })],
        )
        .await
        .expect("first write should succeed");
    let (token, old_snapshot) = engine.pin_snapshot("documents").expect("pin");
    engine
        .flush("documents")
        .await
        .expect("first flush should succeed");

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![2.0, 0.0],
                metadata: json!({"version":2}),
            })],
        )
        .await
        .expect("second write should succeed");
    engine
        .flush("documents")
        .await
        .expect("second flush should succeed");
    engine
        .compact("documents")
        .await
        .expect("compaction should succeed");

    engine
        .write(
            "documents",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("beta"),
                vector: vec![0.0, 1.0],
                metadata: json!({"version":3}),
            })],
        )
        .await
        .expect("third write should succeed");
    engine
        .flush("documents")
        .await
        .expect("third flush should succeed");

    let old_snapshot_stats = engine
        .stats_at_token("documents", token.clone())
        .await
        .expect("pinned stats stay readable after compaction");
    assert_eq!(
        old_snapshot_stats.manifest_generation,
        old_snapshot.manifest_generation
    );
    assert_eq!(old_snapshot_stats.live_record_count, 1);
    assert_eq!(old_snapshot_stats.mutable_op_count, 1);
    assert_eq!(old_snapshot_stats.segment_count, 0);

    let visible = engine
        .scan_exact_at_token("documents", token.clone())
        .await
        .expect("the pinned state keeps the pre-compaction record");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");
    assert_eq!(visible[0].metadata["version"], json!(1));

    assert!(
        engine
            .release_snapshot("documents", &token)
            .expect("release")
    );
    let error = engine
        .scan_exact("documents", Some(old_snapshot))
        .await
        .expect_err("released");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn compact_merges_segments_and_preserves_latest_versions() {
    let root = support::unique_temp_dir("storage-compact");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "profiles",
            2,
            DistanceMetric::L2,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "profiles",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("alpha"),
                vector: vec![1.0, 1.0],
                metadata: json!({"version":1}),
            })],
        )
        .await
        .expect("write should succeed");
    engine
        .flush("profiles")
        .await
        .expect("flush should succeed");

    engine
        .write(
            "profiles",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![2.0, 2.0],
                    metadata: json!({"version":2}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![3.0, 3.0],
                    metadata: json!({"version":1}),
                }),
            ],
        )
        .await
        .expect("write should succeed");
    engine
        .flush("profiles")
        .await
        .expect("flush should succeed");

    let before = engine
        .stats("profiles")
        .await
        .expect("stats should succeed");
    assert_eq!(before.live_record_count, 2);
    assert_eq!(
        before.deleted_record_count, 1,
        "the first segment's alpha row is deleted by the upsert"
    );
    assert_eq!(before.segment_count, 2);

    engine
        .compact("profiles")
        .await
        .expect("compaction should succeed");

    let after = engine
        .stats("profiles")
        .await
        .expect("stats should succeed");
    assert_eq!(after.live_record_count, 2);
    assert_eq!(
        after.deleted_record_count, 0,
        "compaction drops the deleted row"
    );
    assert_eq!(after.segment_count, 1);

    let visible = engine
        .scan_exact("profiles", None)
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
    let root = support::unique_temp_dir("storage-inspect");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Cosine,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"version":1}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![0.0, 1.0],
                    metadata: json!({"version":1}),
                }),
            ],
        )
        .await
        .expect("write should succeed");
    engine
        .flush("documents")
        .await
        .expect("flush should succeed");
    engine
        .write(
            "documents",
            vec![WriteOperation::Delete(DeleteRecord {
                id: RecordId::new("alpha"),
            })],
        )
        .await
        .expect("delete should succeed");

    let manifest = engine
        .inspect("documents", InspectTarget::Manifest)
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

    let wal = engine
        .inspect("documents", InspectTarget::Wal)
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

    let segment = engine
        .inspect("documents", InspectTarget::Segment(segment_id.clone()))
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
            .map(|record| (record["id"].as_str(), record["deleted"].as_bool()))
            .collect::<Vec<_>>(),
        [(Some("alpha"), Some(true)), (Some("beta"), Some(false))]
    );

    let maintenance = engine
        .inspect("documents", InspectTarget::Maintenance)
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

    let stats = engine
        .stats("documents")
        .await
        .expect("stats should succeed");
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(stats.deleted_record_count, 1);
    assert_eq!(stats.mutable_op_count, 1);
    assert_eq!(stats.segment_count, 1);
}

#[tokio::test]
async fn background_maintenance_flushes_and_compacts_using_thresholds() {
    let root = support::unique_temp_dir("storage-background-maintenance");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    create_with_thresholds(
        &engine,
        CreateCollectionRequest::new("events", 2, DistanceMetric::Dot),
        1,
        1024,
        2,
    )
    .expect("collection should be created");

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep","shard":1}),
            })],
        )
        .await
        .expect("first write should succeed");

    wait_for_condition(&engine, "events", |stats| {
        stats.segment_count == 1 && stats.mutable_op_count == 0
    })
    .await;

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-2"),
                vector: vec![2.0, 0.0],
                metadata: json!({"kind":"keep","shard":2}),
            })],
        )
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

    let stats = engine.stats("events").await.expect("stats should succeed");
    assert_eq!(stats.live_record_count, 2);
    assert_eq!(stats.deleted_record_count, 0);
    assert_eq!(stats.segment_count, 1);
    assert!(stats.maintenance.pending.is_empty());
    assert!(stats.maintenance.in_progress.is_none());
    assert_eq!(stats.maintenance.last_error, None);
}

#[tokio::test]
async fn background_maintenance_preserves_namespace_for_duplicate_collection_names() {
    let root = support::unique_temp_dir("storage-background-maintenance-namespace");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
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

    engine
        .write(
            "analytics/events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0, 0.0],
                metadata: json!({"namespace":"analytics","shard":1}),
            })],
        )
        .await
        .expect("database write should succeed");

    wait_for_condition(&engine, "analytics/events", |stats| {
        stats.segment_count == 1 && stats.mutable_op_count == 0
    })
    .await;

    let default_stats = engine
        .stats("events")
        .await
        .expect("default namespace stats should succeed");
    assert_eq!(default_stats.segment_count, 0);
    assert_eq!(default_stats.mutable_op_count, 0);

    let analytics_stats = engine
        .stats("analytics/events")
        .await
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
    let root = support::unique_temp_dir("storage-segment-corruption");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");
    engine
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![2.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                }),
            ],
        )
        .await
        .expect("write should succeed");
    engine
        .flush("documents")
        .await
        .expect("flush should succeed");
    let segment_id = engine
        .inspect("documents", InspectTarget::Manifest)
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

    let engine = LocalStorageEngine::new(&root).expect("storage engine should reopen");
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
    let error = engine
        .scan_exact("documents", None)
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
    let root = support::unique_temp_dir("storage-ann-live-rows");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");
    engine
        .write(
            "documents",
            (1..=12)
                .map(|index| {
                    WriteOperation::Put(PutRecord {
                        id: RecordId::new(format!("doc-{index:02}")),
                        vector: vec![index as f32, 0.0],
                        metadata: json!({
                            "kind": if index % 3 == 0 { "drop" } else { "keep" }
                        }),
                    })
                })
                .collect(),
        )
        .await
        .expect("write should succeed");
    engine
        .flush("documents")
        .await
        .expect("flush should succeed");
    engine
        .write(
            "documents",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("doc-12"),
                    vector: vec![0.5, 0.0],
                    metadata: json!({"kind":"keep"}),
                }),
                WriteOperation::Delete(DeleteRecord {
                    id: RecordId::new("doc-11"),
                }),
            ],
        )
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
        engine
            .flush("documents")
            .await
            .expect("flush should succeed");
    }
}

#[tokio::test]
async fn manual_flush_and_background_maintenance_do_not_race() {
    let root = support::unique_temp_dir("storage-manual-background-race");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

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
            writer
                .write(
                    "events",
                    vec![WriteOperation::Put(PutRecord {
                        id: RecordId::new(format!("evt-{index}")),
                        vector: vec![index as f32, 0.0],
                        metadata: json!({"kind":"keep","version":index}),
                    })],
                )
                .await?;
        }
        logpose_types::Result::<()>::Ok(())
    });
    let manual_task = tokio::spawn(async move {
        for _ in 0..12 {
            let _ = manual.flush("events").await?;
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

    let stats = engine.stats("events").await.expect("stats should succeed");
    assert_eq!(stats.maintenance.last_error, None);
}

#[tokio::test]
async fn background_maintenance_handles_inflight_writes_without_losing_visibility() {
    let root = support::unique_temp_dir("storage-follow-up-background-flush");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    create_with_thresholds(
        &engine,
        CreateCollectionRequest::new("events", 65_536, DistanceMetric::Dot),
        1,
        usize::MAX,
        99,
    )
    .expect("collection should be created");

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0; 65_536],
                metadata: json!({"kind":"keep","version":1}),
            })],
        )
        .await
        .expect("first write should succeed");

    // Under load the first flush can start and finish between two polls, so a
    // completed run also counts; waiting for `in_progress` alone can miss it.
    wait_for_condition(&engine, "events", |stats| {
        stats.maintenance.in_progress.as_deref() == Some("flush")
            || stats.maintenance.completed_runs >= 1
    })
    .await;

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-2"),
                vector: vec![2.0; 65_536],
                metadata: json!({"kind":"keep","version":2}),
            })],
        )
        .await
        .expect("second write should succeed");

    wait_for_condition(&engine, "events", |stats| {
        stats.mutable_op_count == 0
            && stats.segment_count >= 1
            && stats.maintenance.pending.is_empty()
            && stats.maintenance.in_progress.is_none()
    })
    .await;

    let stats = engine.stats("events").await.expect("stats should succeed");
    assert_eq!(stats.live_record_count, 2);
    assert_eq!(stats.mutable_op_count, 0);
    assert_eq!(stats.maintenance.last_error, None);
}

/// Maintenance status is runtime state: nothing about it is persisted. A reopened collection
/// plans whatever its recovered state is due (here, a replayed memtable over a flush trigger
/// lowered while the engine was down) once it has a data-plane access.
#[tokio::test]
async fn reopening_plans_the_maintenance_the_recovered_state_is_due() {
    let root = support::unique_temp_dir("storage-resume-background-maintenance");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
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

    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    let stats = reopened
        .stats("events")
        .await
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
    let root = support::unique_temp_dir("storage-invalid-snapshot");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("write should succeed");

    let invalid_snapshot = Snapshot {
        manifest_generation: 0,
        visible_seq_no: 99,
    };

    let scan_error = engine
        .scan_exact("events", Some(invalid_snapshot.clone()))
        .await
        .expect_err("invalid snapshot should fail");
    assert!(scan_error.to_string().contains("invalid snapshot"));

    let stats_error = engine
        .stats_snapshot("events", Some(invalid_snapshot))
        .await
        .expect_err("invalid snapshot should fail");
    assert!(stats_error.to_string().contains("invalid snapshot"));
}

#[tokio::test]
async fn rejects_snapshots_below_manifest_checkpoint() {
    let root = support::unique_temp_dir("storage-below-checkpoint-snapshot");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    let flushed = engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            })],
        )
        .await
        .expect("write should succeed");
    assert_eq!(flushed.last_seq_no, 1);

    let snapshot = engine.flush("events").await.expect("flush should succeed");
    let invalid_snapshot = Snapshot {
        manifest_generation: snapshot.manifest_generation,
        visible_seq_no: snapshot.visible_seq_no - 1,
    };

    let scan_error = engine
        .scan_exact("events", Some(invalid_snapshot.clone()))
        .await
        .expect_err("below-checkpoint snapshot should fail");
    assert!(scan_error.to_string().contains("invalid snapshot"));

    let stats_error = engine
        .stats_snapshot("events", Some(invalid_snapshot))
        .await
        .expect_err("below-checkpoint snapshot should fail");
    assert!(stats_error.to_string().contains("invalid snapshot"));
}

#[tokio::test]
async fn rejects_invalid_maintenance_thresholds_in_descriptor() {
    let root = support::unique_temp_dir("storage-invalid-thresholds");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

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
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    let error = reopened
        .open_collection("events")
        .await
        .expect_err("invalid thresholds should be rejected");
    assert!(error.to_string().contains("threshold"));
}

#[tokio::test]
async fn a_pinned_snapshot_remains_readable_after_flush_and_an_unpinned_one_expires() {
    let root = support::unique_temp_dir("storage-snapshot-flush");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Cosine,
        ))
        .await
        .expect("collection should be created");

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0, 2.0],
                metadata: json!({"kind":"login"}),
            })],
        )
        .await
        .expect("write should succeed");

    let unpinned = engine
        .snapshot("events")
        .await
        .expect("snapshot should succeed");
    let (token, snapshot) = engine.pin_snapshot("events").expect("pin");
    assert_eq!(snapshot, unpinned);
    engine.flush("events").await.expect("flush should succeed");

    let visible = engine
        .scan_exact_at_token("events", token.clone())
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

    engine.release_snapshot("events", &token).expect("release");
    let error = engine
        .scan_exact("events", Some(unpinned))
        .await
        .expect_err("nothing pins the old generation");
    assert!(
        matches!(error, LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn duplicate_id_batch_rejects_without_committing_anything() {
    let root = support::unique_temp_dir("storage-duplicate-batch");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "items",
            2,
            DistanceMetric::Cosine,
        ))
        .await
        .expect("collection should be created");

    let error = engine
        .write(
            "items",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("dup"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"version":1}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("dup"),
                    vector: vec![2.0, 0.0],
                    metadata: json!({"version":2}),
                }),
            ],
        )
        .await
        .expect_err("duplicate batch should fail");
    assert!(error.to_string().contains("more than once"), "{error}");
    assert_eq!(
        error.details().field_violations[0].field,
        "operations[1]",
        "the second operation repeats the key"
    );

    let visible = engine
        .scan_exact("items", None)
        .await
        .expect("scan should succeed");
    assert!(visible.is_empty(), "invalid batch should commit nothing");
}

#[tokio::test]
async fn dimension_error_batch_rejects_without_committing_anything() {
    let root = support::unique_temp_dir("storage-dimension-batch");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");

    engine
        .create_collection(CreateCollectionRequest::new(
            "embeddings",
            2,
            DistanceMetric::Cosine,
        ))
        .await
        .expect("collection should be created");

    let error = engine
        .write(
            "embeddings",
            vec![
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("ok"),
                    vector: vec![1.0, 1.0],
                    metadata: json!({"kind":"valid"}),
                }),
                WriteOperation::Put(PutRecord {
                    id: RecordId::new("bad"),
                    vector: vec![1.0, 1.0, 1.0],
                    metadata: json!({"kind":"invalid"}),
                }),
            ],
        )
        .await
        .expect_err("dimension mismatch batch should fail");
    assert!(error.to_string().contains("expected 2 dimensions"));

    let visible = engine
        .scan_exact("embeddings", None)
        .await
        .expect("scan should succeed");
    assert!(visible.is_empty(), "invalid batch should commit nothing");
}

/// Create a collection whose maintenance thresholds are set before it is created.
fn create_with_thresholds(
    engine: &LocalStorageEngine,
    request: CreateCollectionRequest,
    flush_ops: usize,
    flush_bytes: usize,
    compact_segments: usize,
) -> logpose_types::Result<logpose_catalog::CollectionDescriptor> {
    let mut descriptor = engine.plan_collection_descriptor(&request)?;
    descriptor.flush_threshold_ops = flush_ops;
    descriptor.flush_threshold_bytes = flush_bytes;
    descriptor.compaction_threshold_segments = compact_segments;
    engine.create_collection_from_descriptor(descriptor, None)
}

async fn wait_for_condition<F>(engine: &LocalStorageEngine, collection_name: &str, predicate: F)
where
    F: Fn(&logpose_types::CollectionStats) -> bool,
{
    // Generous because each poll replays the WAL and can block behind a flush,
    // which takes seconds for large vectors on a loaded machine. Passing tests
    // return as soon as the predicate holds.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats = engine
            .stats(collection_name)
            .await
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
