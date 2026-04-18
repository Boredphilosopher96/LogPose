//! Integration tests for `logpose-storage` workflows.

use async_trait as _;
use crc32fast as _;
use logpose_catalog as _;
use logpose_index as _;
use logpose_query as _;
use logpose_wal as _;
use rand as _;
use serde as _;
use uuid as _;

#[path = "support/fs.rs"]
mod support;

use logpose_storage::{CreateCollectionRequest, InspectTarget, LocalStorageEngine, StorageEngine};
use logpose_types::{
    DEFAULT_DATABASE_NAME, DEFAULT_TENANT_NAME, DeleteRecord, DistanceMetric, PutRecord, RecordId,
    Snapshot, WriteOperation,
};
use serde_json::{Value, json};
use std::{
    fs,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
async fn create_write_scan_and_delete_records() {
    let root = support::unique_temp_dir("storage-write-scan");
    let engine = LocalStorageEngine::new(&root);

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
async fn create_collection_persists_default_tenant_and_database_descriptors() {
    let root = support::unique_temp_dir("storage-default-database");
    let engine = LocalStorageEngine::new(&root);

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "colors",
            2,
            DistanceMetric::Cosine,
        ))
        .await
        .expect("collection should be created");

    let tenant_descriptor_path = root.join("tenants").join("default").join("descriptor.json");
    let tenant_descriptor: logpose_catalog::TenantDescriptor = serde_json::from_slice(
        &fs::read(&tenant_descriptor_path).expect("tenant descriptor should exist"),
    )
    .expect("tenant descriptor JSON should parse");

    let database_descriptor_path = root
        .join("tenants")
        .join(DEFAULT_TENANT_NAME)
        .join("databases")
        .join(DEFAULT_DATABASE_NAME)
        .join("descriptor.json");
    let database_descriptor: logpose_catalog::DatabaseDescriptor = serde_json::from_slice(
        &fs::read(&database_descriptor_path).expect("database descriptor should exist"),
    )
    .expect("database descriptor JSON should parse");

    assert_eq!(tenant_descriptor.name, DEFAULT_TENANT_NAME);
    assert!(tenant_descriptor.is_default);
    assert_eq!(database_descriptor.tenant_name, DEFAULT_TENANT_NAME);
    assert_eq!(descriptor.database_name, DEFAULT_DATABASE_NAME);
    assert_eq!(database_descriptor.name, DEFAULT_DATABASE_NAME);
    assert!(database_descriptor.is_default);
}

#[tokio::test]
async fn duplicate_collection_names_can_exist_in_different_namespaces() {
    let root = support::unique_temp_dir("storage-namespace-duplicates");
    let engine = LocalStorageEngine::new(&root);

    let default_descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("default namespace collection should be created");
    let tenant_descriptor = engine
        .create_collection(CreateCollectionRequest::in_namespace(
            "tenant-a",
            "analytics",
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("tenant namespace collection should be created");

    assert_eq!(default_descriptor.tenant_name, "default");
    assert_eq!(tenant_descriptor.tenant_name, "tenant-a");
    assert_ne!(
        default_descriptor.collection_id,
        tenant_descriptor.collection_id
    );

    let opened_default = engine
        .open_collection("documents")
        .await
        .expect("default namespace lookup should work");
    let opened_tenant = engine
        .open_collection("tenant-a/analytics/documents")
        .await
        .expect("qualified namespace lookup should work");

    assert_eq!(opened_default.tenant_name, "default");
    assert_eq!(opened_default.database_name, "default");
    assert_eq!(opened_tenant.tenant_name, "tenant-a");
    assert_eq!(opened_tenant.database_name, "analytics");

    let explicit_tenant = engine
        .open_collection_in_namespace("tenant-a", "analytics", "documents")
        .await
        .expect("explicit namespace lookup should work");
    assert_eq!(
        explicit_tenant.collection_id,
        tenant_descriptor.collection_id
    );
}

#[tokio::test]
async fn create_collection_allows_duplicate_names_in_distinct_namespaces() {
    let root = support::unique_temp_dir("storage-duplicate-collection-namespaces");
    let engine = LocalStorageEngine::new(&root);

    let left = engine
        .create_collection(CreateCollectionRequest::in_namespace(
            "tenant-a",
            DEFAULT_DATABASE_NAME,
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("first collection should be created");

    let right = engine
        .create_collection(CreateCollectionRequest::in_namespace(
            "tenant-b",
            DEFAULT_DATABASE_NAME,
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("second collection in another tenant should be created");

    assert_eq!(left.name, "events");
    assert_eq!(right.name, "events");
    assert_ne!(left.collection_id, right.collection_id);
    assert_eq!(left.tenant_name, "tenant-a");
    assert_eq!(right.tenant_name, "tenant-b");

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
    let engine = LocalStorageEngine::new(&root);

    let error = engine
        .create_collection(CreateCollectionRequest::in_namespace(
            "tenant-a",
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
async fn open_collection_resolves_namespace_tuple() {
    let root = support::unique_temp_dir("storage-open-collection-namespace");
    let engine = LocalStorageEngine::new(&root);

    let default_descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("default namespace collection should be created");
    let tenant_descriptor = engine
        .create_collection(CreateCollectionRequest::in_namespace(
            "tenant-b",
            DEFAULT_DATABASE_NAME,
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("tenant namespace collection should be created");

    let default_lookup = engine
        .open_collection("events")
        .await
        .expect("default namespace lookup should succeed");
    assert_eq!(
        default_lookup.collection_id,
        default_descriptor.collection_id
    );
    assert_eq!(default_lookup.tenant_name, DEFAULT_TENANT_NAME);

    let explicit_lookup = engine
        .open_collection_in_namespace("tenant-b", DEFAULT_DATABASE_NAME, "events")
        .await
        .expect("explicit namespace lookup should succeed");
    assert_eq!(
        explicit_lookup.collection_id,
        tenant_descriptor.collection_id
    );
    assert_eq!(explicit_lookup.tenant_name, "tenant-b");

    let slash_lookup = engine
        .open_collection("tenant-b/default/events")
        .await
        .expect("slash-qualified lookup should succeed");
    assert_eq!(slash_lookup.collection_id, tenant_descriptor.collection_id);
}

#[tokio::test]
async fn flush_persists_visible_records_for_reopen() {
    let root = support::unique_temp_dir("storage-flush");
    let engine = LocalStorageEngine::new(&root);

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

    let reopened = LocalStorageEngine::new(&root);
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
    assert_eq!(immutable.scalar_fields["topic"].present_count, 1);
}

#[tokio::test]
async fn reopen_after_flush_and_new_write_only_replays_the_post_checkpoint_delta() {
    let root = support::unique_temp_dir("storage-reopen-post-checkpoint-delta");
    let engine = LocalStorageEngine::new(&root);

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

    let reopened = LocalStorageEngine::new(&root);
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
    let engine = LocalStorageEngine::new(&root);

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

    let reopened = LocalStorageEngine::new(&root);
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
async fn checkpointed_frames_left_in_active_wal_do_not_reenter_the_delta() {
    let root = support::unique_temp_dir("storage-active-wal-crash-window");
    let engine = LocalStorageEngine::new(&root);

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
    let rolled_bytes = fs::read(&rolled_wal_path).expect("rolled wal should exist");
    fs::write(
        descriptor.root_path.join("wal").join("active.wal"),
        rolled_bytes,
    )
    .expect("active wal should be repopulated");

    let reopened = LocalStorageEngine::new(&root);
    let stats = reopened
        .stats("documents")
        .await
        .expect("stats should load after reopen");
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(stats.mutable_op_count, 0);
}

#[tokio::test]
async fn corrupted_checkpointed_active_wal_is_ignored_when_rotation_was_pending() {
    let root = support::unique_temp_dir("storage-pending-rotation-active-wal");
    let engine = LocalStorageEngine::new(&root);

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
    let pre_flush_snapshot = engine
        .snapshot("documents")
        .await
        .expect("pre-flush snapshot should succeed");
    let flushed = engine
        .flush("documents")
        .await
        .expect("flush should succeed");

    fs::write(
        descriptor.root_path.join("wal").join("active.wal"),
        b"corrupt checkpointed active wal",
    )
    .expect("corrupted active wal should be written");
    fs::write(
        descriptor.root_path.join("wal").join("PENDING_ROTATION"),
        flushed.visible_seq_no.to_string(),
    )
    .expect("pending rotation marker should be written");

    let reopened = LocalStorageEngine::new(&root);
    let stats = reopened
        .stats("documents")
        .await
        .expect("checkpointed active wal corruption should be ignored when rotation was pending");
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(stats.mutable_op_count, 0);

    let snapshot_stats = reopened
        .stats_snapshot("documents", Some(flushed.clone()))
        .await
        .expect("explicit current-manifest snapshots should also honor pending rotation recovery");
    assert_eq!(snapshot_stats.live_record_count, 1);
    assert_eq!(snapshot_stats.mutable_op_count, 0);

    let visible = reopened
        .scan_exact("documents", Some(flushed))
        .await
        .expect("explicit scans should recover after pending rotation cleanup");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");

    let old_snapshot_stats = reopened
        .stats_snapshot("documents", Some(pre_flush_snapshot.clone()))
        .await
        .expect("older snapshots should also survive pending rotation recovery");
    assert_eq!(
        old_snapshot_stats.manifest_generation,
        pre_flush_snapshot.manifest_generation
    );
    assert_eq!(old_snapshot_stats.live_record_count, 1);
    assert_eq!(old_snapshot_stats.segment_count, 0);
    assert_eq!(old_snapshot_stats.mutable_op_count, 1);

    let old_snapshot_visible = reopened
        .scan_exact("documents", Some(pre_flush_snapshot))
        .await
        .expect("older explicit snapshots should recover after pending rotation cleanup");
    assert_eq!(old_snapshot_visible.len(), 1);
    assert_eq!(old_snapshot_visible[0].id.as_str(), "alpha");
}

#[tokio::test]
async fn older_snapshots_do_not_double_count_rotated_wal_when_rotation_marker_survives() {
    let root = support::unique_temp_dir("storage-pending-rotation-rotated-wal");
    let engine = LocalStorageEngine::new(&root);

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
    let pre_flush_snapshot = engine
        .snapshot("documents")
        .await
        .expect("pre-flush snapshot should succeed");
    let flushed = engine
        .flush("documents")
        .await
        .expect("flush should succeed");

    fs::write(
        descriptor.root_path.join("wal").join("PENDING_ROTATION"),
        flushed.visible_seq_no.to_string(),
    )
    .expect("pending rotation marker should be recreated");

    let reopened = LocalStorageEngine::new(&root);
    let old_snapshot_stats = reopened
        .stats_snapshot("documents", Some(pre_flush_snapshot.clone()))
        .await
        .expect("older snapshots should not double-count rotated wal records");
    assert_eq!(old_snapshot_stats.live_record_count, 1);
    assert_eq!(old_snapshot_stats.mutable_op_count, 1);

    let visible = reopened
        .scan_exact("documents", Some(pre_flush_snapshot))
        .await
        .expect("older snapshots should remain readable");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");
}

#[tokio::test]
async fn older_snapshots_preserve_pre_compaction_history_during_pending_rotation_recovery() {
    let root = support::unique_temp_dir("storage-pending-rotation-compaction-history");
    let engine = LocalStorageEngine::new(&root);

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
        .expect("first write should succeed");
    let old_snapshot = engine
        .snapshot("documents")
        .await
        .expect("old snapshot should be captured before flush");
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
    let flushed = engine
        .flush("documents")
        .await
        .expect("third flush should succeed");

    fs::write(
        descriptor.root_path.join("wal").join("PENDING_ROTATION"),
        flushed.visible_seq_no.to_string(),
    )
    .expect("pending rotation marker should be recreated");

    let reopened = LocalStorageEngine::new(&root);
    let old_snapshot_stats = reopened
        .stats_snapshot("documents", Some(old_snapshot.clone()))
        .await
        .expect("older snapshot stats should remain readable after compaction");
    assert_eq!(
        old_snapshot_stats.manifest_generation,
        old_snapshot.manifest_generation
    );
    assert_eq!(old_snapshot_stats.live_record_count, 1);
    assert_eq!(old_snapshot_stats.mutable_op_count, 1);
    assert_eq!(old_snapshot_stats.segment_count, 0);

    let visible = reopened
        .scan_exact("documents", Some(old_snapshot))
        .await
        .expect("older snapshot should preserve the pre-compaction record state");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "alpha");
    assert_eq!(visible[0].metadata["version"], json!(1));
}

#[cfg(unix)]
#[tokio::test]
async fn recovery_errors_if_pending_rotation_marker_cannot_be_cleared() {
    let root = support::unique_temp_dir("storage-pending-rotation-marker-perms");
    let engine = LocalStorageEngine::new(&root);

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

    fs::write(
        descriptor.root_path.join("wal").join("active.wal"),
        b"corrupt checkpointed active wal",
    )
    .expect("corrupted active wal should be written");
    fs::write(
        descriptor.root_path.join("wal").join("PENDING_ROTATION"),
        flushed.visible_seq_no.to_string(),
    )
    .expect("pending rotation marker should be written");

    let wal_dir = descriptor.root_path.join("wal");
    let original_mode = fs::metadata(&wal_dir)
        .expect("wal dir metadata should exist")
        .permissions()
        .mode();
    fs::set_permissions(&wal_dir, fs::Permissions::from_mode(0o555))
        .expect("wal dir should become read-only");
    let probe_path = wal_dir.join("permission_probe");
    if fs::write(&probe_path, b"probe").is_ok() {
        let _ = fs::remove_file(&probe_path);
        fs::set_permissions(&wal_dir, fs::Permissions::from_mode(original_mode))
            .expect("wal dir permissions should be restored");
        return;
    }

    let reopened = LocalStorageEngine::new(&root);
    let result = reopened.stats("documents").await;

    fs::set_permissions(&wal_dir, fs::Permissions::from_mode(original_mode))
        .expect("wal dir permissions should be restored");

    let error = result.expect_err("recovery should fail when the rotation marker survives");
    assert!(
        error.to_string().contains("pending WAL rotation marker"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn compact_merges_segments_and_preserves_latest_versions() {
    let root = support::unique_temp_dir("storage-compact");
    let engine = LocalStorageEngine::new(&root);

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
    assert_eq!(before.deleted_record_count, 0);
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
    assert_eq!(after.deleted_record_count, 0);
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
    let engine = LocalStorageEngine::new(&root);

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
    assert_eq!(wal_records.len(), 1);

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
        segment
            .payload
            .get("segment")
            .and_then(Value::as_object)
            .and_then(|segment| segment.get("index_kind"))
            .and_then(Value::as_str),
        Some("hnsw")
    );
    assert_eq!(
        segment
            .payload
            .get("artifacts")
            .and_then(Value::as_array)
            .expect("segment artifacts should be an array")
            .iter()
            .filter_map(|artifact| artifact.get("file_name").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        vec![
            format!("{segment_id}.flat.json"),
            format!("{segment_id}.hnsw.bin"),
        ]
    );
    assert_eq!(
        segment
            .payload
            .get("records")
            .and_then(Value::as_array)
            .expect("segment records should be an array")
            .len(),
        2
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
    let engine = LocalStorageEngine::new(&root);

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");
    configure_thresholds(&descriptor.root_path, 1, 1024, 2);

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
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("default namespace collection should be created");
    let tenant_descriptor = engine
        .create_collection(CreateCollectionRequest::in_namespace(
            "tenant-a",
            "analytics",
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("tenant namespace collection should be created");
    configure_thresholds(&tenant_descriptor.root_path, 1, 1024, 2);

    engine
        .write(
            "tenant-a/analytics/events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0, 0.0],
                metadata: json!({"namespace":"tenant-a","shard":1}),
            })],
        )
        .await
        .expect("tenant write should succeed");

    wait_for_condition(&engine, "tenant-a/analytics/events", |stats| {
        stats.segment_count == 1 && stats.mutable_op_count == 0
    })
    .await;

    let default_stats = engine
        .stats("events")
        .await
        .expect("default namespace stats should succeed");
    assert_eq!(default_stats.segment_count, 0);
    assert_eq!(default_stats.mutable_op_count, 0);

    let tenant_stats = engine
        .stats("tenant-a/analytics/events")
        .await
        .expect("tenant namespace stats should succeed");
    assert_eq!(tenant_stats.segment_count, 1);
    assert_eq!(tenant_stats.mutable_op_count, 0);
    assert_eq!(tenant_stats.live_record_count, 1);
}

#[tokio::test]
async fn ann_queries_surface_corrupted_hnsw_sidecars() {
    let root = support::unique_temp_dir("storage-hnsw-corruption");
    let engine = LocalStorageEngine::new(&root);

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

    let manifest = engine
        .inspect("documents", InspectTarget::Manifest)
        .await
        .expect("manifest inspect should succeed");
    let segment_id = manifest
        .payload
        .get("segments")
        .and_then(Value::as_array)
        .and_then(|segments| segments.first())
        .and_then(|segment| segment.get("segment_id"))
        .and_then(Value::as_str)
        .expect("segment id should exist");
    fs::write(
        descriptor
            .root_path
            .join("indexes")
            .join(format!("{segment_id}.hnsw.bin")),
        b"LPH1",
    )
    .expect("corrupted sidecar should be written");

    let error = logpose_query::query_exact(
        &engine,
        logpose_query::QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            filters: Vec::new(),
            predicate: None,
            explain: logpose_query::ExplainMode::None,
        },
    )
    .await
    .expect_err("corrupted hnsw sidecar should fail");
    assert!(
        error.to_string().contains("failed to read hnsw sidecar"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn ann_search_selected_enforces_a_global_candidate_budget() {
    let root = support::unique_temp_dir("storage-ann-budget");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");

    for batch in 0..3 {
        engine
            .write(
                "documents",
                vec![
                    WriteOperation::Put(PutRecord {
                        id: RecordId::new("shared-hot"),
                        vector: vec![12.0 - batch as f32, 0.0],
                        metadata: json!({"kind":"keep"}),
                    }),
                    WriteOperation::Put(PutRecord {
                        id: RecordId::new(format!("doc-{batch}-unique")),
                        vector: vec![9.0 - batch as f32, 0.0],
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
    }

    let immutable_units = engine
        .stats("documents")
        .await
        .expect("stats should succeed")
        .query_units
        .into_iter()
        .filter(|unit| unit.tier == "immutable")
        .map(|unit| unit.unit_id)
        .collect::<Vec<_>>();
    let candidates = engine
        .ann_search_selected(
            "documents",
            None,
            immutable_units,
            logpose_types::AnnSearchRequest {
                vector: vec![1.0, 0.0],
                top_k: 1,
                candidate_budget: 2,
            },
            None,
        )
        .await
        .expect("ann search should succeed");

    assert!(candidates.len() <= 2);
    let record_ids = candidates
        .iter()
        .map(|candidate| candidate.record_id.as_str().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(record_ids.len(), candidates.len());
    assert!(record_ids.contains("shared-hot"));
    assert!(
        record_ids
            .iter()
            .any(|record_id| record_id.ends_with("-unique")),
        "expected a unique immutable candidate alongside the hot id, got {record_ids:?}"
    );
    let shared_hot = candidates
        .iter()
        .find(|candidate| candidate.record_id.as_str() == "shared-hot")
        .expect("shared hot candidate should be present");
    assert_eq!(shared_hot.seq_no, 5);
    assert_eq!(shared_hot.value, 10.0);
    assert!(
        candidates
            .windows(2)
            .all(|pair| pair[0].value >= pair[1].value),
        "candidates should be globally trimmed and sorted by score"
    );
}

#[tokio::test]
async fn manual_flush_and_background_maintenance_do_not_race() {
    let root = support::unique_temp_dir("storage-manual-background-race");
    let engine = LocalStorageEngine::new(&root);

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");
    configure_thresholds(&descriptor.root_path, 1, 1024, 2);

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
    let engine = LocalStorageEngine::new(&root);

    let descriptor = engine
        .create_collection(CreateCollectionRequest::new(
            "events",
            200_000,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created");
    configure_thresholds(&descriptor.root_path, 1, usize::MAX, 99);

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-1"),
                vector: vec![1.0; 200_000],
                metadata: json!({"kind":"keep","version":1}),
            })],
        )
        .await
        .expect("first write should succeed");

    wait_for_condition(&engine, "events", |stats| {
        stats.maintenance.in_progress.as_deref() == Some("flush")
    })
    .await;

    engine
        .write(
            "events",
            vec![WriteOperation::Put(PutRecord {
                id: RecordId::new("evt-2"),
                vector: vec![2.0; 200_000],
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

#[tokio::test]
async fn reopening_resumes_persisted_background_maintenance() {
    let root = support::unique_temp_dir("storage-resume-background-maintenance");
    let engine = LocalStorageEngine::new(&root);

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

    let maintenance_path = descriptor.root_path.join("maintenance.json");
    fs::write(
        &maintenance_path,
        serde_json::to_vec_pretty(&json!({
            "pending": [],
            "in_progress": "flush",
            "last_error": null,
            "completed_runs": 0
        }))
        .expect("status should serialize"),
    )
    .expect("maintenance status should be updated");

    let reopened = LocalStorageEngine::new(&root);
    reopened
        .open_collection("events")
        .await
        .expect("open should succeed");

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
    let engine = LocalStorageEngine::new(&root);

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
    let engine = LocalStorageEngine::new(&root);

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
    let engine = LocalStorageEngine::new(&root);

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

    let reopened = LocalStorageEngine::new(&root);
    let error = reopened
        .open_collection("events")
        .await
        .expect_err("invalid thresholds should be rejected");
    assert!(error.to_string().contains("threshold"));
}

#[tokio::test]
async fn scan_exact_selected_with_empty_immutable_selection_scans_none() {
    let root = support::unique_temp_dir("storage-empty-immutable-selection");
    let engine = LocalStorageEngine::new(&root);

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
    engine.flush("events").await.expect("flush should succeed");

    let visible = engine
        .scan_exact_selected("events", None, false, Vec::new())
        .await
        .expect("selected scan should succeed");
    assert!(
        visible.is_empty(),
        "empty immutable selection should scan no segments"
    );
}

#[tokio::test]
async fn old_snapshot_remains_readable_after_flush() {
    let root = support::unique_temp_dir("storage-snapshot-flush");
    let engine = LocalStorageEngine::new(&root);

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

    let snapshot = engine
        .snapshot("events")
        .await
        .expect("snapshot should succeed");
    engine.flush("events").await.expect("flush should succeed");

    let visible = engine
        .scan_exact("events", Some(snapshot))
        .await
        .expect("old snapshot should still scan");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.as_str(), "evt-1");

    let wal_dir = descriptor.root_path.join("wal");
    let rolled = fs::read_dir(wal_dir)
        .expect("wal dir should exist")
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| {
            path.file_name()
                .map(|name| name != "active.wal")
                .unwrap_or(false)
        })
        .count();
    assert_eq!(rolled, 1);
}

#[tokio::test]
async fn duplicate_id_batch_rejects_without_committing_anything() {
    let root = support::unique_temp_dir("storage-duplicate-batch");
    let engine = LocalStorageEngine::new(&root);

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
    assert!(error.to_string().contains("duplicate"));

    let visible = engine
        .scan_exact("items", None)
        .await
        .expect("scan should succeed");
    assert!(visible.is_empty(), "invalid batch should commit nothing");
}

#[tokio::test]
async fn dimension_error_batch_rejects_without_committing_anything() {
    let root = support::unique_temp_dir("storage-dimension-batch");
    let engine = LocalStorageEngine::new(&root);

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

fn configure_thresholds(
    root_path: &std::path::Path,
    flush_ops: usize,
    flush_bytes: usize,
    compact_segments: usize,
) {
    let descriptor_path = root_path.join("descriptor.json");
    let mut descriptor = serde_json::from_slice::<Value>(
        &fs::read(&descriptor_path).expect("descriptor should exist"),
    )
    .expect("descriptor should parse");
    descriptor["flush_threshold_ops"] = json!(flush_ops);
    descriptor["flush_threshold_bytes"] = json!(flush_bytes);
    descriptor["compaction_threshold_segments"] = json!(compact_segments);
    fs::write(
        &descriptor_path,
        serde_json::to_vec_pretty(&descriptor).expect("descriptor should serialize"),
    )
    .expect("descriptor should be updated");
}

async fn wait_for_condition<F>(engine: &LocalStorageEngine, collection_name: &str, predicate: F)
where
    F: Fn(&logpose_types::CollectionStats) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(5);
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
