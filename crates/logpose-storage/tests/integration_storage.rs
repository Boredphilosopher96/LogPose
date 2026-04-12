//! Integration tests for `logpose-storage` workflows.

use async_trait as _;
use crc32fast as _;
use logpose_catalog as _;
use logpose_query as _;
use logpose_wal as _;
use rand as _;
use serde as _;
use uuid as _;

#[path = "support/fs.rs"]
mod support;

use logpose_storage::{CreateCollectionRequest, LocalStorageEngine, StorageEngine};
use logpose_types::{DeleteRecord, DistanceMetric, PutRecord, RecordId, WriteOperation};
use serde_json::json;
use std::fs;

#[tokio::test]
async fn create_write_scan_and_delete_records() {
    let root = support::unique_temp_dir("storage-write-scan");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest {
            name: "colors".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Cosine,
        })
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
async fn flush_persists_visible_records_for_reopen() {
    let root = support::unique_temp_dir("storage-flush");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest {
            name: "documents".to_owned(),
            dimensions: 3,
            metric: DistanceMetric::Dot,
        })
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
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 0);
}

#[tokio::test]
async fn compact_merges_segments_and_preserves_latest_versions() {
    let root = support::unique_temp_dir("storage-compact");
    let engine = LocalStorageEngine::new(&root);

    engine
        .create_collection(CreateCollectionRequest {
            name: "profiles".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::L2,
        })
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
    assert_eq!(before.segment_count, 2);

    engine
        .compact("profiles")
        .await
        .expect("compaction should succeed");

    let after = engine
        .stats("profiles")
        .await
        .expect("stats should succeed");
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
async fn old_snapshot_remains_readable_after_flush() {
    let root = support::unique_temp_dir("storage-snapshot-flush");
    let engine = LocalStorageEngine::new(&root);

    let descriptor = engine
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Cosine,
        })
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
        .create_collection(CreateCollectionRequest {
            name: "items".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Cosine,
        })
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
        .create_collection(CreateCollectionRequest {
            name: "embeddings".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Cosine,
        })
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
