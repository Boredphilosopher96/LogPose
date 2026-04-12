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
use logpose_types::{DeleteRecord, DistanceMetric, PutRecord, RecordId, Snapshot, WriteOperation};
use serde_json::{Value, json};
use std::{
    fs,
    time::{Duration, Instant},
};

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
    assert_eq!(stats.manifest_generation, 1);
    assert_eq!(stats.visible_seq_no, 1);
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 0);
    assert_eq!(stats.live_record_count, 1);
    assert_eq!(stats.deleted_record_count, 0);
    assert_eq!(stats.query_units.len(), 2);
    assert!(stats.maintenance.pending.is_empty());
    assert!(stats.maintenance.in_progress.is_none());
    assert_eq!(stats.maintenance.last_error, None);

    let immutable = stats
        .query_units
        .iter()
        .find(|unit| unit.tier == "immutable")
        .expect("immutable unit should be reported");
    assert_eq!(immutable.index_kind, "flat");
    assert!(immutable.index_file_name.ends_with(".flat.json"));
    assert_eq!(immutable.scalar_fields["topic"].present_count, 1);
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
        .create_collection(CreateCollectionRequest {
            name: "documents".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Cosine,
        })
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
            .get("index")
            .and_then(Value::as_object)
            .and_then(|index| index.get("index_kind"))
            .and_then(Value::as_str),
        Some("flat")
    );
    assert_eq!(
        segment
            .payload
            .get("index")
            .and_then(Value::as_object)
            .and_then(|index| index.get("vector_norms"))
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(2)
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
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
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
async fn manual_flush_and_background_maintenance_do_not_race() {
    let root = support::unique_temp_dir("storage-manual-background-race");
    let engine = LocalStorageEngine::new(&root);

    let descriptor = engine
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
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
async fn background_maintenance_requeues_follow_up_flushes_after_inflight_writes() {
    let root = support::unique_temp_dir("storage-follow-up-background-flush");
    let engine = LocalStorageEngine::new(&root);

    let descriptor = engine
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 200_000,
            metric: DistanceMetric::Dot,
        })
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
    assert!(stats.maintenance.completed_runs >= 2);
}

#[tokio::test]
async fn reopening_resumes_persisted_background_maintenance() {
    let root = support::unique_temp_dir("storage-resume-background-maintenance");
    let engine = LocalStorageEngine::new(&root);

    let descriptor = engine
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
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
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
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
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
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
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
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
        .create_collection(CreateCollectionRequest {
            name: "events".to_owned(),
            dimensions: 2,
            metric: DistanceMetric::Dot,
        })
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
