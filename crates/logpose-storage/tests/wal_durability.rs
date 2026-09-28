//! Crash-recovery tests for WAL batch atomicity and torn-tail repair.

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

#[path = "support/fs.rs"]
mod support;

use logpose_storage::{CreateCollectionRequest, InspectTarget, LocalStorageEngine, StorageEngine};
use logpose_types::{DeleteRecord, DistanceMetric, PutRecord, RecordId, WriteOperation};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
};

fn put(id: &str) -> WriteOperation {
    WriteOperation::Put(PutRecord {
        id: RecordId::new(id),
        vector: vec![1.0, 0.0],
        metadata: json!({"key": id}),
    })
}

/// Create the collection and return the path of its first WAL file, which receives every write
/// until the first flush rotates the WAL.
async fn create(engine: &LocalStorageEngine) -> PathBuf {
    engine
        .create_collection(CreateCollectionRequest::new(
            "documents",
            2,
            DistanceMetric::Dot,
        ))
        .await
        .expect("collection should be created")
        .root_path
        .join("wal")
        .join(format!("{:020}.wal", 1))
}

async fn visible_ids(engine: &LocalStorageEngine) -> Vec<String> {
    let mut ids = engine
        .scan_exact("documents", None)
        .await
        .expect("scan should succeed")
        .into_iter()
        .map(|record| record.id.as_str().to_owned())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn wal_len(path: &Path) -> usize {
    fs::metadata(path).expect("active wal should exist").len() as usize
}

#[tokio::test]
async fn multi_op_batch_is_committed_as_one_wal_frame_with_contiguous_seq_nos() {
    let root = support::unique_temp_dir("storage-wal-one-frame");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
    let active = create(&engine).await;

    let first = engine
        .write("documents", vec![put("a")])
        .await
        .expect("write should succeed");
    assert_eq!(first.last_seq_no, 1);
    let after_first = wal_len(&active);

    let ack = engine
        .write("documents", vec![put("b"), put("c"), put("d")])
        .await
        .expect("write should succeed");
    assert_eq!(ack.last_seq_no, 4);
    assert_eq!(ack.applied_ops, 3);
    assert_eq!(ack.snapshot.visible_seq_no, 4);

    // Dropping the final byte of the file tears the last frame. If the three operations
    // had been written as three frames, the first two would survive.
    let bytes = fs::read(&active).expect("active wal should exist");
    fs::write(&active, &bytes[..bytes.len() - 1]).expect("tear should succeed");

    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    assert_eq!(visible_ids(&reopened).await, vec!["a"]);
    assert_eq!(
        reopened
            .snapshot("documents")
            .await
            .expect("snapshot should succeed")
            .visible_seq_no,
        1
    );
    assert!(bytes.len() > after_first);
}

#[tokio::test]
async fn batch_torn_at_any_byte_is_invisible_as_a_whole() {
    let root = support::unique_temp_dir("storage-wal-torn-batch");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
    let active = create(&engine).await;

    engine
        .write("documents", vec![put("keep")])
        .await
        .expect("write should succeed");
    let committed = wal_len(&active);
    engine
        .write(
            "documents",
            vec![
                put("x"),
                WriteOperation::Delete(DeleteRecord {
                    id: RecordId::new("keep"),
                }),
                put("y"),
            ],
        )
        .await
        .expect("write should succeed");
    let full = fs::read(&active).expect("active wal should exist");

    drop(engine);

    let step = ((full.len() - committed) / 7).max(1);
    for cut in (committed + 1..full.len()).step_by(step) {
        fs::write(&active, &full[..cut]).expect("tear should succeed");
        let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
        assert_eq!(visible_ids(&reopened).await, vec!["keep"], "cut at {cut}");
        let stats = reopened
            .stats("documents")
            .await
            .expect("stats should succeed");
        assert_eq!(stats.mutable_op_count, 1, "cut at {cut}");
        assert_eq!(stats.visible_seq_no, 1, "cut at {cut}");
    }
}

#[tokio::test]
async fn torn_tail_then_append_then_reopen_keeps_every_acknowledged_write() {
    let root = support::unique_temp_dir("storage-wal-torn-append");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
    let active = create(&engine).await;

    engine
        .write("documents", vec![put("a"), put("b")])
        .await
        .expect("write should succeed");
    let committed = wal_len(&active);
    engine
        .write("documents", vec![put("lost-1"), put("lost-2")])
        .await
        .expect("write should succeed");
    let bytes = fs::read(&active).expect("active wal should exist");
    fs::write(&active, &bytes[..committed + (bytes.len() - committed) / 2])
        .expect("tear should succeed");

    // Readers ignore the torn tail before anything repairs it.
    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    assert_eq!(visible_ids(&reopened).await, vec!["a", "b"]);

    // The torn batch was never acknowledged, so its sequence numbers are reused.
    let ack = reopened
        .write("documents", vec![put("c")])
        .await
        .expect("write after a torn tail should succeed");
    assert_eq!(ack.last_seq_no, 3);
    let ack = reopened
        .write("documents", vec![put("d"), put("e")])
        .await
        .expect("write should succeed");
    assert_eq!(ack.last_seq_no, 5);

    drop(reopened);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    assert_eq!(visible_ids(&reopened).await, vec!["a", "b", "c", "d", "e"]);
    let stats = reopened
        .stats("documents")
        .await
        .expect("stats should succeed");
    assert_eq!(stats.visible_seq_no, 5);
    assert_eq!(stats.mutable_op_count, 5);
}

#[tokio::test]
async fn garbage_tail_is_truncated_before_the_next_write() {
    let root = support::unique_temp_dir("storage-wal-garbage-tail");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
    let active = create(&engine).await;

    engine
        .write("documents", vec![put("a")])
        .await
        .expect("write should succeed");
    let committed = wal_len(&active);
    let mut bytes = fs::read(&active).expect("active wal should exist");
    bytes.extend_from_slice(b"\0\0\0garbage left by a crash");
    fs::write(&active, &bytes).expect("garbage should be written");

    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    assert_eq!(visible_ids(&reopened).await, vec!["a"]);
    reopened
        .write("documents", vec![put("b")])
        .await
        .expect("write should repair the tail and succeed");

    let after = fs::read(&active).expect("active wal should exist");
    assert_eq!(after[..committed], bytes[..committed]);
    assert!(
        !after
            .windows(b"garbage".len())
            .any(|window| window == b"garbage"),
        "the garbage tail must be truncated before appending"
    );
    drop(reopened);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    assert_eq!(visible_ids(&reopened).await, vec!["a", "b"]);
}

#[tokio::test]
async fn multi_op_batches_survive_reopen_flush_and_rotation() {
    let root = support::unique_temp_dir("storage-wal-batch-flush");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
    create(&engine).await;

    engine
        .write("documents", vec![put("a"), put("b"), put("c")])
        .await
        .expect("write should succeed");
    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    assert_eq!(visible_ids(&reopened).await, vec!["a", "b", "c"]);

    let flushed = reopened
        .flush("documents")
        .await
        .expect("flush should succeed");
    assert_eq!(flushed.visible_seq_no, 3);

    let ack = reopened
        .write("documents", vec![put("d"), put("e")])
        .await
        .expect("write should succeed");
    assert_eq!(ack.last_seq_no, 5);

    drop(reopened);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    assert_eq!(visible_ids(&reopened).await, vec!["a", "b", "c", "d", "e"]);
    let stats = reopened
        .stats("documents")
        .await
        .expect("stats should succeed");
    assert_eq!(stats.visible_seq_no, 5);
    assert_eq!(stats.segment_count, 1);
    assert_eq!(stats.mutable_op_count, 2);

    let wal = reopened
        .inspect("documents", InspectTarget::Wal)
        .await
        .expect("inspect should succeed");
    assert_eq!(wal.payload["checkpoint_seq_no"], json!(3));
    let seq_nos = wal.payload["records"]
        .as_array()
        .expect("records should be an array")
        .iter()
        .map(|record| record["seq_no"].clone())
        .collect::<Vec<_>>();
    assert_eq!(seq_nos, vec![json!(4), json!(5)]);

    // The first flush's snapshot is gone after the reopen: tokens do not survive a restart.
    let error = reopened
        .scan_exact(
            "documents",
            Some(logpose_types::Snapshot {
                manifest_generation: flushed.manifest_generation - 1,
                visible_seq_no: 3,
            }),
        )
        .await
        .expect_err("an unpinned older generation is not retained");
    assert!(
        matches!(error, logpose_types::LogPoseError::SnapshotExpired { .. }),
        "{error}"
    );

    // A snapshot pinned before the second flush still reads both batches whole.
    let (token, _) = reopened.pin_snapshot("documents").expect("pin");
    let flushed = reopened
        .flush("documents")
        .await
        .expect("second flush should succeed");
    assert_eq!(flushed.visible_seq_no, 5);
    let old = reopened
        .scan_exact_at_token("documents", token)
        .await
        .expect("the pinned snapshot should scan");
    assert_eq!(old.len(), 5);
    drop(reopened);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    let stats = reopened
        .stats("documents")
        .await
        .expect("stats should succeed");
    assert_eq!(stats.mutable_op_count, 0);
    assert_eq!(stats.live_record_count, 5);
    let ack = reopened
        .write("documents", vec![put("f")])
        .await
        .expect("write should succeed");
    assert_eq!(ack.last_seq_no, 6);
}

#[tokio::test]
async fn a_flush_after_a_torn_tail_rotates_then_deletes_the_repaired_file() {
    let root = support::unique_temp_dir("storage-wal-torn-flush");
    let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
    let active = create(&engine).await;

    engine
        .write("documents", vec![put("a"), put("b")])
        .await
        .expect("write should succeed");
    let mut bytes = fs::read(&active).expect("active wal should exist");
    bytes.extend_from_slice(b"torn");
    fs::write(&active, &bytes).expect("garbage should be written");

    drop(engine);
    let reopened = LocalStorageEngine::new(&root).expect("storage engine should open");
    // Recovery repaired the tail: the file keeps exactly the committed groups.
    let repaired = fs::read(&active).expect("the active wal should exist");
    assert_eq!(repaired, bytes[..bytes.len() - b"torn".len()]);
    let (token, _) = reopened.pin_snapshot("documents").expect("pin");

    let flushed = reopened
        .flush("documents")
        .await
        .expect("flush should succeed");
    assert_eq!(flushed.visible_seq_no, 2);

    // The flush rotated to a new file named for the next sequence number, and deleted the old
    // one once the manifest that checkpoints it was durable.
    let rotated = active.with_file_name(format!("{:020}.wal", flushed.visible_seq_no + 1));
    assert!(rotated.exists(), "the flush rotated the WAL");
    assert!(!active.exists(), "the checkpointed file is deleted");

    // The pre-flush state is read from the pinned version, never from the WAL.
    let old = reopened
        .scan_exact_at_token("documents", token)
        .await
        .expect("the pinned snapshot should scan");
    assert_eq!(old.len(), 2);
}
