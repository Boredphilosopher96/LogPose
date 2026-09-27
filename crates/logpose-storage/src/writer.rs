//! The write path: validate a batch, append it to the WAL as one frame, and publish the next
//! `Version`.
//!
//! Until the writer task and group commit land, the collection's writer mutex stands in for the
//! single writer: one batch at a time is appended, synced, and published, on an I/O thread.

use crate::{
    engine::CoreRef,
    handle::{CollectionHandle, WriterSlot},
    maintenance::{MaintenanceOperation, should_compact, should_flush},
    version::visible_seq_no,
};
use logpose_types::{CommitAck, LogPoseError, RecordId, Result, WriteOperation};
use logpose_wal::{WalBatch, WalRecord};
use std::{collections::BTreeSet, sync::Arc};

impl CoreRef {
    /// Durably commit `operations` as one atomic batch and publish it.
    ///
    /// The acknowledgement is returned only after the batch's WAL frame is synced and the
    /// `Version` that includes it is published, so any read that starts afterwards sees it (I1).
    pub(crate) fn write(
        &self,
        handle: &Arc<CollectionHandle>,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        if operations.is_empty() {
            return Err(LogPoseError::invalid_field(
                "operations",
                "write batch must include at least one operation",
            ));
        }
        handle.ensure_writable()?;
        let descriptor = handle.descriptor();
        let mut seen_ids = BTreeSet::<&RecordId>::new();
        for (index, operation) in operations.iter().enumerate() {
            descriptor
                .validate_operation(operation)
                .map_err(|error| error.with_field_prefix(&format!("operations[{index}]")))?;
            if !seen_ids.insert(operation.id()) {
                return Err(LogPoseError::invalid_field(
                    format!("operations[{index}].id"),
                    format!(
                        "write batch includes duplicate record id '{}'",
                        operation.id()
                    ),
                ));
            }
        }
        drop(seen_ids);

        let mut writer = handle.lock_writer()?;
        handle.ensure_writable()?;
        if writer.wal.is_none() {
            self.reopen_after_failed_append(handle, &mut writer)?;
        }
        let current = handle.current();
        // The whole batch is one WAL frame with one fsync, so replay sees all of it or none.
        let applied_ops = operations.len();
        let batch = WalBatch::new(
            operations
                .into_iter()
                .zip(current.visible_seq_no + 1..)
                .map(|(op, seq_no)| WalRecord { seq_no, op })
                .collect(),
        )?;
        let last_seq_no = batch.last_seq_no();
        let Some(wal) = writer.wal.as_mut() else {
            return Err(handle.unavailable());
        };
        if let Err(error) = wal.append_batch(&batch) {
            writer.wal = None;
            return Err(error);
        }
        let version = handle.publish(&writer, current.with_batch(batch.into_records()));
        drop(writer);

        if should_flush(descriptor, &version) {
            self.enqueue_maintenance(handle, vec![MaintenanceOperation::Flush]);
        } else if should_compact(descriptor, &version) {
            self.enqueue_maintenance(handle, vec![MaintenanceOperation::Compact]);
        }

        Ok(CommitAck {
            last_seq_no,
            applied_ops,
            snapshot: version.snapshot(),
        })
    }

    /// Reopen the active WAL after a failed append closed it.
    ///
    /// The append's rollback can fail too, leaving the failed batch's whole frame in the WAL,
    /// where the next batch, numbered from the published `Version`, would reuse its sequence
    /// numbers. The page cache may also hold that frame without it being on disk, so it must not
    /// be made visible either (I14). So when the WAL holds anything the published `Version` does
    /// not, the collection is poisoned: it serves reads of the last published `Version` and
    /// refuses writes until it is reopened, which replays whatever the WAL durably holds.
    fn reopen_after_failed_append(
        &self,
        handle: &CollectionHandle,
        writer: &mut WriterSlot,
    ) -> Result<()> {
        let descriptor = handle.descriptor();
        // Opening repairs a torn tail the failed append may have left.
        let wal = self.open_active_wal(descriptor)?;
        let (manifest, delta) = self.load_current_state(descriptor)?;
        let published = handle.current().visible_seq_no;
        let on_disk = visible_seq_no(&manifest, &delta);
        if on_disk != published {
            handle.poison(format!(
                "after a failed WAL append the WAL ends at seq {on_disk}, but seq {published} \
                 is published; the failed batch's rollback did not remove it"
            ));
            return Err(handle.unavailable());
        }
        writer.wal = Some(wal);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CreateCollectionRequest, Engine, EngineConfig,
        engine::EngineCore,
        test_support::{put, unique_temp_dir},
    };
    use logpose_types::{CollectionRef, DistanceMetric, SeqNo};
    use logpose_vfs::std_vfs;
    use logpose_wal::{WalBatch, WalRecord, WalWriter};
    use std::collections::BTreeMap;

    fn visible(engine: &Engine) -> BTreeMap<String, SeqNo> {
        let handle = engine
            .collection(&CollectionRef::new_default("documents"))
            .expect("collection should be open");
        handle
            .current()
            .check_invariants()
            .expect("sequence numbers are unique and contiguous");
        engine
            .core()
            .scan_exact_internal(&handle, None, true, None)
            .expect("scan should succeed")
            .into_iter()
            .map(|record| (record.id.as_str().to_owned(), record.seq_no))
            .collect()
    }

    /// After an append whose rollback failed, the failed batch's frame can still be in the WAL.
    /// No later batch may reuse its sequence numbers, and it must not become visible in process.
    #[test]
    fn a_frame_left_by_a_failed_append_is_never_renumbered_over() {
        let root = unique_temp_dir("writer-failed-append");
        let engine =
            Engine::open(std_vfs(), &root, EngineConfig::default()).expect("engine should open");
        let descriptor = engine
            .core()
            .plan_collection_descriptor(&CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .expect("descriptor should plan");
        let handle = engine
            .create_collection(descriptor, None)
            .expect("collection should be created");
        let core = engine.core();
        core.write(&handle, vec![put("alpha", vec![1.0, 0.0])])
            .expect("write should succeed");

        // The state a failed append leaves when its rollback fails too: the writer closed its
        // WAL, and the failed batch's whole frame (seq 2) is still in the file.
        handle.lock_writer().expect("writer slot").wal = None;
        let mut ghost =
            WalWriter::open(std_vfs(), EngineCore::active_wal_path(handle.descriptor()))
                .expect("wal should open");
        ghost
            .append_batch(
                &WalBatch::new(vec![WalRecord {
                    seq_no: 2,
                    op: put("ghost", vec![0.5, 0.5]),
                }])
                .expect("batch should build"),
            )
            .expect("frame should append");
        drop(ghost);

        let error = core
            .write(&handle, vec![put("beta", vec![0.0, 1.0])])
            .expect_err("the next write must not reuse the frame's sequence numbers");
        assert!(
            error.to_string().contains("read-only until it is reopened"),
            "{error}"
        );
        assert_eq!(
            visible(&engine),
            BTreeMap::from([("alpha".to_owned(), 1)]),
            "the frame of the failed batch is not made visible in process"
        );

        // Reopening replays what the WAL holds; numbering continues after it.
        drop((handle, core));
        drop(engine);
        let engine =
            Engine::open(std_vfs(), &root, EngineConfig::default()).expect("engine should reopen");
        let handle = engine
            .collection(&CollectionRef::new_default("documents"))
            .expect("collection should be open");
        let ack = engine
            .core()
            .write(&handle, vec![put("beta", vec![0.0, 1.0])])
            .expect("write should succeed after reopen");
        assert_eq!(ack.last_seq_no, 3);
        let expected = BTreeMap::from([
            ("alpha".to_owned(), 1),
            ("ghost".to_owned(), 2),
            ("beta".to_owned(), 3),
        ]);
        assert_eq!(visible(&engine), expected);
    }

    /// The common failed append, whose rollback succeeded, leaves nothing behind: the next
    /// write reopens the WAL and continues.
    #[test]
    fn a_write_after_a_rolled_back_append_continues() {
        let root = unique_temp_dir("writer-rolled-back-append");
        let engine =
            Engine::open(std_vfs(), &root, EngineConfig::default()).expect("engine should open");
        let descriptor = engine
            .core()
            .plan_collection_descriptor(&CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .expect("descriptor should plan");
        let handle = engine
            .create_collection(descriptor, None)
            .expect("collection should be created");
        let core = engine.core();
        core.write(&handle, vec![put("alpha", vec![1.0, 0.0])])
            .expect("write should succeed");
        handle.lock_writer().expect("writer slot").wal = None;
        let ack = core
            .write(&handle, vec![put("beta", vec![0.0, 1.0])])
            .expect("write should succeed");
        assert_eq!(ack.last_seq_no, 2);
        assert!(!handle.is_poisoned());
    }
}
