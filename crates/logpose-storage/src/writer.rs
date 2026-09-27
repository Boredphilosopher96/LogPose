//! The write path: validate a batch, append it to the WAL as one frame, and publish the next
//! `Version`.
//!
//! Until the writer task and group commit land, the collection's writer mutex stands in for the
//! single writer: one batch at a time is appended, synced, and published, on an I/O thread.

use crate::{
    engine::CoreRef,
    handle::CollectionHandle,
    maintenance::{MaintenanceOperation, should_compact, should_flush},
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
            return Err(LogPoseError::Message(
                "write batch must include at least one operation".to_owned(),
            ));
        }
        handle.ensure_writable()?;
        let descriptor = handle.descriptor();
        let mut seen_ids = BTreeSet::<&RecordId>::new();
        for operation in &operations {
            descriptor.validate_operation(operation)?;
            if !seen_ids.insert(operation.id()) {
                return Err(LogPoseError::Message(format!(
                    "write batch includes duplicate record id '{}'",
                    operation.id()
                )));
            }
        }
        drop(seen_ids);

        let mut writer = handle.lock_writer()?;
        handle.ensure_writable()?;
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
        let wal = match &mut writer.wal {
            Some(wal) => wal,
            // A failed append closed the WAL; reopening repairs a torn tail it may have left.
            slot @ None => slot.insert(self.open_active_wal(descriptor)?),
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
}
