//! The one `apply` function shared by the live writer and WAL replay, and the writer's private
//! state it advances.
//!
//! Both paths feed it the same thing: the decoded content of one WAL data frame and the frame's
//! first sequence number. The writer calls it while preparing a group (before the group's fsync;
//! nothing it produces is published until the fsync returns), and recovery calls it for every
//! replayed frame on top of the segments, their DV files, and the primary-key index rebuilt
//! from them. Because the live path and replay run the same code over the same frames, the
//! recovered state is the state the writer published.
//!
//! For each row operation with sequence number `seq`:
//!
//! 1. `old = pk_index.resolve(pk)`, following forwarding tables.
//! 2. If there is an `old` row, set its bit in its unit's deletion vector.
//! 3. A `Put` appends a slot to the active memtable and points the key at it; a `Delete`
//!    forgets the key (a delete of a missing key changes nothing but still consumes `seq`).
//!
//! So an upsert of an existing key never rewrites a slot: it appends one and deletes the old
//! row, in the same `apply` call, so no `Version` shows zero or two live rows for the key (I5).

use super::pk_index::PkIndex;
use crate::{
    dv::DeletionMap, memtable::MemtableData, segment::SegmentHandle, version::VersionCounters,
};
use logpose_types::{
    CorruptionKind, LogPoseError, Result, RowAddr, SeqNo,
    record::PrimaryKey,
    schema::{CollectionSchema, FieldRef},
};
use logpose_wal::{
    ReplayFrame,
    codec::{PayloadKind, RowImage, RowOp, WalPayload},
};
use std::sync::Arc;

/// The writer's private state: the schema, the units, their deletion vectors, the counters, and
/// the primary-key index. The persistent parts are O(fields) to clone; the index is not cloned
/// but journaled, so a group that is never appended can be taken back.
pub(crate) struct LogicalState {
    pub(crate) schema: Arc<CollectionSchema>,
    /// The memtable new rows go to.
    pub(crate) active: MemtableData,
    /// Memtables frozen for a flush, oldest first.
    pub(crate) frozen: Vec<Arc<MemtableData>>,
    /// Segments of the durable manifest, ascending by unit.
    pub(crate) segments: Arc<[Arc<SegmentHandle>]>,
    pub(crate) deletes: DeletionMap,
    pub(crate) counters: VersionCounters,
    pub(crate) pk: PkIndex,
    /// Whether an invariant violation fails the write (tests) instead of being counted.
    pub(crate) strict: bool,
}

/// The persistent part of a [`LogicalState`], taken before a group is prepared.
pub(crate) struct Savepoint {
    schema: Arc<CollectionSchema>,
    active: MemtableData,
    frozen: Vec<Arc<MemtableData>>,
    segments: Arc<[Arc<SegmentHandle>]>,
    deletes: DeletionMap,
    counters: VersionCounters,
}

impl LogicalState {
    /// The last sequence number applied.
    pub(crate) fn visible_seq_no(&self) -> SeqNo {
        self.active.last_seq_no
    }

    /// Remember the state before a group is prepared and start journaling the key index.
    pub(crate) fn savepoint(&mut self) -> Savepoint {
        self.pk.begin_group();
        Savepoint {
            schema: Arc::clone(&self.schema),
            active: self.active.clone(),
            frozen: self.frozen.clone(),
            segments: Arc::clone(&self.segments),
            deletes: self.deletes.clone(),
            counters: self.counters,
        }
    }

    /// Take back everything since `savepoint`: the group was never appended.
    pub(crate) fn restore(&mut self, savepoint: Savepoint) {
        self.pk.rollback_group();
        self.schema = savepoint.schema;
        self.active = savepoint.active;
        self.frozen = savepoint.frozen;
        self.segments = savepoint.segments;
        self.deletes = savepoint.deletes;
        self.counters = savepoint.counters;
    }

    /// The group since the last savepoint was handed to the WAL: it can no longer be taken back.
    pub(crate) fn release_savepoint(&mut self) {
        self.pk.end_group();
    }

    /// The memtable `unit` names, if it is the active or a frozen one.
    pub(crate) fn memtable(&self, unit: logpose_types::UnitId) -> Option<&MemtableData> {
        if self.active.unit == unit {
            return Some(&self.active);
        }
        self.frozen
            .iter()
            .find(|memtable| memtable.unit == unit)
            .map(AsRef::as_ref)
    }

    /// Resolve `pk` to its live row. A forwarding violation (an index entry for a row that was
    /// already deleted when its unit was retired) fails in strict mode and is otherwise counted
    /// and treated as absent.
    pub(crate) fn resolve(
        &mut self,
        pk: &PrimaryKey,
    ) -> std::result::Result<Option<RowAddr>, ApplyError> {
        match self.pk.resolve(pk) {
            Ok(addr) => Ok(addr),
            Err(violation) if self.strict => Err(ApplyError::Invariant(format!(
                "the primary-key index points key {pk} at {}, a row its job had already \
                 dropped",
                violation.addr
            ))),
            Err(violation) => {
                self.pk.violations += 1;
                tracing::error!(
                    %pk,
                    addr = %violation.addr,
                    "pk forwarding violation: an index entry pointed at a dropped row"
                );
                Ok(None)
            }
        }
    }

    /// Set `addr`'s deletion bit.
    fn mark(&mut self, addr: RowAddr) {
        if self.deletes.mark(addr) {
            self.counters.deleted_rows += 1;
        }
    }
}

/// The content of one WAL data frame.
#[derive(Debug)]
pub(crate) enum Change {
    /// Row operations validated against `schema_version`, one sequence number each.
    Batch {
        schema_version: u64,
        ops: Vec<RowOp>,
    },
    /// A complete new schema; one sequence number.
    Schema(CollectionSchema),
}

/// Why a frame cannot be applied to the state. Replay reports these as WAL corruption; the live
/// writer never produces the first three.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ApplyError {
    /// A batch validated against a schema newer than the one the log reached.
    BatchFromTheFuture { batch: u64, current: u64 },
    /// A schema change that skips a version.
    SchemaGap { change: u64, current: u64 },
    /// A row that does not fit the memtable (a key of the wrong type, a value that does not
    /// decode as its field's type).
    BadRow(String),
    /// An engine invariant was violated (strict mode only).
    Invariant(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BatchFromTheFuture { batch, current } => write!(
                formatter,
                "write batch was validated against schema version {batch}, but the log is at \
                 schema version {current}"
            ),
            Self::SchemaGap { change, current } => write!(
                formatter,
                "schema change to version {change} does not follow schema version {current}"
            ),
            Self::BadRow(reason) => write!(formatter, "a row does not fit the schema: {reason}"),
            Self::Invariant(reason) => write!(formatter, "invariant violated: {reason}"),
        }
    }
}

/// Apply one WAL data frame, whose first sequence number is `first_seq_no`, to `state`.
///
/// - A batch applies its row operations in order, one sequence number each. A batch validated
///   against an older schema is applied normally, except that values of fields the current
///   schema no longer declares are discarded: a later drop hid them, exactly as it did live.
///   A batch from a newer schema than the state's is an error.
/// - A schema change to `current + 1` becomes the state's schema and the active memtable's. One
///   at or below the current version is already reflected (recovery starts from a manifest whose
///   schema may be newer than its checkpoint) and only consumes its sequence number. Any other
///   version is an error.
pub(crate) fn apply(
    state: &mut LogicalState,
    first_seq_no: SeqNo,
    change: Change,
) -> std::result::Result<(), ApplyError> {
    let current = state.schema.schema_version();
    match change {
        Change::Batch {
            schema_version,
            ops,
        } => {
            if schema_version > current {
                return Err(ApplyError::BatchFromTheFuture {
                    batch: schema_version,
                    current,
                });
            }
            let bytes_before = state.active.bytes().total();
            for (op, seq_no) in ops.into_iter().zip(first_seq_no..) {
                match op {
                    RowOp::Put(mut image) => {
                        if schema_version < current {
                            retain_declared(&state.schema, &mut image);
                        }
                        let pk = PrimaryKey::from(image.pk.clone());
                        let old = state.resolve(&pk)?;
                        let slot = state
                            .active
                            .push(seq_no, &image)
                            .map_err(ApplyError::BadRow)?;
                        if let Some(old) = old {
                            state.mark(old);
                        }
                        state.pk.insert(
                            pk,
                            RowAddr {
                                unit: state.active.unit,
                                row: slot,
                            },
                        );
                        state.counters.total_rows += 1;
                        state.counters.memtable_rows += 1;
                    }
                    RowOp::Delete(pk) => {
                        let pk = PrimaryKey::from(pk);
                        if let Some(old) = state.resolve(&pk)? {
                            state.mark(old);
                            state.pk.remove(&pk);
                        }
                        state.active.note_op(seq_no);
                    }
                }
            }
            let bytes_after = state.active.bytes().total();
            state.counters.memtable_bytes += bytes_after.saturating_sub(bytes_before);
        }
        Change::Schema(schema) => {
            let version = schema.schema_version();
            if version > current.saturating_add(1) {
                return Err(ApplyError::SchemaGap {
                    change: version,
                    current,
                });
            }
            if version == current + 1 {
                let schema = Arc::new(schema);
                state.schema = Arc::clone(&schema);
                state.active.apply_schema(schema);
            }
            state.active.note_op(first_seq_no);
        }
    }
    Ok(())
}

/// Drop the vector and scalar values whose field ids `schema` does not declare.
fn retain_declared(schema: &CollectionSchema, image: &mut RowImage) {
    image
        .vectors
        .retain(|(id, _)| matches!(schema.field_by_id(*id), Some(FieldRef::Vector(_))));
    image
        .scalars
        .retain(|(id, _)| matches!(schema.field_by_id(*id), Some(FieldRef::Scalar(_))));
}

/// Replay one recovered frame into `state`.
///
/// `manifest_checkpoint` is the checkpoint of the manifest recovery started from. Checkpoint
/// frames are cross-checked against it: a checkpoint frame is written only after the manifest
/// with that checkpoint is durable, so a checkpoint frame above the manifest's checkpoint means
/// `CURRENT` went backwards, and recovery must not continue.
pub(crate) fn replay_frame(
    state: &mut LogicalState,
    manifest_checkpoint: SeqNo,
    frame: ReplayFrame,
) -> Result<()> {
    let corrupt = |reason: String| LogPoseError::Corrupt {
        kind: CorruptionKind::Wal,
        location: Some(format!("{}:{}", frame.file.display(), frame.offset)),
        message: format!(
            "WAL file '{}' is corrupt at offset {}: {reason}",
            frame.file.display(),
            frame.offset
        ),
    };
    let payload = WalPayload::decode_as(&frame.payload, frame.header.kind)
        .map_err(|error| corrupt(format!("undecodable payload: {error}")))?;
    let change = match payload {
        WalPayload::Checkpoint(checkpoint) => {
            if checkpoint.checkpoint_seq_no > manifest_checkpoint
                || frame.header.first_seq_no > manifest_checkpoint
            {
                return Err(corrupt(format!(
                    "checkpoint frame names checkpoint {} of manifest generation {}, but the \
                     manifest recovery started from has checkpoint {manifest_checkpoint}",
                    checkpoint.checkpoint_seq_no, checkpoint.manifest_generation
                )));
            }
            return Ok(());
        }
        WalPayload::WriteBatch(batch) => {
            let expected = frame.header.last_seq_no - frame.header.first_seq_no + 1;
            if batch.ops.len() as u64 != expected {
                return Err(corrupt(format!(
                    "write batch holds {} operations but its frame covers {expected} sequence \
                     numbers",
                    batch.ops.len()
                )));
            }
            Change::Batch {
                schema_version: batch.schema_version,
                ops: batch.ops,
            }
        }
        WalPayload::SchemaChange(change) => Change::Schema(change.schema),
    };
    debug_assert_ne!(frame.header.kind, PayloadKind::Checkpoint);
    apply(state, frame.header.first_seq_no, change).map_err(|error| match error {
        ApplyError::Invariant(reason) => LogPoseError::internal(reason),
        other => corrupt(other.to_string()),
    })
}

#[cfg(test)]
mod tests;
