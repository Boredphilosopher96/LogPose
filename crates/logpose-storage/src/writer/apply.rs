//! The one `apply` function shared by the live writer and WAL replay.
//!
//! Both paths feed it the same thing: the decoded content of one WAL data frame and the frame's
//! first sequence number. The writer calls it while preparing a group (before the group's fsync;
//! nothing it produces is published until the fsync returns), and recovery and historical reads
//! call it for every replayed frame. Because the live path and replay run the same code over the
//! same frames, the recovered state is the state the writer published.

use crate::version::{DeltaLog, DeltaOp, DeltaRecord};
use logpose_types::{
    CorruptionKind, LogPoseError, Result, SeqNo,
    schema::{CollectionSchema, FieldRef},
};
use logpose_wal::{
    ReplayFrame,
    codec::{PayloadKind, RowImage, RowOp, WalPayload},
};
use std::sync::Arc;

/// The logical state `apply` advances: the schema as of the last applied operation and the
/// delta above the manifest checkpoint.
#[derive(Clone, Debug)]
pub(crate) struct LogicalState {
    pub(crate) schema: Arc<CollectionSchema>,
    pub(crate) delta: DeltaLog,
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
/// writer never produces them.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ApplyError {
    /// A batch validated against a schema newer than the one the log reached.
    BatchFromTheFuture { batch: u64, current: u64 },
    /// A schema change that skips a version.
    SchemaGap { change: u64, current: u64 },
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
        }
    }
}

/// Apply one WAL data frame, whose first sequence number is `first_seq_no`, to `state`.
///
/// - A batch becomes one delta batch, one sequence number per operation. A batch validated
///   against an older schema is applied normally, except that values of fields the current
///   schema no longer declares are discarded: a later drop hid them, exactly as it did live.
///   A batch from a newer schema than the state's is an error.
/// - A schema change to `current + 1` becomes the state's schema. One at or below the current
///   version is already reflected (recovery starts from a manifest whose schema may be newer
///   than its checkpoint) and only consumes its sequence number. Any other version is an error.
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
            let records = ops
                .into_iter()
                .zip(first_seq_no..)
                .map(|(op, seq_no)| DeltaRecord {
                    seq_no,
                    op: match op {
                        RowOp::Put(mut image) => {
                            if schema_version < current {
                                retain_declared(&state.schema, &mut image);
                            }
                            DeltaOp::Put(image)
                        }
                        RowOp::Delete(pk) => DeltaOp::Delete(pk),
                    },
                })
                .collect();
            state.delta.append(records);
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
                state.schema = Arc::new(schema);
            }
            state.delta.append(vec![DeltaRecord {
                seq_no: first_seq_no,
                op: DeltaOp::SchemaChange {
                    schema_version: version,
                },
            }]);
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
    apply(state, frame.header.first_seq_no, change).map_err(|error| corrupt(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_types::{
        DistanceMetric,
        legacy::legacy_schema,
        record::Record,
        schema::{FieldType, ScalarFieldSpec},
        value::Value,
    };
    use logpose_wal::codec::WirePk;

    fn base() -> CollectionSchema {
        legacy_schema(2, DistanceMetric::Dot).expect("schema")
    }

    fn put(schema: &CollectionSchema, id: &str, price: Option<i64>) -> RowOp {
        let mut record = Record::new(id).with_vector("vector", vec![1.0, 0.0]);
        if let Some(price) = price {
            record = record.with_field("price", Value::Int64(price));
        }
        RowOp::Put(RowImage::from_record(schema, record).expect("image"))
    }

    #[test]
    fn batches_from_an_older_schema_lose_fields_a_later_drop_hid() {
        let mut v2 = base();
        v2.add_field(ScalarFieldSpec::new("price", FieldType::Int64))
            .expect("add");
        let mut v3 = v2.clone();
        v3.drop_field("price").expect("drop");

        // Replay from a manifest whose schema already is v3: the add (seq 1) and the drop
        // (seq 3) are skipped, and the batch written under v2 loses its price.
        let mut state = LogicalState {
            schema: Arc::new(v3.clone()),
            delta: DeltaLog::default(),
        };
        apply(&mut state, 1, Change::Schema(v2.clone())).expect("skip v2");
        apply(
            &mut state,
            2,
            Change::Batch {
                schema_version: 2,
                ops: vec![put(&v2, "a", Some(5))],
            },
        )
        .expect("batch");
        apply(&mut state, 3, Change::Schema(v3.clone())).expect("skip v3");
        assert_eq!(state.schema.schema_version(), 3);
        let records = state.delta.iter().collect::<Vec<_>>();
        assert_eq!(records.len(), 3);
        let image = match &records[1].op {
            DeltaOp::Put(image) => Some(image),
            _ => None,
        }
        .expect("the batch's put is in the delta");
        assert!(
            image.scalars.is_empty(),
            "the dropped field's value is gone"
        );
        assert_eq!(image.pk, WirePk::String("a".to_owned()));
    }

    #[test]
    fn replay_from_an_older_schema_applies_changes_in_order() {
        let mut v2 = base();
        v2.add_field(ScalarFieldSpec::new("price", FieldType::Int64))
            .expect("add");
        let mut state = LogicalState {
            schema: Arc::new(base()),
            delta: DeltaLog::default(),
        };
        apply(&mut state, 1, Change::Schema(v2.clone())).expect("v2");
        apply(
            &mut state,
            2,
            Change::Batch {
                schema_version: 2,
                ops: vec![
                    put(&v2, "a", Some(5)),
                    RowOp::Delete(WirePk::String("b".into())),
                ],
            },
        )
        .expect("batch");
        assert_eq!(state.schema.as_ref(), &v2);
        let image = match &state.delta.iter().nth(1).expect("put").op {
            DeltaOp::Put(image) => Some(image),
            _ => None,
        }
        .expect("the batch's put is in the delta");
        assert_eq!(
            image.scalars.len(),
            1,
            "the field is declared, so its value stays"
        );
        assert_eq!(state.delta.last_seq_no(), Some(3));
    }

    #[test]
    fn frames_that_skip_schema_versions_are_rejected() {
        let mut v2 = base();
        v2.add_field(ScalarFieldSpec::new("price", FieldType::Int64))
            .expect("add");
        let mut v3 = v2.clone();
        v3.drop_field("price").expect("drop");
        let mut state = LogicalState {
            schema: Arc::new(base()),
            delta: DeltaLog::default(),
        };
        assert_eq!(
            apply(&mut state, 1, Change::Schema(v3)),
            Err(ApplyError::SchemaGap {
                change: 3,
                current: 1
            })
        );
        assert_eq!(
            apply(
                &mut state,
                1,
                Change::Batch {
                    schema_version: 2,
                    ops: vec![put(&v2, "a", None)],
                },
            ),
            Err(ApplyError::BatchFromTheFuture {
                batch: 2,
                current: 1
            })
        );
        assert!(state.delta.is_empty(), "a rejected frame changes nothing");
    }
}
