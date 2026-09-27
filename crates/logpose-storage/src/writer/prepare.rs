//! Preparing a group: validate each request against the writer's schema at its point in the
//! stream, turn it into row operations and one WAL frame, and apply it to the writer's private
//! state. Pure CPU work over owned data, so it can run on the query pool.

use super::{
    Ack, SchemaChange, WriteRequest,
    apply::{Change, LogicalState, apply},
};
use logpose_types::{
    LogPoseError, Result, SeqNo,
    record::{ClientOp, PrimaryKey},
    schema::CollectionSchema,
};
use logpose_wal::{
    MAX_FRAME_PAYLOAD, WalFrame,
    codec::{RowImage, RowOp, SchemaChangePayload, WalPayload, WirePk, WriteBatchPayload},
};
use std::collections::HashSet;

/// A request that got sequence numbers and a frame, waiting for its group's fsync.
pub(crate) struct Pending {
    pub(crate) ack: Ack,
    pub(crate) last_seq_no: SeqNo,
    pub(crate) applied_ops: usize,
}

/// The output of preparing one group.
#[derive(Default)]
pub(crate) struct PreparedRequests {
    /// One frame per accepted request, in request order.
    pub(crate) frames: Vec<WalFrame>,
    /// The accepted requests, in the same order.
    pub(crate) pending: Vec<Pending>,
    /// Row operations in the accepted requests, for the batching threshold.
    pub(crate) rows: usize,
}

/// Groups with fewer rows than this are prepared inline on the writer task; larger ones on the
/// query pool.
pub(crate) const INLINE_PREPARE_ROWS: usize = 64;

/// Prepare `requests` in order against `state`, whose next sequence number is `next_seq_no`.
/// A request that fails validation is acknowledged with its error right away and consumes no
/// sequence number. Returns the next sequence number after the group.
pub(crate) fn prepare(
    state: &mut LogicalState,
    mut next_seq_no: SeqNo,
    requests: Vec<WriteRequest>,
) -> (PreparedRequests, SeqNo) {
    let mut prepared = PreparedRequests::default();
    for request in requests {
        match request {
            WriteRequest::Batch { ops, ack } => {
                let applied_ops = ops.len();
                match batch_frame(&state.schema, next_seq_no, ops) {
                    Ok((frame, change)) => {
                        let last_seq_no = frame.last_seq_no();
                        if let Err(error) = apply(state, next_seq_no, change) {
                            // Unreachable: the batch was validated against this schema.
                            let _ = ack.send(Err(LogPoseError::Message(error.to_string())));
                            continue;
                        }
                        prepared.frames.push(frame);
                        prepared.pending.push(Pending {
                            ack,
                            last_seq_no,
                            applied_ops,
                        });
                        prepared.rows += applied_ops;
                        next_seq_no = last_seq_no + 1;
                    }
                    Err(error) => {
                        let _ = ack.send(Err(error));
                    }
                }
            }
            WriteRequest::AlterSchema { change, ack } => {
                match schema_frame(&state.schema, next_seq_no, &change) {
                    Ok((frame, schema)) => {
                        if let Err(error) = apply(state, next_seq_no, Change::Schema(schema)) {
                            let _ = ack.send(Err(LogPoseError::Message(error.to_string())));
                            continue;
                        }
                        prepared.frames.push(frame);
                        prepared.pending.push(Pending {
                            ack,
                            last_seq_no: next_seq_no,
                            applied_ops: 1,
                        });
                        next_seq_no += 1;
                    }
                    Err(error) => {
                        let _ = ack.send(Err(error));
                    }
                }
            }
        }
    }
    (prepared, next_seq_no)
}

/// Validate a client batch and build its frame. Nothing is applied yet.
fn batch_frame(
    schema: &CollectionSchema,
    first_seq_no: SeqNo,
    ops: Vec<ClientOp>,
) -> Result<(WalFrame, Change)> {
    if ops.is_empty() {
        return Err(LogPoseError::Message(
            "write batch must include at least one operation".to_owned(),
        ));
    }
    let mut seen = HashSet::with_capacity(ops.len());
    let mut row_ops = Vec::with_capacity(ops.len());
    for op in ops {
        let pk = op.pk().clone();
        if !seen.insert(WirePk::from(pk.clone())) {
            return Err(LogPoseError::Message(format!(
                "write batch includes duplicate record id '{pk}'"
            )));
        }
        row_ops.push(row_op(schema, op).map_err(|error| invalid_record(&pk, error))?);
    }
    let count = row_ops.len() as SeqNo;
    let payload = WalPayload::WriteBatch(WriteBatchPayload {
        schema_version: schema.schema_version(),
        ops: row_ops,
    });
    let bytes = encode(&payload)?;
    let WalPayload::WriteBatch(batch) = payload else {
        return Err(LogPoseError::Message(
            "write batch payload changed kind".to_owned(),
        ));
    };
    let frame = WalFrame::write_batch(first_seq_no, first_seq_no + count - 1, bytes)?;
    Ok((
        frame,
        Change::Batch {
            schema_version: batch.schema_version,
            ops: batch.ops,
        },
    ))
}

/// Validate one client operation and turn it into a blind write.
fn row_op(schema: &CollectionSchema, op: ClientOp) -> std::result::Result<RowOp, String> {
    match op {
        ClientOp::Upsert(record) => RowImage::from_record(schema, record)
            .map(RowOp::Put)
            .map_err(|error| error.to_string()),
        ClientOp::Delete(pk) => {
            schema
                .validate_primary_key(&pk)
                .map_err(|error| error.to_string())?;
            Ok(RowOp::Delete(pk.into()))
        }
        // A partial update must read the old row, which lives in the delta or in a v1 segment.
        // The writer-private primary-key index that makes that O(1) lands with the memtable
        // (PR 10); until then partial updates are refused rather than served by a scan.
        ClientOp::Update(_) => {
            Err("partial updates are unsupported until the memtable lands".to_owned())
        }
    }
}

fn invalid_record(pk: &PrimaryKey, error: String) -> LogPoseError {
    LogPoseError::Message(format!("record '{pk}' is invalid: {error}"))
}

/// Validate a schema change and build its frame. Returns the new schema, not yet applied.
fn schema_frame(
    schema: &CollectionSchema,
    seq_no: SeqNo,
    change: &SchemaChange,
) -> Result<(WalFrame, CollectionSchema)> {
    let mut next = schema.clone();
    change.apply_to(&mut next).map_err(|error| {
        LogPoseError::Message(format!("invalid schema change {change:?}: {error}"))
    })?;
    let payload = WalPayload::SchemaChange(SchemaChangePayload { schema: next });
    let bytes = encode(&payload)?;
    let WalPayload::SchemaChange(SchemaChangePayload { schema: next }) = payload else {
        return Err(LogPoseError::Message(
            "schema change payload changed kind".to_owned(),
        ));
    };
    Ok((WalFrame::schema_change(seq_no, bytes)?, next))
}

fn encode(payload: &WalPayload) -> Result<Vec<u8>> {
    let bytes = payload
        .encode()
        .map_err(|error| LogPoseError::internal(format!("invalid WAL payload: {error}")))?;
    if bytes.len() > MAX_FRAME_PAYLOAD as usize {
        return Err(LogPoseError::TooLarge {
            what: "write batch WAL frame payload".to_owned(),
            size: u64::try_from(bytes.len()).ok(),
            limit: u64::from(MAX_FRAME_PAYLOAD),
        });
    }
    Ok(bytes)
}
