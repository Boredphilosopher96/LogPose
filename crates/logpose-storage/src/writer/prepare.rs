//! Preparing a group: validate each request against the writer's schema at its point in the
//! stream, turn it into row operations and one WAL frame, and apply it to the writer's private
//! state. Pure CPU work over owned data, so it can run on the query pool: the old rows partial
//! updates merge that live in segments were fetched on the I/O pool beforehand
//! ([`FetchedRows`]); rows in memtables are read directly.

use super::{
    Ack, SchemaChange, WriteRequest,
    apply::{Change, LogicalState, apply},
};
use logpose_types::{
    LogPoseError, ResourceKind, Result, RowAddr, SeqNo,
    record::{ClientOp, PartialUpdate, PrimaryKey},
    schema::CollectionSchema,
};
use logpose_wal::{
    MAX_FRAME_PAYLOAD, WalFrame,
    codec::{
        RowImage, RowImageError, RowOp, SchemaChangePayload, WalPayload, WirePk, WriteBatchPayload,
    },
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Old rows of partial updates that live in segments, read on the I/O pool before the group is
/// prepared, by address. A failed read fails only the updates that need that row.
pub(crate) type FetchedRows = HashMap<RowAddr, std::result::Result<RowImage, LogPoseError>>;

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
    /// Set when applying a validated request failed, which only an engine invariant violation
    /// can cause: the private state may be partly changed, so the writer poisons the
    /// collection. Requests after it were refused.
    pub(crate) fatal: Option<String>,
}

/// Groups with fewer rows than this, and fewer approximate bytes than [`INLINE_PREPARE_BYTES`],
/// are prepared inline on the writer task; larger ones on the query pool.
pub(crate) const INLINE_PREPARE_ROWS: usize = 64;
/// Approximate request bytes at which a group is prepared on the query pool however few rows it
/// has: validating, normalizing and encoding a few very large rows (high-dimensional vectors,
/// big documents) is more CPU than the writer runtime may spend inline.
pub(crate) const INLINE_PREPARE_BYTES: usize = 256 * 1024;

/// Whether a group of `rows` row operations and about `bytes` request bytes is prepared inline
/// on the writer task.
pub(crate) fn prepares_inline(rows: usize, bytes: usize) -> bool {
    rows < INLINE_PREPARE_ROWS && bytes < INLINE_PREPARE_BYTES
}

/// Prepare `requests` in order against `state`, whose next sequence number is `next_seq_no`.
/// A request that fails validation is acknowledged with its error right away and consumes no
/// sequence number. Returns the next sequence number after the group.
pub(crate) fn prepare(
    state: &mut LogicalState,
    mut next_seq_no: SeqNo,
    requests: Vec<WriteRequest>,
    fetched: &FetchedRows,
) -> (PreparedRequests, SeqNo) {
    let mut prepared = PreparedRequests::default();
    for request in requests {
        if let Some(reason) = &prepared.fatal {
            let _ = request.ack().send(Err(LogPoseError::internal(format!(
                "an earlier write of the group violated an engine invariant: {reason}"
            ))));
            continue;
        }
        match request {
            WriteRequest::Batch { ops, ack } => {
                let applied_ops = ops.len();
                match batch_frame(state, next_seq_no, ops, fetched) {
                    Ok((frame, change)) => {
                        let last_seq_no = frame.last_seq_no();
                        if let Err(error) = apply(state, next_seq_no, change) {
                            // The batch was validated against this schema, so only a violated
                            // invariant gets here.
                            let _ = ack.send(Err(LogPoseError::internal(error.to_string())));
                            prepared.fatal = Some(error.to_string());
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
                            let _ = ack.send(Err(LogPoseError::internal(error.to_string())));
                            prepared.fatal = Some(error.to_string());
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

/// Validate a client batch and build its frame. Nothing is applied yet; the state is only read
/// (to merge partial updates with the rows they change).
///
/// Validation errors name the operation by its position in the batch (`[2]`, `[2].price`);
/// the API prefixes the request field that holds the batch.
fn batch_frame(
    state: &mut LogicalState,
    first_seq_no: SeqNo,
    ops: Vec<ClientOp>,
    fetched: &FetchedRows,
) -> Result<(WalFrame, Change)> {
    if ops.is_empty() {
        return Err(LogPoseError::invalid_field(
            "",
            "write batch must include at least one operation",
        ));
    }
    let schema = Arc::clone(&state.schema);
    let mut seen = HashSet::with_capacity(ops.len());
    let mut row_ops = Vec::with_capacity(ops.len());
    for (index, op) in ops.into_iter().enumerate() {
        let pk = op.pk().clone();
        if !seen.insert(WirePk::from(pk.clone())) {
            return Err(LogPoseError::invalid_field(
                format!("[{index}]"),
                format!("write batch includes primary key {pk} more than once"),
            ));
        }
        let op = match op {
            ClientOp::Update(update) => {
                RowOp::Put(merge_update(state, &schema, update, fetched, index)?)
            }
            other => row_op(&schema, other, index)?,
        };
        row_ops.push(op);
    }
    let count = row_ops.len() as SeqNo;
    let payload = WalPayload::WriteBatch(WriteBatchPayload {
        schema_version: schema.schema_version(),
        ops: row_ops,
    });
    let bytes = encode(&payload)?;
    let WalPayload::WriteBatch(batch) = payload else {
        return Err(LogPoseError::internal("write batch payload changed kind"));
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

/// Validate an upsert or a delete, the `index`th operation of its batch, and turn it into a
/// blind write.
fn row_op(schema: &CollectionSchema, op: ClientOp, index: usize) -> Result<RowOp> {
    match op {
        ClientOp::Upsert(record) => {
            let pk = record.pk.clone();
            RowImage::from_record(schema, record)
                .map(RowOp::Put)
                .map_err(|error| row_image_error(&error, index, &pk))
        }
        ClientOp::Delete(pk) => {
            schema.validate_primary_key(&pk).map_err(|error| {
                LogPoseError::invalid_field(format!("[{index}]"), error.to_string())
            })?;
            Ok(RowOp::Delete(pk.into()))
        }
        ClientOp::Update(_) => Err(LogPoseError::internal(
            "a partial update needs the row it changes",
        )),
    }
}

/// Merge a partial update, the `index`th operation of its batch, with the key's live row into
/// the full row image a blind `Put` logs. The old row is read with the current schema, so
/// values of dropped fields are gone and `$extra` keys the schema declares or retires are
/// removed from the merged row (they are not promoted into the typed field).
///
/// An update of a key without a live row fails with `NotFound` for the record.
fn merge_update(
    state: &mut LogicalState,
    schema: &CollectionSchema,
    update: PartialUpdate,
    fetched: &FetchedRows,
    index: usize,
) -> Result<RowImage> {
    let pk = update.pk.clone();
    let path = format!("[{index}]");
    let update = schema
        .validate_update(update)
        .map_err(|error| error.to_error(&path, Some(&pk)))?;
    let addr = state
        .resolve(&update.pk)
        .map_err(|error| LogPoseError::internal(error.to_string()))?
        .ok_or_else(|| LogPoseError::not_found(ResourceKind::Record, pk.label()))?;
    let old = match state.memtable(addr.unit) {
        Some(memtable) => memtable
            .row_image(addr.row)
            .map_err(LogPoseError::internal)?,
        None => match fetched.get(&addr) {
            Some(Ok(image)) => image.clone(),
            Some(Err(error)) => return Err(error.clone()),
            None => {
                return Err(LogPoseError::internal(format!(
                    "the row {addr} a partial update changes was not fetched"
                )));
            }
        },
    };
    let mut record = old
        .to_record(schema)
        .map_err(|error| LogPoseError::internal(error.to_string()))?;
    update
        .apply_to(&mut record)
        .map_err(|error| error.to_error(&path, Some(&pk)))?;
    RowImage::from_record(schema, record).map_err(|error| row_image_error(&error, index, &pk))
}

/// The wire error of a record, the `index`th operation of its batch, that cannot become a row
/// image: it names the offending field below `[index]`.
fn row_image_error(error: &RowImageError, index: usize, pk: &PrimaryKey) -> LogPoseError {
    let path = format!("[{index}]");
    match error {
        RowImageError::InvalidRecord(error) => error.to_error(&path, Some(pk)),
        RowImageError::ZeroNormVector { field } | RowImageError::Encode { field, .. } => {
            LogPoseError::invalid_field(
                format!("{path}.{field}"),
                format!("record {pk} is invalid: {error}"),
            )
        }
        other => LogPoseError::internal(format!("record {pk} could not be encoded: {other}")),
    }
}

/// Validate a schema change and build its frame. Returns the new schema, not yet applied.
fn schema_frame(
    schema: &CollectionSchema,
    seq_no: SeqNo,
    change: &SchemaChange,
) -> Result<(WalFrame, CollectionSchema)> {
    let mut next = schema.clone();
    change.apply_to(&mut next).map_err(|error| {
        LogPoseError::invalid_field(change.error_field(&error), error.to_string())
    })?;
    let payload = WalPayload::SchemaChange(SchemaChangePayload { schema: next });
    let bytes = encode(&payload)?;
    let WalPayload::SchemaChange(SchemaChangePayload { schema: next }) = payload else {
        return Err(LogPoseError::internal("schema change payload changed kind"));
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
