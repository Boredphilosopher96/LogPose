//! Client-streaming bulk ingest (`BulkWriteCollection`).
//!
//! Semantics, also documented on the proto message and in `docs/src/api-overview.md`:
//!
//! - The first message names the collection; later messages may omit it or must repeat it.
//! - Each message is one batch, committed atomically like one `WriteCollection` call.
//! - Batches commit in order, one at a time. The next message is read only after the previous
//!   batch commits, so HTTP/2 flow control pushes back on a client that sends faster than the
//!   collection commits.
//! - The first failing batch fails the RPC with a [`LogPoseError::BulkBatchFailed`] that names
//!   it and reports how many batches were committed before it. Nothing after it is read.
//! - The stream is drained on its own task, so a client that disconnects mid-batch cannot
//!   cancel a batch half way: it commits or fails as a whole, and no later batch is applied.

use super::{
    collection_lookup_key, normalize_database_name, proto, request_auth_from_metadata,
    snapshot_message_from_domain, write_operations_from_proto,
};
use crate::error::message_too_large;
use logpose_core::{AppState, RequestAuth};
use logpose_types::{CommitAck, LogPoseError};
use proto::{BulkWriteCollectionReply, BulkWriteCollectionRequest};
use std::sync::Arc;
use tonic::{Code, Request, Status, Streaming};

pub(crate) async fn bulk_write_collection(
    state: Arc<AppState>,
    request: Request<Streaming<BulkWriteCollectionRequest>>,
) -> Result<BulkWriteCollectionReply, LogPoseError> {
    let auth = request_auth_from_metadata(&request)?;
    let stream = request.into_inner();
    // If the client goes away, tonic drops this future; the spawned task keeps a batch that is
    // being committed from being cancelled half way and stops at the next read.
    tokio::spawn(ingest(state, auth, stream))
        .await
        .map_err(|error| LogPoseError::internal(format!("bulk write task failed: {error}")))?
}

/// Where the stream writes, fixed by its first message.
#[derive(Clone)]
struct Target {
    database_name: String,
    collection_name: String,
}

#[derive(Default)]
struct Progress {
    committed_batches: u64,
    committed_operations: u64,
    last_ack: Option<CommitAck>,
}

impl Progress {
    fn commit(&mut self, ack: CommitAck) {
        self.committed_batches += 1;
        self.committed_operations += ack.applied_ops as u64;
        self.last_ack = Some(ack);
    }

    fn fail(&self, error: LogPoseError) -> LogPoseError {
        LogPoseError::BulkBatchFailed {
            batch_index: self.committed_batches,
            committed_batches: self.committed_batches,
            committed_operations: self.committed_operations,
            last_committed_seq_no: self.last_ack.as_ref().map(|ack| ack.last_seq_no),
            source: Box::new(error),
        }
    }
}

async fn ingest(
    state: Arc<AppState>,
    auth: RequestAuth,
    mut stream: Streaming<BulkWriteCollectionRequest>,
) -> Result<BulkWriteCollectionReply, LogPoseError> {
    let limit = state.config.limits.max_grpc_message_bytes;
    let mut target = None;
    let mut progress = Progress::default();
    loop {
        let message = match stream.message().await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(status) => return Err(progress.fail(stream_error(&status, limit))),
        };
        let result = async {
            let target = resolve_target(&mut target, &message)?;
            let operations = write_operations_from_proto(message.operations)?;
            state
                .write_with_auth(
                    &auth,
                    &collection_lookup_key(&target.database_name, &target.collection_name),
                    operations,
                )
                .await
        }
        .await;
        match result {
            Ok(ack) => progress.commit(ack),
            Err(error) => return Err(progress.fail(error)),
        }
    }
    let (Some(target), Some(last_ack)) = (target, progress.last_ack) else {
        return Err(LogPoseError::invalid_argument(
            "bulk write stream must contain at least one batch",
        ));
    };
    Ok(BulkWriteCollectionReply {
        database_name: target.database_name,
        collection_name: target.collection_name,
        committed_batches: progress.committed_batches,
        applied_ops: progress.committed_operations,
        last_seq_no: last_ack.last_seq_no,
        snapshot: Some(snapshot_message_from_domain(last_ack.snapshot)),
    })
}

fn resolve_target(
    target: &mut Option<Target>,
    message: &BulkWriteCollectionRequest,
) -> Result<Target, LogPoseError> {
    let Some(fixed) = target else {
        if message.collection_name.trim().is_empty() {
            return Err(LogPoseError::invalid_field(
                "collection_name",
                "the first bulk write batch must name the collection",
            ));
        }
        let resolved = Target {
            database_name: normalize_database_name(&message.database_name),
            collection_name: message.collection_name.clone(),
        };
        *target = Some(resolved.clone());
        return Ok(resolved);
    };
    if !message.collection_name.is_empty() && message.collection_name != fixed.collection_name {
        return Err(LogPoseError::invalid_field(
            "collection_name",
            format!(
                "every bulk write batch must target collection '{}', got '{}'",
                fixed.collection_name, message.collection_name
            ),
        ));
    }
    if !message.database_name.is_empty()
        && normalize_database_name(&message.database_name) != fixed.database_name
    {
        return Err(LogPoseError::invalid_field(
            "database_name",
            format!(
                "every bulk write batch must target database '{}', got '{}'",
                fixed.database_name, message.database_name
            ),
        ));
    }
    Ok(fixed.clone())
}

/// The error for a stream that failed before a message could be read.
fn stream_error(status: &Status, limit: usize) -> LogPoseError {
    match status.code() {
        // Tonic's decode limit; see `MessageLimitLayer`.
        Code::OutOfRange => message_too_large(limit),
        // Tonic reports an undecodable protobuf message as INTERNAL.
        Code::Internal => LogPoseError::invalid_argument(format!(
            "malformed bulk write message: {}",
            status.message()
        )),
        _ => LogPoseError::unavailable(format!(
            "bulk write stream was interrupted: {}",
            status.message()
        )),
    }
}
