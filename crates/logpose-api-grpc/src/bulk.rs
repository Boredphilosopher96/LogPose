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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestServer, put, small_grpc_limit};
    use logpose_config::LimitsConfig;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic_types::StatusExt;

    fn batch(collection: &str, ids: &[&str]) -> BulkWriteCollectionRequest {
        BulkWriteCollectionRequest {
            collection_name: collection.to_owned(),
            database_name: String::new(),
            operations: ids.iter().map(|id| put(id)).collect(),
        }
    }

    fn error_metadata(status: &Status, key: &str) -> Option<String> {
        status
            .get_details_error_info()
            .and_then(|info| info.metadata.get(key).cloned())
    }

    fn reason(status: &Status) -> Option<String> {
        status.get_details_error_info().map(|info| info.reason)
    }

    #[tokio::test]
    async fn bulk_write_commits_every_batch_and_returns_a_summary() {
        let mut server = TestServer::start("bulk-happy", LimitsConfig::default()).await;
        server.create_collection("docs").await;

        let reply = server
            .client
            .bulk_write_collection(tokio_stream::iter([
                batch("docs", &["a", "b"]),
                batch("", &["c"]),
                batch("docs", &["d", "e", "f"]),
            ]))
            .await
            .expect("bulk write should succeed")
            .into_inner();

        assert_eq!(reply.database_name, "default");
        assert_eq!(reply.collection_name, "docs");
        assert_eq!(reply.committed_batches, 3);
        assert_eq!(reply.applied_ops, 6);
        assert_eq!(reply.last_seq_no, 6);
        assert_eq!(
            reply.snapshot.map(|snapshot| snapshot.visible_seq_no),
            Some(6)
        );
        assert_eq!(server.live_records("docs").await, 6);
    }

    #[tokio::test]
    async fn bulk_write_stops_at_the_first_failed_batch_and_reports_committed_progress() {
        let mut server = TestServer::start("bulk-mid-failure", LimitsConfig::default()).await;
        server.create_collection("docs").await;
        let mut bad = batch("docs", &["c"]);
        if let Some(proto::write_operation::Operation::Put(record)) =
            &mut bad.operations[0].operation
        {
            record.vector = vec![1.0, 2.0, 3.0];
        }

        let status = server
            .client
            .bulk_write_collection(tokio_stream::iter([
                batch("docs", &["a", "b"]),
                bad,
                batch("docs", &["d"]),
            ]))
            .await
            .expect_err("the second batch should fail the stream");

        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(reason(&status).as_deref(), Some("DIMENSION_MISMATCH"));
        assert_eq!(
            error_metadata(&status, "failed_batch_index").as_deref(),
            Some("1")
        );
        assert_eq!(
            error_metadata(&status, "committed_batches").as_deref(),
            Some("1")
        );
        assert_eq!(
            error_metadata(&status, "committed_operations").as_deref(),
            Some("2")
        );
        assert_eq!(
            error_metadata(&status, "last_committed_seq_no").as_deref(),
            Some("2")
        );
        let violations = status
            .get_details_bad_request()
            .map(|bad_request| bad_request.field_violations)
            .unwrap_or_default();
        assert_eq!(violations[0].field, "operations[0].vector");
        // The failed batch is atomic and nothing after it is applied.
        assert_eq!(server.live_records("docs").await, 2);
    }

    #[tokio::test]
    async fn bulk_write_rejects_an_oversized_batch_as_resource_exhausted() {
        let mut server = TestServer::start("bulk-oversize", small_grpc_limit(1024)).await;
        server.create_collection("docs").await;
        let mut oversized = batch("docs", &["big"]);
        if let Some(proto::write_operation::Operation::Put(record)) =
            &mut oversized.operations[0].operation
        {
            record.metadata_json = format!("{{\"blob\":\"{}\"}}", "x".repeat(4096));
        }

        let status = server
            .client
            .bulk_write_collection(tokio_stream::iter([batch("docs", &["a"]), oversized]))
            .await
            .expect_err("an oversized batch should fail the stream");

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(reason(&status).as_deref(), Some("TOO_LARGE"));
        assert_eq!(
            error_metadata(&status, "limit_bytes").as_deref(),
            Some("1024")
        );
        assert_eq!(
            error_metadata(&status, "failed_batch_index").as_deref(),
            Some("1")
        );
        assert_eq!(
            error_metadata(&status, "committed_batches").as_deref(),
            Some("1")
        );
        assert_eq!(server.live_records("docs").await, 1);
    }

    #[tokio::test]
    async fn bulk_write_keeps_committed_batches_when_the_client_disconnects() {
        let mut server = TestServer::start("bulk-disconnect", LimitsConfig::default()).await;
        server.create_collection("docs").await;
        let (sender, receiver) = mpsc::channel(4);
        let mut client = server.client.clone();
        let call = tokio::spawn(async move {
            client
                .bulk_write_collection(ReceiverStream::new(receiver))
                .await
        });
        sender
            .send(batch("docs", &["a", "b"]))
            .await
            .expect("first batch should be sent");
        let mut waited = 0;
        while server.live_records("docs").await < 2 {
            waited += 1;
            assert!(waited < 250, "the first batch was never committed");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Disconnect mid-stream: the call is dropped while the server waits for batch 2.
        call.abort();
        let _ = call.await;
        let _ = sender.send(batch("docs", &["late"])).await;
        drop(sender);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(server.live_records("docs").await, 2);
        // The server keeps serving the collection.
        let reply = server
            .client
            .bulk_write_collection(tokio_stream::iter([batch("docs", &["c"])]))
            .await
            .expect("a new stream should succeed after a disconnect")
            .into_inner();
        assert_eq!(reply.committed_batches, 1);
        assert_eq!(server.live_records("docs").await, 3);
    }

    #[tokio::test]
    async fn bulk_write_rejects_an_empty_stream() {
        let mut server = TestServer::start("bulk-empty", LimitsConfig::default()).await;
        let status = server
            .client
            .bulk_write_collection(tokio_stream::iter(Vec::<BulkWriteCollectionRequest>::new()))
            .await
            .expect_err("an empty stream should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(reason(&status).as_deref(), Some("INVALID_ARGUMENT"));
    }

    #[tokio::test]
    async fn bulk_write_rejects_batches_that_switch_collections() {
        let mut server = TestServer::start("bulk-switch", LimitsConfig::default()).await;
        server.create_collection("docs").await;
        server.create_collection("other").await;

        let status = server
            .client
            .bulk_write_collection(tokio_stream::iter([
                batch("docs", &["a"]),
                batch("other", &["b"]),
            ]))
            .await
            .expect_err("switching collections should be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
        let violations = status
            .get_details_bad_request()
            .map(|bad_request| bad_request.field_violations)
            .unwrap_or_default();
        assert_eq!(violations[0].field, "collection_name");
        assert_eq!(
            error_metadata(&status, "failed_batch_index").as_deref(),
            Some("1")
        );
        assert_eq!(server.live_records("other").await, 0);
    }

    #[tokio::test]
    async fn bulk_write_requires_the_first_batch_to_name_the_collection() {
        let mut server = TestServer::start("bulk-unnamed", LimitsConfig::default()).await;
        let status = server
            .client
            .bulk_write_collection(tokio_stream::iter([batch("", &["a"])]))
            .await
            .expect_err("an unnamed first batch should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(
            error_metadata(&status, "failed_batch_index").as_deref(),
            Some("0")
        );
        assert_eq!(
            error_metadata(&status, "committed_batches").as_deref(),
            Some("0")
        );
    }
}
