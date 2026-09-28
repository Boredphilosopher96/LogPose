//! Client-streaming bulk ingest (`BulkUpsertRecords`).
//!
//! Semantics, also documented on the proto message and in `docs/src/api-overview.md`:
//!
//! - The first message names the collection; later messages may omit it or must repeat it.
//! - Each message is one batch, committed atomically like one `UpsertRecords` call.
//! - Batches commit in order, one at a time. The next message is read only after the previous
//!   batch commits, so HTTP/2 flow control pushes back on a client that sends faster than the
//!   collection commits.
//! - The first failing batch fails the RPC with a [`LogPoseError::BulkBatchFailed`] that names
//!   it and reports how many batches were committed before it. Nothing after it is read.
//! - The stream is drained on its own task, so a client that disconnects mid-batch cannot
//!   cancel a batch half way: it commits or fails as a whole. Once tonic drops the handler for
//!   a cancelled call, the task starts no further batch, even one the client sent before it
//!   cancelled and that is still buffered in the HTTP/2 stream.

use super::{proto, request_auth_from_metadata};
use crate::{
    convert::{records_from_proto, required_name, snapshot_to_proto},
    error::message_too_large,
};
use logpose_core::{AppState, RequestAuth};
use logpose_types::{CommitAck, LogPoseError};
use proto::{BulkUpsertRecordsReply, BulkUpsertRecordsRequest};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Request, Status, Streaming};

pub(crate) async fn bulk_upsert_records(
    state: Arc<AppState>,
    request: Request<Streaming<BulkUpsertRecordsRequest>>,
) -> Result<BulkUpsertRecordsReply, LogPoseError> {
    let auth = request_auth_from_metadata(&request)?;
    let stream = request.into_inner();
    // If the client goes away, tonic drops this future; the spawned task keeps a batch that is
    // being committed from being cancelled half way, and the guard tells it to start no other.
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelOnDrop(Arc::clone(&cancelled));
    tokio::spawn(ingest(state, auth, stream, cancelled))
        .await
        .map_err(|error| LogPoseError::internal(format!("bulk upsert task failed: {error}")))?
}

/// Marks the call cancelled when dropped. The handler future owns it, and tonic drops that
/// future when the client cancels or disconnects.
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Where the stream writes, fixed by its first message.
#[derive(Clone)]
struct Target {
    database_name: String,
    collection_name: String,
}

impl Target {
    fn key(&self) -> String {
        format!("{}/{}", self.database_name, self.collection_name)
    }
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

async fn ingest<S>(
    state: Arc<AppState>,
    auth: RequestAuth,
    mut stream: S,
    cancelled: Arc<AtomicBool>,
) -> Result<BulkUpsertRecordsReply, LogPoseError>
where
    S: Stream<Item = Result<BulkUpsertRecordsRequest, Status>> + Unpin,
{
    let limit = state.config.limits.max_grpc_message_bytes;
    let mut target = None;
    let mut progress = Progress::default();
    loop {
        let message = match stream.next().await {
            Some(Ok(message)) => message,
            None => break,
            Some(Err(status)) => return Err(progress.fail(stream_error(&status, limit))),
        };
        // HTTP/2 can still hand over a message the client sent before it cancelled; nobody is
        // waiting for its result, so do not start it.
        if cancelled.load(Ordering::Acquire) {
            return Err(progress.fail(LogPoseError::unavailable(
                "the client cancelled the bulk upsert stream",
            )));
        }
        let result = async {
            let target = resolve_target(&mut target, &message)?;
            let records = records_from_proto(message.records, "records")?;
            state
                .upsert_records_with_auth(&auth, &target.key(), records)
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
            "bulk upsert stream must contain at least one batch",
        ));
    };
    Ok(BulkUpsertRecordsReply {
        database_name: target.database_name,
        collection_name: target.collection_name,
        committed_batches: progress.committed_batches,
        applied_ops: progress.committed_operations,
        last_seq_no: last_ack.last_seq_no,
        snapshot: Some(snapshot_to_proto(last_ack.snapshot)),
    })
}

fn resolve_target(
    target: &mut Option<Target>,
    message: &BulkUpsertRecordsRequest,
) -> Result<Target, LogPoseError> {
    let Some(fixed) = target else {
        let resolved = Target {
            database_name: required_name("database_name", message.database_name.clone()).map_err(
                |_| {
                    LogPoseError::invalid_field(
                        "database_name",
                        "the first bulk upsert batch must name the database",
                    )
                },
            )?,
            collection_name: required_name("collection_name", message.collection_name.clone())
                .map_err(|_| {
                    LogPoseError::invalid_field(
                        "collection_name",
                        "the first bulk upsert batch must name the collection",
                    )
                })?,
        };
        *target = Some(resolved.clone());
        return Ok(resolved);
    };
    if !message.collection_name.is_empty() && message.collection_name != fixed.collection_name {
        return Err(LogPoseError::invalid_field(
            "collection_name",
            format!(
                "every bulk upsert batch must target collection '{}', got '{}'",
                fixed.collection_name, message.collection_name
            ),
        ));
    }
    if !message.database_name.is_empty() && message.database_name != fixed.database_name {
        return Err(LogPoseError::invalid_field(
            "database_name",
            format!(
                "every bulk upsert batch must target database '{}', got '{}'",
                fixed.database_name, message.database_name
            ),
        ));
    }
    Ok(fixed.clone())
}

/// The error for a stream that failed before a message could be read.
///
/// Tonic reports both an undecodable message and an HTTP/2 transport failure (a protocol
/// error, a reset connection) as `INTERNAL`. It tells them apart the same way this does: a
/// status built from a transport error carries that error as its `source`, while the statuses
/// its decoder builds for bad message bytes carry none.
fn stream_error(status: &Status, limit: usize) -> LogPoseError {
    let transport_failure = std::error::Error::source(status).is_some();
    match status.code() {
        // Tonic's decode limit; see `MessageLimitLayer`.
        Code::OutOfRange if !transport_failure => message_too_large(limit),
        Code::Internal if !transport_failure => LogPoseError::invalid_argument(format!(
            "malformed bulk upsert message: {}",
            status.message()
        )),
        _ => LogPoseError::unavailable(format!(
            "bulk upsert stream was interrupted: {}",
            status.message()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestServer, put, small_grpc_limit, text};
    use logpose_config::LimitsConfig;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic_types::StatusExt;

    /// A batch of `ids`; an empty collection name leaves the target to the first batch.
    fn batch(collection: &str, ids: &[&str]) -> BulkUpsertRecordsRequest {
        BulkUpsertRecordsRequest {
            database_name: if collection.is_empty() {
                String::new()
            } else {
                "default".to_owned()
            },
            collection_name: collection.to_owned(),
            records: ids.iter().map(|id| put(id)).collect(),
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
    async fn bulk_upsert_commits_every_batch_and_returns_a_summary() {
        let mut server = TestServer::start("bulk-happy", LimitsConfig::default()).await;
        server.create_collection("docs").await;

        let reply = server
            .client
            .bulk_upsert_records(tokio_stream::iter([
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
    async fn bulk_upsert_stops_at_the_first_failed_batch_and_reports_committed_progress() {
        let mut server = TestServer::start("bulk-mid-failure", LimitsConfig::default()).await;
        server.create_collection("docs").await;
        let mut bad = batch("docs", &["c"]);
        bad.records[0].vectors.insert(
            "vector".to_owned(),
            proto::Vector {
                values: vec![1.0, 2.0, 3.0],
            },
        );

        let status = server
            .client
            .bulk_upsert_records(tokio_stream::iter([
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
        assert_eq!(violations[0].field, "records[0].vector");
        // The failed batch is atomic and nothing after it is applied.
        assert_eq!(server.live_records("docs").await, 2);
    }

    #[tokio::test]
    async fn bulk_upsert_rejects_an_oversized_batch_as_resource_exhausted() {
        let mut server = TestServer::start("bulk-oversize", small_grpc_limit(1024)).await;
        server.create_collection("docs").await;
        let mut oversized = batch("docs", &["big"]);
        oversized.records[0]
            .fields
            .insert("blob".to_owned(), text("x".repeat(4096)));

        let status = server
            .client
            .bulk_upsert_records(tokio_stream::iter([batch("docs", &["a"]), oversized]))
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
    async fn bulk_upsert_keeps_committed_batches_when_the_client_disconnects() {
        let mut server = TestServer::start("bulk-disconnect", LimitsConfig::default()).await;
        server.create_collection("docs").await;
        let (sender, receiver) = mpsc::channel(4);
        let mut client = server.client.clone();
        let call = tokio::spawn(async move {
            client
                .bulk_upsert_records(ReceiverStream::new(receiver))
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
            .bulk_upsert_records(tokio_stream::iter([batch("docs", &["c"])]))
            .await
            .expect("a new stream should succeed after a disconnect")
            .into_inner();
        assert_eq!(reply.committed_batches, 1);
        assert_eq!(server.live_records("docs").await, 3);
    }

    #[tokio::test]
    async fn bulk_upsert_starts_no_batch_after_the_client_cancels() {
        let mut server = TestServer::start("bulk-cancel-buffered", LimitsConfig::default()).await;
        server.create_collection("docs").await;
        let (sender, receiver) = mpsc::channel(4);
        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = CancelOnDrop(Arc::clone(&cancelled));
        let task = tokio::spawn(ingest(
            Arc::clone(&server.state),
            RequestAuth::default(),
            ReceiverStream::new(receiver),
            cancelled,
        ));
        sender
            .send(Ok(batch("docs", &["a", "b"])))
            .await
            .expect("first batch should be sent");
        let mut waited = 0;
        while server.live_records("docs").await < 2 {
            waited += 1;
            assert!(waited < 250, "the first batch was never committed");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // The client cancels; tonic drops the handler, and so the guard. A batch the client
        // sent before cancelling can still arrive from the HTTP/2 stream buffer.
        drop(guard);
        sender
            .send(Ok(batch("docs", &["late"])))
            .await
            .expect("buffered batch should be delivered");
        drop(sender);

        let error = task
            .await
            .expect("ingest task should finish")
            .expect_err("a cancelled stream should stop");
        assert!(matches!(
            error,
            LogPoseError::BulkBatchFailed {
                batch_index: 1,
                committed_batches: 1,
                ..
            }
        ));
        assert_eq!(server.live_records("docs").await, 2);
    }

    /// A transport failure as tonic reports it: a status whose source chain holds the error.
    #[derive(Debug)]
    struct TransportFailure(Status);

    impl std::fmt::Display for TransportFailure {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("connection error")
        }
    }

    impl std::error::Error for TransportFailure {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn stream_errors_separate_bad_messages_from_transport_failures() {
        // What tonic's decoder returns for bytes that are not a valid message.
        let malformed = stream_error(&Status::internal("failed to decode Protobuf message"), 64);
        assert_eq!(malformed.code(), logpose_types::ErrorCode::InvalidArgument);
        assert!(
            malformed
                .to_string()
                .contains("malformed bulk upsert message")
        );

        // What tonic returns when the HTTP/2 stream itself fails.
        let transport = Status::from_error(Box::new(TransportFailure(Status::internal(
            "h2 protocol error: stream error received: unexpected internal error",
        ))));
        assert_eq!(transport.code(), Code::Internal);
        let interrupted = stream_error(&transport, 64);
        assert_eq!(interrupted.code(), logpose_types::ErrorCode::Unavailable);
        assert!(
            interrupted
                .to_string()
                .contains("bulk upsert stream was interrupted")
        );

        let too_large = stream_error(&Status::out_of_range("message too large"), 64);
        assert_eq!(too_large.reason(), "TOO_LARGE");
    }

    #[tokio::test]
    async fn bulk_upsert_rejects_an_empty_stream() {
        let mut server = TestServer::start("bulk-empty", LimitsConfig::default()).await;
        let status = server
            .client
            .bulk_upsert_records(tokio_stream::iter(Vec::<BulkUpsertRecordsRequest>::new()))
            .await
            .expect_err("an empty stream should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(reason(&status).as_deref(), Some("INVALID_ARGUMENT"));
    }

    #[tokio::test]
    async fn bulk_upsert_rejects_batches_that_switch_collections() {
        let mut server = TestServer::start("bulk-switch", LimitsConfig::default()).await;
        server.create_collection("docs").await;
        server.create_collection("other").await;

        let status = server
            .client
            .bulk_upsert_records(tokio_stream::iter([
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
    async fn bulk_upsert_requires_the_first_batch_to_name_the_collection() {
        let mut server = TestServer::start("bulk-unnamed", LimitsConfig::default()).await;
        let status = server
            .client
            .bulk_upsert_records(tokio_stream::iter([batch("", &["a"])]))
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
