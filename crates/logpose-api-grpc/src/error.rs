//! gRPC status mapping and the request-size layer.
//!
//! [`grpc_code`] is the only place that maps a [`LogPoseError`] to a gRPC status code, and
//! [`status_from_error`] the only place that builds a [`Status`] from one.

use logpose_types::{ErrorCode, LogPoseError};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tonic::{
    Code, Response, Status,
    metadata::{MetadataMap, MetadataValue},
};
use tonic_types::{ErrorDetails, StatusExt};
use tower::{Layer, Service};

/// `ErrorInfo.domain` of every LogPose error.
pub const ERROR_DOMAIN: &str = "logpose";

/// ASCII trailer carrying the retry hint in milliseconds, for clients that do not decode
/// `grpc-status-details-bin`.
pub const RETRY_AFTER_METADATA_KEY: &str = "retry-after-ms";

/// The gRPC status code for `error`.
#[must_use]
pub fn grpc_code(error: &LogPoseError) -> Code {
    code_to_grpc(error.code())
}

/// The gRPC status code of the same name as `code`.
#[must_use]
pub const fn code_to_grpc(code: ErrorCode) -> Code {
    match code {
        ErrorCode::InvalidArgument => Code::InvalidArgument,
        ErrorCode::NotFound => Code::NotFound,
        ErrorCode::AlreadyExists => Code::AlreadyExists,
        ErrorCode::FailedPrecondition => Code::FailedPrecondition,
        ErrorCode::Unauthenticated => Code::Unauthenticated,
        ErrorCode::PermissionDenied => Code::PermissionDenied,
        ErrorCode::ResourceExhausted => Code::ResourceExhausted,
        ErrorCode::Unavailable => Code::Unavailable,
        ErrorCode::DataLoss => Code::DataLoss,
        ErrorCode::Internal => Code::Internal,
    }
}

/// The LogPose code a gRPC status code stands for, the inverse of [`code_to_grpc`].
///
/// `None` for the codes LogPose never sends itself (see [`ErrorCode`]), which a client only
/// sees from its own transport, a proxy, or tonic.
#[must_use]
pub const fn code_from_grpc(code: Code) -> Option<ErrorCode> {
    match code {
        Code::InvalidArgument => Some(ErrorCode::InvalidArgument),
        Code::NotFound => Some(ErrorCode::NotFound),
        Code::AlreadyExists => Some(ErrorCode::AlreadyExists),
        Code::FailedPrecondition => Some(ErrorCode::FailedPrecondition),
        Code::Unauthenticated => Some(ErrorCode::Unauthenticated),
        Code::PermissionDenied => Some(ErrorCode::PermissionDenied),
        Code::ResourceExhausted => Some(ErrorCode::ResourceExhausted),
        Code::Unavailable => Some(ErrorCode::Unavailable),
        Code::DataLoss => Some(ErrorCode::DataLoss),
        Code::Internal => Some(ErrorCode::Internal),
        Code::Ok
        | Code::Cancelled
        | Code::Unknown
        | Code::DeadlineExceeded
        | Code::Aborted
        | Code::OutOfRange
        | Code::Unimplemented => None,
    }
}

/// The status for `error`: its code, its message, rich details (`ErrorInfo`, `BadRequest`,
/// `RetryInfo`), and a `retry-after-ms` trailer when it carries a retry hint.
#[must_use]
pub fn status_from_error(error: &LogPoseError) -> Status {
    let details = error.details();
    let mut rich = ErrorDetails::new();
    rich.set_error_info(
        details.reason,
        ERROR_DOMAIN,
        details.metadata.into_iter().collect::<HashMap<_, _>>(),
    );
    for violation in details.field_violations {
        rich.add_bad_request_violation(violation.field, violation.description);
    }
    let mut metadata = MetadataMap::new();
    if let Some(millis) = details.retry_after_ms {
        rich.set_retry_info(Some(Duration::from_millis(millis)));
        metadata.insert(RETRY_AFTER_METADATA_KEY, MetadataValue::from(millis));
    }
    Status::with_error_details_and_metadata(grpc_code(error), error.to_string(), rich, metadata)
}

/// Turn a handler result into a tonic result.
pub(crate) fn respond<T>(result: Result<T, LogPoseError>) -> Result<Response<T>, Status> {
    result
        .map(Response::new)
        .map_err(|error| status_from_error(&error))
}

pub(crate) fn unauthenticated(message: &str) -> LogPoseError {
    LogPoseError::Unauthenticated {
        message: message.to_owned(),
    }
}

/// The error for a request message above the configured decode limit.
pub(crate) fn message_too_large(limit: usize) -> LogPoseError {
    LogPoseError::TooLarge {
        what: "gRPC request message".to_owned(),
        size: None,
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
    }
}

/// Rewrites tonic's decode-limit rejection into a typed `TOO_LARGE` error.
///
/// Tonic rejects a request message above `max_decoding_message_size` with `OUT_OF_RANGE`
/// before any handler runs. LogPose never returns `OUT_OF_RANGE` itself (see
/// [`ErrorCode`]), so this layer turns every trailers-only `OUT_OF_RANGE` response into
/// `RESOURCE_EXHAUSTED` with reason `TOO_LARGE`, like the REST 413.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MessageLimitLayer {
    limit: usize,
}

impl MessageLimitLayer {
    pub(crate) fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl<S> Layer<S> for MessageLimitLayer {
    type Service = MessageLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MessageLimit {
            inner,
            limit: self.limit,
        }
    }
}

/// See [`MessageLimitLayer`].
#[derive(Clone, Debug)]
pub(crate) struct MessageLimit<S> {
    inner: S,
    limit: usize,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for MessageLimit<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
    ResBody: Default,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let limit = self.limit;
        let response = self.inner.call(request);
        Box::pin(async move {
            let response = response.await?;
            let out_of_range = Status::from_header_map(response.headers())
                .is_some_and(|status| status.code() == Code::OutOfRange);
            if out_of_range {
                return Ok(status_from_error(&message_too_large(limit)).into_http());
            }
            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_types::error::fixtures::{one_of_each_variant, variant_name};

    /// The documented gRPC code of every variant (see `docs/src/api-overview.md`).
    const EXPECTED: [(&str, Code); 25] = [
        ("InvalidArgument", Code::InvalidArgument),
        ("DimensionMismatch", Code::InvalidArgument),
        ("TooLarge", Code::ResourceExhausted),
        ("InvalidConfig", Code::InvalidArgument),
        ("NotFound", Code::NotFound),
        ("AlreadyExists", Code::AlreadyExists),
        ("FailedPrecondition", Code::FailedPrecondition),
        ("WrongNodeRole", Code::FailedPrecondition),
        ("ReconciliationRequired", Code::FailedPrecondition),
        ("StorageRootLocked", Code::FailedPrecondition),
        ("SnapshotExpired", Code::FailedPrecondition),
        ("TooManySnapshots", Code::ResourceExhausted),
        ("WriteStalled", Code::Unavailable),
        ("Unauthenticated", Code::Unauthenticated),
        ("PermissionDenied", Code::PermissionDenied),
        ("NotOwner", Code::Unavailable),
        ("NotLeader", Code::Unavailable),
        ("ReadBarrierNotSatisfied", Code::FailedPrecondition),
        ("Unavailable", Code::Unavailable),
        ("Corrupt", Code::DataLoss),
        ("CollectionPoisoned", Code::FailedPrecondition),
        // The fixture's WAL failure was rolled back (`NotApplied`).
        ("WalWriteFailed", Code::Unavailable),
        ("Io", Code::Internal),
        // The fixture's bulk failure wraps a missing collection.
        ("BulkBatchFailed", Code::NotFound),
        ("Internal", Code::Internal),
    ];

    #[test]
    fn every_error_variant_maps_to_its_documented_grpc_status_and_details() {
        let errors = one_of_each_variant();
        assert_eq!(errors.len(), EXPECTED.len());
        for error in errors {
            let name = variant_name(&error);
            let (_, code) = EXPECTED
                .iter()
                .find(|(variant, _)| *variant == name)
                .expect("every variant has an expected code");
            let status = status_from_error(&error);
            assert_eq!(status.code(), *code, "{name}");
            assert_eq!(status.message(), error.to_string(), "{name}");

            let details = status.check_error_details().expect("status details decode");
            let info = details.error_info().expect("every status has ErrorInfo");
            assert_eq!(info.reason, error.reason(), "{name}");
            assert_eq!(info.domain, ERROR_DOMAIN, "{name}");
            let expected = error.details();
            for (key, value) in &expected.metadata {
                assert_eq!(info.metadata.get(key), Some(value), "{name}: {key}");
            }
            assert_eq!(
                details
                    .bad_request()
                    .map(|bad_request| bad_request.field_violations.len())
                    .unwrap_or_default(),
                expected.field_violations.len(),
                "{name}"
            );
            let retry_delay = details.retry_info().and_then(|retry| retry.retry_delay);
            assert_eq!(retry_delay, error.retry_after(), "{name}");
            assert_eq!(
                status
                    .metadata()
                    .get(RETRY_AFTER_METADATA_KEY)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u128>().ok()),
                error.retry_after().map(|delay| delay.as_millis()),
                "{name}"
            );
        }
    }

    #[test]
    fn grpc_codes_map_back_to_the_logpose_code_of_the_same_name() {
        for code in ErrorCode::ALL {
            assert_eq!(code_from_grpc(code_to_grpc(code)), Some(code), "{code}");
        }
        for foreign in [
            Code::Ok,
            Code::Cancelled,
            Code::Unknown,
            Code::DeadlineExceeded,
            Code::Aborted,
            Code::OutOfRange,
            Code::Unimplemented,
        ] {
            assert_eq!(code_from_grpc(foreign), None, "{foreign:?}");
        }
    }

    #[test]
    fn not_owner_is_unavailable_with_the_owner_hint() {
        let status = status_from_error(&LogPoseError::NotOwner {
            collection: "default/docs".to_owned(),
            node: "node-a".to_owned(),
            owner_node: Some("node-b".to_owned()),
        });
        assert_eq!(status.code(), Code::Unavailable);
        let info = status
            .get_details_error_info()
            .expect("error info is attached");
        assert_eq!(info.reason, "NOT_OWNER");
        assert_eq!(info.metadata["owner_node"], "node-b");
        assert_eq!(
            status
                .metadata()
                .get(RETRY_AFTER_METADATA_KEY)
                .and_then(|value| value.to_str().ok()),
            Some("1000")
        );
    }

    #[tokio::test]
    async fn unary_requests_above_the_message_limit_are_resource_exhausted() {
        use crate::{
            proto,
            test_support::{TestServer, put, small_grpc_limit, text},
        };

        let mut server = TestServer::start("grpc-message-limit", small_grpc_limit(1024)).await;
        server.create_collection("docs").await;

        // Under the limit: accepted.
        server
            .client
            .upsert_records(proto::UpsertRecordsRequest {
                database_name: "default".to_owned(),
                collection_name: "docs".to_owned(),
                records: vec![put("a")],
            })
            .await
            .expect("a small write should succeed");

        // Over the limit: rejected before the handler runs, with a typed error.
        let mut oversized = put("b");
        oversized
            .fields
            .insert("blob".to_owned(), text("x".repeat(4096)));
        let status = server
            .client
            .upsert_records(proto::UpsertRecordsRequest {
                database_name: "default".to_owned(),
                collection_name: "docs".to_owned(),
                records: vec![oversized],
            })
            .await
            .expect_err("an oversized write should be rejected");

        assert_eq!(status.code(), Code::ResourceExhausted);
        let info = status
            .get_details_error_info()
            .expect("error info is attached");
        assert_eq!(info.reason, "TOO_LARGE");
        assert_eq!(info.metadata["limit_bytes"], "1024");
        assert_eq!(server.live_records("docs").await, 1);
    }

    #[tokio::test]
    async fn the_message_limit_layer_leaves_other_statuses_alone() {
        use crate::{proto, test_support::TestServer};
        use logpose_config::LimitsConfig;

        let mut server = TestServer::start("grpc-limit-passthrough", LimitsConfig::default()).await;
        let status = server
            .client
            .get_collection(proto::GetCollectionRequest {
                collection_name: "missing".to_owned(),
                database_name: "default".to_owned(),
            })
            .await
            .expect_err("a missing collection should be reported");
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(
            status
                .get_details_error_info()
                .map(|info| info.reason)
                .as_deref(),
            Some("RESOURCE_NOT_FOUND")
        );
        assert!(!server.address.is_empty());
    }

    #[test]
    fn wal_write_failures_depend_on_whether_the_write_can_reappear() {
        let failed = |outcome| LogPoseError::WalWriteFailed {
            collection: "default/docs".to_owned(),
            outcome,
            reason: "fsync failed".to_owned(),
        };
        let status = status_from_error(&failed(logpose_types::WriteOutcome::NotApplied));
        assert_eq!(status.code(), Code::Unavailable);
        let info = status
            .get_details_error_info()
            .expect("the status carries ErrorInfo");
        assert_eq!(info.reason, "WAL_WRITE_FAILED");
        assert_eq!(
            info.metadata.get("outcome").map(String::as_str),
            Some("not_applied")
        );
        for fenced in [true, false] {
            let unknown = failed(logpose_types::WriteOutcome::Unknown { fenced });
            assert_eq!(grpc_code(&unknown), Code::Internal);
        }
    }
}
