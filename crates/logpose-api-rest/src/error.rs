//! REST error responses and the extractors that turn request rejections into typed errors.
//!
//! [`http_status`] is the only place that maps a [`LogPoseError`] to an HTTP status.

use axum::{
    Json,
    extract::{FromRequest, FromRequestParts, Path, Query, Request},
    http::{HeaderValue, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use logpose_core::AppState;
use logpose_types::{ErrorCode, ErrorDetails, LogPoseError};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

/// The JSON body of every REST error response.
#[derive(Debug, Serialize)]
pub struct ErrorBody {
    /// Canonical error class, named like the gRPC status code.
    pub code: ErrorCode,
    /// Human-readable description.
    pub message: String,
    /// Structured details: reason, metadata, field violations, retry hint.
    pub details: ErrorDetails,
}

impl ErrorBody {
    /// The body for `error`.
    #[must_use]
    pub fn from_error(error: &LogPoseError) -> Self {
        Self {
            code: error.code(),
            message: error.to_string(),
            details: error.details(),
        }
    }
}

/// The HTTP status for `error`.
///
/// `RESOURCE_EXHAUSTED` is 413 for an oversized request and 429 otherwise; every other code
/// maps to one status.
#[must_use]
pub fn http_status(error: &LogPoseError) -> StatusCode {
    match error.code() {
        ErrorCode::InvalidArgument => StatusCode::BAD_REQUEST,
        ErrorCode::NotFound => StatusCode::NOT_FOUND,
        ErrorCode::AlreadyExists | ErrorCode::FailedPrecondition => StatusCode::CONFLICT,
        ErrorCode::Unauthenticated => StatusCode::UNAUTHORIZED,
        ErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
        ErrorCode::ResourceExhausted => match error {
            LogPoseError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            LogPoseError::BulkBatchFailed { source, .. }
                if matches!(**source, LogPoseError::TooLarge { .. }) =>
            {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            _ => StatusCode::TOO_MANY_REQUESTS,
        },
        ErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::DataLoss | ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// A handler error: renders as [`ErrorBody`] with the status from [`http_status`] and, when
/// the error carries a retry hint, a `Retry-After` header in whole seconds (rounded up).
#[derive(Debug)]
pub(crate) struct ApiError(pub(crate) LogPoseError);

impl From<LogPoseError> for ApiError {
    fn from(error: LogPoseError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = http_status(&self.0);
        let body = ErrorBody::from_error(&self.0);
        let retry_after = body
            .details
            .retry_after_ms
            .map(|millis| millis.div_ceil(1000).max(1));
        let mut response = (status, Json(body)).into_response();
        if let Some(seconds) = retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
        }
        response
    }
}

/// A JSON body extractor whose rejections are typed errors: an oversized body is `TOO_LARGE`
/// (HTTP 413) and anything else that fails to parse is `INVALID_ARGUMENT`.
pub(crate) struct ApiJson<T>(pub(crate) T);

impl<T> FromRequest<Arc<AppState>> for ApiJson<T>
where
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &Arc<AppState>) -> Result<Self, ApiError> {
        let declared_size = request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        match Json::<T>::from_request(request, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
                Err(ApiError(LogPoseError::TooLarge {
                    what: "REST request body".to_owned(),
                    size: declared_size,
                    limit: u64::try_from(state.config.limits.max_rest_body_bytes)
                        .unwrap_or(u64::MAX),
                }))
            }
            Err(rejection) => Err(ApiError(LogPoseError::invalid_argument(
                rejection.body_text(),
            ))),
        }
    }
}

/// A query-string extractor whose rejections are `INVALID_ARGUMENT` errors.
pub(crate) struct ApiQuery<T>(pub(crate) T);

impl<T, S> FromRequestParts<S> for ApiQuery<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(value)| Self(value))
            .map_err(|rejection| ApiError(LogPoseError::invalid_argument(rejection.body_text())))
    }
}

/// A path-parameter extractor whose rejections, such as a segment that is not valid UTF-8,
/// are `INVALID_ARGUMENT` errors.
pub(crate) struct ApiPath<T>(pub(crate) T);

impl<T, S> FromRequestParts<S> for ApiPath<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(|rejection| ApiError(LogPoseError::invalid_argument(rejection.body_text())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use logpose_types::error::fixtures::{one_of_each_variant, variant_name};
    use serde_json::Value;

    /// The documented HTTP status of every variant (see `docs/src/api-overview.md`).
    const EXPECTED: [(&str, StatusCode); 24] = [
        ("InvalidArgument", StatusCode::BAD_REQUEST),
        ("DimensionMismatch", StatusCode::BAD_REQUEST),
        ("TooLarge", StatusCode::PAYLOAD_TOO_LARGE),
        ("InvalidConfig", StatusCode::BAD_REQUEST),
        ("NotFound", StatusCode::NOT_FOUND),
        ("AlreadyExists", StatusCode::CONFLICT),
        ("FailedPrecondition", StatusCode::CONFLICT),
        ("WrongNodeRole", StatusCode::CONFLICT),
        ("ReconciliationRequired", StatusCode::CONFLICT),
        ("StorageRootLocked", StatusCode::CONFLICT),
        ("SnapshotExpired", StatusCode::CONFLICT),
        ("TooManySnapshots", StatusCode::TOO_MANY_REQUESTS),
        ("Unauthenticated", StatusCode::UNAUTHORIZED),
        ("PermissionDenied", StatusCode::FORBIDDEN),
        ("NotOwner", StatusCode::SERVICE_UNAVAILABLE),
        ("NotLeader", StatusCode::SERVICE_UNAVAILABLE),
        ("ReadBarrierNotSatisfied", StatusCode::CONFLICT),
        ("Unavailable", StatusCode::SERVICE_UNAVAILABLE),
        ("Corrupt", StatusCode::INTERNAL_SERVER_ERROR),
        ("CollectionPoisoned", StatusCode::CONFLICT),
        // The fixture's WAL failure was rolled back (`NotApplied`).
        ("WalWriteFailed", StatusCode::SERVICE_UNAVAILABLE),
        ("Io", StatusCode::INTERNAL_SERVER_ERROR),
        // The fixture's bulk failure wraps a missing collection.
        ("BulkBatchFailed", StatusCode::NOT_FOUND),
        ("Internal", StatusCode::INTERNAL_SERVER_ERROR),
    ];

    #[tokio::test]
    async fn every_error_variant_maps_to_its_documented_http_status_and_body() {
        let errors = one_of_each_variant();
        assert_eq!(errors.len(), EXPECTED.len());
        for error in errors {
            let name = variant_name(&error);
            let (_, status) = EXPECTED
                .iter()
                .find(|(variant, _)| *variant == name)
                .expect("every variant has an expected status");
            let code = error.code();
            let reason = error.reason();
            let message = error.to_string();
            let retry_after = error.retry_after();

            let response = ApiError(error).into_response();
            assert_eq!(response.status(), *status, "{name}");
            assert_eq!(
                response
                    .headers()
                    .get(header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
                retry_after.map(|delay| delay.as_secs().max(1).to_string()),
                "{name}"
            );
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("error body reads");
            let body: Value = serde_json::from_slice(&body).expect("error body is JSON");
            assert_eq!(body["code"], code.as_str(), "{name}");
            assert_eq!(body["message"], message, "{name}");
            assert_eq!(body["details"]["reason"], reason, "{name}");
            assert!(body["details"]["metadata"].is_object(), "{name}");
            assert!(body["details"]["field_violations"].is_array(), "{name}");
        }
    }

    #[test]
    fn not_owner_and_not_leader_are_service_unavailable_with_a_retry_hint() {
        for error in [
            LogPoseError::NotOwner {
                collection: "default/docs".to_owned(),
                node: "node-a".to_owned(),
                owner_node: Some("node-b".to_owned()),
            },
            LogPoseError::NotLeader {
                node: "node-a".to_owned(),
                leader_node: Some("node-b".to_owned()),
            },
        ] {
            let response = ApiError(error).into_response();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        }
    }

    #[test]
    fn bulk_failures_caused_by_size_are_payload_too_large() {
        let wrapped_size = LogPoseError::BulkBatchFailed {
            batch_index: 0,
            committed_batches: 0,
            committed_operations: 0,
            last_committed_seq_no: None,
            source: Box::new(LogPoseError::TooLarge {
                what: "gRPC message".to_owned(),
                size: None,
                limit: 1,
            }),
        };
        assert_eq!(http_status(&wrapped_size), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn wal_write_failures_depend_on_whether_the_write_can_reappear() {
        let failed = |outcome| LogPoseError::WalWriteFailed {
            collection: "default/docs".to_owned(),
            outcome,
            reason: "fsync failed".to_owned(),
        };
        let not_applied = failed(logpose_types::WriteOutcome::NotApplied);
        assert_eq!(http_status(&not_applied), StatusCode::SERVICE_UNAVAILABLE);
        let body = ErrorBody::from_error(&not_applied);
        assert_eq!(body.details.reason, "WAL_WRITE_FAILED");
        assert_eq!(body.details.metadata["outcome"], "not_applied");
        for fenced in [true, false] {
            let unknown = failed(logpose_types::WriteOutcome::Unknown { fenced });
            assert_eq!(http_status(&unknown), StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}
