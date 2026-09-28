//! Typed client errors decoded from gRPC statuses.
//!
//! The server sends every LogPose error as a gRPC status with rich details: a
//! `google.rpc.ErrorInfo` (the stable reason, domain `logpose`, and string metadata), a
//! `google.rpc.BadRequest` (field violations), and a `google.rpc.RetryInfo` (the retry hint),
//! plus a `retry-after-ms` trailer. [`ServerError::from_status`] decodes all of it back into
//! typed values, so callers match on [`ErrorReason`] instead of parsing messages.

use logpose_api_grpc::{ERROR_DOMAIN, RETRY_AFTER_METADATA_KEY, code_from_grpc};
use logpose_types::{ErrorCode, ErrorReason, FieldViolation, LogPoseError};
use std::{collections::BTreeMap, fmt, time::Duration};
use thiserror::Error;
use tonic::{Code, Status};
use tonic_types::StatusExt;

/// Client-scoped result type.
pub type Result<T> = std::result::Result<T, ClientError>;

/// Errors returned by the gRPC-backed client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// gRPC transport bootstrap failed.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// The server, or something between it and the client, answered with an error status.
    #[error(transparent)]
    Server(Box<ServerError>),
    /// The caller supplied an invalid client-side request.
    #[error("{0}")]
    InvalidRequest(String),
    /// The server returned an invalid or incomplete payload.
    #[error("{0}")]
    InvalidResponse(String),
    /// The caller supplied an invalid bearer token for client transport metadata.
    #[error("{0}")]
    InvalidAuthToken(String),
    /// The server returned malformed JSON payloads.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl ClientError {
    /// The decoded server error, when the server answered with an error status.
    #[must_use]
    pub fn server_error(&self) -> Option<&ServerError> {
        match self {
            Self::Server(error) => Some(error),
            _ => None,
        }
    }

    /// The LogPose reason of a server error this client knows.
    #[must_use]
    pub fn reason(&self) -> Option<ErrorReason> {
        self.server_error().and_then(ServerError::reason)
    }

    /// The raw gRPC status of a server error.
    #[must_use]
    pub fn status(&self) -> Option<&Status> {
        self.server_error().map(ServerError::status)
    }
}

impl From<Status> for ClientError {
    fn from(status: Status) -> Self {
        Self::Server(Box::new(ServerError::from_status(status)))
    }
}

impl From<ServerError> for ClientError {
    fn from(error: ServerError) -> Self {
        Self::Server(Box::new(error))
    }
}

impl From<LogPoseError> for ClientError {
    fn from(error: LogPoseError) -> Self {
        Self::InvalidResponse(error.to_string())
    }
}

/// What kind of error status the server sent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerErrorKind {
    /// A LogPose error: a reason this client knows, in the `logpose` domain, with a status code
    /// LogPose uses.
    LogPose {
        /// The canonical code.
        code: ErrorCode,
        /// The stable reason.
        reason: ErrorReason,
    },
    /// Any other status: a reason this client does not know (from a newer server), an
    /// `ErrorInfo` of another domain, or no `ErrorInfo` at all (from tonic, the transport, or a
    /// proxy).
    Unknown {
        /// The gRPC status code.
        code: Code,
        /// The raw `ErrorInfo` reason, when the status carries one.
        reason: Option<String>,
    },
}

/// An error status decoded into its typed LogPose details.
///
/// The raw [`Status`] stays available through [`status`](Self::status).
#[derive(Clone, Debug)]
pub struct ServerError {
    kind: ServerErrorKind,
    metadata: BTreeMap<String, String>,
    field_violations: Vec<FieldViolation>,
    retry_after: Option<Duration>,
    status: Status,
}

impl ServerError {
    /// Decode `status`.
    ///
    /// The retry hint comes from `RetryInfo`, or from the `retry-after-ms` trailer when the
    /// status has no `RetryInfo`. Details that fail to decode are treated as absent.
    #[must_use]
    pub fn from_status(status: Status) -> Self {
        let details = status.get_error_details();
        let info = details.error_info();
        let raw_reason = info.map(|info| info.reason.clone());
        let known_reason = info
            .filter(|info| info.domain == ERROR_DOMAIN)
            .and_then(|info| ErrorReason::parse(&info.reason));
        let kind = match (code_from_grpc(status.code()), known_reason) {
            (Some(code), Some(reason)) => ServerErrorKind::LogPose { code, reason },
            _ => ServerErrorKind::Unknown {
                code: status.code(),
                reason: raw_reason,
            },
        };
        let metadata = info
            .map(|info| info.metadata.clone().into_iter().collect())
            .unwrap_or_default();
        let field_violations = details
            .bad_request()
            .map(|bad_request| {
                bad_request
                    .field_violations
                    .iter()
                    .map(|violation| FieldViolation {
                        field: violation.field.clone(),
                        description: violation.description.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let retry_after = details
            .retry_info()
            .and_then(|retry| retry.retry_delay)
            .or_else(|| retry_after_trailer(&status));
        Self {
            kind,
            metadata,
            field_violations,
            retry_after,
            status,
        }
    }

    /// What kind of error this is.
    #[must_use]
    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    /// The gRPC status code.
    #[must_use]
    pub fn code(&self) -> Code {
        self.status.code()
    }

    /// The LogPose code, for a LogPose error.
    #[must_use]
    pub fn error_code(&self) -> Option<ErrorCode> {
        match &self.kind {
            ServerErrorKind::LogPose { code, .. } => Some(*code),
            ServerErrorKind::Unknown { .. } => None,
        }
    }

    /// The LogPose reason, for a LogPose error.
    #[must_use]
    pub fn reason(&self) -> Option<ErrorReason> {
        match &self.kind {
            ServerErrorKind::LogPose { reason, .. } => Some(*reason),
            ServerErrorKind::Unknown { .. } => None,
        }
    }

    /// The reason as sent on the wire, known or not.
    #[must_use]
    pub fn reason_str(&self) -> Option<&str> {
        match &self.kind {
            ServerErrorKind::LogPose { reason, .. } => Some(reason.as_str()),
            ServerErrorKind::Unknown { reason, .. } => reason.as_deref(),
        }
    }

    /// The human-readable message.
    #[must_use]
    pub fn message(&self) -> &str {
        self.status.message()
    }

    /// The error's structured fields, such as `owner_node` or `resource_name`.
    #[must_use]
    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// The invalid request fields.
    #[must_use]
    pub fn field_violations(&self) -> &[FieldViolation] {
        &self.field_violations
    }

    /// How long the server asks the client to wait before retrying.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Whether the server marked this error retryable: it carries a retry hint and its code is
    /// `UNAVAILABLE` or `RESOURCE_EXHAUSTED`.
    ///
    /// Every other code, such as `INVALID_ARGUMENT` or `FAILED_PRECONDITION`, is never
    /// retryable, even with a hint.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.retry_after.is_some()
            && matches!(self.code(), Code::Unavailable | Code::ResourceExhausted)
    }

    /// The node to send the request to instead: `owner_node` of a `NOT_OWNER` error or
    /// `leader_node` of a `NOT_LEADER` error, when the server named one.
    #[must_use]
    pub fn redirect_node(&self) -> Option<&str> {
        let key = match self.reason()? {
            ErrorReason::NotOwner => "owner_node",
            ErrorReason::NotLeader => "leader_node",
            _ => return None,
        };
        self.metadata
            .get(key)
            .map(String::as_str)
            .filter(|node| !node.is_empty())
    }

    /// The raw status.
    #[must_use]
    pub fn status(&self) -> &Status {
        &self.status
    }

    /// Take the raw status.
    #[must_use]
    pub fn into_status(self) -> Status {
        self.status
    }
}

/// `REASON: message`, or `CODE: message` when the status carries no reason.
impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = self
            .reason_str()
            .unwrap_or_else(|| grpc_code_name(self.code()));
        if self.message().is_empty() {
            formatter.write_str(label)
        } else {
            write!(formatter, "{label}: {}", self.message())
        }
    }
}

impl std::error::Error for ServerError {}

fn retry_after_trailer(status: &Status) -> Option<Duration> {
    status
        .metadata()
        .get(RETRY_AFTER_METADATA_KEY)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
        .map(Duration::from_millis)
}

/// The canonical name of a gRPC status code, such as `DEADLINE_EXCEEDED`.
#[must_use]
pub fn grpc_code_name(code: Code) -> &'static str {
    match code_from_grpc(code) {
        Some(code) => code.as_str(),
        None => match code {
            Code::Ok => "OK",
            Code::Cancelled => "CANCELLED",
            Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Code::Aborted => "ABORTED",
            Code::OutOfRange => "OUT_OF_RANGE",
            Code::Unimplemented => "UNIMPLEMENTED",
            _ => "UNKNOWN",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_api_grpc::status_from_error;
    use logpose_types::error::fixtures::{one_of_each_variant, variant_name};
    use tonic::metadata::{MetadataMap, MetadataValue};
    use tonic_types::ErrorDetails;

    #[test]
    fn every_server_error_variant_decodes_back_to_its_reason_code_and_details() {
        let errors = one_of_each_variant();
        assert!(!errors.is_empty());
        for error in errors {
            let name = variant_name(&error);
            let expected = error.details();
            let decoded = ServerError::from_status(status_from_error(&error));

            assert_eq!(
                decoded.kind(),
                &ServerErrorKind::LogPose {
                    code: error.code(),
                    reason: error.error_reason(),
                },
                "{name}"
            );
            assert_eq!(decoded.reason_str(), Some(error.reason()), "{name}");
            assert_eq!(decoded.error_code(), Some(error.code()), "{name}");
            assert_eq!(decoded.message(), error.to_string(), "{name}");
            assert_eq!(decoded.metadata(), &expected.metadata, "{name}");
            assert_eq!(
                decoded.field_violations(),
                expected.field_violations.as_slice(),
                "{name}"
            );
            assert_eq!(decoded.retry_after(), error.retry_after(), "{name}");
        }
    }

    #[test]
    fn only_unavailable_or_exhausted_errors_with_a_hint_are_retryable() {
        for error in one_of_each_variant() {
            let name = variant_name(&error);
            let decoded = ServerError::from_status(status_from_error(&error));
            let expected = error.retry_after().is_some()
                && matches!(
                    error.code(),
                    ErrorCode::Unavailable | ErrorCode::ResourceExhausted
                );
            assert_eq!(decoded.is_retryable(), expected, "{name}");
        }
        let hinted_precondition = Status::with_error_details(
            Code::FailedPrecondition,
            "no",
            ErrorDetails::with_retry_info(Some(Duration::from_millis(5))),
        );
        assert!(!ServerError::from_status(hinted_precondition).is_retryable());
        assert!(!ServerError::from_status(Status::unavailable("no hint")).is_retryable());
    }

    #[test]
    fn routing_errors_name_the_node_to_redirect_to() {
        let not_owner = ServerError::from_status(status_from_error(&LogPoseError::NotOwner {
            collection: "default/docs".to_owned(),
            node: "node-a".to_owned(),
            owner_node: Some("node-b".to_owned()),
        }));
        assert_eq!(not_owner.redirect_node(), Some("node-b"));

        let not_leader = ServerError::from_status(status_from_error(&LogPoseError::NotLeader {
            node: "node-a".to_owned(),
            leader_node: Some("node-c".to_owned()),
        }));
        assert_eq!(not_leader.redirect_node(), Some("node-c"));

        let leaderless = ServerError::from_status(status_from_error(&LogPoseError::NotLeader {
            node: "node-a".to_owned(),
            leader_node: None,
        }));
        assert_eq!(leaderless.redirect_node(), None);

        let other = ServerError::from_status(status_from_error(&LogPoseError::unavailable("x")));
        assert_eq!(other.redirect_node(), None);
    }

    #[test]
    fn unknown_reasons_decode_to_the_generic_kind_with_code_reason_and_message() {
        let mut details = ErrorDetails::new();
        details.set_error_info(
            "SHINY_NEW_REASON",
            ERROR_DOMAIN,
            [("k".to_owned(), "v".to_owned())],
        );
        let decoded = ServerError::from_status(Status::with_error_details(
            Code::Unavailable,
            "from the future",
            details,
        ));
        assert_eq!(
            decoded.kind(),
            &ServerErrorKind::Unknown {
                code: Code::Unavailable,
                reason: Some("SHINY_NEW_REASON".to_owned()),
            }
        );
        assert_eq!(decoded.reason(), None);
        assert_eq!(decoded.reason_str(), Some("SHINY_NEW_REASON"));
        assert_eq!(decoded.message(), "from the future");
        assert_eq!(decoded.metadata()["k"], "v");
        assert_eq!(decoded.to_string(), "SHINY_NEW_REASON: from the future");
    }

    #[test]
    fn known_reasons_of_another_domain_or_code_are_not_logpose_errors() {
        let mut details = ErrorDetails::new();
        details.set_error_info("NOT_OWNER", "example.com", std::collections::HashMap::new());
        let foreign = ServerError::from_status(Status::with_error_details(
            Code::Unavailable,
            "proxy says no",
            details,
        ));
        assert_eq!(foreign.reason(), None);
        assert_eq!(foreign.redirect_node(), None);

        let mut details = ErrorDetails::new();
        details.set_error_info("NOT_OWNER", ERROR_DOMAIN, std::collections::HashMap::new());
        let odd_code = ServerError::from_status(Status::with_error_details(
            Code::Aborted,
            "aborted",
            details,
        ));
        assert_eq!(odd_code.reason(), None);
    }

    #[test]
    fn plain_statuses_decode_to_the_generic_kind_named_by_their_code() {
        let decoded = ServerError::from_status(Status::deadline_exceeded("too slow"));
        assert_eq!(
            decoded.kind(),
            &ServerErrorKind::Unknown {
                code: Code::DeadlineExceeded,
                reason: None,
            }
        );
        assert!(decoded.metadata().is_empty());
        assert!(decoded.field_violations().is_empty());
        assert_eq!(decoded.to_string(), "DEADLINE_EXCEEDED: too slow");
        assert_eq!(
            ClientError::from(Status::internal("")).to_string(),
            "INTERNAL"
        );
    }

    #[test]
    fn the_retry_trailer_is_used_when_the_status_has_no_retry_info() {
        let mut metadata = MetadataMap::new();
        metadata.insert(RETRY_AFTER_METADATA_KEY, MetadataValue::from(250_u64));
        let decoded =
            ServerError::from_status(Status::with_metadata(Code::Unavailable, "busy", metadata));
        assert_eq!(decoded.retry_after(), Some(Duration::from_millis(250)));
        assert!(decoded.is_retryable());
    }

    #[test]
    fn typed_errors_display_their_reason_and_keep_the_raw_status() {
        let error = ClientError::from(status_from_error(&LogPoseError::invalid_field(
            "operations[0].id",
            "record id must not be empty",
        )));
        assert_eq!(
            error.to_string(),
            "INVALID_ARGUMENT: record id must not be empty"
        );
        assert_eq!(error.reason(), Some(ErrorReason::InvalidArgument));
        assert_eq!(
            error.status().map(Status::code),
            Some(Code::InvalidArgument)
        );
        let violations = error
            .server_error()
            .map(ServerError::field_violations)
            .unwrap_or_default();
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].field, "operations[0].id");
    }
}
