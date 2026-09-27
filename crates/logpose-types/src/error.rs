//! Typed errors shared by every LogPose layer, from storage to the wire.
//!
//! [`LogPoseError`] is the only error type that crosses crate boundaries. Every variant carries
//! structured fields and knows three things about itself:
//!
//! - its canonical [`ErrorCode`] ([`LogPoseError::code`]), a transport-neutral class whose
//!   names follow the gRPC status codes;
//! - a stable machine-readable `reason` ([`LogPoseError::reason`]) such as `NOT_OWNER`;
//! - its [`ErrorDetails`] ([`LogPoseError::details`]): the reason, string metadata taken from
//!   the variant's fields, field violations with request field paths, and a retry hint.
//!
//! Each transport maps an error to its wire status in exactly one place: the gRPC crate's
//! `status_from_error` and the REST crate's `ApiError`. Nothing classifies an error by its
//! message text.

use crate::NodeRole;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, path::PathBuf, sync::Arc, time::Duration};
use thiserror::Error;

/// Retry hint returned with routing errors ([`LogPoseError::NotOwner`],
/// [`LogPoseError::NotLeader`]) and with transient metadata unavailability.
pub const ROUTING_RETRY_AFTER: Duration = Duration::from_secs(1);

/// Canonical, transport-neutral error class.
///
/// The names and meanings follow the gRPC status codes of the same name. LogPose never uses
/// `OUT_OF_RANGE`, `ABORTED`, `CANCELLED`, `DEADLINE_EXCEEDED`, `UNIMPLEMENTED`, or `UNKNOWN`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    /// The request is malformed or violates a validation rule. Do not retry unchanged.
    InvalidArgument,
    /// A named resource does not exist.
    NotFound,
    /// A resource the request creates already exists.
    AlreadyExists,
    /// The system is not in the state the request needs; it will not change by retrying.
    FailedPrecondition,
    /// The request carries no valid credentials.
    Unauthenticated,
    /// The caller is authenticated but not allowed to do this.
    PermissionDenied,
    /// The request exceeds a size limit or quota.
    ResourceExhausted,
    /// The request cannot be served here right now; retry, possibly on another node.
    Unavailable,
    /// Stored data is corrupt.
    DataLoss,
    /// An unexpected server-side failure.
    Internal,
}

impl ErrorCode {
    /// Every code, in declaration order.
    pub const ALL: [Self; 10] = [
        Self::InvalidArgument,
        Self::NotFound,
        Self::AlreadyExists,
        Self::FailedPrecondition,
        Self::Unauthenticated,
        Self::PermissionDenied,
        Self::ResourceExhausted,
        Self::Unavailable,
        Self::DataLoss,
        Self::Internal,
    ];

    /// The wire name, identical to the gRPC status code name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::Unavailable => "UNAVAILABLE",
            Self::DataLoss => "DATA_LOSS",
            Self::Internal => "INTERNAL",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The kind of resource a [`LogPoseError::NotFound`] or [`LogPoseError::AlreadyExists`] names.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResourceKind {
    /// A database.
    Database,
    /// A collection.
    Collection,
    /// The authoritative placement record of a collection in the metadata store.
    CollectionAssignment,
    /// An authentication principal.
    Principal,
    /// A database access policy.
    DatabasePolicy,
    /// A segment of a collection.
    Segment,
    /// A REST route.
    Route,
}

impl ResourceKind {
    /// Stable machine name, reported as `resource_type` in error metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::Collection => "collection",
            Self::CollectionAssignment => "collection_assignment",
            Self::Principal => "principal",
            Self::DatabasePolicy => "database_policy",
            Self::Segment => "segment",
            Self::Route => "route",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::Collection => "collection",
            Self::CollectionAssignment => "collection assignment",
            Self::Principal => "principal",
            Self::DatabasePolicy => "database access policy",
            Self::Segment => "segment",
            Self::Route => "route",
        }
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

/// Which stored structure a [`LogPoseError::Corrupt`] error found damaged.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CorruptionKind {
    /// A write-ahead log file.
    Wal,
    /// A segment file.
    Segment,
    /// A manifest or the `CURRENT` pointer.
    Manifest,
    /// An index sidecar file.
    Index,
    /// A descriptor file: collection, database, principal, or policy.
    Descriptor,
    /// A record in the distributed metadata store.
    Metadata,
}

impl CorruptionKind {
    /// Stable machine name, reported as `corruption_kind` in error metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::Segment => "segment",
            Self::Manifest => "manifest",
            Self::Index => "index",
            Self::Descriptor => "descriptor",
            Self::Metadata => "metadata",
        }
    }
}

impl fmt::Display for CorruptionKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What a failed WAL group append means for the writes it held.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteOutcome {
    /// The WAL was truncated back to the last synced group and the truncation was synced: the
    /// writes are absent after any crash and will never be replayed.
    NotApplied,
    /// The rollback failed, so the writes may or may not be replayed later. Clients must treat
    /// this like a timeout.
    Unknown {
        /// Whether the `FSYNC_FAILED` fence marker was written durably. When it was not, a
        /// later open in the same boot cannot notice the hazard, so the process must stop.
        fenced: bool,
    },
}

impl WriteOutcome {
    /// Stable machine name, reported as `outcome` in error metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplied => "not_applied",
            Self::Unknown { fenced: true } => "unknown_fenced",
            Self::Unknown { fenced: false } => "unknown_unfenced",
        }
    }
}

impl fmt::Display for WriteOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotApplied => formatter.write_str("not applied"),
            Self::Unknown { fenced: true } => formatter.write_str("outcome unknown, WAL fenced"),
            Self::Unknown { fenced: false } => {
                formatter.write_str("outcome unknown, WAL not fenced")
            }
        }
    }
}

/// One invalid request field.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FieldViolation {
    /// Path of the field in the request, such as `operations[2].vector`.
    pub field: String,
    /// What is wrong with it.
    pub description: String,
}

/// Transport-neutral structured error details.
///
/// REST serializes this as the `details` object of the error body. gRPC sends it as
/// `google.rpc.ErrorInfo` (reason and metadata), `google.rpc.BadRequest` (field violations), and
/// `google.rpc.RetryInfo` (retry hint).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ErrorDetails {
    /// Stable machine-readable reason, such as `NOT_OWNER`.
    pub reason: &'static str,
    /// Structured fields of the error as strings, such as `owner_node`.
    pub metadata: BTreeMap<String, String>,
    /// Invalid request fields, for `INVALID_ARGUMENT` errors that name one.
    pub field_violations: Vec<FieldViolation>,
    /// How long to wait before retrying, in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// Top-level workspace error.
///
/// Variants are grouped by who has to act: the caller (request errors), the caller or an
/// operator (resource state), a router (routing and availability), or an operator (storage
/// health). [`Internal`](Self::Internal) is only for failures nothing else describes.
#[derive(Clone, Debug, Error)]
pub enum LogPoseError {
    // ----- Request errors: the caller must change the request. -----
    /// The request is malformed or violates a validation rule.
    #[error("{message}")]
    InvalidArgument {
        /// Path of the offending request field, such as `operations[2].metadata`, when known.
        field: Option<String>,
        /// What is wrong.
        message: String,
    },
    /// A vector has the wrong number of dimensions.
    #[error(
        "{} expected {expected} dimensions but found {actual}",
        record_id.as_ref().map_or_else(|| field.clone(), |id| format!("record '{id}'"))
    )]
    DimensionMismatch {
        /// Path of the vector field in the request, such as `operations[2].vector`.
        field: String,
        /// The record the vector belongs to, for writes.
        record_id: Option<String>,
        /// Dimensions the collection declares.
        expected: usize,
        /// Dimensions supplied.
        actual: usize,
    },
    /// A request, message, or batch exceeds a size limit.
    #[error(
        "{what}{} exceeds the {limit}-byte limit",
        size.map(|size| format!(" of {size} bytes")).unwrap_or_default()
    )]
    TooLarge {
        /// What was too large, such as `REST request body`.
        what: String,
        /// Its size in bytes, when known.
        size: Option<u64>,
        /// The limit in bytes.
        limit: u64,
    },
    /// Server configuration is invalid.
    #[error("{message}")]
    InvalidConfig {
        /// What is wrong.
        message: String,
    },

    // ----- Resource state: the named resource is missing, present, or in the wrong state. -----
    /// A named resource does not exist.
    #[error("{resource} '{name}' does not exist")]
    NotFound {
        /// The kind of resource.
        resource: ResourceKind,
        /// Its name.
        name: String,
    },
    /// A resource the request creates already exists.
    #[error("{resource} '{name}' already exists")]
    AlreadyExists {
        /// The kind of resource.
        resource: ResourceKind,
        /// Its name.
        name: String,
    },
    /// The system is not in the state the request needs.
    #[error("{message}")]
    FailedPrecondition {
        /// What state is wrong.
        message: String,
    },
    /// This node's role does not serve the operation.
    #[error("node '{node}' is running as '{role}' and cannot accept {operation}")]
    WrongNodeRole {
        /// The node.
        node: String,
        /// Its configured role.
        role: NodeRole,
        /// The refused class of operation, such as `data-plane operations`.
        operation: String,
    },
    /// Collection metadata and local state disagree and need reconciliation first.
    #[error("{message}")]
    ReconciliationRequired {
        /// The collection, as `database/collection`.
        collection: String,
        /// What disagrees and what to do.
        message: String,
    },
    /// The storage root is already served by another engine, in this process or another one.
    ///
    /// Exactly one engine may own a storage root: engines keep collection state resident, so
    /// two engines on one root would each publish state the other never sees.
    #[error(
        "storage root '{}' is already in use by another engine{}; lock file '{}' is held exclusively",
        .root.display(),
        .holder_pid.as_ref().map(|pid| format!(" (held by pid {pid})")).unwrap_or_default(),
        .lock_file.display()
    )]
    StorageRootLocked {
        /// The storage root.
        root: PathBuf,
        /// The lock file inside it.
        lock_file: PathBuf,
        /// Process id recorded in the lock file by the holder, if readable.
        holder_pid: Option<String>,
    },

    // ----- Authentication and authorization. -----
    /// The request carries no valid credentials.
    #[error("{message}")]
    Unauthenticated {
        /// Why the credentials were rejected.
        message: String,
    },
    /// The caller is not allowed to do this.
    #[error("{message}")]
    PermissionDenied {
        /// What was refused.
        message: String,
    },

    // ----- Routing and availability: retry, possibly on another node. -----
    /// This node does not own the collection's write path.
    #[error(
        "collection '{collection}' is not locally served by node '{node}'{}",
        owner_node.as_ref().map(|owner| format!("; it is served by node '{owner}'")).unwrap_or_default()
    )]
    NotOwner {
        /// The collection, as `database/collection`.
        collection: String,
        /// The node that refused the request.
        node: String,
        /// The node that owns the collection, when known.
        owner_node: Option<String>,
    },
    /// This node is not the control-plane leader.
    #[error(
        "node '{node}' is not the active control-plane leader; current leader is '{}'",
        leader_node.as_deref().unwrap_or("none")
    )]
    NotLeader {
        /// The node that refused the request.
        node: String,
        /// The current leader, when known.
        leader_node: Option<String>,
    },
    /// A read barrier is ahead of what the collection has made visible.
    ///
    /// A barrier needs both its manifest generation and its sequence number to be visible, so
    /// the error reports both sides of each.
    ///
    /// This is `FAILED_PRECONDITION` with no retry hint: a single-node engine acknowledges a
    /// write only after publishing it, so a barrier taken from an acknowledgement is always
    /// satisfied, and one that is not will never become satisfied by waiting. Phase 7
    /// replication brings back `UNAVAILABLE` with a retry hint for a replica that lags the
    /// primary.
    #[error(
        "read barrier (manifest generation {required_manifest_generation}, seq {required_seq_no}) is not yet visible; collection '{collection}' is at manifest generation {visible_manifest_generation}, seq {visible_seq_no}"
    )]
    ReadBarrierNotSatisfied {
        /// The collection, as `database/collection`.
        collection: String,
        /// The manifest generation the barrier requires; 0 when it requires none.
        required_manifest_generation: u64,
        /// The sequence number the barrier requires.
        required_seq_no: u64,
        /// The current manifest generation.
        visible_manifest_generation: u64,
        /// The highest visible sequence number.
        visible_seq_no: u64,
    },
    /// A dependency or this node cannot serve the request right now.
    #[error("{message}")]
    Unavailable {
        /// What is unavailable.
        message: String,
        /// How long to wait before retrying, when a retry is expected to help.
        retry_after: Option<Duration>,
    },

    // ----- Storage health: an operator has to act. -----
    /// Stored data is corrupt.
    #[error("{message}")]
    Corrupt {
        /// Which structure is damaged.
        kind: CorruptionKind,
        /// The file or key, when known.
        location: Option<String>,
        /// What is wrong.
        message: String,
    },
    /// A collection refuses writes and maintenance until the engine is reopened.
    ///
    /// This is `FAILED_PRECONDITION` with no retry hint: retrying cannot help until an operator
    /// reopens the engine, and clients must not retry it automatically. Reads keep working.
    #[error(
        "collection '{collection}' is read-only until the engine is reopened (operator action required): {reason}"
    )]
    CollectionPoisoned {
        /// The collection, as `database/collection`.
        collection: String,
        /// The failure that poisoned it.
        reason: String,
    },
    /// The WAL group holding this write could not be made durable, so the write was not
    /// acknowledged, and the collection is poisoned.
    ///
    /// With [`WriteOutcome::NotApplied`] the write is definitely absent: `UNAVAILABLE`, and it
    /// may be retried once the engine is reopened. With [`WriteOutcome::Unknown`] it may still
    /// appear after recovery, like a timeout: `INTERNAL`.
    #[error("WAL write to collection '{collection}' failed ({outcome}): {reason}")]
    WalWriteFailed {
        /// The collection, as `database/collection`.
        collection: String,
        /// Whether the failed group can reappear after recovery.
        outcome: WriteOutcome,
        /// What failed.
        reason: String,
    },
    /// A filesystem operation failed.
    #[error("{context}: {source}")]
    Io {
        /// What LogPose was doing.
        context: String,
        /// The underlying error.
        #[source]
        source: Arc<std::io::Error>,
    },

    // ----- Composite errors. -----
    /// A batch of a bulk write stream failed. Earlier batches are committed; this one and all
    /// later ones are not.
    #[error(
        "bulk write batch {batch_index} failed after {committed_batches} committed batches: {source}"
    )]
    BulkBatchFailed {
        /// Zero-based index of the failed batch in the stream.
        batch_index: u64,
        /// Batches committed before the failure.
        committed_batches: u64,
        /// Operations committed before the failure.
        committed_operations: u64,
        /// Last sequence number of the last committed batch, when one was committed.
        last_committed_seq_no: Option<u64>,
        /// Why the batch failed.
        #[source]
        source: Box<LogPoseError>,
    },

    // ----- Catch-all. -----
    /// An unexpected failure that no other variant describes.
    #[error("{message}")]
    Internal {
        /// What went wrong.
        message: String,
    },
}

impl LogPoseError {
    /// An invalid request that names no single field.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            field: None,
            message: message.into(),
        }
    }

    /// An invalid request field at `field`, such as `operations[2].id`.
    pub fn invalid_field(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            field: Some(field.into()),
            message: message.into(),
        }
    }

    /// Invalid server configuration.
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig {
            message: message.into(),
        }
    }

    /// A named resource does not exist.
    pub fn not_found(resource: ResourceKind, name: impl Into<String>) -> Self {
        Self::NotFound {
            resource,
            name: name.into(),
        }
    }

    /// A resource the request creates already exists.
    pub fn already_exists(resource: ResourceKind, name: impl Into<String>) -> Self {
        Self::AlreadyExists {
            resource,
            name: name.into(),
        }
    }

    /// The system is not in the state the request needs.
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::FailedPrecondition {
            message: message.into(),
        }
    }

    /// Something cannot serve the request now, and retrying is not expected to help soon.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
            retry_after: None,
        }
    }

    /// Stored data of `kind` is corrupt.
    pub fn corrupt(kind: CorruptionKind, message: impl Into<String>) -> Self {
        Self::Corrupt {
            kind,
            location: None,
            message: message.into(),
        }
    }

    /// A filesystem operation failed while doing `context`.
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source: Arc::new(source),
        }
    }

    /// An unexpected failure.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Prefix the request field path of a validation error, turning `vector` into
    /// `operations[2].vector`. Other errors are returned unchanged.
    #[must_use]
    pub fn with_field_prefix(self, prefix: &str) -> Self {
        let join = |field: &str| {
            if field.is_empty() {
                prefix.to_owned()
            } else if field.starts_with('[') {
                format!("{prefix}{field}")
            } else {
                format!("{prefix}.{field}")
            }
        };
        match self {
            Self::InvalidArgument { field, message } => Self::InvalidArgument {
                field: Some(field.as_deref().map_or_else(|| prefix.to_owned(), join)),
                message,
            },
            Self::DimensionMismatch {
                field,
                record_id,
                expected,
                actual,
            } => Self::DimensionMismatch {
                field: join(&field),
                record_id,
                expected,
                actual,
            },
            other => other,
        }
    }

    /// The canonical class of this error.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::InvalidArgument { .. }
            | Self::DimensionMismatch { .. }
            | Self::InvalidConfig { .. } => ErrorCode::InvalidArgument,
            Self::TooLarge { .. } => ErrorCode::ResourceExhausted,
            Self::NotFound { .. } => ErrorCode::NotFound,
            Self::AlreadyExists { .. } => ErrorCode::AlreadyExists,
            Self::FailedPrecondition { .. }
            | Self::WrongNodeRole { .. }
            | Self::ReconciliationRequired { .. }
            | Self::StorageRootLocked { .. }
            | Self::ReadBarrierNotSatisfied { .. }
            | Self::CollectionPoisoned { .. } => ErrorCode::FailedPrecondition,
            Self::Unauthenticated { .. } => ErrorCode::Unauthenticated,
            Self::PermissionDenied { .. } => ErrorCode::PermissionDenied,
            Self::NotOwner { .. }
            | Self::NotLeader { .. }
            | Self::Unavailable { .. }
            | Self::WalWriteFailed {
                outcome: WriteOutcome::NotApplied,
                ..
            } => ErrorCode::Unavailable,
            Self::WalWriteFailed {
                outcome: WriteOutcome::Unknown { .. },
                ..
            } => ErrorCode::Internal,
            Self::Corrupt { .. } => ErrorCode::DataLoss,
            Self::Io { .. } | Self::Internal { .. } => ErrorCode::Internal,
            Self::BulkBatchFailed { source, .. } => source.code(),
        }
    }

    /// Stable machine-readable reason. A [`BulkBatchFailed`](Self::BulkBatchFailed) error
    /// reports the reason of the batch's failure.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidArgument { .. } => "INVALID_ARGUMENT",
            Self::DimensionMismatch { .. } => "DIMENSION_MISMATCH",
            Self::TooLarge { .. } => "TOO_LARGE",
            Self::InvalidConfig { .. } => "INVALID_CONFIG",
            Self::NotFound { .. } => "RESOURCE_NOT_FOUND",
            Self::AlreadyExists { .. } => "RESOURCE_ALREADY_EXISTS",
            Self::FailedPrecondition { .. } => "FAILED_PRECONDITION",
            Self::WrongNodeRole { .. } => "WRONG_NODE_ROLE",
            Self::ReconciliationRequired { .. } => "RECONCILIATION_REQUIRED",
            Self::StorageRootLocked { .. } => "STORAGE_ROOT_LOCKED",
            Self::Unauthenticated { .. } => "UNAUTHENTICATED",
            Self::PermissionDenied { .. } => "PERMISSION_DENIED",
            Self::NotOwner { .. } => "NOT_OWNER",
            Self::NotLeader { .. } => "NOT_LEADER",
            Self::ReadBarrierNotSatisfied { .. } => "READ_BARRIER_NOT_SATISFIED",
            Self::Unavailable { .. } => "UNAVAILABLE",
            Self::Corrupt { .. } => "DATA_CORRUPTION",
            Self::CollectionPoisoned { .. } => "COLLECTION_POISONED",
            Self::WalWriteFailed { .. } => "WAL_WRITE_FAILED",
            Self::Io { .. } => "IO_ERROR",
            Self::Internal { .. } => "INTERNAL",
            Self::BulkBatchFailed { source, .. } => source.reason(),
        }
    }

    /// How long a client should wait before retrying, when a retry is expected to help.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::NotOwner { .. } | Self::NotLeader { .. } => Some(ROUTING_RETRY_AFTER),
            Self::Unavailable { retry_after, .. } => *retry_after,
            Self::BulkBatchFailed { source, .. } => source.retry_after(),
            _ => None,
        }
    }

    /// Structured details for the wire.
    #[must_use]
    pub fn details(&self) -> ErrorDetails {
        let mut metadata = BTreeMap::new();
        let mut field_violations = Vec::new();
        self.collect_details(&mut metadata, &mut field_violations);
        ErrorDetails {
            reason: self.reason(),
            metadata,
            field_violations,
            retry_after_ms: self
                .retry_after()
                .map(|delay| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)),
        }
    }

    fn collect_details(
        &self,
        metadata: &mut BTreeMap<String, String>,
        field_violations: &mut Vec<FieldViolation>,
    ) {
        let mut put = |key: &str, value: String| {
            metadata.insert(key.to_owned(), value);
        };
        match self {
            Self::InvalidArgument { field, message } => {
                if let Some(field) = field {
                    field_violations.push(FieldViolation {
                        field: field.clone(),
                        description: message.clone(),
                    });
                }
            }
            Self::DimensionMismatch {
                field,
                record_id,
                expected,
                actual,
            } => {
                put("expected_dimensions", expected.to_string());
                put("actual_dimensions", actual.to_string());
                if let Some(record_id) = record_id {
                    put("record_id", record_id.clone());
                }
                field_violations.push(FieldViolation {
                    field: field.clone(),
                    description: self.to_string(),
                });
            }
            Self::TooLarge { what, size, limit } => {
                put("what", what.clone());
                put("limit_bytes", limit.to_string());
                if let Some(size) = size {
                    put("size_bytes", size.to_string());
                }
            }
            Self::NotFound { resource, name } | Self::AlreadyExists { resource, name } => {
                put("resource_type", resource.as_str().to_owned());
                put("resource_name", name.clone());
            }
            Self::WrongNodeRole { node, role, .. } => {
                put("node", node.clone());
                put("node_role", role.as_str().to_owned());
            }
            Self::ReconciliationRequired { collection, .. }
            | Self::CollectionPoisoned { collection, .. } => {
                put("collection", collection.clone());
            }
            Self::StorageRootLocked {
                root, holder_pid, ..
            } => {
                put("storage_root", root.display().to_string());
                if let Some(pid) = holder_pid {
                    put("holder_pid", pid.clone());
                }
            }
            Self::NotOwner {
                collection,
                node,
                owner_node,
            } => {
                put("collection", collection.clone());
                put("node", node.clone());
                if let Some(owner) = owner_node {
                    put("owner_node", owner.clone());
                }
            }
            Self::NotLeader { node, leader_node } => {
                put("node", node.clone());
                if let Some(leader) = leader_node {
                    put("leader_node", leader.clone());
                }
            }
            Self::ReadBarrierNotSatisfied {
                collection,
                required_manifest_generation,
                required_seq_no,
                visible_manifest_generation,
                visible_seq_no,
            } => {
                put("collection", collection.clone());
                put(
                    "required_manifest_generation",
                    required_manifest_generation.to_string(),
                );
                put("required_seq_no", required_seq_no.to_string());
                put(
                    "visible_manifest_generation",
                    visible_manifest_generation.to_string(),
                );
                put("visible_seq_no", visible_seq_no.to_string());
            }
            Self::Corrupt { kind, location, .. } => {
                put("corruption_kind", kind.as_str().to_owned());
                if let Some(location) = location {
                    put("location", location.clone());
                }
            }
            Self::WalWriteFailed {
                collection,
                outcome,
                ..
            } => {
                put("collection", collection.clone());
                put("outcome", outcome.as_str().to_owned());
            }
            Self::Io { source, .. } => {
                put("io_error_kind", format!("{:?}", source.kind()));
            }
            Self::BulkBatchFailed {
                batch_index,
                committed_batches,
                committed_operations,
                last_committed_seq_no,
                source,
            } => {
                source.collect_details(metadata, field_violations);
                metadata.insert("failed_batch_index".to_owned(), batch_index.to_string());
                metadata.insert(
                    "committed_batches".to_owned(),
                    committed_batches.to_string(),
                );
                metadata.insert(
                    "committed_operations".to_owned(),
                    committed_operations.to_string(),
                );
                if let Some(seq_no) = last_committed_seq_no {
                    metadata.insert("last_committed_seq_no".to_owned(), seq_no.to_string());
                }
            }
            Self::InvalidConfig { .. }
            | Self::FailedPrecondition { .. }
            | Self::Unauthenticated { .. }
            | Self::PermissionDenied { .. }
            | Self::Unavailable { .. }
            | Self::Internal { .. } => {}
        }
    }
}

#[doc(hidden)]
/// One error of every variant, for transport mapping tests in other crates.
///
/// The `every_variant_is_listed` test keeps this list complete.
pub mod fixtures {
    use super::{CorruptionKind, LogPoseError, ResourceKind, WriteOutcome};
    use crate::NodeRole;
    use std::{path::PathBuf, time::Duration};

    /// The name of `error`'s variant, taken from its `Debug` output.
    #[must_use]
    pub fn variant_name(error: &LogPoseError) -> String {
        format!("{error:?}")
            .split([' ', '{', '('])
            .next()
            .unwrap_or_default()
            .to_owned()
    }

    /// One error of every variant.
    #[must_use]
    pub fn one_of_each_variant() -> Vec<LogPoseError> {
        vec![
            LogPoseError::invalid_field("operations[0].id", "record id must not be empty"),
            LogPoseError::DimensionMismatch {
                field: "operations[1].vector".to_owned(),
                record_id: Some("a".to_owned()),
                expected: 3,
                actual: 2,
            },
            LogPoseError::TooLarge {
                what: "REST request body".to_owned(),
                size: Some(2048),
                limit: 1024,
            },
            LogPoseError::invalid_config("node_name 'local' is reserved"),
            LogPoseError::not_found(ResourceKind::Collection, "default/docs"),
            LogPoseError::already_exists(ResourceKind::Database, "analytics"),
            LogPoseError::failed_precondition("collection 'default/docs' is being dropped"),
            LogPoseError::WrongNodeRole {
                node: "node-a".to_owned(),
                role: NodeRole::Control,
                operation: "data-plane operations".to_owned(),
            },
            LogPoseError::ReconciliationRequired {
                collection: "default/docs".to_owned(),
                message: "manual reconciliation is required".to_owned(),
            },
            LogPoseError::StorageRootLocked {
                root: PathBuf::from("/data"),
                lock_file: PathBuf::from("/data/LOCK"),
                holder_pid: Some("42".to_owned()),
            },
            LogPoseError::Unauthenticated {
                message: "missing bearer token".to_owned(),
            },
            LogPoseError::PermissionDenied {
                message: "principal 'reader' is not allowed to write".to_owned(),
            },
            LogPoseError::NotOwner {
                collection: "default/docs".to_owned(),
                node: "node-a".to_owned(),
                owner_node: Some("node-b".to_owned()),
            },
            LogPoseError::NotLeader {
                node: "node-a".to_owned(),
                leader_node: Some("node-c".to_owned()),
            },
            LogPoseError::ReadBarrierNotSatisfied {
                collection: "default/docs".to_owned(),
                required_manifest_generation: 2,
                required_seq_no: 9,
                visible_manifest_generation: 2,
                visible_seq_no: 7,
            },
            LogPoseError::Unavailable {
                message: "etcd metadata operation failed".to_owned(),
                retry_after: Some(Duration::from_secs(1)),
            },
            LogPoseError::Corrupt {
                kind: CorruptionKind::Segment,
                location: Some("segments/7.seg".to_owned()),
                message: "segment checksum mismatch in footer".to_owned(),
            },
            LogPoseError::CollectionPoisoned {
                collection: "default/docs".to_owned(),
                reason: "WAL fsync failed".to_owned(),
            },
            LogPoseError::WalWriteFailed {
                collection: "default/docs".to_owned(),
                outcome: WriteOutcome::NotApplied,
                reason: "fsync failed: Input/output error".to_owned(),
            },
            LogPoseError::io(
                "failed to write file",
                std::io::Error::other("disk on fire"),
            ),
            LogPoseError::BulkBatchFailed {
                batch_index: 2,
                committed_batches: 2,
                committed_operations: 20,
                last_committed_seq_no: Some(20),
                source: Box::new(LogPoseError::not_found(
                    ResourceKind::Collection,
                    "default/docs",
                )),
            },
            LogPoseError::internal("unexpected"),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{one_of_each_variant, variant_name};
    use super::*;
    use std::collections::BTreeSet;

    /// Exhaustive on purpose: adding a variant fails to compile here until it is also added to
    /// [`fixtures::one_of_each_variant`] and to this list.
    fn variant_index(error: &LogPoseError) -> usize {
        match error {
            LogPoseError::InvalidArgument { .. } => 0,
            LogPoseError::DimensionMismatch { .. } => 1,
            LogPoseError::TooLarge { .. } => 2,
            LogPoseError::InvalidConfig { .. } => 3,
            LogPoseError::NotFound { .. } => 4,
            LogPoseError::AlreadyExists { .. } => 5,
            LogPoseError::FailedPrecondition { .. } => 6,
            LogPoseError::WrongNodeRole { .. } => 7,
            LogPoseError::ReconciliationRequired { .. } => 8,
            LogPoseError::StorageRootLocked { .. } => 9,
            LogPoseError::Unauthenticated { .. } => 10,
            LogPoseError::PermissionDenied { .. } => 11,
            LogPoseError::NotOwner { .. } => 12,
            LogPoseError::NotLeader { .. } => 13,
            LogPoseError::ReadBarrierNotSatisfied { .. } => 14,
            LogPoseError::Unavailable { .. } => 15,
            LogPoseError::Corrupt { .. } => 16,
            LogPoseError::CollectionPoisoned { .. } => 17,
            LogPoseError::Io { .. } => 18,
            LogPoseError::BulkBatchFailed { .. } => 19,
            LogPoseError::Internal { .. } => 20,
            LogPoseError::WalWriteFailed { .. } => 21,
        }
    }
    const VARIANT_COUNT: usize = 22;

    #[test]
    fn every_variant_is_listed() {
        let indexes = one_of_each_variant()
            .iter()
            .map(variant_index)
            .collect::<BTreeSet<_>>();
        assert_eq!(indexes, (0..VARIANT_COUNT).collect::<BTreeSet<_>>());
    }

    #[test]
    fn variant_names_are_distinct_and_match_debug_output() {
        let names = one_of_each_variant()
            .iter()
            .map(variant_name)
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), VARIANT_COUNT);
        assert!(names.contains("NotOwner"));
        assert!(names.contains("Io"));
    }

    #[test]
    fn each_variant_has_the_documented_code_and_reason() {
        let expected = [
            (
                "InvalidArgument",
                ErrorCode::InvalidArgument,
                "INVALID_ARGUMENT",
            ),
            (
                "DimensionMismatch",
                ErrorCode::InvalidArgument,
                "DIMENSION_MISMATCH",
            ),
            ("TooLarge", ErrorCode::ResourceExhausted, "TOO_LARGE"),
            (
                "InvalidConfig",
                ErrorCode::InvalidArgument,
                "INVALID_CONFIG",
            ),
            ("NotFound", ErrorCode::NotFound, "RESOURCE_NOT_FOUND"),
            (
                "AlreadyExists",
                ErrorCode::AlreadyExists,
                "RESOURCE_ALREADY_EXISTS",
            ),
            (
                "FailedPrecondition",
                ErrorCode::FailedPrecondition,
                "FAILED_PRECONDITION",
            ),
            (
                "WrongNodeRole",
                ErrorCode::FailedPrecondition,
                "WRONG_NODE_ROLE",
            ),
            (
                "ReconciliationRequired",
                ErrorCode::FailedPrecondition,
                "RECONCILIATION_REQUIRED",
            ),
            (
                "StorageRootLocked",
                ErrorCode::FailedPrecondition,
                "STORAGE_ROOT_LOCKED",
            ),
            (
                "Unauthenticated",
                ErrorCode::Unauthenticated,
                "UNAUTHENTICATED",
            ),
            (
                "PermissionDenied",
                ErrorCode::PermissionDenied,
                "PERMISSION_DENIED",
            ),
            ("NotOwner", ErrorCode::Unavailable, "NOT_OWNER"),
            ("NotLeader", ErrorCode::Unavailable, "NOT_LEADER"),
            (
                "ReadBarrierNotSatisfied",
                ErrorCode::FailedPrecondition,
                "READ_BARRIER_NOT_SATISFIED",
            ),
            ("Unavailable", ErrorCode::Unavailable, "UNAVAILABLE"),
            ("Corrupt", ErrorCode::DataLoss, "DATA_CORRUPTION"),
            (
                "CollectionPoisoned",
                ErrorCode::FailedPrecondition,
                "COLLECTION_POISONED",
            ),
            ("Io", ErrorCode::Internal, "IO_ERROR"),
            // The fixture's WAL failure was rolled back, so the write is definitely absent.
            ("WalWriteFailed", ErrorCode::Unavailable, "WAL_WRITE_FAILED"),
            // The fixture's bulk failure wraps a missing collection.
            ("BulkBatchFailed", ErrorCode::NotFound, "RESOURCE_NOT_FOUND"),
            ("Internal", ErrorCode::Internal, "INTERNAL"),
        ];
        for error in one_of_each_variant() {
            let name = variant_name(&error);
            let (_, code, reason) = expected
                .iter()
                .find(|(variant, _, _)| *variant == name)
                .expect("every variant has an expectation");
            assert_eq!(error.code(), *code, "{name}");
            assert_eq!(error.reason(), *reason, "{name}");
            assert_eq!(error.details().reason, *reason, "{name}");
        }
    }

    #[test]
    fn read_barrier_errors_report_the_generation_that_is_behind() {
        // The sequence number is visible; only the manifest generation lags.
        let error = LogPoseError::ReadBarrierNotSatisfied {
            collection: "default/docs".to_owned(),
            required_manifest_generation: 5,
            required_seq_no: 3,
            visible_manifest_generation: 2,
            visible_seq_no: 7,
        };
        assert_eq!(
            error.to_string(),
            "read barrier (manifest generation 5, seq 3) is not yet visible; collection \
             'default/docs' is at manifest generation 2, seq 7"
        );
        let details = error.details();
        assert_eq!(details.metadata["required_manifest_generation"], "5");
        assert_eq!(details.metadata["visible_manifest_generation"], "2");
        assert_eq!(details.metadata["required_seq_no"], "3");
        assert_eq!(details.metadata["visible_seq_no"], "7");
        // Waiting cannot satisfy a barrier on a single node, so clients must not retry.
        assert_eq!(error.code(), ErrorCode::FailedPrecondition);
        assert_eq!(error.retry_after(), None);
        assert_eq!(details.retry_after_ms, None);
    }

    #[test]
    fn poisoned_collections_need_an_engine_reopen_and_are_not_retryable() {
        let error = LogPoseError::CollectionPoisoned {
            collection: "default/docs".to_owned(),
            reason: "WAL fsync failed".to_owned(),
        };
        assert_eq!(error.code(), ErrorCode::FailedPrecondition);
        assert_eq!(error.reason(), "COLLECTION_POISONED");
        assert_eq!(error.retry_after(), None);
        assert_eq!(error.details().metadata["collection"], "default/docs");
        assert!(
            error
                .to_string()
                .contains("read-only until the engine is reopened"),
            "{error}"
        );
    }

    #[test]
    fn wal_write_failures_are_unavailable_when_not_applied_and_internal_when_unknown() {
        let failed = |outcome| LogPoseError::WalWriteFailed {
            collection: "default/docs".to_owned(),
            outcome,
            reason: "fsync failed".to_owned(),
        };
        let not_applied = failed(WriteOutcome::NotApplied);
        assert_eq!(not_applied.code(), ErrorCode::Unavailable);
        assert_eq!(not_applied.reason(), "WAL_WRITE_FAILED");
        // Nothing to wait for: the collection is poisoned until the engine is reopened.
        assert_eq!(not_applied.retry_after(), None);
        let details = not_applied.details();
        assert_eq!(details.metadata["collection"], "default/docs");
        assert_eq!(details.metadata["outcome"], "not_applied");
        for (fenced, name) in [(true, "unknown_fenced"), (false, "unknown_unfenced")] {
            let unknown = failed(WriteOutcome::Unknown { fenced });
            assert_eq!(unknown.code(), ErrorCode::Internal);
            assert_eq!(unknown.reason(), "WAL_WRITE_FAILED");
            assert_eq!(unknown.details().metadata["outcome"], name);
        }
        assert!(
            not_applied.to_string().contains("(not applied)"),
            "{not_applied}"
        );
    }

    #[test]
    fn routing_errors_carry_a_retry_hint_and_the_owner_or_leader() {
        let not_owner = LogPoseError::NotOwner {
            collection: "default/docs".to_owned(),
            node: "node-a".to_owned(),
            owner_node: Some("node-b".to_owned()),
        };
        let details = not_owner.details();
        assert_eq!(details.retry_after_ms, Some(1000));
        assert_eq!(details.metadata["owner_node"], "node-b");
        assert!(not_owner.to_string().contains("not locally served"));

        let not_leader = LogPoseError::NotLeader {
            node: "node-a".to_owned(),
            leader_node: None,
        };
        assert_eq!(not_leader.details().retry_after_ms, Some(1000));
        assert!(!not_leader.details().metadata.contains_key("leader_node"));
        assert!(not_leader.to_string().ends_with("current leader is 'none'"));
    }

    #[test]
    fn field_prefix_builds_request_paths() {
        let error = LogPoseError::invalid_field("metadata", "must be an object")
            .with_field_prefix("operations[3]");
        assert!(matches!(
            &error,
            LogPoseError::InvalidArgument { field: Some(field), .. } if field == "operations[3].metadata"
        ));
        assert_eq!(
            error.details().field_violations[0].field,
            "operations[3].metadata"
        );

        let error = LogPoseError::invalid_argument("bad").with_field_prefix("operations[1]");
        assert_eq!(error.details().field_violations[0].field, "operations[1]");

        let error = LogPoseError::DimensionMismatch {
            field: "vector".to_owned(),
            record_id: Some("a".to_owned()),
            expected: 3,
            actual: 2,
        }
        .with_field_prefix("operations[0]");
        assert_eq!(
            error.to_string(),
            "record 'a' expected 3 dimensions but found 2"
        );
        assert_eq!(
            error.details().field_violations[0].field,
            "operations[0].vector"
        );

        let untouched = LogPoseError::internal("x").with_field_prefix("operations[0]");
        assert!(untouched.details().field_violations.is_empty());
    }

    #[test]
    fn bulk_failures_report_progress_and_the_cause() {
        let error = LogPoseError::BulkBatchFailed {
            batch_index: 1,
            committed_batches: 1,
            committed_operations: 4,
            last_committed_seq_no: Some(4),
            source: Box::new(LogPoseError::invalid_field("operations[0].id", "empty")),
        };
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        let details = error.details();
        assert_eq!(details.metadata["failed_batch_index"], "1");
        assert_eq!(details.metadata["committed_batches"], "1");
        assert_eq!(details.metadata["committed_operations"], "4");
        assert_eq!(details.metadata["last_committed_seq_no"], "4");
        assert_eq!(details.field_violations[0].field, "operations[0].id");
    }

    #[test]
    fn details_serialize_with_stable_field_names() {
        let details = LogPoseError::TooLarge {
            what: "gRPC message".to_owned(),
            size: None,
            limit: 10,
        }
        .details();
        let json = serde_json::to_value(details).expect("details serialize");
        assert_eq!(json["reason"], "TOO_LARGE");
        assert_eq!(json["metadata"]["limit_bytes"], "10");
        assert!(
            json["field_violations"]
                .as_array()
                .is_some_and(Vec::is_empty)
        );
        assert!(json.get("retry_after_ms").is_none());
    }

    #[test]
    fn error_codes_use_grpc_names() {
        let names = ErrorCode::ALL.map(ErrorCode::as_str);
        assert_eq!(names[0], "INVALID_ARGUMENT");
        assert_eq!(names[8], "DATA_LOSS");
        assert_eq!(
            serde_json::to_value(ErrorCode::ResourceExhausted).expect("code serializes"),
            "RESOURCE_EXHAUSTED"
        );
    }
}
