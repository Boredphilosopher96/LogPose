//! Shared domain types for LogPose.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

/// Common result type for workspace crates.
pub type Result<T> = std::result::Result<T, LogPoseError>;

/// Top-level workspace error.
#[derive(Debug, Error)]
pub enum LogPoseError {
    /// Generic bootstrap and configuration errors.
    #[error("{0}")]
    Message(String),
}

/// Build metadata surfaced by service entrypoints.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildInfo {
    /// Semantic version for the distribution.
    pub version: String,
    /// Source control revision when available.
    pub git_sha: String,
    /// Build profile used for compilation.
    pub profile: String,
}

impl BuildInfo {
    /// Create build metadata from compile-time environment values.
    #[must_use]
    pub fn current() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            git_sha: option_env!("LOGPOSE_GIT_SHA")
                .unwrap_or("development")
                .to_owned(),
            profile: option_env!("PROFILE").unwrap_or("debug").to_owned(),
        }
    }
}

/// Identifier for a collection or namespace.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ResourceId(pub Uuid);

impl Default for ResourceId {
    fn default() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Monotonic sequence number assigned to durable write operations.
pub type SeqNo = u64;

/// Identifier for a collection.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct CollectionId(pub Uuid);

impl Default for CollectionId {
    fn default() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for CollectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// User-supplied identifier for a stored record.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct RecordId(pub String);

impl RecordId {
    /// Create a record identifier from a string-like value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<&str> for RecordId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for RecordId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Distance function configured for a collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    /// Cosine similarity search.
    Cosine,
    /// Dot-product similarity search.
    Dot,
    /// Euclidean distance search.
    L2,
}

impl fmt::Display for DistanceMetric {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Cosine => "cosine",
            Self::Dot => "dot",
            Self::L2 => "l2",
        };
        formatter.write_str(value)
    }
}

impl std::str::FromStr for DistanceMetric {
    type Err = LogPoseError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "cosine" => Ok(Self::Cosine),
            "dot" => Ok(Self::Dot),
            "l2" => Ok(Self::L2),
            other => Err(LogPoseError::Message(format!(
                "unsupported distance metric '{other}'"
            ))),
        }
    }
}

/// Optional remote blob-store configuration for immutable artifacts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteBlobConfig {
    /// S3-compatible endpoint URL.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Prefix under which collection artifacts are stored.
    pub prefix: String,
}

/// Insert or replace-style payload stored as a new version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PutRecord {
    /// External record identifier.
    pub id: RecordId,
    /// Raw vector payload stored in the MVP segment format.
    pub vector: Vec<f32>,
    /// User metadata preserved as opaque JSON.
    pub metadata: Value,
}

/// Logical delete for a previously written record identifier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteRecord {
    /// External record identifier.
    pub id: RecordId,
}

/// Durable write operation persisted to the WAL and segments.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WriteOperation {
    /// Insert or replace a record version.
    Put(PutRecord),
    /// Tombstone an existing record identifier.
    Delete(DeleteRecord),
}

impl WriteOperation {
    /// Borrow the record identifier for this operation.
    #[must_use]
    pub fn id(&self) -> &RecordId {
        match self {
            Self::Put(record) => &record.id,
            Self::Delete(record) => &record.id,
        }
    }

    /// Validate collection dimensions for vector payloads.
    pub fn validate_dimensions(&self, expected_dimensions: usize) -> Result<()> {
        match self {
            Self::Put(record) if record.vector.len() != expected_dimensions => {
                Err(LogPoseError::Message(format!(
                    "record '{}' expected {} dimensions but found {}",
                    record.id,
                    expected_dimensions,
                    record.vector.len()
                )))
            }
            _ => Ok(()),
        }
    }
}

/// Commit metadata returned after durable append.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitAck {
    /// Largest sequence number applied in the batch.
    pub last_seq_no: SeqNo,
    /// Number of operations durably recorded.
    pub applied_ops: usize,
}

/// Stable visibility boundary for reads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Current manifest generation selected for the read.
    pub manifest_generation: u64,
    /// Highest sequence number visible to the read.
    pub visible_seq_no: SeqNo,
}

/// Visible user record reconstructed from mutable and immutable storage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VisibleRecord {
    /// External record identifier.
    pub id: RecordId,
    /// Vector payload.
    pub vector: Vec<f32>,
    /// Opaque user metadata.
    pub metadata: Value,
    /// Sequence number of the visible version.
    pub seq_no: SeqNo,
}

/// Collection-level storage statistics surfaced to the CLI.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionStats {
    /// Collection identifier.
    pub collection_id: CollectionId,
    /// Human-readable collection name.
    pub collection_name: String,
    /// Current manifest generation.
    pub manifest_generation: u64,
    /// Highest visible durable sequence number.
    pub visible_seq_no: SeqNo,
    /// Number of operations still resident in the mutable delta.
    pub mutable_op_count: usize,
    /// Number of immutable segments referenced by the manifest.
    pub segment_count: usize,
    /// Number of visible records after tombstone resolution.
    pub live_record_count: usize,
    /// Number of tombstoned record identifiers still present in storage state.
    pub deleted_record_count: usize,
}
