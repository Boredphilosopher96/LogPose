//! Shared domain types for LogPose.

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::{collections::BTreeMap, fmt};
use thiserror::Error;
use uuid::Uuid;

/// Common result type for workspace crates.
pub type Result<T> = std::result::Result<T, LogPoseError>;
/// Product name surfaced by operator-visible metadata endpoints.
pub const PRODUCT_NAME: &str = "LogPose";
/// Reserved placement token for collections created through anonymous local storage paths.
pub const ANONYMOUS_LOCAL_NODE_NAME: &str = "local";
/// Built-in tenant name used until callers provision explicit tenants.
pub const DEFAULT_TENANT_NAME: &str = "default";
/// Built-in database name used until callers provision explicit databases.
pub const DEFAULT_DATABASE_NAME: &str = "default";

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

/// Canonical node metadata exposed through operator surfaces.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeMetadata {
    /// Product identifier.
    pub product: String,
    /// Human-readable node name.
    pub node_name: String,
    /// Semantic version for the distribution.
    pub version: String,
    /// Source control revision when available.
    pub git_sha: String,
    /// Build profile used for compilation.
    pub profile: String,
}

impl NodeMetadata {
    /// Build canonical node metadata from a node name and build information.
    #[must_use]
    pub fn new(node_name: impl Into<String>, build: &BuildInfo) -> Self {
        Self {
            product: PRODUCT_NAME.to_owned(),
            node_name: node_name.into(),
            version: build.version.clone(),
            git_sha: build.git_sha.clone(),
            profile: build.profile.clone(),
        }
    }
}

/// Declared runtime role for a LogPose node.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Serve both control-plane and data-plane workflows.
    #[default]
    Combined,
    /// Serve only control-plane workflows.
    Control,
    /// Serve only data-plane workflows.
    Data,
}

impl NodeRole {
    /// Stable string form used in diagnostics and CLI output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Combined => "combined",
            Self::Control => "control",
            Self::Data => "data",
        }
    }
}

impl fmt::Display for NodeRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for NodeRole {
    type Err = LogPoseError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "combined" => Ok(Self::Combined),
            "control" => Ok(Self::Control),
            "data" => Ok(Self::Data),
            other => Err(LogPoseError::Message(format!(
                "unsupported node role '{other}'"
            ))),
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

/// Identifier for a logical tenant.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TenantId(pub Uuid);

impl Default for TenantId {
    fn default() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Identifier for a logical database.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct DatabaseId(pub Uuid);

impl Default for DatabaseId {
    fn default() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Qualified reference to one collection inside a tenant and database namespace.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CollectionRef {
    /// Tenant containing the database.
    #[serde(default = "default_tenant_name")]
    pub tenant_name: String,
    /// Database containing the collection.
    #[serde(default = "default_database_name")]
    pub database_name: String,
    /// Collection name inside the database.
    pub collection_name: String,
}

impl CollectionRef {
    /// Build one qualified collection reference.
    #[must_use]
    pub fn new(
        tenant_name: impl Into<String>,
        database_name: impl Into<String>,
        collection_name: impl Into<String>,
    ) -> Self {
        Self {
            tenant_name: tenant_name.into(),
            database_name: database_name.into(),
            collection_name: collection_name.into(),
        }
    }

    /// Build one reference inside the bootstrap default namespace.
    #[must_use]
    pub fn new_default(collection_name: impl Into<String>) -> Self {
        Self::new(DEFAULT_TENANT_NAME, DEFAULT_DATABASE_NAME, collection_name)
    }

    /// Resolve a user-facing lookup key into a collection reference.
    #[must_use]
    pub fn from_lookup_key(value: &str) -> Self {
        let parts = value.split('/').collect::<Vec<_>>();
        if parts.len() == 3 && parts.iter().all(|part| !part.trim().is_empty()) {
            Self::new(parts[0], parts[1], parts[2])
        } else {
            Self::new_default(value)
        }
    }

    /// Parse a strict collection reference accepted by service/query APIs.
    pub fn parse_reference(value: &str) -> Result<Self> {
        let trimmed = value.trim();
        let reference = match trimmed.split('/').collect::<Vec<_>>().as_slice() {
            [collection_name] => Self::new_default(*collection_name),
            [tenant_name, database_name, collection_name] => {
                Self::new(*tenant_name, *database_name, *collection_name)
            }
            _ => {
                return Err(LogPoseError::Message(format!(
                    "unsupported collection reference '{value}': expected 'collection' or 'tenant/database/collection'"
                )));
            }
        };
        reference.validate()?;
        Ok(reference)
    }

    /// Build the canonical tenant/database/collection lookup key.
    #[must_use]
    pub fn lookup_name(&self) -> String {
        format!(
            "{}/{}/{}",
            self.tenant_name, self.database_name, self.collection_name
        )
    }

    /// Validate that namespace fields are populated.
    pub fn validate(&self) -> Result<()> {
        validate_collection_ref_segment("tenant_name", &self.tenant_name)?;
        validate_collection_ref_segment("database_name", &self.database_name)?;
        validate_collection_ref_segment("collection_name", &self.collection_name)?;
        Ok(())
    }
}

fn validate_collection_ref_segment(field_name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(LogPoseError::Message(format!(
            "{field_name} must not be empty"
        )));
    }
    if value.contains('/') {
        return Err(LogPoseError::Message(format!(
            "{field_name} must not contain '/'"
        )));
    }
    Ok(())
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

/// Scalar metadata value supported by query predicates and planner statistics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScalarMetadataValue {
    /// Match a string metadata value.
    String(String),
    /// Match a numeric metadata value without losing integer precision.
    Number(Number),
    /// Match a boolean metadata value.
    Bool(bool),
    /// Match a null metadata value.
    Null,
}

impl ScalarMetadataValue {
    /// Convert a JSON value into a supported top-level scalar, if possible.
    #[must_use]
    pub fn from_json(value: &Value) -> Option<Self> {
        match value {
            Value::String(value) => Some(Self::String(value.clone())),
            Value::Number(value) => Some(Self::Number(value.clone())),
            Value::Bool(value) => Some(Self::Bool(*value)),
            Value::Null => Some(Self::Null),
            Value::Array(_) | Value::Object(_) => None,
        }
    }

    /// Render a stable string key for planner summaries.
    #[must_use]
    pub fn summary_key(&self) -> String {
        match self {
            Self::String(value) => format!("string:{value}"),
            Self::Number(value) => format!("number:{value}"),
            Self::Bool(value) => format!("bool:{value}"),
            Self::Null => "null".to_owned(),
        }
    }
}

/// Planner-visible scalar field summary for a queryable unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalarFieldStats {
    /// Number of records where the field exists.
    pub present_count: usize,
    /// Number of records where the field is explicitly null.
    pub null_count: usize,
    /// Number of distinct scalar values seen for the field.
    pub distinct_count: usize,
    /// Minimum scalar value when an ordered comparison exists.
    pub min: Option<ScalarMetadataValue>,
    /// Maximum scalar value when an ordered comparison exists.
    pub max: Option<ScalarMetadataValue>,
    /// Stable value histogram keyed by scalar summary string.
    pub value_counts: BTreeMap<String, usize>,
}

/// Background maintenance state surfaced to operators.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceStatus {
    /// Operations waiting to run.
    pub pending: Vec<String>,
    /// Operation currently executing, if any.
    pub in_progress: Option<String>,
    /// Most recent maintenance failure.
    pub last_error: Option<String>,
    /// Number of successfully completed maintenance operations.
    pub completed_runs: usize,
}

/// One physical artifact that backs a queryable unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryUnitArtifactStats {
    /// Stable artifact role such as raw segment, flat exact sidecar, or ann graph.
    pub kind: String,
    /// Operator-visible file name when the artifact is persisted on disk.
    pub file_name: String,
    /// Approximate bytes attributable to the artifact.
    pub approx_bytes: usize,
}

/// Planner-visible description of a mutable or immutable queryable unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryUnitStats {
    /// Stable identifier for the unit.
    pub unit_id: String,
    /// Logical storage tier for the unit.
    pub tier: String,
    /// Index family available for the unit.
    pub index_kind: String,
    /// Lowest sequence number represented by the unit.
    pub min_seq_no: SeqNo,
    /// Highest sequence number represented by the unit.
    pub max_seq_no: SeqNo,
    /// Number of put entries in the unit.
    pub put_count: usize,
    /// Number of delete entries in the unit.
    pub delete_count: usize,
    /// Approximate on-disk or in-memory bytes attributable to the unit.
    pub approx_bytes: usize,
    /// Planner-visible scalar summaries for top-level metadata fields.
    pub scalar_fields: BTreeMap<String, ScalarFieldStats>,
    /// Structured physical artifacts that back this unit.
    pub artifact_stats: Vec<QueryUnitArtifactStats>,
    /// Component-oriented byte accounting surfaced to planners and operators.
    pub component_bytes: BTreeMap<String, usize>,
}

/// Immutable ANN candidate returned before latest-visible resolution and rerank.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnnCandidate {
    /// Immutable unit that produced this candidate.
    pub unit_id: String,
    /// External record identifier.
    pub record_id: RecordId,
    /// Sequence number represented by the candidate.
    pub seq_no: SeqNo,
    /// Approximate vector score returned by candidate generation.
    pub value: f32,
}

/// Planner-provided ANN candidate generation request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnnSearchRequest {
    /// Query embedding vector.
    pub vector: Vec<f32>,
    /// Final top-k requested by the caller.
    pub top_k: usize,
    /// Candidate budget to materialize before rerank.
    pub candidate_budget: usize,
}

/// Collection-level storage statistics surfaced to the CLI.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionStats {
    /// Collection identifier.
    pub collection_id: CollectionId,
    /// Tenant containing the collection.
    #[serde(default = "default_tenant_name")]
    pub tenant_name: String,
    /// Database containing the collection.
    #[serde(default = "default_database_name")]
    pub database_name: String,
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
    /// Current background maintenance state.
    pub maintenance: MaintenanceStatus,
    /// Planner-visible mutable and immutable queryable units.
    pub query_units: Vec<QueryUnitStats>,
}

/// Runtime metadata backend selection for the control-plane authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MetadataBackend {
    /// Use local placement files as metadata authority.
    #[default]
    Local,
    /// Use etcd as metadata authority and keep local placement files as fallback.
    Etcd,
}

impl fmt::Display for MetadataBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => formatter.write_str("local"),
            Self::Etcd => formatter.write_str("etcd"),
        }
    }
}

/// Configuration for etcd-backed metadata authority.
///
/// These fields configure the etcd client used by the control plane when
/// [`MetadataBackend::Etcd`] is selected. The struct lives in `logpose-types`
/// (not `logpose-storage-etcd`) so crates that only need to parse
/// configuration do not have to pull in the etcd-client implementation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EtcdMetadataConfig {
    /// Etcd endpoints, for example `http://127.0.0.1:2379`.
    pub endpoints: Vec<String>,
    /// Key prefix for LogPose metadata state.
    #[serde(default = "default_etcd_key_prefix")]
    pub key_prefix: String,
    /// Request timeout in milliseconds.
    #[serde(default = "default_etcd_timeout_ms")]
    pub timeout_ms: u64,
    /// Node membership lease TTL in seconds.
    #[serde(default = "default_etcd_membership_ttl_secs")]
    pub membership_ttl_secs: i64,
    /// Controller leadership lease TTL in seconds.
    #[serde(default = "default_etcd_leadership_ttl_secs")]
    pub leadership_ttl_secs: i64,
    /// Cluster namespace for metadata coordination keys.
    #[serde(default = "default_etcd_cluster_name")]
    pub cluster_name: String,
}

impl Default for EtcdMetadataConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            key_prefix: default_etcd_key_prefix(),
            timeout_ms: default_etcd_timeout_ms(),
            membership_ttl_secs: default_etcd_membership_ttl_secs(),
            leadership_ttl_secs: default_etcd_leadership_ttl_secs(),
            cluster_name: default_etcd_cluster_name(),
        }
    }
}

impl EtcdMetadataConfig {
    /// Validate etcd-specific configuration invariants.
    pub fn validate(&self) -> Result<()> {
        if self.endpoints.is_empty() {
            return Err(LogPoseError::Message(
                "metadata.etcd.endpoints must be non-empty when metadata.backend is 'etcd'"
                    .to_owned(),
            ));
        }
        if self
            .endpoints
            .iter()
            .any(|endpoint| endpoint.trim().is_empty())
        {
            return Err(LogPoseError::Message(
                "metadata.etcd.endpoints must not contain blank values".to_owned(),
            ));
        }
        if self.key_prefix.trim().is_empty() {
            return Err(LogPoseError::Message(
                "metadata.etcd.key_prefix must not be blank".to_owned(),
            ));
        }
        if self.cluster_name.trim().is_empty() {
            return Err(LogPoseError::Message(
                "metadata.etcd.cluster_name must not be blank".to_owned(),
            ));
        }
        if self.timeout_ms == 0 {
            return Err(LogPoseError::Message(
                "metadata.etcd.timeout_ms must be greater than 0".to_owned(),
            ));
        }
        if self.membership_ttl_secs <= 0 {
            return Err(LogPoseError::Message(
                "metadata.etcd.membership_ttl_secs must be greater than 0".to_owned(),
            ));
        }
        if self.leadership_ttl_secs <= 0 {
            return Err(LogPoseError::Message(
                "metadata.etcd.leadership_ttl_secs must be greater than 0".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Top-level metadata configuration exposed through `LogPoseConfig`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct MetadataConfig {
    /// Selected metadata backend.
    #[serde(default)]
    pub backend: MetadataBackend,
    /// Etcd-specific settings.
    #[serde(default)]
    pub etcd: EtcdMetadataConfig,
}

fn default_etcd_key_prefix() -> String {
    "/logpose/metadata".to_owned()
}

const fn default_etcd_timeout_ms() -> u64 {
    1_500
}

const fn default_etcd_membership_ttl_secs() -> i64 {
    15
}

const fn default_etcd_leadership_ttl_secs() -> i64 {
    10
}

fn default_etcd_cluster_name() -> String {
    "default".to_owned()
}

fn default_tenant_name() -> String {
    DEFAULT_TENANT_NAME.to_owned()
}

fn default_database_name() -> String {
    DEFAULT_DATABASE_NAME.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_ref_from_lookup_key_uses_default_namespace_for_bare_names() {
        let reference = CollectionRef::from_lookup_key("documents");

        assert_eq!(reference, CollectionRef::new_default("documents"));
    }

    #[test]
    fn collection_ref_parse_reference_accepts_bare_and_qualified_names() {
        assert_eq!(
            CollectionRef::parse_reference("documents").expect("bare collection should parse"),
            CollectionRef::new_default("documents")
        );
        assert_eq!(
            CollectionRef::parse_reference("acme/analytics/documents")
                .expect("qualified collection should parse"),
            CollectionRef::new("acme", "analytics", "documents")
        );
    }

    #[test]
    fn collection_ref_parse_reference_rejects_invalid_component_count() {
        let error = CollectionRef::parse_reference("analytics/documents")
            .expect_err("two-part references should be rejected");

        assert!(
            error
                .to_string()
                .contains("unsupported collection reference")
        );
    }

    #[test]
    fn collection_ref_rejects_reserved_namespace_separator() {
        let error = CollectionRef::new("tenant-a", "analytics", "docs/v2")
            .validate()
            .expect_err("slash-containing collection names should fail");

        assert!(error.to_string().contains("collection_name"));
        assert!(error.to_string().contains("/"));
    }
}

/// Persisted node assignment for one collection.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionAssignment {
    /// Node recorded to host the collection.
    pub assigned_node: String,
    /// Runtime role recorded for the node assignment.
    pub assigned_role: NodeRole,
}

/// Operator-visible explanation of where a collection is currently placed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionPlacement {
    /// Stable collection identifier.
    pub collection_id: CollectionId,
    /// Tenant containing the collection.
    #[serde(default = "default_tenant_name")]
    pub tenant_name: String,
    /// Database containing the collection.
    #[serde(default = "default_database_name")]
    pub database_name: String,
    /// Human-readable collection name.
    pub collection_name: String,
    /// Node currently assigned to serve the collection.
    pub assigned_node: String,
    /// Runtime role recorded for the current placement assignment.
    pub assigned_role: NodeRole,
    /// Routing family selected for this placement.
    pub route_kind: String,
    /// Human-readable diagnostic reason for the current route.
    pub route_reason: String,
}

/// Aggregated maintenance backlog surfaced through control-plane diagnostics.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceBacklog {
    /// Number of collections with queued maintenance work.
    pub collections_with_pending: usize,
    /// Number of queued maintenance operations across all collections.
    pub pending_operations: usize,
    /// Number of collections currently executing maintenance.
    pub collections_in_progress: usize,
    /// Number of collections with a recorded maintenance failure.
    pub collections_with_errors: usize,
}

/// Operator-facing runtime summary for a node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeRuntimeStatus {
    /// Canonical node metadata.
    pub metadata: NodeMetadata,
    /// Declared runtime role for the node.
    pub role: NodeRole,
    /// Configured REST listener address reported by this runtime.
    pub rest_endpoint: String,
    /// Configured gRPC listener address reported by this runtime.
    pub grpc_endpoint: String,
    /// Storage engine implementation backing the data plane.
    pub storage_engine: String,
    /// Whether control-plane coordination workflows should be considered available.
    pub control_plane_ready: bool,
    /// Whether data-plane workflows should be considered available.
    pub data_plane_ready: bool,
    /// Number of collections this runtime can currently serve locally.
    pub collection_count: usize,
    /// Collection placement summaries sorted by collection name.
    pub collections: Vec<CollectionPlacement>,
    /// Aggregated maintenance state across local collections.
    pub maintenance: MaintenanceBacklog,
}
