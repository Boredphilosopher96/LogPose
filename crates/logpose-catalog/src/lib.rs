//! Metadata and collection catalog abstractions.

use logpose_types::{
    CollectionId, CollectionRef, DEFAULT_DATABASE_NAME, DatabaseId, DatabaseRef, DistanceMetric,
    LogPoseError, RemoteBlobConfig, WriteOperation, default_database_id,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use logpose_auth::{DatabaseAccessPolicy, Principal};

/// Default mutable-op threshold before the engine should flush.
pub const DEFAULT_FLUSH_THRESHOLD_OPS: usize = 10_000;
/// Default approximate byte threshold before the engine should flush.
pub const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
/// Default number of immutable segments before compaction is recommended.
pub const DEFAULT_COMPACTION_THRESHOLD_SEGMENTS: usize = 4;

/// Logical database metadata scaffold for policy and isolation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseDescriptor {
    /// Stable database identifier.
    pub database_id: DatabaseId,
    /// Human-readable database name.
    pub name: String,
    /// Whether this descriptor is the operator-provisioned default database.
    pub is_default: bool,
}

impl DatabaseDescriptor {
    /// Construct one database descriptor.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            database_id: if name == DEFAULT_DATABASE_NAME {
                default_database_id()
            } else {
                DatabaseId::default()
            },
            is_default: name == DEFAULT_DATABASE_NAME,
            name,
        }
    }

    /// Validate database-level configuration.
    pub fn validate(&self) -> logpose_types::Result<()> {
        validate_namespace_segment("database name", &self.name)?;
        let expected_is_default = self.name == DEFAULT_DATABASE_NAME;
        if self.is_default != expected_is_default {
            return Err(LogPoseError::Message(format!(
                "database descriptor is_default must be {expected_is_default} for database '{}'",
                self.name
            )));
        }
        Ok(())
    }

    /// Return the canonical database reference for this descriptor.
    #[must_use]
    pub fn database_ref(&self) -> DatabaseRef {
        DatabaseRef::new(self.name.clone())
    }

    /// Return the canonical database lookup key for this descriptor.
    #[must_use]
    pub fn lookup_name(&self) -> String {
        self.database_ref().lookup_name()
    }
}

/// Logical collection metadata scaffold.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionDescriptor {
    /// Stable collection identifier.
    pub collection_id: CollectionId,
    /// Database containing the collection.
    #[serde(default = "default_database_name")]
    pub database_name: String,
    /// Human-readable collection name.
    pub name: String,
    /// Embedding dimensions expected for the collection.
    pub dimensions: usize,
    /// Distance metric configured for the collection.
    pub metric: DistanceMetric,
    /// Local filesystem root for this collection.
    pub root_path: PathBuf,
    /// Optional remote blob-store configuration for immutable artifacts.
    pub remote_blob: Option<RemoteBlobConfig>,
    /// Mutable operation threshold before a flush should occur.
    pub flush_threshold_ops: usize,
    /// Mutable byte threshold before a flush should occur.
    pub flush_threshold_bytes: usize,
    /// Immutable segment threshold before compaction is recommended.
    pub compaction_threshold_segments: usize,
}

impl CollectionDescriptor {
    /// Construct a collection descriptor rooted under the provided collections directory.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        dimensions: usize,
        metric: DistanceMetric,
        collections_root: impl AsRef<Path>,
    ) -> Self {
        Self::new_in_database(
            DEFAULT_DATABASE_NAME,
            name,
            dimensions,
            metric,
            collections_root,
        )
    }

    /// Construct a collection descriptor inside one database.
    #[must_use]
    pub fn new_in_database(
        database_name: impl Into<String>,
        name: impl Into<String>,
        dimensions: usize,
        metric: DistanceMetric,
        collections_root: impl AsRef<Path>,
    ) -> Self {
        let collection_id = CollectionId::default();
        Self {
            collection_id: collection_id.clone(),
            database_name: database_name.into(),
            name: name.into(),
            dimensions,
            metric,
            root_path: collections_root.as_ref().join(collection_id.to_string()),
            remote_blob: None,
            flush_threshold_ops: DEFAULT_FLUSH_THRESHOLD_OPS,
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
            compaction_threshold_segments: DEFAULT_COMPACTION_THRESHOLD_SEGMENTS,
        }
    }

    /// Return the canonical database/collection reference for this descriptor.
    #[must_use]
    pub fn collection_ref(&self) -> CollectionRef {
        CollectionRef::new(self.database_name.clone(), self.name.clone())
    }

    /// Return the canonical database/collection lookup key for this descriptor.
    #[must_use]
    pub fn lookup_name(&self) -> String {
        self.collection_ref().lookup_name()
    }

    /// Return a copy of the descriptor without any node-local filesystem path.
    #[must_use]
    pub fn without_root_path(&self) -> Self {
        let mut descriptor = self.clone();
        descriptor.root_path = PathBuf::new();
        descriptor
    }

    /// Return whether two descriptors refer to the same serveable collection identity.
    #[must_use]
    pub fn matches_serving_identity(&self, other: &Self) -> bool {
        self.collection_id == other.collection_id
            && self.database_name == other.database_name
            && self.name == other.name
            && self.dimensions == other.dimensions
            && self.metric == other.metric
            && self.remote_blob == other.remote_blob
            && self.flush_threshold_ops == other.flush_threshold_ops
            && self.flush_threshold_bytes == other.flush_threshold_bytes
            && self.compaction_threshold_segments == other.compaction_threshold_segments
    }

    /// Validate collection-level configuration values.
    pub fn validate(&self) -> logpose_types::Result<()> {
        self.collection_ref().validate()?;
        if self.dimensions == 0 {
            return Err(LogPoseError::Message(
                "dimensions must be greater than 0".to_owned(),
            ));
        }
        if self.flush_threshold_ops == 0 {
            return Err(LogPoseError::Message(
                "flush_threshold_ops must be greater than 0".to_owned(),
            ));
        }
        if self.flush_threshold_bytes == 0 {
            return Err(LogPoseError::Message(
                "flush_threshold_bytes must be greater than 0".to_owned(),
            ));
        }
        if self.compaction_threshold_segments <= 1 {
            return Err(LogPoseError::Message(
                "compaction_threshold_segments must be greater than 1".to_owned(),
            ));
        }
        Ok(())
    }

    /// Validate whether an operation matches this collection's configured dimensions.
    pub fn validate_operation(&self, operation: &WriteOperation) -> logpose_types::Result<()> {
        self.validate()?;
        operation.validate_dimensions(self.dimensions)
    }
}

fn default_database_name() -> String {
    DEFAULT_DATABASE_NAME.to_owned()
}

fn validate_namespace_segment(label: &str, value: &str) -> logpose_types::Result<()> {
    let trimmed = value.trim();
    if value.trim().is_empty() {
        return Err(LogPoseError::Message(format!("{label} must not be empty")));
    }
    if value.contains('/') {
        return Err(LogPoseError::Message(format!(
            "{label} must not contain '/'"
        )));
    }
    if matches!(trimmed, "." | "..") {
        return Err(LogPoseError::Message(format!(
            "{label} must not be a relative path component"
        )));
    }
    Ok(())
}

/// Catalog metadata surface for databases, principals, and policies.
pub trait CatalogStore: Send + Sync {
    /// Create or replace a database descriptor.
    fn put_database(
        &self,
        descriptor: DatabaseDescriptor,
    ) -> logpose_types::Result<DatabaseDescriptor>;

    /// Read one database descriptor by database name.
    fn get_database(&self, database_name: &str) -> logpose_types::Result<DatabaseDescriptor>;

    /// List every database descriptor.
    fn list_databases(&self) -> logpose_types::Result<Vec<DatabaseDescriptor>>;

    /// Create or replace a principal descriptor.
    fn put_principal(&self, principal: Principal) -> logpose_types::Result<Principal>;

    /// Read one principal descriptor by name.
    fn get_principal(&self, principal_name: &str) -> logpose_types::Result<Principal>;

    /// List every stored principal descriptor.
    fn list_principals(&self) -> logpose_types::Result<Vec<Principal>>;

    /// Create or replace one database access policy.
    fn put_database_access_policy(
        &self,
        policy: DatabaseAccessPolicy,
    ) -> logpose_types::Result<DatabaseAccessPolicy>;

    /// Read one database access policy.
    fn get_database_access_policy(
        &self,
        database_name: &str,
    ) -> logpose_types::Result<DatabaseAccessPolicy>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_descriptor_lookup_name_is_database_only() {
        let descriptor = DatabaseDescriptor::new("analytics");

        assert_eq!(descriptor.lookup_name(), "analytics");
        assert_eq!(
            serde_json::to_value(&descriptor).expect("database descriptor should serialize"),
            serde_json::json!({
                "database_id": descriptor.database_id.to_string(),
                "name": "analytics",
                "is_default": false,
            })
        );
    }

    #[test]
    fn database_descriptor_rejects_empty_name() {
        let error = DatabaseDescriptor::new("   ")
            .validate()
            .expect_err("blank database name should fail");

        assert!(error.to_string().contains("database name"));
    }

    #[test]
    fn database_descriptor_rejects_inconsistent_default_flag() {
        let error = DatabaseDescriptor {
            database_id: DatabaseId::default(),
            name: "analytics".to_owned(),
            is_default: true,
        }
        .validate()
        .expect_err("non-default databases must not be flagged as default");

        assert!(error.to_string().contains("is_default"));
    }

    #[test]
    fn collection_descriptor_defaults_to_default_database() {
        let descriptor = CollectionDescriptor::new(
            "events",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/catalog-validation"),
        );

        assert_eq!(descriptor.database_name, DEFAULT_DATABASE_NAME);
        assert_eq!(
            serde_json::to_value(&descriptor).expect("collection descriptor should serialize"),
            serde_json::json!({
                "collection_id": descriptor.collection_id.to_string(),
                "database_name": DEFAULT_DATABASE_NAME,
                "name": "events",
                "dimensions": 2,
                "metric": "dot",
                "root_path": descriptor.root_path,
                "remote_blob": null,
                "flush_threshold_ops": DEFAULT_FLUSH_THRESHOLD_OPS,
                "flush_threshold_bytes": DEFAULT_FLUSH_THRESHOLD_BYTES,
                "compaction_threshold_segments": DEFAULT_COMPACTION_THRESHOLD_SEGMENTS,
            })
        );
    }

    #[test]
    fn namespace_descriptors_reject_reserved_separator() {
        let database_error = DatabaseDescriptor::new("analytics/v2")
            .validate()
            .expect_err("slash-containing database name should fail");
        assert!(database_error.to_string().contains("database name"));

        let collection_error = CollectionDescriptor::new_in_database(
            "analytics",
            "docs/v2",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/catalog-validation"),
        )
        .validate()
        .expect_err("slash-containing collection name should fail");
        assert!(collection_error.to_string().contains("collection_name"));
    }

    #[test]
    fn namespace_descriptors_reject_relative_path_components() {
        let database_error = DatabaseDescriptor::new("..")
            .validate()
            .expect_err("relative database names should fail");
        assert!(database_error.to_string().contains("relative path"));

        let collection_error = CollectionDescriptor::new_in_database(
            ".",
            "events",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/catalog-validation"),
        )
        .validate()
        .expect_err("relative database path components should fail");
        assert!(collection_error.to_string().contains("relative path"));
    }

    #[test]
    fn collection_descriptor_lookup_name_is_database_and_collection_only() {
        let descriptor = CollectionDescriptor::new_in_database(
            "analytics",
            "events",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/catalog-validation"),
        );

        assert_eq!(descriptor.lookup_name(), "analytics/events");
    }

    #[test]
    fn rejects_non_positive_maintenance_thresholds() {
        let root = Path::new("/tmp/catalog-validation");

        let mut descriptor = CollectionDescriptor::new("events", 2, DistanceMetric::Dot, root);
        descriptor.flush_threshold_ops = 0;
        assert!(
            descriptor
                .validate()
                .expect_err("zero flush ops should fail")
                .to_string()
                .contains("flush_threshold_ops")
        );

        let mut descriptor = CollectionDescriptor::new("events", 2, DistanceMetric::Dot, root);
        descriptor.flush_threshold_bytes = 0;
        assert!(
            descriptor
                .validate()
                .expect_err("zero flush bytes should fail")
                .to_string()
                .contains("flush_threshold_bytes")
        );

        let mut descriptor = CollectionDescriptor::new("events", 2, DistanceMetric::Dot, root);
        descriptor.compaction_threshold_segments = 1;
        assert!(
            descriptor
                .validate()
                .expect_err("compaction threshold of one should fail")
                .to_string()
                .contains("compaction_threshold_segments")
        );
    }

    #[test]
    fn rejects_zero_dimensions() {
        let root = Path::new("/tmp/catalog-validation");
        let descriptor = CollectionDescriptor::new("events", 0, DistanceMetric::Dot, root);

        assert!(
            descriptor
                .validate()
                .expect_err("zero dimensions should fail")
                .to_string()
                .contains("dimensions")
        );
    }

    #[test]
    fn serving_identity_ignores_runtime_root_path() {
        let root = Path::new("/tmp/catalog-validation");
        let descriptor = CollectionDescriptor::new("events", 2, DistanceMetric::Dot, root);
        let stripped = descriptor.without_root_path();

        assert!(descriptor.matches_serving_identity(&stripped));
        assert_eq!(stripped.root_path, PathBuf::new());
    }
}
