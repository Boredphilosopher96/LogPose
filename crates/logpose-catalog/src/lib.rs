//! Metadata and collection catalog abstractions.

use logpose_types::{
    CollectionId, CollectionRef, DEFAULT_DATABASE_NAME, DEFAULT_TENANT_NAME, DatabaseId,
    DistanceMetric, LogPoseError, RemoteBlobConfig, TenantId, WriteOperation,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Default mutable-op threshold before the engine should flush.
pub const DEFAULT_FLUSH_THRESHOLD_OPS: usize = 10_000;
/// Default approximate byte threshold before the engine should flush.
pub const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
/// Default number of immutable segments before compaction is recommended.
pub const DEFAULT_COMPACTION_THRESHOLD_SEGMENTS: usize = 4;

/// Tenant metadata scaffold for future namespace management.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantDescriptor {
    /// Stable tenant identifier.
    pub tenant_id: TenantId,
    /// Human-readable tenant name.
    pub name: String,
    /// Whether this descriptor is the operator-provisioned default tenant.
    pub is_default: bool,
}

impl TenantDescriptor {
    /// Construct one tenant descriptor.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            tenant_id: TenantId::default(),
            is_default: name == DEFAULT_TENANT_NAME,
            name,
        }
    }

    /// Validate tenant-level configuration.
    pub fn validate(&self) -> logpose_types::Result<()> {
        validate_namespace_segment("tenant name", &self.name)?;
        Ok(())
    }
}

/// Logical database metadata scaffold for tenancy and policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseDescriptor {
    /// Stable database identifier.
    pub database_id: DatabaseId,
    /// Tenant containing the database.
    #[serde(default = "default_tenant_name")]
    pub tenant_name: String,
    /// Human-readable database name.
    pub name: String,
    /// Whether this descriptor is the operator-provisioned default database.
    pub is_default: bool,
}

impl DatabaseDescriptor {
    /// Construct one database descriptor.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self::new_in_tenant(DEFAULT_TENANT_NAME, name)
    }

    /// Construct one database descriptor in a tenant namespace.
    #[must_use]
    pub fn new_in_tenant(tenant_name: impl Into<String>, name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            database_id: DatabaseId::default(),
            tenant_name: tenant_name.into(),
            is_default: name == DEFAULT_DATABASE_NAME,
            name,
        }
    }

    /// Validate database-level configuration.
    pub fn validate(&self) -> logpose_types::Result<()> {
        validate_namespace_segment("tenant name", &self.tenant_name)?;
        validate_namespace_segment("database name", &self.name)?;
        Ok(())
    }
}

/// Logical collection metadata scaffold.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionDescriptor {
    /// Stable collection identifier.
    pub collection_id: CollectionId,
    /// Tenant containing the collection.
    #[serde(default = "default_tenant_name")]
    pub tenant_name: String,
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
        Self::new_in_namespace(
            DEFAULT_TENANT_NAME,
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
        Self::new_in_namespace(
            DEFAULT_TENANT_NAME,
            database_name,
            name,
            dimensions,
            metric,
            collections_root,
        )
    }

    /// Construct a collection descriptor inside one tenant/database namespace.
    #[must_use]
    pub fn new_in_namespace(
        tenant_name: impl Into<String>,
        database_name: impl Into<String>,
        name: impl Into<String>,
        dimensions: usize,
        metric: DistanceMetric,
        collections_root: impl AsRef<Path>,
    ) -> Self {
        let collection_id = CollectionId::default();
        Self {
            collection_id: collection_id.clone(),
            tenant_name: tenant_name.into(),
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

    /// Return the canonical tenant/database/collection reference for this descriptor.
    #[must_use]
    pub fn collection_ref(&self) -> CollectionRef {
        CollectionRef::new(
            self.tenant_name.clone(),
            self.database_name.clone(),
            self.name.clone(),
        )
    }

    /// Return the canonical tenant/database/collection lookup key for this descriptor.
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
            && self.tenant_name == other.tenant_name
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

fn default_tenant_name() -> String {
    DEFAULT_TENANT_NAME.to_owned()
}

fn default_database_name() -> String {
    DEFAULT_DATABASE_NAME.to_owned()
}

fn validate_namespace_segment(label: &str, value: &str) -> logpose_types::Result<()> {
    if value.trim().is_empty() {
        return Err(LogPoseError::Message(format!("{label} must not be empty")));
    }
    if value.contains('/') {
        return Err(LogPoseError::Message(format!(
            "{label} must not contain '/'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_descriptor_defaults_to_default_database() {
        let descriptor = CollectionDescriptor::new(
            "events",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/catalog-validation"),
        );

        assert_eq!(descriptor.tenant_name, DEFAULT_TENANT_NAME);
        assert_eq!(descriptor.database_name, DEFAULT_DATABASE_NAME);
    }

    #[test]
    fn tenant_descriptor_rejects_empty_name() {
        let error = TenantDescriptor::new("   ")
            .validate()
            .expect_err("blank tenant name should fail");

        assert!(error.to_string().contains("tenant name"));
    }

    #[test]
    fn database_descriptor_rejects_empty_name() {
        let error = DatabaseDescriptor::new_in_tenant(DEFAULT_TENANT_NAME, "   ")
            .validate()
            .expect_err("blank database name should fail");

        assert!(error.to_string().contains("database name"));
    }

    #[test]
    fn namespace_descriptors_reject_reserved_separator() {
        let tenant_error = TenantDescriptor::new("tenant/a")
            .validate()
            .expect_err("slash-containing tenant name should fail");
        assert!(tenant_error.to_string().contains("tenant name"));

        let database_error = DatabaseDescriptor::new_in_tenant("tenant-a", "analytics/v2")
            .validate()
            .expect_err("slash-containing database name should fail");
        assert!(database_error.to_string().contains("database name"));

        let collection_error = CollectionDescriptor::new_in_namespace(
            "tenant-a",
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
