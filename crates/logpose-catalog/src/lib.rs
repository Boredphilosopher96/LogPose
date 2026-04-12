//! Metadata and collection catalog abstractions.

use logpose_types::{CollectionId, DistanceMetric, LogPoseError, RemoteBlobConfig, WriteOperation};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Default mutable-op threshold before the engine should flush.
pub const DEFAULT_FLUSH_THRESHOLD_OPS: usize = 10_000;
/// Default approximate byte threshold before the engine should flush.
pub const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
/// Default number of immutable segments before compaction is recommended.
pub const DEFAULT_COMPACTION_THRESHOLD_SEGMENTS: usize = 4;

/// Logical collection metadata scaffold.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionDescriptor {
    /// Stable collection identifier.
    pub collection_id: CollectionId,
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
        let collection_id = CollectionId::default();
        Self {
            collection_id: collection_id.clone(),
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

    /// Validate collection-level configuration values.
    pub fn validate(&self) -> logpose_types::Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
