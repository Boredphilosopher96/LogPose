//! Metadata and collection catalog abstractions.

use logpose_types::{CollectionId, DistanceMetric, RemoteBlobConfig, WriteOperation};
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

    /// Validate whether an operation matches this collection's configured dimensions.
    pub fn validate_operation(&self, operation: &WriteOperation) -> logpose_types::Result<()> {
        operation.validate_dimensions(self.dimensions)
    }
}
