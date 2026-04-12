//! Metadata and collection catalog abstractions.

/// Logical collection metadata scaffold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionDescriptor {
    /// Human-readable collection name.
    pub name: String,
    /// Embedding dimensions expected for the collection.
    pub dimensions: usize,
}
