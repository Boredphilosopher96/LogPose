//! Vector index abstractions.

/// Index family planned for a collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexKind {
    /// Hierarchical navigable small world graph.
    Hnsw,
    /// Inverted file with product quantization.
    IvfPq,
    /// Brute-force exact search path.
    Flat,
}
