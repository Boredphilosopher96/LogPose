//! Errors produced by graph construction and deserialization.

/// Error returned by graph construction, insertion and deserialization.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GraphError {
    /// Build parameters are out of range.
    #[error("invalid HNSW parameters: {0}")]
    InvalidParams(String),
    /// Vector data given to [`crate::graph::F32Vectors`] is malformed.
    #[error("invalid vector data: {0}")]
    InvalidVectors(String),
    /// Rows must be inserted densely, in order.
    #[error("row {row} cannot be inserted next; the next row id is {expected}")]
    OutOfOrderInsert {
        /// Row the caller tried to insert.
        row: u32,
        /// Row id the graph expects next.
        expected: u32,
    },
    /// A row id is not covered by the vector source.
    #[error("row {row} is outside the vector source, which has {len} rows")]
    RowOutOfRange {
        /// Offending row id.
        row: u32,
        /// Number of rows in the vector source.
        len: usize,
    },
    /// The graph would exceed the supported row count or memory budget.
    #[error("graph exceeds the maximum supported size")]
    TooLarge,
    /// Serialized input ended early.
    #[error("serialized graph is truncated")]
    Truncated,
    /// Serialized input does not start with the graph magic bytes.
    #[error("serialized graph has an unknown magic number")]
    BadMagic,
    /// Serialized input uses a format version this build cannot read.
    #[error("unsupported serialized graph version {0}")]
    UnsupportedVersion(u16),
    /// Serialized input failed its CRC32 check.
    #[error("serialized graph checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        /// Checksum stored in the trailer.
        stored: u32,
        /// Checksum computed over the payload.
        computed: u32,
    },
    /// Serialized input passed its checksum but is structurally invalid.
    #[error("corrupt serialized graph: {0}")]
    Corrupt(String),
}
