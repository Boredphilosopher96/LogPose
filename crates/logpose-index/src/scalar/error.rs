//! Errors returned by scalar index construction, mutation and decoding.

use super::KeyKind;

/// Error returned by scalar index operations.
///
/// Queries never fail; only building, mutating, freezing and decoding can.
#[derive(Debug, thiserror::Error)]
pub enum ScalarError {
    /// A floating point key was NaN, which has no place in a total order.
    #[error("NaN cannot be used as a scalar index key")]
    NanKey,
    /// A key of one kind was inserted into an index of another kind.
    #[error("key kind mismatch: index holds {expected} keys, got a {found} key")]
    KindMismatch {
        /// Kind the index was created with.
        expected: KeyKind,
        /// Kind of the rejected key.
        found: KeyKind,
    },
    /// A row was recorded both as null and as having a value.
    #[error("row {row} is recorded both as null and as having a value")]
    NullConflict {
        /// Offending row id.
        row: u32,
    },
    /// A freeze remapping sent two source rows to the same target row.
    #[error("row remapping is not injective: two rows map to row {row}")]
    NonInjectiveRemap {
        /// Target row id that was hit twice.
        row: u32,
    },
    /// The index would hold more than `u32::MAX` `(key, row)` entries.
    #[error("scalar index exceeds u32::MAX entries")]
    TooManyEntries,
    /// A string key is longer than `u32::MAX` bytes and cannot be encoded.
    #[error("string key longer than u32::MAX bytes")]
    KeyTooLong,
    /// Serialized bytes do not start with the scalar index magic.
    #[error("not a scalar index: bad magic")]
    BadMagic,
    /// Serialized bytes use a format version this build cannot read.
    #[error("unsupported scalar index format version {0}")]
    UnsupportedVersion(u16),
    /// The stored CRC32 does not match the bytes.
    #[error("scalar index checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        /// Checksum stored in the trailer.
        stored: u32,
        /// Checksum computed over the header and body.
        computed: u32,
    },
    /// Serialized bytes passed the checksum but are structurally invalid.
    #[error("corrupt scalar index: {0}")]
    Corrupt(&'static str),
    /// Writing a bitmap failed while encoding.
    #[error("failed to encode scalar index: {0}")]
    Encode(#[from] std::io::Error),
}
