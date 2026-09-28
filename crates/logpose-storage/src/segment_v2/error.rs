//! Typed errors for building and reading segment v2 files.

use super::format::SectionKind;
use logpose_types::{
    CorruptionKind, LogPoseError,
    record::PrimaryKey,
    schema::{FieldId, FieldType, PrimaryKeyType},
};
use std::{fmt, io, sync::Arc};
use thiserror::Error;

/// A part of a segment file, named in corruption errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Region {
    /// The whole file: its length or its overall layout.
    File,
    /// The 128-byte file header.
    Header,
    /// The 64-byte footer.
    Footer,
    /// The section table.
    SectionTable,
    /// Zero padding before the byte at this offset.
    Padding {
        /// Absolute offset of the first padding byte that was checked.
        offset: u64,
    },
    /// One section payload.
    Section {
        /// Index into the section table.
        index: usize,
        /// Raw section kind code.
        kind: u16,
    },
    /// The prefix (header, nulls, page CRCs) of a `VectorF32` section.
    VectorPrefix {
        /// Index into the section table.
        index: usize,
    },
    /// One page of a `VectorF32` section.
    VectorPage {
        /// Index into the section table.
        index: usize,
        /// Page number.
        page: u32,
    },
    /// The header and block index of a `DynamicJson` section.
    DynamicIndex {
        /// Index into the section table.
        index: usize,
    },
    /// One block of a `DynamicJson` section.
    DynamicBlock {
        /// Index into the section table.
        index: usize,
        /// Block number.
        block: u32,
    },
}

impl fmt::Display for Region {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File => formatter.write_str("file"),
            Self::Header => formatter.write_str("header"),
            Self::Footer => formatter.write_str("footer"),
            Self::SectionTable => formatter.write_str("section table"),
            Self::Padding { offset } => write!(formatter, "padding at byte {offset}"),
            Self::Section { index, kind } => {
                write!(
                    formatter,
                    "section {index} ({})",
                    SectionKind::describe(*kind)
                )
            }
            Self::VectorPrefix { index } => write!(formatter, "vector prefix of section {index}"),
            Self::VectorPage { index, page } => {
                write!(formatter, "vector page {page} of section {index}")
            }
            Self::DynamicIndex { index } => {
                write!(formatter, "dynamic block index of section {index}")
            }
            Self::DynamicBlock { index, block } => {
                write!(formatter, "dynamic block {block} of section {index}")
            }
        }
    }
}

/// Reasons a segment cannot be built, written, opened, or read.
///
/// Every defect in stored bytes is [`SegmentError::Corrupt`] or
/// [`SegmentError::Checksum`] (see [`is_corruption`](Self::is_corruption)).
/// The other variants reject invalid builder input.
///
/// Errors are cheap to clone: a failed load is shared by every caller that
/// waited on it (see [`BufferCache`](crate::cache::BufferCache)).
#[derive(Clone, Debug, Error)]
pub enum SegmentError {
    /// Reading or writing the underlying file failed.
    #[error("segment I/O failed: {0}")]
    Io(Arc<io::Error>),
    /// The caller asked for a section, page, block, or row the segment does
    /// not have. A bug in the caller, never a defect of the file, so it is
    /// not corruption.
    #[error("segment access out of range: {what}")]
    OutOfRange {
        /// What was asked for.
        what: String,
    },
    /// A cached load did not finish: the loader panicked, or its executor
    /// shut down before running it. Nothing was cached; a retry loads again.
    #[error("a segment load was aborted before it finished")]
    LoadAborted,
    /// A CRC does not match the bytes it covers.
    #[error("segment checksum mismatch in {region}")]
    Checksum {
        /// Where the mismatch is.
        region: Region,
    },
    /// Stored bytes are structurally invalid.
    #[error("segment is corrupt in {region}: {detail}")]
    Corrupt {
        /// Where the defect is.
        region: Region,
        /// What is wrong.
        detail: String,
    },
    /// The file declares a format version this build cannot read.
    #[error("unsupported segment format version {version}")]
    UnsupportedVersion {
        /// The declared version.
        version: u32,
    },
    /// A field id is not declared by the builder's schema.
    #[error("field {field} is not declared by the segment schema")]
    UnknownField {
        /// The field id.
        field: FieldId,
    },
    /// A field id names a field of another family (vector versus scalar).
    #[error("field {field} is not a {expected} field")]
    FieldKind {
        /// The field id.
        field: FieldId,
        /// The family the call needs.
        expected: &'static str,
    },
    /// A vector has the wrong number of components.
    #[error("vector field {field} has {actual} dimensions; the schema declares {expected}")]
    VectorDimensions {
        /// The field id.
        field: FieldId,
        /// Declared dimensions.
        expected: u32,
        /// Supplied dimensions.
        actual: usize,
    },
    /// A scalar value does not fit its field type.
    #[error("field {field} expects {expected}, found {found}")]
    ValueType {
        /// The field id.
        field: FieldId,
        /// The declared type.
        expected: FieldType,
        /// What was supplied.
        found: &'static str,
    },
    /// A field of the current row was set twice.
    #[error("field {field} was already set for this row")]
    FieldAlreadySet {
        /// The field id.
        field: FieldId,
    },
    /// The dynamic field of the current row was set twice.
    #[error("the dynamic field was already set for this row")]
    DynamicAlreadySet,
    /// Dynamic field bytes are not one JSON object in the binary value codec.
    #[error("dynamic field bytes are invalid: {0}")]
    InvalidDynamic(String),
    /// A primary key has the wrong type for the schema.
    #[error("primary key has type {found}, but the schema's key type is {expected}")]
    PrimaryKeyType {
        /// The schema's key type.
        expected: PrimaryKeyType,
        /// The supplied key's type.
        found: PrimaryKeyType,
    },
    /// Two rows share a primary key.
    #[error("primary key {pk} appears in more than one row")]
    DuplicatePrimaryKey {
        /// The repeated key.
        pk: PrimaryKey,
    },
    /// A sequence number of zero, which means "nothing".
    #[error("row sequence numbers must be at least 1")]
    InvalidSeqNo,
    /// The segment would exceed a format limit.
    #[error("segment exceeds a format limit: {what}")]
    TooLarge {
        /// Which limit.
        what: &'static str,
    },
    /// A value or the schema could not be encoded.
    #[error("failed to encode segment data: {0}")]
    Encode(String),
    /// An index section was added twice for the same kind and field.
    #[error("an index section of kind {kind:?} for field {field} was already added")]
    DuplicateIndexSection {
        /// The section kind.
        kind: SectionKind,
        /// The field id.
        field: FieldId,
    },
}

impl From<io::Error> for SegmentError {
    fn from(error: io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}

impl SegmentError {
    /// Whether this error reports damaged or inconsistent stored bytes.
    #[must_use]
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Self::Checksum { .. } | Self::Corrupt { .. } | Self::UnsupportedVersion { .. }
        )
    }

    /// The corruption kind of this error: [`CorruptionKind::Index`] for a
    /// defect in an index section (`VectorSq8`, `VectorGraph`,
    /// `ScalarInverted`, `ScalarSorted`), [`CorruptionKind::Segment`]
    /// otherwise.
    #[must_use]
    pub fn corruption_kind(&self) -> CorruptionKind {
        let region = match self {
            Self::Checksum { region } | Self::Corrupt { region, .. } => *region,
            _ => return CorruptionKind::Segment,
        };
        match region {
            Region::Section { kind, .. }
                if matches!(
                    SectionKind::from_code(kind),
                    Some(
                        SectionKind::VectorSq8
                            | SectionKind::VectorGraph
                            | SectionKind::ScalarInverted
                            | SectionKind::ScalarSorted
                    )
                ) =>
            {
                CorruptionKind::Index
            }
            _ => CorruptionKind::Segment,
        }
    }

    pub(super) fn out_of_range(what: impl Into<String>) -> Self {
        Self::OutOfRange { what: what.into() }
    }

    pub(super) fn corrupt(region: Region, detail: impl Into<String>) -> Self {
        Self::Corrupt {
            region,
            detail: detail.into(),
        }
    }
}

impl From<SegmentError> for LogPoseError {
    fn from(error: SegmentError) -> Self {
        if error.is_corruption() {
            return LogPoseError::corrupt(error.corruption_kind(), error.to_string());
        }
        match error {
            SegmentError::Io(source) => LogPoseError::Io {
                context: "segment I/O failed".to_owned(),
                source,
            },
            // Every other variant rejects builder input that the engine produced itself.
            other => LogPoseError::internal(other.to_string()),
        }
    }
}

/// A structural defect found while decoding bytes whose region the caller
/// knows. The reader turns it into [`SegmentError::Corrupt`].
#[derive(Debug)]
pub(crate) struct Malformed(pub(crate) String);

impl Malformed {
    pub(crate) fn new(detail: impl Into<String>) -> Self {
        Self(detail.into())
    }

    pub(crate) fn at(self, region: Region) -> SegmentError {
        SegmentError::corrupt(region, self.0)
    }
}

/// Result of decoding bytes whose region the caller knows.
pub(crate) type DecodeResult<T> = Result<T, Malformed>;
