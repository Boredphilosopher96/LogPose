//! Cache keys, file identities, and artifact classes.

use std::sync::atomic::{AtomicU64, Ordering};

/// Artifact classes in priority order from D8, highest first. Eviction
/// starts from the bottom.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactClass {
    /// `VectorSq8` and `VectorGraph` sections: the hot set of vector search.
    GraphAndCodes = 0,
    /// `PkColumn`, `PkSorted`, and `PkFilter` sections (read side).
    PkIndex = 1,
    /// `ScalarInverted`, `ScalarSorted`, and `Stats` sections.
    ScalarIndex = 2,
    /// `ScalarColumn` sections.
    ScalarColumns = 3,
    /// `VectorF32` prefixes and pages.
    RawVectors = 4,
    /// `DynamicJson` block indexes and blocks.
    DynamicJson = 5,
}

impl ArtifactClass {
    /// Number of classes.
    pub const COUNT: usize = 6;

    /// Every class, highest priority first.
    pub const ALL: [Self; Self::COUNT] = [
        Self::GraphAndCodes,
        Self::PkIndex,
        Self::ScalarIndex,
        Self::ScalarColumns,
        Self::RawVectors,
        Self::DynamicJson,
    ];

    /// Position in [`ALL`](Self::ALL), usable as an array index.
    #[must_use]
    pub fn index(self) -> usize {
        self as usize
    }

    /// A short name for logs and `EXPLAIN`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::GraphAndCodes => "graph+codes",
            Self::PkIndex => "pk",
            Self::ScalarIndex => "scalar-index",
            Self::ScalarColumns => "columns",
            Self::RawVectors => "raw-vectors",
            Self::DynamicJson => "dynamic",
        }
    }
}

/// Per-class floors from the design: the share of the budget a class keeps
/// even under pressure from higher classes.
pub const DEFAULT_FLOORS: [f32; ArtifactClass::COUNT] = [0.0, 0.0, 0.0, 0.02, 0.05, 0.01];

/// Process-unique identity of an open file, used in cache keys.
///
/// Ids come from one process-wide counter and are never reused, so entries
/// of a dropped file can never be mistaken for a later file's.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct FileId(u64);

impl FileId {
    /// The id of loads that belong to no file registration (never
    /// allocated by [`next`](Self::next)).
    pub(crate) const DETACHED: Self = Self(0);

    /// Allocate a new id.
    #[must_use]
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// The raw id.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Which part of a section a cache entry holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum CacheUnit {
    /// The whole section payload, checked against the section CRC.
    Section,
    /// The directory of a paged section: the `VectorF32` prefix (header,
    /// null bitmap, page CRCs) or the `DynamicJson` header and block index.
    Index,
    /// One `VectorF32` page or one `DynamicJson` block.
    Page(u32),
}

/// Identity of one cached unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct CacheKey {
    /// The file.
    pub file: FileId,
    /// Index into the file's section table.
    pub section: u32,
    /// Which part of the section.
    pub unit: CacheUnit,
}

impl CacheKey {
    /// Key of a whole section.
    #[must_use]
    pub fn section(file: FileId, section: u32) -> Self {
        Self {
            file,
            section,
            unit: CacheUnit::Section,
        }
    }
}
