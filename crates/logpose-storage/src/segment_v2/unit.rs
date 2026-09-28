//! Load units: the granularity at which segment bytes are read, verified,
//! and cached.
//!
//! | Unit | Verified by | Cache unit |
//! | --- | --- | --- |
//! | a whole section | the section CRC | [`CacheUnit::Section`] |
//! | a `VectorF32` prefix (header, nulls, page CRCs) | `prefix_crc` | [`CacheUnit::Index`] |
//! | one `VectorF32` page | its page CRC, read from the prefix | [`CacheUnit::Page`] |
//! | a `DynamicJson` header and block index | `index_crc` | [`CacheUnit::Index`] |
//! | one `DynamicJson` block | its CRC in the block index | [`CacheUnit::Page`] |
//!
//! A loader returns bytes only after they verified, so everything in the
//! cache is known good and a hit decodes without checking a CRC again.

use super::index::attach_decoded;
use super::{
    dynamic::{DYNAMIC_ENCODING_BLOCKS, DynamicBlockRef, DynamicIndex, IndexError},
    error::{Region, SegmentError},
    format::{SectionEntry, SectionKind, crc},
    source::SectionSource,
    vector::{PrefixError, VECTOR_ENCODING_F32, VectorPrefix},
};
use crate::cache::{AlignedBytes, ArtifactClass, CacheKey, CacheUnit, FileId};
use std::sync::Arc;

/// One load unit of a segment. Build it with
/// [`SegmentReader::section_unit`](super::SegmentReader::section_unit),
/// [`SegmentReader::vector_prefix_unit`](super::SegmentReader::vector_prefix_unit),
/// [`SegmentReader::dynamic_index_unit`](super::SegmentReader::dynamic_index_unit),
/// [`VectorHandle::page_unit`](super::VectorHandle::page_unit), or
/// [`DynamicHandle::block_unit`](super::DynamicHandle::block_unit).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentUnit {
    pub(super) index: usize,
    pub(super) entry: SectionEntry,
    pub(super) part: Part,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Part {
    Whole,
    VectorPrefix {
        rows: u32,
    },
    VectorPage {
        page: u32,
        start: u64,
        end: u64,
        crc: u32,
    },
    DynamicIndex {
        rows: u32,
    },
    DynamicBlock {
        block: u32,
        location: DynamicBlockRef,
    },
}

impl SegmentUnit {
    /// Index of the unit's section in the section table.
    #[must_use]
    pub fn section_index(&self) -> usize {
        self.index
    }

    /// Which part of the section this unit is.
    #[must_use]
    pub fn cache_unit(&self) -> CacheUnit {
        match self.part {
            Part::Whole => CacheUnit::Section,
            Part::VectorPrefix { .. } | Part::DynamicIndex { .. } => CacheUnit::Index,
            Part::VectorPage { page, .. } => CacheUnit::Page(page),
            Part::DynamicBlock { block, .. } => CacheUnit::Page(block),
        }
    }

    /// The unit's key in a cache, for a file registered as `file`.
    #[must_use]
    pub fn key(&self, file: FileId) -> CacheKey {
        CacheKey {
            file,
            section: u32::try_from(self.index).unwrap_or(u32::MAX),
            unit: self.cache_unit(),
        }
    }

    /// The class the unit is charged to (and reported under).
    #[must_use]
    pub fn class(&self) -> ArtifactClass {
        class_of(self.entry.section_kind())
    }

    /// Whether the unit may enter the cache. Schema snapshots are parsed
    /// at open, row meta is transient (recovery reads it once), unknown
    /// kinds are opaque, and whole paged sections (`VectorF32`,
    /// `DynamicJson`) are read only by full scans, which load their pages
    /// and blocks through their own units.
    #[must_use]
    pub fn cacheable(&self) -> bool {
        let kind = self.entry.section_kind();
        match self.part {
            Part::Whole => matches!(
                kind,
                Some(
                    SectionKind::PkColumn
                        | SectionKind::PkSorted
                        | SectionKind::PkFilter
                        | SectionKind::Stats
                        | SectionKind::VectorSq8
                        | SectionKind::VectorGraph
                        | SectionKind::ScalarColumn
                        | SectionKind::ScalarInverted
                        | SectionKind::ScalarSorted
                )
            ),
            _ => true,
        }
    }

    /// Bytes the unit reads, when known before reading. A prefix or block
    /// index learns its own length from its first 64 bytes.
    #[must_use]
    pub fn len_hint(&self) -> Option<u64> {
        match self.part {
            Part::Whole => Some(self.entry.length),
            Part::VectorPage { start, end, .. } => Some(end - start),
            Part::DynamicBlock { location, .. } => Some(u64::from(location.len)),
            Part::VectorPrefix { .. } | Part::DynamicIndex { .. } => None,
        }
    }

    fn region(&self) -> Region {
        let index = self.index;
        match self.part {
            Part::Whole => section_region(index, &self.entry),
            Part::VectorPrefix { .. } => Region::VectorPrefix { index },
            Part::VectorPage { page, .. } => Region::VectorPage { index, page },
            Part::DynamicIndex { .. } => Region::DynamicIndex { index },
            Part::DynamicBlock { block, .. } => Region::DynamicBlock { index, block },
        }
    }

    /// Read and verify the unit. This is the loader the cache runs.
    ///
    /// # Errors
    ///
    /// I/O errors, [`SegmentError::Checksum`] on a CRC mismatch, or
    /// [`SegmentError::Corrupt`] for structurally invalid bytes.
    pub fn load<S: SectionSource + ?Sized>(
        &self,
        source: &S,
        file_len: u64,
    ) -> Result<AlignedBytes, SegmentError> {
        let region = self.region();
        let read = |offset: u64, len: u64| {
            read_in_section(
                source,
                file_len,
                self.index,
                &self.entry,
                offset,
                len,
                region,
            )
        };
        match self.part {
            Part::Whole => {
                let bytes = read(0, self.entry.length)?;
                check_crc(&bytes, self.entry.crc32c, region)?;
                Ok(bytes)
            }
            Part::VectorPage {
                start, end, crc, ..
            } => {
                let bytes = read(start, end - start)?;
                check_crc(&bytes, crc, region)?;
                Ok(bytes)
            }
            Part::DynamicBlock { location, .. } => {
                let bytes = read(location.offset, u64::from(location.len))?;
                check_crc(&bytes, location.crc32c, region)?;
                Ok(bytes)
            }
            Part::VectorPrefix { rows } => {
                if self.entry.encoding != VECTOR_ENCODING_F32 {
                    return Err(SegmentError::corrupt(region, "unknown vector encoding"));
                }
                let head = read(0, VectorPrefix::HEADER_LEN as u64)?;
                let len = VectorPrefix::prefix_len(&head, self.entry.length)
                    .map_err(|error| error.at(region))?;
                let bytes = Arc::new(read(0, len as u64)?);
                VectorPrefix::decode(&bytes, self.entry.aux32, rows, self.entry.length)
                    .map_err(|error| prefix_error(error, self.index, region))?;
                Ok(Arc::unwrap_or_clone(bytes))
            }
            Part::DynamicIndex { rows } => {
                if self.entry.encoding != DYNAMIC_ENCODING_BLOCKS {
                    return Err(SegmentError::corrupt(region, "unknown dynamic encoding"));
                }
                let head = read(0, DynamicIndex::HEADER_LEN as u64)?;
                let len = DynamicIndex::index_len(&head, self.entry.length)
                    .map_err(|error| error.at(region))?;
                let bytes = read(0, len as u64)?;
                DynamicIndex::decode(&bytes, rows, self.entry.length).map_err(
                    |error| match error {
                        IndexError::Checksum => SegmentError::Checksum { region },
                        IndexError::Malformed(error) => error.at(region),
                    },
                )?;
                Ok(bytes)
            }
        }
    }
}

impl SegmentUnit {
    /// [`load`](Self::load), then, for a whole index section, decode it and
    /// attach the decoded form (`index::attach_decoded`) so the cache charges
    /// it and every hit reuses it. A payload that verifies but does not
    /// decode is [`SegmentError::Corrupt`] in the section.
    ///
    /// # Errors
    ///
    /// As [`load`](Self::load), plus decode failures.
    pub fn load_decoded<S: SectionSource + ?Sized>(
        &self,
        source: &S,
        file_len: u64,
        row_count: u32,
    ) -> Result<AlignedBytes, SegmentError> {
        let bytes = self.load(source, file_len)?;
        if matches!(self.part, Part::Whole) {
            attach_decoded(self.entry.section_kind(), row_count, &bytes)
                .map_err(|detail| SegmentError::corrupt(self.region(), detail))?;
        }
        Ok(bytes)
    }
}

/// The class a section kind's units are charged to.
#[must_use]
pub fn class_of(kind: Option<SectionKind>) -> ArtifactClass {
    match kind {
        Some(SectionKind::VectorSq8 | SectionKind::VectorGraph) => ArtifactClass::GraphAndCodes,
        Some(
            SectionKind::PkColumn
            | SectionKind::PkSorted
            | SectionKind::PkFilter
            | SectionKind::RowMeta
            | SectionKind::SchemaSnapshot,
        ) => ArtifactClass::PkIndex,
        Some(SectionKind::Stats | SectionKind::ScalarInverted | SectionKind::ScalarSorted) => {
            ArtifactClass::ScalarIndex
        }
        Some(SectionKind::VectorF32) => ArtifactClass::RawVectors,
        Some(SectionKind::DynamicJson) => ArtifactClass::DynamicJson,
        Some(SectionKind::ScalarColumn) | None => ArtifactClass::ScalarColumns,
    }
}

fn check_crc(bytes: &[u8], expected: u32, region: Region) -> Result<(), SegmentError> {
    if crc(bytes) == expected {
        Ok(())
    } else {
        Err(SegmentError::Checksum { region })
    }
}

pub(super) fn section_region(index: usize, entry: &SectionEntry) -> Region {
    Region::Section {
        index,
        kind: entry.kind,
    }
}

pub(super) fn prefix_error(error: PrefixError, index: usize, region: Region) -> SegmentError {
    match error {
        PrefixError::Checksum => SegmentError::Checksum { region },
        PrefixError::Page(page) => SegmentError::Checksum {
            region: Region::VectorPage { index, page },
        },
        PrefixError::Malformed(error) => error.at(region),
    }
}

/// Read `[offset, offset + len)`, refusing ranges past `file_len` before
/// allocating.
pub(super) fn read_range<S: SectionSource + ?Sized>(
    source: &S,
    file_len: u64,
    offset: u64,
    len: u64,
    region: Region,
) -> Result<AlignedBytes, SegmentError> {
    let fits = offset.checked_add(len).is_some_and(|end| end <= file_len);
    if !fits {
        return Err(SegmentError::corrupt(
            region,
            format!("range {offset}+{len} exceeds the file length {file_len}"),
        ));
    }
    let len = usize::try_from(len)
        .map_err(|_| SegmentError::corrupt(region, "range does not fit in memory"))?;
    let mut buf = AlignedBytes::zeroed(len);
    source.read_exact_at(buf.as_bytes_mut(), offset)?;
    Ok(buf)
}

/// Read bytes `[offset, offset + len)` of section `index`.
pub(super) fn read_in_section<S: SectionSource + ?Sized>(
    source: &S,
    file_len: u64,
    index: usize,
    entry: &SectionEntry,
    offset: u64,
    len: u64,
    region: Region,
) -> Result<AlignedBytes, SegmentError> {
    let in_bounds = offset
        .checked_add(len)
        .is_some_and(|end| end <= entry.length);
    if !in_bounds {
        return Err(SegmentError::corrupt(
            region,
            format!("range {offset}+{len} exceeds section {index}"),
        ));
    }
    read_range(source, file_len, entry.offset + offset, len, region)
}
