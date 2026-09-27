//! [`SegmentReader`]: open a segment from its two ends, then read and
//! verify sections lazily.
//!
//! Opening reads the header, the footer, the section table, and the
//! `SchemaSnapshot`; it checks their CRCs, the file length, the section
//! layout (every section 64-byte aligned, in file order, back to back with
//! only alignment padding between them), and which sections each field
//! has. Every other section is read and verified only when asked for. Every
//! read is bounds-checked against the validated file length before a buffer
//! is allocated, so a corrupt length can never cause a large allocation.

use super::{
    builder::{IndexSectionKind, ROW_META_U32_DELTA, ROW_META_U64, SCHEMA_ENCODING_POSTCARD},
    column::ScalarColumn,
    dynamic::{DYNAMIC_ENCODING_BLOCKS, DynamicBlock, DynamicIndex, IndexError},
    error::{DecodeResult, Malformed, Region, SegmentError},
    format::{
        ENTRY_LEN, FOOTER_LEN, Footer, HEADER_LEN, SECTION_ALIGN, SectionEntry, SectionKind,
        SegmentHeader, crc,
    },
    le::{Cursor, align_up, f32s_from_le, usize_from, usize_from_u64},
    pk::{PkColumn, PkFilter, PkSorted},
    source::SectionSource,
    stats::{STATS_ENCODING_POSTCARD, SegmentStats},
    vector::{PrefixError, VECTOR_ENCODING_F32, VectorPrefix},
};
use logpose_types::{
    SeqNo,
    record::PrimaryKey,
    schema::{CollectionSchema, FieldId, FieldRef, FieldType},
};
use logpose_wal::codec::{F32Bytes, RowImage, ValueBytes, WirePk};
use std::{collections::BTreeSet, sync::Arc};
use twox_hash::XxHash3_64;

/// Sections every segment must have.
const REQUIRED: [SectionKind; 6] = [
    SectionKind::SchemaSnapshot,
    SectionKind::RowMeta,
    SectionKind::PkColumn,
    SectionKind::PkSorted,
    SectionKind::PkFilter,
    SectionKind::Stats,
];

/// A `VectorF32` section with its verified prefix.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorHandle {
    index: usize,
    entry: SectionEntry,
    prefix: VectorPrefix,
}

impl VectorHandle {
    /// Index of the section in the table.
    #[must_use]
    pub fn section_index(&self) -> usize {
        self.index
    }

    /// The decoded prefix: geometry, nulls, and page CRCs.
    #[must_use]
    pub fn prefix(&self) -> &VectorPrefix {
        &self.prefix
    }
}

/// A `DynamicJson` section with its verified block index.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicHandle {
    index: usize,
    entry: SectionEntry,
    blocks: DynamicIndex,
}

impl DynamicHandle {
    /// Index of the section in the table.
    #[must_use]
    pub fn section_index(&self) -> usize {
        self.index
    }

    /// The decoded block index.
    #[must_use]
    pub fn blocks(&self) -> &DynamicIndex {
        &self.blocks
    }
}

/// One row read back from a segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentRow {
    /// Sequence number of the operation that wrote the row.
    pub seq_no: SeqNo,
    /// The row, keyed by field id.
    pub image: RowImage,
}

/// Reads one segment v2 file through a [`SectionSource`].
#[derive(Debug)]
pub struct SegmentReader<S> {
    source: S,
    file_len: u64,
    header: SegmentHeader,
    footer: Footer,
    sections: Arc<[SectionEntry]>,
    schema: Arc<CollectionSchema>,
}

impl<S: SectionSource> SegmentReader<S> {
    /// Open a segment: read and verify the header, footer, section table,
    /// and schema snapshot.
    ///
    /// # Errors
    ///
    /// [`SegmentError::Io`] if the source fails, and a corruption error
    /// ([`SegmentError::is_corruption`]) if anything read is invalid.
    pub fn open(source: S) -> Result<Self, SegmentError> {
        let file_len = source.len()?;
        let min_len = (HEADER_LEN + FOOTER_LEN) as u64;
        if file_len < min_len {
            return Err(SegmentError::corrupt(
                Region::File,
                format!("{file_len} bytes is shorter than a header and footer"),
            ));
        }
        let header_bytes = read_range(&source, file_len, 0, HEADER_LEN as u64, Region::Header)?;
        let (header, header_crc) = SegmentHeader::decode(&header_bytes)?;
        let footer_bytes = read_range(
            &source,
            file_len,
            file_len - FOOTER_LEN as u64,
            FOOTER_LEN as u64,
            Region::Footer,
        )?;
        let footer = Footer::decode(&footer_bytes)?;
        if footer.file_len != file_len {
            return Err(SegmentError::corrupt(
                Region::Footer,
                format!(
                    "footer records {} bytes, file has {file_len}",
                    footer.file_len
                ),
            ));
        }
        if footer.header_crc != header_crc {
            return Err(SegmentError::corrupt(
                Region::Footer,
                "footer's header CRC does not match the header",
            ));
        }
        let table_len = u64::from(footer.section_count) * ENTRY_LEN as u64;
        let table_fits = footer.table_offset >= HEADER_LEN as u64
            && footer.table_offset.is_multiple_of(SECTION_ALIGN)
            && footer
                .table_offset
                .checked_add(table_len)
                .and_then(|end| end.checked_add(FOOTER_LEN as u64))
                == Some(file_len);
        if !table_fits {
            return Err(SegmentError::corrupt(
                Region::Footer,
                "section table location does not fit the file",
            ));
        }
        let table = read_range(
            &source,
            file_len,
            footer.table_offset,
            table_len,
            Region::SectionTable,
        )?;
        if crc(&table) != footer.table_crc {
            return Err(SegmentError::Checksum {
                region: Region::SectionTable,
            });
        }
        let sections = table
            .chunks_exact(ENTRY_LEN)
            .map(SectionEntry::decode)
            .collect::<Result<Vec<_>, _>>()?;
        check_layout(&sections, footer.table_offset)?;
        check_kinds(&sections)?;
        let schema = load_schema(&source, file_len, &header, &sections)?;
        let reader = Self {
            source,
            file_len,
            header,
            footer,
            sections: sections.into(),
            schema: Arc::new(schema),
        };
        reader.check_fields()?;
        Ok(reader)
    }

    /// The file header.
    #[must_use]
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// The footer.
    #[must_use]
    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    /// Total file length.
    #[must_use]
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Number of rows.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        self.header.row_count
    }

    /// The schema snapshot the segment was written under.
    #[must_use]
    pub fn schema(&self) -> &Arc<CollectionSchema> {
        &self.schema
    }

    /// The section table.
    #[must_use]
    pub fn sections(&self) -> &[SectionEntry] {
        &self.sections
    }

    /// The underlying source.
    #[must_use]
    pub fn source(&self) -> &S {
        &self.source
    }

    /// Index of the section of `kind` for `field` (`None` for sections that
    /// are not per field).
    #[must_use]
    pub fn find_section(&self, kind: SectionKind, field: Option<FieldId>) -> Option<usize> {
        self.sections
            .iter()
            .position(|entry| entry.kind == kind.code() && entry.field == field)
    }

    fn entry(&self, index: usize) -> Result<SectionEntry, SegmentError> {
        self.sections.get(index).copied().ok_or_else(|| {
            SegmentError::corrupt(Region::SectionTable, format!("no section {index}"))
        })
    }

    fn required(&self, kind: SectionKind) -> Result<(usize, SectionEntry), SegmentError> {
        required(&self.sections, kind)
    }

    /// Read bytes `[offset, offset + len)` of section `index`.
    fn read_in_section(
        &self,
        index: usize,
        entry: &SectionEntry,
        offset: u64,
        len: u64,
        region: Region,
    ) -> Result<Vec<u8>, SegmentError> {
        let in_bounds = offset
            .checked_add(len)
            .is_some_and(|end| end <= entry.length);
        if !in_bounds {
            return Err(SegmentError::corrupt(
                region,
                format!("range {offset}+{len} exceeds section {index}"),
            ));
        }
        read_range(
            &self.source,
            self.file_len,
            entry.offset + offset,
            len,
            region,
        )
    }

    /// Read the whole payload of section `index` and verify its CRC.
    ///
    /// This is the loader for whole-section cache units and the accessor
    /// for opaque index payloads.
    ///
    /// # Errors
    ///
    /// [`SegmentError::Io`], or [`SegmentError::Checksum`] on a mismatch.
    pub fn read_section(&self, index: usize) -> Result<Vec<u8>, SegmentError> {
        read_verified(&self.source, self.file_len, index, &self.entry(index)?)
    }

    /// Per-field sections must name a field of the right family, and every
    /// row-shaped section must declare the header's row count.
    fn check_fields(&self) -> Result<(), SegmentError> {
        for (index, entry) in self.sections.iter().enumerate() {
            let Some(kind) = entry.section_kind() else {
                continue;
            };
            let region = section_region(index, entry);
            let rows = u64::from(self.header.row_count);
            match kind {
                SectionKind::PkColumn
                | SectionKind::PkSorted
                | SectionKind::ScalarColumn
                | SectionKind::DynamicJson
                    if entry.aux64 != rows =>
                {
                    return Err(SegmentError::corrupt(
                        region,
                        "row count differs from the header",
                    ));
                }
                _ => {}
            }
            let Some(field) = entry.field else {
                continue;
            };
            let found = self.schema.field_by_id(field);
            let ok = match kind {
                SectionKind::VectorF32 => matches!(
                    found,
                    Some(FieldRef::Vector(vector))
                        if vector.dimensions == entry.aux32 && entry.aux64 == rows
                ),
                SectionKind::VectorSq8 | SectionKind::VectorGraph => {
                    matches!(found, Some(FieldRef::Vector(_)))
                }
                SectionKind::ScalarColumn
                | SectionKind::ScalarInverted
                | SectionKind::ScalarSorted => {
                    matches!(found, Some(FieldRef::Scalar(_)))
                }
                _ => false,
            };
            if !ok {
                return Err(SegmentError::corrupt(
                    region,
                    format!("section does not match field {field} of the schema snapshot"),
                ));
            }
        }
        Ok(())
    }

    /// Per-row sequence numbers.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn row_meta(&self) -> Result<Vec<SeqNo>, SegmentError> {
        let (index, entry) = self.required(SectionKind::RowMeta)?;
        let region = section_region(index, &entry);
        let bytes = self.read_section(index)?;
        self.decode_row_meta(&bytes, &entry)
            .map_err(|error| error.at(region))
    }

    fn decode_row_meta(&self, bytes: &[u8], entry: &SectionEntry) -> DecodeResult<Vec<SeqNo>> {
        let rows = usize_from(self.header.row_count);
        let mut cursor = Cursor::new(bytes);
        let seqs = match entry.encoding {
            ROW_META_U64 => {
                if entry.aux64 != 0 {
                    return Err(Malformed::new("plain row meta has a base"));
                }
                cursor.u64s(rows)?
            }
            ROW_META_U32_DELTA => cursor
                .u32s(rows)?
                .into_iter()
                .map(|delta| {
                    entry
                        .aux64
                        .checked_add(u64::from(delta))
                        .ok_or_else(|| Malformed::new("sequence number overflows"))
                })
                .collect::<DecodeResult<Vec<_>>>()?,
            other => return Err(Malformed::new(format!("unknown row meta encoding {other}"))),
        };
        cursor.finish()?;
        let min = seqs.iter().copied().min().unwrap_or(0);
        let max = seqs.iter().copied().max().unwrap_or(0);
        if min != self.header.min_seq_no || max != self.header.max_seq_no {
            return Err(Malformed::new("sequence range differs from the header"));
        }
        if seqs.contains(&0) {
            return Err(Malformed::new("sequence number 0 in row meta"));
        }
        Ok(seqs)
    }

    /// Primary keys in row order.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn pk_column(&self) -> Result<PkColumn, SegmentError> {
        let (index, entry) = self.required(SectionKind::PkColumn)?;
        let region = section_region(index, &entry);
        let bytes = self.read_section(index)?;
        self.check_pk_encoding(&entry, region)?;
        PkColumn::decode(&bytes, entry.encoding, usize_from(self.header.row_count))
            .map_err(|error| error.at(region))
    }

    fn check_pk_encoding(&self, entry: &SectionEntry, region: Region) -> Result<(), SegmentError> {
        if entry.encoding == super::pk::encoding_for(self.schema.primary_key_type()) {
            Ok(())
        } else {
            Err(SegmentError::corrupt(
                region,
                "pk encoding differs from the key type",
            ))
        }
    }

    /// Row ids sorted by primary key.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn pk_sorted(&self) -> Result<PkSorted, SegmentError> {
        let (index, entry) = self.required(SectionKind::PkSorted)?;
        let region = section_region(index, &entry);
        let bytes = self.read_section(index)?;
        self.check_pk_encoding(&entry, region)?;
        PkSorted::decode(&bytes, entry.encoding, usize_from(self.header.row_count))
            .map_err(|error| error.at(region))
    }

    /// The primary-key filter.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn pk_filter(&self) -> Result<PkFilter, SegmentError> {
        let (index, entry) = self.required(SectionKind::PkFilter)?;
        let region = section_region(index, &entry);
        let bytes = self.read_section(index)?;
        PkFilter::decode(&bytes, entry.encoding).map_err(|error| error.at(region))
    }

    /// The row holding `pk`, if any: a filter probe, then a binary search.
    /// Loads the pk sections on every call; the engine caches them.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn find_row(&self, pk: &PrimaryKey) -> Result<Option<u32>, SegmentError> {
        if pk.key_type() != self.schema.primary_key_type() || !self.pk_filter()?.may_contain(pk) {
            return Ok(None);
        }
        let sorted = self.pk_sorted()?;
        let column = match &sorted {
            PkSorted::Int64 { .. } => PkColumn::Int64(Vec::new()),
            PkSorted::String { .. } => self.pk_column()?,
        };
        Ok(sorted.find(&column, pk))
    }

    /// Segment statistics.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn stats(&self) -> Result<SegmentStats, SegmentError> {
        let (index, entry) = self.required(SectionKind::Stats)?;
        let region = section_region(index, &entry);
        let bytes = self.read_section(index)?;
        if entry.encoding != STATS_ENCODING_POSTCARD {
            return Err(SegmentError::corrupt(region, "unknown stats encoding"));
        }
        let stats = SegmentStats::decode(&bytes).map_err(|error| error.at(region))?;
        if stats.row_count != self.header.row_count
            || stats.min_seq_no != self.header.min_seq_no
            || stats.max_seq_no != self.header.max_seq_no
        {
            return Err(SegmentError::corrupt(
                region,
                "stats disagree with the header",
            ));
        }
        Ok(stats)
    }

    /// The scalar column of `field`; `None` if the segment has no section
    /// for it (the field was added later, or is not a scalar field), in
    /// which case every row reads null.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn scalar_column(&self, field: FieldId) -> Result<Option<ScalarColumn>, SegmentError> {
        let Some(index) = self.find_section(SectionKind::ScalarColumn, Some(field)) else {
            return Ok(None);
        };
        let bytes = self.read_section(index)?;
        self.decode_scalar(index, &self.entry(index)?, &bytes)
            .map(Some)
    }

    /// Decode a verified `ScalarColumn` payload.
    fn decode_scalar(
        &self,
        index: usize,
        entry: &SectionEntry,
        bytes: &[u8],
    ) -> Result<ScalarColumn, SegmentError> {
        let region = section_region(index, entry);
        let field = entry
            .field
            .ok_or_else(|| SegmentError::corrupt(region, "no field"))?;
        let field_type = self.scalar_type(field, region)?;
        let column =
            ScalarColumn::decode(bytes, field_type, usize_from(self.header.row_count), region)
                .map_err(|error| error.at(region))?;
        if u16::from(column_encoding_code(bytes)) != entry.encoding {
            return Err(SegmentError::corrupt(
                region,
                "encoding differs from the table entry",
            ));
        }
        Ok(column)
    }

    fn scalar_type(&self, field: FieldId, region: Region) -> Result<FieldType, SegmentError> {
        match self.schema.field_by_id(field) {
            Some(FieldRef::Scalar(scalar)) => Ok(scalar.field_type),
            _ => Err(SegmentError::corrupt(region, "not a scalar field")),
        }
    }

    /// The `VectorF32` section of `field` with its verified prefix; `None`
    /// if the segment has none.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn vector(&self, field: FieldId) -> Result<Option<VectorHandle>, SegmentError> {
        let Some(index) = self.find_section(SectionKind::VectorF32, Some(field)) else {
            return Ok(None);
        };
        let entry = self.entry(index)?;
        let region = Region::VectorPrefix { index };
        let head =
            self.read_in_section(index, &entry, 0, VectorPrefix::HEADER_LEN as u64, region)?;
        let len =
            VectorPrefix::prefix_len(&head, entry.length).map_err(|error| error.at(region))?;
        let bytes = self.read_in_section(index, &entry, 0, len as u64, region)?;
        self.decode_vector_prefix(index, entry, &bytes).map(Some)
    }

    /// Decode a vector prefix; `bytes` may extend past its end.
    fn decode_vector_prefix(
        &self,
        index: usize,
        entry: SectionEntry,
        bytes: &[u8],
    ) -> Result<VectorHandle, SegmentError> {
        let region = Region::VectorPrefix { index };
        if entry.encoding != VECTOR_ENCODING_F32 {
            return Err(SegmentError::corrupt(region, "unknown vector encoding"));
        }
        let len =
            VectorPrefix::prefix_len(bytes, entry.length).map_err(|error| error.at(region))?;
        let prefix_bytes = bytes
            .get(..len)
            .ok_or_else(|| SegmentError::corrupt(region, "vector prefix is truncated"))?;
        let prefix = VectorPrefix::decode(
            prefix_bytes,
            entry.aux32,
            self.header.row_count,
            entry.length,
        )
        .map_err(|error| prefix_error(error, index, region))?;
        Ok(VectorHandle {
            index,
            entry,
            prefix,
        })
    }

    /// Read and verify one page of vectors: `page_rows` rows (fewer on the
    /// last page) of `dim` floats each.
    ///
    /// # Errors
    ///
    /// I/O errors, or [`SegmentError::Checksum`] for the page.
    pub fn vector_page(&self, handle: &VectorHandle, page: u32) -> Result<Vec<f32>, SegmentError> {
        let region = Region::VectorPage {
            index: handle.index,
            page,
        };
        let range = handle
            .prefix
            .page_byte_range(page)
            .ok_or_else(|| SegmentError::corrupt(region, "no such page"))?;
        let bytes = self.read_in_section(
            handle.index,
            &handle.entry,
            range.start,
            range.end - range.start,
            region,
        )?;
        if !handle.prefix.page_matches(page, &bytes) {
            return Err(SegmentError::Checksum { region });
        }
        Ok(f32s_from_le(&bytes))
    }

    /// The vector of `row`, `None` when it is null. Loads one page.
    ///
    /// # Errors
    ///
    /// As [`vector_page`](Self::vector_page).
    pub fn vector_row(
        &self,
        handle: &VectorHandle,
        row: u32,
    ) -> Result<Option<Vec<f32>>, SegmentError> {
        if row >= handle.prefix.row_count() {
            return Err(SegmentError::corrupt(
                Region::VectorPrefix {
                    index: handle.index,
                },
                format!("row {row} is out of range"),
            ));
        }
        if handle.prefix.nulls().contains(row) {
            return Ok(None);
        }
        let page_rows = handle.prefix.page_rows();
        let page = self.vector_page(handle, row / page_rows)?;
        let dim = usize_from(handle.prefix.dim());
        let start = usize_from(row % page_rows) * dim;
        Ok(page.get(start..start + dim).map(<[f32]>::to_vec))
    }

    /// All vectors of the section as little-endian bytes (`row_count * dim
    /// * 4`), checked against the whole-section CRC.
    fn vector_data(&self, handle: &VectorHandle) -> Result<Vec<u8>, SegmentError> {
        let mut bytes = self.read_section(handle.index)?;
        let start = usize_from_u64(handle.prefix.data_offset())
            .map_err(|error| error.at(section_region(handle.index, &handle.entry)))?;
        Ok(bytes.split_off(start.min(bytes.len())))
    }

    /// The `DynamicJson` section with its verified block index; `None` if
    /// no row has dynamic keys.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn dynamic(&self) -> Result<Option<DynamicHandle>, SegmentError> {
        let Some(index) = self.find_section(SectionKind::DynamicJson, None) else {
            return Ok(None);
        };
        let entry = self.entry(index)?;
        let region = Region::DynamicIndex { index };
        let head =
            self.read_in_section(index, &entry, 0, DynamicIndex::HEADER_LEN as u64, region)?;
        let len = DynamicIndex::index_len(&head, entry.length).map_err(|error| error.at(region))?;
        let bytes = self.read_in_section(index, &entry, 0, len as u64, region)?;
        self.decode_dynamic_index(index, entry, &bytes).map(Some)
    }

    /// Decode a dynamic header and block index; `bytes` may extend past it.
    fn decode_dynamic_index(
        &self,
        index: usize,
        entry: SectionEntry,
        bytes: &[u8],
    ) -> Result<DynamicHandle, SegmentError> {
        let region = Region::DynamicIndex { index };
        if entry.encoding != DYNAMIC_ENCODING_BLOCKS {
            return Err(SegmentError::corrupt(region, "unknown dynamic encoding"));
        }
        let len = DynamicIndex::index_len(bytes, entry.length).map_err(|error| error.at(region))?;
        let index_bytes = bytes
            .get(..len)
            .ok_or_else(|| SegmentError::corrupt(region, "dynamic index is truncated"))?;
        let blocks = DynamicIndex::decode(index_bytes, self.header.row_count, entry.length)
            .map_err(|error| match error {
                IndexError::Checksum => SegmentError::Checksum { region },
                IndexError::Malformed(error) => error.at(region),
            })?;
        Ok(DynamicHandle {
            index,
            entry,
            blocks,
        })
    }

    /// Read and verify one block of dynamic values.
    ///
    /// # Errors
    ///
    /// I/O errors, [`SegmentError::Checksum`] for the block, or corruption.
    pub fn dynamic_block(
        &self,
        handle: &DynamicHandle,
        block: u32,
    ) -> Result<DynamicBlock, SegmentError> {
        let region = Region::DynamicBlock {
            index: handle.index,
            block,
        };
        let (location, rows) = handle
            .blocks
            .block(block)
            .zip(handle.blocks.block_rows(block))
            .ok_or_else(|| SegmentError::corrupt(region, "no such block"))?;
        let bytes = self.read_in_section(
            handle.index,
            &handle.entry,
            location.offset,
            u64::from(location.len),
            region,
        )?;
        if crc(&bytes) != location.crc32c {
            return Err(SegmentError::Checksum { region });
        }
        DynamicBlock::decode(&bytes, rows).map_err(|error| error.at(region))
    }

    /// Check and decode `block` from the whole section payload.
    fn dynamic_block_in(
        handle: &DynamicHandle,
        block: u32,
        section: &[u8],
    ) -> Result<DynamicBlock, SegmentError> {
        let region = Region::DynamicBlock {
            index: handle.index,
            block,
        };
        let (location, rows) = handle
            .blocks
            .block(block)
            .zip(handle.blocks.block_rows(block))
            .ok_or_else(|| SegmentError::corrupt(region, "no such block"))?;
        let bytes = usize_from_u64(location.offset)
            .ok()
            .and_then(|start| section.get(start..start + usize_from(location.len)))
            .ok_or_else(|| SegmentError::corrupt(region, "block is outside the section"))?;
        if crc(bytes) != location.crc32c {
            return Err(SegmentError::Checksum { region });
        }
        DynamicBlock::decode(bytes, rows).map_err(|error| error.at(region))
    }

    /// The payload of an opaque index section, CRC-checked, with its table
    /// entry; `None` if the segment has none.
    ///
    /// # Errors
    ///
    /// I/O errors or [`SegmentError::Checksum`].
    pub fn index_section(
        &self,
        kind: IndexSectionKind,
        field: FieldId,
    ) -> Result<Option<(SectionEntry, Vec<u8>)>, SegmentError> {
        let Some(index) = self.find_section(kind.section_kind(), Some(field)) else {
            return Ok(None);
        };
        Ok(Some((self.entry(index)?, self.read_section(index)?)))
    }

    /// Read every row back as a WAL row image. Used by compaction-style
    /// full scans and by tests; it loads every section.
    ///
    /// # Errors
    ///
    /// I/O or corruption errors.
    pub fn read_rows(&self) -> Result<Vec<SegmentRow>, SegmentError> {
        let rows = usize_from(self.header.row_count);
        let seqs = self.row_meta()?;
        let pks = self.pk_column()?;
        let mut vectors = Vec::new();
        let mut vector_fields: Vec<_> =
            self.schema.vectors().iter().map(|field| field.id).collect();
        vector_fields.sort_unstable();
        for field in vector_fields {
            if let Some(handle) = self.vector(field)? {
                let data = self.vector_data(&handle)?;
                vectors.push((field, handle, data));
            }
        }
        let mut columns = Vec::new();
        let mut scalar_fields: Vec<_> = self.schema.fields().iter().map(|field| field.id).collect();
        scalar_fields.sort_unstable();
        for field in scalar_fields {
            if let Some(column) = self.scalar_column(field)? {
                columns.push((field, column));
            }
        }
        let dynamic = match self.dynamic()? {
            Some(handle) => (0..handle.blocks.block_count())
                .map(|block| self.dynamic_block(&handle, block))
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };

        let mut out = Vec::with_capacity(rows);
        for row in 0..rows {
            let row_u32 = u32::try_from(row).unwrap_or(u32::MAX);
            let pk = pks.get(row).ok_or_else(|| {
                SegmentError::corrupt(Region::File, format!("pk of row {row} is missing"))
            })?;
            let mut image = RowImage {
                pk: WirePk::from(pk),
                vectors: Vec::new(),
                scalars: Vec::new(),
                dynamic: None,
            };
            for (field, handle, data) in &vectors {
                if handle.prefix.nulls().contains(row_u32) {
                    continue;
                }
                let stride = usize_from(handle.prefix.dim()) * 4;
                let bytes = data.get(row * stride..(row + 1) * stride).ok_or_else(|| {
                    SegmentError::corrupt(section_region(handle.index, &handle.entry), "short data")
                })?;
                let vector = F32Bytes::from_le_bytes(bytes.to_vec()).ok_or_else(|| {
                    SegmentError::corrupt(section_region(handle.index, &handle.entry), "bad vector")
                })?;
                image.vectors.push((*field, vector));
            }
            for (field, column) in &columns {
                let value = column.value(row)?;
                if !value.is_null() {
                    let bytes = ValueBytes::encode(&value)
                        .map_err(|error| SegmentError::Encode(error.to_string()))?;
                    image.scalars.push((*field, bytes));
                }
            }
            let block = row / usize_from(super::dynamic::DYNAMIC_BLOCK_ROWS);
            if let Some(raw) = dynamic.get(block).and_then(|block| block.raw(row_u32)) {
                image.dynamic = Some(ValueBytes::from_encoded(raw.to_vec()));
            }
            out.push(SegmentRow {
                seq_no: seqs.get(row).copied().unwrap_or(0),
                image,
            });
        }
        Ok(out)
    }

    /// Verify the whole file: every CRC, every padding byte, and every
    /// section's structure and cross-section consistency. This is what
    /// compaction and `inspect --verify` run; normal reads verify only what
    /// they load.
    ///
    /// # Errors
    ///
    /// The first I/O or corruption error found.
    pub fn verify(&self) -> Result<(), SegmentError> {
        self.verify_padding()?;
        let seqs = self.row_meta()?;
        let pks = self.pk_column()?;
        let (sorted_index, sorted_entry) = self.required(SectionKind::PkSorted)?;
        self.pk_sorted()?
            .check_against(&pks)
            .map_err(|error| error.at(section_region(sorted_index, &sorted_entry)))?;
        let (filter_index, filter_entry) = self.required(SectionKind::PkFilter)?;
        self.pk_filter()?
            .check_against(&pks)
            .map_err(|error| error.at(section_region(filter_index, &filter_entry)))?;
        let stats = self.stats()?;
        for (index, entry) in self.sections.iter().enumerate() {
            let region = section_region(index, entry);
            let kind = entry.section_kind();
            if kind.is_some_and(|kind| REQUIRED.contains(&kind)) {
                // Read and checked above, or at open for the schema.
                continue;
            }
            let bytes = self.read_section(index)?;
            match kind {
                Some(SectionKind::VectorF32) => {
                    let handle = self.decode_vector_prefix(index, *entry, &bytes)?;
                    self.verify_vector(&handle, &bytes)?;
                    self.check_vector_stats(&stats, &handle, region)?;
                }
                Some(SectionKind::ScalarColumn) => {
                    let column = self.decode_scalar(index, entry, &bytes)?;
                    column.check_values()?;
                    let nulls = u32::try_from(column.nulls().len()).unwrap_or(u32::MAX);
                    let recorded = entry
                        .field
                        .and_then(|field| stats.field(field))
                        .map(|stats| stats.null_count);
                    if recorded != Some(nulls) {
                        return Err(SegmentError::corrupt(
                            region,
                            "null count differs from stats",
                        ));
                    }
                }
                Some(SectionKind::DynamicJson) => {
                    let handle = self.decode_dynamic_index(index, *entry, &bytes)?;
                    Self::verify_dynamic(&handle, &bytes, &stats)?;
                }
                _ => {}
            }
        }
        if self.find_section(SectionKind::DynamicJson, None).is_none() && stats.dynamic_rows != 0 {
            return Err(SegmentError::corrupt(
                Region::File,
                "dynamic section is missing",
            ));
        }
        if seqs.len() != usize_from(self.header.row_count) {
            return Err(SegmentError::corrupt(Region::File, "row meta length"));
        }
        Ok(())
    }

    fn verify_padding(&self) -> Result<(), SegmentError> {
        let mut end = HEADER_LEN as u64;
        let mut gaps = Vec::new();
        for entry in self.sections.iter() {
            gaps.push((end, entry.offset));
            end = entry.end();
        }
        gaps.push((end, self.footer.table_offset));
        for (start, stop) in gaps {
            let region = Region::Padding { offset: start };
            let bytes = read_range(&self.source, self.file_len, start, stop - start, region)?;
            if bytes.iter().any(|byte| *byte != 0) {
                return Err(SegmentError::corrupt(region, "padding bytes are not zero"));
            }
        }
        Ok(())
    }

    fn verify_vector(&self, handle: &VectorHandle, bytes: &[u8]) -> Result<(), SegmentError> {
        let region = section_region(handle.index, &handle.entry);
        let prefix_end = VectorPrefix::prefix_len(bytes, handle.entry.length)
            .map_err(|error| error.at(region))?;
        let data_start =
            usize_from_u64(handle.prefix.data_offset()).map_err(|error| error.at(region))?;
        let padding = bytes
            .get(prefix_end..data_start)
            .ok_or_else(|| SegmentError::corrupt(region, "vector prefix overruns the data"))?;
        if padding.iter().any(|byte| *byte != 0) {
            return Err(SegmentError::corrupt(region, "vector padding is not zero"));
        }
        let data = bytes.get(data_start..).unwrap_or_default();
        handle
            .prefix
            .check_data(data)
            .map_err(|error| prefix_error(error, handle.index, region))
    }

    fn check_vector_stats(
        &self,
        stats: &SegmentStats,
        handle: &VectorHandle,
        region: Region,
    ) -> Result<(), SegmentError> {
        let nulls = u32::try_from(handle.prefix.nulls().len()).unwrap_or(u32::MAX);
        let recorded = handle
            .entry
            .field
            .and_then(|field| stats.field(field))
            .map(|stats| stats.null_count);
        if recorded == Some(nulls) {
            Ok(())
        } else {
            Err(SegmentError::corrupt(
                region,
                "vector null count differs from stats",
            ))
        }
    }

    fn verify_dynamic(
        handle: &DynamicHandle,
        bytes: &[u8],
        stats: &SegmentStats,
    ) -> Result<(), SegmentError> {
        let region = section_region(handle.index, &handle.entry);
        let mut end = handle.blocks.blocks_start();
        let mut with_values = 0_u32;
        let index_end = DynamicIndex::index_len(bytes, handle.entry.length)
            .map_err(|error| error.at(region))? as u64;
        let mut gaps = vec![(index_end, end)];
        for block in 0..handle.blocks.block_count() {
            let location = handle
                .blocks
                .block(block)
                .ok_or_else(|| SegmentError::corrupt(region, "missing block"))?;
            gaps.push((end, location.offset));
            end = location.offset + u64::from(location.len);
            let decoded = Self::dynamic_block_in(handle, block, bytes)?;
            for row in decoded.rows() {
                let object = decoded.object(row).map_err(|error| {
                    error.at(Region::DynamicBlock {
                        index: handle.index,
                        block,
                    })
                })?;
                if object.is_some() {
                    with_values += 1;
                }
            }
        }
        for (start, stop) in gaps {
            let range = usize_from_u64(start).ok().zip(usize_from_u64(stop).ok());
            let clean = range
                .and_then(|(start, stop)| bytes.get(start..stop))
                .is_some_and(|pad| pad.iter().all(|byte| *byte == 0));
            if !clean {
                return Err(SegmentError::corrupt(region, "dynamic padding is not zero"));
            }
        }
        if with_values != stats.dynamic_rows || with_values == 0 {
            return Err(SegmentError::corrupt(
                region,
                "dynamic row count differs from stats",
            ));
        }
        Ok(())
    }
}

fn column_encoding_code(bytes: &[u8]) -> u8 {
    bytes.first().copied().unwrap_or(0)
}

fn section_region(index: usize, entry: &SectionEntry) -> Region {
    Region::Section {
        index,
        kind: entry.kind,
    }
}

fn prefix_error(error: PrefixError, index: usize, region: Region) -> SegmentError {
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
fn read_range<S: SectionSource>(
    source: &S,
    file_len: u64,
    offset: u64,
    len: u64,
    region: Region,
) -> Result<Vec<u8>, SegmentError> {
    let fits = offset.checked_add(len).is_some_and(|end| end <= file_len);
    if !fits {
        return Err(SegmentError::corrupt(
            region,
            format!("range {offset}+{len} exceeds the file length {file_len}"),
        ));
    }
    let len = usize::try_from(len)
        .map_err(|_| SegmentError::corrupt(region, "range does not fit in memory"))?;
    let mut buf = vec![0_u8; len];
    source.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

/// Sections are in file order, 64-byte aligned, back to back with only
/// alignment padding, and end where the table starts.
fn check_layout(sections: &[SectionEntry], table_offset: u64) -> Result<(), SegmentError> {
    let mut end = HEADER_LEN as u64;
    for (index, entry) in sections.iter().enumerate() {
        let expected = align_up(end, SECTION_ALIGN);
        if Some(entry.offset) != expected {
            return Err(SegmentError::corrupt(
                Region::SectionTable,
                format!("section {index} is not where the layout puts it"),
            ));
        }
        end = entry.offset.checked_add(entry.length).ok_or_else(|| {
            SegmentError::corrupt(Region::SectionTable, format!("section {index} overflows"))
        })?;
        if end > table_offset {
            return Err(SegmentError::corrupt(
                Region::SectionTable,
                format!("section {index} runs into the section table"),
            ));
        }
    }
    if align_up(end, SECTION_ALIGN) != Some(table_offset) {
        return Err(SegmentError::corrupt(
            Region::SectionTable,
            "sections do not end where the table starts",
        ));
    }
    Ok(())
}

/// Known kinds: per-field kinds name a field and others do not, no kind and
/// field repeats, and every required section is present.
fn check_kinds(sections: &[SectionEntry]) -> Result<(), SegmentError> {
    let mut seen = BTreeSet::new();
    for (index, entry) in sections.iter().enumerate() {
        let Some(kind) = entry.section_kind() else {
            continue;
        };
        let region = section_region(index, entry);
        if kind.is_per_field() != entry.field.is_some() {
            return Err(SegmentError::corrupt(
                region,
                "field id does not fit the kind",
            ));
        }
        if !seen.insert((entry.kind, entry.field)) {
            return Err(SegmentError::corrupt(region, "duplicate section"));
        }
    }
    for kind in REQUIRED {
        if !seen.contains(&(kind.code(), None)) {
            return Err(SegmentError::corrupt(
                Region::SectionTable,
                format!("missing {kind:?} section"),
            ));
        }
    }
    Ok(())
}

fn required(
    sections: &[SectionEntry],
    kind: SectionKind,
) -> Result<(usize, SectionEntry), SegmentError> {
    sections
        .iter()
        .enumerate()
        .find(|(_, entry)| entry.kind == kind.code() && entry.field.is_none())
        .map(|(index, entry)| (index, *entry))
        .ok_or_else(|| {
            SegmentError::corrupt(Region::SectionTable, format!("missing {kind:?} section"))
        })
}

/// Read a whole section payload and check its CRC.
fn read_verified<S: SectionSource>(
    source: &S,
    file_len: u64,
    index: usize,
    entry: &SectionEntry,
) -> Result<Vec<u8>, SegmentError> {
    let region = section_region(index, entry);
    let bytes = read_range(source, file_len, entry.offset, entry.length, region)?;
    if crc(&bytes) != entry.crc32c {
        return Err(SegmentError::Checksum { region });
    }
    Ok(bytes)
}

/// Read, verify, and decode the schema snapshot.
fn load_schema<S: SectionSource>(
    source: &S,
    file_len: u64,
    header: &SegmentHeader,
    sections: &[SectionEntry],
) -> Result<CollectionSchema, SegmentError> {
    let (index, entry) = required(sections, SectionKind::SchemaSnapshot)?;
    let region = section_region(index, &entry);
    let bytes = read_verified(source, file_len, index, &entry)?;
    if entry.encoding != SCHEMA_ENCODING_POSTCARD {
        return Err(SegmentError::corrupt(region, "unknown schema encoding"));
    }
    if XxHash3_64::oneshot(&bytes) != header.schema_hash {
        return Err(SegmentError::corrupt(
            region,
            "schema snapshot does not match the header's schema hash",
        ));
    }
    let (schema, rest) = postcard::take_from_bytes::<CollectionSchema>(&bytes)
        .map_err(|error| SegmentError::corrupt(region, format!("schema: {error}")))?;
    if !rest.is_empty() {
        return Err(SegmentError::corrupt(
            region,
            "trailing bytes after the schema",
        ));
    }
    if schema.schema_version() != header.schema_version {
        return Err(SegmentError::corrupt(
            region,
            "schema version differs from the header",
        ));
    }
    Ok(schema)
}
