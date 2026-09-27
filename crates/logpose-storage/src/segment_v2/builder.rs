//! [`SegmentBuilder`]: accumulate rows column by column, then stream the
//! file out section by section.

use super::{
    column::{ColumnBuf, VarBuf},
    dynamic::{self, DYNAMIC_ENCODING_BLOCKS, decode_object},
    error::SegmentError,
    format::{
        ENTRY_LEN, FOOTER_LEN, Footer, HEADER_LEN, SECTION_ALIGN, SectionEntry, SectionKind,
        SegmentHeader, crc, crc_append,
    },
    le::{align_up, put_u32, put_u64},
    pk::{self, FILTER_ENCODING_FUSE8, PkColumn, PkFilter},
    stats::{self, STATS_ENCODING_POSTCARD, SegmentStats, StatsInput},
    vector::{VECTOR_ENCODING_F32, VectorBuf},
};
use logpose_types::{
    CollectionId, SeqNo,
    record::PrimaryKey,
    schema::{CollectionSchema, FieldId, PrimaryKeyType},
    value::Value,
};
use logpose_wal::codec::RowImage;
use std::{collections::BTreeMap, io::Write, sync::Arc};
use twox_hash::XxHash3_64;

pub(crate) const SCHEMA_ENCODING_POSTCARD: u16 = 1;
pub(crate) const ROW_META_U64: u16 = 1;
pub(crate) const ROW_META_U32_DELTA: u16 = 2;

/// Largest number of rows in one segment. `u32::MAX` is reserved as a
/// "no row" sentinel by the engine's forwarding tables.
pub const MAX_SEGMENT_ROWS: u32 = u32::MAX - 1;

/// Which segment a builder writes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentIdentity {
    /// The owning collection.
    pub collection_id: CollectionId,
    /// The segment's unit id.
    pub unit_id: u32,
}

/// Kinds of opaque index section that `logpose-index` produces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum IndexSectionKind {
    /// SQ8 codes of a vector field.
    VectorSq8,
    /// HNSW graph of a vector field.
    VectorGraph,
    /// Inverted index of a scalar field.
    ScalarInverted,
    /// Sorted index of a scalar field.
    ScalarSorted,
}

impl IndexSectionKind {
    /// The section kind.
    #[must_use]
    pub fn section_kind(self) -> SectionKind {
        match self {
            Self::VectorSq8 => SectionKind::VectorSq8,
            Self::VectorGraph => SectionKind::VectorGraph,
            Self::ScalarInverted => SectionKind::ScalarInverted,
            Self::ScalarSorted => SectionKind::ScalarSorted,
        }
    }

    fn is_vector(self) -> bool {
        matches!(self, Self::VectorSq8 | Self::VectorGraph)
    }
}

/// An opaque index payload to store as one section. Storage checks only
/// the field it belongs to; the payload format belongs to `logpose-index`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSection {
    /// What the payload is.
    pub kind: IndexSectionKind,
    /// The field it indexes.
    pub field: FieldId,
    /// Payload-specific encoding code, stored in the section table.
    pub encoding: u16,
    /// Payload-specific value for the table entry's `aux32`.
    pub aux32: u32,
    /// Payload-specific value for the table entry's `aux64`.
    pub aux64: u64,
    /// The payload bytes.
    pub payload: Vec<u8>,
}

/// What [`SegmentBuilder::finish`] wrote: everything a manifest entry needs.
#[derive(Clone, Debug, PartialEq)]
pub struct WrittenSegment {
    /// The file header.
    pub header: SegmentHeader,
    /// Total file length.
    pub file_len: u64,
    /// The footer's CRC.
    pub footer_crc: u32,
    /// The section table.
    pub sections: Vec<SectionEntry>,
    /// The stats written to the `Stats` section.
    pub stats: SegmentStats,
}

#[derive(Clone, Copy, Debug)]
enum Slot {
    Vector(usize),
    Scalar(usize),
}

#[derive(Clone, Debug)]
enum PkBuf {
    Int(Vec<i64>),
    Str(VarBuf),
}

/// Builds one segment v2 file.
///
/// Rows are appended with [`push_row`](Self::push_row) (set fields through
/// the returned [`RowWriter`]; unset fields are null) or
/// [`push_row_image`](Self::push_row_image). Index payloads are attached
/// with [`add_index_section`](Self::add_index_section).
/// [`finish`](Self::finish) writes the file in one forward pass, section by
/// section, so the sink can be an append-only file.
///
/// The builder keeps every column in memory until `finish`; its footprint is
/// about the size of the file it writes.
#[derive(Debug)]
pub struct SegmentBuilder {
    schema: Arc<CollectionSchema>,
    identity: SegmentIdentity,
    schema_bytes: Vec<u8>,
    seqs: Vec<SeqNo>,
    pk: PkBuf,
    vectors: Vec<VectorBuf>,
    columns: Vec<ColumnBuf>,
    slots: BTreeMap<FieldId, Slot>,
    dynamic: VarBuf,
    dynamic_rows: u32,
    row_has_dynamic: bool,
    index_sections: BTreeMap<(IndexSectionKind, FieldId), IndexSection>,
}

impl SegmentBuilder {
    /// A builder for rows of `schema`. The segment stores a snapshot of
    /// the schema and one section per declared vector and scalar field.
    ///
    /// # Errors
    ///
    /// [`SegmentError::Encode`] if the schema cannot be serialized.
    pub fn new(
        schema: Arc<CollectionSchema>,
        identity: SegmentIdentity,
    ) -> Result<Self, SegmentError> {
        let schema_bytes = postcard::to_allocvec(schema.as_ref())
            .map_err(|error| SegmentError::Encode(error.to_string()))?;
        let mut vectors: Vec<VectorBuf> = schema
            .vectors()
            .iter()
            .map(|field| VectorBuf::new(field.id, field.dimensions))
            .collect();
        vectors.sort_by_key(|vector| vector.field);
        let mut columns: Vec<ColumnBuf> = schema
            .fields()
            .iter()
            .map(|field| ColumnBuf::new(field.id, field.field_type))
            .collect();
        columns.sort_by_key(|column| column.field);
        let mut slots = BTreeMap::new();
        for (index, vector) in vectors.iter().enumerate() {
            slots.insert(vector.field, Slot::Vector(index));
        }
        for (index, column) in columns.iter().enumerate() {
            slots.insert(column.field, Slot::Scalar(index));
        }
        let pk = match schema.primary_key_type() {
            PrimaryKeyType::Int64 => PkBuf::Int(Vec::new()),
            PrimaryKeyType::String => PkBuf::Str(VarBuf::default()),
        };
        Ok(Self {
            schema,
            identity,
            schema_bytes,
            seqs: Vec::new(),
            pk,
            vectors,
            columns,
            slots,
            dynamic: VarBuf::default(),
            dynamic_rows: 0,
            row_has_dynamic: false,
            index_sections: BTreeMap::new(),
        })
    }

    /// The schema the segment is written under.
    #[must_use]
    pub fn schema(&self) -> &Arc<CollectionSchema> {
        &self.schema
    }

    /// Rows pushed so far.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        u32::try_from(self.seqs.len()).unwrap_or(u32::MAX)
    }

    /// Append a row with every field null and return a writer for its
    /// fields. The row's id is [`row_count`](Self::row_count) before the
    /// call.
    ///
    /// # Errors
    ///
    /// [`SegmentError::InvalidSeqNo`] for sequence number 0,
    /// [`SegmentError::PrimaryKeyType`] for a key of the wrong type, and
    /// [`SegmentError::TooLarge`] past [`MAX_SEGMENT_ROWS`]. Duplicate keys
    /// are reported by [`finish`](Self::finish).
    pub fn push_row(
        &mut self,
        seq_no: SeqNo,
        pk: &PrimaryKey,
    ) -> Result<RowWriter<'_>, SegmentError> {
        if seq_no == 0 {
            return Err(SegmentError::InvalidSeqNo);
        }
        if self.row_count() >= MAX_SEGMENT_ROWS {
            return Err(SegmentError::TooLarge {
                what: "more than u32::MAX - 1 rows",
            });
        }
        match (&mut self.pk, pk) {
            (PkBuf::Int(keys), PrimaryKey::Int64(value)) => keys.push(*value),
            (PkBuf::Str(keys), PrimaryKey::String(value)) => keys.push(value.as_bytes()),
            _ => {
                return Err(SegmentError::PrimaryKeyType {
                    expected: self.schema.primary_key_type(),
                    found: pk.key_type(),
                });
            }
        }
        self.seqs.push(seq_no);
        for vector in &mut self.vectors {
            vector.push_null();
        }
        for column in &mut self.columns {
            column.push_null();
        }
        self.dynamic.push_empty();
        self.row_has_dynamic = false;
        Ok(RowWriter { builder: self })
    }

    /// Append a WAL row image. Values of fields that the builder's schema
    /// does not declare (dropped after the row was written) are skipped.
    /// Either the whole row is appended or, on error, nothing is.
    ///
    /// # Errors
    ///
    /// As [`push_row`](Self::push_row), plus [`SegmentError::FieldKind`],
    /// [`SegmentError::VectorDimensions`], [`SegmentError::FieldAlreadySet`]
    /// for a repeated field id, [`SegmentError::InvalidDynamic`], and
    /// [`SegmentError::Encode`] for a value that does not decode as its
    /// field's type.
    pub fn push_row_image(&mut self, seq_no: SeqNo, image: &RowImage) -> Result<(), SegmentError> {
        let mut vectors = Vec::with_capacity(image.vectors.len());
        for (field, bytes) in &image.vectors {
            match self.slots.get(field) {
                Some(Slot::Vector(index)) => {
                    let vector = &self.vectors[*index];
                    if bytes.as_bytes().len() != usize::try_from(vector.dim).unwrap_or(0) * 4 {
                        return Err(SegmentError::VectorDimensions {
                            field: *field,
                            expected: vector.dim,
                            actual: bytes.dimensions(),
                        });
                    }
                    vectors.push((*field, bytes.as_bytes()));
                }
                Some(Slot::Scalar(_)) => {
                    return Err(SegmentError::FieldKind {
                        field: *field,
                        expected: "vector",
                    });
                }
                None => {}
            }
        }
        let mut scalars = Vec::with_capacity(image.scalars.len());
        for (field, bytes) in &image.scalars {
            match self.slots.get(field) {
                Some(Slot::Scalar(index)) => {
                    let field_type = self.columns[*index].field_type;
                    let value = bytes
                        .decode(field_type)
                        .map_err(|error| SegmentError::Encode(format!("field {field}: {error}")))?;
                    scalars.push((*field, value));
                }
                Some(Slot::Vector(_)) => {
                    return Err(SegmentError::FieldKind {
                        field: *field,
                        expected: "scalar",
                    });
                }
                None => {}
            }
        }
        check_unique(vectors.iter().map(|(field, _)| *field))?;
        check_unique(scalars.iter().map(|(field, _)| *field))?;
        if let Some(dynamic) = &image.dynamic {
            decode_object(dynamic.as_bytes())
                .map_err(|error| SegmentError::InvalidDynamic(error.0))?;
        }
        // Values decoded for their field type always conform, and field ids
        // are unique, so no setter below can fail and the row is appended
        // whole.
        let pk = PrimaryKey::from(image.pk.clone());
        let mut row = self.push_row(seq_no, &pk)?;
        for (field, bytes) in vectors {
            row.vector_le_bytes(field, bytes)?;
        }
        for (field, value) in &scalars {
            row.scalar(*field, value)?;
        }
        if let Some(dynamic) = &image.dynamic {
            row.dynamic(dynamic.as_bytes())?;
        }
        Ok(())
    }

    /// Attach an opaque index section.
    ///
    /// # Errors
    ///
    /// [`SegmentError::UnknownField`] or [`SegmentError::FieldKind`] when the
    /// field is not a declared field of the right family, and
    /// [`SegmentError::DuplicateIndexSection`] for a second section of the
    /// same kind and field.
    pub fn add_index_section(&mut self, section: IndexSection) -> Result<(), SegmentError> {
        let field = section.field;
        match (self.slots.get(&field), section.kind.is_vector()) {
            (None, _) => return Err(SegmentError::UnknownField { field }),
            (Some(Slot::Vector(_)), false) => {
                return Err(SegmentError::FieldKind {
                    field,
                    expected: "scalar",
                });
            }
            (Some(Slot::Scalar(_)), true) => {
                return Err(SegmentError::FieldKind {
                    field,
                    expected: "vector",
                });
            }
            _ => {}
        }
        let key = (section.kind, field);
        if self.index_sections.contains_key(&key) {
            return Err(SegmentError::DuplicateIndexSection {
                kind: section.kind.section_kind(),
                field,
            });
        }
        self.index_sections.insert(key, section);
        Ok(())
    }

    /// Write the file to `out` in one forward pass and return its summary.
    /// The caller syncs the file.
    ///
    /// # Errors
    ///
    /// [`SegmentError::DuplicatePrimaryKey`], [`SegmentError::TooLarge`]
    /// when a section exceeds a format limit, [`SegmentError::Encode`], or
    /// [`SegmentError::Io`] from the sink.
    pub fn finish<W: Write>(self, out: W) -> Result<WrittenSegment, SegmentError> {
        let pk = self.pk_column()?;
        let (pk_column_bytes, pk_sorted_bytes) = pk::encode_pk_sections(&pk)?;
        let filter = PkFilter::build(&pk).encode();
        let stats = stats::compute(&StatsInput {
            seqs: &self.seqs,
            pk: &pk,
            vectors: &self.vectors,
            columns: &self.columns,
            dynamic_rows: self.dynamic_rows,
        });
        let stats_bytes = stats.encode()?;
        let header = SegmentHeader {
            collection_id: self.identity.collection_id.clone(),
            unit_id: self.identity.unit_id,
            row_count: self.row_count(),
            schema_version: self.schema.schema_version(),
            schema_hash: XxHash3_64::oneshot(&self.schema_bytes),
            min_seq_no: stats.min_seq_no,
            max_seq_no: stats.max_seq_no,
        };
        let rows = u64::from(header.row_count);
        let pk_encoding = pk::encoding_for(self.schema.primary_key_type());

        let mut file = FileWriter::new(out);
        let (header_bytes, header_crc) = header.encode();
        file.write(&header_bytes)?;
        file.section(
            SectionSpec::global(SectionKind::SchemaSnapshot, SCHEMA_ENCODING_POSTCARD),
            &[&self.schema_bytes],
        )?;
        let (row_meta_encoding, base, row_meta) = encode_row_meta(&self.seqs, stats.min_seq_no);
        file.section(
            SectionSpec {
                aux64: base,
                ..SectionSpec::global(SectionKind::RowMeta, row_meta_encoding)
            },
            &[&row_meta],
        )?;
        file.section(
            SectionSpec {
                aux64: rows,
                ..SectionSpec::global(SectionKind::PkColumn, pk_encoding)
            },
            &[&pk_column_bytes],
        )?;
        file.section(
            SectionSpec {
                aux64: rows,
                ..SectionSpec::global(SectionKind::PkSorted, pk_encoding)
            },
            &[&pk_sorted_bytes],
        )?;
        file.section(
            SectionSpec::global(SectionKind::PkFilter, FILTER_ENCODING_FUSE8),
            &[&filter],
        )?;
        file.section(
            SectionSpec::global(SectionKind::Stats, STATS_ENCODING_POSTCARD),
            &[&stats_bytes],
        )?;
        for vector in &self.vectors {
            let prefix = vector.encode_prefix()?;
            file.section(
                SectionSpec {
                    kind: SectionKind::VectorF32.code(),
                    encoding: VECTOR_ENCODING_F32,
                    field: Some(vector.field),
                    aux32: vector.dim,
                    aux64: rows,
                },
                &[&prefix, &vector.data],
            )?;
        }
        for column in &self.columns {
            let (payload, encoding) = column.encode()?;
            file.section(
                SectionSpec {
                    kind: SectionKind::ScalarColumn.code(),
                    encoding: u16::from(encoding.code()),
                    field: Some(column.field),
                    aux32: 0,
                    aux64: rows,
                },
                &[&payload],
            )?;
        }
        if self.dynamic_rows > 0 {
            let payload = dynamic::encode(&self.dynamic)?;
            file.section(
                SectionSpec {
                    aux64: rows,
                    ..SectionSpec::global(SectionKind::DynamicJson, DYNAMIC_ENCODING_BLOCKS)
                },
                &[&payload],
            )?;
        }
        for section in self.index_sections.values() {
            file.section(
                SectionSpec {
                    kind: section.kind.section_kind().code(),
                    encoding: section.encoding,
                    field: Some(section.field),
                    aux32: section.aux32,
                    aux64: section.aux64,
                },
                &[&section.payload],
            )?;
        }
        let (file_len, footer_crc, sections) = file.finish(header_crc)?;
        Ok(WrittenSegment {
            header,
            file_len,
            footer_crc,
            sections,
            stats,
        })
    }

    /// [`finish`](Self::finish) into a new buffer.
    ///
    /// # Errors
    ///
    /// As [`finish`](Self::finish).
    pub fn finish_to_vec(self) -> Result<(Vec<u8>, WrittenSegment), SegmentError> {
        let mut bytes = Vec::new();
        let written = self.finish(&mut bytes)?;
        Ok((bytes, written))
    }

    fn pk_column(&self) -> Result<PkColumn, SegmentError> {
        Ok(match &self.pk {
            PkBuf::Int(keys) => PkColumn::Int64(keys.clone()),
            PkBuf::Str(keys) => {
                let mut offsets = Vec::with_capacity(keys.ends.len() + 1);
                offsets.push(0);
                for end in &keys.ends {
                    offsets.push(u32::try_from(*end).map_err(|_| SegmentError::TooLarge {
                        what: "primary keys over 4 GiB",
                    })?);
                }
                PkColumn::String {
                    offsets,
                    bytes: keys.bytes.clone(),
                }
            }
        })
    }
}

fn check_unique(fields: impl Iterator<Item = FieldId>) -> Result<(), SegmentError> {
    let mut seen = std::collections::BTreeSet::new();
    for field in fields {
        if !seen.insert(field) {
            return Err(SegmentError::FieldAlreadySet { field });
        }
    }
    Ok(())
}

/// `RowMeta` payload: `u32` deltas from `base` when they fit, else `u64`.
fn encode_row_meta(seqs: &[SeqNo], min: SeqNo) -> (u16, u64, Vec<u8>) {
    let fits = seqs.iter().all(|seq| {
        seq.checked_sub(min)
            .is_some_and(|delta| delta <= u64::from(u32::MAX))
    });
    let mut out = Vec::with_capacity(seqs.len() * if fits { 4 } else { 8 });
    if fits && !seqs.is_empty() {
        for seq in seqs {
            // Checked above.
            put_u32(&mut out, u32::try_from(seq - min).unwrap_or(u32::MAX));
        }
        (ROW_META_U32_DELTA, min, out)
    } else {
        for seq in seqs {
            put_u64(&mut out, *seq);
        }
        (ROW_META_U64, 0, out)
    }
}

/// Sets the fields of the row just pushed. Fields that are never set stay
/// null.
#[derive(Debug)]
pub struct RowWriter<'a> {
    builder: &'a mut SegmentBuilder,
}

impl RowWriter<'_> {
    fn slot(&self, field: FieldId) -> Result<Slot, SegmentError> {
        self.builder
            .slots
            .get(&field)
            .copied()
            .ok_or(SegmentError::UnknownField { field })
    }

    /// Set a vector field.
    ///
    /// # Errors
    ///
    /// [`SegmentError::UnknownField`], [`SegmentError::FieldKind`],
    /// [`SegmentError::VectorDimensions`], or
    /// [`SegmentError::FieldAlreadySet`].
    pub fn vector(
        &mut self,
        field: FieldId,
        components: &[f32],
    ) -> Result<&mut Self, SegmentError> {
        let bytes: Vec<u8> = components
            .iter()
            .flat_map(|component| component.to_le_bytes())
            .collect();
        self.vector_le_bytes(field, &bytes)
    }

    /// Set a vector field from little-endian `f32` bytes, such as
    /// `F32Bytes::as_bytes`.
    ///
    /// # Errors
    ///
    /// As [`vector`](Self::vector).
    pub fn vector_le_bytes(
        &mut self,
        field: FieldId,
        bytes: &[u8],
    ) -> Result<&mut Self, SegmentError> {
        match self.slot(field)? {
            Slot::Vector(index) => self.builder.vectors[index].set_last_le(bytes)?,
            Slot::Scalar(_) => {
                return Err(SegmentError::FieldKind {
                    field,
                    expected: "vector",
                });
            }
        }
        Ok(self)
    }

    /// Set a scalar field. [`Value::Null`] leaves it null.
    ///
    /// # Errors
    ///
    /// [`SegmentError::UnknownField`], [`SegmentError::FieldKind`],
    /// [`SegmentError::ValueType`] for a value that does not conform to the
    /// field type, [`SegmentError::FieldAlreadySet`], or
    /// [`SegmentError::Encode`] for a JSON value the codec rejects.
    pub fn scalar(&mut self, field: FieldId, value: &Value) -> Result<&mut Self, SegmentError> {
        match self.slot(field)? {
            Slot::Scalar(index) => self.builder.columns[index].set_last(value)?,
            Slot::Vector(_) => {
                return Err(SegmentError::FieldKind {
                    field,
                    expected: "scalar",
                });
            }
        }
        Ok(self)
    }

    /// Set the dynamic (`$extra`) field: one JSON object node in the binary
    /// value codec, stored unchanged.
    ///
    /// # Errors
    ///
    /// [`SegmentError::InvalidDynamic`] if the bytes are not one canonical
    /// JSON object, or [`SegmentError::DynamicAlreadySet`].
    pub fn dynamic(&mut self, encoded_object: &[u8]) -> Result<&mut Self, SegmentError> {
        if self.builder.row_has_dynamic {
            return Err(SegmentError::DynamicAlreadySet);
        }
        decode_object(encoded_object).map_err(|error| SegmentError::InvalidDynamic(error.0))?;
        self.builder.dynamic.set_last(encoded_object);
        self.builder.row_has_dynamic = true;
        self.builder.dynamic_rows += 1;
        Ok(self)
    }

    /// Whether `field` is declared by the builder's schema.
    #[must_use]
    pub fn declares(&self, field: FieldId) -> bool {
        self.builder.slots.contains_key(&field)
    }
}

/// Where a section goes in the table.
#[derive(Clone, Copy, Debug)]
struct SectionSpec {
    kind: u16,
    encoding: u16,
    field: Option<FieldId>,
    aux32: u32,
    aux64: u64,
}

impl SectionSpec {
    fn global(kind: SectionKind, encoding: u16) -> Self {
        Self {
            kind: kind.code(),
            encoding,
            field: None,
            aux32: 0,
            aux64: 0,
        }
    }
}

/// Appends sections with alignment and records their table entries.
struct FileWriter<W> {
    out: W,
    pos: u64,
    entries: Vec<SectionEntry>,
}

impl<W: Write> FileWriter<W> {
    fn new(out: W) -> Self {
        Self {
            out,
            pos: 0,
            entries: Vec::new(),
        }
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), SegmentError> {
        self.out.write_all(bytes)?;
        self.pos += bytes.len() as u64;
        Ok(())
    }

    fn pad(&mut self) -> Result<(), SegmentError> {
        let target = align_up(self.pos, SECTION_ALIGN).ok_or(SegmentError::TooLarge {
            what: "file length overflows",
        })?;
        let zeros = [0_u8; 64];
        let pad = usize::try_from(target - self.pos).unwrap_or(0);
        self.write(&zeros[..pad])
    }

    fn section(&mut self, spec: SectionSpec, parts: &[&[u8]]) -> Result<(), SegmentError> {
        self.pad()?;
        let offset = self.pos;
        let mut section_crc = 0;
        for part in parts {
            section_crc = crc_append(section_crc, part);
            self.write(part)?;
        }
        self.entries.push(SectionEntry {
            kind: spec.kind,
            encoding: spec.encoding,
            field: spec.field,
            offset,
            length: self.pos - offset,
            crc32c: section_crc,
            aux32: spec.aux32,
            aux64: spec.aux64,
        });
        Ok(())
    }

    fn finish(mut self, header_crc: u32) -> Result<(u64, u32, Vec<SectionEntry>), SegmentError> {
        self.pad()?;
        let table_offset = self.pos;
        let mut table = Vec::with_capacity(self.entries.len() * ENTRY_LEN);
        for entry in &self.entries {
            entry.encode_into(&mut table);
        }
        let section_count =
            u32::try_from(self.entries.len()).map_err(|_| SegmentError::TooLarge {
                what: "more than 2^32 sections",
            })?;
        let file_len = table_offset + table.len() as u64 + FOOTER_LEN as u64;
        let (footer, footer_crc) = Footer::encode(
            table_offset,
            section_count,
            crc(&table),
            file_len,
            header_crc,
        );
        self.write(&table)?;
        self.write(&footer)?;
        self.out.flush()?;
        debug_assert_eq!(self.pos, file_len);
        debug_assert!(table_offset >= HEADER_LEN as u64);
        Ok((file_len, footer_crc, self.entries))
    }
}
