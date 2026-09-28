//! Byte layout of the file header, section table entries, and footer.
//!
//! ```text
//! offset 0
//! +-------------------------------------------------------------+
//! | FileHeader                                       128 bytes  |
//! +-------------------------------------------------------------+ 128
//! | section 0 payload, zero padding to a 64-byte boundary        |
//! | section 1 payload, ...                                       |
//! +-------------------------------------------------------------+ table_offset
//! | SectionTable: section_count x SectionEntry (64 bytes each)  |
//! +-------------------------------------------------------------+
//! | Footer                                            64 bytes  |
//! +-------------------------------------------------------------+ EOF
//! ```
//!
//! All integers are little-endian. CRCs are CRC-32C (Castagnoli).

use super::{
    error::{Region, SegmentError},
    le::{Cursor, put_u16, put_u32, put_u64, put_zeros},
};
use logpose_types::{CollectionId, SeqNo, schema::FieldId};
use uuid::Uuid;

/// First eight bytes of every segment v2 file.
pub const SEGMENT_MAGIC: [u8; 8] = *b"LPSEG\x00\x02\x00";
/// Last eight bytes of every segment v2 file.
pub const FOOTER_MAGIC: [u8; 8] = *b"LPSEGEND";
/// Format version in the header. Bump it with any change to the golden file.
pub const FORMAT_VERSION: u32 = 2;
/// Header flag of an index sidecar: a file in the segment format that holds only the
/// `SchemaSnapshot` of the segment it indexes and index sections built after the segment was
/// written (vector graphs). It carries the indexed segment's header otherwise.
pub const HEADER_FLAG_INDEX_SIDECAR: u32 = 1;
/// Length of the file header.
pub const HEADER_LEN: usize = 128;
/// Length of one section table entry.
pub const ENTRY_LEN: usize = 64;
/// Length of the footer.
pub const FOOTER_LEN: usize = 64;
/// Alignment of every section payload and of the section table.
pub const SECTION_ALIGN: u64 = 64;
/// `field_id` of a section that is not per field.
pub const NO_FIELD: u32 = u32::MAX;

const HEADER_CRC_AT: usize = 124;
const FOOTER_CRC_AT: usize = 52;

/// CRC-32C of `bytes`.
pub(crate) fn crc(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Continue a CRC-32C over more bytes.
pub(crate) fn crc_append(crc: u32, bytes: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, bytes)
}

/// Kinds of section. Codes are part of the format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum SectionKind {
    /// Postcard `CollectionSchema` at write time.
    SchemaSnapshot,
    /// Per-row sequence numbers.
    RowMeta,
    /// Primary keys in row order.
    PkColumn,
    /// Row ids sorted by primary key.
    PkSorted,
    /// Binary fuse filter over primary-key hashes.
    PkFilter,
    /// Postcard `SegmentStats`.
    Stats,
    /// Raw `f32` vectors of one vector field, in pages.
    VectorF32,
    /// SQ8 codes of one vector field (payload owned by `logpose-index`).
    VectorSq8,
    /// HNSW graph of one vector field (payload owned by `logpose-index`).
    VectorGraph,
    /// One typed scalar column.
    ScalarColumn,
    /// Inverted index of one scalar field (payload owned by `logpose-index`).
    ScalarInverted,
    /// Sorted index of one scalar field (payload owned by `logpose-index`).
    ScalarSorted,
    /// The `$extra` dynamic field in blocks of rows.
    DynamicJson,
}

impl SectionKind {
    /// Every known kind, in code order.
    pub const ALL: [Self; 13] = [
        Self::SchemaSnapshot,
        Self::RowMeta,
        Self::PkColumn,
        Self::PkSorted,
        Self::PkFilter,
        Self::Stats,
        Self::VectorF32,
        Self::VectorSq8,
        Self::VectorGraph,
        Self::ScalarColumn,
        Self::ScalarInverted,
        Self::ScalarSorted,
        Self::DynamicJson,
    ];

    /// The on-disk code.
    #[must_use]
    pub fn code(self) -> u16 {
        match self {
            Self::SchemaSnapshot => 1,
            Self::RowMeta => 2,
            Self::PkColumn => 3,
            Self::PkSorted => 4,
            Self::PkFilter => 5,
            Self::Stats => 6,
            Self::VectorF32 => 10,
            Self::VectorSq8 => 11,
            Self::VectorGraph => 12,
            Self::ScalarColumn => 20,
            Self::ScalarInverted => 21,
            Self::ScalarSorted => 22,
            Self::DynamicJson => 30,
        }
    }

    /// The kind for an on-disk code; `None` for codes this build does not
    /// know, which readers ignore.
    #[must_use]
    pub fn from_code(code: u16) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.code() == code)
    }

    /// Whether sections of this kind belong to one field.
    #[must_use]
    pub fn is_per_field(self) -> bool {
        matches!(
            self,
            Self::VectorF32
                | Self::VectorSq8
                | Self::VectorGraph
                | Self::ScalarColumn
                | Self::ScalarInverted
                | Self::ScalarSorted
        )
    }

    pub(crate) fn describe(code: u16) -> String {
        Self::from_code(code).map_or_else(
            || format!("unknown kind {code}"),
            |kind| format!("{kind:?}"),
        )
    }
}

/// The parsed file header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    /// Collection the segment belongs to.
    pub collection_id: CollectionId,
    /// Unit id of the segment.
    pub unit_id: u32,
    /// Number of rows.
    pub row_count: u32,
    /// `CollectionSchema::schema_version` of the schema snapshot.
    pub schema_version: u64,
    /// `xxh3_64` of the `SchemaSnapshot` section payload.
    pub schema_hash: u64,
    /// Smallest row sequence number, or 0 for an empty segment.
    pub min_seq_no: SeqNo,
    /// Largest row sequence number, or 0 for an empty segment.
    pub max_seq_no: SeqNo,
    /// Header flags: 0 for a segment, [`HEADER_FLAG_INDEX_SIDECAR`] for an index sidecar.
    pub flags: u32,
}

impl SegmentHeader {
    /// Whether the file is an index sidecar rather than a segment.
    #[must_use]
    pub fn is_index_sidecar(&self) -> bool {
        self.flags & HEADER_FLAG_INDEX_SIDECAR != 0
    }

    /// Encode with its CRC. Returns the bytes and the CRC.
    pub(crate) fn encode(&self) -> ([u8; HEADER_LEN], u32) {
        let mut out = Vec::with_capacity(HEADER_LEN);
        out.extend_from_slice(&SEGMENT_MAGIC);
        put_u32(&mut out, FORMAT_VERSION);
        put_u32(&mut out, self.flags);
        out.extend_from_slice(self.collection_id.0.as_bytes());
        put_u32(&mut out, self.unit_id);
        put_u32(&mut out, self.row_count);
        put_u64(&mut out, self.schema_version);
        put_u64(&mut out, self.schema_hash);
        put_u64(&mut out, self.min_seq_no);
        put_u64(&mut out, self.max_seq_no);
        out.resize(HEADER_CRC_AT, 0);
        let header_crc = crc(&out);
        put_u32(&mut out, header_crc);
        let mut bytes = [0_u8; HEADER_LEN];
        bytes.copy_from_slice(&out);
        (bytes, header_crc)
    }

    /// Decode and verify. Returns the header and its CRC.
    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, u32), SegmentError> {
        let region = Region::Header;
        let malformed = |error: super::error::Malformed| error.at(region);
        if bytes.len() != HEADER_LEN {
            return Err(SegmentError::corrupt(region, "header is truncated"));
        }
        if bytes[..8] != SEGMENT_MAGIC {
            return Err(SegmentError::corrupt(region, "bad magic"));
        }
        let mut cursor = Cursor::new(bytes);
        cursor.skip(HEADER_CRC_AT).map_err(malformed)?;
        let stored_crc = cursor.u32().map_err(malformed)?;
        if crc(&bytes[..HEADER_CRC_AT]) != stored_crc {
            return Err(SegmentError::Checksum { region });
        }
        let mut cursor = Cursor::new(bytes);
        cursor.skip(8).map_err(malformed)?;
        let version = cursor.u32().map_err(malformed)?;
        if version != FORMAT_VERSION {
            return Err(SegmentError::UnsupportedVersion { version });
        }
        let flags = cursor.u32().map_err(malformed)?;
        if flags != 0 && flags != HEADER_FLAG_INDEX_SIDECAR {
            return Err(SegmentError::corrupt(region, "unknown header flags"));
        }
        let mut uuid = [0_u8; 16];
        uuid.copy_from_slice(cursor.take(16).map_err(malformed)?);
        let header = Self {
            collection_id: CollectionId(Uuid::from_bytes(uuid)),
            unit_id: cursor.u32().map_err(malformed)?,
            row_count: cursor.u32().map_err(malformed)?,
            schema_version: cursor.u64().map_err(malformed)?,
            schema_hash: cursor.u64().map_err(malformed)?,
            min_seq_no: cursor.u64().map_err(malformed)?,
            max_seq_no: cursor.u64().map_err(malformed)?,
            flags,
        };
        cursor
            .zeros(HEADER_CRC_AT - cursor.position())
            .map_err(malformed)?;
        if header.min_seq_no > header.max_seq_no {
            return Err(SegmentError::corrupt(
                region,
                "min_seq_no exceeds max_seq_no",
            ));
        }
        Ok((header, stored_crc))
    }
}

/// One section table entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SectionEntry {
    /// Raw kind code; see [`SectionKind`].
    pub kind: u16,
    /// Kind-specific encoding code.
    pub encoding: u16,
    /// The field, for per-field kinds.
    pub field: Option<FieldId>,
    /// Absolute payload offset, a multiple of 64.
    pub offset: u64,
    /// Payload length, excluding padding.
    pub length: u64,
    /// CRC-32C of the whole payload.
    pub crc32c: u32,
    /// Kind-specific: the dimension for `VectorF32`.
    pub aux32: u32,
    /// Kind-specific: the row count for row-shaped sections, the sequence
    /// base for delta-encoded `RowMeta`.
    pub aux64: u64,
}

impl SectionEntry {
    /// The known kind, if any.
    #[must_use]
    pub fn section_kind(&self) -> Option<SectionKind> {
        SectionKind::from_code(self.kind)
    }

    /// Absolute offset one past the payload's last byte.
    #[must_use]
    pub fn end(&self) -> u64 {
        self.offset.saturating_add(self.length)
    }

    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        let start = out.len();
        put_u16(out, self.kind);
        put_u16(out, self.encoding);
        put_u32(out, self.field.map_or(NO_FIELD, |field| field.0));
        put_u64(out, self.offset);
        put_u64(out, self.length);
        put_u32(out, self.crc32c);
        put_u32(out, self.aux32);
        put_u64(out, self.aux64);
        put_u16(out, 0);
        put_zeros(out, ENTRY_LEN - (out.len() - start));
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, SegmentError> {
        let region = Region::SectionTable;
        let malformed = |error: super::error::Malformed| error.at(region);
        let mut cursor = Cursor::new(bytes);
        let kind = cursor.u16().map_err(malformed)?;
        let encoding = cursor.u16().map_err(malformed)?;
        let field = cursor.u32().map_err(malformed)?;
        let entry = Self {
            kind,
            encoding,
            field: (field != NO_FIELD).then_some(FieldId(field)),
            offset: cursor.u64().map_err(malformed)?,
            length: cursor.u64().map_err(malformed)?,
            crc32c: cursor.u32().map_err(malformed)?,
            aux32: cursor.u32().map_err(malformed)?,
            aux64: cursor.u64().map_err(malformed)?,
        };
        if cursor.u16().map_err(malformed)? != 0 {
            return Err(SegmentError::corrupt(region, "unknown section flags"));
        }
        cursor.zeros(cursor.remaining()).map_err(malformed)?;
        Ok(entry)
    }
}

/// The parsed footer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Footer {
    /// Absolute offset of the section table.
    pub table_offset: u64,
    /// Number of section table entries.
    pub section_count: u32,
    /// CRC-32C of the whole section table.
    pub table_crc: u32,
    /// Total file length, including the footer.
    pub file_len: u64,
    /// Copy of the header's CRC.
    pub header_crc: u32,
    /// CRC-32C of footer bytes 0..52.
    pub footer_crc: u32,
}

impl Footer {
    /// Encode, filling in `footer_crc`. Returns the bytes and the CRC.
    pub(crate) fn encode(
        table_offset: u64,
        section_count: u32,
        table_crc: u32,
        file_len: u64,
        header_crc: u32,
    ) -> ([u8; FOOTER_LEN], u32) {
        let mut out = Vec::with_capacity(FOOTER_LEN);
        put_u64(&mut out, table_offset);
        put_u32(&mut out, section_count);
        put_u32(&mut out, table_crc);
        put_u64(&mut out, file_len);
        put_u32(&mut out, header_crc);
        out.resize(FOOTER_CRC_AT, 0);
        let footer_crc = crc(&out);
        put_u32(&mut out, footer_crc);
        out.extend_from_slice(&FOOTER_MAGIC);
        let mut bytes = [0_u8; FOOTER_LEN];
        bytes.copy_from_slice(&out);
        (bytes, footer_crc)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, SegmentError> {
        let region = Region::Footer;
        let malformed = |error: super::error::Malformed| error.at(region);
        if bytes.len() != FOOTER_LEN {
            return Err(SegmentError::corrupt(region, "footer is truncated"));
        }
        if bytes[FOOTER_LEN - 8..] != FOOTER_MAGIC {
            return Err(SegmentError::corrupt(region, "bad footer magic"));
        }
        let mut cursor = Cursor::new(bytes);
        let table_offset = cursor.u64().map_err(malformed)?;
        let section_count = cursor.u32().map_err(malformed)?;
        let table_crc = cursor.u32().map_err(malformed)?;
        let file_len = cursor.u64().map_err(malformed)?;
        let header_crc = cursor.u32().map_err(malformed)?;
        cursor
            .zeros(FOOTER_CRC_AT - cursor.position())
            .map_err(malformed)?;
        let footer_crc = cursor.u32().map_err(malformed)?;
        if crc(&bytes[..FOOTER_CRC_AT]) != footer_crc {
            return Err(SegmentError::Checksum { region });
        }
        Ok(Self {
            table_offset,
            section_count,
            table_crc,
            file_len,
            header_crc,
            footer_crc,
        })
    }
}
