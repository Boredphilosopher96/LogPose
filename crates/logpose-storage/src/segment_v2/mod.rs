//! Segment v2: the immutable columnar segment file of the v2 engine.
//!
//! A segment is one file: a 128-byte header, 64-byte aligned section
//! payloads, a section table, and a 64-byte footer (see [`format`](mod@format)). Every
//! byte except alignment padding is covered by a CRC-32C: the header, the
//! table, the footer, and each payload, and `VectorF32` pages and
//! `DynamicJson` blocks also carry their own CRCs so they can be loaded and
//! verified one at a time.
//!
//! Storage owns these encodings:
//!
//! | Kind | Contents |
//! | --- | --- |
//! | `SchemaSnapshot` | postcard `CollectionSchema` |
//! | `RowMeta` | per-row sequence numbers ([`builder`]) |
//! | `PkColumn`, `PkSorted`, `PkFilter` | keys in row order, rows in key order, binary fuse filter ([`pk`]) |
//! | `Stats` | zone maps, distinct counts, top values, histograms ([`stats`]) |
//! | `VectorF32` | raw vectors in CRC-checked pages ([`vector`]) |
//! | `ScalarColumn` | typed columns ([`column`](mod@column)) |
//! | `DynamicJson` | `$extra` in 4096-row blocks ([`dynamic`]) |
//!
//! `VectorSq8`, `VectorGraph`, `ScalarInverted`, and `ScalarSorted`
//! payloads are opaque here: [`SegmentBuilder::add_index_section`] stores
//! whatever `logpose-index` produces and [`SegmentReader::index_section`]
//! returns it CRC-checked. Readers ignore section kinds they do not know.
//!
//! [`SegmentReader`] reads through a [`SectionSource`] (a positioned-read
//! file abstraction), opens with two small reads at the ends of the file,
//! and verifies each section only when it is loaded.

pub mod builder;
pub mod column;
pub mod dynamic;
mod error;
pub mod format;
mod le;
pub mod pk;
pub mod reader;
pub mod source;
pub mod stats;
pub mod vector;

#[cfg(test)]
mod tests;

pub use builder::{
    IndexSection, IndexSectionKind, MAX_SEGMENT_ROWS, RowWriter, SegmentBuilder, SegmentIdentity,
    WrittenSegment,
};
pub use column::{ColumnEncoding, ScalarColumn};
pub use dynamic::{DYNAMIC_BLOCK_ROWS, DynamicBlock, DynamicBlockRef, DynamicIndex};
pub use error::{Region, SegmentError};
pub use format::{FORMAT_VERSION, Footer, NO_FIELD, SectionEntry, SectionKind, SegmentHeader};
pub use pk::{PkColumn, PkFilter, PkSorted, canonical_pk_hash};
pub use reader::{DynamicHandle, SegmentReader, SegmentRow, VectorHandle};
pub use source::{FileSource, MemorySource, SectionSource};
pub use stats::{FieldStats, HistogramBucket, SegmentStats, StatValue};
pub use vector::{VectorPrefix, page_rows_for};
