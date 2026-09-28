//! The engine's view of one segment v2 file: [`SegmentHandle`], and writing a new segment file
//! (or an index sidecar) through the `Vfs`.
//!
//! A handle owns the open file (a [`SegmentReader`] over a [`VfsSource`]), the file's buffer
//! cache registration (dropped with the reader, which invalidates the file's cached units), and
//! its [`FileHandle`], which removes the file once the writer marked it obsolete and the last
//! `Version` holding it is gone (I7). Handles are shared by the writer's state and every
//! `Version` that contains the segment.
//!
//! Once the segment's index build commits, the writer replaces its handle with one over the
//! same open segment file plus the index sidecar ([`SegmentHandle::with_index`]). Versions
//! published before hold the old handle, which shares the segment file, so the file is removed
//! only after the last handle of either kind is gone; the sidecar has its own [`FileHandle`],
//! which the writer marks obsolete with the segment's.

use crate::{
    cache::BufferCache,
    gc::{FileHandle, GcQueue},
    manifest::{FieldZone, IndexRef, ManifestSegment, SegmentOrigin, VectorSummary},
    paths::{index_path, segment_path},
    segment_v2::{
        IndexSection, PkColumn, SectionKind, SegmentBuilder, SegmentError, SegmentReader,
        SegmentRow, StatValue, VfsSource, WrittenSegment, WrittenSidecar, write_index_sidecar,
    },
};
use logpose_types::{
    CollectionId, CorruptionKind, LogPoseError, Result, SeqNo, UnitId,
    schema::{CollectionSchema, FieldId, FieldRef},
    value::{Timestamp, Value},
};
use logpose_vfs::{OpenMode, Vfs, VfsFile};
use logpose_wal::codec::ValueBytes;
use std::{
    fmt,
    io::{self, BufWriter, IoSlice, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

/// One open file in the segment format (a segment or an index sidecar): its reader, attached to
/// the buffer cache, and its GC handle.
pub(crate) struct OpenFile {
    // Field order matters: the reader (and its cache registration) is released before the file
    // handle enqueues the removal.
    reader: SegmentReader<VfsSource>,
    file: FileHandle,
}

impl OpenFile {
    pub(crate) fn reader(&self) -> &SegmentReader<VfsSource> {
        &self.reader
    }

    pub(crate) fn path(&self) -> &Path {
        self.file.path()
    }
}

/// One segment of a collection, open for reads.
pub(crate) struct SegmentHandle {
    pub(crate) unit: UnitId,
    /// The manifest entry the segment was committed with (or gained its index sidecar with).
    /// Its `dv` is the one in force when the handle was opened; the writer's durable manifest is
    /// the source of truth for DV files.
    pub(crate) entry: ManifestSegment,
    /// The segment file, shared by every handle of the unit.
    data: Arc<OpenFile>,
    /// The index sidecar the manifest entry names, if any.
    index: Option<Arc<OpenFile>>,
}

impl SegmentHandle {
    /// Open the segment `entry` names in the collection directory `dir`, and its index sidecar
    /// if the entry names one: read and verify their headers, footers, section tables, and schema
    /// snapshots, and check them against the manifest entry and the collection. Loads go
    /// through `cache`.
    pub(crate) fn open(
        vfs: &dyn Vfs,
        dir: &Path,
        collection_id: &CollectionId,
        entry: ManifestSegment,
        cache: &BufferCache,
        gc: GcQueue,
    ) -> Result<Self> {
        let path = segment_path(dir, entry.unit);
        let file = open_named(vfs, &path, CorruptionKind::Segment)?;
        let index = entry.index;
        let segment = Self::from_file(file, path, collection_id, entry, cache, gc.clone())?;
        let Some(reference) = index else {
            return Ok(segment);
        };
        let path = index_path(dir, segment.unit, reference.unit);
        let file = open_named(vfs, &path, CorruptionKind::Index)?;
        let sidecar = segment.index_from_file(file, path, reference, cache, gc)?;
        Ok(Self {
            index: Some(Arc::new(sidecar)),
            ..segment
        })
    }

    /// Wrap an open segment file, checking it against its manifest entry. The handle has no
    /// index sidecar.
    pub(crate) fn from_file(
        file: Arc<dyn VfsFile>,
        path: PathBuf,
        collection_id: &CollectionId,
        entry: ManifestSegment,
        cache: &BufferCache,
        gc: GcQueue,
    ) -> Result<Self> {
        let reader = SegmentReader::open(VfsSource(file))
            .map_err(|error| segment_error(&path, error))?
            .with_cache(cache);
        let header = reader.header();
        let mismatch = |what: String| {
            Err(segment_corrupt(
                &path,
                format!("{what} does not match the manifest"),
            ))
        };
        if header.is_index_sidecar() {
            return mismatch("an index sidecar's header".to_owned());
        }
        if header.unit_id != entry.unit.0 {
            return mismatch(format!("unit id {:08x}", header.unit_id));
        }
        if &header.collection_id != collection_id {
            return mismatch(format!("collection id {}", header.collection_id));
        }
        if reader.file_len() != entry.file_len || reader.footer().footer_crc != entry.footer_crc {
            return mismatch("the file length or footer checksum".to_owned());
        }
        if header.row_count != entry.row_count
            || header.min_seq_no != entry.min_seq_no
            || header.max_seq_no != entry.max_seq_no
            || header.schema_version != entry.schema_version
        {
            return mismatch("the row count, sequence range, or schema version".to_owned());
        }
        Ok(Self {
            unit: entry.unit,
            data: Arc::new(OpenFile {
                file: FileHandle::new(entry.unit, path, gc),
                reader,
            }),
            entry,
            index: None,
        })
    }

    /// Wrap an open index sidecar of this segment, checking it against `reference` and against
    /// the segment's own header: a sidecar carries the header of the segment it indexes.
    pub(crate) fn index_from_file(
        &self,
        file: Arc<dyn VfsFile>,
        path: PathBuf,
        reference: IndexRef,
        cache: &BufferCache,
        gc: GcQueue,
    ) -> Result<OpenFile> {
        let reader = SegmentReader::open(VfsSource(file))
            .map_err(|error| index_error(&path, error))?
            .with_cache(cache);
        let header = reader.header();
        let segment = self.reader().header();
        let mismatch = |what: &str| {
            Err(LogPoseError::Corrupt {
                kind: CorruptionKind::Index,
                location: Some(path.display().to_string()),
                message: format!(
                    "index sidecar '{}': {what} does not match its segment and manifest",
                    path.display()
                ),
            })
        };
        if !header.is_index_sidecar() {
            return mismatch("the header flags");
        }
        let same_segment = header.collection_id == segment.collection_id
            && header.unit_id == segment.unit_id
            && header.row_count == segment.row_count
            && header.schema_version == segment.schema_version
            && header.schema_hash == segment.schema_hash
            && header.min_seq_no == segment.min_seq_no
            && header.max_seq_no == segment.max_seq_no;
        if !same_segment {
            return mismatch("the header");
        }
        if reader.file_len() != reference.file_len
            || reader.footer().footer_crc != reference.footer_crc
        {
            return mismatch("the file length or footer checksum");
        }
        Ok(OpenFile {
            file: FileHandle::new(reference.unit, path, gc),
            reader,
        })
    }

    /// This segment with the index sidecar `index` and the manifest entry that names it: a new
    /// handle over the same open segment file.
    pub(crate) fn with_index(&self, index: Arc<OpenFile>, entry: ManifestSegment) -> Self {
        Self {
            unit: self.unit,
            entry,
            data: Arc::clone(&self.data),
            index: Some(index),
        }
    }

    pub(crate) fn row_count(&self) -> u32 {
        self.entry.row_count
    }

    pub(crate) fn reader(&self) -> &SegmentReader<VfsSource> {
        &self.data.reader
    }

    /// The index sidecar, if the segment has one.
    pub(crate) fn index_file(&self) -> Option<&OpenFile> {
        self.index.as_deref()
    }

    /// The segment file itself.
    pub(crate) fn data_file(&self) -> &OpenFile {
        &self.data
    }

    /// The file holding the section of `kind` for `field`: the index sidecar for a vector
    /// graph, the segment file for everything else. `None` for a graph the segment has not
    /// gained yet.
    pub(crate) fn section_file(&self, kind: SectionKind, field: Option<FieldId>) -> Option<&OpenFile> {
        match (kind, field) {
            (SectionKind::VectorGraph, Some(field)) => self.graph_file(field),
            (SectionKind::VectorGraph, None) => None,
            _ => Some(&self.data),
        }
    }

    /// The file holding `field`'s vector graph, if the segment has one: its index sidecar.
    pub(crate) fn graph_file(&self, field: FieldId) -> Option<&OpenFile> {
        self.index.as_deref().filter(|index| {
            index
                .reader()
                .find_section(SectionKind::VectorGraph, Some(field))
                .is_some()
        })
    }

    pub(crate) fn path(&self) -> &Path {
        self.data.path()
    }

    /// Mark the segment file, and its index sidecar if any, for removal once the last holder
    /// drops them. Only the writer calls this, only after the manifest that drops the segment
    /// is durable.
    pub(crate) fn mark_obsolete(&self) {
        self.data.file.mark_obsolete();
        if let Some(index) = &self.index {
            index.file.mark_obsolete();
        }
    }

    /// Every row, read around the cache (a full scan must not evict hot data).
    pub(crate) fn read_rows(&self) -> Result<Vec<SegmentRow>> {
        self.reader()
            .read_rows()
            .map_err(|error| segment_error(self.path(), error))
    }

    /// Visit the rows `wanted` accepts, in row order, read around the cache. The segment's
    /// sections are held while it runs; each row is decoded when visited, so a caller that
    /// copies rows elsewhere never holds a second copy of every row. A `visit` error is
    /// returned as is.
    pub(crate) fn for_each_row(
        &self,
        wanted: impl FnMut(u32) -> bool,
        mut visit: impl FnMut(u32, SegmentRow) -> Result<()>,
    ) -> Result<()> {
        enum Stop {
            Segment(SegmentError),
            Visit(LogPoseError),
        }
        impl From<SegmentError> for Stop {
            fn from(error: SegmentError) -> Self {
                Self::Segment(error)
            }
        }
        self.reader()
            .for_each_row(wanted, |row, stored| {
                visit(row, stored).map_err(Stop::Visit)
            })
            .map_err(|stop| match stop {
                Stop::Segment(error) => segment_error(self.path(), error),
                Stop::Visit(error) => error,
            })
    }

    /// Keys and sequence numbers in row order, read around the cache.
    pub(crate) fn keys(&self) -> Result<(PkColumn, Vec<SeqNo>)> {
        self.reader()
            .keys_uncached()
            .map_err(|error| segment_error(self.path(), error))
    }
}

/// Open a file a manifest names; a missing file is corruption of `kind`.
fn open_named(vfs: &dyn Vfs, path: &Path, kind: CorruptionKind) -> Result<Arc<dyn VfsFile>> {
    vfs.open(path, OpenMode::Read).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            LogPoseError::Corrupt {
                kind,
                location: Some(path.display().to_string()),
                message: format!(
                    "'{}': the file the manifest names does not exist",
                    path.display()
                ),
            }
        } else {
            LogPoseError::io(format!("failed to open '{}'", path.display()), error)
        }
    })
}

/// Map a segment-format error on the index sidecar at `path`: stored-byte defects are
/// `Corrupt { kind: Index }`, the rest as [`segment_error`].
pub(crate) fn index_error(path: &Path, error: SegmentError) -> LogPoseError {
    if error.is_corruption() {
        return LogPoseError::Corrupt {
            kind: CorruptionKind::Index,
            location: Some(path.display().to_string()),
            message: format!("index sidecar '{}': {error}", path.display()),
        };
    }
    segment_error(path, error)
}

impl fmt::Debug for SegmentHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentHandle")
            .field("unit", &self.unit)
            .field("rows", &self.entry.row_count)
            .field("file", &self.data.file)
            .field("index", &self.index.as_ref().map(|index| &index.file))
            .finish()
    }
}

/// A segment corruption error at `path`.
pub(crate) fn segment_corrupt(path: &Path, message: String) -> LogPoseError {
    LogPoseError::Corrupt {
        kind: CorruptionKind::Segment,
        location: Some(path.display().to_string()),
        message: format!("segment '{}': {message}", path.display()),
    }
}

/// Map a segment error on the file at `path` to the typed taxonomy: stored-byte defects are
/// `Corrupt { kind: Segment }`, or `Corrupt { kind: Index }` in an index section, with the file
/// and region, I/O is `Io`, and anything else is a
/// bug in the engine (`Internal`).
pub(crate) fn segment_error(path: &Path, error: SegmentError) -> LogPoseError {
    if error.is_corruption() {
        return LogPoseError::Corrupt {
            kind: error.corruption_kind(),
            location: Some(path.display().to_string()),
            message: format!("segment '{}': {error}", path.display()),
        };
    }
    match error {
        SegmentError::Io(source) => LogPoseError::Io {
            context: format!("segment '{}' I/O failed", path.display()),
            source,
        },
        other => LogPoseError::internal(format!("segment '{}': {other}", path.display())),
    }
}

/// `io::Write` over a `VfsFile`'s appends. Wrapped in a `BufWriter`, because each `append` is
/// one torn-write unit in the fault model and the segment writer makes many small writes.
struct AppendWriter(Arc<dyn VfsFile>);

impl Write for AppendWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.append(&[IoSlice::new(buf)])?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Write `builder` as the new segment file `path` (`CreateNew`, buffered appends, `sync_all`)
/// and return the open file with what was written. The caller syncs `segments/` before a
/// manifest names the file.
pub(crate) fn write_segment(
    vfs: &dyn Vfs,
    path: &Path,
    builder: SegmentBuilder,
) -> Result<(Arc<dyn VfsFile>, WrittenSegment)> {
    let io_error =
        |what: &str, error| LogPoseError::io(format!("{what} '{}'", path.display()), error);
    let file = vfs
        .open(path, OpenMode::CreateNew)
        .map_err(|error| io_error("failed to create", error))?;
    let mut out = BufWriter::with_capacity(1 << 20, AppendWriter(Arc::clone(&file)));
    let written = builder
        .finish(&mut out)
        .map_err(|error| segment_error(path, error))?;
    out.flush()
        .map_err(|error| io_error("failed to write", error))?;
    drop(out);
    file.sync_all()
        .map_err(|error| io_error("failed to sync", error))?;
    Ok((file, written))
}

/// Write the index sidecar of `segment` as the new file `path` (`CreateNew`, buffered appends,
/// `sync_all`) and return the open file with what was written. The caller syncs `segments/`
/// before a manifest names the file.
pub(crate) fn write_index_file(
    vfs: &dyn Vfs,
    path: &Path,
    segment: &SegmentReader<VfsSource>,
    sections: &[IndexSection],
) -> Result<(Arc<dyn VfsFile>, WrittenSidecar)> {
    let io_error =
        |what: &str, error| LogPoseError::io(format!("{what} '{}'", path.display()), error);
    let schema = segment
        .schema_snapshot_bytes()
        .map_err(|error| segment_error(path, error))?;
    let file = vfs
        .open(path, OpenMode::CreateNew)
        .map_err(|error| io_error("failed to create", error))?;
    let mut out = BufWriter::with_capacity(1 << 20, AppendWriter(Arc::clone(&file)));
    let written = write_index_sidecar(segment.header(), &schema, sections, &mut out)
        .map_err(|error| index_error(path, error))?;
    out.flush()
        .map_err(|error| io_error("failed to write", error))?;
    drop(out);
    file.sync_all()
        .map_err(|error| io_error("failed to sync", error))?;
    Ok((file, written))
}

impl crate::engine::CoreRef {
    /// Build `builder`'s own index sections (SQ8 codes and scalar indexes, per the engine's
    /// [`IndexPolicy`](crate::segment_v2::IndexPolicy)) on the calling job thread. Flush and
    /// compaction call this before writing a segment. It is one or two passes over the rows,
    /// so it never waits behind a graph build on the maintenance pool; graphs come later from
    /// the segment's index-build job.
    pub(crate) fn build_indexes(&self, builder: &mut SegmentBuilder) -> Result<()> {
        builder
            .build_index_sections(&self.index)
            .map(|_| ())
            .map_err(LogPoseError::from)
    }
}

/// Tier 0 holds segments below this many rows; tier `t >= 1` holds
/// `[BASE_ROWS * RATIO^(t-1), BASE_ROWS * RATIO^t)`.
const TIER_BASE_ROWS: u64 = 32_768;
const TIER_RATIO: u64 = 4;

/// The size tier of a segment of `rows` live rows.
pub(crate) fn tier_for(rows: u64) -> u8 {
    let mut tier = 0;
    let mut bound = TIER_BASE_ROWS;
    while rows >= bound {
        tier += 1;
        match bound.checked_mul(TIER_RATIO) {
            Some(next) => bound = next,
            None => break,
        }
    }
    tier
}

/// The manifest entry of a segment written for `unit` with `schema`.
pub(crate) fn manifest_entry(
    unit: UnitId,
    schema: &CollectionSchema,
    written: &WrittenSegment,
    origin: SegmentOrigin,
) -> ManifestSegment {
    let header = &written.header;
    let mut vectors = Vec::new();
    let mut zones = Vec::new();
    let has_section = |kind: SectionKind, field: FieldId| {
        written
            .sections
            .iter()
            .any(|entry| entry.kind == kind.code() && entry.field == Some(field))
    };
    for stats in &written.stats.fields {
        match schema.field_by_id(stats.field) {
            Some(FieldRef::Vector(_)) => vectors.push(VectorSummary {
                field_id: stats.field.0,
                has_graph: has_section(SectionKind::VectorGraph, stats.field),
                has_sq8: has_section(SectionKind::VectorSq8, stats.field),
                non_null: header.row_count.saturating_sub(stats.null_count),
            }),
            Some(FieldRef::Scalar(_)) => zones.push(FieldZone {
                field_id: stats.field.0,
                min: stats.min.as_ref().and_then(zone_bytes),
                max: stats.max.as_ref().and_then(zone_bytes),
                null_count: stats.null_count,
                distinct_estimate: stats
                    .distinct
                    .map_or(0, |distinct| u32::try_from(distinct).unwrap_or(u32::MAX)),
            }),
            _ => {}
        }
    }
    ManifestSegment {
        unit,
        file_len: written.file_len,
        footer_crc: written.footer_crc,
        row_count: header.row_count,
        schema_version: header.schema_version,
        min_seq_no: header.min_seq_no,
        max_seq_no: header.max_seq_no,
        origin,
        tier: tier_for(u64::from(header.row_count)),
        dv: None,
        index: None,
        vectors,
        zones,
    }
}

/// A zone bound in the binary value codec.
fn zone_bytes(value: &StatValue) -> Option<Vec<u8>> {
    let value = match value {
        StatValue::Bool(value) => Value::Bool(*value),
        StatValue::Int64(value) => Value::Int64(*value),
        StatValue::Float64(value) => Value::Float64(*value),
        StatValue::String(value) => Value::String(value.clone()),
        StatValue::Timestamp(micros) => Value::Timestamp(Timestamp::from_micros(*micros).ok()?),
    };
    ValueBytes::encode(&value)
        .ok()
        .map(|bytes| bytes.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiers_grow_by_the_ratio() {
        assert_eq!(tier_for(0), 0);
        assert_eq!(tier_for(32_767), 0);
        assert_eq!(tier_for(32_768), 1);
        assert_eq!(tier_for(4 * 32_768 - 1), 1);
        assert_eq!(tier_for(4 * 32_768), 2);
        assert_eq!(tier_for(u64::MAX), 25);
    }
}
