//! The engine's view of one segment v2 file: [`SegmentHandle`], and writing a new segment file
//! through the `Vfs`.
//!
//! A handle owns the open file (a [`SegmentReader`] over a [`VfsSource`]), the file's buffer
//! cache registration (dropped with the reader, which invalidates the file's cached units), and
//! its [`FileHandle`], which removes the file once the writer marked it obsolete and the last
//! `Version` holding it is gone (I7). Handles are shared by the writer's state and every
//! `Version` that contains the segment.

use crate::{
    cache::BufferCache,
    gc::{FileHandle, GcQueue},
    manifest::{FieldZone, ManifestSegment, SegmentOrigin, VectorSummary},
    paths::segment_path,
    segment_v2::{
        PkColumn, SectionKind, SegmentBuilder, SegmentError, SegmentReader, SegmentRow, StatValue,
        VfsSource, WrittenSegment,
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

/// One segment of a collection, open for reads.
pub(crate) struct SegmentHandle {
    pub(crate) unit: UnitId,
    /// The manifest entry the segment was committed with. Its `dv` is the one in force when the
    /// handle was opened; the writer's durable manifest is the source of truth for DV files.
    pub(crate) entry: ManifestSegment,
    // Field order matters: the reader (and its cache registration) is released before the file
    // handle enqueues the removal.
    reader: SegmentReader<VfsSource>,
    file: FileHandle,
}

impl SegmentHandle {
    /// Open the segment `entry` names in the collection directory `dir`: read and verify its
    /// header, footer, section table, and schema snapshot, and check them against the manifest
    /// entry and the collection. Loads go through `cache`.
    pub(crate) fn open(
        vfs: &dyn Vfs,
        dir: &Path,
        collection_id: &CollectionId,
        entry: ManifestSegment,
        cache: &BufferCache,
        gc: GcQueue,
    ) -> Result<Self> {
        let path = segment_path(dir, entry.unit);
        let file = vfs.open(&path, OpenMode::Read).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                segment_corrupt(
                    &path,
                    "the file the manifest names does not exist".to_owned(),
                )
            } else {
                LogPoseError::io(format!("failed to open '{}'", path.display()), error)
            }
        })?;
        Self::from_file(file, path, collection_id, entry, cache, gc)
    }

    /// Wrap an open segment file, checking it against its manifest entry.
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
            file: FileHandle::new(entry.unit, path, gc),
            entry,
            reader,
        })
    }

    pub(crate) fn row_count(&self) -> u32 {
        self.entry.row_count
    }

    pub(crate) fn reader(&self) -> &SegmentReader<VfsSource> {
        &self.reader
    }

    pub(crate) fn path(&self) -> &Path {
        self.file.path()
    }

    /// Mark the file for removal once the last holder drops it. Only the writer calls this,
    /// only after the manifest that drops the segment is durable.
    pub(crate) fn mark_obsolete(&self) {
        self.file.mark_obsolete();
    }

    /// Every row, read around the cache (a full scan must not evict hot data).
    pub(crate) fn read_rows(&self) -> Result<Vec<SegmentRow>> {
        self.reader
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
        self.reader
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
        self.reader
            .keys_uncached()
            .map_err(|error| segment_error(self.path(), error))
    }
}

impl fmt::Debug for SegmentHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentHandle")
            .field("unit", &self.unit)
            .field("rows", &self.entry.row_count)
            .field("file", &self.file)
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

impl crate::engine::CoreRef {
    /// Build `builder`'s index sections (the engine's [`IndexPolicy`](crate::segment_v2::IndexPolicy))
    /// on the maintenance pool. Flush and compaction call this before writing a segment.
    pub(crate) fn build_indexes(&self, builder: &mut SegmentBuilder) -> Result<()> {
        let policy = self.index;
        self.runtime()
            .maintenance
            .install(|| builder.build_index_sections(&policy))
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
