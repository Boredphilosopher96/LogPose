//! Index build: add a written segment's vector graphs in an index sidecar, then have the writer
//! publish the manifest in which the segment names it.
//!
//! Flush and compaction write a segment with its vectors, SQ8 codes, and scalar indexes, which
//! is one or two passes over its rows. The HNSW graph, which costs far more, is this job's: the
//! writer plans it for a segment that has SQ8 codes and no sidecar yet (see `writer/jobs.rs`),
//! and a search of the segment uses its SQ8 codes until the graph lands.
//!
//! 1. **Begin** (writer, on the permit). Capture the segment's handle and the job's
//!    cancellation flag; the job's unit `u` names its sidecar.
//! 2. **Build** (job thread, graphs on the maintenance pool). For each vector field the
//!    collection still declares that has SQ8 codes in the segment, read its vectors back around
//!    the buffer cache and build the graph over its distinct vectors. The build polls the flag
//!    (set when a compaction takes the segment, or on drop, poison, and shutdown) and the
//!    engine's shutdown before every insert.
//! 3. **Write** `segments/<segment>.idx.<u>` (`CreateNew`): the segment's header with the
//!    sidecar flag, its schema snapshot, and the graphs; `sync_all` (`IndexAfterSidecarSync`),
//!    then `sync_dir(segments/)` (`IndexAfterSegmentsDirSync`). A segment none of whose fields
//!    has two distinct vectors gets a sidecar without graphs, which records that its build ran.
//! 4. **Commit** (writer). If the segment is still in the durable manifest without a sidecar,
//!    manifest `g + 1` is the durable one with the segment's entry naming the sidecar and its
//!    graphs; once it is durable, the writer publishes a `Version` in which the segment's
//!    handle gains the sidecar (a new handle over the same open segment file). Otherwise (a
//!    compaction merged the segment away meanwhile) the sidecar is removed and nothing changes.
//!
//! Crash analysis: before the commit point, the sidecar is a file no durable manifest names,
//! so orphan cleanup removes it at the next open, and the writer plans the build again (a
//! segment without a sidecar needs one). After it, the manifest names the sidecar, and opening
//! the segment opens and checks the sidecar too. The sidecar is removed with its segment: the
//! writer marks both obsolete when a compaction retires the segment, and the last handle
//! holding each removes it.

use crate::{
    engine::CoreRef,
    fs_util::crash_point,
    handle::{CollectionHandle, JobTicket},
    manifest::IndexRef,
    paths::{SEGMENTS_DIR, index_path},
    segment::{segment_error, write_index_file},
    segment_v2::{GraphInput, SectionKind, build_graph_section},
    version::Version,
    writer::{IndexStart, IndexedSegment, JobCommit},
};
use logpose_types::{LogPoseError, Result, UnitId, schema::FieldRef};
use logpose_vfs::CrashPoint;
use std::sync::{Arc, atomic::Ordering};

impl CoreRef {
    /// Build and write what an index build begun at `version` commits: the sidecar of
    /// `work.segment` named by the job's unit `unit`, with a graph for every vector field that
    /// has SQ8 codes and at least two distinct vectors.
    pub(crate) fn build_index(
        &self,
        handle: &Arc<CollectionHandle>,
        version: &Version,
        unit: UnitId,
        work: &IndexStart,
        ticket: &mut JobTicket,
    ) -> Result<JobCommit> {
        let vfs = self.vfs.as_ref();
        let dir = &handle.meta().dir;
        let segment = &work.segment;
        let reader = segment.reader();
        let cancelled = || work.cancel.load(Ordering::Relaxed) || self.is_shutting_down();
        let mut sections = Vec::new();
        let mut graphs = Vec::new();
        for field in reader.schema().vectors() {
            if cancelled() {
                return Err(LogPoseError::unavailable("the index build was cancelled"));
            }
            // A field the collection dropped since is never searched; one without SQ8 codes is
            // scanned in f32, since walks traverse codes.
            let declared = matches!(
                version.schema.field_by_id(field.id),
                Some(FieldRef::Vector(current)) if current.dimensions == field.dimensions
            );
            if !declared
                || reader
                    .find_section(SectionKind::VectorSq8, Some(field.id))
                    .is_none()
            {
                continue;
            }
            let Some((values, nulls)) = reader
                .vectors_uncached(field.id)
                .map_err(|error| segment_error(segment.path(), error))?
            else {
                continue;
            };
            let input = GraphInput {
                field: field.id,
                dim: field.dimensions,
                metric: field.metric,
                rows: segment.row_count(),
                values,
                nulls,
            };
            let built = self
                .runtime()
                .maintenance
                .install(|| build_graph_section(input, self.index.hnsw, 2, &cancelled))
                .map_err(LogPoseError::from)?;
            if let Some(section) = built {
                graphs.push(field.id);
                sections.push(section);
            }
        }
        if cancelled() {
            return Err(LogPoseError::unavailable("the index build was cancelled"));
        }
        let path = index_path(dir, segment.unit, unit);
        ticket.writing_files();
        let (file, written) = write_index_file(vfs, &path, reader, &sections)?;
        drop(sections);
        crash_point(vfs, Some(CrashPoint::IndexAfterSidecarSync))?;
        let segments = dir.join(SEGMENTS_DIR);
        vfs.sync_dir(&segments).map_err(|error| {
            LogPoseError::io(format!("failed to sync '{}'", segments.display()), error)
        })?;
        crash_point(vfs, Some(CrashPoint::IndexAfterSegmentsDirSync))?;
        let reference = IndexRef {
            unit,
            file_len: written.file_len,
            footer_crc: written.footer_crc,
        };
        let open =
            segment.index_from_file(file, path, reference, self.buffer_cache(), self.gc.clone())?;
        Ok(JobCommit::Index {
            segment: segment.unit,
            index: IndexedSegment {
                file: Arc::new(open),
                reference,
                graphs,
            },
        })
    }
}

#[cfg(test)]
mod tests;
