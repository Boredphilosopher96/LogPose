//! `Version`: an immutable, self-consistent view of one collection.
//!
//! A `Version` holds the collection's units (the segments of its durable manifest, the frozen
//! memtables being flushed, and the active memtable, each as of `visible_seq_no`) and the
//! deletion vector of every unit with a deleted row. A row is live in a `Version` exactly when
//! its bit is not set in its unit's deletion vector, and each primary key has at most one live
//! row across all units (I5). Everything reachable from a published `Version` is immutable or
//! persistent: the writer builds the next one with `Arc` and O(1) persistent-structure clones
//! and swaps it in. The segment handles it holds keep their files on disk while it lives (I7).

use crate::{
    dv::DeletionMap, handle::CollectionMeta, manifest::Manifest, memtable::MemtableData,
    segment::SegmentHandle,
};
use logpose_types::{LogPoseError, Result, RowAddr, SeqNo, UnitId, schema::CollectionSchema};
use std::{collections::HashMap, fmt, sync::Arc};

/// Identifier of a published [`Version`]. Strictly increasing per collection (I6).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VersionId(pub u64);

/// Counters maintained incrementally so that statistics and flush triggers are O(1).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VersionCounters {
    /// Rows over every unit (segment rows and memtable slots), deleted ones included.
    pub total_rows: u64,
    /// Rows set in a deletion vector, over every unit.
    pub deleted_rows: u64,
    /// Segments in the manifest.
    pub segment_count: u32,
    /// Slots in the frozen and active memtables.
    pub memtable_rows: u64,
    /// Bytes of the frozen and active memtables, as the flush trigger measures them.
    pub memtable_bytes: u64,
}

impl VersionCounters {
    /// Rows that are not deleted.
    #[must_use]
    pub fn live_rows(&self) -> u64 {
        self.total_rows.saturating_sub(self.deleted_rows)
    }
}

/// A complete, immutable, self-consistent view of one collection.
///
/// Readers `load_full()` it from the collection handle and hold the `Arc` for a whole request;
/// only the collection's writer publishes new ones.
pub struct Version {
    /// Strictly increasing per collection (I6).
    pub id: VersionId,
    /// Identity and configuration of the collection.
    pub meta: Arc<CollectionMeta>,
    /// The schema as of `visible_seq_no`. Readers map names to field ids with this schema only.
    pub schema: Arc<CollectionSchema>,
    /// Last sequence number of the last batch included (I3).
    pub visible_seq_no: SeqNo,
    /// Durable manifest generation at publish time.
    pub manifest_generation: u64,
    /// Every operation at or below this sequence number is in a segment or a DV file.
    pub checkpoint_seq_no: SeqNo,
    /// Incrementally maintained counters.
    pub counters: VersionCounters,
    /// Segments, ascending by unit.
    pub(crate) segments: Arc<[Arc<SegmentHandle>]>,
    /// Frozen memtables being flushed, oldest first.
    pub(crate) frozen: Arc<[Arc<MemtableData>]>,
    /// The active memtable as of `visible_seq_no`.
    pub(crate) active: Arc<MemtableData>,
    /// Deletion vectors of every unit with a deleted row.
    pub(crate) deletes: DeletionMap,
    /// The durable manifest at publish time (diagnostics; not a read key).
    pub(crate) manifest: Arc<Manifest>,
}

/// One unit of a `Version`: a segment or a memtable.
#[derive(Clone, Copy)]
pub(crate) enum UnitRef<'a> {
    Segment(&'a Arc<SegmentHandle>),
    Memtable(&'a Arc<MemtableData>),
}

impl UnitRef<'_> {
    pub(crate) fn id(&self) -> UnitId {
        match self {
            Self::Segment(segment) => segment.unit,
            Self::Memtable(memtable) => memtable.unit,
        }
    }

    pub(crate) fn row_count(&self) -> u32 {
        match self {
            Self::Segment(segment) => segment.row_count(),
            Self::Memtable(memtable) => memtable.slot_count(),
        }
    }
}

impl Version {
    /// The names of the files in `segments/` this version reads: every segment's file and its
    /// index sidecar, if it has one. For tests that check that no file a live version reads is
    /// removed (I7).
    #[doc(hidden)]
    #[must_use]
    pub fn segment_file_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for segment in self.segments.iter() {
            for path in std::iter::once(segment.path())
                .chain(segment.index_file().map(crate::segment::OpenFile::path))
            {
                if let Some(name) = path.file_name() {
                    names.push(name.to_string_lossy().into_owned());
                }
            }
        }
        names
    }

    /// How many segments have their index sidecar (their index build committed), and how many
    /// have a vector graph. For tests and diagnostics.
    #[doc(hidden)]
    #[must_use]
    pub fn indexed_segments(&self) -> (usize, usize) {
        let sidecars = self
            .segments
            .iter()
            .filter(|segment| segment.entry.index.is_some())
            .count();
        let graphs = self
            .segments
            .iter()
            .filter(|segment| segment.entry.vectors.iter().any(|vector| vector.has_graph))
            .count();
        (sidecars, graphs)
    }

    /// Segments ascending by unit, then frozen memtables oldest first, then the active
    /// memtable. By I5 no read depends on the order.
    pub(crate) fn units(&self) -> impl Iterator<Item = UnitRef<'_>> + '_ {
        self.segments
            .iter()
            .map(UnitRef::Segment)
            .chain(self.memtables().map(UnitRef::Memtable))
    }

    /// Frozen memtables oldest first, then the active memtable.
    pub(crate) fn memtables(&self) -> impl DoubleEndedIterator<Item = &Arc<MemtableData>> + '_ {
        self.frozen.iter().chain(std::iter::once(&self.active))
    }

    /// Whether `addr` is set in its unit's deletion vector.
    pub(crate) fn is_deleted(&self, addr: RowAddr) -> bool {
        self.deletes.is_deleted(addr)
    }

    /// Recompute the counters from the units and deletion vectors.
    pub(crate) fn recount(&self) -> VersionCounters {
        let memtable_rows = self
            .memtables()
            .map(|memtable| u64::from(memtable.slot_count()))
            .sum::<u64>();
        VersionCounters {
            total_rows: self
                .segments
                .iter()
                .map(|segment| u64::from(segment.row_count()))
                .sum::<u64>()
                + memtable_rows,
            deleted_rows: self.deletes.total(),
            segment_count: u32::try_from(self.segments.len()).unwrap_or(u32::MAX),
            memtable_rows,
            memtable_bytes: self
                .memtables()
                .map(|memtable| memtable.bytes().total())
                .sum(),
        }
    }

    /// Check the invariants every published version must satisfy. O(data): it reads every
    /// segment's keys, so it is for tests and `strict_invariants` only.
    ///
    /// - I3 and contiguity: the memtables cover `checkpoint + 1 ..= visible_seq_no` back to
    ///   back, and `visible_seq_no` is the active memtable's last operation.
    /// - The segments match the manifest, ascending by unit.
    /// - I10: every deletion vector belongs to a unit of the version and stays below its row
    ///   count.
    /// - I13: the counters equal the values computed from the units and deletion vectors.
    /// - I5: every primary key has at most one live row across all units.
    pub fn check_invariants(&self) -> Result<()> {
        let fail = |message: String| {
            Err(LogPoseError::internal(format!(
                "version {} of collection '{}' violates an invariant: {message}",
                self.id.0,
                self.meta.descriptor.lookup_name()
            )))
        };
        let mut expected_first = self.checkpoint_seq_no + 1;
        for memtable in self.memtables() {
            if memtable.first_seq_no != expected_first {
                return fail(format!(
                    "memtable {} starts at {}, expected {expected_first}",
                    memtable.unit, memtable.first_seq_no
                ));
            }
            if memtable.last_seq_no + 1 < memtable.first_seq_no {
                return fail(format!("memtable {} ends before it starts", memtable.unit));
            }
            expected_first = memtable.last_seq_no + 1;
        }
        if self.visible_seq_no != self.active.last_seq_no {
            return fail(format!(
                "visible_seq_no {} is not the active memtable's last operation {}",
                self.visible_seq_no, self.active.last_seq_no
            ));
        }
        if self.manifest_generation != self.manifest.generation
            || self.checkpoint_seq_no != self.manifest.checkpoint_seq_no
        {
            return fail("manifest summary does not match the manifest".to_owned());
        }
        if !self
            .segments
            .iter()
            .map(|segment| segment.unit)
            .eq(self.manifest.units())
        {
            return fail("segments do not match the manifest's segments".to_owned());
        }
        for (segment, entry) in self.segments.iter().zip(&self.manifest.segments) {
            // A segment's index sidecar and graph summaries are the manifest's (its DV file is
            // the manifest's alone: flushes write new ones without replacing the handle).
            if segment.entry.index != entry.index
                || segment.index_file().is_some() != entry.index.is_some()
                || segment.entry.vectors != entry.vectors
            {
                return fail(format!(
                    "segment {} does not match the manifest's index sidecar",
                    segment.unit
                ));
            }
        }
        let rows = self
            .units()
            .map(|unit| (unit.id(), unit.row_count()))
            .collect::<HashMap<_, _>>();
        for (unit, dv) in self.deletes.iter() {
            let Some(row_count) = rows.get(unit) else {
                return fail(format!(
                    "deletion vector of unit {unit}, which is not in it"
                ));
            };
            if dv.iter().last().is_some_and(|row| row >= *row_count) {
                return fail(format!(
                    "deletion vector of unit {unit} is beyond its {row_count} rows"
                ));
            }
        }
        let recomputed = self.recount();
        if recomputed != self.counters {
            return fail(format!(
                "counters {:?} differ from recomputed {recomputed:?}",
                self.counters
            ));
        }
        let mut live = HashMap::new();
        for unit in self.units() {
            let keys: Vec<(logpose_types::record::PrimaryKey, u32)> = match unit {
                UnitRef::Segment(segment) => {
                    let (pks, _) = segment.keys()?;
                    (0..segment.row_count())
                        .filter_map(|row| pks.get(row as usize).map(|pk| (pk, row)))
                        .collect()
                }
                UnitRef::Memtable(memtable) => (0..memtable.slot_count())
                    .filter_map(|slot| memtable.pk(slot).map(|pk| (pk.clone(), slot)))
                    .collect(),
            };
            for (pk, row) in keys {
                let addr = RowAddr {
                    unit: unit.id(),
                    row,
                };
                if self.is_deleted(addr) {
                    continue;
                }
                if let Some(other) = live.insert(pk.clone(), addr) {
                    return fail(format!(
                        "key {pk} has two live rows, at {other} and at {addr} (I5)"
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl Version {
    /// The live row of `pk` with its sequence number: a point lookup over the units, newest
    /// first, as readers do it (never through the writer's index).
    pub(crate) fn get(
        &self,
        pk: &logpose_types::record::PrimaryKey,
    ) -> Option<(SeqNo, logpose_wal::codec::RowImage)> {
        for memtable in self.memtables().rev() {
            if let Some(slot) = memtable.find(pk)
                && !self.is_deleted(RowAddr {
                    unit: memtable.unit,
                    row: slot,
                })
            {
                return Some((memtable.seq_no(slot)?, memtable.row_image(slot).ok()?));
            }
        }
        for segment in self.segments.iter().rev() {
            // A segment without the key says nothing about older ones.
            let Some(row) = segment.reader().find_row(pk).ok()? else {
                continue;
            };
            if !self.is_deleted(RowAddr {
                unit: segment.unit,
                row,
            }) {
                let image = segment.row_images(&[row]).ok()?.pop()?;
                let (_, seqs) = segment.keys().ok()?;
                return Some((*seqs.get(row as usize)?, image));
            }
        }
        None
    }

    /// Every live row with its sequence number.
    pub(crate) fn live_images(&self) -> Vec<(SeqNo, logpose_wal::codec::RowImage)> {
        let mut rows = Vec::new();
        for unit in self.units() {
            match unit {
                UnitRef::Segment(segment) => {
                    for (row, stored) in (0_u32..).zip(segment.read_rows().expect("rows")) {
                        if !self.is_deleted(RowAddr {
                            unit: segment.unit,
                            row,
                        }) {
                            rows.push((stored.seq_no, stored.image));
                        }
                    }
                }
                UnitRef::Memtable(memtable) => {
                    for slot in 0..memtable.slot_count() {
                        if !self.is_deleted(RowAddr {
                            unit: memtable.unit,
                            row: slot,
                        }) {
                            rows.push((
                                memtable.seq_no(slot).expect("seq"),
                                memtable.row_image(slot).expect("image"),
                            ));
                        }
                    }
                }
            }
        }
        rows
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Version")
            .field("id", &self.id)
            .field("collection", &self.meta.reference)
            .field("schema_version", &self.schema.schema_version())
            .field("visible_seq_no", &self.visible_seq_no)
            .field("manifest_generation", &self.manifest_generation)
            .field("checkpoint_seq_no", &self.checkpoint_seq_no)
            .field("counters", &self.counters)
            .field("frozen", &self.frozen.len())
            .field("deletes", &self.deletes)
            .finish()
    }
}
