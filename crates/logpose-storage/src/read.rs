//! The read path's storage side: [`CollectionReader`], [`ReadView`], and [`UnitView`].
//!
//! `logpose-storage` exposes data access; `logpose-query` owns predicate compilation, strategy
//! choice, and execution. A read starts with [`CollectionReader::read_view`], which resolves the
//! request's [`ReadOptions`] to one `Arc<Version>` (the current one, the one a snapshot token
//! pins, or an exact retained snapshot) and holds it for the whole request, so every stage of a
//! query sees the same state (I4) and no flush or compaction can expire it midway. With
//! `pin: true` the view also pins its version under a new [`SnapshotToken`] for later requests.
//!
//! Execution is staged so that no `rayon` worker ever does I/O:
//!
//! 1. **Fetch** ([`ReadView::fetch`], the only async data access): a [`FetchPlan`] lists, per
//!    unit, the sections a stage needs ([`SectionNeed`]). Segment units load through the buffer
//!    cache on the I/O pool and come back pinned in a [`PinSet`] with a [`FetchReport`]; index
//!    sections are decoded once per cache load and the decoded form rides with the cached
//!    bytes. Memtable units need nothing.
//! 2. **Compute** ([`ReadView::run`] on the query pool): the synchronous [`UnitView`]
//!    accessors read memtables directly and segments only through the pins. A segment section
//!    that was not fetched is a bug in the caller (`Internal`), never a reason to do I/O.
//! 3. **Project** ([`ReadView::rows`]): the final rows are read on the I/O pool.
//!
//! [`ReadView::get`] does its own fetches. [`RowSetResolver`] is the filter-resolution hook the
//! writer calls for delete-by-filter and update-by-filter; `logpose-query` implements it.

use crate::{
    cache::{AlignedBytes, CacheKey, FetchReport, Fetched, PinSet},
    dv::DeletionVector,
    engine::{CoreRef, Engine, EngineCore},
    handle::CollectionHandle,
    memtable::{IndexFlavor, MemScalarIndex, MemtableData, index_keys},
    segment::{SegmentHandle, segment_error},
    segment_v2::{
        DecodedScalarIndex, DynamicBlock, DynamicHandle, PkColumn, PkFilter, PkSorted,
        ScalarColumn, SectionKind, SegmentGraph, SegmentUnit, VectorHandle,
    },
    state::ReadAt,
    tokens::SnapshotToken,
    version::{Version, VersionCounters},
};
pub use logpose_index::scalar::{Direction, ScalarKey};
use logpose_index::{
    scalar::{OrderedScalarIndex, ScalarIndex},
    sq8::Sq8Section,
};
use logpose_types::{
    CollectionRef, DistanceMetric, LogPoseError, Result, RowAddr, RowId, SeqNo, Snapshot, UnitId,
    filter::FilterExpr,
    record::{PrimaryKey, Record},
    schema::{CollectionSchema, FieldId},
    value::Value,
};
use roaring::RoaringBitmap;
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    future::Future,
    ops::Bound,
    pin::Pin,
    sync::{Arc, Weak},
};

/// A boxed, sendable future, for the object-safe reader traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// How a read resolves the state it runs against.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {
    /// Read the `Version` this token pins (and extend the token's expiry) instead of the
    /// current one.
    pub token: Option<SnapshotToken>,
    /// Read exactly this snapshot: the current version, one of the latest versions of the
    /// current manifest generation, or a token-pinned one. Anything else is
    /// [`LogPoseError::SnapshotExpired`]; repeatable reads pin a token instead.
    pub snapshot: Option<Snapshot>,
    /// Read barrier: fail with [`LogPoseError::ReadBarrierNotSatisfied`] unless the current
    /// state is at or past it.
    pub read_barrier: Option<Snapshot>,
    /// Pin the version the view reads under a new token (or keep using `token`), which the view
    /// carries; see [`ReadView::token`].
    pub pin: bool,
}

/// Opens [`ReadView`]s. Implemented by [`Engine`] and the storage-engine adapters over it.
pub trait CollectionReader: Send + Sync {
    /// A view of `collection` as `options` select it. Resolving the state reads no file.
    fn read_view<'a>(
        &'a self,
        collection: &'a CollectionRef,
        options: ReadOptions,
    ) -> BoxFuture<'a, Result<ReadView>>;
}

/// Resolves a filter to live rows per unit. Implemented by `logpose-query` and injected
/// through [`EngineConfig::resolver`](crate::EngineConfig::resolver); the writer calls it for
/// delete-by-filter and update-by-filter against a view of its own latest state.
pub trait RowSetResolver: Send + Sync {
    /// The live rows of `view` matching `filter`, per unit (units with no match may be omitted).
    fn resolve<'a>(
        &'a self,
        view: &'a ReadView,
        filter: &'a FilterExpr,
    ) -> BoxFuture<'a, Result<Vec<(UnitId, RoaringBitmap)>>>;
}

/// One `Version` plus the context to load its sections. Cheap to clone.
///
/// Holding a view keeps its version (and so its segment files) alive, but not the engine: once
/// the engine shuts down, fetches fail with `Unavailable`.
#[derive(Clone)]
pub struct ReadView {
    version: Arc<Version>,
    core: Weak<EngineCore>,
    token: Option<SnapshotToken>,
}

impl fmt::Debug for ReadView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadView")
            .field("version", &self.version.id)
            .field("visible_seq_no", &self.version.visible_seq_no)
            .field("token", &self.token.is_some())
            .finish()
    }
}

/// What part of a row [`ReadView::rows`] reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Projection {
    /// Include vector fields (loads their pages for segment rows).
    pub vectors: bool,
    /// Include each row's sequence number (reads segment row metadata around the cache).
    pub seq_no: bool,
}

impl Projection {
    /// Scalar fields and `$extra` only.
    #[must_use]
    pub fn scalars() -> Self {
        Self::default()
    }

    /// Every field, and the sequence number.
    #[must_use]
    pub fn full() -> Self {
        Self {
            vectors: true,
            seq_no: true,
        }
    }
}

/// One row read by [`ReadView::rows`] or [`ReadView::get`], with names from the view's
/// schema: dropped fields are gone and shadowed `$extra` keys hidden.
#[derive(Clone, Debug, PartialEq)]
pub struct RowData {
    /// Where the row lives in the view.
    pub addr: RowAddr,
    /// Sequence number of the write that produced the row; 0 unless projected.
    pub seq_no: SeqNo,
    /// The row.
    pub record: Record,
}

/// Sections one unit needs for a stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SectionNeed {
    /// The key column, key order, and key filter.
    Pk,
    /// The inverted and sorted indexes of a scalar field, whichever exist.
    ScalarIndex(FieldId),
    /// The column of a scalar field.
    Column(FieldId),
    /// The `$extra` blocks holding these rows.
    DynamicBlocks(RoaringBitmap),
    /// The graph and SQ8 codes of a vector field, whichever exist.
    VectorIndex(FieldId),
    /// The f32 pages of a vector field holding these rows.
    VectorRows(FieldId, RoaringBitmap),
}

/// The sections a fetch stage loads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FetchPlan {
    /// Needs per unit. Needs of memtable units are ignored.
    pub needs: Vec<(UnitId, SectionNeed)>,
}

impl FetchPlan {
    /// Add a need.
    pub fn push(&mut self, unit: UnitId, need: SectionNeed) {
        self.needs.push((unit, need));
    }

    /// Whether the plan loads nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.needs.is_empty()
    }
}

/// Whether a unit's sections are in memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Residency {
    /// Every unit is resident (memtables always are).
    Resident,
    /// Loading would read about this many bytes.
    Cold {
        /// Bytes to read.
        bytes: u64,
    },
}

impl ReadView {
    pub(crate) fn new(
        version: Arc<Version>,
        core: Weak<EngineCore>,
        token: Option<SnapshotToken>,
    ) -> Self {
        Self {
            version,
            core,
            token,
        }
    }

    fn core(&self) -> Result<CoreRef> {
        self.core
            .upgrade()
            .map(|core| CoreRef::new(&core))
            .ok_or_else(|| LogPoseError::unavailable("the storage engine is shut down"))
    }

    /// The schema as of the view: filters and projections resolve names with it only.
    #[must_use]
    pub fn schema(&self) -> &Arc<CollectionSchema> {
        &self.version.schema
    }

    /// The collection's reference.
    #[must_use]
    pub fn collection(&self) -> &CollectionRef {
        &self.version.meta.reference
    }

    /// The metric of the first vector field of the view's schema: the vector the legacy read
    /// paths search.
    #[must_use]
    pub fn metric(&self) -> DistanceMetric {
        self.version
            .schema
            .vectors()
            .first()
            .map_or(DistanceMetric::Cosine, |field| field.metric)
    }

    /// Last sequence number the view includes.
    #[must_use]
    pub fn visible_seq_no(&self) -> SeqNo {
        self.version.visible_seq_no
    }

    /// The snapshot naming exactly this view's state.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.version.snapshot()
    }

    /// The token pinning this view's version, when the view was opened with `pin` or with a
    /// token.
    #[must_use]
    pub fn token(&self) -> Option<&SnapshotToken> {
        self.token.as_ref()
    }

    /// This view pinned under a new snapshot token, or the view itself when it already
    /// carries one, so later requests read exactly its state. A request that decides only after
    /// reading whether a later one needs its state (a scroll page with rows after it) pins
    /// then, so a request that needs no token holds none.
    ///
    /// # Errors
    ///
    /// `Unavailable` once the engine shut down, `NotFound` when the collection was dropped, or
    /// `TooManySnapshots`.
    pub fn pinned(&self) -> Result<ReadView> {
        if self.token.is_some() {
            return Ok(self.clone());
        }
        let core = self
            .core
            .upgrade()
            .ok_or_else(|| LogPoseError::unavailable("the storage engine is shut down"))?;
        let reference = &self.version.meta.reference;
        let handle = core.collection(reference)?;
        if handle.meta().id != self.version.meta.id {
            return Err(crate::engine::not_found(reference));
        }
        let token = handle.pin_version(Arc::clone(&self.version))?;
        Ok(Self {
            token: Some(token),
            ..self.clone()
        })
    }

    /// Unpin the snapshot token this view was opened with or pinned under, for a request that
    /// knows no later request needs it (the last page of a scroll that pinned its own). The
    /// view itself still reads its state; later requests with the token fail like an expired
    /// one. Returns whether a pin was released: `false` without a token, when it was already
    /// released or expired, or once the engine shut down or the collection was dropped.
    pub fn release(&self) -> bool {
        let Some(token) = &self.token else {
            return false;
        };
        let Some(core) = self.core.upgrade() else {
            return false;
        };
        match core.collection(&self.version.meta.reference) {
            Ok(handle) if handle.meta().id == self.version.meta.id => {
                handle.release_snapshot(token)
            }
            _ => false,
        }
    }

    /// Row counters: live rows are `total_rows - deleted_rows`.
    #[must_use]
    pub fn counters(&self) -> VersionCounters {
        self.version.counters
    }

    /// Segments ascending by unit, then frozen memtables oldest first, then the active
    /// memtable. Correctness never depends on the order (I5).
    #[must_use]
    pub fn units(&self) -> Vec<UnitView<'_>> {
        let version = &*self.version;
        version
            .segments
            .iter()
            .map(|segment| UnitView {
                kind: UnitKind::Segment(segment),
                deleted: version.deletes.get(segment.unit),
            })
            .chain(version.memtables().map(|memtable| UnitView {
                kind: UnitKind::Memtable(memtable),
                deleted: version.deletes.get(memtable.unit),
            }))
            .collect()
    }

    /// The unit `id`, if the view has it.
    #[must_use]
    pub fn unit(&self, id: UnitId) -> Option<UnitView<'_>> {
        self.units().into_iter().find(|unit| unit.id() == id)
    }

    /// Run CPU-bound `f` on the query pool and await it. `f` must not do I/O; `rayon` parallel
    /// iterators inside it run on the query pool.
    ///
    /// # Errors
    ///
    /// `Unavailable` once the engine shut down, or `Internal` if `f` panicked.
    pub async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&ReadView) -> T + Send + 'static,
    ) -> Result<T> {
        let core = self.core()?;
        let view = self.clone();
        crate::run_cpu(&core.runtime().query, move || f(&view)).await
    }

    /// Load and pin what `plan` needs. Memtable needs are ignored.
    ///
    /// # Errors
    ///
    /// I/O errors, typed corruption (`Corrupt { Segment }`, or `Corrupt { Index }` for an
    /// index section), or `Unavailable` once the engine shut down.
    pub async fn fetch(&self, plan: &FetchPlan) -> Result<(PinSet, FetchReport)> {
        let mut pins = PinSet::new();
        let mut report = FetchReport::default();
        if plan.is_empty() {
            return Ok((pins, report));
        }
        let core = self.core()?;
        let segments = self.segments_by_unit();
        // Stage one: whole sections, vector prefixes, and dynamic block indexes.
        let mut first = Vec::new();
        for (unit, need) in &plan.needs {
            let Some(segment) = segments.get(unit) else {
                continue;
            };
            let reader = segment.reader();
            let mut section = |kind: SectionKind, field: Option<FieldId>, decoded: bool| {
                if let Some(unit) = reader
                    .find_section(kind, field)
                    .and_then(|index| reader.section_unit(index))
                {
                    first.push((Arc::clone(segment), unit, decoded));
                }
            };
            match need {
                SectionNeed::Pk => {
                    section(SectionKind::PkColumn, None, true);
                    section(SectionKind::PkSorted, None, true);
                    section(SectionKind::PkFilter, None, true);
                }
                SectionNeed::ScalarIndex(field) => {
                    section(SectionKind::ScalarInverted, Some(*field), true);
                    section(SectionKind::ScalarSorted, Some(*field), true);
                }
                SectionNeed::Column(field) => {
                    section(SectionKind::ScalarColumn, Some(*field), false)
                }
                SectionNeed::VectorIndex(field) => {
                    section(SectionKind::VectorSq8, Some(*field), true);
                    section(SectionKind::VectorGraph, Some(*field), true);
                }
                SectionNeed::DynamicBlocks(_) => {
                    if let Some(unit) = reader.dynamic_index_unit() {
                        first.push((Arc::clone(segment), unit, false));
                    }
                }
                SectionNeed::VectorRows(field, _) => {
                    if let Some(unit) = reader.vector_prefix_unit(*field) {
                        first.push((Arc::clone(segment), unit, false));
                    }
                }
            }
        }
        load_units(&core, first, &mut pins, &mut report).await?;

        // Stage two: vector pages and dynamic blocks, located through the stage-one units.
        let mut second = Vec::new();
        for (unit, need) in &plan.needs {
            let Some(segment) = segments.get(unit) else {
                continue;
            };
            let view = UnitView {
                kind: UnitKind::Segment(segment),
                deleted: None,
            };
            match need {
                SectionNeed::VectorRows(field, rows) => {
                    let Some(handle) = view.vector_handle(*field, &pins)? else {
                        continue;
                    };
                    let page_rows = handle.prefix().page_rows().max(1);
                    let mut last = None;
                    for row in rows {
                        let page = row / page_rows;
                        if last == Some(page) {
                            continue;
                        }
                        last = Some(page);
                        if let Some(unit) = handle.page_unit(page) {
                            second.push((Arc::clone(segment), unit, false));
                        }
                    }
                }
                SectionNeed::DynamicBlocks(rows) => {
                    let Some(handle) = view.dynamic_handle(&pins)? else {
                        continue;
                    };
                    let mut last = None;
                    for row in rows {
                        let block = row / crate::segment_v2::DYNAMIC_BLOCK_ROWS;
                        if last == Some(block) {
                            continue;
                        }
                        last = Some(block);
                        if let Some(unit) = handle.block_unit(block) {
                            second.push((Arc::clone(segment), unit, false));
                        }
                    }
                }
                _ => {}
            }
        }
        load_units(&core, second, &mut pins, &mut report).await?;
        Ok((pins, report))
    }

    fn segments_by_unit(&self) -> HashMap<UnitId, &Arc<SegmentHandle>> {
        self.version
            .segments
            .iter()
            .map(|segment| (segment.unit, segment))
            .collect()
    }

    /// Read the rows at `addrs` (each live or not) as `projection` asks, in the order given.
    /// Memtable rows are read directly; segment rows on the I/O pool, through the cache.
    ///
    /// # Errors
    ///
    /// `Internal` for an address outside the view, I/O errors, or typed corruption.
    pub async fn rows(&self, addrs: &[RowAddr], projection: Projection) -> Result<Vec<RowData>> {
        let mut by_segment: BTreeMap<UnitId, Vec<u32>> = BTreeMap::new();
        let mut out: Vec<Option<RowData>> = vec![None; addrs.len()];
        let schema = Arc::clone(self.schema());
        let memtables = self
            .version
            .memtables()
            .map(|memtable| (memtable.unit, memtable))
            .collect::<HashMap<_, _>>();
        for (slot, addr) in addrs.iter().enumerate() {
            if let Some(memtable) = memtables.get(&addr.unit) {
                let mut image = memtable
                    .row_image(addr.row)
                    .map_err(LogPoseError::internal)?;
                if !projection.vectors {
                    image.vectors.clear();
                }
                out[slot] = Some(RowData {
                    addr: *addr,
                    seq_no: memtable.seq_no(addr.row).unwrap_or_default(),
                    record: to_record(&schema, &image)?,
                });
            } else {
                by_segment.entry(addr.unit).or_default().push(addr.row);
            }
        }
        if !by_segment.is_empty() {
            let segments = self.segments_by_unit();
            let mut reads = Vec::new();
            for (unit, mut rows) in by_segment {
                let segment = segments.get(&unit).ok_or_else(|| {
                    LogPoseError::internal(format!("unit {unit} is not in the read view"))
                })?;
                rows.sort_unstable();
                rows.dedup();
                reads.push((Arc::clone(segment), rows));
            }
            let core = self.core()?;
            let read = core
                .runtime()
                .io
                .run(move || -> Result<Vec<(UnitId, u32, SeqNo, _)>> {
                    let mut rows_read = Vec::new();
                    for (segment, rows) in reads {
                        let images = segment
                            .reader()
                            .row_images_projected(&rows, projection.vectors)
                            .map_err(|error| segment_error(segment.path(), error))?;
                        let seqs = if projection.seq_no {
                            Some(
                                segment
                                    .reader()
                                    .row_meta_shared()
                                    .map_err(|error| segment_error(segment.path(), error))?,
                            )
                        } else {
                            None
                        };
                        for (row, image) in rows.into_iter().zip(images) {
                            let seq_no = seqs
                                .as_ref()
                                .and_then(|seqs| seqs.get(row as usize).copied())
                                .unwrap_or_default();
                            rows_read.push((segment.unit, row, seq_no, image));
                        }
                    }
                    Ok(rows_read)
                })
                .await??;
            let mut found = HashMap::new();
            for (unit, row, seq_no, image) in read {
                found.insert(RowAddr { unit, row }, (seq_no, image));
            }
            for (slot, addr) in addrs.iter().enumerate() {
                if out[slot].is_some() {
                    continue;
                }
                let (seq_no, image) = found
                    .get(addr)
                    .ok_or_else(|| LogPoseError::internal(format!("row {addr} was not read")))?;
                out[slot] = Some(RowData {
                    addr: *addr,
                    seq_no: *seq_no,
                    record: to_record(&schema, image)?,
                });
            }
        }
        out.into_iter()
            .map(|row| row.ok_or_else(|| LogPoseError::internal("a row was not read")))
            .collect()
    }

    /// The live row of each key, or `None`: each key is looked up newest unit first
    /// (memtables by their key map, segments by key filter then key order), and the first
    /// live hit wins. By I5 at most one unit holds a live row of a key.
    ///
    /// # Errors
    ///
    /// As [`fetch`](Self::fetch) and [`rows`](Self::rows).
    pub async fn get(
        &self,
        pks: &[PrimaryKey],
        projection: Projection,
    ) -> Result<Vec<Option<RowData>>> {
        let addrs = self.locate(pks).await?;
        let found = addrs.iter().flatten().copied().collect::<Vec<_>>();
        let mut rows = self.rows(&found, projection).await?.into_iter();
        Ok(addrs
            .into_iter()
            .map(|addr| addr.and_then(|_| rows.next()))
            .collect())
    }

    /// The live row address of each key, or `None`.
    ///
    /// # Errors
    ///
    /// As [`fetch`](Self::fetch).
    pub async fn locate(&self, pks: &[PrimaryKey]) -> Result<Vec<Option<RowAddr>>> {
        let mut out = vec![None; pks.len()];
        let mut pending = Vec::new();
        let units = self.units();
        for (index, pk) in pks.iter().enumerate() {
            let mut found = None;
            for unit in units.iter().rev().filter(|unit| unit.is_memtable()) {
                if let UnitKind::Memtable(memtable) = unit.kind
                    && let Some(slot) = memtable.find(pk)
                    && !unit.is_deleted(slot)
                {
                    found = Some(RowAddr {
                        unit: unit.id(),
                        row: slot,
                    });
                    break;
                }
            }
            match found {
                Some(addr) => out[index] = Some(addr),
                None => pending.push(index),
            }
        }
        if pending.is_empty() || self.version.segments.is_empty() {
            return Ok(out);
        }
        let mut plan = FetchPlan::default();
        for segment in self.version.segments.iter() {
            plan.push(segment.unit, SectionNeed::Pk);
        }
        let (pins, _) = self.fetch(&plan).await?;
        for index in pending {
            for unit in units.iter().rev().filter(|unit| !unit.is_memtable()) {
                let keys = unit.pks(&pins)?;
                if let Some(row) = keys.find(&pks[index])
                    && !unit.is_deleted(row)
                {
                    out[index] = Some(RowAddr {
                        unit: unit.id(),
                        row,
                    });
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Bytes a fetch of `need` for `unit` would read, or `Resident`.
    #[must_use]
    pub fn residency(&self, unit: UnitId, need: &SectionNeed) -> Residency {
        let Some(segment) = self
            .version
            .segments
            .iter()
            .find(|segment| segment.unit == unit)
        else {
            return Residency::Resident;
        };
        let reader = segment.reader();
        let kinds: Vec<(SectionKind, Option<FieldId>)> = match need {
            SectionNeed::Pk => vec![
                (SectionKind::PkColumn, None),
                (SectionKind::PkSorted, None),
                (SectionKind::PkFilter, None),
            ],
            SectionNeed::ScalarIndex(field) => vec![
                (SectionKind::ScalarInverted, Some(*field)),
                (SectionKind::ScalarSorted, Some(*field)),
            ],
            SectionNeed::Column(field) => vec![(SectionKind::ScalarColumn, Some(*field))],
            SectionNeed::VectorIndex(field) => vec![
                (SectionKind::VectorSq8, Some(*field)),
                (SectionKind::VectorGraph, Some(*field)),
            ],
            SectionNeed::DynamicBlocks(_) => vec![(SectionKind::DynamicJson, None)],
            SectionNeed::VectorRows(field, _) => vec![(SectionKind::VectorF32, Some(*field))],
        };
        let mut cold = 0;
        for (kind, field) in kinds {
            if let Some(unit) = reader
                .find_section(kind, field)
                .and_then(|index| reader.section_unit(index))
                && !reader.residency(&unit)
            {
                cold += unit.len_hint().unwrap_or(0);
            }
        }
        if cold == 0 {
            Residency::Resident
        } else {
            Residency::Cold { bytes: cold }
        }
    }
}

/// Load `units` concurrently on the I/O pool (the cache dispatches every miss before the first
/// await) and pin them.
async fn load_units(
    core: &CoreRef,
    units: Vec<(Arc<SegmentHandle>, SegmentUnit, bool)>,
    pins: &mut PinSet,
    report: &mut FetchReport,
) -> Result<()> {
    let mut pending = Vec::with_capacity(units.len());
    for (segment, unit, decoded) in units {
        let Some(key) = segment.reader().unit_key(&unit) else {
            return Err(LogPoseError::internal(
                "a segment of a read view has no cache registration",
            ));
        };
        if pins.contains(&key) {
            continue;
        }
        let io = &core.runtime().io;
        let fetch = if decoded {
            segment.reader().fetch_decoded(&unit, io)
        } else {
            segment.reader().fetch(&unit, io)
        };
        pending.push((segment, unit.class(), key, fetch));
    }
    for (segment, class, key, fetch) in pending {
        let (bytes, fetched): (Arc<AlignedBytes>, Fetched) = fetch
            .await
            .map_err(|error| segment_error(segment.path(), error))?;
        report.record(class, fetched);
        pins.insert(key, bytes);
    }
    Ok(())
}

fn to_record(schema: &CollectionSchema, image: &logpose_wal::codec::RowImage) -> Result<Record> {
    image.to_record(schema).map_err(|error| {
        LogPoseError::internal(format!(
            "a row cannot be read with schema version {}: {error}",
            schema.schema_version()
        ))
    })
}

fn not_fetched(unit: UnitId, what: &str) -> LogPoseError {
    LogPoseError::internal(format!(
        "unit {unit}: {what} was not fetched before the compute stage"
    ))
}

#[derive(Clone, Copy)]
enum UnitKind<'v> {
    Memtable(&'v Arc<MemtableData>),
    Segment(&'v Arc<SegmentHandle>),
}

/// A segment's zone map of one scalar field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ZoneMap<'v> {
    /// Smallest value, in the binary value codec.
    pub min: Option<&'v [u8]>,
    /// Largest value, in the binary value codec.
    pub max: Option<&'v [u8]>,
    /// Rows without a value.
    pub null_count: u32,
}

/// A memtable or a segment, with its deletion vector in the view.
#[derive(Clone, Copy)]
pub struct UnitView<'v> {
    kind: UnitKind<'v>,
    deleted: Option<&'v DeletionVector>,
}

impl fmt::Debug for UnitView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnitView")
            .field("id", &self.id())
            .field("memtable", &self.is_memtable())
            .field("rows", &self.row_count())
            .field("live", &self.live_count())
            .finish()
    }
}

impl<'v> UnitView<'v> {
    /// The unit's id.
    #[must_use]
    pub fn id(&self) -> UnitId {
        match self.kind {
            UnitKind::Memtable(memtable) => memtable.unit,
            UnitKind::Segment(segment) => segment.unit,
        }
    }

    /// Whether this is a memtable (always resident, no index sections).
    #[must_use]
    pub fn is_memtable(&self) -> bool {
        matches!(self.kind, UnitKind::Memtable(_))
    }

    /// Rows (slots) the unit holds, live or not.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        match self.kind {
            UnitKind::Memtable(memtable) => memtable.slot_count(),
            UnitKind::Segment(segment) => segment.row_count(),
        }
    }

    /// Rows not deleted in the view.
    #[must_use]
    pub fn live_count(&self) -> u32 {
        let deleted = self.deleted.map_or(0, DeletionVector::len);
        u32::try_from(u64::from(self.row_count()).saturating_sub(deleted)).unwrap_or(u32::MAX)
    }

    /// Whether `row` is deleted in the view.
    #[must_use]
    pub fn is_deleted(&self, row: RowId) -> bool {
        self.deleted.is_some_and(|dv| dv.contains(row))
    }

    /// `0..row_count AND NOT deleted`.
    #[must_use]
    pub fn live(&self) -> RoaringBitmap {
        let mut live = RoaringBitmap::new();
        live.insert_range(0..self.row_count());
        self.subtract_deleted(&mut live);
        live
    }

    /// `rows := rows AND NOT deleted`.
    pub fn subtract_deleted(&self, rows: &mut RoaringBitmap) {
        if let Some(dv) = self.deleted {
            dv.0.subtract_from(rows);
        }
    }

    /// Whether the unit has an inverted (`flavor` false) or sorted (`true`) index for
    /// `field`, without loading it.
    #[must_use]
    pub fn has_scalar_index(&self, field: FieldId, sorted: bool) -> bool {
        let flavor = if sorted {
            IndexFlavor::Sorted
        } else {
            IndexFlavor::Inverted
        };
        match self.kind {
            UnitKind::Memtable(memtable) => memtable.index(field, flavor).is_some(),
            UnitKind::Segment(segment) => {
                let kind = if sorted {
                    SectionKind::ScalarSorted
                } else {
                    SectionKind::ScalarInverted
                };
                segment.reader().find_section(kind, Some(field)).is_some()
            }
        }
    }

    /// Whether the unit has a graph (`graph` true) or SQ8 codes for vector `field`.
    #[must_use]
    pub fn has_vector_index(&self, field: FieldId, graph: bool) -> bool {
        match self.kind {
            UnitKind::Memtable(_) => false,
            UnitKind::Segment(segment) => {
                let kind = if graph {
                    SectionKind::VectorGraph
                } else {
                    SectionKind::VectorSq8
                };
                segment.reader().find_section(kind, Some(field)).is_some()
            }
        }
    }

    /// Whether the unit stores any `$extra` data.
    #[must_use]
    pub fn has_dynamic(&self) -> bool {
        match self.kind {
            UnitKind::Memtable(_) => true,
            UnitKind::Segment(segment) => segment.reader().dynamic_index_unit().is_some(),
        }
    }

    /// Zone map of `field` from the segment's manifest entry. `None` for memtables and fields
    /// without a zone.
    #[must_use]
    pub fn zone(&self, field: FieldId) -> Option<ZoneMap<'v>> {
        let UnitKind::Segment(segment) = self.kind else {
            return None;
        };
        segment
            .entry
            .zones
            .iter()
            .find(|zone| zone.field_id == field.0)
            .map(|zone| ZoneMap {
                min: zone.min.as_deref(),
                max: zone.max.as_deref(),
                null_count: zone.null_count,
            })
    }

    fn pinned(
        &self,
        kind: SectionKind,
        field: Option<FieldId>,
        pins: &PinSet,
    ) -> Result<Option<Arc<AlignedBytes>>> {
        let UnitKind::Segment(segment) = self.kind else {
            return Ok(None);
        };
        let reader = segment.reader();
        let Some(unit) = reader
            .find_section(kind, field)
            .and_then(|index| reader.section_unit(index))
        else {
            return Ok(None);
        };
        let key = reader
            .unit_key(&unit)
            .ok_or_else(|| not_fetched(segment.unit, "a section"))?;
        pins.get(&key)
            .cloned()
            .map(Some)
            .ok_or_else(|| not_fetched(segment.unit, &format!("{kind:?} section")))
    }

    fn pinned_key(
        &self,
        key: Option<CacheKey>,
        pins: &PinSet,
        what: &str,
    ) -> Result<Arc<AlignedBytes>> {
        key.and_then(|key| pins.get(&key).cloned())
            .ok_or_else(|| not_fetched(self.id(), what))
    }

    /// The unit's keys.
    ///
    /// # Errors
    ///
    /// `Internal` if a segment's key sections were not fetched ([`SectionNeed::Pk`]), or typed
    /// corruption.
    pub fn pks(&self, pins: &PinSet) -> Result<PkRef<'v>> {
        let segment = match self.kind {
            UnitKind::Memtable(memtable) => return Ok(PkRef(PkInner::Memtable(memtable))),
            UnitKind::Segment(segment) => segment,
        };
        let path = segment.path();
        let rows = segment.row_count() as usize;
        let decode_error = |error: crate::segment_v2::SegmentError| segment_error(path, error);
        let column_bytes = self
            .pinned(SectionKind::PkColumn, None, pins)?
            .ok_or_else(|| not_fetched(segment.unit, "the key column"))?;
        let sorted_bytes = self
            .pinned(SectionKind::PkSorted, None, pins)?
            .ok_or_else(|| not_fetched(segment.unit, "the key order"))?;
        let filter_bytes = self
            .pinned(SectionKind::PkFilter, None, pins)?
            .ok_or_else(|| not_fetched(segment.unit, "the key filter"))?;
        let reader = segment.reader();
        let entry = |kind: SectionKind| {
            reader
                .find_section(kind, None)
                .and_then(|index| reader.sections().get(index).copied())
                .ok_or_else(|| not_fetched(segment.unit, "a key section"))
        };
        let column_entry = entry(SectionKind::PkColumn)?;
        let sorted_entry = entry(SectionKind::PkSorted)?;
        let filter_entry = entry(SectionKind::PkFilter)?;
        let region = |entry: &crate::segment_v2::SectionEntry, index: SectionKind| {
            crate::segment_v2::Region::Section {
                index: reader.find_section(index, None).unwrap_or(0),
                kind: entry.kind,
            }
        };
        let column = column_bytes
            .decoded(|bytes| {
                PkColumn::decode(bytes, column_entry.encoding, rows)
                    .map(|column| (column, bytes.len() as u64))
                    .map_err(|error| error.at(region(&column_entry, SectionKind::PkColumn)))
            })
            .map_err(decode_error)?;
        let sorted = sorted_bytes
            .decoded(|bytes| {
                PkSorted::decode(bytes, sorted_entry.encoding, rows)
                    .map(|sorted| (sorted, bytes.len() as u64))
                    .map_err(|error| error.at(region(&sorted_entry, SectionKind::PkSorted)))
            })
            .map_err(decode_error)?;
        let filter = filter_bytes
            .decoded(|bytes| {
                PkFilter::decode(bytes, filter_entry.encoding)
                    .map(|filter| (filter, bytes.len() as u64))
                    .map_err(|error| error.at(region(&filter_entry, SectionKind::PkFilter)))
            })
            .map_err(decode_error)?;
        Ok(PkRef(PkInner::Segment {
            column,
            sorted,
            filter,
            key_type: reader.schema().primary_key_type(),
        }))
    }

    /// The scalar index of `field` of the given flavor (`sorted` or inverted), if the unit
    /// has one.
    ///
    /// # Errors
    ///
    /// `Internal` if a segment's index was not fetched ([`SectionNeed::ScalarIndex`]), or
    /// `Corrupt { Index }`.
    pub fn scalar_index(
        &self,
        field: FieldId,
        sorted: bool,
        pins: &PinSet,
    ) -> Result<Option<ScalarIndexRef<'v>>> {
        let segment = match self.kind {
            UnitKind::Memtable(memtable) => {
                let flavor = if sorted {
                    IndexFlavor::Sorted
                } else {
                    IndexFlavor::Inverted
                };
                return Ok(memtable
                    .index(field, flavor)
                    .map(|index| ScalarIndexRef(ScalarInner::Memtable(index))));
            }
            UnitKind::Segment(segment) => segment,
        };
        let kind = if sorted {
            SectionKind::ScalarSorted
        } else {
            SectionKind::ScalarInverted
        };
        let Some(bytes) = self.pinned(kind, Some(field), pins)? else {
            return Ok(None);
        };
        let reader = segment.reader();
        let index = reader.find_section(kind, Some(field)).unwrap_or(0);
        let region = crate::segment_v2::Region::Section {
            index,
            kind: kind.code(),
        };
        let decoded = bytes
            .decoded(|raw| {
                let decoded = if sorted {
                    logpose_index::scalar::SortedIndex::from_bytes(raw)
                        .map(DecodedScalarIndex::Sorted)
                } else {
                    logpose_index::scalar::InvertedIndex::from_bytes(raw)
                        .map(DecodedScalarIndex::Inverted)
                };
                decoded
                    .map(|decoded| (decoded, raw.len() as u64))
                    .map_err(|error| crate::segment_v2::SegmentError::Corrupt {
                        region,
                        detail: error.to_string(),
                    })
            })
            .map_err(|error| segment_error(segment.path(), error))?;
        Ok(Some(ScalarIndexRef(ScalarInner::Segment(decoded))))
    }

    /// The column of scalar `field`. A unit without one (the field was added later) reads
    /// null everywhere.
    ///
    /// # Errors
    ///
    /// `Internal` if a segment's column was not fetched ([`SectionNeed::Column`]), or typed
    /// corruption.
    pub fn column(&self, field: FieldId, pins: &PinSet) -> Result<ColumnRef<'v>> {
        let segment = match self.kind {
            UnitKind::Memtable(memtable) => {
                return Ok(ColumnRef(ColumnInner::Memtable(memtable, field)));
            }
            UnitKind::Segment(segment) => segment,
        };
        let Some(bytes) = self.pinned(SectionKind::ScalarColumn, Some(field), pins)? else {
            return Ok(ColumnRef(ColumnInner::Missing));
        };
        let reader = segment.reader();
        let column = bytes
            .decoded(|raw| {
                reader
                    .decode_scalar_column(field, raw)
                    .map(|column| (column, raw.len() as u64 * 2))
            })
            .map_err(|error| segment_error(segment.path(), error))?;
        Ok(ColumnRef(ColumnInner::Segment(column)))
    }

    fn dynamic_handle(&self, pins: &PinSet) -> Result<Option<Arc<DynamicHandle>>> {
        let UnitKind::Segment(segment) = self.kind else {
            return Ok(None);
        };
        let reader = segment.reader();
        let Some(unit) = reader.dynamic_index_unit() else {
            return Ok(None);
        };
        let bytes = self.pinned_key(reader.unit_key(&unit), pins, "the dynamic block index")?;
        bytes
            .decoded(|raw| {
                reader
                    .dynamic_handle(&unit, raw)
                    .map(|handle| (handle, raw.len() as u64))
            })
            .map(Some)
            .map_err(|error| segment_error(segment.path(), error))
    }

    /// `$extra` of the unit's rows. Segment rows read only blocks fetched with
    /// [`SectionNeed::DynamicBlocks`].
    ///
    /// # Errors
    ///
    /// `Internal` if the block index was not fetched, or typed corruption.
    pub fn dynamic(&self, pins: &PinSet) -> Result<DynamicRef<'v>> {
        match self.kind {
            UnitKind::Memtable(memtable) => Ok(DynamicRef(DynamicInner::Memtable(memtable))),
            UnitKind::Segment(segment) => match self.dynamic_handle(pins)? {
                Some(handle) => {
                    let mut blocks = HashMap::new();
                    for block in 0..handle.blocks().block_count() {
                        let Some(unit) = handle.block_unit(block) else {
                            continue;
                        };
                        let Some(key) = segment.reader().unit_key(&unit) else {
                            continue;
                        };
                        let Some(bytes) = pins.get(&key) else {
                            continue;
                        };
                        let Some(rows) = handle.blocks().block_rows(block) else {
                            continue;
                        };
                        let region = crate::segment_v2::Region::DynamicBlock {
                            index: handle.section_index(),
                            block,
                        };
                        let decoded = bytes
                            .decoded(|raw| {
                                DynamicBlock::decode(raw, rows)
                                    .map(|block| (block, raw.len() as u64))
                                    .map_err(|error| error.at(region))
                            })
                            .map_err(|error| segment_error(segment.path(), error))?;
                        blocks.insert(block, decoded);
                    }
                    Ok(DynamicRef(DynamicInner::Segment {
                        unit: segment.unit,
                        blocks,
                    }))
                }
                None => Ok(DynamicRef(DynamicInner::Empty)),
            },
        }
    }

    /// The graph and SQ8 codes of vector `field` (none for memtables).
    ///
    /// # Errors
    ///
    /// `Internal` if they were not fetched ([`SectionNeed::VectorIndex`]), or
    /// `Corrupt { Index }`.
    pub fn vector_index(&self, field: FieldId, pins: &PinSet) -> Result<VectorIndexRef> {
        let UnitKind::Segment(segment) = self.kind else {
            return Ok(VectorIndexRef::default());
        };
        let rows = segment.row_count();
        let reader = segment.reader();
        let region = |kind: SectionKind| crate::segment_v2::Region::Section {
            index: reader.find_section(kind, Some(field)).unwrap_or(0),
            kind: kind.code(),
        };
        let graph = match self.pinned(SectionKind::VectorGraph, Some(field), pins)? {
            Some(bytes) => Some(
                bytes
                    .decoded(|raw| {
                        SegmentGraph::decode(raw, rows)
                            .map(|graph| {
                                let heap = graph.heap_bytes();
                                (graph, heap)
                            })
                            .map_err(|detail| crate::segment_v2::SegmentError::Corrupt {
                                region: region(SectionKind::VectorGraph),
                                detail,
                            })
                    })
                    .map_err(|error| segment_error(segment.path(), error))?,
            ),
            None => None,
        };
        let sq8 = match self.pinned(SectionKind::VectorSq8, Some(field), pins)? {
            Some(bytes) => {
                let section = bytes
                    .decoded(|raw| {
                        Sq8Section::parse(raw)
                            .map_err(|error| error.to_string())
                            .and_then(|section| {
                                if section.rows() == rows as usize {
                                    Ok(section)
                                } else {
                                    Err("sq8 row count differs from the segment".to_owned())
                                }
                            })
                            .map(|section| (section, 0))
                            .map_err(|detail| crate::segment_v2::SegmentError::Corrupt {
                                region: region(SectionKind::VectorSq8),
                                detail,
                            })
                    })
                    .map_err(|error| segment_error(segment.path(), error))?;
                Some(Sq8Codes { section, bytes })
            }
            None => None,
        };
        Ok(VectorIndexRef { graph, sq8 })
    }

    fn vector_handle(&self, field: FieldId, pins: &PinSet) -> Result<Option<Arc<VectorHandle>>> {
        let UnitKind::Segment(segment) = self.kind else {
            return Ok(None);
        };
        let reader = segment.reader();
        let Some(unit) = reader.vector_prefix_unit(field) else {
            return Ok(None);
        };
        let bytes = self.pinned_key(reader.unit_key(&unit), pins, "a vector prefix")?;
        let handle = bytes
            .decoded(|_| {
                reader
                    .vector_handle(&unit, &bytes)
                    .map(|handle| (handle, 0))
            })
            .map_err(|error| segment_error(segment.path(), error))?;
        Ok(Some(handle))
    }

    /// The f32 vectors of `field`. Segment rows read only pages fetched with
    /// [`SectionNeed::VectorRows`].
    ///
    /// # Errors
    ///
    /// `Internal` if the prefix was not fetched, or typed corruption.
    pub fn vector_rows<'p>(&self, field: FieldId, pins: &'p PinSet) -> Result<VectorRowsRef<'p>>
    where
        'v: 'p,
    {
        let segment = match self.kind {
            UnitKind::Memtable(memtable) => {
                return Ok(VectorRowsRef(VectorRowsInner::Memtable(memtable, field)));
            }
            UnitKind::Segment(segment) => segment,
        };
        let Some(handle) = self.vector_handle(field, pins)? else {
            return Ok(VectorRowsRef(VectorRowsInner::Missing));
        };
        Ok(VectorRowsRef(VectorRowsInner::Segment {
            segment,
            handle,
            pins,
        }))
    }
}

/// A unit's keys.
pub struct PkRef<'v>(PkInner<'v>);

enum PkInner<'v> {
    /// A memtable's key map.
    Memtable(&'v MemtableData),
    /// A segment's key sections, decoded from pinned bytes.
    Segment {
        /// Keys in row order.
        column: Arc<PkColumn>,
        /// Rows in key order.
        sorted: Arc<PkSorted>,
        /// The key filter.
        filter: Arc<PkFilter>,
        /// The segment's key type.
        key_type: logpose_types::schema::PrimaryKeyType,
    },
}

impl PkRef<'_> {
    /// The key of `row`.
    #[must_use]
    pub fn pk_at(&self, row: RowId) -> Option<PrimaryKey> {
        match &self.0 {
            PkInner::Memtable(memtable) => memtable.pk(row).cloned(),
            PkInner::Segment { column, .. } => column.get(row as usize),
        }
    }

    /// The row of `pk` in this unit, live or not (a memtable's latest slot of the key).
    #[must_use]
    pub fn find(&self, pk: &PrimaryKey) -> Option<RowId> {
        match &self.0 {
            PkInner::Memtable(memtable) => memtable.find(pk),
            PkInner::Segment {
                column,
                sorted,
                filter,
                key_type,
            } => {
                if pk.key_type() != *key_type || !filter.may_contain(pk) {
                    return None;
                }
                sorted.find(column, pk)
            }
        }
    }

    /// `(key, row)` in ascending key order, strictly after `after`. A memtable yields each
    /// key's latest slot only; older slots of a key are always deleted.
    #[must_use]
    pub fn ascending_after(
        &self,
        after: Option<&PrimaryKey>,
    ) -> Box<dyn Iterator<Item = (PrimaryKey, RowId)> + '_> {
        match &self.0 {
            PkInner::Memtable(memtable) => {
                let after = after.cloned();
                Box::new(
                    memtable
                        .keys_after(after.as_ref())
                        .map(|(pk, slot)| (pk.clone(), slot)),
                )
            }
            PkInner::Segment { column, sorted, .. } => {
                let rows = sorted.rows();
                let start = match after {
                    None => 0,
                    Some(after) => rows.partition_point(|row| {
                        column.get(*row as usize).is_some_and(|pk| &pk <= after)
                    }),
                };
                Box::new(
                    rows[start..]
                        .iter()
                        .filter_map(move |row| column.get(*row as usize).map(|pk| (pk, *row))),
                )
            }
        }
    }
}

/// A scalar index of one unit, over its row ids.
#[derive(Clone)]
pub struct ScalarIndexRef<'v>(ScalarInner<'v>);

#[derive(Clone)]
enum ScalarInner<'v> {
    /// A memtable's live postings.
    Memtable(&'v MemScalarIndex),
    /// A segment's decoded index section.
    Segment(Arc<DecodedScalarIndex>),
}

impl ScalarIndexRef<'_> {
    /// Rows holding `key` (for arrays, as an element).
    #[must_use]
    pub fn equals(&self, key: &ScalarKey) -> RoaringBitmap {
        match &self.0 {
            ScalarInner::Memtable(index) => index.eq(key),
            ScalarInner::Segment(index) => match &**index {
                DecodedScalarIndex::Inverted(index) => index.equals(key),
                DecodedScalarIndex::Sorted(index) => index.equals(key),
            },
        }
    }

    /// Rows with a key in the interval; `None` for an inverted index.
    #[must_use]
    pub fn range(&self, low: Bound<&ScalarKey>, high: Bound<&ScalarKey>) -> Option<RoaringBitmap> {
        match &self.0 {
            ScalarInner::Memtable(index) => index.range(low, high),
            ScalarInner::Segment(index) => match &**index {
                DecodedScalarIndex::Inverted(_) => None,
                DecodedScalarIndex::Sorted(index) => Some(index.range(low, high)),
            },
        }
    }

    /// Rows with no value (null, missing, or written before the field existed).
    #[must_use]
    pub fn nulls(&self) -> RoaringBitmap {
        match &self.0 {
            ScalarInner::Memtable(index) => index.nulls(),
            ScalarInner::Segment(index) => match &**index {
                DecodedScalarIndex::Inverted(index) => index.is_null().clone(),
                DecodedScalarIndex::Sorted(index) => index.is_null().clone(),
            },
        }
    }

    /// Rows with at least one value.
    #[must_use]
    pub fn exists(&self) -> RoaringBitmap {
        match &self.0 {
            ScalarInner::Memtable(index) => index.values(),
            ScalarInner::Segment(index) => match &**index {
                DecodedScalarIndex::Inverted(index) => index.exists().clone(),
                DecodedScalarIndex::Sorted(index) => index.exists().clone(),
            },
        }
    }

    /// `(key, row)` in key order from `start` in `direction` (rows of one key ascending for
    /// `Ascending`, descending for `Descending`); `None` for an inverted index.
    #[must_use]
    pub fn ordered(
        &self,
        start: Bound<&ScalarKey>,
        direction: Direction,
    ) -> Option<Box<dyn Iterator<Item = (ScalarKey, RowId)> + '_>> {
        match &self.0 {
            ScalarInner::Memtable(index) => index.ordered(start, direction),
            ScalarInner::Segment(index) => match &**index {
                DecodedScalarIndex::Inverted(_) => None,
                DecodedScalarIndex::Sorted(index) => {
                    let (low, high) = match direction {
                        Direction::Ascending => (start, Bound::Unbounded),
                        Direction::Descending => (Bound::Unbounded, start),
                    };
                    Some(Box::new(
                        index
                            .scan_range(low, high, direction, None)
                            .map(|(key, row)| (key.to_key(), row)),
                    ))
                }
            },
        }
    }
}

/// A scalar column of one unit.
pub struct ColumnRef<'v>(ColumnInner<'v>);

enum ColumnInner<'v> {
    /// A memtable's column.
    Memtable(&'v MemtableData, FieldId),
    /// A segment's decoded column.
    Segment(Arc<ScalarColumn>),
    /// The unit has no column for the field: every row is null.
    Missing,
}

impl ColumnRef<'_> {
    /// The value of `row`; `None` for null.
    ///
    /// # Errors
    ///
    /// Typed corruption for an undecodable segment value.
    pub fn value(&self, row: RowId) -> Result<Option<Value>> {
        match &self.0 {
            ColumnInner::Memtable(memtable, field) => Ok(memtable.value(*field, row)),
            ColumnInner::Segment(column) => {
                let value = column.value(row as usize).map_err(LogPoseError::from)?;
                Ok((!value.is_null()).then_some(value))
            }
            ColumnInner::Missing => Ok(None),
        }
    }
}

/// `$extra` of one unit.
pub struct DynamicRef<'v>(DynamicInner<'v>);

enum DynamicInner<'v> {
    /// A memtable's objects.
    Memtable(&'v MemtableData),
    /// A segment's decoded blocks (only fetched ones).
    Segment {
        /// The unit.
        unit: UnitId,
        /// Decoded blocks by block number.
        blocks: HashMap<u32, Arc<DynamicBlock>>,
    },
    /// No row of the unit has dynamic keys.
    Empty,
}

impl DynamicRef<'_> {
    /// The `$extra` object of `row` as stored (callers apply shadowing), or `None`.
    ///
    /// # Errors
    ///
    /// `Internal` if the row's block was not fetched, or typed corruption.
    pub fn object(&self, row: RowId) -> Result<Option<serde_json::Map<String, serde_json::Value>>> {
        match &self.0 {
            DynamicInner::Memtable(memtable) => {
                memtable.dynamic_object(row).map_err(LogPoseError::internal)
            }
            DynamicInner::Segment { unit, blocks } => {
                let block = row / crate::segment_v2::DYNAMIC_BLOCK_ROWS;
                let block = blocks
                    .get(&block)
                    .ok_or_else(|| not_fetched(*unit, "a dynamic block"))?;
                block.object(row).map_err(|error| {
                    LogPoseError::corrupt(logpose_types::CorruptionKind::Segment, error.0)
                })
            }
            DynamicInner::Empty => Ok(None),
        }
    }
}

/// SQ8 codes of one segment field, over pinned bytes.
#[derive(Clone)]
pub struct Sq8Codes {
    section: Arc<Sq8Section>,
    bytes: Arc<AlignedBytes>,
}

impl Sq8Codes {
    /// The parsed section (params and geometry).
    #[must_use]
    pub fn section(&self) -> &Sq8Section {
        &self.section
    }

    /// Every row's code, `rows * dims` bytes.
    #[must_use]
    pub fn codes(&self) -> &[u8] {
        self.section.codes(&self.bytes)
    }
}

/// The vector index of one unit's field.
#[derive(Clone, Default)]
pub struct VectorIndexRef {
    /// The graph over distinct vectors, if the segment has one.
    pub graph: Option<Arc<SegmentGraph>>,
    /// The SQ8 codes, if the segment has them.
    pub sq8: Option<Sq8Codes>,
}

/// The f32 vectors of one unit's field.
pub struct VectorRowsRef<'p>(VectorRowsInner<'p>);

enum VectorRowsInner<'p> {
    /// A memtable's arena.
    Memtable(&'p MemtableData, FieldId),
    /// A segment's pinned pages, looked up per row (a search touches a few of many pages).
    Segment {
        /// The segment.
        segment: &'p SegmentHandle,
        /// The verified prefix.
        handle: Arc<VectorHandle>,
        /// The pins holding the fetched pages.
        pins: &'p PinSet,
    },
    /// The unit has no vectors for the field.
    Missing,
}

impl VectorRowsRef<'_> {
    /// The vector of `row`, or `None` when it is null.
    ///
    /// # Errors
    ///
    /// `Internal` if the row's page was not fetched.
    pub fn get(&self, row: RowId) -> Result<Option<std::borrow::Cow<'_, [f32]>>> {
        match &self.0 {
            VectorRowsInner::Memtable(memtable, field) => {
                Ok(memtable.vector(*field, row).map(std::borrow::Cow::Borrowed))
            }
            VectorRowsInner::Segment {
                segment,
                handle,
                pins,
            } => {
                let prefix = handle.prefix();
                if row >= prefix.row_count() || prefix.nulls().contains(row) {
                    return Ok(None);
                }
                let page_rows = prefix.page_rows().max(1);
                let page = handle
                    .page_unit(row / page_rows)
                    .and_then(|unit| segment.reader().unit_key(&unit))
                    .and_then(|key| pins.get(&key))
                    .ok_or_else(|| not_fetched(segment.unit, "a vector page"))?;
                let dim = prefix.dim() as usize;
                let start = (row % page_rows) as usize * dim * 4;
                let bytes = page
                    .get(start..start + dim * 4)
                    .ok_or_else(|| LogPoseError::internal("a vector page is short"))?;
                Ok(Some(match bytemuck::try_cast_slice::<u8, f32>(bytes) {
                    Ok(floats) if cfg!(target_endian = "little") => {
                        std::borrow::Cow::Borrowed(floats)
                    }
                    _ => std::borrow::Cow::Owned(
                        bytes
                            .chunks_exact(4)
                            .map(|chunk| {
                                f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
                            })
                            .collect(),
                    ),
                }))
            }
            VectorRowsInner::Missing => Ok(None),
        }
    }
}

impl EngineCore {
    /// The view `options` select of the collection `handle` serves. Reads no file.
    pub(crate) fn read_view(
        self: &Arc<Self>,
        handle: &CollectionHandle,
        options: &ReadOptions,
    ) -> Result<ReadView> {
        handle.ensure_open()?;
        if options.token.is_some() && options.snapshot.is_some() {
            return Err(LogPoseError::invalid_field(
                "snapshot",
                "a snapshot and a snapshot token cannot be provided together",
            ));
        }
        if options.snapshot.is_some() && options.read_barrier.is_some() {
            return Err(LogPoseError::invalid_field(
                "read_barrier",
                "snapshot and read_barrier cannot be provided together",
            ));
        }
        let (version, token) = match &options.token {
            Some(token) => (handle.snapshot_version(token)?, Some(token.clone())),
            None => {
                let (version, _) = handle.read_state(ReadAt::Snapshot(options.snapshot.clone()))?;
                (version, None)
            }
        };
        // The barrier holds for the state the view reads, not the current one: a token pins an
        // older state, which must not pass a barrier only a later state satisfies.
        if let Some(barrier) = &options.read_barrier {
            let read = version.snapshot();
            if !read.satisfies_read_barrier(barrier) {
                return Err(LogPoseError::ReadBarrierNotSatisfied {
                    collection: handle.descriptor().lookup_name(),
                    required_manifest_generation: barrier.manifest_generation,
                    required_seq_no: barrier.visible_seq_no,
                    visible_manifest_generation: read.manifest_generation,
                    visible_seq_no: read.visible_seq_no,
                });
            }
        }
        let token = match token {
            Some(token) => Some(token),
            None if options.pin => Some(handle.pin_version(Arc::clone(&version))?),
            None => None,
        };
        Ok(ReadView::new(version, Arc::downgrade(self), token))
    }
}

impl Engine {
    /// A view of the collection `reference` names; see [`CollectionReader`]. The first read
    /// of a recovered collection lets its background maintenance run.
    ///
    /// # Errors
    ///
    /// `NotFound` for an unknown collection, the collection's failure, `SnapshotExpired`,
    /// `TooManySnapshots` when `pin` cannot pin, `ReadBarrierNotSatisfied`, or
    /// `InvalidArgument` for an invalid snapshot.
    pub fn read_view_blocking(
        &self,
        reference: &CollectionRef,
        options: &ReadOptions,
    ) -> Result<ReadView> {
        let handle = self.collection(reference)?;
        handle.arm_maintenance();
        self.core().arc().read_view(&handle, options)
    }
}

impl CollectionReader for Engine {
    fn read_view<'a>(
        &'a self,
        collection: &'a CollectionRef,
        options: ReadOptions,
    ) -> BoxFuture<'a, Result<ReadView>> {
        Box::pin(async move { self.read_view_blocking(collection, &options) })
    }
}

/// Keys in `memtable` after `after`, used by [`PkRef::ascending_after`].
impl MemtableData {
    pub(crate) fn keys_after<'a>(
        &'a self,
        after: Option<&PrimaryKey>,
    ) -> Box<dyn Iterator<Item = (&'a PrimaryKey, RowId)> + 'a> {
        match after {
            None => Box::new(self.keys()),
            Some(after) => {
                Box::new(self.keys_range((Bound::Excluded(after.clone()), Bound::Unbounded)))
            }
        }
    }
}

/// Index keys of a value, for callers that compile filters against [`ScalarIndexRef`]: one
/// per array element, none for null or JSON.
#[must_use]
pub fn value_index_keys(value: &Value) -> Vec<ScalarKey> {
    index_keys(value)
}
