//! Compaction: the size-tiered policy that picks a job's inputs, and the job that rewrites
//! them into one segment, dropping deleted rows, with the deletions that arrive while it runs
//! reconciled onto the output at commit.
//!
//! **Policy.** The writer runs [`Policy::plan`] over its unreserved segments, using live rows
//! (`row_count - dv.len`), whenever the segment set or the deletion counts may have changed:
//!
//! 1. **Deletion-driven.** The segment with the most deleted rows among those whose deleted
//!    fraction reaches `deleted_ratio` (segments of at least `base_rows` rows) or
//!    `small_deleted_ratio` with at least `small_deleted_rows` deleted rows (smaller ones). A
//!    segment of at least `base_rows` rows takes up to `min_merge - 1` of the smallest
//!    unreserved segments in the same or a lower tier along; a smaller one is rewritten alone.
//! 2. **Tiered.** For each tier from the lowest (tier 0 holds segments below `base_rows` live
//!    rows, tier `t >= 1` holds `[base_rows * ratio^(t-1), base_rows * ratio^t)`): once it has
//!    `min_merge` unreserved segments, take them in ascending unit order until `max_merge`,
//!    `max_output_rows`, `max_output_bytes`, or the job's memory cap would be exceeded.
//! 3. **Quiet.** Once the collection took no write for `quiet_after`, segments below the graph
//!    threshold (`IndexPolicy::graph_min_rows` live rows) merge together, smallest first
//!    ([`Policy::plan_quiet`]), so a bulk load that went quiet does not leave small segments
//!    that every search scans exactly. What remains below the threshold (a lone small segment)
//!    gets its graph from the writer's quiet index builds instead of being merged into a large
//!    segment, which would rewrite that segment and its graph for a few rows.
//! 4. At most `max_jobs_per_collection` jobs at once, and no segment in two of them.
//!
//! Every job is sized to its maintenance-memory reservation, [`build_bytes`] (the output's rows
//! plus the largest input, which the build reads whole): the policy caps a background job at
//! half the pool, so two jobs can run at once, except that a deletion-driven rewrite of one
//! segment alone may take the whole pool. The scheduler grants a permit only once that much of
//! the pool is free. Every output is also capped so that its index build
//! ([`index_build_bytes`]) fits the whole pool, or it could never get its graph. A row is
//! rewritten about once per tier it climbs.
//!
//! Compaction builds no graph: its output gets SQ8 codes and scalar indexes (one pass over its
//! rows), and the writer then plans the output's index build, which adds the graph in a
//! sidecar. Without the graph, a build holds about the output's stored bytes plus two bytes
//! per vector dimension, instead of more than twice that, so the same pool merges about twice
//! the rows per job. An explicit compaction ([`Policy::plan_explicit`]) merges smallest first
//! and the writer repeats it until the segments it covers settle, so it converges on as few
//! segments as the pool allows instead of stopping after one job.
//!
//! **Job.** The protocol:
//!
//! 1. **Begin** (writer, on the permit). Capture every input with `D0_i`, its deletion vector
//!    now, and the job's unit `o`. The inputs were reserved when the job was planned.
//! 2. **Build** (job thread, reads around the buffer cache). For each input in order, copy
//!    every row not in `D0_i` to the output (fields as the current schema declares them:
//!    dropped fields are gone, added fields read null) and record `map_i[r]`, the output row,
//!    or `u32::MAX` for a row in `D0_i`. Build the output's SQ8 codes and scalar indexes (its
//!    graph comes later, from its index-build job).
//! 3. **Write** `segments/<o>.seg`, `sync_all`, `sync_dir(segments/)`
//!    (`CompactionAfterOutputSync`).
//! 4. **Commit** (writer, with no write processed until the new version is published): for each
//!    input, every deletion set since the begin (`deletes[I_i] AND NOT D0_i`, the bits whose
//!    row was copied) lands on `map_i[r]` in `DV_o`; if `DV_o` is not empty its DV file is
//!    written and synced (`CompactionAfterDvSync`) before manifest `g + 1` (the durable
//!    segments minus the inputs plus `o`) is published. Then the inputs leave the version (their
//!    files once the last version holding them is released), the primary-key index is
//!    forwarded from each input to `o`, and the inputs' DV files are removed.
//!
//! Every live row is therefore live exactly once before and after the swap (I4, I5).

use crate::{
    engine::CoreRef,
    fs_util::crash_point,
    handle::{CollectionHandle, JobTicket},
    manifest::SegmentOrigin,
    paths::{SEGMENTS_DIR, segment_path},
    segment::{SegmentHandle, manifest_entry, write_segment},
    segment_v2::{SegmentBuilder, SegmentIdentity},
    version::Version,
    writer::{CompactStart, CompactedSegment, JobCommit},
};
use logpose_types::{
    LogPoseError, Result, RowAddr, UnitId, record::PrimaryKey, schema::CollectionSchema,
};
use logpose_vfs::CrashPoint;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

impl CoreRef {
    /// Build and write what a compaction begun at `version` commits: the output segment of
    /// unit `unit` from the inputs' rows outside their captured deletion vectors, with the row
    /// maps the writer reconciles later deletions through.
    pub(crate) fn build_compaction(
        &self,
        handle: &Arc<CollectionHandle>,
        version: &Version,
        unit: UnitId,
        work: &CompactStart,
        ticket: &mut JobTicket,
    ) -> Result<JobCommit> {
        let vfs = self.vfs.as_ref();
        let dir = &handle.meta().dir;
        let schema = Arc::clone(&version.schema);
        let inputs = work
            .inputs
            .iter()
            .map(|(segment, _)| segment.unit)
            .collect::<Vec<_>>();
        let mut builder = SegmentBuilder::new(
            Arc::clone(&schema),
            SegmentIdentity {
                collection_id: handle.meta().id.clone(),
                unit_id: unit.0,
            },
        )
        .map_err(LogPoseError::from)?;
        let mut maps = Vec::with_capacity(work.inputs.len());
        let mut pks = Vec::new();
        let mut sources = Vec::new();
        for (segment, deleted) in &work.inputs {
            // One input at a time, with each copied row decoded only as it is visited: the build
            // holds the output, plus the sections of the input it reads (`build_bytes`).
            let mut map = Vec::new();
            segment.for_each_row(
                |row| !deleted.contains(row),
                |row, stored| {
                    // Rows arrive in order; the skipped ones in between were deleted.
                    map.resize(row as usize, u32::MAX);
                    map.push(builder.row_count());
                    builder
                        .push_row_image(stored.seq_no, &stored.image)
                        .map_err(LogPoseError::from)?;
                    pks.push(PrimaryKey::from(stored.image.pk));
                    sources.push(RowAddr {
                        unit: segment.unit,
                        row,
                    });
                    Ok(())
                },
            )?;
            // Trailing deleted rows.
            let rows = (segment.row_count() as usize).max(map.len());
            map.resize(rows, u32::MAX);
            maps.push(Arc::<[u32]>::from(map));
        }
        let output = if builder.row_count() == 0 {
            None
        } else {
            let path = segment_path(dir, unit);
            ticket.writing_files();
            self.build_indexes(&mut builder)?;
            let (file, written) = write_segment(vfs, &path, builder)?;
            let segments = dir.join(SEGMENTS_DIR);
            vfs.sync_dir(&segments).map_err(|error| {
                LogPoseError::io(format!("failed to sync '{}'", segments.display()), error)
            })?;
            crash_point(vfs, Some(CrashPoint::CompactionAfterOutputSync))?;
            let entry = manifest_entry(
                unit,
                &schema,
                &written,
                SegmentOrigin::Compaction {
                    inputs: inputs.clone(),
                },
            );
            let segment = SegmentHandle::from_file(
                file,
                path,
                &handle.meta().id,
                entry,
                self.buffer_cache(),
                self.gc.clone(),
            )?;
            Some(CompactedSegment {
                handle: Arc::new(segment),
                maps,
                pks: Arc::from(pks),
                sources: Arc::from(sources),
            })
        };
        Ok(JobCommit::Compact { inputs, output })
    }
}

/// Size-tiered compaction settings, engine-wide. A collection's
/// `compaction_threshold_segments` overrides `min_merge`; `usize::MAX` turns its background
/// compaction off (explicit compactions still run).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompactionConfig {
    /// Tier 0 holds segments with fewer live rows than this. Default 32,768.
    pub base_rows: u32,
    /// Each tier above 0 spans this factor of live rows. Default 4.
    pub tier_ratio: u32,
    /// Segments of one tier that start a merge. Default 4.
    pub min_merge: usize,
    /// Most segments one job merges. Default 10.
    pub max_merge: usize,
    /// Most live rows in one output. Default 2,000,000; the memory cap may lower it.
    pub max_output_rows: u32,
    /// Most `f32` vector bytes in one output. Default 8 GiB.
    pub max_output_bytes: u64,
    /// Rewrite a segment of at least `base_rows` rows once this fraction of its rows is
    /// deleted. Default 0.2.
    pub deleted_ratio: f64,
    /// Rewrite a segment of fewer than `base_rows` rows once this fraction of its rows is
    /// deleted, and at least `small_deleted_rows` of them. Default 0.5.
    pub small_deleted_ratio: f64,
    /// The fewest deleted rows that get a segment of fewer than `base_rows` rows rewritten for
    /// its deletions, so a small collection is not rewritten on every delete. Default 64.
    pub small_deleted_rows: u32,
    /// Most compaction jobs of one collection at once. Default 2.
    pub max_jobs_per_collection: usize,
    /// A collection that took no write for this long is quiet: its segments below the graph
    /// threshold merge together, and every segment with SQ8 codes gets its graph. Default 10 s.
    pub quiet_after: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            base_rows: 32_768,
            tier_ratio: 4,
            min_merge: 4,
            max_merge: 10,
            max_output_rows: 2_000_000,
            max_output_bytes: 8 << 30,
            deleted_ratio: 0.2,
            small_deleted_ratio: 0.5,
            small_deleted_rows: 64,
            max_jobs_per_collection: 2,
            quiet_after: Duration::from_secs(10),
        }
    }
}

/// Graph bytes a build holds per row and vector field: 32 neighbours of 4 bytes, plus 10
/// percent of structure.
const GRAPH_BYTES_PER_ROW: u64 = 141;

/// Bytes a segment's own index sections hold per row and vector dimension beyond the stored
/// rows: the SQ8 codes, and their encoded payload. The bounds are found in a pass over the
/// stored bytes, with no f32 copy.
const VECTOR_INDEX_BYTES_PER_DIM: u64 = 2;

/// Bytes a graph build holds per row and vector dimension: the f32 copy of the vectors it reads
/// back from the segment.
const GRAPH_INPUT_BYTES_PER_DIM: u64 = 4;

/// Bytes a graph build holds per row and vector field while it deduplicates vectors: the map
/// from each distinct vector to its node.
const GRAPH_DEDUP_BYTES_PER_ROW: u64 = 32;

/// Bytes a graph build holds per row and vector field besides its input: the neighbour lists
/// while it links, then the lists and two serialized copies while it encodes, plus the node map
/// (`3 * 141 + 64`).
const GRAPH_BUILD_BYTES_PER_ROW: u64 = 3 * GRAPH_BYTES_PER_ROW + 64;

/// Bytes a scalar index build holds per row and indexed scalar field: its `(key, row)` pairs
/// while it sorts them, then the encoded postings.
const SCALAR_INDEX_BYTES_PER_ROW: u64 = 32;

/// What the policy knows about one segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Candidate {
    pub(crate) unit: UnitId,
    pub(crate) rows: u32,
    pub(crate) deleted: u64,
    /// The segment file's length, for the bytes a row takes.
    pub(crate) file_len: u64,
}

impl Candidate {
    /// The candidate for `segment` with `deleted` deleted rows.
    pub(crate) fn of(segment: &SegmentHandle, deleted: u64) -> Self {
        Self {
            unit: segment.unit,
            rows: segment.row_count(),
            deleted,
            file_len: segment.entry.file_len,
        }
    }

    pub(crate) fn live(&self) -> u64 {
        u64::from(self.rows).saturating_sub(self.deleted)
    }
}

/// The shape of the rows a job copies, for sizing it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RowShape {
    /// Vector fields in the schema.
    pub(crate) vector_fields: u64,
    /// Sum of their dimensions.
    pub(crate) vector_dims: u64,
    /// Scalar fields with an inverted or sorted index.
    pub(crate) indexed_scalar_fields: u64,
}

impl RowShape {
    pub(crate) fn of(schema: &CollectionSchema) -> Self {
        Self {
            vector_fields: schema.vectors().len() as u64,
            vector_dims: schema
                .vectors()
                .iter()
                .map(|field| u64::from(field.dimensions))
                .sum(),
            indexed_scalar_fields: schema
                .fields()
                .iter()
                .filter(|field| field.index.has_inverted() || field.index.has_sorted())
                .count() as u64,
        }
    }

    /// Bytes a segment's own index sections hold per output row beyond the rows themselves,
    /// as flush and compaction build them: the SQ8 codes and their payload, and the scalar
    /// indexes' pairs and postings. No graph: that is the index-build job's
    /// ([`graph_bytes_per_row`](Self::graph_bytes_per_row)).
    pub(crate) fn index_bytes_per_row(self) -> u64 {
        self.vector_dims * VECTOR_INDEX_BYTES_PER_DIM
            + self.indexed_scalar_fields * SCALAR_INDEX_BYTES_PER_ROW
    }

    /// Bytes a segment's index build holds per row: the f32 vectors it reads back, the
    /// deduplication map, and the graph's link lists while it links and encodes them. Before
    /// the graph moved out of flush and compaction, the same terms were measured with a
    /// counting allocator around the inline build (4,064 bytes per row at 768 dimensions
    /// against 4,327 charged, with the SQ8 codes included).
    pub(crate) fn graph_bytes_per_row(self) -> u64 {
        self.vector_dims * GRAPH_INPUT_BYTES_PER_DIM
            + self.vector_fields * (GRAPH_BUILD_BYTES_PER_ROW + GRAPH_DEDUP_BYTES_PER_ROW)
    }
}

/// Memory the index build of a segment of `rows` rows holds; its permit reserves it. Graphs of
/// several fields are built one after another, but the charge covers every field's input at
/// once, which errs high.
pub(crate) fn index_build_bytes(rows: u32, shape: RowShape) -> u64 {
    u64::from(rows).saturating_mul(shape.graph_bytes_per_row())
}

/// Memory a build of `inputs` holds: every copied row as stored (vectors, scalar columns, and
/// keys, from the input files' bytes per row), plus the index sections' build for each row
/// ([`RowShape::index_bytes_per_row`]), plus the input being read. The build reads one input at
/// a time with every section loaded whole, deleted rows included, so the largest input's file
/// is charged in full: a rewrite of a mostly deleted segment holds far more than its live rows.
pub(crate) fn build_bytes(inputs: &[Candidate], shape: RowShape) -> u64 {
    let output = inputs
        .iter()
        .map(|input| {
            let per_row =
                input.file_len / u64::from(input.rows.max(1)) + shape.index_bytes_per_row();
            input.live().saturating_mul(per_row)
        })
        .fold(0_u64, u64::saturating_add);
    let largest_input = inputs.iter().map(|input| input.file_len).max().unwrap_or(0);
    output.saturating_add(largest_input)
}

/// Memory a flush of a memtable of `slots` slots and `payload` bytes holds beyond the memtable:
/// the segment builder's copy of the rows, and its SQ8 and scalar index sections.
pub(crate) fn flush_build_bytes(slots: u32, payload: u64, shape: RowShape) -> u64 {
    payload.saturating_add(u64::from(slots).saturating_mul(shape.index_bytes_per_row()))
}

/// Why a job was planned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PlanReason {
    /// A segment's deleted fraction reached `deleted_ratio`.
    Deletions,
    /// A tier filled up.
    Tiered {
        /// The tier the inputs came from.
        tier: u8,
    },
    /// An explicit compaction request.
    Explicit,
    /// The collection went quiet with segments below the graph threshold.
    Quiet,
}

/// One planned compaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompactionPlan {
    /// Ascending by unit.
    pub(crate) inputs: Vec<UnitId>,
    /// The maintenance memory its permit reserves.
    pub(crate) build_bytes: u64,
    /// Live rows of the output.
    pub(crate) live_rows: u64,
    pub(crate) reason: PlanReason,
}

/// The size-tiered policy of one collection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Policy {
    pub(crate) config: CompactionConfig,
    /// Whether background compaction runs for the collection.
    pub(crate) background: bool,
    /// The engine's maintenance-memory pool; a background job reserves at most half of it,
    /// except a deletion-driven rewrite of one segment alone.
    pub(crate) pool_bytes: u64,
    pub(crate) shape: RowShape,
    /// Segments with fewer live rows get their graph only once the collection is quiet, and
    /// merge together then ([`plan_quiet`](Self::plan_quiet)).
    pub(crate) graph_min_rows: u32,
}

impl Policy {
    /// The policy of a collection whose descriptor sets `threshold` segments.
    pub(crate) fn new(
        config: CompactionConfig,
        threshold: usize,
        pool_bytes: u64,
        shape: RowShape,
    ) -> Self {
        let background = threshold != usize::MAX;
        Self {
            config: CompactionConfig {
                min_merge: if background {
                    threshold.max(2)
                } else {
                    config.min_merge
                },
                ..config
            },
            background,
            pool_bytes,
            shape,
            graph_min_rows: crate::segment_v2::IndexPolicy::default().graph_min_rows,
        }
    }

    /// The size tier of a segment with `live` rows.
    pub(crate) fn tier(&self, live: u64) -> u8 {
        tier_with(
            live,
            u64::from(self.config.base_rows.max(1)),
            u64::from(self.config.tier_ratio.max(2)),
        )
    }

    /// Whether adding `next` to `taken` stays within a job's caps: `max_merge`,
    /// `max_output_rows`, `max_output_bytes`, `memory_cap` of build memory, and an output whose
    /// index build fits the whole pool.
    fn fits(&self, taken: &[Candidate], next: &Candidate, memory_cap: u64) -> bool {
        if taken.len() + 1 > self.config.max_merge.max(2) {
            return false;
        }
        let rows = taken.iter().map(Candidate::live).sum::<u64>() + next.live();
        if rows > u64::from(self.config.max_output_rows) {
            return false;
        }
        if rows.saturating_mul(self.shape.vector_dims.saturating_mul(4))
            > self.config.max_output_bytes
        {
            return false;
        }
        if rows.saturating_mul(self.shape.graph_bytes_per_row()) > self.pool_bytes {
            return false;
        }
        let mut all = taken.to_vec();
        all.push(*next);
        build_bytes(&all, self.shape) <= memory_cap
    }

    fn job(&self, taken: &[Candidate], reason: PlanReason) -> CompactionPlan {
        let mut inputs = taken.iter().map(|input| input.unit).collect::<Vec<_>>();
        inputs.sort_unstable();
        CompactionPlan {
            inputs,
            build_bytes: build_bytes(taken, self.shape),
            live_rows: taken.iter().map(Candidate::live).sum(),
            reason,
        }
    }

    /// Whether `plan`'s output fits the whole pool, both to build and to get its graph: the
    /// two inputs an explicit compaction always takes may not.
    pub(crate) fn fits_pool(&self, plan: &CompactionPlan) -> bool {
        plan.build_bytes <= self.pool_bytes
            && plan
                .live_rows
                .saturating_mul(self.shape.graph_bytes_per_row())
                <= self.pool_bytes
    }

    /// Background jobs over `segments` that avoid `reserved`, at most `slots` of them. Each job
    /// reserves its inputs for the next one planned.
    pub(crate) fn plan(
        &self,
        segments: &[Candidate],
        reserved: &BTreeSet<UnitId>,
        slots: usize,
    ) -> Vec<CompactionPlan> {
        let mut plans = Vec::new();
        if !self.background || slots == 0 {
            return plans;
        }
        let cap = self.pool_bytes / 2;
        let mut free = segments
            .iter()
            .filter(|segment| !reserved.contains(&segment.unit))
            .copied()
            .collect::<Vec<_>>();

        // 1. Deletion-driven. The rewrite of the heavy segment alone may use the whole pool:
        // the largest segments are built to half of it, and a rewrite also holds the input file
        // it reads, so within half the pool a large segment could never shed its deleted rows.
        // Smaller segments join only while the job stays within half the pool.
        while plans.len() < slots {
            let heavy = free
                .iter()
                .filter(|segment| self.too_deleted(segment))
                .max_by_key(|segment| (segment.deleted, std::cmp::Reverse(segment.unit)))
                .copied();
            let Some(heavy) = heavy else { break };
            free.retain(|segment| segment.unit != heavy.unit);
            if !self.fits(&[], &heavy, self.pool_bytes) {
                // Too large to rewrite even with the whole pool, so too large for any job.
                continue;
            }
            let mut taken = vec![heavy];
            if heavy.rows < self.config.base_rows {
                // A small segment is rewritten alone, so the rewrite copies no more rows than
                // it drops. The tier-0 segments beside it have nothing to reclaim; they wait
                // for their tier to fill instead of being copied again for every small
                // segment that reaches the deleted-row floor.
                plans.push(self.job(&taken, PlanReason::Deletions));
                continue;
            }
            let tier = self.tier(heavy.live());
            let mut smaller = free
                .iter()
                .filter(|segment| self.tier(segment.live()) <= tier)
                .copied()
                .collect::<Vec<_>>();
            smaller.sort_by_key(|segment| (segment.live(), segment.unit));
            for segment in smaller {
                if taken.len() >= self.config.min_merge {
                    break;
                }
                if self.fits(&taken, &segment, cap) {
                    taken.push(segment);
                }
            }
            free.retain(|segment| !taken.iter().any(|input| input.unit == segment.unit));
            plans.push(self.job(&taken, PlanReason::Deletions));
        }

        // 2. Tiered, from the lowest tier.
        let mut tiers = BTreeMap::<u8, Vec<Candidate>>::new();
        for segment in &free {
            tiers
                .entry(self.tier(segment.live()))
                .or_default()
                .push(*segment);
        }
        for (tier, mut members) in tiers {
            members.sort_by_key(|segment| segment.unit);
            while plans.len() < slots && members.len() >= self.config.min_merge {
                let mut taken = Vec::new();
                for segment in &members {
                    if !self.fits(&taken, segment, cap) {
                        break;
                    }
                    taken.push(*segment);
                }
                if taken.len() < 2 {
                    break;
                }
                members.drain(..taken.len());
                plans.push(self.job(&taken, PlanReason::Tiered { tier }));
            }
        }
        plans
    }

    /// Jobs for a quiet collection, over `segments` that avoid `reserved`, at most `slots` of
    /// them: the unreserved segments below `graph_min_rows` live rows merge together, smallest
    /// first, within half the pool. A lone one is left alone (its graph is built instead).
    /// Nothing when background compaction is off.
    pub(crate) fn plan_quiet(
        &self,
        segments: &[Candidate],
        reserved: &BTreeSet<UnitId>,
        slots: usize,
    ) -> Vec<CompactionPlan> {
        let mut plans = Vec::new();
        if !self.background {
            return plans;
        }
        let cap = self.pool_bytes / 2;
        let mut small = segments
            .iter()
            .filter(|segment| {
                !reserved.contains(&segment.unit) && segment.live() < u64::from(self.graph_min_rows)
            })
            .copied()
            .collect::<Vec<_>>();
        small.sort_by_key(|segment| (segment.live(), segment.unit));
        while plans.len() < slots && small.len() >= 2 {
            let mut taken = Vec::new();
            for segment in &small {
                if !self.fits(&taken, segment, cap) {
                    break;
                }
                taken.push(*segment);
            }
            if taken.len() < 2 {
                break;
            }
            small.drain(..taken.len());
            plans.push(self.job(&taken, PlanReason::Quiet));
        }
        plans
    }

    /// Whether `segment` holds enough deleted rows to be rewritten for them. A segment of at
    /// least `base_rows` rows qualifies once its deleted fraction reaches `deleted_ratio`. A
    /// smaller one needs `small_deleted_ratio` and at least `small_deleted_rows` deleted rows:
    /// the tiered rule merges it (dropping its deleted rows) once its tier fills, and the floor
    /// keeps a small collection from being rewritten on every delete. The ratio bounds the
    /// cost: such a rewrite, of the segment alone, copies no more live rows than the deleted
    /// rows it drops, and a
    /// segment that never fills its tier keeps fewer than half its rows (or fewer than the
    /// floor) deleted.
    #[allow(clippy::cast_precision_loss)]
    fn too_deleted(&self, segment: &Candidate) -> bool {
        if segment.deleted == 0 {
            return false;
        }
        let fraction = segment.deleted as f64 / f64::from(segment.rows.max(1));
        if segment.rows >= self.config.base_rows {
            fraction >= self.config.deleted_ratio
        } else {
            segment.deleted >= u64::from(self.config.small_deleted_rows)
                && fraction >= self.config.small_deleted_ratio
        }
    }

    /// One job of an explicit compaction over `segments` that avoid `reserved`: the two with
    /// the fewest live rows, then as many more, smallest first, as fit one job whose build uses
    /// at most the whole pool (and within `max_output_rows` and `max_output_bytes`). Merging
    /// the smallest first is what lets repeated jobs converge on as few segments as the pool
    /// allows. A job whose first two inputs already need more than the pool is planned anyway;
    /// the writer then stops, or the scheduler declines it. A single segment is rewritten alone
    /// if it has deleted rows, so an explicit compaction always reclaims them; one without any
    /// is left alone.
    pub(crate) fn plan_explicit(
        &self,
        segments: &[Candidate],
        reserved: &BTreeSet<UnitId>,
    ) -> Option<CompactionPlan> {
        let explicit = Self {
            config: CompactionConfig {
                max_merge: usize::MAX,
                ..self.config
            },
            ..*self
        };
        let mut free = segments
            .iter()
            .filter(|segment| !reserved.contains(&segment.unit))
            .copied()
            .collect::<Vec<_>>();
        free.sort_by_key(|segment| (segment.live(), segment.unit));
        let mut taken = Vec::new();
        for segment in &free {
            if taken.len() >= 2 && !explicit.fits(&taken, segment, self.pool_bytes) {
                break;
            }
            taken.push(*segment);
        }
        let worth = match taken.as_slice() {
            [] => false,
            [only] => only.deleted > 0,
            _ => true,
        };
        worth.then(|| explicit.job(&taken, PlanReason::Explicit))
    }
}

/// The size tier of `rows` live rows, with tier 0 below `base` and tiers growing by `ratio`.
pub(crate) fn tier_with(rows: u64, base: u64, ratio: u64) -> u8 {
    let mut tier = 0;
    let mut bound = base;
    while rows >= bound {
        tier += 1;
        match bound.checked_mul(ratio) {
            Some(next) => bound = next,
            None => break,
        }
    }
    tier
}

#[cfg(test)]
mod tests;
