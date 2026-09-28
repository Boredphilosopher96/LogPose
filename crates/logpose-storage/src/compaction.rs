//! Compaction: the size-tiered policy that picks a job's inputs, and the job that rewrites
//! them into one segment, dropping deleted rows, with the deletions that arrive while it runs
//! reconciled onto the output at commit.
//!
//! **Policy.** The writer runs [`Policy::plan`] over its unreserved segments, using live rows
//! (`row_count - dv.len`), whenever the segment set or the deletion counts may have changed:
//!
//! 1. **Deletion-driven.** The segment with the most deleted rows among those of at least
//!    `base_rows` rows whose deleted fraction reaches `deleted_ratio`, plus up to
//!    `min_merge - 1` of the smallest unreserved segments in the same or a lower tier.
//! 2. **Tiered.** For each tier from the lowest (tier 0 holds segments below `base_rows` live
//!    rows, tier `t >= 1` holds `[base_rows * ratio^(t-1), base_rows * ratio^t)`): once it has
//!    `min_merge` unreserved segments, take them in ascending unit order until `max_merge`,
//!    `max_output_rows`, `max_output_bytes`, or the job's memory cap would be exceeded.
//! 3. At most `max_jobs_per_collection` jobs at once, and no segment in two of them.
//!
//! Every job is sized to its maintenance-memory reservation, [`build_bytes`] (the output's rows
//! plus the largest input, which the build reads whole): the policy caps a background job at
//! half the pool, so two jobs can run at once, except that a deletion-driven rewrite of one
//! segment alone may take the whole pool. The scheduler grants a permit only once that much of
//! the pool is free. A row is rewritten about once per tier it climbs.
//!
//! **Job.** The protocol:
//!
//! 1. **Begin** (writer, on the permit). Capture every input with `D0_i`, its deletion vector
//!    now, and the job's unit `o`. The inputs were reserved when the job was planned.
//! 2. **Build** (job thread, reads around the buffer cache). For each input in order, copy
//!    every row not in `D0_i` to the output (fields as the current schema declares them:
//!    dropped fields are gone, added fields read null) and record `map_i[r]`, the output row,
//!    or `u32::MAX` for a row in `D0_i`.
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
    /// Rewrite a segment once this fraction of its rows is deleted. Default 0.2.
    pub deleted_ratio: f64,
    /// Most compaction jobs of one collection at once. Default 2.
    pub max_jobs_per_collection: usize,
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
            max_jobs_per_collection: 2,
        }
    }
}

/// Graph bytes a build holds per row and vector field: 32 neighbours of 4 bytes, plus 10
/// percent of structure.
const GRAPH_BYTES_PER_ROW: u64 = 141;

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
        }
    }
}

/// Memory a build of `inputs` holds: every copied row as stored (vectors, scalar columns, and
/// keys, from the input files' bytes per row), plus the graph under construction for each
/// vector field (`32 * 4 * 1.1` bytes per row), plus the input being read. The build reads one
/// input at a time with every section loaded whole, deleted rows included, so the largest
/// input's file is charged in full: a rewrite of a mostly deleted segment holds far more than
/// its live rows.
pub(crate) fn build_bytes(inputs: &[Candidate], shape: RowShape) -> u64 {
    let output = inputs
        .iter()
        .map(|input| {
            let per_row = input.file_len / u64::from(input.rows.max(1))
                + shape.vector_fields * GRAPH_BYTES_PER_ROW;
            input.live().saturating_mul(per_row)
        })
        .fold(0_u64, u64::saturating_add);
    let largest_input = inputs.iter().map(|input| input.file_len).max().unwrap_or(0);
    output.saturating_add(largest_input)
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
}

/// One planned compaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompactionPlan {
    /// Ascending by unit.
    pub(crate) inputs: Vec<UnitId>,
    /// The maintenance memory its permit reserves.
    pub(crate) build_bytes: u64,
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
    /// `max_output_rows`, `max_output_bytes`, and `memory_cap` of build memory.
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
            reason,
        }
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
            let tier = self.tier(heavy.live());
            let mut smaller = free
                .iter()
                .filter(|segment| self.tier(segment.live()) <= tier)
                .copied()
                .collect::<Vec<_>>();
            smaller.sort_by_key(|segment| (segment.live(), segment.unit));
            let mut taken = vec![heavy];
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

    /// Whether `segment`'s deleted fraction reached `deleted_ratio`. Only segments of at least
    /// `base_rows` rows qualify: a tier-0 segment is cheap to keep, and the tiered rule merges
    /// it (dropping its deleted rows) soon enough, so a small collection is not rewritten on
    /// every delete.
    #[allow(clippy::cast_precision_loss)]
    fn too_deleted(&self, segment: &Candidate) -> bool {
        segment.rows >= self.config.base_rows
            && segment.deleted > 0
            && segment.deleted as f64 / f64::from(segment.rows.max(1)) >= self.config.deleted_ratio
    }

    /// An explicit compaction over `segments` that avoid `reserved`, if at least two do: the
    /// first two in ascending unit order, then as many more as fit one job whose build uses at
    /// most the whole pool (and within `max_output_rows` and `max_output_bytes`). A job whose
    /// first two inputs already need more than the pool is planned anyway, and the scheduler
    /// declines it.
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
        let mut taken = Vec::new();
        for segment in segments
            .iter()
            .filter(|segment| !reserved.contains(&segment.unit))
        {
            if taken.len() >= 2 && !explicit.fits(&taken, segment, self.pool_bytes) {
                break;
            }
            taken.push(*segment);
        }
        (taken.len() >= 2).then(|| explicit.job(&taken, PlanReason::Explicit))
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
