//! Maintenance at the writer: freezing memtables at their flush triggers, the write stall,
//! asking the scheduler for permits, beginning a job on its permit, committing it, and explicit
//! flushes and compactions.
//!
//! A job's life:
//!
//! 1. **Plan.** A frozen memtable is due a flush; the compaction policy picks a set of
//!    unreserved segments (and reserves them); a segment lacks its vector graphs (an index
//!    build). The writer asks the scheduler for a permit (`Phase::Waiting`).
//! 2. **Begin**, on `PermitGranted`, with the pipeline drained: allocate the output unit,
//!    capture the inputs from the private state (which equals the published, durable state),
//!    and start the build on a job thread (`Phase::Running`).
//! 3. **Commit**, on `JobDone`, with the pipeline drained: publish the manifest, then the
//!    `Version`, then release what the manifest superseded.
//! 4. **End**: release the permit and the reservations, remove the files of a job that did not
//!    commit, answer the explicit requests the job settles, and plan again.
//!
//! Flush triggers freeze the active memtable after a group, once the pipeline is drained (so
//! the frozen memtable holds exactly the operations in WAL files that end before the
//! rotation). With `max_frozen` memtables frozen and the active one over a trigger, the writer
//! stops taking requests (the bounded channel pushes back on clients) until a flush commits; a
//! request that waits longer than `write_stall_timeout` fails with `WriteStalled`.
//!
//! Index builds add the vector graphs flush and compaction no longer build: after every commit
//! the writer plans one build per collection for the largest segment that has SQ8 codes, no
//! index sidecar yet, and at least `graph_min_rows` non-null vectors (any number once the
//! collection is quiet). A build is never reserved against compaction: a compaction that takes
//! its segment cancels it, since the graph would be merged away. The commit publishes a
//! manifest in which the segment names its sidecar, and a `Version` in which its handle gains
//! the graphs; until then searches use the segment's SQ8 codes.
//!
//! An explicit compaction waits for background compactions, then merges the segments present
//! when it was asked (and the outputs they become), smallest first, one job at a time, until
//! one is left or no two fit the maintenance-memory pool; then it builds every graph those
//! segments lack, and answers.
//!
//! A failed job is retried in the background after a backoff of its own kind, so a failing
//! compaction or index build never holds back a freeze or a flush: flushes wait
//! `FLUSH_RETRY_BACKOFF`, and compactions and index builds wait twice as long after each
//! further failure in a row, up to `COMPACTION_RETRY_BACKOFF_MAX`. Once `max_flush_failures` flushes fail in a row, or one fails
//! in a way no retry can fix (corrupt data, a full or read-only device), the collection is
//! poisoned: writes, stalled ones included, fail at once with `CollectionPoisoned` instead of
//! stalling against a device that cannot take a flush, and reads keep serving.

use super::*;
use crate::{
    compaction::{Candidate, RowShape, flush_build_bytes, index_build_bytes},
    dv::{DvFile, dv_path, write_dv_file},
    fs_util::crash_point,
    handle::JobTicket,
    manifest::{DvRef, MANIFEST_FORMAT_VERSION, manifest_path, publish_manifest},
    paths::{SEGMENTS_DIR, index_path},
};
use logpose_types::MaintenanceError;
use logpose_vfs::CrashPoint;
use std::{
    collections::HashMap,
    io,
    sync::atomic::Ordering,
    time::{SystemTime, UNIX_EPOCH},
};

/// How long background flushes wait after a failed flush (or freeze) before they are tried
/// again.
pub(crate) const FLUSH_RETRY_BACKOFF: Duration = Duration::from_secs(1);
/// How long background compactions wait after one failed compaction; each further failure in a
/// row doubles the wait, up to [`COMPACTION_RETRY_BACKOFF_MAX`].
pub(crate) const COMPACTION_RETRY_BACKOFF: Duration = Duration::from_secs(1);
/// The longest wait between background attempts of compactions that keep failing.
pub(crate) const COMPACTION_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(60);

/// When background jobs of one kind may run again after failures: never before `retry_at`,
/// which is `first` after one failure and doubles with each further failure in a row, up to
/// `max`. A job of the kind that completes resets it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Backoff {
    first: Duration,
    max: Duration,
    /// Jobs of the kind that failed in a row.
    failures: u32,
    /// Engine-clock time before which no background job of the kind is requested.
    retry_at: Option<Duration>,
}

impl Backoff {
    /// A backoff of `first` after one failure, doubling up to `max`.
    pub(super) const fn new(first: Duration, max: Duration) -> Self {
        Self {
            first,
            max,
            failures: 0,
            retry_at: None,
        }
    }

    /// The backoff of background flushes: always [`FLUSH_RETRY_BACKOFF`], since the collection
    /// is poisoned after `max_flush_failures` in a row anyway.
    pub(super) const fn flush() -> Self {
        Self::new(FLUSH_RETRY_BACKOFF, FLUSH_RETRY_BACKOFF)
    }

    /// The backoff of background compactions.
    pub(super) const fn compaction() -> Self {
        Self::new(COMPACTION_RETRY_BACKOFF, COMPACTION_RETRY_BACKOFF_MAX)
    }

    /// Whether a background job of the kind may be requested at `now`.
    pub(super) fn ready(&self, now: Duration) -> bool {
        self.retry_at.is_none_or(|at| now >= at)
    }

    /// Record a failure at `now`. Returns the failures in a row, this one included.
    pub(super) fn failed(&mut self, now: Duration) -> u32 {
        self.failures = self.failures.saturating_add(1);
        let doublings = (self.failures - 1).min(31);
        let wait = self.first.saturating_mul(1_u32 << doublings).min(self.max);
        self.retry_at = Some(now.saturating_add(wait));
        self.failures
    }

    /// A job of the kind completed: the next failure waits `first` again.
    pub(super) fn succeeded(&mut self) {
        self.failures = 0;
        self.retry_at = None;
    }

    /// The engine-clock time before which no background job of the kind is requested.
    #[cfg(test)]
    pub(super) fn retry_at(&self) -> Option<Duration> {
        self.retry_at
    }
}

/// Whether a failed flush cannot succeed on a retry: the data it read is corrupt, or the
/// device is full or read-only. Such a failure poisons the collection at once.
fn lasting_failure(error: &LogPoseError) -> bool {
    match error {
        LogPoseError::Corrupt { .. } => true,
        LogPoseError::Io { source, .. } => matches!(
            source.kind(),
            io::ErrorKind::StorageFull
                | io::ErrorKind::QuotaExceeded
                | io::ErrorKind::ReadOnlyFilesystem
        ),
        _ => false,
    }
}

/// Whether `segment` needs its index build: it has none yet, and it has SQ8 codes for a vector
/// field with at least `min_rows` (and two) non-null vectors, so a graph can be walked.
fn needs_index(segment: &SegmentHandle, min_rows: u32) -> bool {
    segment.entry.index.is_none()
        && segment
            .entry
            .vectors
            .iter()
            .any(|vector| vector.has_sq8 && vector.non_null >= min_rows.max(2))
}

/// Wall-clock milliseconds since the Unix epoch, for operators reading a failure's time.
fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// An explicit compaction in progress: who waits for it, and the segments it settles.
pub(super) struct ExplicitCompaction {
    /// The explicit compactions to answer once it settles.
    waiters: Vec<SnapshotReply>,
    /// The segments present when it was asked (widened by later requests), and the outputs
    /// they were merged into.
    scope: BTreeSet<UnitId>,
    /// Compaction jobs it planned so far: only the first one's refusal is an error; later ones
    /// stop merging where the pool ends.
    merges: usize,
    /// Whether it rewrote a lone segment for its deleted rows, which it does once.
    rewrote_lone: bool,
}

/// An answer to send once the maintenance status is updated.
type Settled = Vec<(SnapshotReply, Result<Snapshot>)>;

/// How a job ended.
enum Outcome {
    /// Its manifest is durable and its version published.
    Committed(Snapshot),
    /// There was nothing to do.
    Nothing,
    /// It failed before its commit point (or its commit poisoned the collection).
    Failed(LogPoseError),
    /// A test dropped its ticket without committing.
    Abandoned,
}

impl Writer {
    pub(super) fn clock_now(&self) -> Duration {
        self.core.tokens.clock.now()
    }

    /// Rewrite one slice of primary-key entries that still point into retired units. Only
    /// between groups: never while a prepared group could still be taken back.
    pub(super) fn rewrite_slice(&mut self) {
        if let Some(state) = self.state.as_mut()
            && state.pk.rewriting()
        {
            state.pk.rewrite_slice(PK_REWRITE_SLICE);
        }
    }

    /// A checkpoint frame for the durable manifest.
    pub(super) fn checkpoint_frame(&self) -> Result<WalFrame> {
        checkpoint_frame(&self.manifest)
    }

    /// After a publish: report the primary-key index's size for the cache budget, freeze once
    /// the pipeline is drained if a flush trigger fired, and ask for a flush permit if a
    /// frozen memtable waits for one.
    pub(super) fn after_publish(&mut self) {
        if let Some(state) = &self.state {
            self.handle.set_pk_index_bytes(state.pk.approximate_bytes());
        }
        if self.freeze_due() {
            self.freeze_pending = true;
        }
        self.request_flush();
    }

    /// The periodic tick: expire requests a write stall held too long, the memtable age
    /// trigger, retries after a failed job, and the compaction policy over deletion counts
    /// that writes changed.
    pub(super) fn tick(&mut self) {
        if self.stalled() {
            self.expire_stalled_requests();
        }
        if self.freeze_due() {
            self.freeze_pending = true;
        }
        self.schedule();
    }

    /// Whether a background freeze or flush may be requested now: the collection had a
    /// data-plane access since it was opened, and no flush failed within the flush backoff.
    /// Compaction failures never hold it back.
    fn flush_allowed(&self) -> bool {
        self.handle.maintenance_armed() && self.flush_retry.ready(self.clock_now())
    }

    /// Whether background compactions may be planned now: the collection had a data-plane
    /// access since it was opened, and no compaction failed within the compaction backoff.
    fn compaction_allowed(&self) -> bool {
        self.handle.maintenance_armed() && self.compaction_retry.ready(self.clock_now())
    }

    /// Whether background index builds may be planned now: the collection had a data-plane
    /// access, its background maintenance is on, and no index build failed within the backoff.
    fn index_allowed(&self) -> bool {
        self.handle.maintenance_armed()
            && self.policy.background
            && self.index_retry.ready(self.clock_now())
    }

    /// Whether the collection is quiet: no write request for `quiet_after`.
    fn quiet(&self) -> bool {
        self.clock_now().saturating_sub(self.last_write_at) >= self.core.compaction.quiet_after
    }

    /// Whether the active memtable reached a flush trigger.
    fn flush_triggered(&self) -> bool {
        self.state.as_ref().is_some_and(|state| {
            should_flush(
                self.handle.descriptor(),
                &self.core.memtable,
                &state.active,
                self.clock_now(),
            )
        })
    }

    /// Whether the active memtable should be frozen now: it has operations, fewer than
    /// `max_frozen` memtables are frozen, and a flush trigger fired or an explicit flush waits
    /// for an operation in it.
    fn freeze_due(&self) -> bool {
        if self.refusal().is_some() {
            return false;
        }
        let Some(state) = &self.state else {
            return false;
        };
        if !state.active.has_ops() || state.frozen.len() >= self.core.memtable.max_frozen.max(1) {
            return false;
        }
        let waited = self
            .flush_waiters
            .iter()
            .any(|(target, _)| *target >= state.active.first_seq_no);
        waited || (self.flush_allowed() && self.flush_triggered())
    }

    /// Whether writes stall: `max_frozen` memtables are frozen and the active one reached a
    /// flush trigger. A refusing writer never stalls; it answers requests with its refusal.
    pub(super) fn stalled(&self) -> bool {
        if self.refusal().is_some() || !self.handle.maintenance_armed() {
            return false;
        }
        self.state
            .as_ref()
            .is_some_and(|state| state.frozen.len() >= self.core.memtable.max_frozen.max(1))
            && self.flush_triggered()
    }

    /// Fail the requests that waited through a write stall for longer than
    /// `write_stall_timeout`, oldest first. Requests arrive in order, so the rest are younger
    /// once the oldest is: at most one is held back to look at.
    fn expire_stalled_requests(&mut self) {
        let timeout = self.core.memtable.write_stall_timeout;
        let now = self.clock_now();
        loop {
            if self.held.is_none() {
                match self.requests.try_recv() {
                    Ok(request) => self.held = Some(request),
                    Err(_) => return,
                }
            }
            let expired = self
                .held
                .as_ref()
                .is_some_and(|held| now.saturating_sub(held.enqueued_at) >= timeout);
            if !expired {
                return;
            }
            if let Some(held) = self.held.take() {
                let error = LogPoseError::WriteStalled {
                    collection: self.handle.descriptor().lookup_name(),
                    reason: format!(
                        "{} memtables are waiting to be flushed and the active one is full; \
                         the write waited longer than {timeout:?}",
                        self.core.memtable.max_frozen.max(1)
                    ),
                };
                let _ = held.request.ack().send(Err(error));
            }
        }
    }

    /// Freeze the active memtable if it is due. The pipeline must be drained.
    pub(super) async fn freeze_if_due(&mut self) {
        if !self.freeze_due() {
            return;
        }
        match self.freeze().await {
            Ok(()) => {
                self.request_flush();
                self.update_status();
            }
            Err(error) => {
                tracing::warn!(
                    collection = %self.handle.descriptor().lookup_name(),
                    %error,
                    "freezing the memtable for a flush failed"
                );
                // Report the failure before answering, so a caller whose flush failed reads it
                // in the status. The waiters are taken first: should the failure poison the
                // collection, they still get the freeze's own error.
                let waiters = std::mem::take(&mut self.flush_waiters);
                self.job_failed(JobKind::Flush, &error);
                for (_, reply) in waiters {
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(error.clone()));
                    }
                }
            }
        }
    }

    /// Freeze the active memtable: rotate the WAL so the new file starts at the next sequence
    /// number, move the active memtable into `frozen`, start a new one with a fresh unit id,
    /// and publish. The pipeline must be drained, so the private state equals the published
    /// one.
    async fn freeze(&mut self) -> Result<()> {
        self.rotate_for_flush().await?;
        let unit = self.allocate_unit()?;
        let now = self.clock_now();
        let Some(state) = self.state.as_mut() else {
            return Err(self.handle.unavailable());
        };
        state.freeze(unit, now);
        let Some(version) = self.candidate() else {
            return Err(self.handle.unavailable());
        };
        self.handle.publish(version);
        Ok(())
    }

    /// Plan what is due: a flush of the oldest frozen memtable, then compactions (or the next
    /// step of an explicit compaction), then an index build. An explicit compaction that
    /// settled is answered after the status is updated.
    pub(super) fn schedule(&mut self) {
        let mut settled = Settled::new();
        if self.refusal().is_none() && self.state.is_some() {
            self.request_flush();
            self.schedule_compactions(&mut settled);
            self.schedule_indexes();
        }
        self.update_status();
        for (reply, result) in settled {
            let _ = reply.send(result);
        }
    }

    /// Ask for a flush permit if a memtable is frozen and no flush is planned or running.
    fn request_flush(&mut self) {
        if self.refusal().is_some() {
            return;
        }
        let frozen = self
            .state
            .as_ref()
            .is_some_and(|state| !state.frozen.is_empty());
        let flushing = self.jobs.values().any(|job| job.kind == JobKind::Flush)
            || !self.manual_flushes.is_empty();
        if !frozen || flushing {
            return;
        }
        if !self.flush_allowed() && self.flush_waiters.is_empty() {
            return;
        }
        // The flush builds the oldest frozen memtable; its reservation covers the builder's
        // copy of the rows and the index sections, which the memtable reservation does not.
        let bytes = self.state.as_ref().map_or(0, |state| {
            state.frozen.first().map_or(0, |memtable| {
                flush_build_bytes(
                    memtable.slot_count(),
                    memtable.bytes().payload,
                    RowShape::of(&state.schema),
                )
            })
        });
        if let Err(error) = self.request_job(JobKind::Flush, bytes, Vec::new(), None, false) {
            tracing::warn!(%error, "the scheduler refused a flush");
        }
        self.update_status();
    }

    /// The segments the policy sees, with their deletion counts now.
    fn candidates(&self) -> Vec<Candidate> {
        let Some(state) = &self.state else {
            return Vec::new();
        };
        state
            .segments
            .iter()
            .map(|segment| Candidate::of(segment, state.deletes.len_of(segment.unit)))
            .collect()
    }

    /// The policy for the schema now (the build estimate depends on the vector fields).
    fn policy(&self) -> Policy {
        let shape = self
            .state
            .as_ref()
            .map_or(self.policy.shape, |state| RowShape::of(&state.schema));
        Policy {
            shape,
            ..self.policy
        }
    }

    /// Plan compactions: the next step of an explicit compaction once no background
    /// compaction runs, otherwise what the size-tiered policy (and, once the collection is
    /// quiet, the quiet rule) picks for the free job slots.
    fn schedule_compactions(&mut self, settled: &mut Settled) {
        let compactions = self
            .jobs
            .values()
            .filter(|job| job.kind == JobKind::Compact)
            .count();
        if self.explicit.is_some() {
            if compactions == 0 {
                // Background compactions finish first; their outputs are merged too.
                self.schedule_explicit(settled);
            }
            return;
        }
        if !self.compaction_allowed() {
            return;
        }
        let slots = self
            .core
            .compaction
            .max_jobs_per_collection
            .saturating_sub(compactions);
        if slots == 0 {
            return;
        }
        let candidates = self.candidates();
        let policy = self.policy();
        let mut plans = policy.plan(&candidates, &self.reserved, slots);
        if plans.len() < slots && self.quiet() {
            let mut reserved = self.reserved.clone();
            reserved.extend(plans.iter().flat_map(|plan| plan.inputs.iter().copied()));
            plans.extend(policy.plan_quiet(&candidates, &reserved, slots - plans.len()));
        }
        for plan in plans {
            tracing::debug!(
                collection = %self.handle.descriptor().lookup_name(),
                inputs = ?plan.inputs,
                reason = ?plan.reason,
                build_bytes = plan.build_bytes,
                "planned a compaction"
            );
            if let Err(error) =
                self.request_job(JobKind::Compact, plan.build_bytes, plan.inputs, None, false)
            {
                tracing::warn!(%error, "the scheduler declined a compaction");
                let now = self.clock_now();
                self.compaction_retry.failed(now);
                break;
            }
        }
    }

    /// The next step of the explicit compaction: merge the smallest segments of its scope that
    /// fit one job, or, once nothing more merges, build the graphs its segments lack, or, once
    /// none is missing, answer its waiters. No compaction runs.
    fn schedule_explicit(&mut self, settled: &mut Settled) {
        let Some(mut explicit) = self.explicit.take() else {
            return;
        };
        let candidates = self
            .candidates()
            .into_iter()
            .filter(|candidate| explicit.scope.contains(&candidate.unit))
            .collect::<Vec<_>>();
        let policy = self.policy();
        let plan = policy
            .plan_explicit(&candidates, &self.reserved)
            .filter(|plan| plan.inputs.len() >= 2 || !explicit.rewrote_lone);
        if let Some(plan) = plan {
            let first = explicit.merges == 0;
            // The first job asks for its permit whatever it needs, so a compaction larger than
            // the pool fails with `TooLarge`. After it, a merge that does not fit the pool, or
            // whose output's graph would not, is where the compaction stops merging.
            if (first && plan.build_bytes > self.core.scheduler.pool_bytes())
                || policy.fits_pool(&plan)
            {
                explicit.merges += 1;
                explicit.rewrote_lone |= plan.inputs.len() == 1;
                match self.request_job(JobKind::Compact, plan.build_bytes, plan.inputs, None, true)
                {
                    Ok(_) => {
                        self.explicit = Some(explicit);
                        return;
                    }
                    Err(error) if first => {
                        for reply in explicit.waiters {
                            settled.push((reply, Err(error.clone())));
                        }
                        return;
                    }
                    Err(error) => {
                        tracing::debug!(%error, "an explicit compaction stops merging");
                    }
                }
            }
        }
        // Merged as far as it goes: every segment of the scope gets its graphs.
        let indexing = self.jobs.values().any(|job| {
            (job.explicit && job.kind == JobKind::Index)
                || job
                    .target
                    .is_some_and(|target| explicit.scope.contains(&target))
        });
        if indexing {
            self.explicit = Some(explicit);
            return;
        }
        if let Some((target, bytes)) = self.next_index_target(Some(&explicit.scope), 0) {
            match self.request_job(JobKind::Index, bytes, Vec::new(), Some(target), true) {
                Ok(_) => {
                    self.explicit = Some(explicit);
                    return;
                }
                Err(error) => {
                    for reply in explicit.waiters {
                        settled.push((reply, Err(error.clone())));
                    }
                    return;
                }
            }
        }
        let snapshot = self.handle.current().snapshot();
        for reply in explicit.waiters {
            settled.push((reply, Ok(snapshot.clone())));
        }
    }

    /// Plan a background index build: one per collection at a time, for the largest segment
    /// that needs its graphs (at least `graph_min_rows` non-null vectors, or any once the
    /// collection is quiet).
    fn schedule_indexes(&mut self) {
        if !self.index_allowed()
            || self
                .jobs
                .values()
                .any(|job| job.kind == JobKind::Index && !job.explicit)
        {
            return;
        }
        let min_rows = if self.quiet() {
            0
        } else {
            self.policy.graph_min_rows
        };
        let Some((target, bytes)) = self.next_index_target(None, min_rows) else {
            return;
        };
        tracing::debug!(
            collection = %self.handle.descriptor().lookup_name(),
            segment = %target,
            build_bytes = bytes,
            "planned an index build"
        );
        if let Err(error) = self.request_job(JobKind::Index, bytes, Vec::new(), Some(target), false)
        {
            tracing::warn!(%error, "the scheduler declined an index build");
            let now = self.clock_now();
            self.index_retry.failed(now);
        }
    }

    /// The segment an index build should take next, with the memory its build reserves: the
    /// largest segment (in `scope`, if given) that has SQ8 codes for a vector field with at
    /// least `min_rows` non-null vectors and no index sidecar, that no compaction holds and no
    /// index build takes, and whose build fits the maintenance-memory pool.
    fn next_index_target(
        &self,
        scope: Option<&BTreeSet<UnitId>>,
        min_rows: u32,
    ) -> Option<(UnitId, u64)> {
        let state = self.state.as_ref()?;
        let shape = RowShape::of(&state.schema);
        let pool = self.core.scheduler.pool_bytes();
        let taken = self
            .jobs
            .values()
            .filter_map(|job| job.target)
            .collect::<BTreeSet<_>>();
        state
            .segments
            .iter()
            .filter(|segment| {
                scope.is_none_or(|scope| scope.contains(&segment.unit))
                    && !self.reserved.contains(&segment.unit)
                    && !taken.contains(&segment.unit)
                    && needs_index(segment, min_rows)
                    && index_build_bytes(segment.row_count(), shape) <= pool
            })
            .max_by_key(|segment| (segment.row_count(), std::cmp::Reverse(segment.unit)))
            .map(|segment| (segment.unit, index_build_bytes(segment.row_count(), shape)))
    }

    /// Stop every index build of one of `segments`, which a compaction is taking: a waiting
    /// one is withdrawn, a running one is told to stop (it then ends without committing).
    fn cancel_indexing(&mut self, segments: &[UnitId]) {
        let doomed = self
            .jobs
            .iter()
            .filter(|(_, job)| job.target.is_some_and(|target| segments.contains(&target)))
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in doomed {
            let Some(job) = self.jobs.get(&id) else {
                continue;
            };
            match &job.phase {
                Phase::Waiting(request) => {
                    self.core.scheduler.cancel(*request);
                    self.jobs.remove(&id);
                }
                Phase::Running(running) => {
                    if let Some(cancel) = &running.cancel {
                        cancel.store(true, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Tell every running index build to stop: the collection is being dropped, poisoned, or
    /// shut down, and a graph build can take minutes.
    fn cancel_running_indexes(&self) {
        for job in self.jobs.values() {
            if let Phase::Running(running) = &job.phase
                && let Some(cancel) = &running.cancel
            {
                cancel.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Ask the scheduler for a permit for a new job; the grant arrives as `PermitGranted`. A
    /// compaction reserves its `inputs` and cancels the index builds of any of them; an index
    /// build names its `target`.
    fn request_job(
        &mut self,
        kind: JobKind,
        bytes: u64,
        inputs: Vec<UnitId>,
        target: Option<UnitId>,
        explicit: bool,
    ) -> Result<JobId> {
        let job = JobId(self.next_job);
        self.next_job += 1;
        let control = self.handle.control_sender();
        let request = self.core.scheduler.request(kind, bytes, move |permit| {
            // A writer that is gone drops the permit, which releases it.
            let _ = control.send(ControlMsg::PermitGranted { job, permit });
        })?;
        self.cancel_indexing(&inputs);
        self.reserved.extend(inputs.iter().copied());
        self.jobs.insert(
            job,
            Job {
                kind,
                inputs,
                target,
                explicit,
                phase: Phase::Waiting(request),
            },
        );
        Ok(job)
    }

    /// Withdraw every job still waiting for a permit (an explicit compaction plans its step
    /// again). A permit already on its way is dropped when it arrives for a job that no longer
    /// exists.
    fn cancel_waiting_jobs(&mut self) {
        let waiting = self
            .jobs
            .iter()
            .filter_map(|(job, entry)| match entry.phase {
                Phase::Waiting(request) => Some((*job, request)),
                Phase::Running(_) => None,
            })
            .collect::<Vec<_>>();
        for (job, request) in waiting {
            self.core.scheduler.cancel(request);
            if let Some(entry) = self.jobs.remove(&job) {
                for unit in &entry.inputs {
                    self.reserved.remove(unit);
                }
            }
        }
        self.update_status();
    }

    /// Refresh the maintenance status the handle reports.
    pub(super) fn update_status(&self) {
        let mut pending = Vec::new();
        let mut flushing = false;
        let mut compacting = false;
        let mut indexing = false;
        for job in self.jobs.values() {
            match (&job.phase, job.kind) {
                (Phase::Waiting(_), kind) => pending.push(kind.label().to_owned()),
                (Phase::Running(_), JobKind::Flush) => flushing = true,
                (Phase::Running(_), JobKind::Compact) => compacting = true,
                (Phase::Running(_), JobKind::Index) => indexing = true,
            }
        }
        let in_progress = if flushing || !self.manual_flushes.is_empty() {
            Some(JobKind::Flush.label().to_owned())
        } else if compacting {
            Some(JobKind::Compact.label().to_owned())
        } else if indexing {
            Some(JobKind::Index.label().to_owned())
        } else {
            None
        };
        self.handle.update_maintenance_status(|status| {
            status.pending = pending;
            status.in_progress = in_progress;
        });
    }

    pub(super) async fn handle_control(&mut self, message: ControlMsg) -> Flow {
        match message {
            ControlMsg::PermitGranted { job, permit } => self.begin(job, permit).await,
            ControlMsg::JobDone {
                job,
                result,
                wrote_files,
                reply,
            } => {
                let cancelled = self.jobs.get(&job).is_some_and(|entry| match &entry.phase {
                    Phase::Running(running) => running.cancelled(),
                    Phase::Waiting(_) => false,
                });
                let outcome = match result {
                    // A cancelled index build ends without a change, whatever it built.
                    _ if cancelled => Outcome::Abandoned,
                    Ok(commit) => match self.commit_job(job, commit).await {
                        Ok(snapshot) => Outcome::Committed(snapshot),
                        Err(error) => Outcome::Failed(error),
                    },
                    Err(error) => Outcome::Failed(error),
                };
                self.end_job(job, outcome, wrote_files, reply).await;
            }
            ControlMsg::EndJob { job, wrote_files } => {
                self.end_job(job, Outcome::Abandoned, wrote_files, None)
                    .await;
            }
            ControlMsg::Flush { reply } => self.explicit_flush(reply).await,
            ControlMsg::Compact { reply } => {
                if let Some(refusal) = self.refusal() {
                    let _ = reply.send(Err(refusal.clone_error()));
                } else {
                    self.handle.arm_maintenance();
                    let present = self
                        .state
                        .as_ref()
                        .map(|state| state.segments.iter().map(|segment| segment.unit).collect())
                        .unwrap_or_default();
                    let explicit = self.explicit.get_or_insert_with(|| ExplicitCompaction {
                        waiters: Vec::new(),
                        scope: BTreeSet::new(),
                        merges: 0,
                        rewrote_lone: false,
                    });
                    explicit.waiters.push(reply);
                    explicit.scope.extend::<BTreeSet<UnitId>>(present);
                    self.schedule();
                }
            }
            ControlMsg::BeginJob { kind, reply } => self.begin_by_hand(kind, reply).await,
            ControlMsg::Tick { reply } => {
                self.tick();
                // The control path drained the pipeline, so a freeze the tick found due runs
                // now rather than before the next group.
                if std::mem::take(&mut self.freeze_pending) {
                    self.freeze_if_due().await;
                }
                let _ = reply.try_send(());
            }
            ControlMsg::Quiesce { reply } => {
                // A drop voids every job that has not begun, and stops the index builds that
                // run. Should the drop not commit, the next tick plans them again.
                self.cancel_waiting_jobs();
                self.cancel_running_indexes();
                if self.handle.is_dropped() {
                    self.fail_waiters();
                }
                if self.running_jobs() == 0 {
                    let _ = reply.try_send(());
                } else {
                    self.quiesce_waiters.push(reply);
                }
            }
            ControlMsg::Shutdown => return Flow::Stop,
        }
        Flow::Continue
    }

    fn running_jobs(&self) -> usize {
        self.jobs
            .values()
            .filter(|job| matches!(job.phase, Phase::Running(_)))
            .count()
    }

    /// An explicit flush: wait until the checkpoint covers every operation visible now.
    async fn explicit_flush(&mut self, reply: Option<SnapshotReply>) {
        if let Some(refusal) = self.refusal() {
            if let Some(reply) = reply {
                let _ = reply.send(Err(refusal.clone_error()));
            }
            return;
        }
        self.handle.arm_maintenance();
        let Some(state) = &self.state else {
            if let Some(reply) = reply {
                let _ = reply.send(Err(self.handle.unavailable()));
            }
            return;
        };
        let target = state.visible_seq_no();
        if self.manifest.checkpoint_seq_no >= target {
            if let Some(reply) = reply {
                let _ = reply.send(Ok(self.handle.current().snapshot()));
            }
            return;
        }
        if reply.is_none() && self.flush_waiters.iter().any(|(_, reply)| reply.is_none()) {
            // The memtable-budget trigger asked already.
            return;
        }
        self.flush_waiters.push((target, reply));
        // The control path drained the pipeline.
        self.freeze_if_due().await;
        self.request_flush();
    }

    /// Allocate a unit id; never issued again.
    fn allocate_unit(&mut self) -> Result<UnitId> {
        let next = self
            .next_unit_id
            .checked_add(1)
            .ok_or_else(|| LogPoseError::internal("the collection has used every unit id"))?;
        let unit = UnitId(self.next_unit_id);
        self.next_unit_id = next;
        Ok(unit)
    }

    /// Begin job `job` on its permit: capture its inputs and start its build on a job thread.
    async fn begin(&mut self, job: JobId, permit: Permit) {
        let Some(entry) = self.jobs.get(&job) else {
            // Cancelled while the permit was on its way; dropping it releases it.
            return;
        };
        if !matches!(entry.phase, Phase::Waiting(_)) {
            return;
        }
        let (kind, inputs) = (entry.kind, entry.inputs.clone());
        if let Some(refusal) = self.refusal() {
            drop(permit);
            self.end_job(job, Outcome::Failed(refusal.clone_error()), false, None)
                .await;
            return;
        }
        let unit = match self.allocate_unit() {
            Ok(unit) => unit,
            Err(error) => {
                drop(permit);
                self.end_job(job, Outcome::Failed(error), false, None).await;
                return;
            }
        };
        let target = self.jobs.get(&job).and_then(|entry| entry.target);
        let running = self.running(kind, unit, target, Some(permit));
        if let Some(entry) = self.jobs.get_mut(&job) {
            entry.phase = Phase::Running(running);
        }
        self.update_status();
        let work = match kind {
            JobKind::Flush => self.begin_flush(job).await,
            JobKind::Compact => self.begin_compaction(&inputs),
            JobKind::Index => self.begin_index(job, target),
        };
        let work = match work {
            Ok(JobWork::Nothing) => {
                self.end_job(job, Outcome::Nothing, false, None).await;
                return;
            }
            Ok(work) => work,
            Err(error) => {
                self.end_job(job, Outcome::Failed(error), false, None).await;
                return;
            }
        };
        let start = JobStart {
            job,
            version: self.handle.current(),
            unit,
            work,
        };
        let core = self.core.clone();
        let handle = Arc::clone(&self.handle);
        let spawned = self.core.jobs.execute(move || {
            let mut ticket = JobTicket::new(Arc::clone(&handle), start.job);
            let result = if core.is_shutting_down() {
                Err(shutting_down())
            } else {
                match &start.work {
                    JobWork::Flush(work) => {
                        core.build_flush(&handle, &start.version, start.unit, work, &mut ticket)
                    }
                    JobWork::Compact(work) => core.build_compaction(
                        &handle,
                        &start.version,
                        start.unit,
                        work,
                        &mut ticket,
                    ),
                    JobWork::Index(work) => {
                        core.build_index(&handle, &start.version, start.unit, work, &mut ticket)
                    }
                    JobWork::Nothing => Err(LogPoseError::internal("a job with nothing to do")),
                }
            };
            // Release the inputs (and the version they came from) before the commit: once it
            // publishes, the last holder of a retired unit removes its files.
            drop(start);
            ticket.done(result);
        });
        if let Err(error) = spawned {
            self.end_job(job, Outcome::Failed(error), false, None).await;
        }
    }

    /// Begin a job without a permit, for a test that builds and commits it by hand.
    async fn begin_by_hand(&mut self, kind: JobKind, reply: oneshot::Sender<Result<JobStart>>) {
        if let Some(refusal) = self.refusal() {
            let _ = reply.send(Err(refusal.clone_error()));
            return;
        }
        let inputs = match kind {
            JobKind::Flush => {
                let running = self.jobs.values().any(|job| {
                    job.kind == JobKind::Flush && matches!(job.phase, Phase::Running(_))
                });
                if running {
                    self.manual_flushes.push(reply);
                    return;
                }
                // A flush waiting for its permit gives way.
                let waiting = self
                    .jobs
                    .iter()
                    .filter(|(_, job)| job.kind == JobKind::Flush)
                    .filter_map(|(id, job)| match job.phase {
                        Phase::Waiting(request) => Some((*id, request)),
                        Phase::Running(_) => None,
                    })
                    .collect::<Vec<_>>();
                for (id, request) in waiting {
                    self.core.scheduler.cancel(request);
                    self.jobs.remove(&id);
                }
                Vec::new()
            }
            JobKind::Compact => self
                .policy()
                .plan_explicit(&self.candidates(), &self.reserved)
                .map(|plan| plan.inputs)
                .unwrap_or_default(),
            JobKind::Index => Vec::new(),
        };
        let target = match kind {
            JobKind::Index => self.next_index_target(None, 0).map(|(target, _)| target),
            _ => None,
        };
        let job = JobId(self.next_job);
        self.next_job += 1;
        let unit = match self.allocate_unit() {
            Ok(unit) => unit,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        self.cancel_indexing(&inputs);
        self.reserved.extend(inputs.iter().copied());
        let running = self.running(kind, unit, target, None);
        self.jobs.insert(
            job,
            Job {
                kind,
                inputs: inputs.clone(),
                target,
                explicit: false,
                phase: Phase::Running(running),
            },
        );
        self.update_status();
        let work = match kind {
            JobKind::Flush => self.begin_flush(job).await,
            JobKind::Compact if inputs.is_empty() => Ok(JobWork::Nothing),
            JobKind::Compact => self.begin_compaction(&inputs),
            JobKind::Index => self.begin_index(job, target),
        };
        let work = match work {
            Ok(work) => work,
            Err(error) => {
                // The job is over before the caller hears of it, as for every answer.
                self.end_job(job, Outcome::Failed(error.clone()), false, None)
                    .await;
                let _ = reply.send(Err(error));
                return;
            }
        };
        let nothing = matches!(work, JobWork::Nothing);
        let start = JobStart {
            job,
            version: self.handle.current(),
            unit,
            work,
        };
        if nothing {
            // Nothing to do: the job is over before the caller hears of it.
            self.end_job(job, Outcome::Abandoned, false, None).await;
            let _ = reply.send(Ok(start));
        } else if reply.send(Ok(start)).is_err() {
            // Gone before it wrote anything.
            self.end_job(job, Outcome::Abandoned, false, None).await;
        }
    }

    /// Capture a flush's inputs: the oldest frozen memtable (freezing the active one first if
    /// none is frozen), its deleted slots, a DV file generation for every segment whose
    /// deletion vector grew since its durable generation, and `J`. The pipeline is drained.
    async fn begin_flush(&mut self, job: JobId) -> Result<JobWork> {
        let needs_freeze = self
            .state
            .as_ref()
            .is_some_and(|state| state.frozen.is_empty() && state.active.has_ops());
        if needs_freeze {
            self.freeze().await?;
        }
        let Some(state) = self.state.as_ref() else {
            return Err(self.handle.unavailable());
        };
        let Some(memtable) = state.frozen.first().cloned() else {
            return Ok(JobWork::Nothing);
        };
        let deleted = state
            .deletes
            .get(memtable.unit)
            .cloned()
            .unwrap_or_default();
        let mut dvs = Vec::new();
        for segment in state.segments.iter() {
            let current = state.deletes.get(segment.unit);
            let current_len = current.map_or(0, DeletionVector::len);
            let durable_len = self
                .manifest
                .segments
                .iter()
                .find(|entry| entry.unit == segment.unit)
                .and_then(|entry| entry.dv)
                .map_or(0, |dv| u64::from(dv.cardinality));
            // Bits are only ever added, so equal cardinality means an equal set.
            if current_len != durable_len {
                dvs.push((Arc::clone(segment), current.cloned().unwrap_or_default()));
            }
        }
        let covered_seq_no = state.visible_seq_no();
        let dir = self.handle.meta().dir.clone();
        let mut writes = Vec::with_capacity(dvs.len());
        for (segment, deletes) in dvs {
            let generation = self.next_dv_gen;
            self.next_dv_gen += 1;
            if let Some(running) = self.running_mut(job) {
                running
                    .dv_files
                    .push(dv_path(&dir, segment.unit, generation));
            }
            writes.push(DvWrite {
                segment,
                generation,
                deletes,
            });
        }
        Ok(JobWork::Flush(FlushStart {
            memtable,
            deleted,
            dvs: writes,
            covered_seq_no,
        }))
    }

    /// Capture a compaction's inputs, the segments it reserved, each with its deletion vector
    /// now (`D0`). The pipeline is drained.
    fn begin_compaction(&self, inputs: &[UnitId]) -> Result<JobWork> {
        let Some(state) = self.state.as_ref() else {
            return Err(self.handle.unavailable());
        };
        let mut captured = Vec::with_capacity(inputs.len());
        for unit in inputs {
            let segment = state
                .segments
                .iter()
                .find(|segment| segment.unit == *unit)
                .ok_or_else(|| {
                    LogPoseError::internal(format!(
                        "reserved compaction input {unit} is no longer a segment"
                    ))
                })?;
            captured.push((
                Arc::clone(segment),
                state.deletes.get(*unit).cloned().unwrap_or_default(),
            ));
        }
        if captured.is_empty() {
            return Ok(JobWork::Nothing);
        }
        Ok(JobWork::Compact(CompactStart { inputs: captured }))
    }

    /// Capture an index build's input: the segment it targets, if it is still a segment
    /// without an index sidecar (a compaction may have merged it since it was planned), and the
    /// build's cancellation flag. The pipeline is drained.
    fn begin_index(&self, job: JobId, target: Option<UnitId>) -> Result<JobWork> {
        let Some(state) = self.state.as_ref() else {
            return Err(self.handle.unavailable());
        };
        let segment = target.and_then(|target| {
            state
                .segments
                .iter()
                .find(|segment| segment.unit == target && segment.entry.index.is_none())
        });
        let cancel = self.jobs.get(&job).and_then(|entry| match &entry.phase {
            Phase::Running(running) => running.cancel.clone(),
            Phase::Waiting(_) => None,
        });
        match (segment, cancel) {
            (Some(segment), Some(cancel)) => Ok(JobWork::Index(IndexStart {
                segment: Arc::clone(segment),
                cancel,
            })),
            _ => Ok(JobWork::Nothing),
        }
    }

    /// The running state of a job of `kind` allocated `unit`: where its output goes, and an
    /// index build's cancellation flag.
    fn running(
        &self,
        kind: JobKind,
        unit: UnitId,
        target: Option<UnitId>,
        permit: Option<Permit>,
    ) -> Running {
        let dir = &self.handle.meta().dir;
        let output = match (kind, target) {
            (JobKind::Index, Some(target)) => index_path(dir, target, unit),
            _ => segment_path(dir, unit),
        };
        Running {
            unit,
            output,
            dv_files: Vec::new(),
            owns_files: true,
            permit,
            cancel: (kind == JobKind::Index).then(|| Arc::new(AtomicBool::new(false))),
        }
    }

    fn running_mut(&mut self, job: JobId) -> Option<&mut Running> {
        match self.jobs.get_mut(&job).map(|entry| &mut entry.phase) {
            Some(Phase::Running(running)) => Some(running),
            _ => None,
        }
    }

    /// Rotate the WAL so that the memtable a flush freezes ends in an older file than every
    /// later write, and a checkpoint falls on a file boundary. The pipeline is drained, so the
    /// private state equals the published one.
    async fn rotate_for_flush(&mut self) -> Result<()> {
        let checkpoint = self.checkpoint_frame()?;
        let Some(mut wal) = self.wal.take() else {
            return Err(self.handle.unavailable());
        };
        let rotated = self
            .core
            .runtime()
            .io
            .run(move || {
                let result = wal.rotate(&checkpoint);
                (wal, result)
            })
            .await;
        match rotated {
            Ok((wal, Ok(_))) => {
                self.wal = Some(wal);
                Ok(())
            }
            Ok((wal, Err(error))) => {
                if wal.failure().is_none() {
                    // The new file could not be created; the writer stays on the old one.
                    self.wal = Some(wal);
                    return Err(error.into());
                }
                // Hand over the rotation's own error: an unfenced rollback failure of the new
                // file's checkpoint group must reach the fatal handler, exactly as it does for
                // a rotation before a group.
                drop(wal);
                self.wal_failed(IoFailure::Rotate(error));
                Err(self.handle.unavailable())
            }
            Err(error) => {
                self.poison(
                    PoisonKind::Failed {
                        rollback_failed: false,
                    },
                    format!("the WAL rotation job failed: {error}"),
                );
                Err(error)
            }
        }
    }

    /// Publish job `job`'s manifest, then the `Version` over it; then release what the
    /// manifest superseded.
    ///
    /// The manifest takes the next generation from `next_manifest_gen`, which advances on every
    /// attempt. A publish that fails before the `CURRENT` rename abandons the job without a
    /// state change: its generation, unit, and DV generations are burned, and its files and
    /// partial manifest are removed right away because no durable manifest names them. A
    /// publish that fails at or after the rename poisons the collection, and nothing is
    /// removed.
    async fn commit_job(&mut self, job: JobId, commit: JobCommit) -> Result<Snapshot> {
        if let Some(error) = self.refusal() {
            return Err(error.clone_error());
        }
        let Some(entry) = self.jobs.get(&job) else {
            return Err(LogPoseError::internal(format!(
                "maintenance job {} is not active",
                job.0
            )));
        };
        let Phase::Running(running) = &entry.phase else {
            return Err(LogPoseError::internal(format!(
                "maintenance job {} has not begun",
                job.0
            )));
        };
        let (kind, unit, reserved_inputs) = (entry.kind, running.unit, entry.inputs.clone());
        let dir = self.handle.meta().dir.clone();
        let durable = Arc::clone(&self.manifest);
        let Some(state) = self.state.as_ref() else {
            return Err(self.handle.unavailable());
        };
        let schema = Arc::clone(&state.schema);
        let (manifest, install) = match commit {
            JobCommit::Flush {
                memtable,
                checkpoint_seq_no,
                segment,
                dvs,
            } => {
                let oldest = state.frozen.first();
                if kind != JobKind::Flush
                    || oldest.map(|frozen| (frozen.unit, frozen.last_seq_no))
                        != Some((memtable, checkpoint_seq_no))
                {
                    return Err(LogPoseError::internal(format!(
                        "flush of memtable {memtable} at {checkpoint_seq_no} is not the oldest \
                         frozen memtable"
                    )));
                }
                if checkpoint_seq_no < durable.checkpoint_seq_no
                    || checkpoint_seq_no >= self.next_seq_no
                {
                    return Err(LogPoseError::internal(format!(
                        "flush checkpoint {checkpoint_seq_no} is outside the log (durable \
                         checkpoint {}, next sequence number {})",
                        durable.checkpoint_seq_no, self.next_seq_no
                    )));
                }
                let dv_by_unit = dvs.iter().copied().collect::<HashMap<_, _>>();
                let mut segments = durable
                    .segments
                    .iter()
                    .cloned()
                    .map(|mut entry| {
                        if let Some(dv) = dv_by_unit.get(&entry.unit) {
                            entry.dv = Some(*dv);
                        }
                        entry
                    })
                    .collect::<Vec<_>>();
                if let Some(segment) = &segment {
                    check_job_unit(kind, unit, &segment.handle.entry)?;
                    segments.push(segment.handle.entry.clone());
                    segments.sort_by_key(|entry| entry.unit);
                }
                let superseded_dvs = durable
                    .segments
                    .iter()
                    .filter(|entry| dv_by_unit.contains_key(&entry.unit))
                    .filter_map(|entry| entry.dv.map(|dv| dv_path(&dir, entry.unit, dv.generation)))
                    .chain(
                        // A DV file written for a segment that a compaction removed since.
                        dvs.iter()
                            .filter(|(unit, _)| {
                                !durable.segments.iter().any(|entry| entry.unit == *unit)
                            })
                            .map(|(unit, dv)| dv_path(&dir, *unit, dv.generation)),
                    )
                    .collect::<Vec<_>>();
                (
                    (segments, checkpoint_seq_no),
                    Install::Flush {
                        memtable,
                        segment,
                        superseded_dvs,
                    },
                )
            }
            JobCommit::Compact { inputs, output } => {
                if kind != JobKind::Compact || inputs != reserved_inputs {
                    return Err(LogPoseError::internal(format!(
                        "compaction of {inputs:?} is not the job's reserved inputs \
                         {reserved_inputs:?}"
                    )));
                }
                if let Some(output) = &output {
                    check_job_unit(kind, unit, &output.handle.entry)?;
                }
                let present = inputs.iter().all(|unit| {
                    durable.segments.iter().any(|entry| entry.unit == *unit)
                        && state.segments.iter().any(|segment| segment.unit == *unit)
                });
                if inputs.is_empty() || !present {
                    return Err(LogPoseError::internal(
                        "compaction inputs are no longer in the manifest",
                    ));
                }
                // Reconcile: every deletion that reached an input while the job ran lands on
                // the output row it was copied to. The maps are injective and the writer
                // handles no write until the new version is published.
                let mut reconciled = DeletionVector::default();
                if let Some(output) = &output {
                    for (unit, map) in inputs.iter().zip(&output.maps) {
                        if let Some(deletes) = state.deletes.get(*unit) {
                            for row in deletes.iter() {
                                if let Some(&target) = map.get(row as usize)
                                    && target != u32::MAX
                                {
                                    reconciled.mark(target);
                                }
                            }
                        }
                    }
                }
                let covered_seq_no = state.visible_seq_no();
                let mut output_entry = output.as_ref().map(|output| output.handle.entry.clone());
                if let (Some(entry), false) = (&mut output_entry, reconciled.is_empty()) {
                    let generation = self.next_dv_gen;
                    self.next_dv_gen += 1;
                    let path = dv_path(&dir, entry.unit, generation);
                    if let Some(running) = self.running_mut(job) {
                        running.dv_files.push(path.clone());
                    }
                    let file = DvFile {
                        unit: entry.unit,
                        row_count: entry.row_count,
                        generation,
                        covered_seq_no,
                        bitmap: reconciled.to_bitmap(),
                    };
                    let core = self.core.clone();
                    let segments_dir = dir.join(SEGMENTS_DIR);
                    let written = self
                        .core
                        .runtime()
                        .io
                        .run(move || {
                            let vfs = core.vfs.as_ref();
                            write_dv_file(vfs, &path, &file)?;
                            vfs.sync_dir(&segments_dir).map_err(|error| {
                                LogPoseError::io(
                                    format!("failed to sync '{}'", segments_dir.display()),
                                    error,
                                )
                            })?;
                            crash_point(vfs, Some(CrashPoint::CompactionAfterDvSync))
                        })
                        .await;
                    match written {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) | Err(error) => {
                            self.abandon_job_files(job);
                            return Err(error);
                        }
                    }
                    entry.dv = Some(DvRef {
                        generation,
                        cardinality: u32::try_from(reconciled.len()).unwrap_or(u32::MAX),
                        covered_seq_no,
                    });
                }
                let mut segments = durable
                    .segments
                    .iter()
                    .filter(|entry| !inputs.contains(&entry.unit))
                    .cloned()
                    .collect::<Vec<_>>();
                if let Some(entry) = &output_entry {
                    segments.push(entry.clone());
                    segments.sort_by_key(|entry| entry.unit);
                }
                let superseded_dvs = durable
                    .segments
                    .iter()
                    .filter(|entry| inputs.contains(&entry.unit))
                    .filter_map(|entry| entry.dv.map(|dv| dv_path(&dir, entry.unit, dv.generation)))
                    .collect::<Vec<_>>();
                (
                    (segments, durable.checkpoint_seq_no),
                    Install::Compact {
                        inputs,
                        output,
                        reconciled,
                        superseded_dvs,
                    },
                )
            }
            JobCommit::Index { segment, index } => {
                if kind != JobKind::Index || index.reference.unit != unit {
                    return Err(LogPoseError::internal(format!(
                        "the index build of segment {segment} is not the job's (unit {unit})"
                    )));
                }
                let durable_entry = durable
                    .segments
                    .iter()
                    .find(|entry| entry.unit == segment && entry.index.is_none());
                let present = state.segments.iter().any(|handle| handle.unit == segment);
                let Some(durable_entry) = durable_entry.filter(|_| present) else {
                    // A compaction merged the segment away (or another build indexed it) since
                    // the build began: its sidecar is of no use, and no manifest names it.
                    self.abandon_job_files(job);
                    return Ok(self.handle.current().snapshot());
                };
                let mut entry = durable_entry.clone();
                entry.index = Some(index.reference);
                for vector in &mut entry.vectors {
                    vector.has_graph |= index.graphs.iter().any(|field| field.0 == vector.field_id);
                }
                let segments = durable
                    .segments
                    .iter()
                    .map(|current| {
                        if current.unit == segment {
                            entry.clone()
                        } else {
                            current.clone()
                        }
                    })
                    .collect::<Vec<_>>();
                (
                    (segments, durable.checkpoint_seq_no),
                    Install::Index {
                        segment,
                        file: index.file,
                        entry,
                    },
                )
            }
        };
        let (segments, checkpoint) = manifest;
        let generation = self.next_manifest_gen;
        // Orphan cleanup starts the counter above every generation on disk, so a leftover named
        // with the last generation exhausts it.
        let Some(next_manifest_gen) = generation.checked_add(1) else {
            return Err(LogPoseError::internal(
                "the collection has used every manifest generation",
            ));
        };
        self.next_manifest_gen = next_manifest_gen;
        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            collection_id: durable.collection_id.clone(),
            generation,
            epoch: durable.epoch,
            checkpoint_seq_no: checkpoint,
            schema: schema.as_ref().clone(),
            next_unit_id: self.next_unit_id,
            next_dv_gen: self.next_dv_gen,
            segments,
            totals: durable.totals,
        }
        .with_totals();

        let core = self.core.clone();
        let to_publish = manifest.clone();
        let publish_dir = dir.clone();
        let published = self
            .core
            .runtime()
            .io
            .run(move || publish_manifest(core.vfs.as_ref(), &publish_dir, &to_publish))
            .await;
        match published {
            Ok(Ok(())) => {}
            Ok(Err(failure)) => {
                if failure.current_unknown {
                    // The rename may be visible in the page cache but not on disk: nothing may
                    // be built on either manifest, and no file of either may be removed, until
                    // a reopen's durability barrier settles it.
                    self.disown_job_files(job);
                    self.poison(
                        PoisonKind::ReadOnly,
                        format!(
                            "publishing manifest {} failed: {}",
                            manifest.generation, failure.error
                        ),
                    );
                } else {
                    // `CURRENT` is unchanged, so no durable manifest names the partial manifest
                    // or the job's files: remove them before the job hears of the failure.
                    self.core.gc.remove([manifest_path(&dir, generation)]);
                    self.abandon_job_files(job);
                }
                return Err(failure.error);
            }
            Err(error) => {
                self.disown_job_files(job);
                self.poison(
                    PoisonKind::ReadOnly,
                    format!("the manifest publish job failed: {error}"),
                );
                return Err(error);
            }
        }

        // The manifest is durable: install it.
        self.disown_job_files(job);
        let superseded_generation = self.previous_generation.replace(durable.generation);
        self.manifest = Arc::new(manifest);
        let Some(state) = self.state.as_mut() else {
            return Err(self.handle.unavailable());
        };
        let (retired, superseded_dvs, checkpointed, rows, bytes) = match install {
            Install::Flush {
                memtable,
                segment,
                superseded_dvs,
            } => {
                let written = segment.as_ref().map_or((0, 0), |segment| {
                    (
                        u64::from(segment.handle.row_count()),
                        segment.handle.entry.file_len,
                    )
                });
                state.install_flush(memtable, segment);
                // Tell a WAL tailer what is safe to discard; it rides along with the next group.
                self.pending_checkpoint = checkpoint_frame(&self.manifest).ok();
                (
                    Vec::new(),
                    superseded_dvs,
                    Some(checkpoint),
                    written.0,
                    written.1,
                )
            }
            Install::Compact {
                inputs,
                output,
                reconciled,
                superseded_dvs,
            } => {
                let written = output.as_ref().map_or((0, 0), |output| {
                    (
                        u64::from(output.handle.row_count()),
                        output.handle.entry.file_len,
                    )
                });
                let retired = state.install_compaction(&inputs, output, reconciled);
                (retired, superseded_dvs, None, written.0, written.1)
            }
            Install::Index {
                segment,
                file,
                entry,
            } => {
                let bytes = entry.index.map_or(0, |index| index.file_len);
                state.install_index(segment, file, entry);
                (Vec::new(), Vec::new(), None, 0, bytes)
            }
        };
        self.handle.record_written(kind, rows, bytes);
        let Some(version) = self.candidate() else {
            return Err(self.handle.unavailable());
        };
        let version = self.handle.publish(version);
        // Older versions still hold the retired segments until readers and tokens let go.
        drop(retired);
        self.core.gc.remove(superseded_dvs);
        if let Some(superseded) = superseded_generation {
            self.core.gc.remove([manifest_path(&dir, superseded)]);
        }
        if let Some(checkpoint) = checkpointed {
            self.remove_checkpointed_wal(checkpoint).await;
        }
        self.after_publish();
        Ok(version.snapshot())
    }

    /// Job `job`'s files are no longer its to remove.
    fn disown_job_files(&mut self, job: JobId) {
        if let Some(running) = self.running_mut(job) {
            running.owns_files = false;
        }
    }

    /// Remove every file job `job` may have written: no durable manifest names them.
    fn abandon_job_files(&mut self, job: JobId) {
        let files = match self.running_mut(job) {
            Some(running) if running.owns_files => {
                running.owns_files = false;
                running.files()
            }
            _ => return,
        };
        self.core.gc.remove(files);
    }

    /// Delete the WAL files a durable manifest with checkpoint `checkpoint` made obsolete, on
    /// the I/O pool. Only after that manifest is durable: every operation in them is in a
    /// segment or a DV file. A failure only delays the removal to the next checkpoint or open.
    async fn remove_checkpointed_wal(&mut self, checkpoint: SeqNo) {
        let Some(mut wal) = self.wal.take() else {
            return;
        };
        let removed = self
            .core
            .runtime()
            .io
            .run(move || {
                let result = wal.remove_checkpointed(checkpoint);
                (wal, result)
            })
            .await;
        match removed {
            Ok((wal, result)) => {
                self.wal = Some(wal);
                if let Err(error) = result {
                    tracing::warn!(
                        collection = %self.handle.descriptor().lookup_name(),
                        %error,
                        "failed to remove checkpointed WAL files; the next checkpoint retries"
                    );
                }
            }
            Err(error) => self.poison(
                PoisonKind::Failed {
                    rollback_failed: false,
                },
                format!("the WAL cleanup job failed: {error}"),
            ),
        }
    }

    /// Job `job` is over: release its permit and reservations, remove what it wrote if no
    /// manifest names it, publish the maintenance status without it, settle the requests it
    /// answers (`reply`, a hand-stepped commit's, among them), and plan again.
    ///
    /// The status goes out before any answer, so a caller whose flush, compaction, or commit
    /// returned never reads a status that still shows the job pending or running.
    async fn end_job(
        &mut self,
        job: JobId,
        outcome: Outcome,
        wrote_files: bool,
        reply: Option<SnapshotReply>,
    ) {
        let Some(entry) = self.jobs.remove(&job) else {
            if let Some(reply) = reply {
                let _ = reply.send(self.answer(&outcome));
            }
            return;
        };
        for unit in &entry.inputs {
            self.reserved.remove(unit);
        }
        let mut output = None;
        if let Phase::Running(running) = entry.phase {
            if running.owns_files && wrote_files {
                self.core.gc.remove(running.files());
            }
            output = Some(running.unit);
            // Dropping the permit releases its slot and memory to the next waiting job.
            drop(running.permit);
        }
        match &outcome {
            Outcome::Committed(_) | Outcome::Nothing => self.job_succeeded(entry.kind),
            Outcome::Failed(error) => self.job_failed(entry.kind, error),
            Outcome::Abandoned => {}
        }
        // The explicit compaction follows its segments into the outputs they are merged into,
        // and ends with the first failure of one of its steps.
        let mut explicit_failed = Vec::new();
        if let Some(explicit) = self.explicit.as_mut() {
            if let (Outcome::Committed(_), JobKind::Compact) = (&outcome, entry.kind)
                && entry
                    .inputs
                    .iter()
                    .any(|unit| explicit.scope.contains(unit))
            {
                for unit in &entry.inputs {
                    explicit.scope.remove(unit);
                }
                explicit.scope.extend(output);
            }
            if let (Outcome::Failed(_), true) = (&outcome, entry.explicit) {
                explicit_failed = std::mem::take(&mut explicit.waiters);
                self.explicit = None;
            }
        }
        self.update_status();
        for reply in reply.into_iter().chain(explicit_failed) {
            let _ = reply.send(self.answer(&outcome));
        }
        if entry.kind == JobKind::Flush {
            self.settle_flush_waiters(&outcome);
        }
        if self.running_jobs() == 0 {
            for waiter in self.quiesce_waiters.drain(..) {
                let _ = waiter.try_send(());
            }
        }
        // A hand-stepped flush waited for this one.
        if entry.kind == JobKind::Flush
            && !self.jobs.values().any(|job| job.kind == JobKind::Flush)
            && !self.manual_flushes.is_empty()
        {
            let reply = self.manual_flushes.remove(0);
            Box::pin(self.begin_by_hand(JobKind::Flush, reply)).await;
        }
        if self.freeze_due() {
            self.freeze_pending = true;
        }
        self.schedule();
    }

    /// What a request that job answers gets for `outcome`.
    fn answer(&self, outcome: &Outcome) -> Result<Snapshot> {
        match outcome {
            Outcome::Committed(snapshot) => Ok(snapshot.clone()),
            Outcome::Failed(error) => Err(error.clone()),
            Outcome::Nothing | Outcome::Abandoned => Ok(self.handle.current().snapshot()),
        }
    }

    /// A job of `kind` completed: reset the kind's backoff, count the run, and clear the last
    /// error if a job of this kind reported it.
    fn job_succeeded(&mut self, kind: JobKind) {
        match kind {
            JobKind::Flush => self.flush_retry.succeeded(),
            JobKind::Compact => self.compaction_retry.succeeded(),
            JobKind::Index => self.index_retry.succeeded(),
        }
        self.handle.update_maintenance_status(|status| {
            status.completed_runs += 1;
            if status
                .last_error
                .as_ref()
                .is_some_and(|error| error.job == kind.label())
            {
                status.last_error = None;
            }
        });
    }

    /// A job of `kind` (or the freeze before a flush) failed with `error`: back off the kind's
    /// background jobs, report the failure, and poison the collection once flushes keep
    /// failing (`max_flush_failures` in a row) or one failed in a way no retry can fix.
    ///
    /// A job that failed because the writer refuses work (the collection was poisoned or
    /// dropped, or the engine is shutting down) is no maintenance failure: its error is the
    /// refusal, and the failure behind it stays the one reported.
    fn job_failed(&mut self, kind: JobKind, error: &LogPoseError) {
        if self.refusal().is_some()
            && matches!(
                error,
                LogPoseError::CollectionPoisoned { .. }
                    | LogPoseError::NotFound { .. }
                    | LogPoseError::Unavailable { .. }
            )
        {
            return;
        }
        let now = self.clock_now();
        let failures = match kind {
            JobKind::Flush => self.flush_retry.failed(now),
            JobKind::Compact => self.compaction_retry.failed(now),
            JobKind::Index => self.index_retry.failed(now),
        };
        tracing::warn!(
            collection = %self.handle.descriptor().lookup_name(),
            ?kind,
            failures,
            %error,
            "a maintenance job failed"
        );
        let reported = MaintenanceError {
            job: kind.label().to_owned(),
            message: error.to_string(),
            failed_at_unix_ms: unix_ms_now(),
            consecutive_failures: failures,
        };
        self.handle.update_maintenance_status(|status| {
            status.last_error = Some(reported);
        });
        if kind != JobKind::Flush || self.refusal().is_some() {
            return;
        }
        let limit = self.core.memtable.max_flush_failures.max(1);
        let reason = if lasting_failure(error) {
            format!("a flush failed and cannot succeed on a retry: {error}")
        } else if failures >= limit {
            format!("{failures} flushes in a row failed, the last with: {error}")
        } else {
            return;
        };
        self.poison(PoisonKind::ReadOnly, reason);
    }

    /// After a flush ended: answer the explicit flushes the checkpoint now covers, or fail them
    /// all if the flush failed.
    fn settle_flush_waiters(&mut self, outcome: &Outcome) {
        if let Outcome::Failed(error) = outcome {
            for (_, reply) in self.flush_waiters.drain(..) {
                if let Some(reply) = reply {
                    let _ = reply.send(Err(error.clone()));
                }
            }
            return;
        }
        let checkpoint = self.manifest.checkpoint_seq_no;
        let snapshot = self.handle.current().snapshot();
        let mut waiting = Vec::new();
        for (target, reply) in self.flush_waiters.drain(..) {
            if target <= checkpoint {
                if let Some(reply) = reply {
                    let _ = reply.send(Ok(snapshot.clone()));
                }
            } else {
                waiting.push((target, reply));
            }
        }
        self.flush_waiters = waiting;
    }

    /// Fail every explicit request and cancel every job that has not begun: the collection was
    /// dropped or poisoned.
    pub(super) fn fail_waiters(&mut self) {
        self.cancel_waiting_jobs();
        self.cancel_running_indexes();
        let error = self.handle.unavailable();
        self.fail_explicit_requests(&error);
    }

    /// Answer every explicit flush and compaction still waiting with `error`.
    fn fail_explicit_requests(&mut self, error: &LogPoseError) {
        for (_, reply) in std::mem::take(&mut self.flush_waiters) {
            if let Some(reply) = reply {
                let _ = reply.send(Err(error.clone()));
            }
        }
        if let Some(explicit) = self.explicit.take() {
            for reply in explicit.waiters {
                let _ = reply.send(Err(error.clone()));
            }
        }
        for reply in std::mem::take(&mut self.manual_flushes) {
            let _ = reply.send(Err(error.clone()));
        }
    }

    /// Fail everything still queued and stop.
    ///
    /// Both channels are closed and then drained with `recv`, which after a close also waits
    /// for a send that already passed the channel's open check but has not queued its message
    /// yet. Dropping a receiver, or draining it with `try_recv`, misses such a message: it then
    /// sits in the channel with its reply sender alive, and its caller (a client write, an
    /// explicit flush or compaction, a job a test steps by hand, a quiescing drop) waits
    /// forever, which hangs engine shutdown. Jobs waiting for a permit are withdrawn first, so
    /// no grant is delivered to the closed channel; a permit already queued is dropped with its
    /// message, which releases it.
    pub(super) async fn stop(&mut self) {
        self.requests.close();
        self.control.close();
        self.cancel_waiting_jobs();
        self.cancel_running_indexes();
        if let Some(held) = self.held.take() {
            let _ = held.request.ack().send(Err(shutting_down()));
        }
        while let Some(queued) = self.requests.recv().await {
            let _ = queued.request.ack().send(Err(shutting_down()));
        }
        while let Some(message) = self.control.recv().await {
            match message {
                ControlMsg::JobDone { reply, .. } => {
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(shutting_down()));
                    }
                }
                ControlMsg::Flush { reply } => {
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(shutting_down()));
                    }
                }
                ControlMsg::Compact { reply } => {
                    let _ = reply.send(Err(shutting_down()));
                }
                ControlMsg::BeginJob { reply, .. } => {
                    let _ = reply.send(Err(shutting_down()));
                }
                ControlMsg::Quiesce { reply } => self.quiesce_waiters.push(reply),
                // A tick that never ran: dropping its reply tells the caller the writer stopped.
                ControlMsg::PermitGranted { .. }
                | ControlMsg::EndJob { .. }
                | ControlMsg::Tick { .. }
                | ControlMsg::Shutdown => {}
            }
        }
        self.fail_explicit_requests(&shutting_down());
        for waiter in self.quiesce_waiters.drain(..) {
            let _ = waiter.try_send(());
        }
        self.wal = None;
    }
}
