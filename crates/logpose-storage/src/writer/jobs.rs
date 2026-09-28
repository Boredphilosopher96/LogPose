//! Maintenance at the writer: freezing memtables at their flush triggers, the write stall,
//! asking the scheduler for permits, beginning a job on its permit, committing it, and explicit
//! flushes and compactions.
//!
//! A job's life:
//!
//! 1. **Plan.** A frozen memtable is due a flush; the compaction policy picks a set of
//!    unreserved segments (and reserves them). The writer asks the scheduler for a permit
//!    (`Phase::Waiting`).
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

use super::*;
use crate::{
    compaction::{Candidate, RowShape},
    dv::{DvFile, dv_path, write_dv_file},
    fs_util::crash_point,
    handle::JobTicket,
    manifest::{DvRef, MANIFEST_FORMAT_VERSION, manifest_path, publish_manifest},
    paths::SEGMENTS_DIR,
};
use logpose_vfs::CrashPoint;
use std::collections::HashMap;

/// How long background maintenance waits after a failed job before it plans again.
pub(crate) const RETRY_BACKOFF: Duration = Duration::from_secs(1);

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

    /// Whether background maintenance may plan now: the collection had a data-plane access
    /// since it was opened, and no job failed within the last backoff.
    fn background_allowed(&self) -> bool {
        self.handle.maintenance_armed() && self.retry_at.is_none_or(|at| self.clock_now() >= at)
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
        waited || (self.background_allowed() && self.flush_triggered())
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
                self.retry_at = Some(self.clock_now() + RETRY_BACKOFF);
                for (_, reply) in self.flush_waiters.drain(..) {
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

    /// Plan what is due: a flush of the oldest frozen memtable, then compactions.
    pub(super) fn schedule(&mut self) {
        if self.refusal().is_none() && self.state.is_some() {
            self.request_flush();
            self.schedule_compactions();
        }
        self.update_status();
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
        if !self.background_allowed() && self.flush_waiters.is_empty() {
            return;
        }
        if let Err(error) = self.request_job(JobKind::Flush, 0, Vec::new()) {
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

    /// Plan compactions: an explicit one once no background compaction runs, otherwise what
    /// the size-tiered policy picks for the free job slots.
    fn schedule_compactions(&mut self) {
        let compactions = self
            .jobs
            .values()
            .filter(|job| job.kind == JobKind::Compact)
            .count();
        if !self.compact_waiters.is_empty() {
            if compactions > 0 {
                // Background compactions finish first; the explicit one then takes every
                // segment it can.
                return;
            }
            let plan = self
                .policy()
                .plan_explicit(&self.candidates(), &self.reserved);
            let Some(plan) = plan else {
                let snapshot = self.handle.current().snapshot();
                for reply in self.compact_waiters.drain(..) {
                    let _ = reply.send(Ok(snapshot.clone()));
                }
                return;
            };
            // The job answers the requests made so far; later ones wait for the next job.
            let waiters = std::mem::take(&mut self.compact_waiters);
            match self.request_job(JobKind::Compact, plan.build_bytes, plan.inputs) {
                Ok(job) => {
                    if let Some(entry) = self.jobs.get_mut(&job) {
                        entry.waiters = waiters;
                    }
                }
                Err(error) => {
                    for reply in waiters {
                        let _ = reply.send(Err(error.clone()));
                    }
                }
            }
            return;
        }
        if !self.background_allowed() {
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
        let plans = self
            .policy()
            .plan(&self.candidates(), &self.reserved, slots);
        for plan in plans {
            tracing::debug!(
                collection = %self.handle.descriptor().lookup_name(),
                inputs = ?plan.inputs,
                reason = ?plan.reason,
                build_bytes = plan.build_bytes,
                "planned a compaction"
            );
            if let Err(error) = self.request_job(JobKind::Compact, plan.build_bytes, plan.inputs) {
                tracing::warn!(%error, "the scheduler declined a compaction");
                self.retry_at = Some(self.clock_now() + RETRY_BACKOFF);
                break;
            }
        }
    }

    /// Ask the scheduler for a permit for a new job; the grant arrives as `PermitGranted`.
    fn request_job(&mut self, kind: JobKind, bytes: u64, inputs: Vec<UnitId>) -> Result<JobId> {
        let job = JobId(self.next_job);
        self.next_job += 1;
        let control = self.handle.control_sender();
        let request = self.core.scheduler.request(kind, bytes, move |permit| {
            // A writer that is gone drops the permit, which releases it.
            let _ = control.send(ControlMsg::PermitGranted { job, permit });
        })?;
        self.reserved.extend(inputs.iter().copied());
        self.jobs.insert(
            job,
            Job {
                kind,
                inputs,
                waiters: Vec::new(),
                phase: Phase::Waiting(request),
            },
        );
        Ok(job)
    }

    /// Withdraw every job still waiting for a permit. The explicit compactions they would have
    /// answered wait for the next one. A permit already on its way is dropped when it arrives
    /// for a job that no longer exists.
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
                self.compact_waiters.extend(entry.waiters);
            }
        }
        self.update_status();
    }

    /// Refresh the maintenance status the handle reports.
    pub(super) fn update_status(&self) {
        let mut pending = Vec::new();
        let mut flushing = false;
        let mut compacting = false;
        for job in self.jobs.values() {
            match (&job.phase, job.kind) {
                (Phase::Waiting(_), kind) => pending.push(kind.label().to_owned()),
                (Phase::Running(_), JobKind::Flush) => flushing = true,
                (Phase::Running(_), JobKind::Compact) => compacting = true,
            }
        }
        let in_progress = if flushing || !self.manual_flushes.is_empty() {
            Some(JobKind::Flush.label().to_owned())
        } else if compacting {
            Some(JobKind::Compact.label().to_owned())
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
                let outcome = match result {
                    Ok(commit) => match self.commit_job(job, commit).await {
                        Ok(snapshot) => Outcome::Committed(snapshot),
                        Err(error) => Outcome::Failed(error),
                    },
                    Err(error) => Outcome::Failed(error),
                };
                if let Some(reply) = reply {
                    let _ = reply.send(match &outcome {
                        Outcome::Committed(snapshot) => Ok(snapshot.clone()),
                        Outcome::Failed(error) => Err(error.clone()),
                        Outcome::Nothing | Outcome::Abandoned => {
                            Ok(self.handle.current().snapshot())
                        }
                    });
                }
                self.end_job(job, outcome, wrote_files).await;
            }
            ControlMsg::EndJob { job, wrote_files } => {
                self.end_job(job, Outcome::Abandoned, wrote_files).await;
            }
            ControlMsg::Flush { reply } => self.explicit_flush(reply).await,
            ControlMsg::Compact { reply } => {
                if let Some(refusal) = self.refusal() {
                    let _ = reply.send(Err(refusal.clone_error()));
                } else {
                    self.handle.arm_maintenance();
                    self.compact_waiters.push(reply);
                    self.schedule();
                }
            }
            ControlMsg::BeginJob { kind, reply } => self.begin_by_hand(kind, reply).await,
            ControlMsg::Quiesce { reply } => {
                // A drop voids every job that has not begun. Should the drop not commit, the
                // next tick plans them again.
                self.cancel_waiting_jobs();
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
            self.end_job(job, Outcome::Failed(refusal.clone_error()), false)
                .await;
            return;
        }
        let unit = match self.allocate_unit() {
            Ok(unit) => unit,
            Err(error) => {
                drop(permit);
                self.end_job(job, Outcome::Failed(error), false).await;
                return;
            }
        };
        if let Some(entry) = self.jobs.get_mut(&job) {
            entry.phase = Phase::Running(Running {
                unit,
                dv_files: Vec::new(),
                owns_files: true,
                permit: Some(permit),
            });
        }
        self.update_status();
        let work = match kind {
            JobKind::Flush => self.begin_flush(job).await,
            JobKind::Compact => self.begin_compaction(&inputs),
        };
        let work = match work {
            Ok(JobWork::Nothing) => {
                self.end_job(job, Outcome::Nothing, false).await;
                return;
            }
            Ok(work) => work,
            Err(error) => {
                self.end_job(job, Outcome::Failed(error), false).await;
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
                    JobWork::Nothing => Err(LogPoseError::internal("a job with nothing to do")),
                }
            };
            // Release the inputs (and the version they came from) before the commit: once it
            // publishes, the last holder of a retired unit removes its files.
            drop(start);
            ticket.done(result);
        });
        if let Err(error) = spawned {
            self.end_job(job, Outcome::Failed(error), false).await;
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
        self.reserved.extend(inputs.iter().copied());
        self.jobs.insert(
            job,
            Job {
                kind,
                inputs: inputs.clone(),
                waiters: Vec::new(),
                phase: Phase::Running(Running {
                    unit,
                    dv_files: Vec::new(),
                    owns_files: true,
                    permit: None,
                }),
            },
        );
        self.update_status();
        let work = match kind {
            JobKind::Flush => self.begin_flush(job).await,
            JobKind::Compact if inputs.is_empty() => Ok(JobWork::Nothing),
            JobKind::Compact => self.begin_compaction(&inputs),
        };
        let work = match work {
            Ok(work) => work,
            Err(error) => {
                let _ = reply.send(Err(error.clone()));
                self.end_job(job, Outcome::Failed(error), false).await;
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
        if reply.send(Ok(start)).is_err() || nothing {
            // Gone before it wrote anything, or nothing to do.
            self.end_job(job, Outcome::Abandoned, false).await;
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
        let dir = self.handle.meta().dir.clone();
        let files = match self.running_mut(job) {
            Some(running) if running.owns_files => {
                running.owns_files = false;
                running.files(&dir)
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
    /// manifest names it, settle the explicit requests it answers, and plan again.
    async fn end_job(&mut self, job: JobId, outcome: Outcome, wrote_files: bool) {
        let Some(entry) = self.jobs.remove(&job) else {
            return;
        };
        for unit in &entry.inputs {
            self.reserved.remove(unit);
        }
        if let Phase::Running(running) = entry.phase {
            if running.owns_files && wrote_files {
                self.core.gc.remove(running.files(&self.handle.meta().dir));
            }
            // Dropping the permit releases its slot and memory to the next waiting job.
            drop(running.permit);
        }
        match &outcome {
            Outcome::Committed(_) | Outcome::Nothing => {
                self.handle.update_maintenance_status(|status| {
                    status.completed_runs += 1;
                    status.last_error = None;
                });
            }
            Outcome::Failed(error) => {
                tracing::warn!(
                    collection = %self.handle.descriptor().lookup_name(),
                    kind = ?entry.kind,
                    %error,
                    "a maintenance job failed"
                );
                self.retry_at = Some(self.clock_now() + RETRY_BACKOFF);
                let message = error.to_string();
                self.handle.update_maintenance_status(|status| {
                    status.last_error = Some(message);
                });
            }
            Outcome::Abandoned => {}
        }
        match entry.kind {
            JobKind::Flush => self.settle_flush_waiters(&outcome),
            JobKind::Compact => {
                for reply in entry.waiters {
                    let _ = reply.send(match &outcome {
                        Outcome::Committed(snapshot) => Ok(snapshot.clone()),
                        Outcome::Failed(error) => Err(error.clone()),
                        Outcome::Nothing | Outcome::Abandoned => {
                            Ok(self.handle.current().snapshot())
                        }
                    });
                }
            }
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
        for reply in std::mem::take(&mut self.compact_waiters) {
            let _ = reply.send(Err(error.clone()));
        }
        for reply in std::mem::take(&mut self.manual_flushes) {
            let _ = reply.send(Err(error.clone()));
        }
    }

    /// Fail everything still queued and stop.
    pub(super) fn stop(&mut self) {
        self.requests.close();
        if let Some(held) = self.held.take() {
            let _ = held.request.ack().send(Err(shutting_down()));
        }
        while let Ok(queued) = self.requests.try_recv() {
            let _ = queued.request.ack().send(Err(shutting_down()));
        }
        self.cancel_waiting_jobs();
        for job in self.jobs.values_mut() {
            self.compact_waiters.append(&mut job.waiters);
        }
        self.fail_explicit_requests(&shutting_down());
        for waiter in self.quiesce_waiters.drain(..) {
            let _ = waiter.try_send(());
        }
        self.wal = None;
    }
}
