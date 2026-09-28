//! The action table and its executor: every action runs against the engine and the model, and
//! every outcome is checked against what the model predicts.
//!
//! | Action | Model effect | Check |
//! | --- | --- | --- |
//! | `Write` (upserts, partial updates, deletes) | apply the batch | ack range; every key read back (I1); `NotFound` for an update without a live row |
//! | `DeleteByFilter`, `UpdateByFilter` | evaluate the filter | affected count and ack range |
//! | `Alter` (add, drop, rename) | change the schema | reads use the new names, null for added fields, and shadowing |
//! | `Read` at the current state or `ReadAt` a token | none | get, count, full scan, order by, search: exact equality (searches over indexed segments: soundness) |
//! | `ScrollStart`, `ScrollNext` | none | the pages of one scroll concatenate to the model at its start (I4, I12) |
//! | `Pin`, `Release`, `AdvanceClock` | a model clone per token | token reads equal their clone (I12); released and expired tokens fail |
//! | `Flush`, `Compact`, `BeginJob`, `BuildJob`, `CommitJob`, `AbandonJob`, `StepScheduler` | none | full-state equality afterwards |
//! | `Crash` (during any action, in any tear mode, optionally again in recovery) | the durable prefix | I2, I3, I8, and recovery idempotence (I11) |
//! | `FailSync` (a file or directory sync during any action) | an unacknowledged batch may be absent | nothing new visible; the collection is read-only; reopen agrees |
//! | `Reopen` (in process, same boot) | none | the durability barrier settles an unknown outcome; later crashes agree |
//!
//! After every action the harness checks the published `Version`'s invariants (I5, I10, I13)
//! and the whole visible state: sequence number, schema, live counter, a full scan, and a point
//! lookup of every key.

use crate::{
    model::{Model, Refusal, metric_value},
    session::{Maintenance, Session, TTL},
};
use logpose_query::{
    Cursor, FilterExpr, ScrollOrder, ScrollRequest, SearchRequest, count_view, scroll, scroll_view,
    search,
};
use logpose_storage::{
    JobKind, Projection, ReadOptions, ReadView, RowData, SchemaChange, SnapshotToken, SteppedJob,
    Version, read::Direction,
};
use logpose_types::SeqNo;
use logpose_types::{
    LogPoseError, ResourceKind, WriteOutcome,
    record::{ClientOp, PartialUpdate, PrimaryKey, Record},
};
use logpose_vfs::{FaultPlan, TearMode};
use std::{fmt, time::Duration};

/// A read, checked against a model.
#[derive(Clone, Debug)]
pub enum Read {
    Get(Vec<PrimaryKey>),
    Count(Option<FilterExpr>),
    /// Every live row by key, `limit` rows per page of one view.
    Scan {
        limit: u32,
    },
    /// Every matching row by a declared field, `limit` rows per page of one view.
    OrderBy {
        field: String,
        descending: bool,
        filter: Option<FilterExpr>,
        limit: u32,
    },
    Search {
        query: Vec<f32>,
        k: usize,
        filter: Option<FilterExpr>,
    },
}

/// One step of a run. Every parameter is concrete, so a recorded list of actions replays.
#[derive(Clone)]
pub enum Action {
    Write(Vec<ClientOp>),
    DeleteByFilter(FilterExpr),
    UpdateByFilter(FilterExpr, PartialUpdate),
    Alter(SchemaChange),
    /// An explicit flush of everything visible.
    Flush,
    /// An explicit compaction.
    Compact,
    BeginJob(JobKind),
    BuildJob(JobKind),
    CommitJob(JobKind),
    AbandonJob(JobKind),
    /// Grant one waiting background job its permit and wait for it.
    StepScheduler,
    Pin,
    Release(u64),
    AdvanceClock(Duration),
    Read(Read),
    ReadAt(u64, Read),
    ScrollStart {
        id: u64,
        by: Option<(String, bool)>,
        filter: Option<FilterExpr>,
        limit: u32,
    },
    ScrollNext(u64),
    /// Lose power before the `after_ops`-th mutating operation from now while `during` runs
    /// (or after it, if it does fewer), tearing unsynced data per `tear`; optionally lose power
    /// again after `recovery.0` operations of the recovery that follows; then recover.
    Crash {
        during: Option<Box<Action>>,
        after_ops: u64,
        tear: TearMode,
        recovery: Option<(u64, TearMode)>,
    },
    /// Fail the `nth` file sync (or directory sync) from now while `during` runs.
    FailSync {
        during: Box<Action>,
        nth: u64,
        dir: bool,
    },
    /// Drop the engine and open it again on the same boot.
    Reopen,
}

impl fmt::Debug for Action {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write(ops) => {
                write!(formatter, "Write[")?;
                for (index, op) in ops.iter().enumerate() {
                    if index > 0 {
                        write!(formatter, ", ")?;
                    }
                    match op {
                        ClientOp::Upsert(record) => write!(
                            formatter,
                            "upsert {} {:?} {:?} {}",
                            record.pk,
                            record.vectors.values().next(),
                            record.fields,
                            serde_json::Value::Object(record.extra.clone())
                        )?,
                        ClientOp::Update(update) => write!(
                            formatter,
                            "update {} {:?} {:?} {}",
                            update.pk,
                            update.vectors.values().next(),
                            update.fields,
                            serde_json::Value::Object(update.extra.clone())
                        )?,
                        ClientOp::Delete(pk) => write!(formatter, "delete {pk}")?,
                    }
                }
                write!(formatter, "]")
            }
            Self::DeleteByFilter(filter) => write!(formatter, "DeleteByFilter({filter:?})"),
            Self::UpdateByFilter(filter, patch) => write!(
                formatter,
                "UpdateByFilter({filter:?}, {:?} {})",
                patch.fields,
                serde_json::Value::Object(patch.extra.clone())
            ),
            Self::Alter(change) => write!(formatter, "Alter({change:?})"),
            Self::Flush => write!(formatter, "Flush"),
            Self::Compact => write!(formatter, "Compact"),
            Self::BeginJob(kind) => write!(formatter, "BeginJob({kind:?})"),
            Self::BuildJob(kind) => write!(formatter, "BuildJob({kind:?})"),
            Self::CommitJob(kind) => write!(formatter, "CommitJob({kind:?})"),
            Self::AbandonJob(kind) => write!(formatter, "AbandonJob({kind:?})"),
            Self::StepScheduler => write!(formatter, "StepScheduler"),
            Self::Pin => write!(formatter, "Pin"),
            Self::Release(id) => write!(formatter, "Release({id})"),
            Self::AdvanceClock(by) => write!(formatter, "AdvanceClock({}s)", by.as_secs()),
            Self::Read(read) => write!(formatter, "Read({read:?})"),
            Self::ReadAt(id, read) => write!(formatter, "ReadAt({id}, {read:?})"),
            Self::ScrollStart {
                id,
                by,
                filter,
                limit,
            } => write!(
                formatter,
                "ScrollStart({id}, by {by:?}, {filter:?}, limit {limit})"
            ),
            Self::ScrollNext(id) => write!(formatter, "ScrollNext({id})"),
            Self::Crash {
                during,
                after_ops,
                tear,
                recovery,
            } => write!(
                formatter,
                "Crash(after {after_ops} ops, {tear:?}, recovery {recovery:?}, during {during:?})"
            ),
            Self::FailSync { during, nth, dir } => write!(
                formatter,
                "FailSync({} sync {nth}, during {during:?})",
                if *dir { "dir" } else { "file" }
            ),
            Self::Reopen => write!(formatter, "Reopen"),
        }
    }
}

/// A pinned snapshot token and the model it must read.
struct Pinned {
    id: u64,
    token: SnapshotToken,
    model: Model,
    expires: Duration,
    /// Released by the harness, or lost with a restart: reads must fail.
    dead: bool,
    /// A view of the pinned version while the token lives, whose segment files must exist
    /// (I7): a file is deleted only once no live version references it.
    view: Option<ReadView>,
}

/// An open scroll.
struct Scroll {
    id: u64,
    order: ScrollOrder,
    filter: Option<FilterExpr>,
    limit: u32,
    /// The keys the whole scroll must return, in order, and the model they are read from.
    expected: Vec<PrimaryKey>,
    model: Model,
    returned: usize,
    cursor: Option<Cursor>,
    expires: Duration,
    /// Lost with a restart.
    dead: bool,
}

/// Counts of what a run did, so tests can assert that the interesting cases ran.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub actions: u64,
    pub writes_acked: u64,
    pub writes_refused: u64,
    pub filter_writes: u64,
    pub alters: u64,
    pub jobs_committed: u64,
    pub steps_granted: u64,
    pub crashes: u64,
    pub recovery_crashes: u64,
    pub unacked_recovered: u64,
    pub unacked_lost: u64,
    pub failed_syncs: u64,
    pub poisoned: u64,
    pub reopens: u64,
    pub token_reads: u64,
    pub expired_reads: u64,
    pub scrolls_finished: u64,
    pub searches: u64,
    pub explicit_compactions: u64,
    /// Scheduler permits granted over the run (background jobs and explicit requests).
    pub flushes_granted: u64,
    pub compactions_granted: u64,
    /// Segment files checked to exist for a live version (I7).
    pub files_checked: u64,
    /// The most maintenance jobs seen running at once after an action.
    pub peak_jobs: usize,
}

impl Stats {
    pub fn add(&mut self, other: &Self) {
        self.actions += other.actions;
        self.writes_acked += other.writes_acked;
        self.writes_refused += other.writes_refused;
        self.filter_writes += other.filter_writes;
        self.alters += other.alters;
        self.jobs_committed += other.jobs_committed;
        self.steps_granted += other.steps_granted;
        self.crashes += other.crashes;
        self.recovery_crashes += other.recovery_crashes;
        self.unacked_recovered += other.unacked_recovered;
        self.unacked_lost += other.unacked_lost;
        self.failed_syncs += other.failed_syncs;
        self.poisoned += other.poisoned;
        self.reopens += other.reopens;
        self.token_reads += other.token_reads;
        self.expired_reads += other.expired_reads;
        self.scrolls_finished += other.scrolls_finished;
        self.searches += other.searches;
        self.explicit_compactions += other.explicit_compactions;
        self.flushes_granted += other.flushes_granted;
        self.compactions_granted += other.compactions_granted;
        self.files_checked += other.files_checked;
        self.peak_jobs = self.peak_jobs.max(other.peak_jobs);
    }
}

/// The engine, the model, and the state of open tokens, scrolls, and jobs.
pub struct Runner {
    pub session: Session,
    pub model: Model,
    /// A second state recovery may produce: the model after a write whose outcome is unknown.
    pub pending: Option<Model>,
    /// The collection refused further writes after a failure.
    pub poisoned: bool,
    /// A failure was injected since the last restart.
    faulted: bool,
    jobs: Vec<SteppedJob>,
    tokens: Vec<Pinned>,
    scrolls: Vec<Scroll>,
    pub next_id: u64,
    pub stats: Stats,
    /// The highest manifest generation and checkpoint published so far, across restarts:
    /// neither ever goes back, since a manifest is durable before its version is published.
    generation: u64,
    checkpoint: SeqNo,
}

impl Drop for Runner {
    fn drop(&mut self) {
        // A stepped job or a view holds the engine open; end them before the session drops the
        // engine.
        self.jobs.clear();
        self.drop_views();
    }
}

type Check = Result<(), String>;

fn fail<T>(message: impl Into<String>) -> Result<T, String> {
    Err(message.into())
}

impl Runner {
    pub fn new(session: Session) -> Self {
        let model = Self::new_model(&session);
        Self {
            session,
            model,
            pending: None,
            poisoned: false,
            faulted: false,
            jobs: Vec::new(),
            tokens: Vec::new(),
            scrolls: Vec::new(),
            next_id: 1,
            stats: Stats::default(),
            generation: 0,
            checkpoint: 0,
        }
    }

    /// The run's stats, with the scheduler permits granted so far.
    pub fn stats(&self) -> Stats {
        let (flushes, compactions) = self.session.permits_granted();
        Stats {
            flushes_granted: flushes,
            compactions_granted: compactions,
            ..self.stats
        }
    }

    /// Drop every held view (before the engine closes).
    fn drop_views(&mut self) {
        for pinned in &mut self.tokens {
            pinned.view = None;
        }
    }

    /// Drop the views of tokens that died or expired: the harness must not keep a version
    /// alive longer than the engine's own pins would.
    fn drop_dead_views(&mut self) {
        let now = self.session.clock_now();
        for pinned in &mut self.tokens {
            if pinned.dead || now >= pinned.expires {
                pinned.view = None;
            }
        }
    }

    pub fn open_jobs(&self) -> Vec<(JobKind, bool)> {
        self.jobs
            .iter()
            .map(|job| (job.kind(), job.is_built()))
            .collect()
    }

    pub fn live_tokens(&self) -> Vec<u64> {
        self.tokens
            .iter()
            .filter(|pinned| !pinned.dead)
            .map(|pinned| pinned.id)
            .collect()
    }

    pub fn all_tokens(&self) -> Vec<u64> {
        self.tokens.iter().map(|pinned| pinned.id).collect()
    }

    /// The model token `id` reads.
    pub fn token_model(&self, id: u64) -> Option<&Model> {
        self.tokens
            .iter()
            .find(|pinned| pinned.id == id)
            .map(|pinned| &pinned.model)
    }

    pub fn open_scrolls(&self) -> Vec<u64> {
        self.scrolls.iter().map(|scroll| scroll.id).collect()
    }

    /// Run one action and check its outcome and the state it leaves.
    pub fn execute(&mut self, action: &Action) -> Check {
        self.stats.actions += 1;
        if let Action::Read(Read::Search { .. }) | Action::ReadAt(_, Read::Search { .. }) = action {
            self.stats.searches += 1;
        }
        if !self.poisoned && self.session.handle().is_poisoned() {
            // Only an injected failure may poison the collection, and possibly after the
            // action that injected it returned (a background job that ran into it).
            if !self.faulted {
                return fail("the collection was poisoned without an injected failure");
            }
            self.poisoned = true;
            self.stats.poisoned += 1;
        }
        match action {
            Action::Crash {
                during,
                after_ops,
                tear,
                recovery,
            } => return self.crash(during.as_deref(), *after_ops, *tear, *recovery),
            Action::FailSync { during, nth, dir } => self.fail_sync(during, *nth, *dir)?,
            Action::Reopen => self.reopen()?,
            other => self.attempt(other, false)?,
        }
        if self.session.setup.maintenance == Maintenance::Stepped {
            self.session.settle()?;
        }
        self.drop_dead_views();
        let running = self.session.engine().scheduler().stats().running;
        self.stats.peak_jobs = self.stats.peak_jobs.max(running);
        self.check_state()
    }

    /// Run `action`. With `armed`, a crash or an injected failure may interrupt it: engine
    /// errors are then expected, and a write whose outcome is unknown becomes `pending`.
    fn attempt(&mut self, action: &Action, armed: bool) -> Check {
        match action {
            Action::Write(ops) => self.write(ops, armed),
            // Replayed filter requests over fields the schema no longer declares do nothing:
            // the model does not evaluate `$extra` filters.
            Action::DeleteByFilter(filter) | Action::UpdateByFilter(filter, _)
                if !filter_declared(&self.model, filter) =>
            {
                Ok(())
            }
            Action::ScrollStart { by, filter, .. }
                if filter
                    .as_ref()
                    .is_some_and(|filter| !filter_declared(&self.model, filter))
                    || by.as_ref().is_some_and(|(field, _)| {
                        self.model.schema.scalar_field(field).is_none()
                    }) =>
            {
                Ok(())
            }
            Action::DeleteByFilter(filter) => self.delete_by_filter(filter, armed),
            Action::UpdateByFilter(filter, patch) => self.update_by_filter(filter, patch, armed),
            Action::Alter(change) => self.alter(change, armed),
            // An explicit flush or compaction waits for the jobs this runner holds open.
            Action::Flush | Action::Compact if !self.jobs.is_empty() => Ok(()),
            Action::Flush => {
                let handle = self.session.handle().clone();
                let before = handle.current();
                let result = self
                    .session
                    .call("flush", move || handle.flush_blocking())?;
                let flushed = result.is_ok() && !armed;
                self.maintenance_result("flush", result.map(drop), armed)?;
                if flushed {
                    self.check_flushed(&before)?;
                }
                Ok(())
            }
            Action::Compact => {
                let flushes = self.session.engine().scheduler().stats().flushes_granted;
                let handle = self.session.handle().clone();
                let result = self
                    .session
                    .call("compact", move || handle.compact_blocking())?;
                let compacted = result.is_ok() && !armed;
                self.stats.explicit_compactions += 1;
                self.maintenance_result("compact", result.map(drop), armed)?;
                if compacted && self.session.setup.maintenance == Maintenance::Stepped {
                    self.check_compacted(flushes)?;
                }
                Ok(())
            }
            Action::BeginJob(kind) => {
                if self.poisoned || self.jobs.iter().any(|job| job.kind() == *kind) {
                    return Ok(());
                }
                let handle = self.session.handle().clone();
                let engine = self.session.engine().clone();
                let kind = *kind;
                let begun = self
                    .session
                    .call("begin", move || engine.begin_job(&handle, kind))?;
                match begun {
                    Ok(job) => {
                        self.jobs.push(job);
                        Ok(())
                    }
                    Err(error) => self.maintenance_result("begin", Err(error), armed),
                }
            }
            Action::BuildJob(kind) => {
                let Some(index) = self.jobs.iter().position(|job| job.kind() == *kind) else {
                    return Ok(());
                };
                let mut job = self.jobs.remove(index);
                let (job, result) = self.session.call("build", move || {
                    let result = job.build();
                    (job, result)
                })?;
                if result.is_ok() {
                    self.jobs.push(job);
                }
                self.maintenance_result("build", result, armed)
            }
            Action::CommitJob(kind) => {
                let Some(index) = self.jobs.iter().position(|job| job.kind() == *kind) else {
                    return Ok(());
                };
                let job = self.jobs.remove(index);
                let result = self
                    .session
                    .call("commit", move || job.commit().map(drop))?;
                if result.is_ok() {
                    self.stats.jobs_committed += 1;
                }
                self.maintenance_result("commit", result, armed)
            }
            Action::AbandonJob(kind) => {
                self.jobs.retain(|job| job.kind() != *kind);
                Ok(())
            }
            Action::StepScheduler => {
                if self.session.step_scheduler()? {
                    self.stats.steps_granted += 1;
                }
                Ok(())
            }
            Action::Pin => self.pin(),
            Action::Release(id) => self.release(*id),
            Action::AdvanceClock(by) => {
                self.session.clock.advance(*by);
                Ok(())
            }
            Action::Read(read) => {
                let view = self
                    .session
                    .view(&ReadOptions::default())
                    .map_err(|error| format!("read view: {error}"))?;
                let model = self.model.clone();
                self.checker()
                    .check_read(&view, &model, read)
                    .map_err(|error| format!("{read:?} at the current state: {error}"))
            }
            Action::ReadAt(id, read) => self.read_at(*id, read),
            Action::ScrollStart {
                id,
                by,
                filter,
                limit,
            } => self.scroll_start(*id, by.clone(), filter.clone(), *limit),
            Action::ScrollNext(id) => self.scroll_next(*id),
            Action::Crash { .. } | Action::FailSync { .. } | Action::Reopen => {
                fail("nested fault actions are not supported")
            }
        }
    }

    /// After an explicit compaction that succeeded in a hand-stepped run: at most one segment
    /// is left. The compaction takes every segment present when the writer plans it, and a
    /// flush that commits after that adds one, which is why this holds only with no flush
    /// running beside it: the runner holds no job open, and the engine's own flushes were
    /// settled before the action and cannot start during it (one past its backoff was
    /// requested at the settle, and the clock does not move). `flushes` is the flush permits
    /// granted before the call.
    fn check_compacted(&self, flushes: u64) -> Check {
        let granted = self.session.engine().scheduler().stats().flushes_granted - flushes;
        if granted > 0 {
            return fail(format!(
                "{granted} background flushes ran during an explicit compaction of a settled \
                 hand-stepped run"
            ));
        }
        let segments = self.session.handle().current().counters.segment_count;
        if segments > 1 {
            return fail(format!(
                "an explicit compaction with no job running left {segments} segments"
            ));
        }
        Ok(())
    }

    /// After an explicit flush that succeeded: the durable checkpoint covers everything that
    /// was visible. With no background job, the memtables are empty afterwards, and a flush
    /// with nothing to write published no manifest.
    fn check_flushed(&self, before: &Version) -> Check {
        let after = self.session.handle().current();
        if after.checkpoint_seq_no < self.model.visible_seq_no {
            return fail(format!(
                "an explicit flush left the checkpoint at {}, below the visible sequence number {}",
                after.checkpoint_seq_no, self.model.visible_seq_no
            ));
        }
        if self.session.setup.maintenance != Maintenance::Stepped {
            return Ok(());
        }
        if after.counters.memtable_rows != 0 {
            return fail(format!(
                "an explicit flush left {} memtable rows",
                after.counters.memtable_rows
            ));
        }
        let nothing = before.checkpoint_seq_no == self.model.visible_seq_no
            && before.counters.memtable_rows == 0;
        if nothing && after.manifest_generation != before.manifest_generation {
            return fail(format!(
                "a flush with nothing to write published manifest generation {} over {}",
                after.manifest_generation, before.manifest_generation
            ));
        }
        Ok(())
    }

    /// A maintenance call's result: success, or an error only when something was injected
    /// (a crash, a failed sync) or the collection is poisoned.
    fn maintenance_result(
        &mut self,
        what: &str,
        result: logpose_types::Result<()>,
        armed: bool,
    ) -> Check {
        match result {
            Ok(()) => Ok(()),
            Err(_) if armed => Ok(()),
            Err(error) if self.refused_after_fault(&error) => Ok(()),
            Err(error) => fail(format!("{what} failed: {error}")),
        }
    }

    /// Whether `error` is the refusal of a collection that an injected failure poisoned. A
    /// failure injected into one action can poison the collection after that action returned
    /// (a freeze's WAL rotation or a background job ran into it), so the next request may be
    /// the first to see it.
    fn refused_after_fault(&mut self, error: &LogPoseError) -> bool {
        let refused = matches!(error, LogPoseError::CollectionPoisoned { .. });
        if refused && (self.poisoned || self.faulted) {
            if !self.poisoned {
                self.poisoned = true;
                self.stats.poisoned += 1;
            }
            return true;
        }
        false
    }

    /// The outcome of a write-like request against the model's prediction.
    fn settle_write(
        &mut self,
        result: logpose_types::Result<logpose_types::CommitAck>,
        expected: Result<(Model, usize), Refusal>,
        armed: bool,
    ) -> Check {
        if let Err(error) = &result
            && !armed
            && self.refused_after_fault(error)
        {
            return Ok(());
        }
        match (result, expected) {
            (Ok(ack), Ok((next, applied))) => {
                if ack.applied_ops != applied {
                    return fail(format!(
                        "the ack applied {} operations, the model {applied}",
                        ack.applied_ops
                    ));
                }
                if applied > 0 && ack.last_seq_no != next.visible_seq_no {
                    return fail(format!(
                        "the ack's last sequence number is {}, the model's {}",
                        ack.last_seq_no, next.visible_seq_no
                    ));
                }
                if self.poisoned {
                    return fail("a poisoned collection acknowledged a write");
                }
                self.model = next;
                self.pending = None;
                self.stats.writes_acked += 1;
                Ok(())
            }
            (
                Err(LogPoseError::NotFound {
                    resource: ResourceKind::Record,
                    ..
                }),
                Err(Refusal::NotFound(_)),
            ) => {
                self.stats.writes_refused += 1;
                Ok(())
            }
            (Err(LogPoseError::InvalidArgument { .. }), Err(Refusal::Invalid(_))) => {
                self.stats.writes_refused += 1;
                Ok(())
            }
            // A crash or an injected failure can fail a valid request, but never make it
            // invalid.
            (Err(error), Ok((next, applied)))
                if (armed || self.poisoned)
                    && !matches!(
                        error,
                        LogPoseError::InvalidArgument { .. } | LogPoseError::NotFound { .. }
                    ) =>
            {
                if self.poisoned && !armed {
                    return Ok(());
                }
                let not_applied = matches!(
                    error,
                    LogPoseError::WalWriteFailed {
                        outcome: WriteOutcome::NotApplied,
                        ..
                    }
                );
                if !not_applied && applied > 0 {
                    self.pending = Some(next);
                }
                Ok(())
            }
            (Err(_), Err(_)) if armed => Ok(()),
            (result, expected) => fail(format!(
                "the write returned {result:?}, the model expected {expected:?}"
            )),
        }
    }

    fn write(&mut self, ops: &[ClientOp], armed: bool) -> Check {
        let expected = self.model.apply_batch(ops).map(|next| (next, ops.len()));
        let handle = self.session.handle().clone();
        let batch = ops.to_vec();
        let result = self
            .session
            .call("write", move || handle.write_blocking(batch))?;
        let acked = result.is_ok();
        self.settle_write(result, expected, armed)?;
        if acked && !armed {
            self.check_acked_keys(ops)?;
        }
        Ok(())
    }

    /// I1: a view opened right after an ack reflects the whole batch.
    fn check_acked_keys(&self, ops: &[ClientOp]) -> Check {
        let keys = ops.iter().map(|op| op.pk().clone()).collect::<Vec<_>>();
        let view = self
            .session
            .view(&ReadOptions::default())
            .map_err(|error| format!("view after an ack: {error}"))?;
        if view.visible_seq_no() < self.model.visible_seq_no {
            return fail(format!(
                "I1: a view after the ack sees {}, the ack was {}",
                view.visible_seq_no(),
                self.model.visible_seq_no
            ));
        }
        self.checker()
            .check_get(&view, &self.model, &keys)
            .map_err(|error| format!("I1: {error}"))
    }

    fn delete_by_filter(&mut self, filter: &FilterExpr, armed: bool) -> Check {
        let (next, deleted) = self.model.delete_by_filter(filter);
        let handle = self.session.handle().clone();
        let request = filter.clone();
        let result = self.session.call("delete by filter", move || {
            block(handle.delete_by_filter(request))
        })?;
        self.stats.filter_writes += 1;
        self.settle_write(result, Ok((next, deleted)), armed)
    }

    fn update_by_filter(
        &mut self,
        filter: &FilterExpr,
        patch: &PartialUpdate,
        armed: bool,
    ) -> Check {
        let expected = self.model.update_by_filter(filter, patch);
        let handle = self.session.handle().clone();
        let (request, update) = (filter.clone(), patch.clone());
        let result = self.session.call("update by filter", move || {
            block(handle.update_by_filter(request, update))
        })?;
        self.stats.filter_writes += 1;
        self.settle_write(result, expected, armed)
    }

    fn alter(&mut self, change: &SchemaChange, armed: bool) -> Check {
        let expected = self.model.apply_schema(change).map(|next| (next, 1));
        let handle = self.session.handle().clone();
        let request = change.clone();
        let result = self
            .session
            .call("alter", move || handle.alter_schema_blocking(request))?;
        if result.is_ok() {
            self.stats.alters += 1;
        }
        self.settle_write(result, expected, armed)
    }

    fn pin(&mut self) -> Check {
        if self.poisoned {
            return Ok(());
        }
        // Stay well below the per-collection cap: release the oldest live token past eight,
        // and forget dead ones past twelve.
        if self.tokens.iter().filter(|pinned| !pinned.dead).count() >= 8
            && let Some(pinned) = self.tokens.iter_mut().find(|pinned| !pinned.dead)
        {
            self.session.handle().release_snapshot(&pinned.token);
            pinned.dead = true;
        }
        while self.tokens.len() >= 12 {
            match self.tokens.iter().position(|pinned| pinned.dead) {
                Some(index) => {
                    self.tokens.remove(index);
                }
                None => break,
            }
        }
        let token = self
            .session
            .handle()
            .pin_snapshot()
            .map_err(|error| format!("pin: {error}"))?;
        // Resolving the token now leaves its expiry where the pin put it (the clock is manual).
        let view = self
            .session
            .view(&ReadOptions {
                token: Some(token.clone()),
                ..ReadOptions::default()
            })
            .map_err(|error| format!("view of a new token: {error}"))?;
        let id = self.next_id;
        self.next_id += 1;
        self.tokens.push(Pinned {
            id,
            token,
            model: self.model.clone(),
            expires: self.session.clock_now() + TTL,
            dead: false,
            view: Some(view),
        });
        Ok(())
    }

    fn release(&mut self, id: u64) -> Check {
        let now = self.session.clock_now();
        let Some(pinned) = self.tokens.iter_mut().find(|pinned| pinned.id == id) else {
            return Ok(());
        };
        let released = self.session.handle().release_snapshot(&pinned.token);
        let live = !pinned.dead && now < pinned.expires;
        pinned.dead = true;
        pinned.view = None;
        if live && !released {
            return fail(format!(
                "releasing live token {id} found nothing to release"
            ));
        }
        Ok(())
    }

    fn read_at(&mut self, id: u64, read: &Read) -> Check {
        let now = self.session.clock_now();
        let Some(index) = self.tokens.iter().position(|pinned| pinned.id == id) else {
            return Ok(());
        };
        let (token, model, live, boundary) = {
            let pinned = &self.tokens[index];
            (
                pinned.token.clone(),
                pinned.model.clone(),
                !pinned.dead && now < pinned.expires,
                !pinned.dead && now == pinned.expires,
            )
        };
        let result = self.session.view(&ReadOptions {
            token: Some(token),
            ..ReadOptions::default()
        });
        match result {
            Ok(view) if live || boundary => {
                self.stats.token_reads += 1;
                self.tokens[index].expires = now + TTL;
                if view.visible_seq_no() != model.visible_seq_no {
                    return fail(format!(
                        "I12: token {id} reads sequence {}, pinned at {}",
                        view.visible_seq_no(),
                        model.visible_seq_no
                    ));
                }
                self.checker()
                    .check_read(&view, &model, read)
                    .map_err(|error| format!("I12: {read:?} at token {id}: {error}"))
            }
            Err(LogPoseError::SnapshotExpired { .. }) if !live => {
                self.stats.expired_reads += 1;
                self.tokens[index].dead = true;
                Ok(())
            }
            Ok(_) => fail(format!(
                "token {id} is released, expired, or lost with a restart but still reads"
            )),
            Err(error) => fail(format!("read at live token {id}: {error}")),
        }
    }

    fn scroll_request(scroll: &Scroll, cursor: Option<Cursor>) -> ScrollRequest {
        ScrollRequest {
            filter: scroll.filter.clone(),
            order: scroll.order.clone(),
            limit: scroll.limit,
            projection: Projection::full(),
            cursor,
        }
    }

    fn scroll_start(
        &mut self,
        id: u64,
        by: Option<(String, bool)>,
        filter: Option<FilterExpr>,
        limit: u32,
    ) -> Check {
        if self.poisoned {
            return Ok(());
        }
        if self.scrolls.len() >= 3 {
            let oldest = self.scrolls[0].id;
            self.drop_scroll(oldest);
        }
        let (order, expected) = match &by {
            None => (ScrollOrder::Pk, self.model.matching(filter.as_ref())),
            Some((field, descending)) => (
                ScrollOrder::Field {
                    field: field.clone(),
                    direction: if *descending {
                        Direction::Descending
                    } else {
                        Direction::Ascending
                    },
                },
                self.model.order_by(field, *descending, filter.as_ref()),
            ),
        };
        let open = Scroll {
            id,
            order,
            filter,
            limit,
            expected,
            model: self.model.clone(),
            returned: 0,
            cursor: None,
            expires: self.session.clock_now() + TTL,
            dead: false,
        };
        let request = Self::scroll_request(&open, None);
        let page = self
            .session
            .block_on(scroll(
                self.session.engine(),
                &crate::session::reference(),
                request,
            ))
            .map_err(|error| format!("scroll {id} first page: {error}"))?;
        self.next_id = self.next_id.max(id + 1);
        self.scrolls.push(open);
        self.scroll_page(id, page)
    }

    fn scroll_next(&mut self, id: u64) -> Check {
        let now = self.session.clock_now();
        let Some(index) = self.scrolls.iter().position(|scroll| scroll.id == id) else {
            return Ok(());
        };
        let (request, live, boundary) = {
            let scroll = &self.scrolls[index];
            (
                Self::scroll_request(scroll, scroll.cursor.clone()),
                !scroll.dead && now < scroll.expires,
                !scroll.dead && now == scroll.expires,
            )
        };
        let result = self.session.block_on(scroll(
            self.session.engine(),
            &crate::session::reference(),
            request,
        ));
        match result {
            Ok(page) if live || boundary => {
                self.scrolls[index].expires = now + TTL;
                self.scroll_page(id, page)
            }
            Err(logpose_query::QueryError::Storage(LogPoseError::SnapshotExpired { .. }))
                if !live =>
            {
                self.stats.expired_reads += 1;
                self.scrolls.remove(index);
                Ok(())
            }
            Ok(_) => fail(format!(
                "scroll {id} continued past a restart or its expiry"
            )),
            Err(error) => fail(format!("scroll {id} next page: {error}")),
        }
    }

    /// Check one page against the scroll's expected keys and rows, and keep or finish it.
    fn scroll_page(&mut self, id: u64, page: logpose_query::ScrollPage) -> Check {
        let Some(index) = self.scrolls.iter().position(|scroll| scroll.id == id) else {
            return Ok(());
        };
        let scroll = &mut self.scrolls[index];
        let from = scroll.returned;
        let to = (from + page.rows.len()).min(scroll.expected.len());
        let wanted = &scroll.expected[from..to];
        let got = page
            .rows
            .iter()
            .map(|row| row.record.pk.clone())
            .collect::<Vec<_>>();
        if got != wanted {
            return fail(format!(
                "I12: scroll {id} ({:?}, {:?}) page after {from} rows returned {got:?}, the \
                 model at its start {wanted:?}",
                scroll.order, scroll.filter
            ));
        }
        for row in &page.rows {
            check_row(&scroll.model, row).map_err(|error| format!("I12: scroll {id}: {error}"))?;
        }
        scroll.returned = to;
        let token = scroll.cursor.as_ref().map(|cursor| cursor.token.clone());
        match page.next {
            Some(cursor) => {
                if page.rows.len() < scroll.limit as usize {
                    return fail(format!("scroll {id} returned a short page with a cursor"));
                }
                scroll.cursor = Some(cursor);
                Ok(())
            }
            None => {
                if scroll.returned != scroll.expected.len() {
                    return fail(format!(
                        "scroll {id} ended after {} of {} rows",
                        scroll.returned,
                        scroll.expected.len()
                    ));
                }
                self.scrolls.remove(index);
                self.stats.scrolls_finished += 1;
                if let Some(token) = token {
                    self.session.handle().release_snapshot(&token);
                }
                Ok(())
            }
        }
    }

    /// Abandon a scroll, releasing its token.
    pub fn drop_scroll(&mut self, id: u64) {
        if let Some(index) = self.scrolls.iter().position(|scroll| scroll.id == id) {
            let scroll = self.scrolls.remove(index);
            if let (Some(cursor), false) = (scroll.cursor, scroll.dead)
                && self.session.is_open()
            {
                self.session.handle().release_snapshot(&cursor.token);
            }
        }
    }

    /// Fail the `nth` file or directory sync from now while `during` runs. Afterwards nothing
    /// that was not acknowledged is visible, and a poisoned collection refuses writes.
    fn fail_sync(&mut self, during: &Action, nth: u64, dir: bool) -> Check {
        let fault = self.session.fault().clone();
        let mut plan = FaultPlan::default();
        if dir {
            plan.fail_sync_dir = Some(fault.dir_syncs() + nth);
        } else {
            plan.fail_sync = Some(fault.file_syncs() + nth);
        }
        fault.set_plan(plan);
        self.faulted = true;
        let outcome = self.attempt(during, true);
        // Background jobs may still be running into the failure.
        if self.session.setup.maintenance == Maintenance::Stepped {
            self.session.settle()?;
        } else {
            self.session.wait_for_jobs()?;
        }
        fault.set_plan(FaultPlan::default());
        outcome?;
        self.stats.failed_syncs += 1;
        if self.session.handle().is_poisoned() && !self.poisoned {
            self.poisoned = true;
            self.stats.poisoned += 1;
        }
        if self.poisoned {
            // Read-only: a write is refused and changes nothing.
            let refused = self
                .session
                .handle()
                .write_blocking(vec![ClientOp::Delete(PrimaryKey::from("k00"))]);
            if refused.is_ok() {
                return fail("a poisoned collection accepted a write");
            }
        }
        Ok(())
    }

    /// Drop the jobs this runner holds open, as a restart must before it closes the engine. In
    /// a hand-stepped run, then settle the engine's own flush of the memtable an abandoned
    /// flush left frozen: it runs to its end (or into a planned crash) now, instead of racing
    /// the close, which cancels it if it has not begun yet.
    fn end_jobs(&mut self) -> Check {
        self.jobs.clear();
        if self.session.setup.maintenance == Maintenance::Stepped {
            self.session.settle()?;
        }
        Ok(())
    }

    /// Drop the engine (no crash) and open it again in the same boot. The durability barrier
    /// settles what an earlier failure left unknown.
    fn reopen(&mut self) -> Check {
        self.end_jobs()?;
        self.drop_views();
        self.session.close();
        self.session.open()?;
        self.stats.reopens += 1;
        self.after_restart()
    }

    fn crash(
        &mut self,
        during: Option<&Action>,
        after_ops: u64,
        tear: TearMode,
        recovery: Option<(u64, TearMode)>,
    ) -> Check {
        let fault = self.session.fault().clone();
        fault.set_plan(FaultPlan {
            crash_after_ops: Some(fault.mutating_ops() + after_ops),
            tear,
            ..FaultPlan::default()
        });
        if let Some(action) = during {
            self.attempt(action, true)?;
        }
        // Background jobs run into the crash too.
        self.end_jobs()?;
        if self.session.setup.maintenance != Maintenance::Stepped {
            self.session.wait_for_jobs()?;
        }
        self.drop_views();
        self.session.close();
        fault.crash();
        self.stats.crashes += 1;
        if let Some((ops, tear)) = recovery {
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(ops),
                tear,
                ..FaultPlan::default()
            });
            self.session.open_and_drop();
            fault.crash();
            self.stats.recovery_crashes += 1;
        }
        self.session.open()?;
        self.after_restart()?;
        self.check_state()
    }

    /// After a restart: the recovered state is the model or the pending state (I8), tokens and
    /// scrolls are gone, and the collection is writable again.
    fn after_restart(&mut self) -> Check {
        let had_pending = self.pending.is_some();
        match self.checker().state_equals(&self.model.clone()) {
            Ok(()) => {
                if had_pending {
                    self.stats.unacked_lost += 1;
                }
            }
            Err(error) => match self.pending.clone() {
                Some(pending) => {
                    self.checker().state_equals(&pending).map_err(|pending_error| {
                        format!(
                            "I8: the recovered state is neither the acknowledged prefix ({error}) \
                             nor the prefix with the unacknowledged write ({pending_error})"
                        )
                    })?;
                    self.model = pending;
                    self.stats.unacked_recovered += 1;
                }
                None => return fail(format!("I8: the recovered state differs: {error}")),
            },
        }
        self.pending = None;
        self.poisoned = false;
        self.faulted = false;
        for pinned in &mut self.tokens {
            pinned.dead = true;
        }
        for scroll in &mut self.scrolls {
            scroll.dead = true;
        }
        Ok(())
    }

    /// Check the invariants of the published version and that the visible state equals the
    /// model.
    pub fn check_state(&mut self) -> Check {
        if self
            .session
            .fatal
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            return fail("the engine called its fatal handler");
        }
        self.checker().state_equals(&self.model)?;
        let version = self.session.handle().current();
        if version.manifest_generation < self.generation
            || version.checkpoint_seq_no < self.checkpoint
        {
            return fail(format!(
                "the published manifest went back from generation {} (checkpoint {}) to {} \
                 (checkpoint {})",
                self.generation,
                self.checkpoint,
                version.manifest_generation,
                version.checkpoint_seq_no
            ));
        }
        self.generation = version.manifest_generation;
        self.checkpoint = version.checkpoint_seq_no;
        self.check_files()
    }

    /// I7: every segment of the current version and of every version a live token pins is on
    /// disk.
    fn check_files(&mut self) -> Check {
        let current = self
            .session
            .view(&ReadOptions::default())
            .map_err(|error| format!("read view: {error}"))?;
        let mut wanted = std::collections::BTreeSet::new();
        for view in std::iter::once(&current)
            .chain(self.tokens.iter().filter_map(|pinned| pinned.view.as_ref()))
        {
            for unit in view.units() {
                if !unit.is_memtable() {
                    wanted.insert(format!("{:08x}.seg", unit.id().0));
                }
            }
        }
        let present = self.session.segment_files()?;
        if let Some(missing) = wanted.iter().find(|name| !present.contains(*name)) {
            return fail(format!(
                "I7: segment file {missing} was deleted while a live version references it"
            ));
        }
        self.stats.files_checked += wanted.len() as u64;
        Ok(())
    }

    /// Checks over this runner's session.
    pub fn checker(&self) -> Checker<'_> {
        Checker {
            session: &self.session,
        }
    }

    /// The model of the collection `session` holds now, with no rows: the schema and sequence
    /// number of a collection that was just created.
    pub fn new_model(session: &Session) -> Model {
        Model::new(
            (*session.handle().current().schema).clone(),
            session.handle().visible_seq_no(),
        )
    }
}

/// Checks of what the engine serves against a model, over one session.
pub struct Checker<'a> {
    session: &'a Session,
}

impl<'a> Checker<'a> {
    pub fn new(session: &'a Session) -> Self {
        Self { session }
    }

    /// Whether the visible state is exactly `model`: invariants, sequence number, schema, live
    /// counter, a full scan, and a point lookup of every key plus two absent ones.
    pub fn state_equals(&self, model: &Model) -> Check {
        let handle = self.session.handle();
        let version = handle.current();
        version
            .check_invariants()
            .map_err(|error| format!("invariants: {error}"))?;
        let view = self
            .session
            .view(&ReadOptions::default())
            .map_err(|error| format!("read view: {error}"))?;
        if view.visible_seq_no() != model.visible_seq_no {
            return fail(format!(
                "visible sequence number {} but the model's is {}",
                view.visible_seq_no(),
                model.visible_seq_no
            ));
        }
        if **view.schema() != model.schema {
            return fail(format!(
                "schema differs:\nengine {:?}\nmodel  {:?}",
                view.schema(),
                model.schema
            ));
        }
        if view.counters().live_rows() != model.len() as u64 {
            return fail(format!(
                "the live counter is {}, the model has {} rows",
                view.counters().live_rows(),
                model.len()
            ));
        }
        self.check_scan(&view, model, 1 + (model.visible_seq_no % 5) as u32)?;
        let mut keys = model.rows.keys().cloned().collect::<Vec<_>>();
        keys.push(PrimaryKey::from("absent"));
        keys.push(PrimaryKey::from("k99"));
        self.check_get(&view, model, &keys)
    }

    /// Check a read of `view` against `model`.
    pub fn check_read(&self, view: &ReadView, model: &Model, read: &Read) -> Check {
        if !declared(model, read) {
            // A replayed read whose fields the schema no longer declares (the model does not
            // evaluate `$extra` filters).
            return Ok(());
        }
        match read {
            Read::Get(keys) => self.check_get(view, model, keys),
            Read::Count(filter) => {
                let count = self
                    .session
                    .block_on(count_view(view, filter.as_ref()))
                    .map_err(|error| format!("count: {error}"))?;
                let wanted = model.matching(filter.as_ref()).len() as u64;
                if count == wanted {
                    Ok(())
                } else {
                    fail(format!("count({filter:?}) = {count}, the model {wanted}"))
                }
            }
            Read::Scan { limit } => self.check_scan(view, model, *limit),
            Read::OrderBy {
                field,
                descending,
                filter,
                limit,
            } => self.check_order_by(view, model, field, *descending, filter.as_ref(), *limit),
            Read::Search { query, k, filter } => {
                self.check_search(view, model, query, *k, filter.as_ref())
            }
        }
    }

    fn check_get(&self, view: &ReadView, model: &Model, keys: &[PrimaryKey]) -> Check {
        let rows = self
            .session
            .block_on(view.get(keys, Projection::full()))
            .map_err(|error| format!("get: {error}"))?;
        for (key, row) in keys.iter().zip(rows) {
            match (row, model.rows.contains_key(key)) {
                (None, false) => {}
                (Some(row), true) => check_row(model, &row)?,
                (row, _) => {
                    return fail(format!(
                        "get({key}) = {:?}, the model holds {:?}",
                        row.map(|row| row.record),
                        model.record(key)
                    ));
                }
            }
        }
        Ok(())
    }

    /// Page through every live row by key over one view.
    fn check_scan(&self, view: &ReadView, model: &Model, limit: u32) -> Check {
        let rows = self.pages(view, None, &ScrollOrder::Pk, limit)?;
        let keys = rows
            .iter()
            .map(|row| row.record.pk.clone())
            .collect::<Vec<_>>();
        let wanted = model.rows.keys().cloned().collect::<Vec<_>>();
        if keys != wanted {
            return fail(format!(
                "a full scan returned {keys:?}, the model holds {wanted:?}"
            ));
        }
        for row in &rows {
            check_row(model, row)?;
        }
        Ok(())
    }

    fn pages(
        &self,
        view: &ReadView,
        filter: Option<&FilterExpr>,
        order: &ScrollOrder,
        limit: u32,
    ) -> Result<Vec<RowData>, String> {
        let mut rows = Vec::new();
        let mut after = None;
        loop {
            let (page, last) = self
                .session
                .block_on(scroll_view(
                    view,
                    filter,
                    order,
                    limit,
                    Projection::full(),
                    after.as_ref(),
                ))
                .map_err(|error| format!("scroll ({order:?}, {filter:?}): {error}"))?;
            let done = page.len() < limit.max(1) as usize;
            rows.extend(page);
            if rows.len() > 10_000 {
                return fail("a scroll does not end");
            }
            match last {
                Some(last) if !done => after = Some(last),
                _ => return Ok(rows),
            }
        }
    }

    fn check_order_by(
        &self,
        view: &ReadView,
        model: &Model,
        field: &str,
        descending: bool,
        filter: Option<&FilterExpr>,
        limit: u32,
    ) -> Check {
        let order = ScrollOrder::Field {
            field: field.to_owned(),
            direction: if descending {
                Direction::Descending
            } else {
                Direction::Ascending
            },
        };
        let rows = self.pages(view, filter, &order, limit)?;
        let keys = rows
            .iter()
            .map(|row| row.record.pk.clone())
            .collect::<Vec<_>>();
        let wanted = model.order_by(field, descending, filter);
        if keys != wanted {
            return fail(format!(
                "order by {field} (descending {descending}, {filter:?}) returned {keys:?}, the \
                 model {wanted:?}"
            ));
        }
        for row in &rows {
            check_row(model, row)?;
        }
        Ok(())
    }

    fn check_search(
        &self,
        view: &ReadView,
        model: &Model,
        query: &[f32],
        k: usize,
        filter: Option<&FilterExpr>,
    ) -> Check {
        let request = SearchRequest {
            filter: filter.cloned(),
            projection: Projection::full(),
            ..SearchRequest::new(query.to_vec(), k)
        };
        let outcome = self
            .session
            .block_on(search(view, &request))
            .map_err(|error| format!("search: {error}"))?;
        let metric = model.metric();
        let context = format!("search {query:?} k={k} {filter:?}");
        let mut seen = std::collections::BTreeSet::new();
        for hit in &outcome.hits {
            let pk = &hit.row.record.pk;
            if !seen.insert(pk.clone()) {
                return fail(format!("{context}: {pk} returned twice"));
            }
            let Some(row) = model.rows.get(pk) else {
                return fail(format!("{context}: {pk} is not a live row"));
            };
            if filter.is_some_and(|filter| !model.matches(filter, row)) {
                return fail(format!("{context}: {pk} does not match the filter"));
            }
            check_row(model, &hit.row).map_err(|error| format!("{context}: {error}"))?;
            let exact = metric_value(metric, query, &row.vector);
            if hit.value != exact {
                return fail(format!(
                    "{context}: {pk} reported {} but its value is {exact}",
                    hit.value
                ));
            }
        }
        let expected = model.search(query, k, filter);
        if self.session.setup.indexed {
            // Graph walks may miss rows; everything returned must be sound and ordered.
            let values = outcome.hits.iter().map(|hit| hit.value).collect::<Vec<_>>();
            let mut sorted = values.clone();
            sorted.sort_by(|left, right| match metric {
                logpose_types::DistanceMetric::L2 => left.total_cmp(right),
                _ => right.total_cmp(left),
            });
            if values != sorted || outcome.hits.len() > k {
                return fail(format!(
                    "{context}: hits out of order or too many: {values:?}"
                ));
            }
            return Ok(());
        }
        let got = outcome
            .hits
            .iter()
            .map(|hit| (hit.row.record.pk.clone(), hit.value))
            .collect::<Vec<_>>();
        if got != expected {
            return fail(format!(
                "{context}: exact search returned {got:?}, the model {expected:?}"
            ));
        }
        Ok(())
    }
}

/// Whether every field `filter` compares is a declared scalar field of `model`.
fn filter_declared(model: &Model, filter: &FilterExpr) -> bool {
    match filter {
        FilterExpr::And { children } | FilterExpr::Or { children } => {
            children.iter().all(|child| filter_declared(model, child))
        }
        FilterExpr::Not { child } => filter_declared(model, child),
        FilterExpr::Comparison(comparison) => {
            model.schema.scalar_field(&comparison.field).is_some()
        }
    }
}

/// Whether every field `read` filters or orders on is a declared scalar field of `model`.
fn declared(model: &Model, read: &Read) -> bool {
    let filter = match read {
        Read::Get(_) | Read::Scan { .. } => None,
        Read::Count(filter) | Read::Search { filter, .. } => filter.as_ref(),
        Read::OrderBy { field, filter, .. } => {
            if model.schema.scalar_field(field).is_none() {
                return false;
            }
            filter.as_ref()
        }
    };
    filter.is_none_or(|filter| filter_declared(model, filter))
}

/// Run a future to completion on a runtime of its own (on a thread outside every runtime).
fn block<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime builds")
        .block_on(future)
}

/// A returned row equals the model's row: every field, the vector, `$extra`, and a sequence
/// number in the range of the row's last write.
fn check_row(model: &Model, row: &RowData) -> Check {
    let pk = &row.record.pk;
    let Some(stored) = model.rows.get(pk) else {
        return fail(format!("{pk} is returned but not live"));
    };
    let expected: Record = model.visible(pk, stored);
    if row.record != expected {
        return fail(format!(
            "{pk} reads {:?}\nbut the model holds {:?}",
            row.record, expected
        ));
    }
    if row.seq_no < stored.seq.0 || row.seq_no > stored.seq.1 {
        return fail(format!(
            "{pk} carries sequence number {}, its last write had {:?}",
            row.seq_no, stored.seq
        ));
    }
    Ok(())
}
