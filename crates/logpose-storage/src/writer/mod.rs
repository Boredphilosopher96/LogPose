//! The single writer task of a collection and its group commit pipeline.
//!
//! Each collection has one writer task on the engine's writer runtime. It owns the collection's
//! [`WalWriter`], its private logical state (schema and delta, which run ahead of the published
//! `Version` by at most one prepared group), the durable manifest, and the maintenance job slot.
//! Nothing else touches them, so the writer takes no locks, and it is the only code that
//! publishes a `Version`.
//!
//! Group commit is a two-stage pipeline with at most one WAL I/O in flight:
//!
//! ```text
//!  requests ─► collect G(n+1) ─► prepare G(n+1) (apply to private state) ─┐
//!                                                                         │ await io(n)
//!             io(n): append all frames of G(n) + one fsync (I/O pool) ────┤
//!                                                                         ▼
//!                        publish V(n): store, notify, ack G(n) ─► start io(n+1)
//! ```
//!
//! While `io(n)` runs, requests accumulate in the channel and become `G(n+1)`, so concurrent
//! writers share one fsync. A group becomes visible only after its own fsync returned (I14), and
//! acknowledgements are sent only after the `Version` that includes them is stored (I1).
//!
//! Control messages (maintenance job begin and commit, quiesce for a drop, shutdown) are
//! polled first with a biased select, and each one drains the pipeline before it is handled:
//! a continuous stream of writes can delay a flush or compaction publish by at most one group.
//!
//! A failed group append poisons the collection. The WAL layer rolls the file back to the last
//! synced group; the group's writes fail with [`LogPoseError::WalWriteFailed`] carrying the
//! rollback's outcome, and the next prepared group fails with `NotApplied`. A clean rollback
//! leaves the collection read-only until it is reopened. A failed rollback fences the WAL and
//! fails the collection for this process; when the fence could not be written either, the
//! engine's fatal handler stops the process.

mod apply;
mod prepare;
#[cfg(test)]
mod tests;

pub(crate) use apply::{LogicalState, replay_frame};

use crate::{
    engine::CoreRef,
    gc::FileHandle,
    handle::{CollectionHandle, PoisonKind},
    maintenance::{MaintenanceOperation, should_compact, should_flush},
    manifest::{
        MANIFEST_FORMAT_VERSION, Manifest, ManifestSegment, manifest_path, publish_manifest,
    },
    paths::UnitFiles,
    runtime::run_cpu,
    version::{Version, VersionId},
};
use logpose_types::{
    CommitAck, LogPoseError, Result, SeqNo, Snapshot, UnitId, WriteOutcome,
    record::ClientOp,
    schema::{CollectionSchema, ScalarFieldSpec, SchemaError},
};
use logpose_vfs::is_crashed;
use logpose_wal::{
    WalError, WalFrame, WalWriter,
    codec::{CheckpointPayload, WalPayload},
};
use prepare::{INLINE_PREPARE_ROWS, Pending, PreparedRequests};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

/// Group commit settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupCommitConfig {
    /// Most requests in one fsync group. Default 256.
    pub max_group_requests: usize,
    /// Approximate request bytes after which a group stops collecting. Default 16 MiB.
    pub max_group_bytes: usize,
    /// How long a group smaller than `min_group_requests` waits for more requests. Default 0:
    /// the pipeline alone batches, because requests queue up while the previous group's fsync
    /// runs.
    pub commit_delay: Duration,
    /// Group size below which `commit_delay` applies. Default 1.
    pub min_group_requests: usize,
    /// Requests queued per collection before writers wait (backpressure). Default 1024.
    pub request_queue_depth: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_group_requests: 256,
            max_group_bytes: 16 * 1024 * 1024,
            commit_delay: Duration::ZERO,
            min_group_requests: 1,
            request_queue_depth: 1024,
        }
    }
}

/// An online schema change, ordered with writes in the collection's request stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaChange {
    /// Add a nullable scalar field.
    AddField(ScalarFieldSpec),
    /// Drop a scalar or vector field.
    DropField {
        /// The field's current name.
        name: String,
    },
    /// Rename any field.
    RenameField {
        /// The current name.
        from: String,
        /// The new name.
        to: String,
    },
}

impl SchemaChange {
    /// Apply the change to `schema`, which is unchanged on error.
    pub fn apply_to(&self, schema: &mut CollectionSchema) -> std::result::Result<(), SchemaError> {
        match self {
            Self::AddField(spec) => schema.add_field(spec.clone()).map(|_| ()),
            Self::DropField { name } => schema.drop_field(name).map(|_| ()),
            Self::RenameField { from, to } => schema.rename_field(from, to).map(|_| ()),
        }
    }
}

/// Where the acknowledgement of one request goes.
pub(crate) type Ack = oneshot::Sender<Result<CommitAck>>;

/// A client request, in the order it reached the writer.
pub(crate) enum WriteRequest {
    /// Upserts and deletes by key; atomic.
    Batch { ops: Vec<ClientOp>, ack: Ack },
    /// A schema change; a batch of one.
    AlterSchema { change: SchemaChange, ack: Ack },
}

impl WriteRequest {
    fn ack(self) -> Ack {
        match self {
            Self::Batch { ack, .. } | Self::AlterSchema { ack, .. } => ack,
        }
    }

    fn rows(&self) -> usize {
        match self {
            Self::Batch { ops, .. } => ops.len(),
            Self::AlterSchema { .. } => 1,
        }
    }

    /// A cheap estimate of the request's encoded size, for the group byte limit.
    fn approximate_bytes(&self) -> usize {
        match self {
            Self::Batch { ops, .. } => ops
                .iter()
                .map(|op| match op {
                    ClientOp::Upsert(record) => {
                        64 + record
                            .vectors
                            .values()
                            .map(|vector| vector.len() * 4)
                            .sum::<usize>()
                            + 16 * record.fields.len()
                            + 32 * record.extra.len()
                    }
                    ClientOp::Update(_) | ClientOp::Delete(_) => 64,
                })
                .sum(),
            Self::AlterSchema { .. } => 256,
        }
    }
}

/// A maintenance job that publishes a manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JobKind {
    Flush,
    Compact,
}

/// What a maintenance job starts from.
pub(crate) struct JobStart {
    /// The published (hence durable) state the job works from.
    pub(crate) version: Arc<Version>,
    /// The unit id the job's output must use. Allocated for this job alone and never issued
    /// again, whether or not the job commits.
    pub(crate) unit: UnitId,
}

/// What a maintenance job built, for the writer to commit.
#[derive(Debug)]
pub(crate) enum JobCommit {
    /// A flush of the delta up to `checkpoint_seq_no`, as a segment (none when the delta held
    /// only schema changes).
    Flush {
        checkpoint_seq_no: SeqNo,
        segment: Option<ManifestSegment>,
    },
    /// A compaction of `inputs` into `output`.
    Compact {
        inputs: Vec<UnitId>,
        output: ManifestSegment,
    },
}

/// Messages polled before client requests.
pub(crate) enum ControlMsg {
    /// Start a maintenance job once no other job of the collection is active. The writer drains
    /// the pipeline and, for a flush, rotates the WAL so that the delta it replies with ends in
    /// an older file; the reply is the published (hence durable) state the job works from.
    BeginJob {
        kind: JobKind,
        reply: oneshot::Sender<Result<JobStart>>,
    },
    /// Commit the active job: publish its manifest, then the `Version` over it.
    CommitJob {
        commit: Box<JobCommit>,
        reply: oneshot::Sender<Result<Snapshot>>,
    },
    /// The active job ended without committing. `wrote_files` says whether it may have created
    /// files for its unit, which are then removed: no durable manifest names them.
    EndJob { wrote_files: bool },
    /// Reply once the pipeline is drained and no job is active (a drop waits for this after it
    /// marked the handle dropped, so nothing is written afterwards). A std channel, because a
    /// drop blocks its caller and may be called from anywhere.
    Quiesce {
        reply: std::sync::mpsc::SyncSender<()>,
    },
    /// Stop the task.
    Shutdown,
}

/// The sending halves, held by the collection handle.
pub(crate) struct WriterChannels {
    pub(crate) requests: mpsc::Sender<WriteRequest>,
    pub(crate) control: mpsc::UnboundedSender<ControlMsg>,
}

/// The receiving halves, owned by the writer task.
pub(crate) struct WriterInbox {
    requests: mpsc::Receiver<WriteRequest>,
    control: mpsc::UnboundedReceiver<ControlMsg>,
}

/// Create a collection's writer channels.
pub(crate) fn channels(config: &GroupCommitConfig) -> (WriterChannels, WriterInbox) {
    let (requests, request_inbox) = mpsc::channel(config.request_queue_depth.max(1));
    let (control, control_inbox) = mpsc::unbounded_channel();
    (
        WriterChannels { requests, control },
        WriterInbox {
            requests: request_inbox,
            control: control_inbox,
        },
    )
}

/// Everything a writer task starts from: the open WAL, the recovered private state (equal to
/// the handle's first published `Version`), the durable manifest, and the file handles of its
/// segments.
pub(crate) struct WriterSeed {
    pub(crate) wal: WalWriter,
    pub(crate) state: LogicalState,
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) next_seq_no: SeqNo,
    pub(crate) version_id: VersionId,
    /// One handle per segment of `manifest`.
    pub(crate) live_files: BTreeMap<UnitId, Arc<FileHandle>>,
    /// The manifest generation kept beside the durable one, if any.
    pub(crate) previous_generation: Option<u64>,
    /// The first manifest generation to issue.
    pub(crate) next_manifest_gen: u64,
    /// The first unit id to issue.
    pub(crate) next_unit_id: u32,
}

/// Start the writer task of `handle` on the engine's writer runtime.
pub(crate) fn spawn(
    core: CoreRef,
    handle: Arc<CollectionHandle>,
    inbox: WriterInbox,
    seed: WriterSeed,
) {
    let runtime = core.writer_runtime().clone();
    let group = core.group_commit;
    core.register_writer(handle.control_sender());
    let writer = Writer {
        core,
        handle,
        requests: inbox.requests,
        control: inbox.control,
        config: group,
        wal: Some(seed.wal),
        state: Some(seed.state),
        next_manifest_gen: seed.next_manifest_gen,
        next_unit_id: seed.next_unit_id,
        manifest: seed.manifest,
        live_files: seed.live_files,
        previous_generation: seed.previous_generation,
        next_seq_no: seed.next_seq_no,
        next_version_id: seed.version_id.0 + 1,
        pending_checkpoint: None,
        active_job: None,
        waiting_jobs: VecDeque::new(),
        quiesce_waiters: Vec::new(),
        requested: [false; 2],
    };
    runtime.spawn(writer.run());
}

type IoResult = (WalWriter, std::result::Result<(), IoFailure>);
type IoFuture = Pin<Box<dyn Future<Output = Result<IoResult>> + Send>>;

/// Why a group's I/O failed.
enum IoFailure {
    /// The size-triggered rotation before the group failed the WAL writer; the group's frames
    /// were never appended.
    Rotate(WalError),
    /// Appending or syncing the group failed.
    Append(WalError),
}

/// A group whose I/O is running: its acknowledgements and the `Version` that includes it.
struct InFlight {
    io: IoFuture,
    pending: Vec<Pending>,
    version: Version,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Flow {
    Continue,
    Stop,
}

struct Writer {
    core: CoreRef,
    handle: Arc<CollectionHandle>,
    requests: mpsc::Receiver<WriteRequest>,
    control: mpsc::UnboundedReceiver<ControlMsg>,
    config: GroupCommitConfig,
    /// `None` while a group's I/O runs, and after the writer lost it.
    wal: Option<WalWriter>,
    /// The private logical state; `None` only while a large group is prepared on the query pool,
    /// or after that preparation panicked.
    state: Option<LogicalState>,
    /// The durable manifest.
    manifest: Arc<Manifest>,
    /// One handle per segment of the durable manifest (I7).
    live_files: BTreeMap<UnitId, Arc<FileHandle>>,
    /// The manifest generation kept beside the durable one (for inspection); the next commit
    /// removes it.
    previous_generation: Option<u64>,
    /// Next manifest generation to try. Advances on every publish attempt, so a generation is
    /// never written twice, even by a retry.
    next_manifest_gen: u64,
    /// Next unit id to allocate; recorded in every manifest.
    next_unit_id: u32,
    next_seq_no: SeqNo,
    next_version_id: u64,
    /// A checkpoint frame to prepend to the next group, written after a flush commit.
    pending_checkpoint: Option<WalFrame>,
    active_job: Option<ActiveJob>,
    waiting_jobs: VecDeque<(JobKind, oneshot::Sender<Result<JobStart>>)>,
    quiesce_waiters: Vec<std::sync::mpsc::SyncSender<()>>,
    /// Whether a flush (0) or compaction (1) was already requested from the scheduler.
    requested: [bool; 2],
}

/// The maintenance job that holds the collection's job slot.
#[derive(Clone, Copy, Debug)]
struct ActiveJob {
    kind: JobKind,
    unit: UnitId,
    /// Whether the job's files are still the job's to clean up. Cleared once a commit made them
    /// live, or made their durability unknown.
    owns_files: bool,
}

impl Writer {
    async fn run(mut self) {
        let mut inflight: Option<InFlight> = None;
        loop {
            tokio::select! {
                biased;
                message = self.control.recv() => {
                    if let Some(io) = inflight.take() {
                        self.finish(io).await;
                    }
                    let flow = match message {
                        Some(message) => self.handle_control(message).await,
                        None => Flow::Stop,
                    };
                    if flow == Flow::Stop {
                        break;
                    }
                }
                done = io_done(&mut inflight), if inflight.is_some() => {
                    if let Some(io) = inflight.take() {
                        self.complete(io.pending, io.version, done);
                    }
                }
                request = self.requests.recv() => {
                    let Some(request) = request else { break };
                    let requests = self.collect(request).await;
                    if let Some(error) = self.refusal() {
                        for request in requests {
                            let _ = request.ack().send(Err(error.clone_error()));
                        }
                        continue;
                    }
                    // The private state before this group, to restore if the group is never
                    // appended: the log must not skip the sequence numbers it took.
                    let before = self
                        .state
                        .clone()
                        .map(|state| (state, self.next_seq_no));
                    let prepared = self.prepare(requests).await;
                    if let Some(io) = inflight.take() {
                        self.finish(io).await;
                    }
                    if let Some((prepared, version)) = prepared {
                        inflight = self.start(prepared, version, before);
                    }
                }
            }
        }
        if let Some(io) = inflight.take() {
            self.finish(io).await;
        }
        self.stop();
    }

    /// Why the writer refuses new work, if it does.
    fn refusal(&self) -> Option<Refusal> {
        if self.handle.is_dropped() || self.handle.is_poisoned() {
            return Some(Refusal::Handle(Arc::clone(&self.handle)));
        }
        if self.core.is_shutting_down() {
            return Some(Refusal::Shutdown);
        }
        None
    }

    /// Take `first` and whatever else is queued, up to the group limits.
    async fn collect(&mut self, first: WriteRequest) -> Vec<WriteRequest> {
        let mut bytes = first.approximate_bytes();
        let mut group = vec![first];
        let full = |group: &Vec<WriteRequest>, bytes: usize| {
            group.len() >= self.config.max_group_requests.max(1)
                || bytes >= self.config.max_group_bytes
        };
        while !full(&group, bytes) {
            match self.requests.try_recv() {
                Ok(request) => {
                    bytes += request.approximate_bytes();
                    group.push(request);
                }
                Err(_) => break,
            }
        }
        if !self.config.commit_delay.is_zero() && group.len() < self.config.min_group_requests {
            let deadline = tokio::time::Instant::now() + self.config.commit_delay;
            while group.len() < self.config.min_group_requests && !full(&group, bytes) {
                match tokio::time::timeout_at(deadline, self.requests.recv()).await {
                    Ok(Some(request)) => {
                        bytes += request.approximate_bytes();
                        group.push(request);
                    }
                    _ => break,
                }
            }
        }
        group
    }

    /// Validate, apply to the private state, and encode a group. Returns the frames with their
    /// acknowledgements and the candidate `Version`, or `None` when no request was accepted.
    async fn prepare(
        &mut self,
        requests: Vec<WriteRequest>,
    ) -> Option<(PreparedRequests, Version)> {
        let Some(mut state) = self.state.take() else {
            for request in requests {
                let _ = request.ack().send(Err(self.handle.unavailable()));
            }
            return None;
        };
        let next_seq_no = self.next_seq_no;
        let rows = requests.iter().map(WriteRequest::rows).sum::<usize>();
        let (state, prepared, next_seq_no) = if rows < INLINE_PREPARE_ROWS {
            let (prepared, next) = prepare::prepare(&mut state, next_seq_no, requests);
            (state, prepared, next)
        } else {
            let prepared = run_cpu(&self.core.runtime().query, move || {
                let (prepared, next) = prepare::prepare(&mut state, next_seq_no, requests);
                (state, prepared, next)
            })
            .await;
            match prepared {
                Ok(prepared) => prepared,
                Err(error) => {
                    // The private state went down with the panic, and the requests' acks with
                    // it (their callers see the writer drop them).
                    self.poison(
                        PoisonKind::ReadOnly,
                        format!("preparing a write group failed: {error}"),
                    );
                    return None;
                }
            }
        };
        self.state = Some(state);
        self.next_seq_no = next_seq_no;
        if prepared.frames.is_empty() {
            return None;
        }
        let version = self.candidate()?;
        Some((prepared, version))
    }

    /// The next `Version` over the private state and the durable manifest.
    fn candidate(&mut self) -> Option<Version> {
        let state = self.state.as_ref()?;
        let version = Version::build(
            VersionId(self.next_version_id),
            Arc::clone(self.handle.meta()),
            Arc::clone(&state.schema),
            Arc::clone(&self.manifest),
            state.delta.clone(),
            self.live_files.values().cloned().collect(),
        );
        self.next_version_id += 1;
        Some(version)
    }

    /// Start the I/O of a prepared group on the I/O pool. When the group cannot be started (the
    /// collection was dropped or poisoned, or the engine is shutting down, while it was
    /// prepared), its writes fail and the private state goes back to `before`, so a drop that
    /// does not commit leaves the writer exactly where the log is.
    fn start(
        &mut self,
        prepared: PreparedRequests,
        version: Version,
        before: Option<(LogicalState, SeqNo)>,
    ) -> Option<InFlight> {
        let refusal = self.refusal();
        if refusal.is_some() || self.wal.is_none() {
            let refusal = refusal.unwrap_or_else(|| Refusal::Handle(Arc::clone(&self.handle)));
            fail_all(prepared.pending, || refusal.not_applied(&self.handle));
            if let Some((state, next_seq_no)) = before {
                self.state = Some(state);
                self.next_seq_no = next_seq_no;
            }
            return None;
        }
        let mut wal = self.wal.take()?;
        let mut frames = prepared.frames;
        if let Some(checkpoint) = self.pending_checkpoint.take() {
            frames.insert(0, checkpoint);
        }
        let rotation_checkpoint = self.checkpoint_frame().ok();
        let io = self.core.runtime().io.run(move || {
            if let Some(checkpoint) = rotation_checkpoint
                && wal.should_rotate()
                && let Err(error) = wal.rotate(&checkpoint)
            {
                if wal.failure().is_some() {
                    return (wal, Err(IoFailure::Rotate(error)));
                }
                tracing::warn!(%error, "WAL rotation failed; the group stays in the current file");
            }
            let result = wal.append_group(&frames).map(|_| ());
            (wal, result.map_err(IoFailure::Append))
        });
        Some(InFlight {
            io: Box::pin(io),
            pending: prepared.pending,
            version,
        })
    }

    /// Await a group's I/O and publish or fail it.
    async fn finish(&mut self, mut io: InFlight) {
        let done = (&mut io.io).await;
        self.complete(io.pending, io.version, done);
    }

    /// Publish a group whose I/O succeeded, or poison the collection.
    fn complete(&mut self, pending: Vec<Pending>, version: Version, done: Result<IoResult>) {
        match done {
            Ok((wal, Ok(()))) => {
                self.wal = Some(wal);
                // Store, notify, then acknowledge (I1).
                let version = self.handle.publish(version);
                let snapshot = version.snapshot();
                for pending in pending {
                    let _ = pending.ack.send(Ok(CommitAck {
                        last_seq_no: pending.last_seq_no,
                        applied_ops: pending.applied_ops,
                        snapshot: snapshot.clone(),
                    }));
                }
                self.after_publish(&version);
            }
            Ok((wal, Err(failure))) => {
                drop(wal);
                let outcome = self.wal_failed(failure);
                fail_all(pending, || self.write_failed(outcome));
            }
            Err(error) => {
                // The I/O pool lost the job and the WAL writer with it; nothing says whether the
                // group reached the disk.
                self.poison(
                    PoisonKind::Failed {
                        rollback_failed: false,
                    },
                    format!("the WAL I/O job failed: {error}"),
                );
                fail_all(pending, || {
                    self.write_failed(WriteOutcome::Unknown { fenced: false })
                });
            }
        }
    }

    /// Poison the collection after a WAL failure. Returns the outcome for the group's writes.
    fn wal_failed(&mut self, failure: IoFailure) -> WriteOutcome {
        let (error, group_written) = match failure {
            IoFailure::Rotate(error) => (error, false),
            IoFailure::Append(error) => (error, true),
        };
        let reason = error.to_string();
        let (kind, outcome) = match &error {
            WalError::WriteFailed { outcome, source } => {
                let kind = match outcome {
                    WriteOutcome::NotApplied => PoisonKind::ReadOnly,
                    WriteOutcome::Unknown { fenced: true } => PoisonKind::Failed {
                        rollback_failed: true,
                    },
                    WriteOutcome::Unknown { fenced: false } => {
                        if !is_crashed(source) {
                            // Nothing on disk records the hazard, so a later open in this boot
                            // could make frames visible that the device never got. Stop.
                            self.core.fatal(&LogPoseError::WalWriteFailed {
                                collection: self.handle.descriptor().lookup_name(),
                                outcome: *outcome,
                                reason: format!(
                                    "{reason}; the fence marker could not be written, so the \
                                     process must stop"
                                ),
                            });
                        }
                        PoisonKind::Failed {
                            rollback_failed: true,
                        }
                    }
                };
                (kind, *outcome)
            }
            // Only an interrupted write (a crash point) reports a plain I/O error from the
            // append path; the process is gone as far as the disk is concerned.
            WalError::Io { .. } => (
                PoisonKind::Failed {
                    rollback_failed: false,
                },
                WriteOutcome::Unknown { fenced: false },
            ),
            WalError::WriterFailed { outcome } => (
                match outcome {
                    WriteOutcome::NotApplied => PoisonKind::ReadOnly,
                    WriteOutcome::Unknown { .. } => PoisonKind::Failed {
                        rollback_failed: true,
                    },
                },
                WriteOutcome::NotApplied,
            ),
            _ => (PoisonKind::ReadOnly, WriteOutcome::NotApplied),
        };
        self.poison(kind, format!("a WAL write failed: {reason}"));
        if group_written {
            outcome
        } else {
            WriteOutcome::NotApplied
        }
    }

    /// The error a failed group's writes get.
    fn write_failed(&self, outcome: WriteOutcome) -> LogPoseError {
        LogPoseError::WalWriteFailed {
            collection: self.handle.descriptor().lookup_name(),
            outcome,
            reason: self
                .handle
                .poison_reason()
                .unwrap_or_else(|| "the WAL group failed".to_owned()),
        }
    }

    fn poison(&mut self, kind: PoisonKind, reason: String) {
        tracing::error!(
            collection = %self.handle.descriptor().lookup_name(),
            ?kind,
            %reason,
            "collection poisoned: it serves reads of its last published version until reopened"
        );
        self.handle.poison(kind, reason);
        // The WAL writer and the private state may now be ahead of anything durable; nothing
        // is built from them again.
        self.wal = None;
        self.fail_waiting_jobs();
    }

    /// After a publish: ask the scheduler for a flush or compaction when a threshold is reached.
    fn after_publish(&mut self, version: &Version) {
        let descriptor = self.handle.descriptor();
        let mut operations = Vec::new();
        if !self.requested[0] && should_flush(descriptor, version) {
            self.requested[0] = true;
            operations.push(MaintenanceOperation::Flush);
        } else if !self.requested[1] && should_compact(descriptor, version) {
            self.requested[1] = true;
            operations.push(MaintenanceOperation::Compact);
        }
        if operations.is_empty() {
            return;
        }
        // Queueing persists the maintenance status, which is blocking I/O.
        let core = self.core.clone();
        let handle = Arc::clone(&self.handle);
        let queued = self
            .core
            .runtime()
            .io
            .execute(move || core.enqueue_maintenance(&handle, operations));
        if queued.is_err() {
            self.requested = [false; 2];
        }
    }

    /// A checkpoint frame for the durable manifest.
    fn checkpoint_frame(&self) -> Result<WalFrame> {
        checkpoint_frame(&self.manifest)
    }

    async fn handle_control(&mut self, message: ControlMsg) -> Flow {
        match message {
            ControlMsg::BeginJob { kind, reply } => {
                if let Some(error) = self.refusal() {
                    let _ = reply.send(Err(error.clone_error()));
                } else if self.active_job.is_some() {
                    self.waiting_jobs.push_back((kind, reply));
                } else {
                    self.start_job(kind, reply).await;
                }
            }
            ControlMsg::CommitJob { commit, reply } => {
                let result = self.commit_job(*commit).await;
                let _ = reply.send(result);
                self.end_job(true).await;
            }
            ControlMsg::EndJob { wrote_files } => self.end_job(wrote_files).await,
            ControlMsg::Quiesce { reply } => {
                if self.active_job.is_none() {
                    let _ = reply.try_send(());
                } else {
                    self.quiesce_waiters.push(reply);
                }
                if self.handle.is_dropped() {
                    self.fail_waiting_jobs();
                }
            }
            ControlMsg::Shutdown => return Flow::Stop,
        }
        Flow::Continue
    }

    /// Make `kind` the active job and reply with the state it works from and its unit id.
    async fn start_job(&mut self, kind: JobKind, reply: oneshot::Sender<Result<JobStart>>) {
        match kind {
            JobKind::Flush => self.requested[0] = false,
            JobKind::Compact => self.requested[1] = false,
        }
        let Some(next_unit_id) = self.next_unit_id.checked_add(1) else {
            let _ = reply.send(Err(LogPoseError::internal(
                "the collection has used every unit id",
            )));
            return;
        };
        let unit = UnitId(self.next_unit_id);
        self.next_unit_id = next_unit_id;
        self.active_job = Some(ActiveJob {
            kind,
            unit,
            owns_files: true,
        });
        let current = self.handle.current();
        if kind == JobKind::Flush
            && !current.delta.is_empty()
            && let Err(error) = self.rotate_for_flush().await
        {
            self.active_job = None;
            let _ = reply.send(Err(error));
            Box::pin(self.start_next_job()).await;
            return;
        }
        let start = JobStart {
            version: current,
            unit,
        };
        if reply.send(Ok(start)).is_err() {
            // The job is gone before it wrote anything.
            self.active_job = None;
            Box::pin(self.start_next_job()).await;
        }
    }

    /// Rotate the WAL so that the delta a flush freezes ends in an older file than every later
    /// write, and a checkpoint falls on a file boundary. The pipeline is drained, so the private
    /// state equals the published one.
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
                } else {
                    self.wal_failed(IoFailure::Rotate(WalError::WriterFailed {
                        outcome: wal.failure().unwrap_or(WriteOutcome::NotApplied),
                    }));
                }
                Err(error.into())
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

    /// Publish the active job's manifest, then the `Version` over it; then release what the
    /// manifest superseded.
    ///
    /// The manifest takes the next generation from `next_manifest_gen`, which advances on every
    /// attempt. A publish that fails before the `CURRENT` rename abandons the job without a
    /// state change: its generation and unit are burned, and its files and partial manifest are
    /// removed right away because no durable manifest names them. A publish that fails at or
    /// after the rename poisons the collection, and nothing is removed.
    async fn commit_job(&mut self, commit: JobCommit) -> Result<Snapshot> {
        if let Some(error) = self.refusal() {
            return Err(error.clone_error());
        }
        let Some(job) = self.active_job else {
            return Err(LogPoseError::internal("no maintenance job is active"));
        };
        let Some(schema) = self.state.as_ref().map(|state| Arc::clone(&state.schema)) else {
            return Err(self.handle.unavailable());
        };
        let durable = Arc::clone(&self.manifest);
        let (segments, checkpoint, removed_units) = match commit {
            JobCommit::Flush {
                checkpoint_seq_no,
                segment,
            } => {
                if checkpoint_seq_no < durable.checkpoint_seq_no
                    || checkpoint_seq_no >= self.next_seq_no
                {
                    return Err(LogPoseError::internal(format!(
                        "flush checkpoint {checkpoint_seq_no} is outside the log (durable \
                         checkpoint {}, next sequence number {})",
                        durable.checkpoint_seq_no, self.next_seq_no
                    )));
                }
                let mut segments = durable.segments.clone();
                if let Some(segment) = segment {
                    check_job_unit(job, &segment)?;
                    segments.push(segment);
                }
                (segments, Some(checkpoint_seq_no), Vec::new())
            }
            JobCommit::Compact { inputs, output } => {
                check_job_unit(job, &output)?;
                let present = durable
                    .segments
                    .iter()
                    .filter(|segment| inputs.contains(&segment.unit))
                    .count();
                if inputs.is_empty() || present != inputs.len() {
                    return Err(LogPoseError::internal(
                        "compaction inputs are no longer in the manifest",
                    ));
                }
                let mut segments = durable
                    .segments
                    .iter()
                    .filter(|segment| !inputs.contains(&segment.unit))
                    .cloned()
                    .collect::<Vec<_>>();
                // Unit ids ascend with allocation: the job's unit is newer than every unit of
                // the durable manifest, so the list stays ascending.
                segments.push(output);
                (segments, None, inputs)
            }
        };
        let generation = self.next_manifest_gen;
        self.next_manifest_gen += 1;
        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            collection_id: durable.collection_id.clone(),
            generation,
            epoch: durable.epoch,
            checkpoint_seq_no: checkpoint.unwrap_or(durable.checkpoint_seq_no),
            schema: schema.as_ref().clone(),
            next_unit_id: self.next_unit_id,
            next_dv_gen: durable.next_dv_gen,
            segments,
            totals: durable.totals,
        }
        .with_totals();

        let core = self.core.clone();
        let dir = self.handle.meta().dir.clone();
        let to_publish = manifest.clone();
        let published = self
            .core
            .runtime()
            .io
            .run(move || publish_manifest(core.vfs.as_ref(), &dir, &to_publish))
            .await;
        match published {
            Ok(Ok(())) => {}
            Ok(Err(failure)) => {
                if failure.current_unknown {
                    // The rename may be visible in the page cache but not on disk: nothing may
                    // be built on either manifest, and no file of either may be removed, until
                    // a reopen's durability barrier settles it.
                    self.disown_job_files();
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
                    let dir = &self.handle.meta().dir;
                    let mut garbage = UnitFiles::new(dir, job.unit).all();
                    garbage.push(manifest_path(dir, generation));
                    self.core.gc.remove(garbage);
                    self.disown_job_files();
                }
                return Err(failure.error);
            }
            Err(error) => {
                self.disown_job_files();
                self.poison(
                    PoisonKind::ReadOnly,
                    format!("the manifest publish job failed: {error}"),
                );
                return Err(error);
            }
        }

        // The manifest is durable: install it.
        self.disown_job_files();
        let superseded_generation = self.previous_generation.replace(durable.generation);
        self.manifest = Arc::new(manifest);
        let dir = self.handle.meta().dir.clone();
        for segment in &self.manifest.segments {
            if !self.live_files.contains_key(&segment.unit) {
                let files = UnitFiles::new(&dir, segment.unit).published();
                self.live_files.insert(
                    segment.unit,
                    Arc::new(FileHandle::new(segment.unit, files, self.core.gc.clone())),
                );
            }
        }
        let mut retired = Vec::with_capacity(removed_units.len());
        for unit in removed_units {
            if let Some(file) = self.live_files.remove(&unit) {
                // Marked while this reference is still held, so the last holder (this writer,
                // a reader, or a pinned snapshot) sees the mark and removes the files.
                file.mark_obsolete();
                retired.push(file);
            }
        }
        if let Some(checkpoint) = checkpoint
            && let Some(state) = self.state.as_mut()
        {
            state.delta = state.delta.after(checkpoint);
            // Tell a WAL tailer what is safe to discard; it rides along with the next group.
            self.pending_checkpoint = self.checkpoint_frame().ok();
        }
        let Some(version) = self.candidate() else {
            return Err(self.handle.unavailable());
        };
        let version = self.handle.publish(version);
        // Older versions still hold the retired files until readers and tokens let go.
        drop(retired);
        if let Some(superseded) = superseded_generation {
            self.core.gc.remove([manifest_path(&dir, superseded)]);
        }
        if let Some(checkpoint) = checkpoint {
            self.remove_checkpointed_wal(checkpoint).await;
        }
        self.after_publish(&version);
        Ok(version.snapshot())
    }

    /// The active job's files are no longer its to remove.
    fn disown_job_files(&mut self) {
        if let Some(job) = self.active_job.as_mut() {
            job.owns_files = false;
        }
    }

    /// Delete the WAL files a durable manifest with checkpoint `checkpoint` made obsolete, on
    /// the I/O pool. Only after that manifest is durable: every operation in them is in a
    /// segment. A failure only delays the removal to the next checkpoint or open.
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

    /// The active job is over; remove what it wrote if no manifest names it, and start the next
    /// waiting job.
    async fn end_job(&mut self, wrote_files: bool) {
        if let Some(job) = self.active_job.take()
            && job.owns_files
            && wrote_files
        {
            self.core
                .gc
                .remove(UnitFiles::new(&self.handle.meta().dir, job.unit).all());
        }
        self.start_next_job().await;
    }

    async fn start_next_job(&mut self) {
        while self.active_job.is_none() {
            let Some((kind, reply)) = self.waiting_jobs.pop_front() else {
                for waiter in self.quiesce_waiters.drain(..) {
                    let _ = waiter.try_send(());
                }
                return;
            };
            if let Some(error) = self.refusal() {
                let _ = reply.send(Err(error.clone_error()));
                continue;
            }
            Box::pin(self.start_job(kind, reply)).await;
        }
    }

    fn fail_waiting_jobs(&mut self) {
        for (_, reply) in self.waiting_jobs.drain(..) {
            let _ = reply.send(Err(self.handle.unavailable()));
        }
    }

    /// Fail everything still queued and stop.
    fn stop(&mut self) {
        self.requests.close();
        while let Ok(request) = self.requests.try_recv() {
            let _ = request.ack().send(Err(shutting_down()));
        }
        for (_, reply) in self.waiting_jobs.drain(..) {
            let _ = reply.send(Err(shutting_down()));
        }
        for waiter in self.quiesce_waiters.drain(..) {
            let _ = waiter.try_send(());
        }
        self.wal = None;
    }
}

/// Await the in-flight I/O, or never when there is none.
async fn io_done(inflight: &mut Option<InFlight>) -> Result<IoResult> {
    match inflight {
        Some(io) => (&mut io.io).await,
        None => std::future::pending().await,
    }
}

/// Why the writer refuses work.
enum Refusal {
    /// The handle is dropped or poisoned; it builds the error.
    Handle(Arc<CollectionHandle>),
    /// The engine is shutting down.
    Shutdown,
}

impl Refusal {
    fn clone_error(&self) -> LogPoseError {
        match self {
            Self::Handle(handle) => handle.unavailable(),
            Self::Shutdown => shutting_down(),
        }
    }

    /// The error for writes that were prepared but never appended.
    fn not_applied(&self, handle: &CollectionHandle) -> LogPoseError {
        match self {
            Self::Handle(_) if handle.is_poisoned() => LogPoseError::WalWriteFailed {
                collection: handle.descriptor().lookup_name(),
                outcome: WriteOutcome::NotApplied,
                reason: handle
                    .poison_reason()
                    .unwrap_or_else(|| "an earlier WAL group failed".to_owned()),
            },
            _ => self.clone_error(),
        }
    }
}

/// Acknowledge every pending write of a group with the error `error` builds.
fn fail_all(pending: Vec<Pending>, error: impl Fn() -> LogPoseError) {
    for pending in pending {
        let _ = pending.ack.send(Err(error()));
    }
}

fn shutting_down() -> LogPoseError {
    LogPoseError::Message("the storage engine is shutting down".to_owned())
}

/// Fail unless `segment` is the active job's unit.
fn check_job_unit(job: ActiveJob, segment: &ManifestSegment) -> Result<()> {
    if segment.unit != job.unit {
        return Err(LogPoseError::internal(format!(
            "the {:?} job was allocated unit {} but committed unit {}",
            job.kind, job.unit, segment.unit
        )));
    }
    Ok(())
}

/// A checkpoint frame naming `manifest`'s generation and checkpoint.
pub(crate) fn checkpoint_frame(manifest: &Manifest) -> Result<WalFrame> {
    let payload = WalPayload::Checkpoint(CheckpointPayload {
        manifest_generation: manifest.generation,
        checkpoint_seq_no: manifest.checkpoint_seq_no,
    })
    .encode()
    .map_err(|error| LogPoseError::internal(format!("invalid WAL checkpoint frame: {error}")))?;
    Ok(WalFrame::checkpoint(manifest.checkpoint_seq_no, payload)?)
}
