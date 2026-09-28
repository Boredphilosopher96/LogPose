//! The single writer task of a collection and its group commit pipeline.
//!
//! Each collection has one writer task on the engine's writer runtime. It owns the collection's
//! [`WalWriter`], its private state (schema, memtables, segments, deletion vectors, and the
//! primary-key index, which run ahead of the published `Version` by at most one prepared group),
//! the durable manifest, and the maintenance job slot. Nothing else touches them, so the writer
//! takes no locks, and it is the only code that publishes a `Version`.
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
//! A flush begins by freezing the active memtable (and rotating the WAL, so the frozen
//! memtable's operations end in an older file than every later write); its commit installs the
//! new segment, reconciles the frozen memtable's late deletions onto it, and forwards the
//! primary-key index. A compaction's commit reconciles the deletions that reached its inputs
//! while it ran onto its output (and writes the output's DV file), then swaps the output in.
//!
//! A failed group append poisons the collection. The WAL layer rolls the file back to the last
//! synced group; the group's writes fail with [`LogPoseError::WalWriteFailed`] carrying the
//! rollback's outcome, and the next prepared group fails with `NotApplied`. A clean rollback
//! leaves the collection read-only until the engine is reopened. A failed rollback fences the WAL and
//! fails the collection for this process; when the fence could not be written either, the
//! engine's fatal handler stops the process.

mod apply;
#[cfg(test)]
mod dv_tests;
#[cfg(test)]
mod model_tests;
mod pk_index;
mod prepare;
#[cfg(test)]
mod tests;

pub(crate) use apply::{LogicalState, replay_frame};
pub(crate) use pk_index::{PkIndex, row_map};

use crate::{
    dv::{DeletionVector, DvFile, dv_path, write_dv_file},
    engine::CoreRef,
    fs_util::crash_point,
    handle::{CollectionHandle, PoisonKind},
    maintenance::{MaintenanceOperation, should_compact, should_flush},
    manifest::{
        DvRef, MANIFEST_FORMAT_VERSION, Manifest, ManifestSegment, manifest_path, publish_manifest,
    },
    memtable::MemtableData,
    paths::{SEGMENTS_DIR, segment_path},
    read::{FetchPlan, ReadView, RowSetResolver, SectionNeed},
    runtime::run_cpu,
    segment::SegmentHandle,
    version::{Version, VersionId},
};
use logpose_types::{
    CommitAck, LogPoseError, Result, RowAddr, SeqNo, Snapshot, UnitId, WriteOutcome,
    filter::FilterExpr,
    record::{ClientOp, PartialUpdate, PrimaryKey},
    schema::{CollectionSchema, ScalarFieldSpec, SchemaError},
};
use logpose_vfs::{CrashPoint, is_crashed};
use logpose_wal::{
    WalError, WalFrame, WalWriter,
    codec::{CheckpointPayload, RowImage, WalPayload},
};
use pk_index::{Forwarding, PK_REWRITE_SLICE, RewriteRows};
use prepare::{FetchedRows, Pending, PreparedRequests, prepares_inline};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future,
    path::PathBuf,
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
    /// Upserts, partial updates, and deletes by key; atomic.
    Batch { ops: Vec<ClientOp>, ack: Ack },
    /// A schema change; a batch of one.
    AlterSchema { change: SchemaChange, ack: Ack },
    /// Delete every live row matching a filter. Resolved once, against the writer's latest
    /// state, to a fixed key set that commits as one batch.
    DeleteByFilter { filter: FilterExpr, ack: Ack },
    /// Apply `patch` (its key is ignored) to every live row matching a filter; resolved like
    /// [`WriteRequest::DeleteByFilter`].
    UpdateByFilter {
        filter: FilterExpr,
        patch: PartialUpdate,
        ack: Ack,
    },
}

impl WriteRequest {
    fn ack(self) -> Ack {
        match self {
            Self::Batch { ack, .. }
            | Self::AlterSchema { ack, .. }
            | Self::DeleteByFilter { ack, .. }
            | Self::UpdateByFilter { ack, .. } => ack,
        }
    }

    /// Whether the request is resolved against the writer's state before it is prepared,
    /// which makes it a group of its own.
    fn is_filter(&self) -> bool {
        matches!(
            self,
            Self::DeleteByFilter { .. } | Self::UpdateByFilter { .. }
        )
    }

    fn rows(&self) -> usize {
        match self {
            Self::Batch { ops, .. } => ops.len(),
            Self::AlterSchema { .. }
            | Self::DeleteByFilter { .. }
            | Self::UpdateByFilter { .. } => 1,
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
                    ClientOp::Update(update) => {
                        64 + update
                            .vectors
                            .values()
                            .map(|vector| vector.len() * 4)
                            .sum::<usize>()
                            + 16 * update.fields.len()
                            + 32 * update.extra.len()
                    }
                    ClientOp::Delete(_) => 64,
                })
                .sum(),
            Self::AlterSchema { .. }
            | Self::DeleteByFilter { .. }
            | Self::UpdateByFilter { .. } => 256,
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
    pub(crate) work: JobWork,
}

/// What a job is to build.
pub(crate) enum JobWork {
    /// Nothing: no operation since the checkpoint (flush), or fewer than two segments
    /// (compaction).
    Nothing,
    Flush(FlushStart),
    Compact(CompactStart),
}

/// The inputs of a flush, captured at its begin.
pub(crate) struct FlushStart {
    /// The oldest frozen memtable, `F`.
    pub(crate) memtable: Arc<MemtableData>,
    /// `D_F`: `F`'s deleted slots at the begin; the flush skips them.
    pub(crate) deleted: DeletionVector,
    /// `D_S`: a new DV file generation for every segment whose deletion vector grew since its
    /// durable generation, with the vector to write.
    pub(crate) dvs: Vec<DvWrite>,
    /// `J`, the published `visible_seq_no` at the begin: every deletion at or below it is in
    /// the snapshots (and each is durable, I14).
    pub(crate) covered_seq_no: SeqNo,
}

/// One DV file a flush writes.
pub(crate) struct DvWrite {
    pub(crate) segment: Arc<SegmentHandle>,
    pub(crate) generation: u64,
    pub(crate) deletes: DeletionVector,
}

/// The inputs of a compaction, captured at its begin: every segment, ascending, with `D0`, its
/// deletion vector at the begin.
pub(crate) struct CompactStart {
    pub(crate) inputs: Vec<(Arc<SegmentHandle>, DeletionVector)>,
}

/// What a maintenance job built, for the writer to commit.
pub(crate) enum JobCommit {
    /// A flush of frozen memtable `memtable`, which ends at `checkpoint_seq_no`.
    Flush {
        memtable: UnitId,
        checkpoint_seq_no: SeqNo,
        /// The new segment; `None` when no slot was live (a pure checkpoint).
        segment: Option<FlushedSegment>,
        /// The DV files written, by segment.
        dvs: Vec<(UnitId, DvRef)>,
    },
    /// A compaction of `inputs` into `output` (`None` when every input row was deleted).
    Compact {
        inputs: Vec<UnitId>,
        output: Option<CompactedSegment>,
    },
}

/// A flush output.
pub(crate) struct FlushedSegment {
    pub(crate) handle: Arc<SegmentHandle>,
    /// Frozen slot to segment row, `u32::MAX` for a skipped (deleted) slot.
    pub(crate) slot_to_row: Arc<[u32]>,
    /// Segment row to frozen slot.
    pub(crate) row_to_slot: Arc<[u32]>,
}

/// A compaction output.
pub(crate) struct CompactedSegment {
    pub(crate) handle: Arc<SegmentHandle>,
    /// Per input, in input order: input row to output row, `u32::MAX` for a row in `D0`.
    pub(crate) maps: Vec<Arc<[u32]>>,
    /// Output rows' keys and source addresses, in output row order.
    pub(crate) pks: Arc<[PrimaryKey]>,
    pub(crate) sources: Arc<[RowAddr]>,
}

/// Messages polled before client requests.
pub(crate) enum ControlMsg {
    /// Start a maintenance job once no other job of the collection is active. The writer drains
    /// the pipeline and, for a flush, freezes the active memtable (rotating the WAL) unless an
    /// earlier flush left one frozen; the reply is the published (hence durable) state the job
    /// works from.
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
    /// files, which are then removed: no durable manifest names them.
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
/// the handle's first published `Version`), the durable manifest, and the id counters.
pub(crate) struct WriterSeed {
    pub(crate) wal: WalWriter,
    pub(crate) state: LogicalState,
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) next_seq_no: SeqNo,
    pub(crate) version_id: VersionId,
    /// The manifest generation kept beside the durable one, if any.
    pub(crate) previous_generation: Option<u64>,
    /// The first manifest generation to issue.
    pub(crate) next_manifest_gen: u64,
    /// The first unit id to issue.
    pub(crate) next_unit_id: u32,
    /// The first DV file generation to issue.
    pub(crate) next_dv_gen: u64,
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
    handle.set_pk_index_bytes(seed.state.pk.approximate_bytes());
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
        next_dv_gen: seed.next_dv_gen,
        manifest: seed.manifest,
        previous_generation: seed.previous_generation,
        next_seq_no: seed.next_seq_no,
        next_version_id: seed.version_id.0 + 1,
        pending_checkpoint: None,
        active_job: None,
        waiting_jobs: VecDeque::new(),
        quiesce_waiters: Vec::new(),
        requested: [false; 2],
        held: None,
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
    /// The private state; `None` only while a large group is prepared on the query pool, or
    /// after that preparation panicked.
    state: Option<LogicalState>,
    /// The durable manifest.
    manifest: Arc<Manifest>,
    /// The manifest generation kept beside the durable one (for inspection); the next commit
    /// removes it.
    previous_generation: Option<u64>,
    /// Next manifest generation to try. Advances on every publish attempt, so a generation is
    /// never written twice, even by a retry.
    next_manifest_gen: u64,
    /// Next unit id to allocate; recorded in every manifest.
    next_unit_id: u32,
    /// Next DV file generation to allocate; recorded in every manifest.
    next_dv_gen: u64,
    next_seq_no: SeqNo,
    next_version_id: u64,
    /// A checkpoint frame to prepend to the next group, written after a flush commit.
    pending_checkpoint: Option<WalFrame>,
    active_job: Option<ActiveJob>,
    waiting_jobs: VecDeque<(JobKind, oneshot::Sender<Result<JobStart>>)>,
    quiesce_waiters: Vec<std::sync::mpsc::SyncSender<()>>,
    /// Whether a flush (0) or compaction (1) was already requested from the scheduler.
    requested: [bool; 2],
    /// A request taken from the channel while collecting a group that must start a group of its
    /// own (a filter request); handled before the channel is read again.
    held: Option<WriteRequest>,
}

/// The maintenance job that holds the collection's job slot.
#[derive(Clone, Debug)]
struct ActiveJob {
    kind: JobKind,
    unit: UnitId,
    /// DV files the job may write, besides its unit's segment.
    dv_files: Vec<PathBuf>,
    /// Whether the job's files are still the job's to clean up. Cleared once a commit made them
    /// live, or made their durability unknown.
    owns_files: bool,
}

impl ActiveJob {
    /// Every file an attempt of this job may have created.
    fn files(&self, dir: &std::path::Path) -> Vec<PathBuf> {
        let mut files = vec![segment_path(dir, self.unit)];
        files.extend(self.dv_files.iter().cloned());
        files
    }
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
                    self.rewrite_slice();
                }
                done = io_done(&mut inflight), if inflight.is_some() => {
                    if let Some(io) = inflight.take() {
                        self.complete(io.pending, io.version, done);
                    }
                    self.rewrite_slice();
                }
                request = self.requests.recv() => {
                    let Some(request) = request else { break };
                    self.handle_request(request, &mut inflight).await;
                    while let Some(held) = self.held.take() {
                        self.handle_request(held, &mut inflight).await;
                    }
                }
            }
        }
        if let Some(io) = inflight.take() {
            self.finish(io).await;
        }
        self.stop().await;
    }

    /// Collect a group starting at `first`, prepare it, and start its I/O once the group in
    /// flight is published.
    async fn handle_request(&mut self, first: WriteRequest, inflight: &mut Option<InFlight>) {
        let mut requests = self.collect(first).await;
        if let Some(error) = self.refusal() {
            for request in requests {
                let _ = request.ack().send(Err(error.clone_error()));
            }
            return;
        }
        if requests.len() == 1 && requests[0].is_filter() {
            let Some(request) = requests.pop() else {
                return;
            };
            match self.resolve_filter_request(request).await {
                Some(batch) => requests.push(batch),
                None => return,
            }
        }
        // The private state before this group, to restore if the group is never
        // appended: the log must not skip the sequence numbers it took.
        let before = self
            .state
            .as_mut()
            .map(|state| (state.savepoint(), self.next_seq_no));
        let prepared = self.prepare(requests).await;
        if let Some(io) = inflight.take() {
            self.finish(io).await;
        }
        match prepared {
            Some((prepared, version)) => {
                *inflight = self.start(prepared, version, before);
            }
            None => {
                if let Some(state) = self.state.as_mut() {
                    state.release_savepoint();
                }
            }
        }
        self.rewrite_slice();
    }

    /// Resolve a filter request against the private state, which includes every earlier
    /// request (prepared groups whose I/O is still in flight too), into a batch over the
    /// matching keys. Returns `None` after answering the request itself: with its error, or
    /// right away when no row matches (nothing is logged and no sequence number is used).
    async fn resolve_filter_request(&mut self, request: WriteRequest) -> Option<WriteRequest> {
        let (filter, patch, ack) = match request {
            WriteRequest::DeleteByFilter { filter, ack } => (filter, None, ack),
            WriteRequest::UpdateByFilter { filter, patch, ack } => (filter, Some(patch), ack),
            other => return Some(other),
        };
        let Some(resolver) = self.core.resolver.clone() else {
            let _ = ack.send(Err(LogPoseError::failed_precondition(
                "filter writes need a row-set resolver, and this engine has none",
            )));
            return None;
        };
        let Some(state) = self.state.as_ref() else {
            let _ = ack.send(Err(self.handle.unavailable()));
            return None;
        };
        let version = Arc::new(state.version(
            VersionId(self.next_version_id),
            Arc::clone(self.handle.meta()),
            Arc::clone(&self.manifest),
        ));
        let view = ReadView::new(version, Arc::downgrade(self.core.arc()), None);
        let keys = match resolve_keys(resolver.as_ref(), &view, &filter).await {
            Ok(keys) => keys,
            Err(error) => {
                let _ = ack.send(Err(error));
                return None;
            }
        };
        if keys.is_empty() {
            let current = self.handle.current();
            let _ = ack.send(Ok(CommitAck {
                last_seq_no: current.visible_seq_no,
                applied_ops: 0,
                snapshot: current.snapshot(),
            }));
            return None;
        }
        let ops = keys
            .into_iter()
            .map(|pk| match &patch {
                None => ClientOp::Delete(pk),
                Some(patch) => {
                    let mut update = patch.clone();
                    update.pk = pk;
                    ClientOp::Update(update)
                }
            })
            .collect();
        Some(WriteRequest::Batch { ops, ack })
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

    /// Take `first` and whatever else is queued, up to the group limits. A filter request is
    /// a group of its own: it is resolved against the state every earlier request left, so a
    /// filter request met while collecting is held for the next group.
    async fn collect(&mut self, first: WriteRequest) -> Vec<WriteRequest> {
        if first.is_filter() {
            return vec![first];
        }
        let mut bytes = first.approximate_bytes();
        let mut group = vec![first];
        let full = |group: &Vec<WriteRequest>, bytes: usize| {
            group.len() >= self.config.max_group_requests.max(1)
                || bytes >= self.config.max_group_bytes
        };
        while !full(&group, bytes) {
            match self.requests.try_recv() {
                Ok(request) if request.is_filter() => {
                    self.held = Some(request);
                    return group;
                }
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
                    Ok(Some(request)) if request.is_filter() => {
                        self.held = Some(request);
                        return group;
                    }
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
        let fetched = self.fetch_update_rows(&state, &requests).await;
        let next_seq_no = self.next_seq_no;
        let rows = requests.iter().map(WriteRequest::rows).sum::<usize>();
        let bytes = requests
            .iter()
            .map(WriteRequest::approximate_bytes)
            .sum::<usize>();
        let (state, prepared, next_seq_no) = if prepares_inline(rows, bytes) {
            let (prepared, next) = prepare::prepare(&mut state, next_seq_no, requests, &fetched);
            (state, prepared, next)
        } else {
            let prepared = run_cpu(&self.core.runtime().query, move || {
                let (prepared, next) =
                    prepare::prepare(&mut state, next_seq_no, requests, &fetched);
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
        if let Some(reason) = prepared.fatal {
            self.poison(
                PoisonKind::ReadOnly,
                format!("a write violated an engine invariant: {reason}"),
            );
            let error = LogPoseError::WalWriteFailed {
                collection: self.handle.descriptor().lookup_name(),
                outcome: WriteOutcome::NotApplied,
                reason,
            };
            fail_all(prepared.pending, || error.clone());
            return None;
        }
        if prepared.frames.is_empty() {
            return None;
        }
        let version = self.candidate()?;
        Some((prepared, version))
    }

    /// Read, on the I/O pool, the rows that the group's partial updates change and that live in
    /// segments. Memtable rows are read during prepare. A key is resolved against the state
    /// before the group; an earlier request of the same group can only move the key into the
    /// active memtable or delete it, never to another segment row, so the fetch covers every
    /// segment row prepare can ask for.
    async fn fetch_update_rows(
        &self,
        state: &LogicalState,
        requests: &[WriteRequest],
    ) -> FetchedRows {
        let mut wanted = BTreeMap::<UnitId, Vec<u32>>::new();
        for request in requests {
            let WriteRequest::Batch { ops, .. } = request else {
                continue;
            };
            for op in ops {
                let ClientOp::Update(update) = op else {
                    continue;
                };
                if let Ok(Some(addr)) = state.pk.resolve(&update.pk)
                    && state.memtable(addr.unit).is_none()
                {
                    wanted.entry(addr.unit).or_default().push(addr.row);
                }
            }
        }
        if wanted.is_empty() {
            return FetchedRows::new();
        }
        let reads = wanted
            .into_iter()
            .filter_map(|(unit, mut rows)| {
                rows.sort_unstable();
                rows.dedup();
                let segment = state.segments.iter().find(|segment| segment.unit == unit)?;
                Some((Arc::clone(segment), rows))
            })
            .collect::<Vec<_>>();
        let addrs = reads
            .iter()
            .flat_map(|(segment, rows)| {
                rows.iter().map(|row| RowAddr {
                    unit: segment.unit,
                    row: *row,
                })
            })
            .collect::<Vec<_>>();
        let fetched = self
            .core
            .runtime()
            .io
            .run(move || {
                let mut fetched = FetchedRows::new();
                for (segment, rows) in reads {
                    match segment.row_images(&rows) {
                        Ok(images) => {
                            for (row, image) in rows.into_iter().zip(images) {
                                fetched.insert(
                                    RowAddr {
                                        unit: segment.unit,
                                        row,
                                    },
                                    Ok(image),
                                );
                            }
                        }
                        Err(error) => {
                            for row in rows {
                                fetched.insert(
                                    RowAddr {
                                        unit: segment.unit,
                                        row,
                                    },
                                    Err(error.clone()),
                                );
                            }
                        }
                    }
                }
                fetched
            })
            .await;
        match fetched {
            Ok(fetched) => fetched,
            Err(error) => addrs
                .into_iter()
                .map(|addr| (addr, Err(error.clone())))
                .collect(),
        }
    }

    /// The next `Version` over the private state and the durable manifest.
    fn candidate(&mut self) -> Option<Version> {
        let state = self.state.as_ref()?;
        let version = state.version(
            VersionId(self.next_version_id),
            Arc::clone(self.handle.meta()),
            Arc::clone(&self.manifest),
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
        before: Option<(apply::Savepoint, SeqNo)>,
    ) -> Option<InFlight> {
        let refusal = self.refusal();
        if refusal.is_some() || self.wal.is_none() {
            let refusal = refusal.unwrap_or_else(|| Refusal::Handle(Arc::clone(&self.handle)));
            fail_all(prepared.pending, || refusal.not_applied(&self.handle));
            if let Some((savepoint, next_seq_no)) = before
                && let Some(state) = self.state.as_mut()
            {
                state.restore(savepoint);
                self.next_seq_no = next_seq_no;
            }
            return None;
        }
        if let Some(state) = self.state.as_mut() {
            state.release_savepoint();
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

    /// After a publish: ask the scheduler for a flush or compaction when a threshold is reached,
    /// and report the primary-key index's size for the cache budget.
    fn after_publish(&mut self, version: &Version) {
        if let Some(state) = &self.state {
            self.handle.set_pk_index_bytes(state.pk.approximate_bytes());
        }
        let descriptor = self.handle.descriptor();
        let flush_running = self
            .active_job
            .as_ref()
            .is_some_and(|job| job.kind == JobKind::Flush);
        let mut operations = Vec::new();
        let wants_flush = should_flush(descriptor, &self.core.memtable, version, self.clock_now())
            || (!version.frozen.is_empty() && !flush_running);
        if !self.requested[0] && wants_flush {
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

    fn clock_now(&self) -> Duration {
        self.core.tokens.clock.now()
    }

    /// Rewrite one slice of primary-key entries that still point into retired units. Only
    /// between groups: never while a prepared group could still be taken back.
    fn rewrite_slice(&mut self) {
        if let Some(state) = self.state.as_mut()
            && state.pk.rewriting()
        {
            state.pk.rewrite_slice(PK_REWRITE_SLICE);
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
                // A drop voids every outstanding maintenance request: waiting jobs fail below,
                // and a job that has not begun is refused. Should the drop not commit, the
                // next publish over a threshold requests again, instead of a flag left set by a
                // request that never began blocking every later one.
                self.requested = [false; 2];
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

    /// Make `kind` the active job and reply with the state it works from and its unit id.
    async fn start_job(&mut self, kind: JobKind, reply: oneshot::Sender<Result<JobStart>>) {
        match kind {
            JobKind::Flush => self.requested[0] = false,
            JobKind::Compact => self.requested[1] = false,
        }
        let unit = match self.allocate_unit() {
            Ok(unit) => unit,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        self.active_job = Some(ActiveJob {
            kind,
            unit,
            dv_files: Vec::new(),
            owns_files: true,
        });
        let work = match kind {
            JobKind::Flush => self.begin_flush().await,
            JobKind::Compact => Ok(self.begin_compaction()),
        };
        let work = match work {
            Ok(work) => work,
            Err(error) => {
                self.active_job = None;
                let _ = reply.send(Err(error));
                Box::pin(self.start_next_job()).await;
                return;
            }
        };
        let start = JobStart {
            version: self.handle.current(),
            unit,
            work,
        };
        if reply.send(Ok(start)).is_err() {
            // The job is gone before it wrote anything.
            self.active_job = None;
            Box::pin(self.start_next_job()).await;
        }
    }

    /// Capture a flush's inputs. Unless an earlier flush left a memtable frozen, freeze the
    /// active one first: rotate the WAL so the new file starts at the next sequence number,
    /// move the active memtable into `frozen`, start a new one with a fresh unit id, and
    /// publish. The pipeline is drained, so the private state equals the published one.
    async fn begin_flush(&mut self) -> Result<JobWork> {
        let needs_freeze = self
            .state
            .as_ref()
            .is_some_and(|state| state.frozen.is_empty() && state.active.has_ops());
        if needs_freeze {
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
            if let Some(job) = self.active_job.as_mut() {
                job.dv_files.push(dv_path(&dir, segment.unit, generation));
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

    /// Capture a compaction's inputs: every segment, with its deletion vector now.
    fn begin_compaction(&self) -> JobWork {
        let Some(state) = self.state.as_ref() else {
            return JobWork::Nothing;
        };
        if state.segments.len() <= 1 {
            return JobWork::Nothing;
        }
        JobWork::Compact(CompactStart {
            inputs: state
                .segments
                .iter()
                .map(|segment| {
                    (
                        Arc::clone(segment),
                        state.deletes.get(segment.unit).cloned().unwrap_or_default(),
                    )
                })
                .collect(),
        })
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

    /// Publish the active job's manifest, then the `Version` over it; then release what the
    /// manifest superseded.
    ///
    /// The manifest takes the next generation from `next_manifest_gen`, which advances on every
    /// attempt. A publish that fails before the `CURRENT` rename abandons the job without a
    /// state change: its generation, unit, and DV generations are burned, and its files and
    /// partial manifest are removed right away because no durable manifest names them. A
    /// publish that fails at or after the rename poisons the collection, and nothing is
    /// removed.
    async fn commit_job(&mut self, commit: JobCommit) -> Result<Snapshot> {
        if let Some(error) = self.refusal() {
            return Err(error.clone_error());
        }
        let Some(job) = self.active_job.clone() else {
            return Err(LogPoseError::internal("no maintenance job is active"));
        };
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
                if oldest.map(|frozen| (frozen.unit, frozen.last_seq_no))
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
                    check_job_unit(&job, &segment.handle.entry)?;
                    segments.push(segment.handle.entry.clone());
                    segments.sort_by_key(|entry| entry.unit);
                }
                let superseded_dvs = durable
                    .segments
                    .iter()
                    .filter(|entry| dv_by_unit.contains_key(&entry.unit))
                    .filter_map(|entry| entry.dv.map(|dv| dv_path(&dir, entry.unit, dv.generation)))
                    .chain(
                        // A DV file written for a segment that is no longer in the manifest.
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
                if let Some(output) = &output {
                    check_job_unit(&job, &output.handle.entry)?;
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
                let mut output_entry = output.as_ref().map(|output| output.handle.entry.clone());
                if let (Some(entry), false) = (&mut output_entry, reconciled.is_empty()) {
                    let generation = self.next_dv_gen;
                    self.next_dv_gen += 1;
                    let path = dv_path(&dir, entry.unit, generation);
                    if let Some(job) = self.active_job.as_mut() {
                        job.dv_files.push(path.clone());
                    }
                    let file = DvFile {
                        unit: entry.unit,
                        row_count: entry.row_count,
                        generation,
                        covered_seq_no: state.visible_seq_no(),
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
                            self.abandon_job_files();
                            return Err(error);
                        }
                    }
                    entry.dv = Some(DvRef {
                        generation,
                        cardinality: u32::try_from(reconciled.len()).unwrap_or(u32::MAX),
                        covered_seq_no: state.visible_seq_no(),
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
                    self.core.gc.remove([manifest_path(&dir, generation)]);
                    self.abandon_job_files();
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
        let Some(state) = self.state.as_mut() else {
            return Err(self.handle.unavailable());
        };
        let (retired, superseded_dvs, checkpointed) = match install {
            Install::Flush {
                memtable,
                segment,
                superseded_dvs,
            } => {
                state.install_flush(memtable, segment);
                // Tell a WAL tailer what is safe to discard; it rides along with the next group.
                self.pending_checkpoint = checkpoint_frame(&self.manifest).ok();
                (Vec::new(), superseded_dvs, Some(checkpoint))
            }
            Install::Compact {
                inputs,
                output,
                reconciled,
                superseded_dvs,
            } => {
                let retired = state.install_compaction(&inputs, output, reconciled);
                (retired, superseded_dvs, None)
            }
        };
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
        self.after_publish(&version);
        Ok(version.snapshot())
    }

    /// The active job's files are no longer its to remove.
    fn disown_job_files(&mut self) {
        if let Some(job) = self.active_job.as_mut() {
            job.owns_files = false;
        }
    }

    /// Remove every file the active job may have written: no durable manifest names them.
    fn abandon_job_files(&mut self) {
        if let Some(job) = self.active_job.as_mut()
            && job.owns_files
        {
            job.owns_files = false;
            self.core.gc.remove(job.files(&self.handle.meta().dir));
        }
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

    /// The active job is over; remove what it wrote if no manifest names it, and start the next
    /// waiting job.
    async fn end_job(&mut self, wrote_files: bool) {
        if let Some(job) = self.active_job.take()
            && job.owns_files
            && wrote_files
        {
            self.core.gc.remove(job.files(&self.handle.meta().dir));
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
    ///
    /// Both channels are closed and then drained with `recv`, which after a close also waits
    /// for a send that already passed the channel's open check but has not queued its message
    /// yet. Dropping a receiver, or draining it with `try_recv`, misses such a message: it then
    /// sits in the channel with its reply sender alive, and its caller (a client write, a job
    /// thread blocked on a begin or commit, a quiescing drop) waits forever, which hangs engine
    /// shutdown.
    async fn stop(&mut self) {
        self.requests.close();
        self.control.close();
        while let Some(request) = self.requests.recv().await {
            let _ = request.ack().send(Err(shutting_down()));
        }
        while let Some(message) = self.control.recv().await {
            match message {
                ControlMsg::BeginJob { reply, .. } => {
                    let _ = reply.send(Err(shutting_down()));
                }
                ControlMsg::CommitJob { reply, .. } => {
                    let _ = reply.send(Err(shutting_down()));
                }
                ControlMsg::Quiesce { reply } => self.quiesce_waiters.push(reply),
                ControlMsg::EndJob { .. } | ControlMsg::Shutdown => {}
            }
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

/// What a durable job manifest changes in the private state.
enum Install {
    Flush {
        memtable: UnitId,
        segment: Option<FlushedSegment>,
        superseded_dvs: Vec<PathBuf>,
    },
    Compact {
        inputs: Vec<UnitId>,
        output: Option<CompactedSegment>,
        reconciled: DeletionVector,
        superseded_dvs: Vec<PathBuf>,
    },
}

impl LogicalState {
    /// Freeze the active memtable: it joins `frozen` and a new, empty one with unit `unit`
    /// starts at the next sequence number.
    pub(crate) fn freeze(&mut self, unit: UnitId, now: Duration) {
        let next = MemtableData::new(
            unit,
            Arc::clone(&self.schema),
            self.visible_seq_no() + 1,
            now,
        );
        let frozen = std::mem::replace(&mut self.active, next);
        self.frozen.push(Arc::new(frozen));
    }

    /// Install a durable flush of frozen memtable `memtable` into `segment`: drop the memtable,
    /// add the segment with the memtable's late deletions (slots deleted after the job's
    /// snapshot, mapped to their segment rows), and forward the primary-key index from the
    /// memtable to the segment.
    fn install_flush(&mut self, memtable: UnitId, segment: Option<FlushedSegment>) {
        let Some(index) = self
            .frozen
            .iter()
            .position(|frozen| frozen.unit == memtable)
        else {
            return;
        };
        let frozen = self.frozen.remove(index);
        let deletes = self.deletes.remove(memtable).unwrap_or_default();
        self.counters.total_rows -= u64::from(frozen.slot_count());
        self.counters.deleted_rows -= deletes.len();
        self.counters.memtable_rows -= u64::from(frozen.slot_count());
        self.counters.memtable_bytes = self
            .counters
            .memtable_bytes
            .saturating_sub(frozen.bytes().total());
        let Some(segment) = segment else {
            return;
        };
        let mut late = DeletionVector::default();
        for slot in deletes.iter() {
            if let Some(&row) = segment.slot_to_row.get(slot as usize)
                && row != u32::MAX
            {
                late.mark(row);
            }
        }
        let unit = segment.handle.unit;
        self.counters.total_rows += u64::from(segment.handle.row_count());
        self.counters.deleted_rows += late.len();
        self.counters.segment_count += 1;
        self.deletes.set(unit, late);
        let mut segments = self.segments.to_vec();
        segments.push(Arc::clone(&segment.handle));
        segments.sort_by_key(|segment| segment.unit);
        self.segments = Arc::from(segments);
        self.pk.retire(
            vec![(
                memtable,
                Forwarding {
                    target: unit,
                    map: Arc::clone(&segment.slot_to_row),
                },
            )],
            unit,
            RewriteRows::Flush {
                memtable: frozen,
                row_to_slot: segment.row_to_slot,
            },
        );
    }

    /// Install a durable compaction of `inputs` into `output` with its reconciled deletions:
    /// swap the segments, mark the inputs obsolete, and forward the primary-key index. Returns
    /// the retired inputs, for the caller to drop after publishing.
    fn install_compaction(
        &mut self,
        inputs: &[UnitId],
        output: Option<CompactedSegment>,
        reconciled: DeletionVector,
    ) -> Vec<Arc<SegmentHandle>> {
        let mut retired = Vec::new();
        let mut kept = Vec::new();
        for segment in self.segments.iter() {
            if inputs.contains(&segment.unit) {
                retired.push(Arc::clone(segment));
            } else {
                kept.push(Arc::clone(segment));
            }
        }
        for segment in &retired {
            let deletes = self.deletes.remove(segment.unit).unwrap_or_default();
            self.counters.total_rows -= u64::from(segment.row_count());
            self.counters.deleted_rows -= deletes.len();
            self.counters.segment_count -= 1;
            // Marked while this reference is still held, so the last holder (a published
            // version, a reader, or a pinned snapshot) sees the mark and removes the file.
            segment.mark_obsolete();
        }
        if let Some(output) = output {
            let unit = output.handle.unit;
            self.counters.total_rows += u64::from(output.handle.row_count());
            self.counters.deleted_rows += reconciled.len();
            self.counters.segment_count += 1;
            self.deletes.set(unit, reconciled);
            kept.push(Arc::clone(&output.handle));
            kept.sort_by_key(|segment| segment.unit);
            let forwards = inputs
                .iter()
                .zip(output.maps)
                .map(|(input, map)| (*input, Forwarding { target: unit, map }))
                .collect();
            self.pk.retire(
                forwards,
                unit,
                RewriteRows::Compaction {
                    pks: output.pks,
                    sources: output.sources,
                },
            );
        }
        self.segments = Arc::from(kept);
        retired
    }

    /// A `Version` over this state and the durable `manifest`.
    pub(crate) fn version(
        &self,
        id: VersionId,
        meta: Arc<crate::handle::CollectionMeta>,
        manifest: Arc<Manifest>,
    ) -> Version {
        Version {
            id,
            meta,
            schema: Arc::clone(&self.schema),
            visible_seq_no: self.visible_seq_no(),
            manifest_generation: manifest.generation,
            checkpoint_seq_no: manifest.checkpoint_seq_no,
            counters: self.counters,
            segments: Arc::clone(&self.segments),
            frozen: Arc::from(self.frozen.clone()),
            active: Arc::new(self.active.clone()),
            deletes: self.deletes.clone(),
            manifest,
        }
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
    LogPoseError::unavailable("the storage engine is shutting down")
}

/// Fail unless `segment` is the active job's unit.
fn check_job_unit(job: &ActiveJob, segment: &ManifestSegment) -> Result<()> {
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

/// The keys of the live rows `filter` matches in `view`, in unit order.
async fn resolve_keys(
    resolver: &dyn RowSetResolver,
    view: &ReadView,
    filter: &FilterExpr,
) -> Result<Vec<PrimaryKey>> {
    let matches = resolver.resolve(view, filter).await?;
    let mut plan = FetchPlan::default();
    for (unit, rows) in &matches {
        if !rows.is_empty() {
            plan.push(*unit, SectionNeed::Pk);
        }
    }
    let (pins, _) = view.fetch(&plan).await?;
    let mut keys = Vec::new();
    for (unit, rows) in matches {
        if rows.is_empty() {
            continue;
        }
        let unit_view = view
            .unit(unit)
            .ok_or_else(|| LogPoseError::internal(format!("unit {unit} is not in the view")))?;
        let pks = unit_view.pks(&pins)?;
        for row in &rows {
            if unit_view.is_deleted(row) {
                continue;
            }
            keys.push(pks.pk_at(row).ok_or_else(|| {
                LogPoseError::internal(format!("row {row} of unit {unit} has no key"))
            })?);
        }
    }
    Ok(keys)
}

/// Every row image a segment's rows hold, for partial updates.
impl SegmentHandle {
    pub(crate) fn row_images(&self, rows: &[u32]) -> Result<Vec<RowImage>> {
        self.reader()
            .row_images(rows)
            .map_err(|error| crate::segment::segment_error(self.path(), error))
    }
}
