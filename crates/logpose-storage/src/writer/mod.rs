//! The single writer task of a collection and its group commit pipeline.
//!
//! Each collection has one writer task on the engine's writer runtime. It owns the collection's
//! [`WalWriter`], its private state (schema, memtables, segments, deletion vectors, and the
//! primary-key index, which run ahead of the published `Version` by at most one prepared group),
//! the durable manifest, and the collection's maintenance jobs. Nothing else touches them, so
//! the writer takes no locks, and it is the only code that publishes a `Version`.
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
//! Control messages (scheduler permits, job builds that finished, explicit flushes and
//! compactions, quiesce for a drop, shutdown) are polled first with a biased select, and each
//! one drains the pipeline before it is handled: a continuous stream of writes can delay a
//! flush or compaction publish by at most one group. Maintenance itself is in `jobs.rs`: a
//! flush trigger freezes the active memtable once the pipeline is drained (rotating the WAL, so
//! the frozen memtable's operations end in an older file than every later write), with
//! `max_frozen` memtables frozen writes stall, and each flush or compaction the size-tiered
//! policy plans waits for a scheduler permit. A flush's commit installs the new segment,
//! reconciles the frozen memtable's late deletions onto it, and forwards the primary-key index.
//! A compaction's commit reconciles the deletions that reached its inputs while it ran onto its
//! output (and writes the output's DV file), then swaps the output in.
//!
//! A failed group append poisons the collection. The WAL layer rolls the file back to the last
//! synced group; the group's writes fail with [`LogPoseError::WalWriteFailed`] carrying the
//! rollback's outcome, and the next prepared group fails with `NotApplied`. A clean rollback
//! leaves the collection read-only until the engine is reopened. A failed rollback fences the WAL and
//! fails the collection for this process; when the fence could not be written either, the
//! engine's fatal handler stops the process.

mod apply;
#[cfg(test)]
mod compaction_tests;
#[cfg(test)]
mod dv_tests;
#[cfg(test)]
mod failure_tests;
mod jobs;
mod pk_index;
mod prepare;
#[cfg(test)]
mod status_tests;
#[cfg(test)]
mod tests;

pub(crate) use apply::{LogicalState, replay_frame};
pub(crate) use pk_index::{PkIndex, row_map};

use crate::{
    compaction::Policy,
    dv::DeletionVector,
    engine::CoreRef,
    handle::{CollectionHandle, PoisonKind},
    maintenance::should_flush,
    manifest::{DvRef, IndexRef, Manifest, ManifestSegment},
    memtable::MemtableData,
    paths::segment_path,
    read::{FetchPlan, ReadView, RowSetResolver, SectionNeed},
    runtime::run_cpu,
    scheduler::{Permit, RequestId},
    segment::{OpenFile, SegmentHandle},
    version::{Version, VersionId},
};
use logpose_types::{
    CommitAck, LogPoseError, Result, RowAddr, SeqNo, Snapshot, UnitId, WriteOutcome,
    filter::FilterExpr,
    record::{ClientOp, PartialUpdate, PrimaryKey},
    schema::FieldId,
};
use logpose_vfs::is_crashed;
use logpose_wal::{
    WalError, WalFrame, WalWriter,
    codec::{CheckpointPayload, RowImage, WalPayload},
};
use pk_index::{Forwarding, PK_REWRITE_SLICE, RewriteRows};
use prepare::{FetchedRows, Pending, PreparedRequests, prepares_inline};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

/// How often a writer ticks: the memtable age trigger, the write-stall timeout, retries after a
/// failed job, the compaction policy over deletion counts, and primary-key rewrite slices.
pub(crate) const TICK_INTERVAL: Duration = Duration::from_millis(100);

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

pub use logpose_types::schema::SchemaChange;

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

/// A write request with the engine-clock time it was submitted, for the write-stall timeout.
pub(crate) struct Queued {
    pub(crate) request: WriteRequest,
    pub(crate) enqueued_at: Duration,
}

/// A maintenance job that publishes a manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobKind {
    /// Write the oldest frozen memtable as a segment, with the grown deletion vectors.
    Flush,
    /// Merge segments into one, dropping their deleted rows.
    Compact,
    /// Build a segment's vector graphs into its index sidecar.
    Index,
}

impl JobKind {
    /// The label maintenance status reports.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Flush => "flush",
            Self::Compact => "compact",
            Self::Index => "index",
        }
    }
}

/// One maintenance job of a collection; never reused within the writer's life.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct JobId(pub(crate) u64);

/// What a maintenance job starts from.
pub(crate) struct JobStart {
    pub(crate) job: JobId,
    /// The published (hence durable) state the job works from.
    pub(crate) version: Arc<Version>,
    /// The unit id the job's output must use. Allocated for this job alone and never issued
    /// again, whether or not the job commits.
    pub(crate) unit: UnitId,
    pub(crate) work: JobWork,
}

/// What a job is to build.
pub(crate) enum JobWork {
    /// Nothing: no operation since the checkpoint (flush), fewer than two segments
    /// (compaction), or no segment that needs its graphs (index build).
    Nothing,
    Flush(FlushStart),
    Compact(CompactStart),
    Index(IndexStart),
}

/// The input of an index build, captured at its begin: the segment whose graphs it builds.
pub(crate) struct IndexStart {
    pub(crate) segment: Arc<SegmentHandle>,
    /// Set by the writer to stop the build early: a compaction took the segment, or the
    /// collection is being dropped, poisoned, or shut down.
    pub(crate) cancel: Arc<AtomicBool>,
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

/// The inputs of a compaction, captured at its begin: the planned segments, ascending, each with
/// `D0`, its deletion vector at the begin.
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
    /// An index build of `segment` into the sidecar `index`.
    Index {
        segment: UnitId,
        index: IndexedSegment,
    },
}

/// An index build's output: the open sidecar and what the manifest records of it.
pub(crate) struct IndexedSegment {
    pub(crate) file: Arc<OpenFile>,
    pub(crate) reference: IndexRef,
    /// The vector fields that got a graph.
    pub(crate) graphs: Vec<FieldId>,
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

/// Where the outcome of an explicit flush or compaction goes.
pub(crate) type SnapshotReply = oneshot::Sender<Result<Snapshot>>;

/// Messages polled before client requests. Every one but `Shutdown` drains the pipeline before
/// it is handled, so the writer's private state equals the published (durable) state.
pub(crate) enum ControlMsg {
    /// The scheduler granted the permit job `job` asked for. The writer begins the job: it
    /// captures the job's inputs from the drained state and starts its build on a job thread.
    PermitGranted { job: JobId, permit: Permit },
    /// A job's build finished (the design's `FlushDone` and `CompactionDone`). The writer
    /// commits it: publishes its manifest, then the `Version` over it. `wrote_files` says
    /// whether it may have created files, which are removed if it does not commit. `reply`
    /// receives the outcome of a job a test stepped by hand.
    JobDone {
        job: JobId,
        result: Result<JobCommit>,
        wrote_files: bool,
        reply: Option<SnapshotReply>,
    },
    /// Flush every operation visible now: freeze the active memtable and flush the frozen
    /// memtables until the checkpoint reaches it. `reply`, if any, gets the version after the
    /// last flush (the engine's memtable-budget trigger sends none).
    Flush { reply: Option<SnapshotReply> },
    /// Compact the collection's segments until they settle: merge them, smallest first, as far
    /// as the maintenance-memory pool allows, then build every missing graph; answer once done.
    /// Background compactions finish first.
    Compact { reply: SnapshotReply },
    /// Begin a job without a permit, for a test to build and commit by hand. A flush waits for
    /// a running flush; a compaction takes what one explicit compaction job would; an index
    /// build takes the largest segment that has SQ8 codes and no index sidecar.
    #[cfg_attr(not(test), allow(dead_code))]
    BeginJob {
        kind: JobKind,
        reply: oneshot::Sender<Result<JobStart>>,
    },
    /// A job ended without committing (its ticket was dropped).
    EndJob { job: JobId, wrote_files: bool },
    /// Run the tick now, after every control message sent before this one, and reply once it
    /// ran; for tests that place the tick's work (a job's retry after its backoff) themselves.
    /// A std channel, so the caller can bound its wait without a runtime.
    Tick {
        reply: std::sync::mpsc::SyncSender<()>,
    },
    /// Reply once the pipeline is drained and no job runs (a drop waits for this after it
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
    pub(crate) requests: mpsc::Sender<Queued>,
    pub(crate) control: mpsc::UnboundedSender<ControlMsg>,
}

/// The receiving halves, owned by the writer task.
pub(crate) struct WriterInbox {
    requests: mpsc::Receiver<Queued>,
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
    let policy = Policy {
        graph_min_rows: core.index.graph_min_rows,
        ..Policy::new(
            core.compaction,
            handle.descriptor().compaction_threshold_segments,
            core.scheduler.pool_bytes(),
            crate::compaction::RowShape::of(&seed.state.schema),
        )
    };
    let last_write_at = core.tokens.clock.now();
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
        jobs: BTreeMap::new(),
        next_job: 0,
        reserved: BTreeSet::new(),
        flush_waiters: Vec::new(),
        explicit: None,
        manual_flushes: Vec::new(),
        quiesce_waiters: Vec::new(),
        freeze_pending: false,
        held: None,
        flush_retry: jobs::Backoff::flush(),
        compaction_retry: jobs::Backoff::compaction(),
        index_retry: jobs::Backoff::compaction(),
        last_write_at,
        policy,
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
    requests: mpsc::Receiver<Queued>,
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
    /// Maintenance jobs waiting for a permit or running. At most one flush; at most
    /// `max_jobs_per_collection` compactions, with disjoint inputs.
    jobs: BTreeMap<JobId, Job>,
    next_job: u64,
    /// Segments some compaction job planned or running holds.
    reserved: BTreeSet<UnitId>,
    /// Explicit flushes: the sequence number each waits for the checkpoint to reach.
    flush_waiters: Vec<(SeqNo, Option<SnapshotReply>)>,
    /// The explicit compaction in progress, if any.
    explicit: Option<jobs::ExplicitCompaction>,
    /// Hand-stepped flushes waiting for the running flush to end.
    manual_flushes: Vec<oneshot::Sender<Result<JobStart>>>,
    quiesce_waiters: Vec<std::sync::mpsc::SyncSender<()>>,
    /// A flush trigger fired: freeze the active memtable once the pipeline is drained.
    freeze_pending: bool,
    /// The request the channel yields next: the oldest request taken off the channel while
    /// writes stall, so its age can be checked, or a filter request met while collecting a
    /// group, which starts a group of its own.
    held: Option<Queued>,
    /// When background freezes and flushes may run again after failed ones.
    flush_retry: jobs::Backoff,
    /// When background compactions may be planned again after failed ones; independent of
    /// `flush_retry`, so a failing compaction never holds back a flush.
    compaction_retry: jobs::Backoff,
    /// When background index builds may be planned again after failed ones.
    index_retry: jobs::Backoff,
    /// Engine-clock time of the last write request; the collection is quiet once
    /// `CompactionConfig::quiet_after` passed since.
    last_write_at: Duration,
    policy: Policy,
}

/// A maintenance job of the collection.
struct Job {
    kind: JobKind,
    /// The segments a compaction merges, reserved from planning until the job ends.
    inputs: Vec<UnitId>,
    /// The segment an index build indexes. Not reserved: a compaction that takes it cancels
    /// the build instead.
    target: Option<UnitId>,
    /// Whether the job is a step of the explicit compaction, whose waiters its failure fails.
    explicit: bool,
    phase: Phase,
}

enum Phase {
    /// Waiting for the scheduler's permit.
    Waiting(RequestId),
    /// Begun: building, or committing.
    Running(Running),
}

/// A job that began.
struct Running {
    unit: UnitId,
    /// The file the job's output goes to: its unit's segment, or an index build's sidecar.
    output: PathBuf,
    /// DV files the job may write, besides its output.
    dv_files: Vec<PathBuf>,
    /// Whether the job's files are still the job's to clean up. Cleared once a commit made them
    /// live, or made their durability unknown.
    owns_files: bool,
    /// The scheduler permit; `None` for a job a test steps by hand. Released when the job ends.
    permit: Option<Permit>,
    /// An index build's cancellation flag.
    cancel: Option<Arc<AtomicBool>>,
}

impl Running {
    /// Every file an attempt of this job may have created.
    fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![self.output.clone()];
        files.extend(self.dv_files.iter().cloned());
        files
    }

    /// Whether the writer cancelled the job (an index build).
    fn cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(|cancel| cancel.load(std::sync::atomic::Ordering::Relaxed))
    }
}

impl Writer {
    async fn run(mut self) {
        let mut inflight: Option<InFlight> = None;
        let mut ticker = tokio::time::interval(TICK_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if self.freeze_pending {
                // Freeze only with nothing prepared but unpublished, so the frozen memtable
                // holds exactly the operations in WAL files that end before the rotation.
                if let Some(io) = inflight.take() {
                    self.finish(io).await;
                }
                self.freeze_pending = false;
                self.freeze_if_due().await;
            }
            let stalled = self.stalled();
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
                _ = ticker.tick() => {
                    self.tick();
                    self.rewrite_slice();
                }
                request = next_request(&mut self.held, &mut self.requests), if !stalled => {
                    let Some(request) = request else { break };
                    self.handle.arm_maintenance();
                    self.last_write_at = self.clock_now();
                    self.handle_request(request.request, &mut inflight).await;
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
                Ok(queued) if queued.request.is_filter() => {
                    self.held = Some(queued);
                    return group;
                }
                Ok(Queued { request, .. }) => {
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
                    Ok(Some(queued)) if queued.request.is_filter() => {
                        self.held = Some(queued);
                        return group;
                    }
                    Ok(Some(Queued { request, .. })) => {
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
                self.after_publish();
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
        self.fail_waiters();
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
    Index {
        segment: UnitId,
        file: Arc<OpenFile>,
        entry: ManifestSegment,
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

    /// Install a durable index build: `segment` gains its sidecar `file`, as a new handle over
    /// the same segment file with the manifest `entry` that names the sidecar. Published
    /// versions keep the old handle.
    fn install_index(&mut self, segment: UnitId, file: Arc<OpenFile>, entry: ManifestSegment) {
        let segments = self
            .segments
            .iter()
            .map(|handle| {
                if handle.unit == segment {
                    Arc::new(handle.with_index(Arc::clone(&file), entry.clone()))
                } else {
                    Arc::clone(handle)
                }
            })
            .collect::<Vec<_>>();
        self.segments = Arc::from(segments);
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

/// Fail unless `segment` is the unit job `kind` was allocated.
fn check_job_unit(kind: JobKind, unit: UnitId, segment: &ManifestSegment) -> Result<()> {
    if segment.unit != unit {
        return Err(LogPoseError::internal(format!(
            "the {kind:?} job was allocated unit {unit} but committed unit {}",
            segment.unit
        )));
    }
    Ok(())
}

/// The request a stalled writer held back, or the next one from the channel.
async fn next_request(
    held: &mut Option<Queued>,
    requests: &mut mpsc::Receiver<Queued>,
) -> Option<Queued> {
    match held.take() {
        Some(request) => Some(request),
        None => requests.recv().await,
    }
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
