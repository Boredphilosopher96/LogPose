//! `CollectionHandle`: one resident collection, its published [`Version`], and the channels to
//! its writer task.

use crate::{
    clock::Clock,
    engine::EngineCore,
    tokens::{SnapshotToken, TokenConfig, TokenRegistry},
    version::Version,
    writer::{
        ControlMsg, JobCommit, JobId, JobKind, Queued, SchemaChange, WriteRequest, WriterChannels,
    },
};
use arc_swap::ArcSwap;
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    CollectionAssignment, CollectionId, CollectionRef, CommitAck, LogPoseError, MaintenanceStatus,
    ResourceKind, Result, SeqNo, Snapshot,
    filter::FilterExpr,
    record::{ClientOp, PartialUpdate},
};
use std::{
    collections::VecDeque,
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{oneshot, watch};

/// Immutable identity and configuration of a collection.
#[derive(Clone, Debug)]
pub struct CollectionMeta {
    /// Stable collection id.
    pub id: CollectionId,
    /// `(database, collection)` name the collection is registered under.
    pub reference: CollectionRef,
    /// The collection's directory.
    pub dir: PathBuf,
    /// The persisted descriptor.
    pub descriptor: CollectionDescriptor,
    /// The persisted placement assignment, if the collection has one.
    pub assignment: Option<CollectionAssignment>,
}

impl CollectionMeta {
    pub(crate) fn new(
        descriptor: CollectionDescriptor,
        assignment: Option<CollectionAssignment>,
    ) -> Self {
        Self {
            id: descriptor.collection_id.clone(),
            reference: descriptor.collection_ref(),
            dir: descriptor.root_path.clone(),
            descriptor,
            assignment,
        }
    }
}

/// Versions of the current manifest generation kept for exact legacy snapshots.
const RECENT_VERSIONS: usize = 8;

const STATE_OPEN: u8 = 0;
const STATE_DROPPED: u8 = 1;

/// How a poisoned collection failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PoisonKind {
    /// Read-only until reopened; reopening in process is safe (the failed state was rolled
    /// back durably, or never reached the log).
    ReadOnly,
    /// Failed for this process: the page cache may hold WAL frames that are not on disk.
    Failed {
        /// Whether the failed group's rollback failed.
        rollback_failed: bool,
    },
}

#[derive(Debug)]
struct Poison {
    kind: PoisonKind,
    reason: String,
}

/// Engine-wide snapshot-token state every collection handle shares.
pub(crate) struct TokenContext {
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) config: TokenConfig,
    /// The engine, to total the retired memory every collection's pins hold.
    core: Weak<EngineCore>,
}

impl TokenContext {
    pub(crate) fn new(clock: Arc<dyn Clock>, config: TokenConfig, core: Weak<EngineCore>) -> Self {
        Self {
            clock,
            config,
            core,
        }
    }

    /// The engine-wide pinned-memory limit.
    pub(crate) fn memory_limit(&self) -> u64 {
        self.config.memory_limit.unwrap_or(u64::MAX)
    }

    /// Whether pinned snapshots, engine-wide, hold more retired memory than the limit.
    fn memory_exceeded(&self) -> bool {
        self.core
            .upgrade()
            .is_some_and(|core| core.pinned_retired_bytes() > self.memory_limit())
    }
}

impl fmt::Debug for TokenContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenContext")
            .field("clock", &self.clock)
            .field("config", &self.config)
            .finish()
    }
}

/// One resident collection, shared by readers, writers, and maintenance jobs.
///
/// Readers call [`CollectionHandle::current`] and hold the returned `Arc<Version>` for a whole
/// request; they never block and never take a lock. Writes and schema changes go to the
/// collection's single writer task, which is the only code that publishes a `Version`.
pub struct CollectionHandle {
    meta: Arc<CollectionMeta>,
    /// The published state. Only the writer task stores into it.
    current: ArcSwap<Version>,
    /// Latest published `visible_seq_no`, for read-barrier waits.
    visible: watch::Sender<SeqNo>,
    /// Open or dropped.
    state: AtomicU8,
    /// First fatal error; once set, the collection refuses writes and maintenance and serves
    /// reads of its last published version.
    poison: OnceLock<Poison>,
    /// Whether background maintenance may run. A recovered collection runs none until its
    /// first data-plane access, so a node that only reports status for a collection never
    /// runs its jobs.
    armed: AtomicBool,
    writer: WriterChannels,
    /// Maintenance status, runtime state the writer keeps current.
    status: Mutex<MaintenanceStatus>,
    /// Rows and bytes flushes and compactions wrote, for write amplification.
    written: [AtomicU64; 4],
    /// Pinned snapshots.
    tokens: TokenRegistry,
    token_context: Arc<TokenContext>,
    /// The writer's primary-key index size, for the cache budget.
    pk_index_bytes: AtomicU64,
    /// The latest versions of the current manifest generation, newest last, so an exact legacy
    /// snapshot taken moments ago still resolves while writes continue. Cleared whenever the
    /// manifest generation changes, so it never holds a retired memtable or segment.
    recent: Mutex<VecDeque<Arc<Version>>>,
}

impl CollectionHandle {
    pub(crate) fn new(
        version: Version,
        writer: WriterChannels,
        armed: bool,
        token_context: Arc<TokenContext>,
    ) -> Self {
        let meta = Arc::clone(&version.meta);
        let (visible, _) = watch::channel(version.visible_seq_no);
        let tokens = TokenRegistry::new(meta.id.clone(), meta.descriptor.lookup_name());
        Self {
            tokens,
            token_context,
            meta,
            current: ArcSwap::from_pointee(version),
            visible,
            state: AtomicU8::new(STATE_OPEN),
            poison: OnceLock::new(),
            armed: AtomicBool::new(armed),
            writer,
            status: Mutex::new(MaintenanceStatus::default()),
            written: Default::default(),
            pk_index_bytes: AtomicU64::new(0),
            recent: Mutex::new(VecDeque::new()),
        }
    }

    /// Identity and configuration.
    #[must_use]
    pub fn meta(&self) -> &Arc<CollectionMeta> {
        &self.meta
    }

    /// The persisted descriptor.
    #[must_use]
    pub fn descriptor(&self) -> &CollectionDescriptor {
        &self.meta.descriptor
    }

    /// Pin the current `Version`. Never blocks.
    #[must_use]
    pub fn current(&self) -> Arc<Version> {
        self.current.load_full()
    }

    /// The latest published `visible_seq_no`, without pinning a version.
    #[must_use]
    pub fn visible_seq_no(&self) -> SeqNo {
        self.current.load().visible_seq_no
    }

    /// Wait until a version with `visible_seq_no >= min_seq_no` is published, and return it.
    ///
    /// Fails immediately when `timeout` is zero and the barrier is not yet satisfied.
    pub async fn wait_visible(&self, min_seq_no: SeqNo, timeout: Duration) -> Result<Arc<Version>> {
        if !timeout.is_zero() {
            let mut updates = self.visible.subscribe();
            // A timeout or a closed channel both fall through to the check below.
            let _ =
                tokio::time::timeout(timeout, updates.wait_for(|visible| *visible >= min_seq_no))
                    .await;
        }
        let version = self.current();
        if version.visible_seq_no >= min_seq_no {
            return Ok(version);
        }
        Err(LogPoseError::ReadBarrierNotSatisfied {
            collection: self.meta.descriptor.lookup_name(),
            required_manifest_generation: 0,
            required_seq_no: min_seq_no,
            visible_manifest_generation: version.manifest_generation,
            visible_seq_no: version.visible_seq_no,
        })
    }

    /// Pin the current `Version` and return a token for it, so later reads can see exactly
    /// this state (I12). The pin lives until it is released or `ttl` passes without a use.
    ///
    /// Fails with [`LogPoseError::TooManySnapshots`] when the collection already holds the most
    /// pins it allows, or when pinned snapshots hold more retired memory than the engine allows.
    pub fn pin_snapshot(&self) -> Result<SnapshotToken> {
        self.ensure_open()?;
        self.pin_version(self.current())
    }

    /// Pin `version`, a version of this collection a request already holds.
    pub fn pin_version(&self, version: Arc<Version>) -> Result<SnapshotToken> {
        self.ensure_open()?;
        if version.meta.id != self.meta.id {
            return Err(LogPoseError::internal(
                "cannot pin a version of another collection",
            ));
        }
        let context = &self.token_context;
        self.tokens.pin(
            version,
            context.clock.now(),
            &context.config,
            context.memory_exceeded(),
        )
    }

    /// The `Version` `token` pins, extending the token's expiry. Fails with
    /// [`LogPoseError::SnapshotExpired`] when the token expired, was released, or was never
    /// issued for this collection. Hold the returned `Arc` for the whole request.
    pub fn snapshot_version(&self, token: &SnapshotToken) -> Result<Arc<Version>> {
        self.ensure_open()?;
        let context = &self.token_context;
        self.tokens
            .resolve(token, context.clock.now(), context.config.ttl)
    }

    /// Unpin `token`. Returns whether it was pinned. A released token fails like an expired one.
    pub fn release_snapshot(&self, token: &SnapshotToken) -> bool {
        self.tokens.release(token)
    }

    /// Snapshots pinned now, including expired ones the reaper has not dropped yet.
    #[must_use]
    pub fn pinned_snapshots(&self) -> usize {
        self.tokens.len()
    }

    /// Bytes of retired memtables that only this collection's pinned snapshots hold.
    #[must_use]
    pub fn pinned_retired_bytes(&self) -> u64 {
        self.tokens.retired_bytes(&self.current())
    }

    /// The writer's last report of its primary-key index's size.
    pub(crate) fn pk_index_bytes(&self) -> u64 {
        self.pk_index_bytes.load(Ordering::Relaxed)
    }

    /// Report the primary-key index's size; only the writer calls this.
    pub(crate) fn set_pk_index_bytes(&self, bytes: u64) {
        self.pk_index_bytes.store(bytes, Ordering::Relaxed);
    }

    /// The version an exact legacy `Snapshot` names, if one is still retained: one of the
    /// latest versions of the current manifest generation, or a token-pinned one.
    pub(crate) fn version_for(&self, snapshot: &Snapshot) -> Option<Arc<Version>> {
        let recent = self
            .recent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .rev()
            .find(|version| {
                version.manifest_generation == snapshot.manifest_generation
                    && version.visible_seq_no == snapshot.visible_seq_no
            })
            .cloned();
        recent.or_else(|| {
            let context = &self.token_context;
            self.tokens
                .find(snapshot, context.clock.now(), context.config.ttl)
        })
    }

    pub(crate) fn tokens(&self) -> &TokenRegistry {
        &self.tokens
    }

    /// Durably commit `ops` as one atomic batch. Returns once the batch's WAL group is synced
    /// and the `Version` that includes it is published, so every read that starts afterwards
    /// sees it (I1). A write that fails validation consumes no sequence number.
    pub async fn write(&self, ops: Vec<ClientOp>) -> Result<CommitAck> {
        self.submit(|ack| WriteRequest::Batch { ops, ack }).await
    }

    /// [`CollectionHandle::write`] for threads outside any async runtime.
    pub fn write_blocking(&self, ops: Vec<ClientOp>) -> Result<CommitAck> {
        self.submit_blocking(|ack| WriteRequest::Batch { ops, ack })
    }

    /// Change the schema, ordered with the writes around it. The change consumes one sequence
    /// number and is durable, and visible, once this returns.
    pub async fn alter_schema(&self, change: SchemaChange) -> Result<CommitAck> {
        self.submit(|ack| WriteRequest::AlterSchema { change, ack })
            .await
    }

    /// [`CollectionHandle::alter_schema`] for threads outside any async runtime.
    pub fn alter_schema_blocking(&self, change: SchemaChange) -> Result<CommitAck> {
        self.submit_blocking(|ack| WriteRequest::AlterSchema { change, ack })
    }

    /// Delete every live row matching `filter`, resolved once against the writer's latest
    /// state (every earlier write included) to a fixed key set that commits as one atomic
    /// batch. The ack's `applied_ops` is the number of rows deleted; none matching uses no
    /// sequence number.
    ///
    /// # Errors
    ///
    /// As [`CollectionHandle::write`], plus `FailedPrecondition` when the engine has no
    /// [`RowSetResolver`](crate::RowSetResolver) and the resolver's errors.
    pub async fn delete_by_filter(&self, filter: FilterExpr) -> Result<CommitAck> {
        self.submit(|ack| WriteRequest::DeleteByFilter { filter, ack })
            .await
    }

    /// Apply `patch` (its key is ignored) to every live row matching `filter`, resolved like
    /// [`CollectionHandle::delete_by_filter`]. The ack's `applied_ops` is the number of rows
    /// updated.
    ///
    /// # Errors
    ///
    /// As [`CollectionHandle::delete_by_filter`], plus the validation errors of the updates.
    pub async fn update_by_filter(
        &self,
        filter: FilterExpr,
        patch: PartialUpdate,
    ) -> Result<CommitAck> {
        self.submit(|ack| WriteRequest::UpdateByFilter { filter, patch, ack })
            .await
    }

    /// A request stamped with the engine-clock time it is submitted at: a request that waits
    /// through a write stall for longer than `write_stall_timeout` fails with `WriteStalled`.
    fn queued(
        &self,
        request: impl FnOnce(oneshot::Sender<Result<CommitAck>>) -> WriteRequest,
    ) -> (Queued, oneshot::Receiver<Result<CommitAck>>) {
        let (ack, acked) = oneshot::channel();
        let queued = Queued {
            request: request(ack),
            enqueued_at: self.token_context.clock.now(),
        };
        (queued, acked)
    }

    async fn submit(
        &self,
        request: impl FnOnce(oneshot::Sender<Result<CommitAck>>) -> WriteRequest,
    ) -> Result<CommitAck> {
        self.ensure_writable()?;
        let (queued, acked) = self.queued(request);
        self.writer
            .requests
            .send(queued)
            .await
            .map_err(|_| self.unavailable())?;
        acked.await.map_err(|_| self.writer_stopped())?
    }

    fn submit_blocking(
        &self,
        request: impl FnOnce(oneshot::Sender<Result<CommitAck>>) -> WriteRequest,
    ) -> Result<CommitAck> {
        self.ensure_writable()?;
        let (queued, acked) = self.queued(request);
        self.writer
            .requests
            .blocking_send(queued)
            .map_err(|_| self.unavailable())?;
        acked.blocking_recv().map_err(|_| self.writer_stopped())?
    }

    /// Flush every operation visible now: freeze the active memtable and flush the frozen
    /// memtables until the checkpoint covers it. Returns the snapshot after the last flush.
    pub async fn flush(&self) -> Result<Snapshot> {
        let replied = self.control_request(|reply| ControlMsg::Flush { reply: Some(reply) })?;
        replied.await.map_err(|_| self.writer_stopped())?
    }

    /// [`CollectionHandle::flush`] for threads outside any async runtime.
    pub fn flush_blocking(&self) -> Result<Snapshot> {
        let replied = self.control_request(|reply| ControlMsg::Flush { reply: Some(reply) })?;
        replied.blocking_recv().map_err(|_| self.writer_stopped())?
    }

    /// Compact the collection's segments into one, as far as one job's maintenance-memory
    /// reservation allows, once no background compaction runs. Fails with
    /// [`LogPoseError::TooLarge`] when even two segments need more than the whole pool.
    pub async fn compact(&self) -> Result<Snapshot> {
        let replied = self.control_request(|reply| ControlMsg::Compact { reply })?;
        replied.await.map_err(|_| self.writer_stopped())?
    }

    /// [`CollectionHandle::compact`] for threads outside any async runtime.
    pub fn compact_blocking(&self) -> Result<Snapshot> {
        let replied = self.control_request(|reply| ControlMsg::Compact { reply })?;
        replied.blocking_recv().map_err(|_| self.writer_stopped())?
    }

    /// Ask the writer to flush what is visible now, without waiting (the engine's memtable
    /// budget trigger).
    pub(crate) fn request_flush(&self) {
        let _ = self.writer.control.send(ControlMsg::Flush { reply: None });
    }

    fn control_request(
        &self,
        message: impl FnOnce(oneshot::Sender<Result<Snapshot>>) -> ControlMsg,
    ) -> Result<oneshot::Receiver<Result<Snapshot>>> {
        self.ensure_writable()?;
        let (reply, replied) = oneshot::channel();
        self.writer
            .control
            .send(message(reply))
            .map_err(|_| self.unavailable())?;
        Ok(replied)
    }

    /// The collection's maintenance status: jobs waiting for a permit, the job running, the
    /// last failure, and the jobs completed since the engine opened. Runtime state only.
    #[must_use]
    pub fn maintenance_status(&self) -> MaintenanceStatus {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Change the maintenance status; only the writer calls this.
    pub(crate) fn update_maintenance_status(&self, change: impl FnOnce(&mut MaintenanceStatus)) {
        change(&mut self.status.lock().unwrap_or_else(PoisonError::into_inner));
    }

    /// Rows and bytes that flushes and compactions wrote since the engine opened.
    #[must_use]
    pub fn maintenance_written(&self) -> MaintenanceWritten {
        let load = |index: usize| self.written[index].load(Ordering::Relaxed);
        MaintenanceWritten {
            flush_rows: load(0),
            flush_bytes: load(1),
            compaction_rows: load(2),
            compaction_bytes: load(3),
        }
    }

    /// Count what a committed job wrote; only the writer calls this.
    pub(crate) fn record_written(&self, kind: JobKind, rows: u64, bytes: u64) {
        let base = match kind {
            JobKind::Flush => 0,
            JobKind::Compact => 2,
        };
        self.written[base].fetch_add(rows, Ordering::Relaxed);
        self.written[base + 1].fetch_add(bytes, Ordering::Relaxed);
    }

    /// Let background maintenance run: the collection had a data-plane access.
    pub(crate) fn arm_maintenance(&self) {
        if !self.armed.load(Ordering::Relaxed) {
            self.armed.store(true, Ordering::Relaxed);
        }
    }

    /// Whether background maintenance may run.
    pub(crate) fn maintenance_armed(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
    }

    /// The error for a request the writer dropped without answering: it stopped part-way.
    fn writer_stopped(&self) -> LogPoseError {
        if self.is_poisoned() || self.is_dropped() {
            return self.unavailable();
        }
        LogPoseError::internal(format!(
            "the writer of collection '{}' stopped before answering; the write's outcome is \
             unknown",
            self.meta.descriptor.lookup_name()
        ))
    }

    /// Begin a maintenance job without a scheduler permit, for a test to build and commit by
    /// hand (a flush waits for a running flush; a compaction takes every unreserved segment).
    /// Returns the published state the job works from and a ticket that ends the job when it
    /// is committed or dropped. Blocking.
    #[cfg(test)]
    pub(crate) fn begin_job(
        self: &Arc<Self>,
        kind: JobKind,
    ) -> Result<(JobTicket, crate::writer::JobStart)> {
        self.ensure_writable()?;
        let (reply, replied) = oneshot::channel();
        self.writer
            .control
            .send(ControlMsg::BeginJob { kind, reply })
            .map_err(|_| self.unavailable())?;
        let start = replied.blocking_recv().map_err(|_| self.unavailable())??;
        Ok((JobTicket::new(Arc::clone(self), start.job), start))
    }

    /// Wait until the writer has drained its pipeline and no maintenance job is active. Used by
    /// a drop after it marked the handle dropped. Blocking.
    pub(crate) fn quiesce(&self) {
        let (reply, replied) = std::sync::mpsc::sync_channel(1);
        if self
            .writer
            .control
            .send(ControlMsg::Quiesce { reply })
            .is_ok()
        {
            let _ = replied.recv();
        }
    }

    /// A sender for the writer's control messages, for engine shutdown.
    pub(crate) fn control_sender(&self) -> tokio::sync::mpsc::UnboundedSender<ControlMsg> {
        self.writer.control.clone()
    }

    /// Ask the writer task to stop. It finishes the group in flight and fails queued requests.
    pub(crate) fn stop_writer(&self) {
        let _ = self.writer.control.send(ControlMsg::Shutdown);
    }

    /// Publish `version` as the current state. Only the writer task calls this.
    ///
    /// Store strictly before notifying, so a waiter woken for `visible_seq_no` always loads a
    /// version that includes it; the writer acknowledges only after this returns.
    pub(crate) fn publish(&self, version: Version) -> Arc<Version> {
        let version = Arc::new(version);
        let previous = self.current.swap(Arc::clone(&version));
        self.visible.send_replace(version.visible_seq_no);
        let dropped = {
            let mut recent = self.recent.lock().unwrap_or_else(PoisonError::into_inner);
            let mut dropped = Vec::new();
            if previous.manifest_generation != version.manifest_generation {
                dropped.extend(recent.drain(..));
            } else {
                recent.push_back(previous);
                while recent.len() > RECENT_VERSIONS {
                    dropped.extend(recent.pop_front());
                }
            }
            dropped
        };
        // Dropping a version may drop the last reference to a retired unit: never under the
        // lock.
        drop(dropped);
        version
    }

    /// Fail unless the collection accepts writes and maintenance.
    pub(crate) fn ensure_writable(&self) -> Result<()> {
        if self.is_dropped() || self.is_poisoned() {
            return Err(self.unavailable());
        }
        Ok(())
    }

    /// Fail if the collection was dropped.
    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.is_dropped() {
            return Err(self.unavailable());
        }
        Ok(())
    }

    /// The error every refused call returns.
    pub(crate) fn unavailable(&self) -> LogPoseError {
        let name = self.meta.descriptor.lookup_name();
        if self.is_dropped() {
            return LogPoseError::not_found(ResourceKind::Collection, name);
        }
        match self.poison.get() {
            Some(Poison {
                kind: PoisonKind::ReadOnly,
                reason,
            }) => LogPoseError::CollectionPoisoned {
                collection: name,
                reason: reason.clone(),
            },
            Some(Poison {
                kind: PoisonKind::Failed { rollback_failed },
                reason,
            }) => LogPoseError::CollectionPoisoned {
                collection: name,
                reason: format!(
                    "{reason}; {}, so it must not be reopened in this process: restart the \
                     process after checking the device",
                    if *rollback_failed {
                        "the WAL rollback failed and the page cache may hold writes the device \
                         never got"
                    } else {
                        "the outcome of its last WAL write is unknown"
                    }
                ),
            },
            None => LogPoseError::unavailable(format!("collection '{name}' is unavailable")),
        }
    }

    /// Refuse every later write and maintenance job. The first reason wins; reads keep serving
    /// the last published version.
    pub(crate) fn poison(&self, kind: PoisonKind, reason: String) {
        let _ = self.poison.set(Poison { kind, reason });
    }

    /// The reason the collection was poisoned, if it was.
    pub(crate) fn poison_reason(&self) -> Option<String> {
        self.poison.get().map(|poison| poison.reason.clone())
    }

    /// Whether the collection refuses writes because a WAL write or manifest publish failed.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poison.get().is_some()
    }

    pub(crate) fn mark_dropped(&self) {
        self.state.store(STATE_DROPPED, Ordering::Release);
    }

    /// Undo [`CollectionHandle::mark_dropped`] after a drop that changed nothing on disk.
    pub(crate) fn mark_open(&self) {
        self.state.store(STATE_OPEN, Ordering::Release);
    }

    /// Whether the collection was dropped. A dropped handle refuses every call.
    #[must_use]
    pub fn is_dropped(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_DROPPED
    }
}

/// Rows and bytes flushes and compactions wrote, for write amplification.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaintenanceWritten {
    /// Rows flushes wrote into segments.
    pub flush_rows: u64,
    /// Segment bytes flushes wrote.
    pub flush_bytes: u64,
    /// Rows compactions rewrote.
    pub compaction_rows: u64,
    /// Segment bytes compactions wrote.
    pub compaction_bytes: u64,
}

/// A running maintenance job's build. Handing it the build's result sends it to the writer to
/// commit; dropping it ends the job without a change.
pub(crate) struct JobTicket {
    handle: Arc<CollectionHandle>,
    job: JobId,
    open: bool,
    /// Whether the job may have created files for its unit.
    wrote_files: bool,
}

impl JobTicket {
    pub(crate) fn new(handle: Arc<CollectionHandle>, job: JobId) -> Self {
        Self {
            handle,
            job,
            open: true,
            wrote_files: false,
        }
    }

    /// Record that the job is about to create files for its unit, so that they are removed
    /// if it ends without committing.
    pub(crate) fn writing_files(&mut self) {
        self.wrote_files = true;
    }

    /// Hand the build's result to the writer, which commits it (or ends the job on an error).
    pub(crate) fn done(mut self, result: Result<JobCommit>) {
        self.open = false;
        let _ = self.handle.writer.control.send(ControlMsg::JobDone {
            job: self.job,
            result,
            wrote_files: self.wrote_files,
            reply: None,
        });
    }

    /// Publish what the job built and wait for the outcome. Blocking; for tests that step a
    /// job by hand.
    #[cfg(test)]
    pub(crate) fn commit(mut self, commit: JobCommit) -> Result<Snapshot> {
        self.open = false;
        let (reply, replied) = oneshot::channel();
        self.handle
            .writer
            .control
            .send(ControlMsg::JobDone {
                job: self.job,
                result: Ok(commit),
                wrote_files: self.wrote_files,
                reply: Some(reply),
            })
            .map_err(|_| self.handle.unavailable())?;
        replied
            .blocking_recv()
            .map_err(|_| self.handle.unavailable())?
    }
}

impl Drop for JobTicket {
    fn drop(&mut self) {
        if self.open {
            let _ = self.handle.writer.control.send(ControlMsg::EndJob {
                job: self.job,
                wrote_files: self.wrote_files,
            });
        }
    }
}

impl fmt::Debug for CollectionHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CollectionHandle")
            .field("collection", &self.meta.reference)
            .field("current", &*self.current.load())
            .field("dropped", &self.is_dropped())
            .field("poison", &self.poison.get())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CreateCollectionRequest, Engine, EngineConfig,
        test_support::{put, unique_temp_dir},
    };
    use logpose_types::DistanceMetric;
    use logpose_vfs::std_vfs;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    };

    #[tokio::test]
    async fn wait_visible_returns_once_a_write_reaches_the_barrier() {
        let root = unique_temp_dir("handle-wait-visible");
        let engine =
            Engine::open(std_vfs(), &root, EngineConfig::default()).expect("engine should open");
        let descriptor = engine
            .core()
            .plan_collection_descriptor(&CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .expect("descriptor should plan");
        let handle = engine
            .create_collection(descriptor, None)
            .expect("collection should be created");

        let error = handle
            .wait_visible(1, Duration::ZERO)
            .await
            .expect_err("a zero timeout fails at once when the barrier is not met");
        assert!(error.to_string().contains("read barrier"), "{error}");

        let writer = {
            let engine = engine.clone();
            let handle = Arc::clone(&handle);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                engine
                    .io(move |core| core.write(&handle, vec![put("alpha", vec![1.0, 0.0])]))
                    .await
            })
        };
        let version = handle
            .wait_visible(1, Duration::from_secs(30))
            .await
            .expect("the barrier is met once the write is published");
        assert!(version.visible_seq_no >= 1);
        writer
            .await
            .expect("writer should join")
            .expect("write should succeed");
        handle
            .wait_visible(1, Duration::ZERO)
            .await
            .expect("a met barrier returns at once");
    }

    /// `wait_visible` trusts that a notified `visible_seq_no` is already published: the watch
    /// channel must never run ahead of `current`.
    #[test]
    fn the_visible_watch_never_runs_ahead_of_the_published_version() {
        let root = unique_temp_dir("handle-watch-order");
        let engine =
            Engine::open(std_vfs(), &root, EngineConfig::default()).expect("engine should open");
        let descriptor = engine
            .core()
            .plan_collection_descriptor(&CreateCollectionRequest::new(
                "documents",
                2,
                DistanceMetric::Dot,
            ))
            .expect("descriptor should plan");
        let handle = engine
            .create_collection(descriptor, None)
            .expect("collection should be created");
        let done = Arc::new(AtomicBool::new(false));
        let readers = (0..4)
            .map(|_| {
                let handle = Arc::clone(&handle);
                let done = Arc::clone(&done);
                thread::spawn(move || {
                    let mut checks = 0_u64;
                    while !done.load(Ordering::Acquire) {
                        let notified = *handle.visible.borrow();
                        let published = handle.current().visible_seq_no;
                        assert!(
                            published >= notified,
                            "watch announced seq {notified} before version at {published} was \
                             published"
                        );
                        checks += 1;
                    }
                    checks
                })
            })
            .collect::<Vec<_>>();
        let core = engine.core();
        for index in 0..1000 {
            core.write(&handle, vec![put(&format!("id-{index}"), vec![1.0, 0.0])])
                .expect("write should succeed");
        }
        done.store(true, Ordering::Release);
        for reader in readers {
            assert!(reader.join().expect("reader should join") > 0);
        }
    }
}
