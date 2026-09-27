//! `CollectionHandle`: one resident collection, its published [`Version`], and the channels to
//! its writer task.

use crate::{
    maintenance::MaintenanceState,
    version::Version,
    writer::{ControlMsg, JobCommit, JobKind, SchemaChange, WriteRequest, WriterChannels},
};
use arc_swap::ArcSwap;
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    CollectionAssignment, CollectionId, CollectionRef, CommitAck, LogPoseError, Result, SeqNo,
    Snapshot, record::ClientOp,
};
use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, Ordering},
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
    /// Set when recovery found persisted pending maintenance that has not been resumed yet.
    resume_maintenance: AtomicBool,
    writer: WriterChannels,
    /// Background maintenance queue and its status.
    pub(crate) jobs: Mutex<MaintenanceState>,
    /// Serializes writes of `maintenance.json` and holds the status version last written, so
    /// the jobs lock is never held across the file's fsync.
    pub(crate) status_file: Mutex<u64>,
}

impl CollectionHandle {
    pub(crate) fn new(version: Version, writer: WriterChannels, jobs: MaintenanceState) -> Self {
        let meta = Arc::clone(&version.meta);
        let (visible, _) = watch::channel(version.visible_seq_no);
        Self {
            meta,
            current: ArcSwap::from_pointee(version),
            visible,
            state: AtomicU8::new(STATE_OPEN),
            poison: OnceLock::new(),
            resume_maintenance: AtomicBool::new(false),
            writer,
            jobs: Mutex::new(jobs),
            status_file: Mutex::new(0),
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
        Err(LogPoseError::Message(format!(
            "read barrier seq {min_seq_no} is not yet visible; collection '{}' is at seq {}",
            self.meta.descriptor.lookup_name(),
            version.visible_seq_no
        )))
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

    async fn submit(
        &self,
        request: impl FnOnce(oneshot::Sender<Result<CommitAck>>) -> WriteRequest,
    ) -> Result<CommitAck> {
        self.ensure_writable()?;
        let (ack, acked) = oneshot::channel();
        self.writer
            .requests
            .send(request(ack))
            .await
            .map_err(|_| self.unavailable())?;
        acked.await.map_err(|_| self.writer_stopped())?
    }

    fn submit_blocking(
        &self,
        request: impl FnOnce(oneshot::Sender<Result<CommitAck>>) -> WriteRequest,
    ) -> Result<CommitAck> {
        self.ensure_writable()?;
        let (ack, acked) = oneshot::channel();
        self.writer
            .requests
            .blocking_send(request(ack))
            .map_err(|_| self.unavailable())?;
        acked.blocking_recv().map_err(|_| self.writer_stopped())?
    }

    /// The error for a request the writer dropped without answering: it stopped part-way.
    fn writer_stopped(&self) -> LogPoseError {
        if self.is_poisoned() || self.is_dropped() {
            return self.unavailable();
        }
        LogPoseError::Message(format!(
            "the writer of collection '{}' stopped before answering; the write's outcome is \
             unknown",
            self.meta.descriptor.lookup_name()
        ))
    }

    /// Start a maintenance job, waiting for any other job of this collection to end first.
    /// Returns the published state the job works from and a ticket that ends the job when it
    /// is committed or dropped. Blocking; for job threads only.
    pub(crate) fn begin_job(&self, kind: JobKind) -> Result<(JobTicket<'_>, Arc<Version>)> {
        self.ensure_writable()?;
        let (reply, replied) = oneshot::channel();
        self.writer
            .control
            .send(ControlMsg::BeginJob { kind, reply })
            .map_err(|_| self.unavailable())?;
        let version = replied.blocking_recv().map_err(|_| self.unavailable())??;
        Ok((
            JobTicket {
                handle: self,
                open: true,
            },
            version,
        ))
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
        self.current.store(Arc::clone(&version));
        self.visible.send_replace(version.visible_seq_no);
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
            return LogPoseError::Message(format!("collection '{name}' does not exist"));
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
            None => LogPoseError::Message(format!("collection '{name}' is unavailable")),
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

    /// Resume the persisted maintenance on the next data-plane access.
    pub(crate) fn arm_maintenance_resume(&self) {
        self.resume_maintenance.store(true, Ordering::Release);
    }

    /// Whether persisted maintenance is waiting to resume; clears the flag.
    pub(crate) fn take_maintenance_resume(&self) -> bool {
        self.resume_maintenance.load(Ordering::Acquire)
            && self.resume_maintenance.swap(false, Ordering::AcqRel)
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

/// An active maintenance job. Committing it publishes its manifest; dropping it ends the job
/// without a change, so the next job can start.
pub(crate) struct JobTicket<'a> {
    handle: &'a CollectionHandle,
    open: bool,
}

impl JobTicket<'_> {
    /// Publish what the job built. Blocking.
    pub(crate) fn commit(mut self, commit: JobCommit) -> Result<Snapshot> {
        self.open = false;
        let (reply, replied) = oneshot::channel();
        self.handle
            .writer
            .control
            .send(ControlMsg::CommitJob {
                commit: Box::new(commit),
                reply,
            })
            .map_err(|_| self.handle.unavailable())?;
        replied
            .blocking_recv()
            .map_err(|_| self.handle.unavailable())?
    }
}

impl Drop for JobTicket<'_> {
    fn drop(&mut self) {
        if self.open {
            let _ = self.handle.writer.control.send(ControlMsg::EndJob);
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
