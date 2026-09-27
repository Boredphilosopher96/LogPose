//! `CollectionHandle`: one resident collection, its published [`Version`], and publication.

use crate::{maintenance::MaintenanceState, version::Version};
use arc_swap::ArcSwap;
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    CollectionAssignment, CollectionId, CollectionRef, LogPoseError, ResourceKind, Result, SeqNo,
};
use logpose_wal::WalWriter;
use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;

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

/// One resident collection, shared by readers, writers, and maintenance jobs.
///
/// Readers call [`CollectionHandle::current`] and hold the returned `Arc<Version>` for a whole
/// request; they never block and never take a lock. Only the collection's writer publishes.
pub struct CollectionHandle {
    meta: Arc<CollectionMeta>,
    /// The published state.
    current: ArcSwap<Version>,
    /// Latest published `visible_seq_no`, for read-barrier waits.
    visible: watch::Sender<SeqNo>,
    /// Open or dropped.
    state: AtomicU8,
    /// First fatal error; once set, the collection refuses writes and maintenance.
    poison: OnceLock<String>,
    /// Set when recovery found persisted pending maintenance that has not been resumed yet.
    resume_maintenance: AtomicBool,
    /// Stand-in for the single writer task until group commit lands: serializes WAL appends,
    /// flush, recovery after a failed job, and every `Version` publication.
    pub(crate) writer: Mutex<WriterSlot>,
    /// Serializes flush and compaction, which both publish manifests. Taken before `writer`.
    pub(crate) maintenance: Mutex<()>,
    /// Background maintenance queue and its persisted status.
    pub(crate) jobs: Mutex<MaintenanceState>,
}

/// State owned by whoever holds [`CollectionHandle::writer`].
pub(crate) struct WriterSlot {
    /// The open active WAL, or `None` after a failure until the next writer reopens it.
    pub(crate) wal: Option<WalWriter>,
}

impl CollectionHandle {
    pub(crate) fn new(version: Version, wal: Option<WalWriter>, jobs: MaintenanceState) -> Self {
        let meta = Arc::clone(&version.meta);
        let (visible, _) = watch::channel(version.visible_seq_no);
        Self {
            meta,
            current: ArcSwap::from_pointee(version),
            visible,
            state: AtomicU8::new(STATE_OPEN),
            poison: OnceLock::new(),
            resume_maintenance: AtomicBool::new(false),
            writer: Mutex::new(WriterSlot { wal }),
            maintenance: Mutex::new(()),
            jobs: Mutex::new(jobs),
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
            required_seq_no: min_seq_no,
            visible_seq_no: version.visible_seq_no,
        })
    }

    /// Publish `version` as the current state. The caller holds [`CollectionHandle::writer`].
    ///
    /// Store strictly before notifying, so a waiter woken for `visible_seq_no` always loads a
    /// version that includes it.
    pub(crate) fn publish(&self, _writer: &WriterSlot, version: Version) -> Arc<Version> {
        let version = Arc::new(version);
        self.current.store(Arc::clone(&version));
        self.visible.send_replace(version.visible_seq_no);
        version
    }

    /// Lock the writer slot. A panic while it was held leaves the WAL and the published state
    /// possibly out of step, so it poisons the collection.
    pub(crate) fn lock_writer(&self) -> Result<MutexGuard<'_, WriterSlot>> {
        self.writer.lock().map_err(|_| {
            self.poison("a writer panicked while holding the collection".to_owned());
            self.unavailable()
        })
    }

    /// Lock the maintenance slot; see [`CollectionHandle::lock_writer`].
    pub(crate) fn lock_maintenance(&self) -> Result<MutexGuard<'_, ()>> {
        self.maintenance.lock().map_err(|_| {
            self.poison("a maintenance job panicked while holding the collection".to_owned());
            self.unavailable()
        })
    }

    /// Fail unless the collection accepts writes and maintenance.
    pub(crate) fn ensure_writable(&self) -> Result<()> {
        if self.is_dropped() || self.poison.get().is_some() {
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
            Some(reason) => LogPoseError::CollectionPoisoned {
                collection: name,
                reason: reason.clone(),
            },
            None => LogPoseError::unavailable(format!("collection '{name}' is unavailable")),
        }
    }

    /// Refuse every later write and maintenance job. The first reason wins; reads keep serving
    /// the last published version.
    pub(crate) fn poison(&self, reason: String) {
        let _ = self.poison.set(reason);
    }

    /// Whether the collection refuses writes.
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
