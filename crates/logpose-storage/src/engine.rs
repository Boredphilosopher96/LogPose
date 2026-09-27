//! `Engine`: the storage-root lock, the resident collection map, the thread pools, and
//! collection create and drop.

use crate::{
    BlobStore,
    durable_fs::{create_dir_all_synced, path_exists, sync_dir},
    error::io_message,
    handle::CollectionHandle,
    recovery::RecoveredCollection,
    root_lock::lock_root_exclusively,
    runtime::{IoPool, Runtime, RuntimeConfig, run_cpu},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    CollectionAssignment, CollectionRef, CorruptionKind, LogPoseError, ResourceKind, Result,
};
use logpose_vfs::{Vfs, VfsLock};
use std::{
    collections::BTreeMap,
    fmt,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

/// Suffix of a collection directory whose drop is committed but whose files are not yet removed.
pub(crate) const DROPPED_DIR_SUFFIX: &str = ".dropped";

/// Engine configuration.
#[derive(Clone, Default)]
pub struct EngineConfig {
    /// Thread pool sizes.
    pub runtime: RuntimeConfig,
    /// Remote blob store that flushed segments are marked for upload to, if any.
    pub blob_store: Option<Arc<dyn BlobStore>>,
}

impl fmt::Debug for EngineConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EngineConfig")
            .field("runtime", &self.runtime)
            .field("blob_store", &self.blob_store.is_some())
            .finish()
    }
}

/// The storage engine for one storage root. Cheap to clone; clones share everything.
///
/// Opening an engine locks the root exclusively (see [`LogPoseError::StorageRootLocked`]) and
/// recovers every collection into memory, so lookups are map lookups and reads load the
/// published [`Version`](crate::Version) without touching metadata files. Dropping the last clone
/// stops background maintenance, waits for every engine task still running, and then releases
/// the root, so a new engine can open it as soon as the drop returns.
#[derive(Clone)]
pub struct Engine {
    shared: Arc<EngineShared>,
}

/// Owned by user-facing clones only. Its drop is the engine's shutdown.
struct EngineShared {
    core: Arc<EngineCore>,
}

impl Drop for EngineShared {
    fn drop(&mut self) {
        self.core.shutdown.store(true, Ordering::Release);
        self.core.tasks.wait_idle();
    }
}

/// Engine state shared with the engine's own tasks.
pub(crate) struct EngineCore {
    pub(crate) root: PathBuf,
    pub(crate) vfs: Arc<dyn Vfs>,
    pub(crate) blob_store: Option<Arc<dyn BlobStore>>,
    /// Collections keyed by `(database, name)`. Populated at open; changed only by create and
    /// drop. Critical sections are map operations only.
    collections: RwLock<BTreeMap<CollectionRef, CollectionSlot>>,
    /// Collection directories whose descriptor could not be read at open.
    unreadable: OnceLock<Vec<UnreadableCollection>>,
    runtime: Runtime,
    /// Threads that run legacy flush and compaction jobs, which interleave CPU and blocking
    /// I/O and so can run on neither the I/O pool nor a rayon pool.
    pub(crate) jobs: IoPool,
    tasks: Arc<TaskTracker>,
    shutdown: AtomicBool,
    /// Held for the engine's lifetime; declared last so it is released last.
    _root_lock: Box<dyn VfsLock>,
}

/// One entry of the collection map.
#[derive(Clone)]
enum CollectionSlot {
    /// Name reserved by a create that has not finished.
    Creating,
    /// Name held by a drop that has not committed yet.
    Dropping,
    Open(Arc<CollectionHandle>),
    /// Recovery failed; every call returns the error. Reopen the engine to retry.
    Failed(Arc<FailedCollection>),
}

/// A collection whose recovery failed at open.
pub(crate) struct FailedCollection {
    /// The descriptor, when it was readable and valid.
    pub(crate) descriptor: Option<CollectionDescriptor>,
    pub(crate) error: LogPoseError,
}

/// A collection directory whose descriptor could not be parsed.
struct UnreadableCollection {
    dir: PathBuf,
    error: String,
}

impl Engine {
    /// Lock `root`, start the thread pools, and recover every collection.
    ///
    /// Creates `root` if needed. Fails with [`LogPoseError::StorageRootLocked`] if another engine
    /// holds it. A collection whose recovery fails does not fail the open: it is registered as
    /// failed and every call on it returns its error.
    pub fn open(vfs: Arc<dyn Vfs>, root: impl AsRef<Path>, config: EngineConfig) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        create_dir_all_synced(vfs.as_ref(), &root)?;
        let root_lock = lock_root_exclusively(vfs.as_ref(), &root)?;
        let runtime = Runtime::new(config.runtime)?;
        let jobs = IoPool::new(
            "logpose-job",
            config.runtime.maintenance_threads,
            config.runtime.io_queue_depth,
        )?;
        let core = Arc::new(EngineCore {
            root,
            vfs,
            blob_store: config.blob_store,
            collections: RwLock::new(BTreeMap::new()),
            unreadable: OnceLock::new(),
            runtime,
            jobs,
            tasks: Arc::new(TaskTracker::default()),
            shutdown: AtomicBool::new(false),
            _root_lock: root_lock,
        });
        core.recover_collections()?;
        Ok(Self {
            shared: Arc::new(EngineShared { core }),
        })
    }

    /// The storage root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.shared.core.root
    }

    /// The filesystem every file access goes through.
    #[must_use]
    pub fn vfs(&self) -> &Arc<dyn Vfs> {
        &self.shared.core.vfs
    }

    /// The engine's thread pools.
    #[must_use]
    pub fn runtime(&self) -> &Runtime {
        &self.shared.core.runtime
    }

    /// Look up an open collection. A map lookup; never touches the filesystem.
    pub fn collection(&self, reference: &CollectionRef) -> Result<Arc<CollectionHandle>> {
        self.shared.core.collection(reference)
    }

    /// Every open collection, ordered by `(database, name)`.
    #[must_use]
    pub fn collections(&self) -> Vec<Arc<CollectionHandle>> {
        let mut handles = self
            .shared
            .core
            .read_collections()
            .values()
            .filter_map(|slot| match slot {
                CollectionSlot::Open(handle) => Some(Arc::clone(handle)),
                _ => None,
            })
            .collect::<Vec<_>>();
        handles.sort_by(|left, right| left.meta().reference.cmp(&right.meta().reference));
        handles
    }

    /// Durably create a collection from a validated descriptor and register it.
    ///
    /// Blocking. Fails if a collection with the same `(database, name)` exists or is being
    /// created or dropped concurrently.
    pub fn create_collection(
        &self,
        descriptor: CollectionDescriptor,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<Arc<CollectionHandle>> {
        self.core().create_collection(descriptor, assignment)
    }

    /// Drop a collection: refuse new calls on it, wait for its in-flight write and maintenance
    /// job, durably retire its directory, and remove its files.
    ///
    /// Blocking. Readers that already pinned a `Version` finish normally unless they need a
    /// file that the drop removed.
    pub fn drop_collection(&self, reference: &CollectionRef) -> Result<()> {
        self.core().drop_collection(reference)
    }

    /// A tracked reference to the engine state for an engine task.
    pub(crate) fn core(&self) -> CoreRef {
        CoreRef::new(&self.shared.core)
    }

    /// Run blocking `f` on the I/O pool.
    pub(crate) async fn io<T: Send + 'static>(
        &self,
        f: impl FnOnce(&CoreRef) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let core = self.core();
        self.shared.core.runtime.io.run(move || f(&core)).await?
    }

    /// Run `f` on the legacy maintenance job threads.
    pub(crate) async fn job<T: Send + 'static>(
        &self,
        f: impl FnOnce(&CoreRef) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let core = self.core();
        self.shared.core.jobs.run(move || f(&core)).await?
    }

    /// Run CPU-bound `f` on the query pool.
    pub async fn run_query<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T> {
        run_cpu(&self.shared.core.runtime.query, f).await
    }
}

impl fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("root", &self.shared.core.root)
            .field("collections", &self.shared.core.read_collections().len())
            .field("runtime", &self.shared.core.runtime)
            .finish()
    }
}

impl EngineCore {
    pub(crate) fn collections_root(&self) -> PathBuf {
        self.root.join("collections")
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    fn read_collections(&self) -> RwLockReadGuard<'_, BTreeMap<CollectionRef, CollectionSlot>> {
        self.collections
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write_collections(&self) -> RwLockWriteGuard<'_, BTreeMap<CollectionRef, CollectionSlot>> {
        self.collections
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn collection(&self, reference: &CollectionRef) -> Result<Arc<CollectionHandle>> {
        match self.read_collections().get(reference) {
            Some(CollectionSlot::Open(handle)) => Ok(Arc::clone(handle)),
            Some(CollectionSlot::Failed(failed)) => Err(failed.error.clone()),
            Some(CollectionSlot::Creating | CollectionSlot::Dropping) | None => {
                Err(not_found(reference))
            }
        }
    }

    /// Whether `reference` names a registered collection, open or failed.
    pub(crate) fn contains(&self, reference: &CollectionRef) -> bool {
        matches!(
            self.read_collections().get(reference),
            Some(CollectionSlot::Open(_) | CollectionSlot::Failed(_))
        )
    }

    /// Descriptors of every registered collection, ordered by `(database, name)`.
    ///
    /// Fails if any collection directory has an unreadable or invalid descriptor, because the
    /// listing would silently omit it.
    pub(crate) fn list_descriptors(&self) -> Result<Vec<CollectionDescriptor>> {
        if let Some(unreadable) = self.unreadable.get().and_then(|list| list.first()) {
            return Err(LogPoseError::Corrupt {
                kind: CorruptionKind::Descriptor,
                location: Some(unreadable.dir.display().to_string()),
                message: format!(
                    "collection directory '{}' is unreadable: {}",
                    unreadable.dir.display(),
                    unreadable.error
                ),
            });
        }
        let mut descriptors = Vec::new();
        for slot in self.read_collections().values() {
            match slot {
                CollectionSlot::Open(handle) => descriptors.push(handle.descriptor().clone()),
                CollectionSlot::Failed(failed) => match &failed.descriptor {
                    Some(descriptor) => descriptors.push(descriptor.clone()),
                    None => return Err(failed.error.clone()),
                },
                CollectionSlot::Creating | CollectionSlot::Dropping => {}
            }
        }
        descriptors.sort_by(|left, right| {
            (&left.database_name, &left.name).cmp(&(&right.database_name, &right.name))
        });
        Ok(descriptors)
    }

    /// Reserve `reference` for a create. The reservation is released on drop unless committed.
    pub(crate) fn reserve(&self, reference: &CollectionRef) -> Result<Reservation<'_>> {
        let mut collections = self.write_collections();
        match collections.get(reference) {
            None => {
                collections.insert(reference.clone(), CollectionSlot::Creating);
                Ok(Reservation {
                    core: self,
                    reference: reference.clone(),
                    committed: false,
                })
            }
            Some(CollectionSlot::Dropping) => Err(LogPoseError::failed_precondition(format!(
                "collection '{}/{}' is being dropped",
                reference.database_name, reference.collection_name
            ))),
            Some(_) => Err(already_exists(reference)),
        }
    }

    /// Recover every collection directory, in parallel on the I/O pool, and register each one.
    fn recover_collections(self: &Arc<Self>) -> Result<()> {
        let collections_root = self.collections_root();
        let dirs = self.collection_dirs_to_recover(&collections_root)?;
        let (sender, results) = mpsc::channel();
        for (index, dir) in dirs.iter().enumerate() {
            let core = CoreRef::new(self);
            let dir = dir.clone();
            let sender = sender.clone();
            self.runtime.io.execute(move || {
                let _ = sender.send((index, core.recover_collection(&dir)));
            })?;
        }
        drop(sender);
        let mut recovered = (0..dirs.len()).map(|_| None).collect::<Vec<_>>();
        for (index, result) in results {
            recovered[index] = Some(result);
        }

        let mut unreadable = Vec::new();
        let mut collections = self.write_collections();
        for (dir, result) in dirs.into_iter().zip(recovered) {
            let result = result.unwrap_or_else(|| RecoveredCollection::Unreadable {
                error: "recovery panicked".to_owned(),
            });
            let (reference, slot) = match result {
                RecoveredCollection::Open(handle) => (
                    handle.meta().reference.clone(),
                    CollectionSlot::Open(handle),
                ),
                RecoveredCollection::Failed {
                    reference,
                    descriptor,
                    error,
                } => (
                    reference,
                    CollectionSlot::Failed(Arc::new(FailedCollection {
                        descriptor: descriptor.map(|descriptor| *descriptor),
                        error,
                    })),
                ),
                RecoveredCollection::Unreadable { error } => {
                    unreadable.push(UnreadableCollection { dir, error });
                    continue;
                }
            };
            if collections.contains_key(&reference) {
                unreadable.push(UnreadableCollection {
                    dir,
                    error: format!(
                        "another directory already holds collection '{}/{}'",
                        reference.database_name, reference.collection_name
                    ),
                });
                continue;
            }
            collections.insert(reference, slot);
        }
        drop(collections);
        let _ = self.unreadable.set(unreadable);
        Ok(())
    }

    /// Collection directories in name order, after removing the ones a crash left behind: a
    /// directory without `descriptor.json` is an unacknowledged create (the descriptor is
    /// written last), and a `*.dropped` directory is a committed drop.
    fn collection_dirs_to_recover(&self, collections_root: &Path) -> Result<Vec<PathBuf>> {
        if !path_exists(self.vfs.as_ref(), collections_root)? {
            return Ok(Vec::new());
        }
        let mut entries = self
            .vfs
            .list(collections_root)
            .map_err(|error| io_message("failed to list collections", error))?;
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        let mut dirs = Vec::new();
        let mut removed = false;
        for entry in entries.into_iter().filter(|entry| entry.is_dir) {
            let dir = collections_root.join(&entry.name);
            let abandoned = entry.name.ends_with(DROPPED_DIR_SUFFIX)
                || !path_exists(self.vfs.as_ref(), &dir.join("descriptor.json"))?;
            if abandoned {
                self.vfs.remove_dir_all(&dir).map_err(|error| {
                    io_message("failed to remove an abandoned collection directory", error)
                })?;
                removed = true;
            } else {
                dirs.push(dir);
            }
        }
        if removed {
            sync_dir(self.vfs.as_ref(), collections_root)?;
        }
        Ok(dirs)
    }
}

impl fmt::Debug for EngineCore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EngineCore")
            .field("root", &self.root)
            .finish()
    }
}

/// A name reserved in the collection map for a create in progress.
pub(crate) struct Reservation<'a> {
    core: &'a EngineCore,
    reference: CollectionRef,
    committed: bool,
}

impl Reservation<'_> {
    /// Replace the reservation with the created collection.
    pub(crate) fn commit(mut self, handle: Arc<CollectionHandle>) {
        self.core
            .write_collections()
            .insert(self.reference.clone(), CollectionSlot::Open(handle));
        self.committed = true;
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let mut collections = self.core.write_collections();
            if matches!(
                collections.get(&self.reference),
                Some(CollectionSlot::Creating)
            ) {
                collections.remove(&self.reference);
            }
        }
    }
}

impl CoreRef {
    fn drop_collection(&self, reference: &CollectionRef) -> Result<()> {
        let slot = {
            let mut collections = self.write_collections();
            match collections.get(reference) {
                Some(CollectionSlot::Open(_) | CollectionSlot::Failed(_)) => {}
                Some(CollectionSlot::Creating) => {
                    return Err(LogPoseError::failed_precondition(format!(
                        "collection '{}/{}' is being created",
                        reference.database_name, reference.collection_name
                    )));
                }
                Some(CollectionSlot::Dropping) | None => return Err(not_found(reference)),
            }
            collections.insert(reference.clone(), CollectionSlot::Dropping)
        };
        let result = match &slot {
            Some(CollectionSlot::Open(handle)) => self.retire_open_collection(handle),
            Some(CollectionSlot::Failed(failed)) => match &failed.descriptor {
                Some(descriptor) => self
                    .retire_collection_dir(&descriptor.root_path)
                    .map_err(|failure| failure.error),
                None => Err(LogPoseError::failed_precondition(format!(
                    "collection '{}/{}' has an unreadable descriptor and cannot be dropped: {}",
                    reference.database_name, reference.collection_name, failed.error
                ))),
            },
            _ => Ok(()),
        };
        let mut collections = self.write_collections();
        match result {
            Ok(()) => {
                collections.remove(reference);
                Ok(())
            }
            Err(error) => {
                // The drop did not commit, so the collection still exists. If its directory was
                // not renamed, an open handle serves again; otherwise the rename's durability is
                // unknown, the handle keeps refusing calls, and a reopen settles the outcome.
                if let Some(slot) = slot {
                    collections.insert(reference.clone(), slot);
                }
                Err(error)
            }
        }
    }

    /// Stop an open collection and retire its directory.
    fn retire_open_collection(&self, handle: &Arc<CollectionHandle>) -> Result<()> {
        handle.mark_dropped();
        // Wait for the in-flight maintenance job and write; both check the state again under
        // these locks, so nothing starts after them. The lock order is maintenance, then writer.
        let _maintenance = handle
            .maintenance
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut writer = handle.writer.lock().unwrap_or_else(PoisonError::into_inner);
        writer.wal = None;
        self.retire_collection_dir(&handle.meta().dir)
            .map_err(|failure| {
                if !failure.renamed {
                    // Nothing on disk changed, so the collection serves again; the next write
                    // reopens its WAL.
                    handle.mark_open();
                }
                failure.error
            })
    }

    /// Durably rename `dir` to `<dir>.dropped`, which commits the drop, then remove it. A crash
    /// after the rename leaves a `*.dropped` directory that the next open removes.
    fn retire_collection_dir(&self, dir: &Path) -> std::result::Result<(), RetireFailure> {
        let parent = logpose_vfs::parent_dir(dir);
        let mut retired = dir.as_os_str().to_owned();
        retired.push(DROPPED_DIR_SUFFIX);
        let retired = PathBuf::from(retired);
        self.vfs
            .rename(dir, &retired)
            .map_err(|error| RetireFailure {
                renamed: false,
                error: io_message("failed to retire the collection directory", error),
            })?;
        sync_dir(self.vfs.as_ref(), parent).map_err(|error| RetireFailure {
            renamed: true,
            error,
        })?;
        // The drop is committed. Failing to remove the files only delays their cleanup to the
        // next open.
        if self.vfs.remove_dir_all(&retired).is_ok() {
            let _ = sync_dir(self.vfs.as_ref(), parent);
        }
        Ok(())
    }
}

/// Why a collection directory could not be retired.
struct RetireFailure {
    /// Whether the rename happened in the live namespace (its durability is then unknown).
    renamed: bool,
    error: LogPoseError,
}

/// A reference to the engine state held by an engine task (an I/O pool job, a maintenance job).
///
/// The engine's drop waits until every `CoreRef` is gone, so no task can touch the storage root
/// after the engine released its lock.
pub(crate) struct CoreRef {
    // Field order matters: the core reference is released before the task is counted done.
    core: Arc<EngineCore>,
    _task: TaskGuard,
}

impl CoreRef {
    fn new(core: &Arc<EngineCore>) -> Self {
        core.tasks.enter();
        Self {
            core: Arc::clone(core),
            _task: TaskGuard(Arc::clone(&core.tasks)),
        }
    }
}

impl Clone for CoreRef {
    fn clone(&self) -> Self {
        Self::new(&self.core)
    }
}

impl Deref for CoreRef {
    type Target = EngineCore;

    fn deref(&self) -> &EngineCore {
        &self.core
    }
}

/// Counts live [`CoreRef`]s so that shutdown can wait for them.
#[derive(Default)]
struct TaskTracker {
    live: Mutex<usize>,
    idle: Condvar,
}

impl TaskTracker {
    fn enter(&self) {
        *self.live.lock().unwrap_or_else(PoisonError::into_inner) += 1;
    }

    fn exit(&self) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        *live = live.saturating_sub(1);
        if *live == 0 {
            self.idle.notify_all();
        }
    }

    fn wait_idle(&self) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        while *live > 0 {
            live = self.idle.wait(live).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

struct TaskGuard(Arc<TaskTracker>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.exit();
    }
}

pub(crate) fn not_found(reference: &CollectionRef) -> LogPoseError {
    LogPoseError::not_found(
        ResourceKind::Collection,
        format!("{}/{}", reference.database_name, reference.collection_name),
    )
}

pub(crate) fn already_exists(reference: &CollectionRef) -> LogPoseError {
    LogPoseError::already_exists(
        ResourceKind::Collection,
        format!("{}/{}", reference.database_name, reference.collection_name),
    )
}

#[cfg(test)]
mod tests;
