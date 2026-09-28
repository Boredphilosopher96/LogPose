//! Garbage collection: version-refcounted segment files, the engine-wide deletion queue, the
//! recovery durability barrier, and orphan cleanup.
//!
//! Segment files are the only files readers touch lazily, so they are the only files counted
//! through `Version`s. The writer holds one [`FileHandle`] per segment of the durable manifest
//! (`live_files`), and every `Version` holds the handles of its segments. When a manifest that
//! drops a segment becomes durable, the writer marks its handle obsolete and then drops its
//! reference, so whichever thread drops the last reference (the writer, a reader finishing a
//! request, or the token reaper) sees the mark and enqueues the removal (I7).
//!
//! WAL files, manifests and abandoned job outputs are never read by a `Version`: the writer
//! enqueues them directly once a durable manifest supersedes them (or, for a failed job, once
//! it is certain no durable manifest names them).
//!
//! The queue drains on the I/O pool: `remove_file`, `crash_point(GcAfterRemove)`, and one
//! directory sync per touched directory per batch. A removal that is lost to a crash, or that
//! never ran because the engine shut down, is redone by orphan cleanup at the next open.

use crate::{
    engine::{CoreRef, EngineCore},
    manifest::{CURRENT_TEMP_FILE, Manifest, manifests_dir, parse_manifest_file_name},
    paths::{
        FLAT_SIDECAR_EXTENSION, HNSW_SIDECAR_EXTENSION, INDEXES_DIR, SEGMENTS_DIR, TMP_DIR,
        V1_SEGMENT_EXTENSION, parse_unit_file_name,
    },
};
use logpose_types::{LogPoseError, Result, UnitId};
use logpose_vfs::{CrashPoint, Vfs, parent_dir};
use std::{
    collections::{BTreeSet, HashSet},
    fmt,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, PoisonError, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

/// The files of one segment, shared by the writer's `live_files` and every `Version` that
/// contains the segment. Removed from disk when the last reference drops after the writer
/// marked it obsolete.
pub(crate) struct FileHandle {
    unit: UnitId,
    paths: Vec<PathBuf>,
    obsolete: AtomicBool,
    gc: GcQueue,
}

impl FileHandle {
    pub(crate) fn new(unit: UnitId, paths: Vec<PathBuf>, gc: GcQueue) -> Self {
        Self {
            unit,
            paths,
            obsolete: AtomicBool::new(false),
            gc,
        }
    }

    /// The segment's unit.
    pub(crate) fn unit(&self) -> UnitId {
        self.unit
    }

    /// Mark the files for removal once the last reference drops. Called only by the writer,
    /// only after the manifest that drops the segment is durable, and while the writer still
    /// holds its own reference, so the last dropper always observes the mark.
    pub(crate) fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire) {
            self.gc.remove(std::mem::take(&mut self.paths));
        }
    }
}

impl fmt::Debug for FileHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileHandle")
            .field("unit", &self.unit)
            .field("obsolete", &self.obsolete.load(Ordering::Relaxed))
            .finish()
    }
}

/// The engine-wide queue of file removals. Cheap to clone.
#[derive(Clone)]
pub(crate) struct GcQueue {
    shared: Arc<GcShared>,
}

struct GcShared {
    /// The engine whose I/O pool drains the queue. Weak, because file handles live inside the
    /// engine's own `Version`s.
    core: Weak<EngineCore>,
    state: Mutex<GcState>,
    idle: Condvar,
}

#[derive(Default)]
struct GcState {
    pending: Vec<PathBuf>,
    /// Whether a drain job is queued or running.
    draining: bool,
    /// Releases handed to a pool by [`GcQueue::release_on`] that have not run yet. Each may
    /// drop the last reference to a `Version` and so enqueue removals.
    releasing: usize,
    /// Files removed since the engine opened (a test and diagnostics counter).
    removed: u64,
}

/// Counts one [`GcQueue::release_on`] job until it has dropped what it releases, even if the
/// drop panics or the pool discards the job.
struct Releasing(GcQueue);

impl Drop for Releasing {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.releasing -= 1;
        if state.releasing == 0 && !state.draining {
            self.0.shared.idle.notify_all();
        }
    }
}

impl GcQueue {
    pub(crate) fn new(core: Weak<EngineCore>) -> Self {
        Self {
            shared: Arc::new(GcShared {
                core,
                state: Mutex::new(GcState::default()),
                idle: Condvar::new(),
            }),
        }
    }

    /// Remove `paths` in the background. Missing files are ignored.
    pub(crate) fn remove(&self, paths: impl IntoIterator<Item = PathBuf>) {
        let start = {
            let mut state = self.lock();
            state.pending.extend(paths);
            if state.draining || state.pending.is_empty() {
                false
            } else {
                state.draining = true;
                true
            }
        };
        if start {
            self.start_drain();
        }
    }

    /// Drop `value` (released pins' `Version`s, which may be the last holders of large retired
    /// state and of obsolete files) on `pool` instead of the caller's thread. [`Self::wait_idle`]
    /// waits for the drop and for the removals it enqueues.
    pub(crate) fn release_on(&self, pool: &rayon::ThreadPool, value: impl Send + 'static) {
        self.lock().releasing += 1;
        let releasing = Releasing(self.clone());
        pool.spawn(move || {
            drop(value);
            drop(releasing);
        });
    }

    /// Block until every queued removal has run (or was dropped because the engine shut down),
    /// including those the releases queued by [`Self::release_on`] enqueue.
    pub(crate) fn wait_idle(&self) {
        let mut state = self.lock();
        while state.draining || state.releasing > 0 {
            state = self
                .shared
                .idle
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Files removed so far.
    pub(crate) fn removed(&self) -> u64 {
        self.lock().removed
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, GcState> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn start_drain(&self) {
        let core = self
            .shared
            .core
            .upgrade()
            .map(|core| CoreRef::new(&core))
            .filter(|core| !core.is_shutting_down());
        let Some(core) = core else {
            // The engine is gone or going: nothing may touch the root now. Orphan cleanup at
            // the next open removes these files.
            self.abandon();
            return;
        };
        let queue = self.clone();
        let pool = core.clone();
        if pool
            .runtime()
            .io
            .execute(move || queue.drain(&core))
            .is_err()
        {
            self.abandon();
        }
    }

    fn abandon(&self) {
        let mut state = self.lock();
        state.pending.clear();
        state.draining = false;
        self.shared.idle.notify_all();
    }

    /// Remove queued files until the queue is empty. Runs on the I/O pool.
    fn drain(&self, core: &CoreRef) {
        let vfs = core.vfs.as_ref();
        loop {
            let batch = std::mem::take(&mut self.lock().pending);
            if batch.is_empty() {
                let mut state = self.lock();
                if state.pending.is_empty() {
                    state.draining = false;
                    self.shared.idle.notify_all();
                    return;
                }
                continue;
            }
            let mut dirs = BTreeSet::new();
            let mut removed = 0;
            for path in batch {
                match vfs.remove_file(&path) {
                    Ok(()) => {
                        removed += 1;
                        dirs.insert(parent_dir(&path).to_path_buf());
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => {
                        tracing::warn!(
                            path = %path.display(),
                            %error,
                            "failed to remove an obsolete file; orphan cleanup retries at the next open"
                        );
                    }
                }
                if vfs.crash_point(CrashPoint::GcAfterRemove).is_err() {
                    self.abandon();
                    return;
                }
            }
            // Only for prompt space reclamation: nothing references these files, so a removal
            // a crash undoes is harmless.
            for dir in dirs {
                if let Err(error) = vfs.sync_dir(&dir) {
                    tracing::warn!(dir = %dir.display(), %error, "failed to sync after removals");
                }
            }
            self.lock().removed += removed;
        }
    }
}

impl fmt::Debug for GcQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        formatter
            .debug_struct("GcQueue")
            .field("pending", &state.pending.len())
            .field("draining", &state.draining)
            .field("removed", &state.removed)
            .finish()
    }
}

/// The durability barrier of recovery, for everything but `wal/` (the WAL layer syncs its own
/// files and directory in `WalRecovery::open`): sync the collection directory and each of its
/// data directories, so that recovery reasons only about state that is on disk. Without it, an
/// in-process reopen after a poisoned publish (a `CURRENT` rename whose directory sync failed)
/// could trust a `CURRENT` that exists only in the page cache, and orphan cleanup would delete
/// the segments of the manifest that is actually durable.
pub(crate) fn durability_barrier(vfs: &dyn Vfs, dir: &Path) -> Result<()> {
    let sync = |path: &Path| {
        vfs.sync_dir(path).map_err(|error| {
            LogPoseError::io(
                format!(
                    "durability barrier: failed to sync directory '{}'",
                    path.display()
                ),
                error,
            )
        })
    };
    sync(dir)?;
    for child in [
        manifests_dir(dir),
        dir.join(SEGMENTS_DIR),
        dir.join(INDEXES_DIR),
        dir.join(TMP_DIR),
    ] {
        if exists(vfs, &child)? {
            sync(&child)?;
        }
    }
    Ok(())
}

/// What orphan cleanup found.
#[derive(Debug, Default)]
pub(crate) struct OrphanCleanup {
    /// The newest manifest generation below the current one that was kept, for inspection.
    pub(crate) previous_generation: Option<u64>,
    /// The first manifest generation above every generation seen on disk, removed ones included.
    pub(crate) next_manifest_gen: u64,
    /// The first unit id above every unit id seen on disk, removed files included.
    pub(crate) next_unit_id: u32,
    /// Every removed path.
    pub(crate) removed: Vec<PathBuf>,
}

/// Remove every file of the collection in `dir` that `manifest` (the one `CURRENT` names)
/// does not reference, before the writer starts, so no id the counters hand out again can
/// collide with a leftover:
///
/// 1. `segments/` and `indexes/`: every segment or sidecar whose unit is not in `manifest`,
///    and every `.tmp`; `tmp/`: everything.
/// 2. `manifests/`: every generation but the current one and the newest one below it
///    (generations can have gaps after a failed publish), including those above the current.
/// 3. `CURRENT.tmp`.
/// 4. `sync_dir` of every changed directory, then `crash_point(RecoveryAfterOrphanCleanup)`.
///
/// It also reports the first unit id and manifest generation above every one seen on disk, so
/// the writer never reissues a name a leftover of a failed attempt used, even when the reopen
/// happens in the same process (the counters in `manifest` alone would reissue them, which is
/// safe only because the leftovers are gone).
///
/// WAL files at or below the checkpoint are removed by the caller through the WAL writer.
/// Safe only after [`durability_barrier`]: it deletes relative to `manifest`.
pub(crate) fn remove_orphans(
    vfs: &dyn Vfs,
    dir: &Path,
    manifest: &Manifest,
) -> Result<OrphanCleanup> {
    let live = manifest.units().collect::<HashSet<_>>();
    let mut cleanup = OrphanCleanup {
        next_manifest_gen: manifest.generation + 1,
        next_unit_id: manifest.next_unit_id,
        ..OrphanCleanup::default()
    };
    let mut changed = BTreeSet::new();
    let mut remove = |path: PathBuf, cleanup: &mut OrphanCleanup| -> Result<()> {
        match vfs.remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(LogPoseError::io(
                    format!("orphan cleanup: failed to remove '{}'", path.display()),
                    error,
                ));
            }
        }
        changed.insert(parent_dir(&path).to_path_buf());
        cleanup.removed.push(path);
        Ok(())
    };

    let unit_dirs: [(&str, &[&str]); 3] = [
        (SEGMENTS_DIR, &[V1_SEGMENT_EXTENSION]),
        (
            INDEXES_DIR,
            &[FLAT_SIDECAR_EXTENSION, HNSW_SIDECAR_EXTENSION],
        ),
        (TMP_DIR, &[]),
    ];
    for (child, extensions) in unit_dirs {
        for name in files_in(vfs, &dir.join(child))? {
            if let Some(unit) = unit_prefix(&name) {
                cleanup.next_unit_id = cleanup.next_unit_id.max(unit.0.saturating_add(1));
            }
            let orphan = child == TMP_DIR
                || name.ends_with(".tmp")
                || parse_unit_file_name(&name, extensions)
                    .is_some_and(|unit| !live.contains(&unit));
            if orphan {
                remove(dir.join(child).join(name), &mut cleanup)?;
            }
        }
    }

    let manifests = manifests_dir(dir);
    let generations = files_in(vfs, &manifests)?
        .into_iter()
        .filter_map(|name| parse_manifest_file_name(&name).map(|generation| (generation, name)))
        .collect::<Vec<_>>();
    cleanup.previous_generation = generations
        .iter()
        .map(|(generation, _)| *generation)
        .filter(|generation| *generation < manifest.generation)
        .max();
    for (generation, name) in generations {
        cleanup.next_manifest_gen = cleanup.next_manifest_gen.max(generation.saturating_add(1));
        if generation != manifest.generation && Some(generation) != cleanup.previous_generation {
            remove(manifests.join(name), &mut cleanup)?;
        }
    }
    remove(dir.join(CURRENT_TEMP_FILE), &mut cleanup)?;

    for changed_dir in &changed {
        vfs.sync_dir(changed_dir).map_err(|error| {
            LogPoseError::io(
                format!(
                    "orphan cleanup: failed to sync directory '{}'",
                    changed_dir.display()
                ),
                error,
            )
        })?;
    }
    vfs.crash_point(CrashPoint::RecoveryAfterOrphanCleanup)
        .map_err(|error| LogPoseError::io("interrupted after orphan cleanup", error))?;
    Ok(cleanup)
}

/// The unit a file name in `segments/`, `indexes/` or `tmp/` starts with: eight lowercase hex
/// digits and a dot.
fn unit_prefix(name: &str) -> Option<UnitId> {
    let (digits, rest) = name.split_at_checked(8)?;
    if !rest.starts_with('.')
        || !digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    u32::from_str_radix(digits, 16).ok().map(UnitId)
}

fn exists(vfs: &dyn Vfs, path: &Path) -> Result<bool> {
    logpose_vfs::exists(vfs, path)
        .map_err(|error| LogPoseError::io(format!("failed to look up '{}'", path.display()), error))
}

/// Names of the files (not directories) in `dir`; none when it does not exist.
fn files_in(vfs: &dyn Vfs, dir: &Path) -> Result<Vec<String>> {
    if !exists(vfs, dir)? {
        return Ok(Vec::new());
    }
    let mut names = vfs
        .list(dir)
        .map_err(|error| LogPoseError::io(format!("failed to list '{}'", dir.display()), error))?
        .into_iter()
        .filter(|entry| !entry.is_dir)
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests;
