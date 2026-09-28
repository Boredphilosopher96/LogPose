//! Helpers shared by the crate's unit tests.

use logpose_types::{PutRecord, RecordId, WriteOperation};
use logpose_vfs::{CrashPoint, DirEntry, OpenMode, Vfs, VfsFile, VfsLock};
use serde_json::json;
use std::{
    fs, io,
    io::IoSlice,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(crate) fn put(id: &str, vector: Vec<f32>) -> WriteOperation {
    WriteOperation::Put(PutRecord {
        id: RecordId::new(id),
        vector,
        metadata: json!({"key": id}),
    })
}

pub(crate) fn unique_temp_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}

/// A [`Vfs`] wrapper that counts file syncs, can hold them at a gate, and fails chosen syncs
/// and file creations, to steer the writer into group commits and failures that a
/// [`FaultPlan`](logpose_vfs::FaultPlan) alone cannot express.
pub(crate) struct ControlledVfs {
    inner: Arc<dyn Vfs>,
    control: Arc<Control>,
}

#[derive(Default)]
struct Control {
    /// File syncs started (`sync_data` and `sync_all`).
    syncs: AtomicU64,
    state: Mutex<ControlState>,
    changed: Condvar,
}

#[derive(Default)]
struct ControlState {
    /// Syncs wait while this is set.
    held: bool,
    /// Syncs waiting at the gate.
    waiting: u64,
    /// The next this many syncs fail with EIO.
    fail_syncs: u32,
    /// Creations of paths containing this fail with ENOSPC.
    fail_creates: Option<String>,
    /// Syncs of files whose path contains this fail with EIO, this many times.
    fail_file_syncs: Option<(String, u32)>,
    /// Directory syncs of exactly this directory fail with EIO, this many times.
    fail_dir_syncs: Option<(PathBuf, u32)>,
    /// Renames onto paths containing this fail with EIO, this many times.
    fail_renames: Option<(String, u32)>,
}

/// Take one failure from `slot` when `matches` accepts its key.
fn take_failure<K>(slot: &mut Option<(K, u32)>, matches: impl FnOnce(&K) -> bool) -> bool {
    match slot {
        Some((key, count)) if *count > 0 && matches(key) => {
            *count -= 1;
            true
        }
        _ => false,
    }
}

impl ControlledVfs {
    pub(crate) fn wrap(inner: Arc<dyn Vfs>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            control: Arc::default(),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ControlState> {
        self.control
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// File syncs started so far.
    pub(crate) fn file_syncs(&self) -> u64 {
        self.control.syncs.load(Ordering::SeqCst)
    }

    /// Make every later file sync wait until [`release_syncs`](Self::release_syncs).
    pub(crate) fn hold_syncs(&self) {
        self.state().held = true;
    }

    /// Let held and later syncs through.
    pub(crate) fn release_syncs(&self) {
        self.state().held = false;
        self.control.changed.notify_all();
    }

    /// Wait until a sync is waiting at the gate.
    pub(crate) fn wait_for_held_sync(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state();
        while state.waiting == 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            state = self
                .control
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        true
    }

    /// Fail the next `count` file syncs with EIO.
    pub(crate) fn fail_next_syncs(&self, count: u32) {
        self.state().fail_syncs = count;
    }

    /// Fail creations of files whose path contains `fragment`.
    pub(crate) fn fail_creates_containing(&self, fragment: &str) {
        self.state().fail_creates = Some(fragment.to_owned());
    }

    /// Fail the next `count` file syncs of files whose path contains `fragment` with EIO.
    pub(crate) fn fail_file_syncs_containing(&self, fragment: &str, count: u32) {
        self.state().fail_file_syncs = Some((fragment.to_owned(), count));
    }

    /// Fail the next `count` syncs of directory `dir` with EIO.
    pub(crate) fn fail_dir_syncs(&self, dir: &Path, count: u32) {
        self.state().fail_dir_syncs = Some((dir.to_path_buf(), count));
    }

    /// Fail the next `count` renames onto paths containing `fragment` with EIO, changing
    /// nothing.
    pub(crate) fn fail_renames_to(&self, fragment: &str, count: u32) {
        self.state().fail_renames = Some((fragment.to_owned(), count));
    }

    /// Let every later rename through.
    pub(crate) fn stop_failing_renames(&self) {
        self.state().fail_renames = None;
    }
}

impl Control {
    fn sync(&self, path: &Path, sync: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.held {
            state.waiting += 1;
            self.changed.notify_all();
            while state.held {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            state.waiting -= 1;
        }
        let fail = state.fail_syncs > 0
            || take_failure(&mut state.fail_file_syncs, |fragment| {
                path.to_string_lossy().contains(fragment.as_str())
            });
        state.fail_syncs = state.fail_syncs.saturating_sub(1);
        drop(state);
        if fail {
            Err(io::Error::from_raw_os_error(5))
        } else {
            sync()
        }
    }
}

impl Vfs for ControlledVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        if mode == OpenMode::CreateNew
            && let Some(fragment) = &self.state().fail_creates
            && path.to_string_lossy().contains(fragment.as_str())
        {
            return Err(io::Error::from_raw_os_error(28));
        }
        let inner = self.inner.open(path, mode)?;
        Ok(Arc::new(ControlledFile {
            inner,
            path: path.to_path_buf(),
            control: Arc::clone(&self.control),
        }))
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        self.inner.list(dir)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        if take_failure(&mut self.state().fail_renames, |fragment| {
            to.to_string_lossy().contains(fragment.as_str())
        }) {
            return Err(io::Error::from_raw_os_error(5));
        }
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        if take_failure(&mut self.state().fail_dir_syncs, |failing| failing == dir) {
            return Err(io::Error::from_raw_os_error(5));
        }
        self.inner.sync_dir(dir)
    }
    fn try_lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        self.inner.try_lock_exclusive(path)
    }
    fn crash_point(&self, point: CrashPoint) -> io::Result<()> {
        self.inner.crash_point(point)
    }
}

struct ControlledFile {
    inner: Arc<dyn VfsFile>,
    path: PathBuf,
    control: Arc<Control>,
}

impl VfsFile for ControlledFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn append(&self, bufs: &[IoSlice<'_>]) -> io::Result<u64> {
        self.inner.append(bufs)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.control.sync(&self.path, || self.inner.sync_data())
    }
    fn sync_all(&self) -> io::Result<()> {
        self.control.sync(&self.path, || self.inner.sync_all())
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
}
