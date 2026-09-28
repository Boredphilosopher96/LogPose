//! Helpers shared by the crate's unit tests.

use logpose_types::{
    Result, SeqNo,
    record::{ClientOp, PrimaryKey, Record},
};
use logpose_vfs::{CrashPoint, DirEntry, OpenMode, Vfs, VfsFile, VfsLock};
use serde_json::json;
use std::{
    io,
    io::IoSlice,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// An upsert of `id` into a collection of the shape [`CreateCollectionRequest::new`] creates,
/// with `$extra` `{"key": id}`.
///
/// [`CreateCollectionRequest::new`]: crate::CreateCollectionRequest::new
pub(crate) fn put(id: &str, vector: Vec<f32>) -> ClientOp {
    let mut record = Record::new(id).with_vector("vector", vector);
    record.extra.insert("key".to_owned(), json!(id));
    ClientOp::Upsert(record)
}

/// The schema of the single-vector shape [`CreateCollectionRequest::new`] creates.
///
/// [`CreateCollectionRequest::new`]: crate::CreateCollectionRequest::new
pub(crate) fn vector_schema(
    dimensions: usize,
    metric: logpose_types::DistanceMetric,
) -> logpose_types::schema::CollectionSchema {
    crate::CreateCollectionRequest::new("test", dimensions, metric)
        .spec
        .build_schema()
        .expect("the single-vector schema builds")
}

/// A delete of `id`.
pub(crate) fn delete(id: &str) -> ClientOp {
    ClientOp::Delete(PrimaryKey::from(id))
}

/// A fresh directory named `logpose-{prefix}-…` under the system temp directory, removed when
/// the returned guard drops, also when the test panics. Keep the guard alive for as long as
/// anything uses the directory, reopens after a simulated crash included.
pub(crate) fn unique_temp_dir(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("logpose-{prefix}-"))
        .tempdir()
        .expect("temp dir should be created")
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
    /// Every hit of this crash point fails with EIO (without crashing).
    fail_crash_point: Option<CrashPoint>,
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

    /// Fail every later hit of crash point `point` with EIO, without crashing (`None` stops
    /// failing): a job that passes it fails, and nothing else does.
    pub(crate) fn fail_crash_point(&self, point: Option<CrashPoint>) {
        self.state().fail_crash_point = point;
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
        if self.state().fail_crash_point == Some(point) {
            return Err(io::Error::from_raw_os_error(5));
        }
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

/// Every live row of `version` as its reader sees it, with the sequence number of its write,
/// ordered by key. Read straight from the `Version` (the read path proper lives in
/// `logpose-query`, which unit tests cannot link against this crate's types).
pub(crate) fn live_records(version: &crate::version::Version) -> Result<Vec<(SeqNo, Record)>> {
    let mut records = version
        .live_images()
        .into_iter()
        .map(|(seq_no, image)| {
            image
                .to_record(&version.schema)
                .map(|record| (seq_no, record))
                .map_err(|error| logpose_types::LogPoseError::internal(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    records.sort_by(|left, right| left.1.pk.cmp(&right.1.pk));
    Ok(records)
}

/// [`live_records`] of the state `at` names.
pub(crate) fn scan(
    handle: &crate::handle::CollectionHandle,
    at: impl Into<crate::state::ReadAt>,
) -> Result<Vec<(SeqNo, Record)>> {
    let (version, _) = handle.read_state(at)?;
    live_records(&version)
}

/// A row as a reader of `schema` sees it, flattened for assertions: its key as a label, its
/// first vector field, and its visible `$extra` keys plus typed scalar fields as one JSON
/// object.
pub(crate) fn flat_row(
    schema: &logpose_types::schema::CollectionSchema,
    image: &logpose_wal::codec::RowImage,
) -> (String, Vec<f32>, serde_json::Value) {
    let mut record = image.to_record(schema).expect("row should read");
    let vector = schema
        .vectors()
        .first()
        .and_then(|field| record.vectors.remove(&field.name))
        .unwrap_or_default();
    let mut fields = std::mem::take(&mut record.extra);
    for (name, value) in record.fields {
        fields.insert(name, value.into_json());
    }
    (record.pk.label(), vector, serde_json::Value::Object(fields))
}
