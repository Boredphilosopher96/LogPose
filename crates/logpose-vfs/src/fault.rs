//! [`FaultVfs`]: an in-memory filesystem that models what a crash can do to a POSIX filesystem.
//!
//! The fault model, stated as the rules the implementation enforces:
//!
//! - **Unsynced data is lost on crash.** On [`FaultVfs::crash`], each file's content becomes its
//!   content as of its last successful `sync_data`/`sync_all`, plus, per [`TearMode`], some of the
//!   bytes written since.
//! - **Torn last write.** [`TearMode::KeepRandomPrefix`] and [`TearMode::TornGarbage`] make the
//!   unsynced suffix partially present, possibly with garbage in its final 512-byte sector.
//!   [`TearMode::ReorderedPages`] persists an arbitrary subset of the unsynced 4 KiB pages, as a
//!   kernel writing back dirty pages out of order can; pages that were not written read as zeros.
//! - **Truncation is volatile too.** A `set_len` that was not followed by a sync may or may not
//!   survive a crash (except under [`TearMode::DropUnsynced`], where it never does).
//! - **fsync can fail.** A failed sync returns EIO. Like Linux writeback, it may already have
//!   written part of the unsynced suffix (a random prefix; never under
//!   [`TearMode::DropUnsynced`]), so data whose sync failed can still be on disk after a crash.
//!   The rest is left undefined: it is randomly kept or dropped at crash, and a later successful
//!   sync does not make it durable (it reads back as zeros after a crash), because the kernel
//!   marked those pages clean.
//! - **Namespace changes are volatile until the directory is synced.** Create, rename and remove
//!   edit the live namespace; only `sync_dir` makes a directory's entry set durable. On crash,
//!   each directory keeps its durable entry set plus a prefix of the changes made to it since,
//!   in order, as a journaling filesystem that commits metadata in the background can: nothing
//!   under [`TearMode::DropUnsynced`], a random prefix otherwise. So a renamed file may appear
//!   under its old name, a newly created file may vanish, a removed file may come back, and an
//!   unsynced change may also survive. A rename within one directory is atomic; a rename across
//!   directories is two independent changes, so the file may end up under both names or none.
//!   A directory whose own entry set was never synced keeps only the persisted prefix of its
//!   changes.
//! - **Crash halts the process.** After a crash triggers, every call returns an
//!   [`io::ErrorKind::Other`] error with a [`Crashed`](crate::Crashed) payload until the test calls
//!   [`FaultVfs::crash`] to compute the post-crash state. Handles from [`FaultVfs::process`] and
//!   files opened before the crash stay dead afterwards, so a leftover thread of the crashed
//!   "process" can never write into the rebooted state.
//!
//! Every random choice comes from one seeded generator, so a run is reproducible from its seed
//! as long as the engine issues the same operations in the same order.

use crate::{CrashPoint, DirEntry, OpenMode, Vfs, VfsFile, VfsLock, crashed_error};
use std::{
    collections::BTreeMap,
    io,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

const ROOT_DIR: u64 = 0;
const SECTOR_BYTES: usize = 512;
const PAGE_BYTES: usize = 4096;
const EIO: i32 = 5;
const ENOSPC: i32 = 28;

/// What faults a [`FaultVfs`] injects. The default injects nothing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    /// Crash (fail this and every later operation) when the named point is hit.
    pub crash_at: Option<CrashPoint>,
    /// Crash instead of performing the mutating operation with this zero-based index, counted
    /// since the last reboot. Mutating operations are creates, appends, syncs (file and
    /// directory), truncations, renames and removals. `Some(k)` lets exactly `k` mutating
    /// operations succeed.
    pub crash_after_ops: Option<u64>,
    /// Fail the file sync (`sync_data` or `sync_all`) with this zero-based index with EIO.
    pub fail_sync: Option<u64>,
    /// Fail the directory sync with this zero-based index with EIO.
    pub fail_sync_dir: Option<u64>,
    /// Fail appends with ENOSPC once the total appended bytes would exceed this.
    pub enospc_after_bytes: Option<u64>,
    /// How a crash treats unsynced appended bytes.
    pub tear: TearMode,
}

/// How a crash treats bytes written since a file's last successful sync.
///
/// Every mode except [`DropUnsynced`](Self::DropUnsynced) also persists a random prefix of each
/// directory's unsynced entry changes, and lets a failed sync write back part of the data.
/// Random cut points favor the extremes (nothing or everything survives), where durability
/// bugs hide.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum TearMode {
    /// Drop all unsynced bytes, truncations and directory changes.
    #[default]
    DropUnsynced,
    /// Keep a random prefix of the unsynced suffix of each file.
    KeepRandomPrefix,
    /// Keep a random prefix, then overwrite the last kept 512-byte sector with random bytes.
    TornGarbage,
    /// Keep or zero each unsynced 4 KiB page independently (out-of-order writeback).
    ReorderedPages,
}

impl TearMode {
    /// Every tear mode.
    pub const ALL: [TearMode; 4] = [
        TearMode::DropUnsynced,
        TearMode::KeepRandomPrefix,
        TearMode::TornGarbage,
        TearMode::ReorderedPages,
    ];
}

/// What [`FaultVfs::crash`] changed, for test logs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CrashReport {
    /// The crash point that triggered the crash, if a named point did.
    pub triggered_at: Option<CrashPoint>,
    /// Files whose content after the crash differs from what readers saw before it.
    pub files: Vec<TornFile>,
    /// Directories whose entry set after the crash differs from the one readers saw before it.
    pub reverted_dirs: Vec<PathBuf>,
}

/// One file changed by a crash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TornFile {
    /// A durable path of the file after the crash.
    pub path: PathBuf,
    /// Length as of the last successful sync.
    pub durable_len: u64,
    /// Length readers saw just before the crash.
    pub current_len: u64,
    /// Length after the crash.
    pub recovered_len: u64,
}

/// In-memory filesystem with deterministic, seeded fault injection. See the module docs for the
/// fault model.
///
/// Paths must be absolute and must not contain `..`. The root directory `/` always exists.
pub struct FaultVfs {
    world: Arc<Mutex<World>>,
    /// `Some(boot)` for a handle bound to one simulated process; `None` follows every reboot.
    pinned_boot: Option<u64>,
}

impl FaultVfs {
    /// Create an empty filesystem whose random choices derive from `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Arc<Self> {
        let mut dirs = BTreeMap::new();
        dirs.insert(ROOT_DIR, Namespace::new());
        Arc::new(Self {
            world: Arc::new(Mutex::new(World {
                rng: SplitMix64(seed),
                plan: FaultPlan::default(),
                boot: 0,
                crashed: false,
                triggered_at: None,
                mutating_ops: 0,
                file_syncs: 0,
                dir_syncs: 0,
                appended_bytes: 0,
                points_hit: Vec::new(),
                next_id: ROOT_DIR + 1,
                inodes: BTreeMap::new(),
                durable_dirs: dirs.clone(),
                dirs,
                unsynced_changes: BTreeMap::new(),
                locks: BTreeMap::new(),
            })),
            pinned_boot: None,
        })
    }

    /// A handle bound to the current boot: the "process" that the engine under test runs in.
    ///
    /// Once [`crash`](Self::crash) reboots the filesystem, every operation through this handle
    /// fails with a crashed error, so threads left over from the crashed process cannot touch
    /// the rebooted state. Open a new process handle for each engine open.
    #[must_use]
    pub fn process(&self) -> Arc<dyn Vfs> {
        let boot = self.lock().boot;
        Arc::new(Self {
            world: Arc::clone(&self.world),
            pinned_boot: Some(boot),
        })
    }

    /// Replace the fault plan. Counters are not reset.
    pub fn set_plan(&self, plan: FaultPlan) {
        self.lock().plan = plan;
    }

    /// The current fault plan.
    #[must_use]
    pub fn plan(&self) -> FaultPlan {
        self.lock().plan.clone()
    }

    /// Simulate power loss now: apply the crash model, clear the crashed state and every lock,
    /// reset the plan to [`FaultPlan::default`] and the operation counters to zero, and start a
    /// new boot. Works whether or not a planned crash has triggered.
    pub fn crash(&self) -> CrashReport {
        self.lock().apply_crash()
    }

    /// Whether a planned crash has triggered and [`crash`](Self::crash) has not run since.
    #[must_use]
    pub fn is_crashed(&self) -> bool {
        self.lock().crashed
    }

    /// Number of mutating operations performed since the last reboot. Tests run a scenario once
    /// cleanly and use this to enumerate [`FaultPlan::crash_after_ops`].
    #[must_use]
    pub fn mutating_ops(&self) -> u64 {
        self.lock().mutating_ops
    }

    /// Number of file syncs (`sync_data`/`sync_all`) attempted since the last reboot; the index
    /// the next one has for [`FaultPlan::fail_sync`].
    #[must_use]
    pub fn file_syncs(&self) -> u64 {
        self.lock().file_syncs
    }

    /// Crash points reached since the last reboot, in order.
    #[must_use]
    pub fn crash_points_hit(&self) -> Vec<CrashPoint> {
        self.lock().points_hit.clone()
    }

    /// Number of reboots so far.
    #[must_use]
    pub fn boot(&self) -> u64 {
        self.lock().boot
    }

    /// Overwrite bytes of a file in place, in both its live and its durable content, extending
    /// it with zeros if needed. Byte-level fault injection for corruption tests.
    pub fn corrupt(&self, path: &Path, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut world = self.lock();
        let ino = world.file_inode(path)?;
        let offset = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset too large"))?;
        let inode = world.inode_mut(ino)?;
        for content in [&mut inode.current, &mut inode.durable] {
            let end = offset + bytes.len();
            if content.len() < end {
                content.resize(end, 0);
            }
            content[offset..end].copy_from_slice(bytes);
        }
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, World> {
        self.world.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn live(&self) -> io::Result<MutexGuard<'_, World>> {
        let world = self.lock();
        world.check_alive(self.pinned_boot)?;
        Ok(world)
    }
}

impl Vfs for FaultVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        let mut world = self.live()?;
        let (parent, name) = world.split(path)?;
        let existing = world.entry(parent, &name)?;
        let ino = match (mode, existing) {
            (OpenMode::Read | OpenMode::Append, Some(Node::File(ino))) => ino,
            (OpenMode::Read | OpenMode::Append, Some(Node::Dir(_))) => {
                return Err(io::Error::new(
                    io::ErrorKind::IsADirectory,
                    format!("'{}' is a directory", path.display()),
                ));
            }
            (OpenMode::Read | OpenMode::Append, None) => return Err(not_found(path)),
            (OpenMode::CreateNew, Some(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("'{}' already exists", path.display()),
                ));
            }
            (OpenMode::CreateNew, None) => {
                world.begin_mutation(self.pinned_boot)?;
                let ino = world.allocate_id();
                world.inodes.insert(ino, Inode::default());
                world.change_namespace(parent, vec![(name, Some(Node::File(ino)))])?;
                ino
            }
        };
        Ok(Arc::new(FaultFile {
            world: Arc::clone(&self.world),
            ino,
            boot: world.boot,
            writable: mode != OpenMode::Read,
        }))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut world = self.live()?;
        let components = components(path)?;
        let mut dir = ROOT_DIR;
        let mut index = 0;
        while index < components.len() {
            match world.entry(dir, &components[index])? {
                Some(Node::Dir(child)) => dir = child,
                Some(Node::File(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotADirectory,
                        format!("'{}' has a file component", path.display()),
                    ));
                }
                None => break,
            }
            index += 1;
        }
        if index == components.len() {
            return Ok(());
        }
        world.begin_mutation(self.pinned_boot)?;
        for name in &components[index..] {
            let child = world.allocate_id();
            world.dirs.insert(child, Namespace::new());
            world.change_namespace(dir, vec![(name.clone(), Some(Node::Dir(child)))])?;
            dir = child;
        }
        Ok(())
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        let world = self.live()?;
        let dir = world.dir_id(dir)?;
        let namespace = world.namespace(dir)?;
        namespace
            .iter()
            .map(|(name, node)| {
                Ok(match node {
                    Node::Dir(_) => DirEntry {
                        name: name.clone(),
                        is_dir: true,
                        len: 0,
                    },
                    Node::File(ino) => DirEntry {
                        name: name.clone(),
                        is_dir: false,
                        len: world.inode(*ino)?.current.len() as u64,
                    },
                })
            })
            .collect()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut world = self.live()?;
        let (from_parent, from_name) = world.split(from)?;
        let (to_parent, to_name) = world.split(to)?;
        let ino = match world.entry(from_parent, &from_name)? {
            Some(Node::File(ino)) => ino,
            Some(Node::Dir(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "FaultVfs renames files only",
                ));
            }
            None => return Err(not_found(from)),
        };
        if let Some(Node::Dir(_)) = world.entry(to_parent, &to_name)? {
            return Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("rename target '{}' is a directory", to.display()),
            ));
        }
        if from_parent == to_parent && from_name == to_name {
            return Ok(());
        }
        world.begin_mutation(self.pinned_boot)?;
        if from_parent == to_parent {
            world.change_namespace(
                from_parent,
                vec![(from_name, None), (to_name, Some(Node::File(ino)))],
            )
        } else {
            world.change_namespace(from_parent, vec![(from_name, None)])?;
            world.change_namespace(to_parent, vec![(to_name, Some(Node::File(ino)))])
        }
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut world = self.live()?;
        let (parent, name) = world.split(path)?;
        match world.entry(parent, &name)? {
            Some(Node::File(_)) => {
                world.begin_mutation(self.pinned_boot)?;
                world.change_namespace(parent, vec![(name, None)])
            }
            Some(Node::Dir(_)) => Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("'{}' is a directory", path.display()),
            )),
            None => Err(not_found(path)),
        }
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut world = self.live()?;
        let (parent, name) = world.split(path)?;
        match world.entry(parent, &name)? {
            Some(Node::Dir(_)) => {
                world.begin_mutation(self.pinned_boot)?;
                world.change_namespace(parent, vec![(name, None)])
            }
            Some(Node::File(_)) => Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                format!("'{}' is not a directory", path.display()),
            )),
            None => Err(not_found(path)),
        }
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut world = self.live()?;
        let dir = world.dir_id(dir)?;
        world.begin_mutation(self.pinned_boot)?;
        let index = world.dir_syncs;
        world.dir_syncs += 1;
        if world.plan.fail_sync_dir == Some(index) {
            return Err(io::Error::from_raw_os_error(EIO));
        }
        let namespace = world.namespace(dir)?.clone();
        world.durable_dirs.insert(dir, namespace);
        world.unsynced_changes.remove(&dir);
        Ok(())
    }

    fn try_lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        let mut world = self.live()?;
        let (parent, _) = world.split(path)?;
        world.namespace(parent)?;
        let key = path.to_path_buf();
        if world.locks.contains_key(&key) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("'{}' is locked by another holder", path.display()),
            ));
        }
        let id = world.allocate_id();
        world.locks.insert(key.clone(), id);
        Ok(Box::new(FaultLock {
            world: Arc::clone(&self.world),
            path: key,
            id,
        }))
    }

    fn crash_point(&self, point: CrashPoint) -> io::Result<()> {
        let mut world = self.live()?;
        world.points_hit.push(point);
        if world.plan.crash_at == Some(point) {
            world.crashed = true;
            world.triggered_at = Some(point);
            return Err(crashed_error());
        }
        Ok(())
    }
}

struct FaultFile {
    world: Arc<Mutex<World>>,
    ino: u64,
    /// Boot the file was opened in; the handle dies with that boot.
    boot: u64,
    writable: bool,
}

impl FaultFile {
    fn live(&self) -> io::Result<MutexGuard<'_, World>> {
        let world = self.world.lock().unwrap_or_else(PoisonError::into_inner);
        world.check_alive(Some(self.boot))?;
        Ok(world)
    }

    fn writable(&self) -> io::Result<()> {
        if self.writable {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "file was opened read-only",
            ))
        }
    }

    fn sync(&self) -> io::Result<()> {
        let mut world = self.live()?;
        world.begin_mutation(Some(self.boot))?;
        let index = world.file_syncs;
        world.file_syncs += 1;
        if world.plan.fail_sync == Some(index) {
            // Writeback may have reached the disk for part of the data before the error.
            let written = if world.plan.tear == TearMode::DropUnsynced {
                0
            } else {
                let unsynced = world.inode(self.ino)?.unsynced_len();
                world.rng.cut(unsynced)
            };
            world.inode_mut(self.ino)?.fail_sync(written);
            return Err(io::Error::from_raw_os_error(EIO));
        }
        world.inode_mut(self.ino)?.sync();
        Ok(())
    }
}

impl VfsFile for FaultFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let world = self.live()?;
        let content = &world.inode(self.ino)?.current;
        let Ok(start) = usize::try_from(offset) else {
            return Ok(0);
        };
        if start >= content.len() {
            return Ok(0);
        }
        let read = buf.len().min(content.len() - start);
        buf[..read].copy_from_slice(&content[start..start + read]);
        Ok(read)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        if self.read_at(buf, offset)? == buf.len() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        }
    }

    fn append(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<u64> {
        self.writable()?;
        let mut world = self.live()?;
        world.begin_mutation(Some(self.boot))?;
        let total = bufs.iter().map(|slice| slice.len() as u64).sum::<u64>();
        if let Some(limit) = world.plan.enospc_after_bytes
            && world.appended_bytes.saturating_add(total) > limit
        {
            return Err(io::Error::from_raw_os_error(ENOSPC));
        }
        world.appended_bytes = world.appended_bytes.saturating_add(total);
        let inode = world.inode_mut(self.ino)?;
        for slice in bufs {
            inode.current.extend_from_slice(slice);
        }
        Ok(inode.current.len() as u64)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.sync()
    }

    fn sync_all(&self) -> io::Result<()> {
        self.sync()
    }

    fn len(&self) -> io::Result<u64> {
        let world = self.live()?;
        Ok(world.inode(self.ino)?.current.len() as u64)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.writable()?;
        let len = usize::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "length too large"))?;
        let mut world = self.live()?;
        world.begin_mutation(Some(self.boot))?;
        let inode = world.inode_mut(self.ino)?;
        inode.current.resize(len, 0);
        inode.stable_len = inode.stable_len.min(len);
        for range in &mut inode.poisoned {
            range.1 = range.1.min(len);
        }
        inode.poisoned.retain(|range| range.0 < range.1);
        Ok(())
    }
}

struct FaultLock {
    world: Arc<Mutex<World>>,
    path: PathBuf,
    id: u64,
}

impl VfsLock for FaultLock {}

impl Drop for FaultLock {
    fn drop(&mut self) {
        let mut world = self.world.lock().unwrap_or_else(PoisonError::into_inner);
        // A crash clears every lock, and a new holder may have taken this path since.
        if world.locks.get(&self.path) == Some(&self.id) {
            world.locks.remove(&self.path);
        }
    }
}

type Namespace = BTreeMap<String, Node>;

/// One namespace operation on one directory, applied atomically: each name is set to the node
/// or, for `None`, removed.
type NamespaceChange = Vec<(String, Option<Node>)>;

fn apply_change(namespace: &mut Namespace, change: &NamespaceChange) {
    for (name, node) in change {
        match node {
            Some(node) => {
                namespace.insert(name.clone(), *node);
            }
            None => {
                namespace.remove(name);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Node {
    File(u64),
    Dir(u64),
}

#[derive(Default)]
struct Inode {
    /// Content visible to readers now.
    current: Vec<u8>,
    /// Content as of the last successful sync (with poisoned ranges zeroed).
    durable: Vec<u8>,
    /// Smallest length `current` had since the last successful sync. `current[..stable_len]`
    /// equals `durable[..stable_len]` apart from poisoned ranges.
    stable_len: usize,
    /// Byte ranges that a failed sync left undefined; a later successful sync persists zeros
    /// there instead of the data readers see.
    poisoned: Vec<(usize, usize)>,
}

impl Inode {
    fn with_content(content: Vec<u8>) -> Self {
        Self {
            stable_len: content.len(),
            durable: content.clone(),
            current: content,
            poisoned: Vec::new(),
        }
    }

    /// The bytes a writeback of `current[start..end]` puts on disk: zeros where an earlier failed
    /// sync already dropped the data.
    fn written_back(&self, start: usize, end: usize) -> Vec<u8> {
        let mut bytes = self.current[start..end].to_vec();
        for &(poison_start, poison_end) in &self.poisoned {
            let from = poison_start.max(start);
            let to = poison_end.min(end);
            if from < to {
                bytes[from - start..to - start].fill(0);
            }
        }
        bytes
    }

    fn sync(&mut self) {
        self.durable = self.written_back(0, self.current.len());
        self.stable_len = self.current.len();
    }

    /// Bytes written since the last successful sync.
    fn unsynced_len(&self) -> usize {
        self.current.len() - self.stable_len
    }

    /// A sync failed after writing back the first `written` unsynced bytes and the size that
    /// covers them: those are on disk now, and the rest of the unsynced suffix is poisoned.
    fn fail_sync(&mut self, written: usize) {
        if written > 0 {
            let start = self.stable_len;
            let end = start + written;
            let mut durable = self.durable.clone();
            durable.resize(start, 0);
            durable.extend_from_slice(&self.written_back(start, end));
            self.durable = durable;
            self.stable_len = end;
        }
        self.poison_unsynced();
    }

    fn poison_unsynced(&mut self) {
        let start = self.stable_len.min(self.durable.len());
        if start < self.current.len() {
            self.poisoned.push((start, self.current.len()));
        }
    }

    fn is_clean(&self) -> bool {
        self.current == self.durable
    }
}

struct World {
    rng: SplitMix64,
    plan: FaultPlan,
    boot: u64,
    crashed: bool,
    triggered_at: Option<CrashPoint>,
    mutating_ops: u64,
    file_syncs: u64,
    dir_syncs: u64,
    appended_bytes: u64,
    points_hit: Vec<CrashPoint>,
    next_id: u64,
    inodes: BTreeMap<u64, Inode>,
    /// Live directory namespaces, keyed by directory id.
    dirs: BTreeMap<u64, Namespace>,
    /// Namespaces as of each directory's last `sync_dir`.
    durable_dirs: BTreeMap<u64, Namespace>,
    /// Changes made to each directory since its last `sync_dir`, oldest first.
    unsynced_changes: BTreeMap<u64, Vec<NamespaceChange>>,
    locks: BTreeMap<PathBuf, u64>,
}

impl World {
    fn check_alive(&self, pinned_boot: Option<u64>) -> io::Result<()> {
        if self.crashed || pinned_boot.is_some_and(|boot| boot != self.boot) {
            return Err(crashed_error());
        }
        Ok(())
    }

    /// Count one mutating operation, or trigger the planned crash instead of performing it.
    fn begin_mutation(&mut self, pinned_boot: Option<u64>) -> io::Result<()> {
        self.check_alive(pinned_boot)?;
        if self
            .plan
            .crash_after_ops
            .is_some_and(|limit| self.mutating_ops >= limit)
        {
            self.crashed = true;
            return Err(crashed_error());
        }
        self.mutating_ops += 1;
        Ok(())
    }

    fn allocate_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn namespace(&self, dir: u64) -> io::Result<&Namespace> {
        self.dirs
            .get(&dir)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "directory was removed"))
    }

    /// Apply `change` to the live namespace of `dir` and log it as unsynced.
    fn change_namespace(&mut self, dir: u64, change: NamespaceChange) -> io::Result<()> {
        let namespace = self
            .dirs
            .get_mut(&dir)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "directory was removed"))?;
        apply_change(namespace, &change);
        self.unsynced_changes.entry(dir).or_default().push(change);
        Ok(())
    }

    fn inode(&self, ino: u64) -> io::Result<&Inode> {
        self.inodes
            .get(&ino)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode does not exist"))
    }

    fn inode_mut(&mut self, ino: u64) -> io::Result<&mut Inode> {
        self.inodes
            .get_mut(&ino)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode does not exist"))
    }

    fn entry(&self, dir: u64, name: &str) -> io::Result<Option<Node>> {
        Ok(self.namespace(dir)?.get(name).copied())
    }

    fn dir_id(&self, path: &Path) -> io::Result<u64> {
        let mut dir = ROOT_DIR;
        for name in components(path)? {
            match self.entry(dir, &name)? {
                Some(Node::Dir(child)) => dir = child,
                Some(Node::File(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotADirectory,
                        format!("'{}' is not a directory", path.display()),
                    ));
                }
                None => return Err(not_found(path)),
            }
        }
        Ok(dir)
    }

    /// The directory id of `path`'s parent (which must exist) and `path`'s final name.
    fn split(&self, path: &Path) -> io::Result<(u64, String)> {
        let mut components = components(path)?;
        let Some(name) = components.pop() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the root directory has no parent",
            ));
        };
        let parent = self.dir_id(
            &components
                .iter()
                .fold(PathBuf::from("/"), |path, name| path.join(name)),
        )?;
        Ok((parent, name))
    }

    fn file_inode(&self, path: &Path) -> io::Result<u64> {
        let (parent, name) = self.split(path)?;
        match self.entry(parent, &name)? {
            Some(Node::File(ino)) => Ok(ino),
            Some(Node::Dir(_)) => Err(io::Error::new(
                io::ErrorKind::IsADirectory,
                format!("'{}' is a directory", path.display()),
            )),
            None => Err(not_found(path)),
        }
    }

    fn apply_crash(&mut self) -> CrashReport {
        let mut report = CrashReport {
            triggered_at: self.triggered_at,
            ..CrashReport::default()
        };

        // Rebuild the namespace from the durable entry sets reachable from the root, each plus
        // the prefix of its unsynced changes that reached the journal.
        let keep_changes = self.plan.tear != TearMode::DropUnsynced;
        let mut unsynced_changes = std::mem::take(&mut self.unsynced_changes);
        let mut recovered_dirs = BTreeMap::new();
        let mut reachable_files = BTreeMap::<u64, PathBuf>::new();
        let mut pending = vec![(ROOT_DIR, PathBuf::from("/"))];
        while let Some((dir, path)) = pending.pop() {
            if recovered_dirs.contains_key(&dir) {
                continue;
            }
            let mut entries = self.durable_dirs.get(&dir).cloned().unwrap_or_default();
            let changes = unsynced_changes.remove(&dir).unwrap_or_default();
            let persisted = if keep_changes {
                self.rng.cut(changes.len())
            } else {
                0
            };
            for change in &changes[..persisted] {
                apply_change(&mut entries, change);
            }
            if self.dirs.get(&dir) != Some(&entries) {
                report.reverted_dirs.push(path.clone());
            }
            for (name, node) in &entries {
                match *node {
                    Node::Dir(child) => pending.push((child, path.join(name))),
                    Node::File(ino) => {
                        reachable_files
                            .entry(ino)
                            .or_insert_with(|| path.join(name));
                    }
                }
            }
            recovered_dirs.insert(dir, entries);
        }
        report.reverted_dirs.sort();

        // Tear every surviving file per the plan; unreachable inodes are gone.
        let tear = self.plan.tear;
        let mut inodes = std::mem::take(&mut self.inodes);
        for (ino, path) in reachable_files {
            let Some(inode) = inodes.remove(&ino) else {
                continue;
            };
            let recovered = if inode.is_clean() {
                inode.current
            } else {
                let recovered = self.rng.recover_contents(&inode, tear);
                if recovered != inode.current {
                    report.files.push(TornFile {
                        path,
                        durable_len: inode.durable.len() as u64,
                        current_len: inode.current.len() as u64,
                        recovered_len: recovered.len() as u64,
                    });
                }
                recovered
            };
            self.inodes.insert(ino, Inode::with_content(recovered));
        }

        self.dirs = recovered_dirs.clone();
        self.durable_dirs = recovered_dirs;
        self.locks.clear();
        self.plan = FaultPlan::default();
        self.crashed = false;
        self.triggered_at = None;
        self.boot += 1;
        self.mutating_ops = 0;
        self.file_syncs = 0;
        self.dir_syncs = 0;
        self.appended_bytes = 0;
        self.points_hit.clear();
        report
    }
}

fn components(path: &Path) -> io::Result<Vec<String>> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("FaultVfs paths must be absolute: '{}'", path.display()),
        ));
    }
    let mut names = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => names.push(name.to_string_lossy().into_owned()),
            Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("FaultVfs paths must not contain '..': '{}'", path.display()),
                ));
            }
        }
    }
    Ok(names)
}

fn not_found(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("'{}' does not exist", path.display()),
    )
}

/// SplitMix64: a tiny, well-distributed, seedable generator. Deterministic across platforms and
/// dependency upgrades, which keeps recorded failing seeds reproducible.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    /// Uniform-enough value in `0..=max`.
    fn up_to(&mut self, max: usize) -> usize {
        let bound = max as u64 + 1;
        (self.next_u64() % bound) as usize
    }

    fn coin(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    /// A cut point in `0..=max`: how much of something unsynced survives. Half the time it is an
    /// extreme (none or all), the outcomes most likely to expose a missing sync.
    fn cut(&mut self, max: usize) -> usize {
        match self.next_u64() % 4 {
            0 => 0,
            1 => max,
            _ => self.up_to(max),
        }
    }

    /// Content of `inode` after a crash under `tear`.
    fn recover_contents(&mut self, inode: &Inode, tear: TearMode) -> Vec<u8> {
        if tear == TearMode::DropUnsynced {
            return inode.durable.clone();
        }
        let stable = inode.stable_len.min(inode.durable.len());
        // An unsynced truncation may not have reached the disk at all.
        if stable < inode.durable.len() && self.coin() {
            return inode.durable.clone();
        }
        let mut recovered = inode.durable[..stable].to_vec();
        let suffix = inode.current.get(stable..).unwrap_or_default();
        match tear {
            TearMode::DropUnsynced => {}
            TearMode::KeepRandomPrefix => {
                let keep = self.cut(suffix.len());
                recovered.extend_from_slice(&suffix[..keep]);
            }
            TearMode::TornGarbage => {
                let keep = self.cut(suffix.len());
                recovered.extend_from_slice(&suffix[..keep]);
                if keep > 0 {
                    let end = stable + keep;
                    let sector_start = ((end - 1) / SECTOR_BYTES * SECTOR_BYTES).max(stable);
                    for byte in &mut recovered[sector_start..end] {
                        *byte = (self.next_u64() & 0xFF) as u8;
                    }
                }
            }
            TearMode::ReorderedPages => {
                let total = inode.current.len();
                let mut pages = Vec::new();
                let mut page_start = stable;
                while page_start < total {
                    let page_end = ((page_start / PAGE_BYTES + 1) * PAGE_BYTES).min(total);
                    if self.coin() {
                        pages.push((page_start, page_end));
                    }
                    page_start = page_end;
                }
                let recovered_len = pages.last().map_or(stable, |&(_, end)| end);
                recovered.resize(recovered_len, 0);
                for (start, end) in pages {
                    recovered[start..end].copy_from_slice(&inode.current[start..end]);
                }
            }
        }
        recovered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{exists, is_crashed, read_file};

    fn write_synced(vfs: &dyn Vfs, path: &Path, bytes: &[u8]) {
        let file = vfs
            .open(path, OpenMode::CreateNew)
            .expect("file should be created");
        file.append(&[io::IoSlice::new(bytes)])
            .expect("append should succeed");
        file.sync_all().expect("sync should succeed");
    }

    fn names(vfs: &dyn Vfs, dir: &str) -> Vec<String> {
        let mut names = vfs
            .list(Path::new(dir))
            .expect("list should succeed")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn reads_see_unsynced_data_and_a_crash_drops_it() {
        let vfs = FaultVfs::new(1);
        vfs.create_dir_all(Path::new("/db")).expect("mkdir");
        vfs.sync_dir(Path::new("/")).expect("sync root");
        let path = Path::new("/db/wal");
        write_synced(vfs.as_ref(), path, b"synced");
        vfs.sync_dir(Path::new("/db")).expect("sync dir");

        let file = vfs.open(path, OpenMode::Append).expect("open");
        file.append(&[io::IoSlice::new(b"-unsynced")])
            .expect("append");
        assert_eq!(
            read_file(vfs.as_ref(), path).expect("read"),
            b"synced-unsynced"
        );

        let report = vfs.crash();
        assert_eq!(report.files.len(), 1);
        assert_eq!(read_file(vfs.as_ref(), path).expect("read"), b"synced");
    }

    #[test]
    fn namespace_changes_revert_unless_the_directory_was_synced() {
        let vfs = FaultVfs::new(2);
        vfs.create_dir_all(Path::new("/db")).expect("mkdir");
        vfs.sync_dir(Path::new("/")).expect("sync root");
        write_synced(vfs.as_ref(), Path::new("/db/old"), b"old");
        vfs.sync_dir(Path::new("/db")).expect("sync dir");

        write_synced(vfs.as_ref(), Path::new("/db/tmp"), b"new");
        vfs.rename(Path::new("/db/tmp"), Path::new("/db/renamed"))
            .expect("rename");
        vfs.remove_file(Path::new("/db/old")).expect("remove");
        write_synced(vfs.as_ref(), Path::new("/db/created"), b"created");
        assert_eq!(names(vfs.as_ref(), "/db"), vec!["created", "renamed"]);

        let report = vfs.crash();
        assert_eq!(report.reverted_dirs, vec![PathBuf::from("/db")]);
        assert_eq!(names(vfs.as_ref(), "/db"), vec!["old"]);
        assert_eq!(
            read_file(vfs.as_ref(), Path::new("/db/old")).expect("read"),
            b"old"
        );

        write_synced(vfs.as_ref(), Path::new("/db/tmp"), b"new");
        vfs.rename(Path::new("/db/tmp"), Path::new("/db/old"))
            .expect("rename over");
        vfs.sync_dir(Path::new("/db")).expect("sync dir");
        vfs.crash();
        assert_eq!(names(vfs.as_ref(), "/db"), vec!["old"]);
        assert_eq!(
            read_file(vfs.as_ref(), Path::new("/db/old")).expect("read"),
            b"new"
        );
    }

    #[test]
    fn unsynced_directory_changes_survive_as_an_ordered_prefix() {
        let prefixes: [&[&str]; 5] = [
            &["old"],
            &["a", "old"],
            &["b", "old"],
            &["b", "c", "old"],
            &["b", "c"],
        ];
        let mut seen = std::collections::BTreeSet::new();
        for seed in 0..64 {
            let vfs = FaultVfs::new(seed);
            vfs.create_dir_all(Path::new("/db")).expect("mkdir");
            vfs.sync_dir(Path::new("/")).expect("sync root");
            write_synced(vfs.as_ref(), Path::new("/db/old"), b"old");
            vfs.sync_dir(Path::new("/db")).expect("sync dir");

            write_synced(vfs.as_ref(), Path::new("/db/a"), b"a");
            vfs.rename(Path::new("/db/a"), Path::new("/db/b"))
                .expect("rename");
            write_synced(vfs.as_ref(), Path::new("/db/c"), b"c");
            vfs.remove_file(Path::new("/db/old")).expect("remove");
            vfs.set_plan(FaultPlan {
                tear: TearMode::KeepRandomPrefix,
                ..FaultPlan::default()
            });
            vfs.crash();

            let state = names(vfs.as_ref(), "/db");
            let state = state.iter().map(String::as_str).collect::<Vec<_>>();
            assert!(
                prefixes.contains(&state.as_slice()),
                "seed {seed}: {state:?} is not an ordered prefix of the changes"
            );
            seen.insert(state.join(","));
        }
        assert_eq!(
            seen.len(),
            prefixes.len(),
            "every prefix, including all changes, should be reachable: {seen:?}"
        );
    }

    #[test]
    fn unsynced_directories_come_back_empty_or_not_at_all() {
        let vfs = FaultVfs::new(3);
        vfs.create_dir_all(Path::new("/a/b")).expect("mkdir");
        vfs.sync_dir(Path::new("/")).expect("sync root");
        write_synced(vfs.as_ref(), Path::new("/a/b/file"), b"x");
        vfs.sync_dir(Path::new("/a/b")).expect("sync leaf");
        vfs.crash();
        assert!(exists(vfs.as_ref(), Path::new("/a")).expect("exists"));
        assert!(
            !exists(vfs.as_ref(), Path::new("/a/b")).expect("exists"),
            "/a was never synced, so its entry for b is lost"
        );
    }

    #[test]
    fn crash_after_ops_fails_the_kth_mutation_and_everything_after_it() {
        let vfs = FaultVfs::new(4);
        vfs.set_plan(FaultPlan {
            crash_after_ops: Some(2),
            ..FaultPlan::default()
        });
        let file = vfs
            .open(Path::new("/f"), OpenMode::CreateNew)
            .expect("op 0");
        file.append(&[io::IoSlice::new(b"a")]).expect("op 1");
        let error = file.sync_all().expect_err("op 2 crashes");
        assert!(is_crashed(&error));
        assert!(vfs.is_crashed());
        assert!(is_crashed(
            &vfs.list(Path::new("/")).expect_err("reads fail too")
        ));
        assert_eq!(vfs.mutating_ops(), 2);

        vfs.crash();
        assert!(!exists(vfs.as_ref(), Path::new("/f")).expect("exists"));
        assert!(
            is_crashed(&file.len().expect_err("handles die with their boot")),
            "files opened before the crash stay dead"
        );
    }

    #[test]
    fn crash_at_a_named_point_and_process_handles_die_with_their_boot() {
        let vfs = FaultVfs::new(5);
        let process = vfs.process();
        vfs.set_plan(FaultPlan {
            crash_at: Some(CrashPoint::WalAfterSync),
            ..FaultPlan::default()
        });
        process
            .crash_point(CrashPoint::WalAfterAppend)
            .expect("other points pass");
        let error = process
            .crash_point(CrashPoint::WalAfterSync)
            .expect_err("the planned point crashes");
        assert!(is_crashed(&error));
        assert_eq!(
            vfs.crash_points_hit(),
            vec![CrashPoint::WalAfterAppend, CrashPoint::WalAfterSync]
        );

        let report = vfs.crash();
        assert_eq!(report.triggered_at, Some(CrashPoint::WalAfterSync));
        assert!(is_crashed(
            &process
                .list(Path::new("/"))
                .expect_err("the crashed process stays dead")
        ));
        let rebooted = vfs.process();
        rebooted.list(Path::new("/")).expect("a new process works");
    }

    #[test]
    fn tear_modes_keep_a_prefix_or_pages_of_the_unsynced_suffix() {
        for tear in TearMode::ALL {
            for seed in 0..32 {
                let vfs = FaultVfs::new(seed);
                let path = Path::new("/f");
                write_synced(vfs.as_ref(), path, b"durable");
                vfs.sync_dir(Path::new("/")).expect("sync root");
                let unsynced = (0..3 * PAGE_BYTES)
                    .map(|index| (index % 251) as u8 + 1)
                    .collect::<Vec<u8>>();
                vfs.open(path, OpenMode::Append)
                    .expect("open")
                    .append(&[io::IoSlice::new(&unsynced)])
                    .expect("append");
                vfs.set_plan(FaultPlan {
                    tear,
                    ..FaultPlan::default()
                });
                vfs.crash();

                let recovered = read_file(vfs.as_ref(), path).expect("read");
                assert!(recovered.starts_with(b"durable"), "{tear:?} seed {seed}");
                assert!(
                    recovered.len() <= 7 + unsynced.len(),
                    "{tear:?} seed {seed}"
                );
                let tail = &recovered[7..];
                match tear {
                    TearMode::DropUnsynced => assert!(tail.is_empty()),
                    TearMode::KeepRandomPrefix => assert_eq!(tail, &unsynced[..tail.len()]),
                    TearMode::TornGarbage => {
                        let intact = tail.len().saturating_sub(SECTOR_BYTES);
                        assert_eq!(&tail[..intact], &unsynced[..intact], "seed {seed}");
                    }
                    TearMode::ReorderedPages => {
                        for (index, byte) in tail.iter().enumerate() {
                            assert!(*byte == 0 || *byte == unsynced[index], "seed {seed}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn failed_sync_poisons_the_unsynced_suffix_for_later_syncs() {
        let vfs = FaultVfs::new(6);
        let path = Path::new("/f");
        write_synced(vfs.as_ref(), path, b"aaaa");
        vfs.sync_dir(Path::new("/")).expect("sync root");
        vfs.set_plan(FaultPlan {
            fail_sync: Some(1),
            ..FaultPlan::default()
        });
        let file = vfs.open(path, OpenMode::Append).expect("open");
        file.append(&[io::IoSlice::new(b"bbbb")]).expect("append");
        let error = file.sync_data().expect_err("first sync fails");
        assert_eq!(error.raw_os_error(), Some(EIO));
        file.append(&[io::IoSlice::new(b"cccc")]).expect("append");
        file.sync_data().expect("second sync succeeds");
        assert_eq!(
            read_file(vfs.as_ref(), path).expect("read"),
            b"aaaabbbbcccc"
        );

        vfs.crash();
        assert_eq!(
            read_file(vfs.as_ref(), path).expect("read"),
            b"aaaa\0\0\0\0cccc",
            "the bytes whose writeback failed are not made durable by a later sync"
        );
    }

    #[test]
    fn rollback_after_failed_sync_makes_the_file_consistent_again() {
        for tear in TearMode::ALL {
            for seed in 0..16 {
                let vfs = FaultVfs::new(seed);
                let path = Path::new("/f");
                write_synced(vfs.as_ref(), path, b"aaaa");
                vfs.sync_dir(Path::new("/")).expect("sync root");
                vfs.set_plan(FaultPlan {
                    fail_sync: Some(1),
                    tear,
                    ..FaultPlan::default()
                });
                let file = vfs.open(path, OpenMode::Append).expect("open");
                file.append(&[io::IoSlice::new(b"bbbb")]).expect("append");
                file.sync_data().expect_err("sync fails");
                file.set_len(4).expect("roll back");
                file.sync_all().expect("sync rollback");
                file.append(&[io::IoSlice::new(b"cccc")]).expect("append");
                file.sync_data().expect("sync");
                vfs.crash();
                assert_eq!(
                    read_file(vfs.as_ref(), path).expect("read"),
                    b"aaaacccc",
                    "{tear:?} seed {seed}"
                );
            }
        }
    }

    #[test]
    fn a_failed_sync_may_have_written_its_data_so_an_unsynced_rollback_can_lose() {
        let mut resurrected = false;
        for seed in 0..32 {
            let vfs = FaultVfs::new(seed);
            let path = Path::new("/f");
            write_synced(vfs.as_ref(), path, b"aaaa");
            vfs.sync_dir(Path::new("/")).expect("sync root");
            vfs.set_plan(FaultPlan {
                fail_sync: Some(1),
                tear: TearMode::KeepRandomPrefix,
                ..FaultPlan::default()
            });
            let file = vfs.open(path, OpenMode::Append).expect("open");
            file.append(&[io::IoSlice::new(b"bbbb")]).expect("append");
            file.sync_data().expect_err("sync fails");
            file.set_len(4).expect("roll back without a sync");
            vfs.crash();

            let recovered = read_file(vfs.as_ref(), path).expect("read");
            assert!(
                b"aaaabbbb".starts_with(&recovered) && recovered.len() >= 4,
                "seed {seed}: {recovered:?}"
            );
            resurrected |= recovered == b"aaaabbbb";
        }
        assert!(
            resurrected,
            "data whose sync failed must be able to reappear when the rollback is not synced"
        );
    }

    #[test]
    fn unsynced_truncation_is_dropped_by_drop_unsynced() {
        let vfs = FaultVfs::new(8);
        let path = Path::new("/f");
        write_synced(vfs.as_ref(), path, b"checkpointed");
        vfs.sync_dir(Path::new("/")).expect("sync root");
        vfs.open(path, OpenMode::Append)
            .expect("open")
            .set_len(0)
            .expect("truncate");
        vfs.crash();
        assert_eq!(
            read_file(vfs.as_ref(), path).expect("read"),
            b"checkpointed"
        );
    }

    #[test]
    fn enospc_rejects_appends_past_the_limit() {
        let vfs = FaultVfs::new(9);
        vfs.set_plan(FaultPlan {
            enospc_after_bytes: Some(4),
            ..FaultPlan::default()
        });
        let file = vfs
            .open(Path::new("/f"), OpenMode::CreateNew)
            .expect("create");
        file.append(&[io::IoSlice::new(b"abc")]).expect("fits");
        let error = file
            .append(&[io::IoSlice::new(b"de")])
            .expect_err("exceeds");
        assert_eq!(error.raw_os_error(), Some(ENOSPC));
        assert_eq!(file.len().expect("len"), 3);
    }

    #[test]
    fn locks_are_exclusive_and_cleared_by_a_crash() {
        let vfs = FaultVfs::new(10);
        let lock = vfs
            .try_lock_exclusive(Path::new("/LOCK"))
            .expect("first lock");
        let error = vfs
            .try_lock_exclusive(Path::new("/LOCK"))
            .err()
            .expect("second lock fails");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        vfs.crash();
        let relocked = vfs
            .try_lock_exclusive(Path::new("/LOCK"))
            .expect("a crash releases every lock");
        drop(lock);
        assert!(
            vfs.try_lock_exclusive(Path::new("/LOCK")).is_err(),
            "dropping a stale guard must not release the new holder's lock"
        );
        drop(relocked);
        drop(
            vfs.try_lock_exclusive(Path::new("/LOCK"))
                .expect("released"),
        );
    }

    #[test]
    fn corrupt_overwrites_live_and_durable_bytes() {
        let vfs = FaultVfs::new(11);
        let path = Path::new("/f");
        write_synced(vfs.as_ref(), path, b"hello");
        vfs.sync_dir(Path::new("/")).expect("sync root");
        vfs.corrupt(path, 1, b"EY").expect("corrupt");
        assert_eq!(read_file(vfs.as_ref(), path).expect("read"), b"hEYlo");
        vfs.crash();
        assert_eq!(read_file(vfs.as_ref(), path).expect("read"), b"hEYlo");
    }

    #[test]
    fn same_seed_same_outcome() {
        let run = |seed| {
            let vfs = FaultVfs::new(seed);
            let path = Path::new("/f");
            write_synced(vfs.as_ref(), path, b"base");
            vfs.sync_dir(Path::new("/")).expect("sync root");
            vfs.open(path, OpenMode::Append)
                .expect("open")
                .append(&[io::IoSlice::new(&[7u8; 10_000])])
                .expect("append");
            vfs.set_plan(FaultPlan {
                tear: TearMode::TornGarbage,
                ..FaultPlan::default()
            });
            vfs.crash();
            read_file(vfs.as_ref(), path).expect("read")
        };
        assert_eq!(run(42), run(42));
    }
}
