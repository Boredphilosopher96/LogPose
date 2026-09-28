//! One engine and one collection under test: the storage backend, the engine configuration the
//! harness runs with, opening, closing, crashing, and a small runtime for the query crate's
//! async reads.

use logpose_storage::{
    CollectionHandle, CompactionConfig, CreateCollectionRequest, Engine, EngineConfig,
    GroupCommitConfig, IndexPolicy, ManualClock, MemtableConfig, ReadOptions, ReadView,
    RuntimeConfig, SchemaChange, TokenConfig,
};
use logpose_types::{
    CollectionRef, DistanceMetric,
    schema::{FieldType, ScalarFieldSpec},
};
use logpose_vfs::{FaultVfs, Vfs, std_vfs};
use logpose_wal::BootId;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

/// Storage root on `FaultVfs`.
pub const ROOT: &str = "/storage";
/// The collection's name.
pub const NAME: &str = "harness";
/// Vector dimensions.
pub const DIMS: usize = 4;
/// How long a blocking engine call may take before the run fails as hung.
pub const CALL_DEADLINE: Duration = Duration::from_secs(30);
/// Snapshot token time to live on the manual clock.
pub const TTL: Duration = Duration::from_secs(60);

/// Where the collection's files live.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    /// The in-memory `FaultVfs`, which crash and fault actions power-cycle.
    Fault,
    /// The real filesystem, in a temporary directory; no crash or fault actions.
    Std,
}

/// Who runs maintenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Maintenance {
    /// Background maintenance off: flushes and compactions run only when an action begins,
    /// builds, and commits a job by hand, or asks for one explicitly. The one exception is the
    /// engine's own: a frozen memtable that an abandoned or failed flush left behind is
    /// flushed by a background job right away, as it must be.
    Stepped,
    /// Tiny flush and compaction thresholds with the scheduler paused: background jobs start
    /// only when an action grants a permit (or a blocked call needs one).
    Paused,
    /// Tiny thresholds and a free-running scheduler: background jobs race every action.
    Free,
}

/// What a run is set up with.
#[derive(Clone, Copy, Debug)]
pub struct Setup {
    pub backend: Backend,
    pub maintenance: Maintenance,
    pub metric: DistanceMetric,
    /// Whether segments get SQ8 codes and graphs (searches are then checked for soundness and
    /// not for exactness).
    pub indexed: bool,
}

impl Setup {
    /// The setup of seed `seed` on `backend` with `maintenance`: the metric alternates, and
    /// every fourth pair of seeds writes index sections.
    pub fn for_seed(seed: u64, backend: Backend, maintenance: Maintenance) -> Self {
        Self {
            backend,
            maintenance,
            metric: if seed.is_multiple_of(2) {
                DistanceMetric::Dot
            } else {
                DistanceMetric::L2
            },
            indexed: (seed / 2) % 4 == 3,
        }
    }

    pub fn background(&self) -> bool {
        self.maintenance != Maintenance::Stepped
    }
}

/// An engine open on the harness's collection, and everything needed to reopen it.
pub struct Session {
    pub setup: Setup,
    pub fault: Option<Arc<FaultVfs>>,
    std_root: Option<PathBuf>,
    pub clock: Arc<ManualClock>,
    /// Calls of the engine's fatal handler (which must never abort the test process).
    pub fatal: Arc<AtomicUsize>,
    /// Group commit settings for later opens (the default when `None`).
    pub group: Option<GroupCommitConfig>,
    engine: Option<Engine>,
    handle: Option<Arc<CollectionHandle>>,
    runtime: tokio::runtime::Runtime,
    /// Flush and compaction permits granted by engines already closed.
    granted: (u64, u64),
}

impl Session {
    /// A fresh storage root with the harness collection created: string keys, one vector field,
    /// dynamic fields, and the typed fields `n` (int64) and `s` (string).
    pub fn create(setup: Setup, seed: u64) -> Result<Self, String> {
        let (fault, std_root) = match setup.backend {
            Backend::Fault => (Some(FaultVfs::new(seed)), None),
            Backend::Std => (None, Some(unique_temp_dir(seed))),
        };
        let mut session = Self::detached(setup, fault, std_root)?;
        let engine = session.open_engine()?;
        let mut descriptor = engine
            .plan_collection_descriptor(&CreateCollectionRequest::new(NAME, DIMS, setup.metric))
            .map_err(|error| error.to_string())?;
        if setup.background() {
            descriptor.flush_threshold_ops = 3;
            descriptor.flush_threshold_bytes = 64 * 1024;
            descriptor.compaction_threshold_segments = 2;
        } else {
            descriptor.flush_threshold_ops = usize::MAX;
            descriptor.flush_threshold_bytes = usize::MAX;
            descriptor.compaction_threshold_segments = usize::MAX;
        }
        let handle = engine
            .create_collection_blocking(descriptor, None)
            .map_err(|error| error.to_string())?;
        for (name, field_type) in [("n", FieldType::Int64), ("s", FieldType::String)] {
            handle
                .alter_schema_blocking(SchemaChange::AddField(ScalarFieldSpec::new(
                    name, field_type,
                )))
                .map_err(|error| error.to_string())?;
        }
        session.engine = Some(engine);
        session.handle = Some(handle);
        Ok(session)
    }

    fn detached(
        setup: Setup,
        fault: Option<Arc<FaultVfs>>,
        std_root: Option<PathBuf>,
    ) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            setup,
            fault,
            std_root,
            clock: Arc::new(ManualClock::new()),
            fatal: Arc::new(AtomicUsize::new(0)),
            group: None,
            engine: None,
            handle: None,
            runtime,
            granted: (0, 0),
        })
    }

    /// A closed session over an existing `FaultVfs` image (a fork of another session's disk),
    /// with the same setup and group commit settings; [`open`](Self::open) recovers it.
    pub fn attach(&self, fault: Arc<FaultVfs>) -> Result<Self, String> {
        let mut session = Self::detached(self.setup, Some(fault), None)?;
        session.group = self.group;
        Ok(session)
    }

    fn vfs(&self) -> Arc<dyn Vfs> {
        match &self.fault {
            Some(fault) => fault.process(),
            None => std_vfs(),
        }
    }

    fn root(&self) -> PathBuf {
        self.std_root.clone().unwrap_or_else(|| PathBuf::from(ROOT))
    }

    /// The engine configuration: small pools, strict invariants, the manual clock, tokens that
    /// expire only by their TTL, and a boot id per simulated boot.
    pub fn config(&self) -> EngineConfig {
        let fatal = Arc::clone(&self.fatal);
        let boot = self.fault.as_ref().map_or(0, |fault| fault.boot());
        EngineConfig {
            boot_id: Some(BootId::new(format!("boot-{boot}"))),
            strict_invariants: true,
            runtime: RuntimeConfig {
                io_threads: 2,
                query_threads: 1,
                // Free-running maintenance gets two job slots, so a collection's flush and
                // compactions (tiers merge independently) run at once, racing each other too.
                maintenance_threads: if self.setup.maintenance == Maintenance::Free {
                    2
                } else {
                    1
                },
                writer_threads: 1,
                ..RuntimeConfig::default()
            },
            memtable: MemtableConfig {
                max_age: Duration::from_secs(86_400 * 365),
                max_frozen: 3,
                write_stall_timeout: Duration::from_secs(86_400 * 365),
                ..MemtableConfig::default()
            },
            compaction: CompactionConfig {
                base_rows: 2,
                tier_ratio: 4,
                min_merge: 2,
                max_merge: 4,
                deleted_ratio: 0.3,
                ..CompactionConfig::default()
            },
            tokens: TokenConfig {
                ttl: TTL,
                memory_limit: Some(u64::MAX),
                ..TokenConfig::default()
            },
            clock: Some(self.clock.clone()),
            index: if self.setup.indexed {
                IndexPolicy {
                    graph_min_rows: 6,
                    sq8_min_rows: 3,
                    ..IndexPolicy::default()
                }
            } else {
                IndexPolicy {
                    graph_min_rows: u32::MAX,
                    sq8_min_rows: u32::MAX,
                    ..IndexPolicy::default()
                }
            },
            group: self.group.unwrap_or_default(),
            resolver: Some(logpose_query::resolver()),
            on_fatal: Some(Arc::new(move |_| {
                fatal.fetch_add(1, Ordering::Relaxed);
            })),
            ..EngineConfig::default()
        }
    }

    fn open_engine(&self) -> Result<Engine, String> {
        let engine = Engine::open(self.vfs(), self.root(), self.config())
            .map_err(|error| format!("engine open: {error}"))?;
        if self.setup.maintenance == Maintenance::Paused {
            engine.scheduler().pause();
        }
        Ok(engine)
    }

    /// Open the engine and look the collection up.
    pub fn open(&mut self) -> Result<(), String> {
        let engine = self.open_engine()?;
        let handle = engine
            .collection(&reference())
            .map_err(|error| format!("collection after open: {error}"))?;
        self.engine = Some(engine);
        self.handle = Some(handle);
        Ok(())
    }

    /// Open the engine and drop it again, whatever happens (a recovery that a planned crash
    /// interrupts).
    pub fn open_and_drop(&mut self) {
        if let Ok(engine) = self.open_engine() {
            let _ = engine.collection(&reference());
        }
    }

    /// Drop the collection handle and the engine (waiting for every engine task).
    pub fn close(&mut self) {
        if let Some(engine) = &self.engine {
            let stats = engine.scheduler().stats();
            self.granted.0 += stats.flushes_granted;
            self.granted.1 += stats.compactions_granted;
        }
        self.handle = None;
        self.engine = None;
    }

    /// Flush and compaction permits the scheduler granted over every engine this session
    /// opened.
    pub fn permits_granted(&self) -> (u64, u64) {
        let now = self.engine.as_ref().map_or((0, 0), |engine| {
            let stats = engine.scheduler().stats();
            (stats.flushes_granted, stats.compactions_granted)
        });
        (self.granted.0 + now.0, self.granted.1 + now.1)
    }

    /// The names of every segment file under the storage root.
    pub fn segment_files(&self) -> Result<std::collections::BTreeSet<String>, String> {
        let vfs = self.vfs();
        let mut names = std::collections::BTreeSet::new();
        let mut dirs = vec![self.root()];
        while let Some(dir) = dirs.pop() {
            let entries = vfs
                .list(&dir)
                .map_err(|error| format!("list {}: {error}", dir.display()))?;
            for entry in entries {
                if entry.is_dir {
                    dirs.push(dir.join(&entry.name));
                } else if entry.name.ends_with(".seg") {
                    names.insert(entry.name);
                }
            }
        }
        Ok(names)
    }

    pub fn is_open(&self) -> bool {
        self.engine.is_some()
    }

    pub fn engine(&self) -> &Engine {
        self.engine.as_ref().expect("the engine is open")
    }

    pub fn handle(&self) -> &Arc<CollectionHandle> {
        self.handle.as_ref().expect("the collection is open")
    }

    pub fn fault(&self) -> &Arc<FaultVfs> {
        self.fault.as_ref().expect("a FaultVfs backend")
    }

    /// The engine clock's time now.
    pub fn clock_now(&self) -> Duration {
        use logpose_storage::Clock as _;
        self.clock.now()
    }

    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    pub fn view(&self, options: &ReadOptions) -> logpose_types::Result<ReadView> {
        self.engine().read_view_blocking(&reference(), options)
    }

    /// Run a blocking engine call on its own thread and wait at most [`CALL_DEADLINE`] for it,
    /// so a call that never returns fails the run with its seed instead of hanging the test.
    /// With the scheduler paused, grant a waiting permit whenever the call has waited a little
    /// and no job runs, so calls that wait for maintenance (a write held by the stall, an
    /// explicit flush or compaction) finish.
    pub fn call<T: Send + 'static>(
        &self,
        what: &str,
        call: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, String> {
        let (done, finished) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _ = done.send(call());
        });
        let started = Instant::now();
        let paused = self.setup.maintenance == Maintenance::Paused;
        let mut nudges = 0_u32;
        loop {
            match finished.recv_timeout(Duration::from_millis(1)) {
                Ok(value) => return Ok(value),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(format!("{what} panicked"));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            if started.elapsed() > CALL_DEADLINE {
                return Err(format!("{what} did not return within {CALL_DEADLINE:?}"));
            }
            let scheduler = self.engine().scheduler();
            let stats = scheduler.stats();
            if paused
                && started.elapsed() > Duration::from_millis(5)
                && stats.waiting > 0
                && stats.running == 0
            {
                scheduler.step(1);
            }
            // Time passes on the manual clock only when the harness says so. A call held up by
            // a job's retry backoff (a write stalled behind flushes that failed) waits for it,
            // so let a second pass whenever the call has been idle for a while.
            let idle = started.elapsed().saturating_sub(Duration::from_millis(200));
            if stats.running == 0 && idle > Duration::from_millis(100) * nudges {
                self.clock.advance(Duration::from_secs(1));
                nudges += 1;
            }
        }
    }

    /// Grant one waiting permit (if any job waits) and wait until no job runs.
    pub fn step_scheduler(&self) -> Result<bool, String> {
        let scheduler = self.engine().scheduler();
        if scheduler.stats().waiting == 0 {
            return Ok(false);
        }
        scheduler.step(1);
        self.wait_for_jobs()?;
        Ok(true)
    }

    /// Wait until no maintenance job runs, or the planned crash has happened.
    pub fn wait_for_jobs(&self) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let crashed = self.fault.as_ref().is_some_and(|fault| fault.is_crashed());
            if crashed || self.engine().scheduler().stats().running == 0 {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err("timed out waiting for maintenance jobs".to_owned());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.close();
        if let Some(root) = &self.std_root {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}

pub fn reference() -> CollectionRef {
    CollectionRef::new_default(NAME)
}

fn unique_temp_dir(seed: u64) -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    std::env::temp_dir().join(format!(
        "logpose-harness-{seed}-{}-{unique}",
        std::process::id()
    ))
}
