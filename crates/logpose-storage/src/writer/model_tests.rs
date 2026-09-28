//! Model-based randomized test of row visibility across memtables, deletion vectors, the
//! primary-key index, flush, compaction, and recovery.
//!
//! Each seed runs random write batches (upserts, partial updates, and deletes over a small key
//! pool) interleaved with flush and compaction jobs whose begin, build, and commit are driven
//! step by step, so writes land between a job's begin and its commit (on the frozen memtable,
//! on compaction inputs). Jobs sometimes end without committing, which leaves a memtable frozen
//! for the next flush, and sometimes crash at a random operation of their build (segment and DV
//! file writes) or commit. Crashes and clean reopens rebuild the primary-key index from
//! segments, DV files, and the WAL. A second mode runs writes, crashes, and reopens with tiny
//! flush and compaction thresholds, so the engine's own background jobs race the writes.
//!
//! After every step the published version must satisfy its invariants (one live row per key
//! among them), its live rows must equal the model, and a point lookup of every key (newest
//! unit first, as readers do) must agree, both on the version and through a read view (whose
//! live counts must also sum to the model's size). Partial updates and deletes resolve keys through the
//! primary-key index, so a wrong index entry shows up as a wrong merged row, a wrong
//! `NotFound`, or two live rows.
//!
//! `LOGPOSE_MODEL_SEEDS` sets the number of seeds and `LOGPOSE_MODEL_FIRST_SEED` the first;
//! `LOGPOSE_MODEL_VERBOSE` prints each background seed as it starts.

use super::*;
use crate::{
    CreateCollectionRequest, Engine, EngineConfig,
    legacy_view::legacy_put,
    read::{Projection, ReadOptions},
};
use logpose_types::{
    CollectionRef, DistanceMetric, ResourceKind,
    record::{PartialUpdate, PrimaryKey, Record},
};
use logpose_vfs::{FaultPlan, FaultVfs, TearMode};
use logpose_wal::BootId;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const ROOT: &str = "/storage";
const NAME: &str = "model";
const KEYS: usize = 10;

type Row = (Vec<f32>, Value);
type Model = BTreeMap<String, Row>;

fn config() -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new("boot")),
        strict_invariants: true,
        ..EngineConfig::default()
    }
}

fn open(fault: &Arc<FaultVfs>) -> Engine {
    Engine::open(fault.process(), ROOT, config()).expect("engine should open")
}

fn create(engine: &Engine, background: bool) -> Arc<CollectionHandle> {
    let mut descriptor = engine
        .core()
        .plan_collection_descriptor(&CreateCollectionRequest::new(NAME, 2, DistanceMetric::Dot))
        .expect("descriptor should plan");
    if background {
        descriptor.flush_threshold_ops = 3;
        descriptor.flush_threshold_bytes = 4096;
        descriptor.compaction_threshold_segments = 2;
    } else {
        descriptor.flush_threshold_ops = usize::MAX;
        descriptor.flush_threshold_bytes = usize::MAX;
        descriptor.compaction_threshold_segments = usize::MAX;
    }
    engine
        .create_collection(descriptor, None)
        .expect("collection should be created")
}

fn reopen(engine: &Engine) -> Arc<CollectionHandle> {
    engine
        .collection(&CollectionRef::new_default(NAME))
        .expect("collection should reopen")
}

/// The live rows of the view the read path serves: point lookups of every key and the sum of
/// the units' live counts, which the caller compares with the model.
fn read_path(
    engine: &Engine,
    handle: &CollectionHandle,
    model: &Model,
) -> std::result::Result<(), String> {
    let view = engine
        .core()
        .read_view_of(handle, &ReadOptions::default())
        .map_err(|error| error.to_string())?;
    let live = view
        .units()
        .iter()
        .map(|unit| u64::from(unit.live_count()))
        .sum::<u64>();
    if live != model.len() as u64 {
        return Err(format!(
            "the read view counts {live} live rows, the model {}",
            model.len()
        ));
    }
    let keys = (0..KEYS)
        .map(|key| PrimaryKey::from(format!("k{key}").as_str()))
        .collect::<Vec<_>>();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|error| error.to_string())?;
    let rows = runtime
        .block_on(view.get(&keys, Projection::full()))
        .map_err(|error| error.to_string())?;
    for (key, row) in (0..KEYS).zip(rows) {
        let id = format!("k{key}");
        let expected = model.get(&id).map(|(vector, _)| vector);
        let found = row
            .as_ref()
            .and_then(|row| row.record.vectors.values().next());
        if found != expected {
            return Err(format!(
                "read view get({id}) = {:?}, but the model holds {expected:?}",
                row.map(|row| row.record)
            ));
        }
    }
    Ok(())
}

fn live(handle: &CollectionHandle) -> std::result::Result<Model, String> {
    let version = handle.current();
    version
        .check_invariants()
        .map_err(|error| error.to_string())?;
    let mut rows = Model::new();
    for (_, image) in version.live_images() {
        let put = legacy_put(&version.schema, &image).map_err(|error| error.to_string())?;
        if rows
            .insert(put.id.as_str().to_owned(), (put.vector, put.metadata))
            .is_some()
        {
            return Err(format!("two live rows for {}", put.id.as_str()));
        }
    }
    // Point lookups probe each unit newest first, as readers do, never through the writer's
    // index.
    for key in 0..KEYS {
        let id = format!("k{key}");
        let found = match version.get(&PrimaryKey::from(id.as_str())) {
            Some((_, image)) => {
                let put = legacy_put(&version.schema, &image).map_err(|error| error.to_string())?;
                Some((put.vector, put.metadata))
            }
            None => None,
        };
        if found.as_ref() != rows.get(&id) {
            return Err(format!(
                "get({id}) = {found:?}, but the live rows hold {:?}",
                rows.get(&id)
            ));
        }
    }
    Ok(rows)
}

/// A random batch over distinct keys, and the model after it, or `None` when the batch must
/// fail (an update of a key with no live row).
fn random_batch(
    rng: &mut StdRng,
    model: &Model,
    counter: &mut u64,
) -> (Vec<ClientOp>, Option<Model>) {
    let size = rng.random_range(1..=4);
    let mut keys = Vec::new();
    while keys.len() < size {
        let key = format!("k{}", rng.random_range(0..KEYS));
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    let mut next = model.clone();
    let mut fails = false;
    let mut ops = Vec::new();
    for key in keys {
        *counter += 1;
        let n = *counter;
        match rng.random_range(0..10) {
            0..=4 => {
                let x = n as f32;
                let mut record = Record::new(key.as_str()).with_vector("vector", vec![x, 1.0]);
                let mut extra = serde_json::Map::new();
                extra.insert("v".to_owned(), json!(n));
                if rng.random_bool(0.5) {
                    extra.insert("tag".to_owned(), json!(format!("t{}", n % 3)));
                }
                record.extra = extra.clone();
                next.insert(key.clone(), (vec![x, 1.0], Value::Object(extra)));
                ops.push(ClientOp::Upsert(record));
            }
            5..=7 => {
                let mut update = PartialUpdate::new(key.as_str());
                let set_vector = rng.random_bool(0.3);
                let drop_tag = rng.random_bool(0.3);
                update.extra.insert("u".to_owned(), json!(n));
                if drop_tag {
                    update.extra.insert("tag".to_owned(), Value::Null);
                }
                if set_vector {
                    update
                        .vectors
                        .insert("vector".to_owned(), vec![0.5, n as f32]);
                }
                match next.get_mut(&key) {
                    Some((vector, Value::Object(extra))) => {
                        extra.insert("u".to_owned(), json!(n));
                        if drop_tag {
                            extra.remove("tag");
                        }
                        if set_vector {
                            *vector = vec![0.5, n as f32];
                        }
                    }
                    Some(_) => unreachable!("rows hold objects"),
                    None => fails = true,
                }
                ops.push(ClientOp::Update(update));
            }
            _ => {
                next.remove(&key);
                ops.push(ClientOp::Delete(PrimaryKey::from(key.as_str())));
            }
        }
    }
    (ops, (!fails).then_some(next))
}

struct Run {
    seed: u64,
    rng: StdRng,
    fault: Arc<FaultVfs>,
    engine: Option<Engine>,
    handle: Option<Arc<CollectionHandle>>,
    model: Model,
    counter: u64,
    trace: Vec<String>,
    background: bool,
}

impl Run {
    fn new(seed: u64, background: bool) -> Self {
        let fault = FaultVfs::new(seed);
        let engine = open(&fault);
        let handle = create(&engine, background);
        Self {
            seed,
            rng: StdRng::seed_from_u64(seed),
            fault,
            engine: Some(engine),
            handle: Some(handle),
            model: Model::new(),
            counter: 0,
            trace: Vec::new(),
            background,
        }
    }

    fn handle(&self) -> Arc<CollectionHandle> {
        Arc::clone(self.handle.as_ref().expect("open"))
    }

    #[allow(clippy::panic, reason = "a failed check reports the seed and trace")]
    fn fail(&self, message: String) -> ! {
        panic!(
            "seed {} (background {}): {message}\ntrace:\n{}",
            self.seed,
            self.background,
            self.trace.join("\n")
        );
    }

    fn check(&self, what: &str) {
        match live(&self.handle()) {
            Ok(rows) if rows == self.model => {
                let engine = self.engine.as_ref().expect("open");
                if let Err(error) = read_path(engine, &self.handle(), &self.model) {
                    self.fail(format!("{what}: {error}"));
                }
            }
            Ok(rows) => self.fail(format!(
                "{what}: live rows differ\nexpected {:?}\nactual   {:?}",
                self.model, rows
            )),
            Err(error) => self.fail(format!("{what}: {error}")),
        }
    }

    fn write(&mut self) {
        let (ops, next) = random_batch(&mut self.rng, &self.model, &mut self.counter);
        self.trace.push(format!("write {ops:?}"));
        let result = self.handle().write_blocking(ops);
        match (result, next) {
            (Ok(_), Some(next)) => self.model = next,
            (
                Err(LogPoseError::NotFound {
                    resource: ResourceKind::Record,
                    ..
                }),
                None,
            ) => {}
            (result, next) => self.fail(format!(
                "write result {result:?} but the model expected success = {}",
                next.is_some()
            )),
        }
        self.check("after write");
    }

    /// Writes while a job is between its begin and its commit.
    fn writes_during_job(&mut self) {
        for _ in 0..self.rng.random_range(0..5) {
            self.write();
        }
    }

    fn job(&mut self, kind: JobKind) {
        let handle = self.handle();
        let core = self.engine.as_ref().expect("open").core();
        // 0: end without committing (a flush leaves its memtable frozen); 1: crash at a random
        // operation of the commit; 2: crash at a random operation of the build (the segment
        // and DV file writes) or of the commit; otherwise commit.
        let ending = self.rng.random_range(0..10);
        self.trace.push(format!("job {kind:?} ending {ending}"));
        let (mut ticket, start) = match handle.begin_job(kind) {
            Ok(begun) => begun,
            Err(error) => self.fail(format!("begin {kind:?}: {error}")),
        };
        self.check("after begin");
        self.writes_during_job();
        if ending == 2 && !matches!(start.work, JobWork::Nothing) {
            self.arm_crash(40);
        }
        let built = match &start.work {
            JobWork::Nothing => None,
            JobWork::Flush(work) => {
                Some(core.build_flush(&handle, &start.version, start.unit, work, &mut ticket))
            }
            JobWork::Compact(work) => {
                Some(core.build_compaction(&handle, &start.version, start.unit, work, &mut ticket))
            }
        };
        drop(start);
        let commit = match built {
            None => None,
            Some(Ok(commit)) => Some(commit),
            Some(Err(_)) if ending == 2 => {
                // The build crashed.
                drop(ticket);
                drop((handle, core));
                self.crash_and_reopen();
                return;
            }
            Some(Err(error)) => self.fail(format!("build {kind:?}: {error}")),
        };
        if ending != 2 {
            self.writes_during_job();
        }
        match (commit, ending) {
            (None, _) => drop(ticket),
            (Some(_), 0) => drop(ticket),
            (Some(commit), 1 | 2) => {
                if ending == 1 {
                    self.arm_crash(12);
                }
                let _ = ticket.commit(commit);
                drop((handle, core));
                self.crash_and_reopen();
                return;
            }
            (Some(commit), _) => {
                if let Err(error) = ticket.commit(commit) {
                    self.fail(format!("commit {kind:?}: {error}"));
                }
            }
        }
        self.check("after job");
    }

    /// Crash before one of the next `within` mutating operations, with a random tear mode.
    fn arm_crash(&mut self, within: u64) {
        let k = self.rng.random_range(0..within);
        let tear = TearMode::ALL[self.rng.random_range(0..TearMode::ALL.len())];
        self.trace.push(format!("crash after {k} ops, {tear:?}"));
        self.fault.set_plan(FaultPlan {
            crash_after_ops: Some(self.fault.mutating_ops() + k),
            tear,
            ..FaultPlan::default()
        });
    }

    fn crash_and_reopen(&mut self) {
        self.handle = None;
        if let Some(engine) = self.engine.take() {
            drop(engine);
        }
        self.fault.crash();
        self.fault.set_plan(FaultPlan::default());
        let engine = open(&self.fault);
        self.handle = Some(reopen(&engine));
        self.engine = Some(engine);
        self.check("after recovery");
    }

    fn step(&mut self) {
        match self.rng.random_range(0..100) {
            0..=49 => self.write(),
            50..=64 => self.job(JobKind::Flush),
            65..=77 => self.job(JobKind::Compact),
            78..=84 => {
                let core = self.engine.as_ref().expect("open").core();
                self.trace.push("flush_collection".to_owned());
                if let Err(error) = core.flush_collection(&self.handle()) {
                    self.fail(format!("flush: {error}"));
                }
                self.check("after flush");
            }
            85..=92 => {
                self.trace.push("crash".to_owned());
                let tear = TearMode::ALL[self.rng.random_range(0..TearMode::ALL.len())];
                self.fault.set_plan(FaultPlan {
                    tear,
                    ..FaultPlan::default()
                });
                self.crash_and_reopen();
            }
            _ => {
                self.trace.push("reopen".to_owned());
                self.handle = None;
                self.engine = None;
                let engine = open(&self.fault);
                self.handle = Some(reopen(&engine));
                self.engine = Some(engine);
                self.check("after reopen");
            }
        }
    }

    /// Background mode: writes with tiny thresholds while the engine's own jobs run.
    fn background_step(&mut self) {
        match self.rng.random_range(0..100) {
            0..=89 => self.write(),
            90..=95 => {
                self.trace.push("crash".to_owned());
                self.crash_and_reopen();
            }
            _ => {
                self.trace.push("reopen".to_owned());
                self.handle = None;
                self.engine = None;
                let engine = open(&self.fault);
                self.handle = Some(reopen(&engine));
                self.engine = Some(engine);
                self.check("after reopen");
            }
        }
    }
}

fn seeds(default: u64) -> std::ops::Range<u64> {
    let count = std::env::var("LOGPOSE_MODEL_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default);
    let first = std::env::var("LOGPOSE_MODEL_FIRST_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    first..first + count
}

#[test]
fn random_writes_jobs_and_crashes_keep_exactly_the_model_rows_live() {
    for seed in seeds(24) {
        let mut run = Run::new(seed, false);
        for _ in 0..60 {
            run.step();
        }
    }
}

#[test]
fn random_writes_with_tiny_thresholds_keep_exactly_the_model_rows_live() {
    for seed in seeds(8) {
        if std::env::var_os("LOGPOSE_MODEL_VERBOSE").is_some() {
            eprintln!("background seed {}", 10_000 + seed);
        }
        let mut run = Run::new(10_000 + seed, true);
        for _ in 0..80 {
            run.background_step();
        }
    }
}
