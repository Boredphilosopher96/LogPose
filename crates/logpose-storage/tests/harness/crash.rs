//! Crash-recovery equivalence: exhaustive crash enumeration over the design's scenarios.
//!
//! Each scenario is a setup, run without faults, and a body. A clean run counts the body's
//! mutating operations `T`; then for every `k` in `0..=T` and every tear mode the body runs
//! again with the power lost before its `k`-th operation, and the recovered collection must be:
//!
//! - **I8**: exactly the model after some prefix of the body's requests that includes every
//!   acknowledged one and no partial one (sequence number, schema, every row);
//! - **I14**: at or past every state a concurrent reader observed before the crash, and every
//!   observed state must itself have been such a prefix;
//! - **I11**: the same as a clean recovery of the same disk image, when that recovery is itself
//!   crashed at any of its operations (in any tear mode) and rerun, or crashed again and again
//!   (each recovery one operation further than the last) before a clean one: rows, sequence
//!   number, schema, manifest generation, checkpoint, and every file's name and bytes;
//! - usable: it takes a write and a flush afterwards.
//!
//! Scenarios: one group commit of concurrent batches, a flush, a compaction with concurrent
//! deletes, a flush during a compaction (committing in either order), a checkpoint-only flush,
//! a schema change followed by a flush, a failed manifest publish followed by a retry, GC
//! after a pinned token is released, an index build beside writes, and an index build that a
//! compaction of its segment cancels (both over segments with SQ8 codes).
//!
//! `LOGPOSE_CRASH_I11_STRIDE` sets which crash states also check I11 (every `n`-th `k`;
//! default 3, `1` checks all); `LOGPOSE_CRASH_I11_TEARS=all` crashes each recovery in every tear
//! mode instead of one per state.

use crate::{
    actions::{Checker, Runner},
    model::Model,
    session::{Backend, Maintenance, ROOT, Session, Setup, reference},
};
use logpose_query::{ScrollOrder, scroll_view};
use logpose_storage::{
    GroupCommitConfig, JobKind, Projection, ReadOptions, SchemaChange, SnapshotToken, SteppedJob,
};
use logpose_types::{
    DistanceMetric, SeqNo,
    record::{ClientOp, PrimaryKey, Record},
    schema::{FieldType, ScalarFieldSpec},
    value::Value,
};
use logpose_vfs::{FaultPlan, FaultVfs, TearMode, Vfs};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn setup(indexed: bool) -> Setup {
    Setup {
        backend: Backend::Fault,
        maintenance: Maintenance::Stepped,
        metric: DistanceMetric::Dot,
        indexed,
    }
}

pub fn key(index: u64) -> PrimaryKey {
    PrimaryKey::from(format!("k{index:02}").as_str())
}

/// What ends a scenario body early: in a clean run a failure, after a planned crash the
/// expected end.
type Step = Result<(), String>;

/// A reader thread that records every state it sees: the visible sequence number and the rows.
struct Observer {
    stop: Arc<AtomicBool>,
    seen: Arc<Mutex<Vec<(SeqNo, Vec<Record>)>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Observer {
    fn start(session: &Session) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let engine = session.engine().clone();
        let thread = {
            let (stop, seen) = (Arc::clone(&stop), Arc::clone(&seen));
            std::thread::spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread().build() else {
                    return;
                };
                let collection = reference();
                while !stop.load(Ordering::Relaxed) {
                    let Ok(view) = engine.read_view_blocking(&collection, &ReadOptions::default())
                    else {
                        break;
                    };
                    let seq = view.visible_seq_no();
                    let known = seen
                        .lock()
                        .map(
                            |seen: std::sync::MutexGuard<'_, Vec<(SeqNo, Vec<Record>)>>| {
                                seen.last().is_some_and(|(last, _)| *last == seq)
                            },
                        )
                        .unwrap_or(true);
                    if known {
                        std::thread::sleep(Duration::from_micros(200));
                        continue;
                    }
                    let rows = runtime.block_on(scroll_view(
                        &view,
                        None,
                        &ScrollOrder::Pk,
                        u32::MAX,
                        Projection::full(),
                        None,
                    ));
                    match rows {
                        Ok((rows, _)) => {
                            let records = rows.into_iter().map(|row| row.record).collect();
                            if let Ok(mut seen) = seen.lock() {
                                seen.push((seq, records));
                            }
                        }
                        // The crash reached the files this view needs.
                        Err(_) => break,
                    }
                }
            })
        };
        Self {
            stop,
            seen,
            thread: Some(thread),
        }
    }

    fn finish(mut self) -> Vec<(SeqNo, Vec<Record>)> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.seen
            .lock()
            .map(|seen| seen.clone())
            .unwrap_or_default()
    }
}

/// One run of a scenario: the session, the models after each request, and what the body
/// holds (a pinned token, stepped jobs).
pub struct Ctx {
    pub session: Session,
    /// The model after each request since the collection was created, the first one being
    /// the created collection. Requests run one at a time, so this is a chain.
    history: Vec<Model>,
    /// Index into `history` of the last acknowledged state.
    acked: usize,
    token: Option<SnapshotToken>,
    jobs: Vec<SteppedJob>,
}

impl Drop for Ctx {
    fn drop(&mut self) {
        self.jobs.clear();
    }
}

impl Ctx {
    pub fn new(seed: u64) -> Result<Self, String> {
        Self::with_indexes(seed, false)
    }

    /// A context whose segments get SQ8 codes and graphs (from a few rows) when `indexed`.
    pub fn with_indexes(seed: u64, indexed: bool) -> Result<Self, String> {
        let session = Session::create(setup(indexed), seed)?;
        let model = Runner::new_model(&session);
        Ok(Self {
            session,
            history: vec![model],
            acked: 0,
            token: None,
            jobs: Vec::new(),
        })
    }

    pub fn fault(&self) -> Arc<FaultVfs> {
        self.session.fault().clone()
    }

    pub fn last(&self) -> &Model {
        self.history
            .last()
            .expect("the history starts with the created collection")
    }

    fn record(&self, index: u64, x: f32) -> Record {
        let model = self.last();
        let mut record =
            Record::new(key(index)).with_vector(model.vector_name(), vec![x, 1.0, 0.0, 0.0]);
        if model.schema.scalar_field("n").is_some() {
            record
                .fields
                .insert("n".to_owned(), Value::Int64(index as i64));
        }
        record
    }

    pub fn upsert(&self, index: u64, x: f32) -> ClientOp {
        ClientOp::Upsert(self.record(index, x))
    }

    pub fn delete(&self, index: u64) -> ClientOp {
        ClientOp::Delete(key(index))
    }

    pub fn update(&self, index: u64, x: f32) -> ClientOp {
        let mut update = logpose_types::record::PartialUpdate::new(key(index));
        update
            .vectors
            .insert(self.last().vector_name(), vec![x, 2.0, 0.0, 0.0]);
        ClientOp::Update(update)
    }

    /// Write a batch; its model joins the history before the call (it may be durable even if
    /// the call fails).
    pub fn write(&mut self, ops: Vec<ClientOp>) -> Step {
        let next = self
            .last()
            .apply_batch(&ops)
            .map_err(|refusal| format!("the model refuses {refusal:?}"))?;
        self.history.push(next);
        let ack = self
            .session
            .handle()
            .write_blocking(ops)
            .map_err(|error| format!("write: {error}"))?;
        if ack.last_seq_no != self.last().visible_seq_no {
            return Err(format!(
                "the ack ends at {}, the model at {}",
                ack.last_seq_no,
                self.last().visible_seq_no
            ));
        }
        self.acked = self.history.len() - 1;
        Ok(())
    }

    pub fn alter(&mut self, change: SchemaChange) -> Step {
        let next = self
            .last()
            .apply_schema(&change)
            .map_err(|refusal| format!("the model refuses {refusal:?}"))?;
        self.history.push(next);
        self.session
            .handle()
            .alter_schema_blocking(change)
            .map_err(|error| format!("alter: {error}"))?;
        self.acked = self.history.len() - 1;
        Ok(())
    }

    pub fn flush(&mut self) -> Step {
        self.session
            .handle()
            .flush_blocking()
            .map(drop)
            .map_err(|error| format!("flush: {error}"))
    }

    pub fn compact(&mut self) -> Step {
        self.session
            .handle()
            .compact_blocking()
            .map(drop)
            .map_err(|error| format!("compact: {error}"))
    }

    pub fn begin(&mut self, kind: JobKind) -> Step {
        if self.begin_if_due(kind)? {
            Ok(())
        } else {
            Err(format!("the {kind:?} job has nothing to do"))
        }
    }

    /// Begin a job of `kind` if it has work, and say whether it had: an index build begun while
    /// a compaction holds every segment has none.
    pub fn begin_if_due(&mut self, kind: JobKind) -> Result<bool, String> {
        let job = self
            .session
            .engine()
            .begin_job(self.session.handle(), kind)
            .map_err(|error| format!("begin {kind:?}: {error}"))?;
        if !job.has_work() {
            return Ok(false);
        }
        self.jobs.push(job);
        Ok(true)
    }

    /// Whether a job of `kind` is open.
    pub fn has_job(&self, kind: JobKind) -> bool {
        self.jobs.iter().any(|job| job.kind() == kind)
    }

    fn job(&mut self, kind: JobKind) -> Result<usize, String> {
        self.jobs
            .iter()
            .position(|job| job.kind() == kind)
            .ok_or_else(|| format!("no open {kind:?} job"))
    }

    pub fn build(&mut self, kind: JobKind) -> Step {
        let index = self.job(kind)?;
        self.jobs[index]
            .build()
            .map_err(|error| format!("build {kind:?}: {error}"))
    }

    pub fn commit(&mut self, kind: JobKind) -> Step {
        let index = self.job(kind)?;
        self.jobs
            .remove(index)
            .commit()
            .map(drop)
            .map_err(|error| format!("commit {kind:?}: {error}"))
    }

    pub fn pin(&mut self) -> Step {
        self.token = Some(
            self.session
                .handle()
                .pin_snapshot()
                .map_err(|error| format!("pin: {error}"))?,
        );
        Ok(())
    }

    pub fn release(&mut self) -> Step {
        if let Some(token) = self.token.take() {
            self.session.handle().release_snapshot(&token);
        }
        Ok(())
    }

    pub fn wait_for_gc(&mut self) -> Step {
        self.session.engine().wait_for_gc();
        Ok(())
    }

    /// Fail the file sync `nth` from now (keeping any planned crash).
    pub fn fail_sync(&mut self, nth: u64) {
        let fault = self.fault();
        let mut plan = fault.plan();
        plan.fail_sync = Some(fault.file_syncs() + nth);
        fault.set_plan(plan);
    }

    pub fn clear_fail_sync(&mut self) {
        let fault = self.fault();
        let mut plan = fault.plan();
        plan.fail_sync = None;
        fault.set_plan(plan);
    }

    pub fn close(&mut self) {
        self.jobs.clear();
        self.token = None;
        self.session.close();
    }
}

/// One scenario: a setup run without faults and a body whose every operation is a crash
/// candidate.
pub struct Scenario {
    pub name: &'static str,
    /// Whether segments get SQ8 codes and graphs.
    pub indexed: bool,
    pub setup: fn(&mut Ctx) -> Step,
    pub body: fn(&mut Ctx) -> Step,
}

/// What the physical and logical state of a recovered collection is, for comparing two
/// recoveries of one image.
#[derive(Debug, PartialEq)]
struct Digest {
    records: Vec<Record>,
    visible_seq_no: SeqNo,
    schema: String,
    manifest_generation: u64,
    checkpoint_seq_no: SeqNo,
    files: BTreeMap<PathBuf, Vec<u8>>,
}

fn digest(session: &Session) -> Result<Digest, String> {
    let version = session.handle().current();
    let view = session
        .view(&ReadOptions::default())
        .map_err(|error| format!("view: {error}"))?;
    let (rows, _) = session
        .block_on(scroll_view(
            &view,
            None,
            &ScrollOrder::Pk,
            u32::MAX,
            Projection::full(),
            None,
        ))
        .map_err(|error| format!("scan: {error}"))?;
    let mut files = BTreeMap::new();
    list_files(session.fault().as_ref(), Path::new(ROOT), &mut files)?;
    Ok(Digest {
        records: rows.into_iter().map(|row| row.record).collect(),
        visible_seq_no: version.visible_seq_no,
        schema: format!("{:?}", version.schema),
        manifest_generation: version.manifest_generation,
        checkpoint_seq_no: version.checkpoint_seq_no,
        files,
    })
}

/// Every file under `dir` with its content, except the root lock.
fn list_files(vfs: &dyn Vfs, dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) -> Step {
    let entries = vfs
        .list(dir)
        .map_err(|error| format!("list {}: {error}", dir.display()))?;
    for entry in entries {
        let path = dir.join(&entry.name);
        if entry.is_dir {
            list_files(vfs, &path, files)?;
        } else if entry.name != "LOCK" {
            let bytes = logpose_vfs::read_file(vfs, &path)
                .map_err(|error| format!("read {}: {error}", path.display()))?;
            files.insert(path, bytes);
        }
    }
    Ok(())
}

/// Differences between two digests, for failure messages.
fn difference(left: &Digest, right: &Digest) -> String {
    let mut out = Vec::new();
    if left.records != right.records {
        out.push(format!("rows {:?} vs {:?}", left.records, right.records));
    }
    if left.visible_seq_no != right.visible_seq_no {
        out.push(format!(
            "sequence {} vs {}",
            left.visible_seq_no, right.visible_seq_no
        ));
    }
    if left.schema != right.schema {
        out.push("schema differs".to_owned());
    }
    if left.manifest_generation != right.manifest_generation
        || left.checkpoint_seq_no != right.checkpoint_seq_no
    {
        out.push(format!(
            "manifest {}/{} vs {}/{}",
            left.manifest_generation,
            left.checkpoint_seq_no,
            right.manifest_generation,
            right.checkpoint_seq_no
        ));
    }
    let names = |digest: &Digest| digest.files.keys().cloned().collect::<BTreeSet<_>>();
    let (a, b) = (names(left), names(right));
    for only in a.difference(&b) {
        out.push(format!("only in the first: {}", only.display()));
    }
    for only in b.difference(&a) {
        out.push(format!("only in the second: {}", only.display()));
    }
    for name in a.intersection(&b) {
        if left.files[name] != right.files[name] {
            out.push(format!(
                "{} differs ({} vs {} bytes)",
                name.display(),
                left.files[name].len(),
                right.files[name].len()
            ));
        }
    }
    out.join("; ")
}

/// What an enumeration covered.
#[derive(Clone, Copy, Debug, Default)]
pub struct Coverage {
    pub crash_states: u64,
    pub unacked_kept: u64,
    pub observed_states: u64,
    pub recovery_crashes: u64,
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

/// Check the recovered collection against the history: I8 and I14.
fn check_recovered(
    session: &Session,
    history: &[Model],
    acked: usize,
    observed: &[(SeqNo, Vec<Record>)],
) -> Result<usize, String> {
    let runner = Checker::new(session);
    let mut errors = Vec::new();
    let mut kept = None;
    for (index, model) in history.iter().enumerate().skip(acked) {
        match runner.state_equals(model) {
            Ok(()) => {
                kept = Some(index);
                break;
            }
            Err(error) => errors.push(format!("prefix {index}: {error}")),
        }
    }
    let Some(kept) = kept else {
        return Err(format!(
            "I8: the recovered state is no prefix at or past the acknowledged request {acked} \
             of {}: {}",
            history.len() - 1,
            errors.join(" | ")
        ));
    };
    let recovered = history[kept].visible_seq_no;
    for (seq, records) in observed {
        if *seq > recovered {
            return Err(format!(
                "I14: a reader saw sequence {seq} before the crash, but recovery ends at \
                 {recovered}"
            ));
        }
        let Some(model) = history.iter().find(|model| model.visible_seq_no == *seq) else {
            return Err(format!(
                "I3: a reader saw sequence {seq}, which ends no request"
            ));
        };
        if *records != model.records() {
            return Err(format!(
                "I4: a reader saw other rows at sequence {seq}: {records:?} vs {:?}",
                model.records()
            ));
        }
    }
    Ok(kept)
}

/// Enumerate every crash point of `scenario` under every tear mode.
pub fn enumerate(scenario: &Scenario, seed: u64) -> Coverage {
    let context = |detail: String| format!("scenario {}: {detail}", scenario.name);
    // A clean run counts the body's operations.
    let ops = {
        let mut ctx =
            Ctx::with_indexes(seed, scenario.indexed).unwrap_or_else(|error| fail(context(error)));
        (scenario.setup)(&mut ctx).unwrap_or_else(|error| fail(context(format!("setup: {error}"))));
        let start = ctx.fault().mutating_ops();
        (scenario.body)(&mut ctx).unwrap_or_else(|error| fail(context(format!("body: {error}"))));
        let ops = ctx.fault().mutating_ops() - start;
        let history = ctx.history.clone();
        ctx.close();
        // The clean run recovers its final state.
        ctx.session
            .open()
            .unwrap_or_else(|error| fail(context(error)));
        check_recovered(&ctx.session, &history, history.len() - 1, &[])
            .unwrap_or_else(|error| fail(context(format!("clean run: {error}"))));
        ops
    };
    let stride = env_u64("LOGPOSE_CRASH_I11_STRIDE").unwrap_or(3).max(1);
    let all_tears = std::env::var("LOGPOSE_CRASH_I11_TEARS").as_deref() == Ok("all");
    let mut coverage = Coverage::default();
    for k in 0..=ops {
        for tear in TearMode::ALL {
            let detail =
                |message: String| context(format!("crash after {k} ops, {tear:?}: {message}"));
            let run_seed = seed.wrapping_mul(1_000_003) ^ (k * 4 + tear as u64);
            let mut ctx = Ctx::with_indexes(run_seed, scenario.indexed)
                .unwrap_or_else(|error| fail(detail(error)));
            (scenario.setup)(&mut ctx)
                .unwrap_or_else(|error| fail(detail(format!("setup: {error}"))));
            let fault = ctx.fault();
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let observer = Observer::start(&ctx.session);
            let _ = (scenario.body)(&mut ctx);
            let observed = observer.finish();
            let (history, acked) = (ctx.history.clone(), ctx.acked);
            ctx.close();
            fault.crash();
            let image = fault.fork();

            // Recover, and check I8 and I14.
            ctx.session
                .open()
                .unwrap_or_else(|error| fail(detail(format!("recovery: {error}"))));
            let recovery_ops = fault.mutating_ops();
            let kept = check_recovered(&ctx.session, &history, acked, &observed)
                .unwrap_or_else(|error| fail(detail(error)));
            coverage.crash_states += 1;
            coverage.observed_states += observed.len() as u64;
            if kept > acked {
                coverage.unacked_kept += 1;
            }
            let reference = digest(&ctx.session).unwrap_or_else(|error| fail(detail(error)));

            // I11: recovery crashed at each of its operations, then rerun, ends where the clean
            // recovery did.
            if k % stride == 0 {
                let tears: Vec<TearMode> = if all_tears {
                    TearMode::ALL.to_vec()
                } else {
                    vec![TearMode::ALL[(k as usize / stride as usize) % TearMode::ALL.len()]]
                };
                for recovery_tear in tears {
                    for j in 0..recovery_ops {
                        let copy = image.fork();
                        copy.set_plan(FaultPlan {
                            crash_after_ops: Some(j),
                            tear: recovery_tear,
                            ..FaultPlan::default()
                        });
                        let mut session = ctx
                            .session
                            .attach(Arc::clone(&copy))
                            .unwrap_or_else(|error| fail(detail(error)));
                        session.open_and_drop();
                        copy.crash();
                        session.open().unwrap_or_else(|error| {
                            fail(detail(format!(
                                "I11: recovery crashed after {j} ops ({recovery_tear:?}) does \
                                 not reopen: {error}"
                            )))
                        });
                        let rerun = digest(&session).unwrap_or_else(|error| fail(detail(error)));
                        if rerun != reference {
                            fail(detail(format!(
                                "I11: recovery crashed after {j} of {recovery_ops} ops \
                                 ({recovery_tear:?}) then rerun differs from a clean recovery: \
                                 {}",
                                difference(&rerun, &reference)
                            )));
                        }
                        coverage.recovery_crashes += 1;
                    }
                }
                // Crashes compound: each recovery crashes one operation later than the last,
                // on what the last one left, before a clean one.
                let copy = image.fork();
                let mut session = ctx
                    .session
                    .attach(Arc::clone(&copy))
                    .unwrap_or_else(|error| fail(detail(error)));
                for j in 0..recovery_ops {
                    copy.set_plan(FaultPlan {
                        crash_after_ops: Some(copy.mutating_ops() + j),
                        tear: TearMode::ALL[(k + j) as usize % TearMode::ALL.len()],
                        ..FaultPlan::default()
                    });
                    session.open_and_drop();
                    copy.crash();
                    coverage.recovery_crashes += 1;
                }
                copy.set_plan(FaultPlan::default());
                session.open().unwrap_or_else(|error| {
                    fail(detail(format!(
                        "I11: recovery after {recovery_ops} compounding recovery crashes does not \
                         reopen: {error}"
                    )))
                });
                let rerun = digest(&session).unwrap_or_else(|error| fail(detail(error)));
                if rerun != reference {
                    fail(detail(format!(
                        "I11: recovery after {recovery_ops} compounding recovery crashes differs \
                         from a clean recovery: {}",
                        difference(&rerun, &reference)
                    )));
                }
            }

            // The recovered collection keeps working.
            let handle = ctx.session.handle();
            handle
                .write_blocking(vec![ctx.upsert(15, 99.0)])
                .unwrap_or_else(|error| fail(detail(format!("write after recovery: {error}"))));
            handle
                .flush_blocking()
                .unwrap_or_else(|error| fail(detail(format!("flush after recovery: {error}"))));
        }
    }
    println!("{}: {ops} body ops, {coverage:?}", scenario.name);
    coverage
}

#[allow(
    clippy::panic,
    reason = "a failed check reports the scenario and crash point"
)]
fn fail(message: String) -> ! {
    panic!("{message}");
}

// Scenarios.

/// Rows `k00..k05` in a segment, then `k06`, `k07` in the memtable, a segment row deleted and
/// one updated (deletion bits for the next flush's DV file).
pub fn rows_in_a_segment_and_the_memtable(ctx: &mut Ctx) -> Step {
    let ops = (0..6)
        .map(|index| ctx.upsert(index, index as f32))
        .collect();
    ctx.write(ops)?;
    ctx.flush()?;
    let ops = vec![ctx.upsert(6, 6.0), ctx.upsert(7, 7.0)];
    ctx.write(ops)?;
    let ops = vec![ctx.delete(1), ctx.update(2, 20.0)];
    ctx.write(ops)
}

/// Three segments of four rows each.
pub fn three_segments(ctx: &mut Ctx) -> Step {
    for segment in 0..3 {
        let ops = (0..4)
            .map(|row| ctx.upsert(segment * 4 + row, (segment * 4 + row) as f32))
            .collect();
        ctx.write(ops)?;
        ctx.flush()?;
    }
    Ok(())
}

fn flush_body(ctx: &mut Ctx) -> Step {
    let ops = vec![ctx.upsert(8, 8.0), ctx.delete(6)];
    ctx.write(ops)?;
    ctx.flush()?;
    let ops = vec![ctx.upsert(9, 9.0), ctx.delete(3)];
    ctx.write(ops)
}

fn compaction_with_concurrent_deletes(ctx: &mut Ctx) -> Step {
    ctx.begin(JobKind::Compact)?;
    let ops = vec![ctx.delete(1), ctx.delete(5)];
    ctx.write(ops)?;
    ctx.build(JobKind::Compact)?;
    let ops = vec![ctx.upsert(2, 22.0), ctx.update(9, 99.0)];
    ctx.write(ops)?;
    let op = ctx.delete(2);
    ctx.write(vec![op])?;
    ctx.commit(JobKind::Compact)?;
    let ops = vec![ctx.delete(10), ctx.upsert(12, 12.0)];
    ctx.write(ops)
}

/// A flush begins and commits while a compaction runs; `flush_first` decides which commits
/// first.
fn flush_during_compaction(ctx: &mut Ctx, flush_first: bool) -> Step {
    ctx.begin(JobKind::Compact)?;
    ctx.build(JobKind::Compact)?;
    let ops = vec![ctx.delete(1), ctx.upsert(12, 12.0), ctx.update(6, 60.0)];
    ctx.write(ops)?;
    ctx.begin(JobKind::Flush)?;
    let ops = vec![ctx.delete(12), ctx.delete(5)];
    ctx.write(ops)?;
    ctx.build(JobKind::Flush)?;
    if flush_first {
        ctx.commit(JobKind::Flush)?;
        ctx.commit(JobKind::Compact)?;
    } else {
        ctx.commit(JobKind::Compact)?;
        ctx.commit(JobKind::Flush)?;
    }
    let ops = vec![ctx.upsert(13, 13.0), ctx.delete(9)];
    ctx.write(ops)
}

fn checkpoint_only_flush(ctx: &mut Ctx) -> Step {
    let op = ctx.upsert(8, 8.0);
    ctx.write(vec![op])?;
    let ops = vec![ctx.delete(8), ctx.delete(1)];
    ctx.write(ops)?;
    // Every memtable row is deleted: the flush writes no segment, only the grown DV file and
    // the checkpoint.
    ctx.flush()?;
    let op = ctx.upsert(9, 9.0);
    ctx.write(vec![op])
}

fn schema_change_then_flush(ctx: &mut Ctx) -> Step {
    ctx.alter(SchemaChange::AddField(ScalarFieldSpec::new(
        "x",
        FieldType::Int64,
    )))?;
    let mut record = ctx.record(10, 10.0);
    record.fields.insert("x".to_owned(), Value::Int64(7));
    ctx.write(vec![ClientOp::Upsert(record)])?;
    ctx.alter(SchemaChange::RenameField {
        from: "s".to_owned(),
        to: "q".to_owned(),
    })?;
    ctx.flush()?;
    ctx.alter(SchemaChange::DropField {
        name: "n".to_owned(),
    })?;
    let op = ctx.upsert(11, 11.0);
    ctx.write(vec![op])?;
    ctx.flush()
}

/// The file sync, counted from a flush's start, that a failure there leaves the flush
/// abandoned without poisoning the collection: the last such one is the `CURRENT.tmp` sync of
/// the manifest publish, before its rename.
fn publish_sync_index() -> u64 {
    static INDEX: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *INDEX.get_or_init(|| {
        let mut found = None;
        for nth in 0..40 {
            let mut ctx = Ctx::new(7).unwrap_or_else(|error| fail(error));
            rows_in_a_segment_and_the_memtable(&mut ctx).unwrap_or_else(|error| fail(error));
            let op = ctx.upsert(8, 8.0);
            ctx.write(vec![op]).unwrap_or_else(|error| fail(error));
            ctx.fail_sync(nth);
            let failed = ctx.flush().is_err();
            if failed && !ctx.session.handle().is_poisoned() {
                found = Some(nth);
            }
            if !failed {
                break;
            }
        }
        found.unwrap_or_else(|| fail("no sync failure abandons a flush".to_owned()))
    })
}

fn failed_publish_then_retry(ctx: &mut Ctx) -> Step {
    let op = ctx.upsert(8, 8.0);
    ctx.write(vec![op])?;
    ctx.fail_sync(publish_sync_index());
    if ctx.flush().is_ok() {
        return Err("the flush with a failed publish succeeded".to_owned());
    }
    ctx.clear_fail_sync();
    if ctx.session.handle().is_poisoned() {
        return Err("a failed publish before the rename poisoned the collection".to_owned());
    }
    // The retry burns new ids and commits.
    ctx.flush()?;
    let op = ctx.upsert(9, 9.0);
    ctx.write(vec![op])
}

/// Two segments; a token pins them; a compaction retires them; a write lands.
fn pinned_retired_segments(ctx: &mut Ctx) -> Step {
    for segment in 0..2 {
        let ops = (0..3)
            .map(|row| ctx.upsert(segment * 3 + row, (segment * 3 + row) as f32))
            .collect();
        ctx.write(ops)?;
        ctx.flush()?;
    }
    ctx.pin()?;
    ctx.compact()?;
    let op = ctx.delete(4);
    ctx.write(vec![op])
}

fn gc_after_token_release(ctx: &mut Ctx) -> Step {
    ctx.release()?;
    ctx.wait_for_gc()?;
    let op = ctx.upsert(7, 7.0);
    ctx.write(vec![op])?;
    ctx.flush()?;
    ctx.wait_for_gc()
}

fn no_setup(_: &mut Ctx) -> Step {
    Ok(())
}

pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            indexed: false,
            name: "flush",
            setup: rows_in_a_segment_and_the_memtable,
            body: flush_body,
        },
        Scenario {
            indexed: false,
            name: "compaction with concurrent deletes",
            setup: three_segments,
            body: compaction_with_concurrent_deletes,
        },
        Scenario {
            indexed: false,
            name: "flush during compaction, flush commits first",
            setup: three_segments,
            body: |ctx| flush_during_compaction(ctx, true),
        },
        Scenario {
            indexed: false,
            name: "flush during compaction, compaction commits first",
            setup: three_segments,
            body: |ctx| flush_during_compaction(ctx, false),
        },
        Scenario {
            indexed: false,
            name: "checkpoint-only flush",
            setup: rows_in_a_segment_and_the_memtable,
            body: checkpoint_only_flush,
        },
        Scenario {
            indexed: false,
            name: "schema change then flush",
            setup: rows_in_a_segment_and_the_memtable,
            body: schema_change_then_flush,
        },
        Scenario {
            indexed: false,
            name: "failed manifest publish then retry",
            setup: rows_in_a_segment_and_the_memtable,
            body: failed_publish_then_retry,
        },
        Scenario {
            indexed: false,
            name: "GC after token release",
            setup: pinned_retired_segments,
            body: gc_after_token_release,
        },
        Scenario {
            indexed: false,
            name: "writes from an empty collection",
            setup: no_setup,
            body: flush_body_from_empty,
        },
        Scenario {
            indexed: true,
            name: "index build beside writes",
            setup: three_segments,
            body: index_build_beside_writes,
        },
        Scenario {
            indexed: true,
            name: "index build cancelled by a compaction",
            setup: three_segments,
            body: index_build_cancelled_by_a_compaction,
        },
    ]
}

/// An index build of one segment, with writes that delete and move its rows between its
/// phases, then a flush that writes that segment's DV file beside its new sidecar.
fn index_build_beside_writes(ctx: &mut Ctx) -> Step {
    ctx.begin(JobKind::Index)?;
    let ops = vec![ctx.delete(9), ctx.upsert(10, 100.0)];
    ctx.write(ops)?;
    ctx.build(JobKind::Index)?;
    let op = ctx.update(11, 110.0);
    ctx.write(vec![op])?;
    ctx.commit(JobKind::Index)?;
    let ops = vec![ctx.delete(8), ctx.upsert(12, 12.0)];
    ctx.write(ops)?;
    ctx.flush()
}

/// A compaction takes the segment an index build is building: the build ends without a change
/// and removes its sidecar, the compaction commits, and its output is indexed.
fn index_build_cancelled_by_a_compaction(ctx: &mut Ctx) -> Step {
    ctx.begin(JobKind::Index)?;
    ctx.build(JobKind::Index)?;
    ctx.begin(JobKind::Compact)?;
    let op = ctx.delete(3);
    ctx.write(vec![op])?;
    ctx.commit(JobKind::Index)?;
    ctx.build(JobKind::Compact)?;
    ctx.commit(JobKind::Compact)?;
    ctx.begin(JobKind::Index)?;
    ctx.commit(JobKind::Index)?;
    let op = ctx.upsert(12, 12.0);
    ctx.write(vec![op])
}

fn flush_body_from_empty(ctx: &mut Ctx) -> Step {
    let ops = (0..3)
        .map(|index| ctx.upsert(index, index as f32))
        .collect();
    ctx.write(ops)?;
    ctx.flush()?;
    let ops = vec![ctx.delete(0), ctx.upsert(3, 3.0)];
    ctx.write(ops)
}

/// The model after one group of concurrent batches. The engine orders them as they arrive,
/// so a row the group wrote may carry any sequence number of the group.
fn apply_group(model: &Model, group: &[(Vec<ClientOp>, bool)]) -> Model {
    let mut next = model.clone();
    for (ops, _) in group {
        next = next
            .apply_batch(ops)
            .unwrap_or_else(|error| fail(format!("the model refuses {error:?}")));
    }
    let range = (model.visible_seq_no + 1, next.visible_seq_no);
    for row in next.rows.values_mut() {
        if row.seq.0 > model.visible_seq_no {
            row.seq = range;
        }
    }
    next
}

/// Batches per group in the group commit scenario.
const GROUP_WRITERS: u64 = 6;

/// Every crash point of one group commit of `GROUP_WRITERS` concurrent two-operation batches
/// on disjoint keys, under every tear mode: the group is whole or absent (I3), present when any
/// batch was acknowledged (I8), and the earlier group is kept.
pub fn group_commit(seed: u64) -> Coverage {
    let open_grouped = |seed: u64| -> Ctx {
        let mut ctx = Ctx::new(seed).unwrap_or_else(|error| fail(error));
        ctx.session.group = Some(GroupCommitConfig {
            commit_delay: Duration::from_secs(30),
            min_group_requests: GROUP_WRITERS as usize,
            ..GroupCommitConfig::default()
        });
        ctx.close();
        ctx.session.open().unwrap_or_else(|error| fail(error));
        ctx
    };
    // Each writer's batch, as the model's operations.
    let batch = |ctx: &Ctx, round: u64, writer: u64| {
        let base = round * 100 + writer * 2;
        vec![ctx.upsert(base, base as f32), ctx.upsert(base + 1, 1.0)]
    };
    let write_group = |ctx: &Ctx, round: u64| -> Vec<(Vec<ClientOp>, bool)> {
        let threads = (0..GROUP_WRITERS)
            .map(|writer| {
                let ops = batch(ctx, round, writer);
                let handle = ctx.session.handle().clone();
                std::thread::spawn(move || {
                    let acked = handle.write_blocking(ops.clone()).is_ok();
                    (ops, acked)
                })
            })
            .collect::<Vec<_>>();
        threads
            .into_iter()
            .map(|thread| thread.join().expect("a writer thread joins"))
            .collect()
    };
    let (group_ops, group_syncs) = {
        let ctx = open_grouped(seed);
        assert!(write_group(&ctx, 0).iter().all(|(_, acked)| *acked));
        let fault = ctx.fault();
        let (ops, syncs) = (fault.mutating_ops(), fault.file_syncs());
        assert!(write_group(&ctx, 1).iter().all(|(_, acked)| *acked));
        (fault.mutating_ops() - ops, fault.file_syncs() - syncs)
    };
    assert_eq!(
        group_syncs, 1,
        "{GROUP_WRITERS} concurrent batches share one fsync"
    );
    let mut coverage = Coverage::default();
    for k in 0..=group_ops {
        for tear in TearMode::ALL {
            let detail = format!("group commit: crash after {k} ops, {tear:?}");
            let mut ctx = open_grouped(seed ^ (k * 4 + tear as u64 + 1));
            let base = {
                let setup = write_group(&ctx, 0);
                assert!(setup.iter().all(|(_, acked)| *acked), "{detail}");
                apply_group(ctx.last(), &setup)
            };
            let fault = ctx.fault();
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + k),
                tear,
                ..FaultPlan::default()
            });
            let results = write_group(&ctx, 1);
            ctx.close();
            fault.crash();
            ctx.session
                .open()
                .unwrap_or_else(|error| fail(format!("{detail}: {error}")));
            // Either the whole group or none of it, in whatever order its batches took.
            let whole = apply_group(&base, &results);
            let history = vec![base, whole];
            let acked = usize::from(results.iter().any(|(_, acked)| *acked));
            let kept = check_recovered(&ctx.session, &history, acked, &[])
                .unwrap_or_else(|error| fail(format!("{detail}: {error}")));
            coverage.crash_states += 1;
            if kept > acked {
                coverage.unacked_kept += 1;
            }
        }
    }
    println!("group commit: {group_ops} ops, {coverage:?}");
    coverage
}

#[test]
fn every_crash_point_of_one_group_commit_keeps_the_group_whole() {
    let coverage = group_commit(1);
    assert!(coverage.unacked_kept > 0, "{coverage:?}");
}

#[test]
fn every_crash_point_of_a_flush_recovers_a_prefix() {
    enumerate(&scenarios()[0], 11);
}

#[test]
fn every_crash_point_of_a_compaction_with_concurrent_deletes_recovers_a_prefix() {
    enumerate(&scenarios()[1], 12);
}

#[test]
fn every_crash_point_of_a_flush_during_a_compaction_recovers_a_prefix() {
    enumerate(&scenarios()[2], 13);
    enumerate(&scenarios()[3], 14);
}

#[test]
fn every_crash_point_of_a_checkpoint_only_flush_recovers_a_prefix() {
    enumerate(&scenarios()[4], 15);
}

#[test]
fn every_crash_point_of_a_schema_change_then_flush_recovers_a_prefix() {
    enumerate(&scenarios()[5], 16);
}

#[test]
fn every_crash_point_of_a_failed_publish_and_its_retry_recovers_a_prefix() {
    enumerate(&scenarios()[6], 17);
}

#[test]
fn every_crash_point_of_gc_after_a_token_release_recovers_a_prefix() {
    enumerate(&scenarios()[7], 18);
}

#[test]
fn every_crash_point_of_an_index_build_recovers_a_prefix() {
    enumerate(&scenarios()[9], 20);
}

#[test]
fn every_crash_point_of_an_index_build_a_compaction_cancels_recovers_a_prefix() {
    enumerate(&scenarios()[10], 21);
}

#[test]
fn every_crash_point_of_writes_from_an_empty_collection_recovers_a_prefix() {
    let coverage = enumerate(&scenarios()[8], 19);
    assert!(
        coverage.unacked_kept > 0 && coverage.recovery_crashes > 0,
        "{coverage:?}"
    );
}
