//! Compaction v2 and maintenance scheduling at the writer: the size-tiered policy picking
//! segments as flushes land, flush priority over compaction, the write stall, the
//! maintenance-memory reservation, background compaction beside writes, deletes, and flushes,
//! write amplification, several compactions of one collection at once beside concurrent
//! clients and crashes, and what a collection drop and an engine drop release and answer. The
//! randomized model check with background jobs and crashes is harness v2's
//! (`tests/harness/random.rs`).

use super::*;
use crate::{
    CompactionConfig, CreateCollectionRequest, Engine, EngineConfig, ManualClock, MemtableConfig,
    RuntimeConfig, test_support::flat_row,
};
use logpose_types::{
    CollectionRef, DistanceMetric,
    record::{PartialUpdate, PrimaryKey, Record},
};
use logpose_vfs::{FaultPlan, FaultVfs, TearMode};
use logpose_wal::BootId;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

const ROOT: &str = "/storage";
const NAME: &str = "tiers";

/// A small engine: few threads, so hundreds of them open quickly.
fn config() -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new("boot")),
        runtime: RuntimeConfig {
            io_threads: 2,
            query_threads: 1,
            maintenance_threads: 1,
            writer_threads: 1,
            ..RuntimeConfig::default()
        },
        ..EngineConfig::default()
    }
}

/// Tiers of 2, 8, 32, ... live rows and merges of two segments of a tier.
fn tiny_tiers() -> CompactionConfig {
    CompactionConfig {
        base_rows: 2,
        tier_ratio: 4,
        min_merge: 2,
        max_merge: 4,
        ..CompactionConfig::default()
    }
}

fn open(fault: &Arc<FaultVfs>, config: EngineConfig) -> Engine {
    Engine::open(fault.process(), ROOT, config).expect("engine should open")
}

/// Create the collection: a flush every `flush_ops` operations and background compaction
/// merging `min_merge` segments of a tier (`usize::MAX` for neither).
fn create(engine: &Engine, flush_ops: usize, min_merge: usize) -> Arc<CollectionHandle> {
    let mut descriptor = engine
        .core()
        .plan_collection_descriptor(&CreateCollectionRequest::new(NAME, 2, DistanceMetric::Dot))
        .expect("descriptor should plan");
    descriptor.flush_threshold_ops = flush_ops;
    descriptor.flush_threshold_bytes = usize::MAX;
    descriptor.compaction_threshold_segments = min_merge;
    engine
        .create_collection_blocking(descriptor, None)
        .expect("collection should be created")
}

fn reopen(engine: &Engine) -> Arc<CollectionHandle> {
    engine
        .collection(&CollectionRef::new_default(NAME))
        .expect("collection should reopen")
}

fn upsert(id: &str, x: f32) -> ClientOp {
    let mut record = Record::new(id).with_vector("vector", vec![x, 1.0]);
    record.extra.insert("n".to_owned(), json!(x));
    ClientOp::Upsert(record)
}

fn delete(id: &str) -> ClientOp {
    ClientOp::Delete(PrimaryKey::from(id))
}

fn write(handle: &CollectionHandle, ops: Vec<ClientOp>) {
    handle.write_blocking(ops).expect("write should commit");
}

/// Every live row, as `id -> x`, after checking the version's invariants (I5, I13).
fn live(handle: &CollectionHandle) -> BTreeMap<String, f32> {
    let version = handle.current();
    version.check_invariants().expect("invariants hold");
    version
        .live_images()
        .into_iter()
        .map(|(_, image)| {
            let (id, vector, _) = flat_row(&version.schema, &image);
            (id, vector[0])
        })
        .collect()
}

fn wait_for(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Wait until the collection has no maintenance job planned or running and none is due.
fn wait_idle(engine: &Engine, handle: &CollectionHandle) {
    let settled = || {
        let status = handle.maintenance_status();
        status.pending.is_empty()
            && status.in_progress.is_none()
            && handle.current().frozen.is_empty()
            && engine.scheduler().stats().running == 0
    };
    // Settled twice across a tick, so a job the tick would plan is seen.
    wait_for("maintenance to settle", || {
        settled() && {
            std::thread::sleep(TICK_INTERVAL * 2);
            settled()
        }
    });
}

fn segment_rows(handle: &CollectionHandle) -> Vec<u32> {
    handle
        .current()
        .segments
        .iter()
        .map(|segment| segment.row_count())
        .collect()
}

/// As flushes land, the policy merges each tier as it fills: two 1-row segments into a 2-row
/// one (tier 1), two of those into a 4-row one, and so on, so the collection never holds two
/// segments of one tier once maintenance settles.
#[test]
fn background_compaction_merges_each_tier_as_flushes_fill_it() {
    let fault = FaultVfs::new(1);
    let engine = open(
        &fault,
        EngineConfig {
            compaction: tiny_tiers(),
            ..config()
        },
    );
    let handle = create(&engine, 1, 2);
    for index in 0..16 {
        write(&handle, vec![upsert(&format!("k{index:02}"), index as f32)]);
        wait_idle(&engine, &handle);
        let rows = segment_rows(&handle);
        let policy = Policy::new(tiny_tiers(), 2, u64::MAX, Default::default());
        let mut tiers = rows
            .iter()
            .map(|rows| policy.tier(u64::from(*rows)))
            .collect::<Vec<_>>();
        tiers.sort_unstable();
        let distinct = tiers.len();
        tiers.dedup();
        assert_eq!(tiers.len(), distinct, "after {index}: segments {rows:?}");
        assert_eq!(live(&handle).len(), index + 1);
    }
    // Sixteen rows: one segment per tier the binary representation needs.
    assert_eq!(segment_rows(&handle), [16]);
    let written = handle.maintenance_written();
    assert_eq!(written.flush_rows, 16);
    assert!(written.compaction_rows > 0);
    drop(handle);
    drop(engine);
    fault.crash();
    let engine = open(&fault, config());
    let handle = reopen(&engine);
    assert_eq!(live(&handle).len(), 16);
}

/// A waiting flush is granted before a compaction that asked earlier: with every permit held
/// back, a compaction is planned, then a flush; the next grant goes to the flush.
#[test]
fn a_flush_is_granted_before_a_compaction_that_asked_first() {
    let fault = FaultVfs::new(2);
    let engine = open(
        &fault,
        EngineConfig {
            compaction: tiny_tiers(),
            ..config()
        },
    );
    let scheduler = engine.scheduler().clone();
    let handle = create(&engine, usize::MAX, 2);
    scheduler.pause();
    let flush = |handle: &Arc<CollectionHandle>| {
        let handle = Arc::clone(handle);
        std::thread::spawn(move || handle.flush_blocking())
    };
    // Two one-row segments, one grant each.
    for id in ["a", "b"] {
        write(&handle, vec![upsert(id, 1.0)]);
        let flushed = flush(&handle);
        wait_for("the flush to ask", || scheduler.stats().waiting == 1);
        scheduler.step(1);
        flushed
            .join()
            .expect("flush thread")
            .expect("flush should commit");
    }
    // The two tier-0 segments make a compaction ask for a permit.
    wait_for("the compaction to ask", || {
        handle.maintenance_status().pending == ["compact"]
    });
    assert_eq!(segment_rows(&handle), [1, 1]);

    // Then a flush asks.
    write(&handle, vec![upsert("c", 1.0)]);
    let flushed = flush(&handle);
    wait_for("the flush to ask", || scheduler.stats().waiting == 2);
    scheduler.step(1);
    wait_for("the flush to be granted first", || flushed.is_finished());
    flushed
        .join()
        .expect("flush thread")
        .expect("the flush is granted first");
    assert_eq!(
        segment_rows(&handle),
        [1, 1, 1],
        "nothing was compacted yet"
    );
    assert_eq!(handle.maintenance_status().pending, ["compact"]);
    let stats = scheduler.stats();
    assert_eq!((stats.flushes_granted, stats.compactions_granted), (3, 0));

    scheduler.resume();
    wait_idle(&engine, &handle);
    assert!(scheduler.stats().compactions_granted >= 1);
    assert_eq!(live(&handle).len(), 3);
}

/// With `max_frozen` memtables frozen and the active one over its trigger, the writer stops
/// taking requests: a write waits until a flush commits, and then goes through.
#[test]
fn writes_stall_at_max_frozen_and_resume_once_a_flush_commits() {
    let fault = FaultVfs::new(3);
    let engine = open(
        &fault,
        EngineConfig {
            memtable: MemtableConfig {
                max_frozen: 1,
                ..MemtableConfig::default()
            },
            ..config()
        },
    );
    let scheduler = engine.scheduler().clone();
    let handle = create(&engine, 2, usize::MAX);
    scheduler.pause();
    // Two operations reach the trigger: the memtable freezes and its flush waits.
    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    wait_for("the freeze", || handle.current().frozen.len() == 1);
    // Two more reach it again; nothing more can freeze, so writes stall.
    write(&handle, vec![upsert("c", 3.0), upsert("d", 4.0)]);
    let stalled = {
        let handle = Arc::clone(&handle);
        std::thread::spawn(move || handle.write_blocking(vec![upsert("e", 5.0)]))
    };
    std::thread::sleep(TICK_INTERVAL * 3);
    assert!(!stalled.is_finished(), "the write must wait for a flush");
    assert_eq!(handle.current().visible_seq_no, 4);

    // The flush of a and b commits; c and d freeze in turn, and the write goes through.
    scheduler.step(1);
    wait_for("the stall to end", || stalled.is_finished());
    let ack = stalled
        .join()
        .expect("writer thread")
        .expect("the write succeeds once the stall ends");
    assert_eq!(ack.last_seq_no, 5);
    assert_eq!(handle.current().checkpoint_seq_no, 2);
    scheduler.resume();
    wait_idle(&engine, &handle);
    assert_eq!(live(&handle).len(), 5);
}

/// A write that waits through a stall for longer than `write_stall_timeout` fails with
/// `WriteStalled` and is never applied; later writes go through once the stall ends.
#[test]
fn a_write_stalled_past_the_timeout_fails_and_is_never_applied() {
    let fault = FaultVfs::new(4);
    let clock = Arc::new(ManualClock::new());
    let engine = open(
        &fault,
        EngineConfig {
            memtable: MemtableConfig {
                max_frozen: 1,
                write_stall_timeout: Duration::from_secs(5),
                ..MemtableConfig::default()
            },
            clock: Some(Arc::clone(&clock) as Arc<dyn crate::Clock>),
            ..config()
        },
    );
    let scheduler = engine.scheduler().clone();
    let handle = create(&engine, 2, usize::MAX);
    scheduler.pause();
    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    wait_for("the freeze", || handle.current().frozen.len() == 1);
    write(&handle, vec![upsert("c", 3.0), upsert("d", 4.0)]);
    let stalled = {
        let handle = Arc::clone(&handle);
        std::thread::spawn(move || handle.write_blocking(vec![upsert("late", 5.0)]))
    };
    std::thread::sleep(TICK_INTERVAL * 2);
    assert!(!stalled.is_finished());
    clock.advance(Duration::from_secs(6));
    let error = stalled
        .join()
        .expect("writer thread")
        .expect_err("the stall outlasted the timeout");
    assert!(
        matches!(error, LogPoseError::WriteStalled { .. }),
        "{error:?}"
    );
    assert_eq!(error.retry_after(), Some(Duration::from_secs(1)));

    scheduler.resume();
    write(&handle, vec![upsert("f", 6.0)]);
    let rows = live(&handle);
    assert!(
        !rows.contains_key("late"),
        "a stalled write is never applied"
    );
    assert_eq!(rows.len(), 5);
    assert_eq!(
        handle.current().visible_seq_no,
        5,
        "it took no sequence number"
    );
}

/// Compaction permits reserve their build memory from the maintenance pool: jobs wait when it
/// is taken, and the reservation never exceeds the pool.
#[test]
fn compactions_never_reserve_more_than_the_maintenance_memory_pool() {
    let fault = FaultVfs::new(5);
    // A pool of about 60 KiB: a few small segments per job, and not every job at once. Four
    // job threads, so memory, not the compaction slots, is what holds jobs back.
    let base = config();
    let engine = open(
        &fault,
        EngineConfig {
            memory_limit: 300 << 10,
            compaction: tiny_tiers(),
            runtime: RuntimeConfig {
                maintenance_threads: 4,
                ..base.runtime
            },
            ..base
        },
    );
    let handles = (0..3)
        .map(|index| {
            let mut descriptor = engine
                .core()
                .plan_collection_descriptor(&CreateCollectionRequest::new(
                    format!("pool-{index}"),
                    2,
                    DistanceMetric::Dot,
                ))
                .expect("descriptor should plan");
            descriptor.flush_threshold_ops = 4;
            descriptor.flush_threshold_bytes = usize::MAX;
            descriptor.compaction_threshold_segments = 2;
            engine
                .create_collection_blocking(descriptor, None)
                .expect("collection should be created")
        })
        .collect::<Vec<_>>();
    let writers = handles
        .iter()
        .map(|handle| {
            let handle = Arc::clone(handle);
            std::thread::spawn(move || {
                for index in 0..64 {
                    let ops = (0..4)
                        .map(|row| upsert(&format!("k{index}-{row}"), index as f32))
                        .collect();
                    handle.write_blocking(ops).expect("write");
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().expect("writer thread");
    }
    for handle in &handles {
        wait_idle(&engine, handle);
        assert_eq!(live(handle).len(), 256);
    }
    let stats = engine.scheduler().stats();
    assert!(stats.compactions_granted > 0, "{stats:?}");
    assert!(stats.peak_reserved_bytes > 0, "{stats:?}");
    assert!(stats.peak_reserved_bytes <= stats.pool_bytes, "{stats:?}");
    assert_eq!(stats.reserved_bytes, 0);
}

/// An explicit compaction whose first two segments already need more build memory than the
/// whole pool is declined, and changes nothing; background compaction never plans it.
#[test]
fn a_compaction_larger_than_the_pool_is_declined() {
    let fault = FaultVfs::new(6);
    let engine = open(
        &fault,
        EngineConfig {
            memory_limit: 4 << 10,
            ..config()
        },
    );
    let handle = create(&engine, usize::MAX, 2);
    for index in 0..3 {
        let ops = (0..8)
            .map(|row| upsert(&format!("k{index}-{row}"), 1.0))
            .collect();
        write(&handle, ops);
        handle.flush_blocking().expect("flush");
    }
    std::thread::sleep(TICK_INTERVAL * 3);
    assert_eq!(segment_rows(&handle), [8, 8, 8], "never planned");
    let error = handle
        .compact_blocking()
        .expect_err("the job needs more than the pool");
    assert!(matches!(error, LogPoseError::TooLarge { .. }), "{error:?}");
    assert_eq!(segment_rows(&handle), [8, 8, 8]);
    assert!(engine.scheduler().stats().declined >= 1);
    assert_eq!(live(&handle).len(), 24);
}

/// An explicit compaction waits for the background compactions to end, then merges every
/// segment one job can hold.
#[test]
fn an_explicit_compaction_waits_for_background_ones_then_merges_everything() {
    let fault = FaultVfs::new(7);
    let engine = open(
        &fault,
        EngineConfig {
            compaction: tiny_tiers(),
            ..config()
        },
    );
    let scheduler = engine.scheduler().clone();
    let handle = create(&engine, usize::MAX, 2);
    for index in 0..5 {
        write(&handle, vec![upsert(&format!("k{index}"), 1.0)]);
        handle.flush_blocking().expect("flush");
    }
    scheduler.pause();
    // Whatever the policy planned waits; the explicit compaction waits behind it.
    let compacted = {
        let handle = Arc::clone(&handle);
        std::thread::spawn(move || handle.compact_blocking())
    };
    std::thread::sleep(TICK_INTERVAL * 2);
    assert!(!compacted.is_finished());
    scheduler.resume();
    compacted
        .join()
        .expect("compaction thread")
        .expect("the explicit compaction commits");
    assert_eq!(segment_rows(&handle), [5]);
    assert_eq!(live(&handle).len(), 5);
}

/// Background flushes and compactions run beside concurrent upserts, updates, and deletes from
/// several clients (each on its own keys, so each client's model is exact), and readers that
/// check every version they see: no row is lost or resurrected, and every version keeps one
/// live row per key.
#[test]
fn background_compaction_beside_writes_deletes_and_flushes_loses_nothing() {
    let fault = FaultVfs::new(8);
    let engine = open(
        &fault,
        EngineConfig {
            compaction: tiny_tiers(),
            ..config()
        },
    );
    let handle = create(&engine, 3, 2);
    let stop = Arc::new(AtomicBool::new(false));
    let readers = (0..2)
        .map(|_| {
            let handle = Arc::clone(&handle);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut checks = 0_u64;
                let mut last = 0;
                while !stop.load(Ordering::Acquire) {
                    let version = handle.current();
                    version
                        .check_invariants()
                        .expect("every version is consistent");
                    assert!(version.visible_seq_no >= last, "visibility went back");
                    last = version.visible_seq_no;
                    checks += 1;
                }
                checks
            })
        })
        .collect::<Vec<_>>();
    let clients = (0..3)
        .map(|client| {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(100 + client);
                let mut model = BTreeMap::new();
                for step in 0..150 {
                    let id = format!("c{client}-{}", rng.random_range(0..12));
                    let x = step as f32;
                    let op = match rng.random_range(0..10) {
                        0..=5 => {
                            model.insert(id.clone(), x);
                            upsert(&id, x)
                        }
                        6..=7 if model.contains_key(&id) => {
                            model.insert(id.clone(), x);
                            let mut update = PartialUpdate::new(id.as_str());
                            update.vectors.insert("vector".to_owned(), vec![x, 1.0]);
                            ClientOp::Update(update)
                        }
                        _ => {
                            model.remove(&id);
                            delete(&id)
                        }
                    };
                    handle.write_blocking(vec![op]).expect("write");
                }
                model
            })
        })
        .collect::<Vec<_>>();
    let mut expected = BTreeMap::new();
    for client in clients {
        expected.extend(client.join().expect("client thread"));
    }
    stop.store(true, Ordering::Release);
    for reader in readers {
        assert!(reader.join().expect("reader thread") > 0);
    }
    wait_idle(&engine, &handle);
    assert_eq!(live(&handle), expected);
    let stats = engine.scheduler().stats();
    assert!(stats.flushes_granted > 10, "{stats:?}");
    assert!(stats.compactions_granted > 3, "{stats:?}");

    drop(handle);
    drop(engine);
    fault.crash();
    let engine = open(&fault, config());
    assert_eq!(live(&reopen(&engine)), expected);
}

/// After N flushes of R rows with tiers of `base_rows = R`, compactions rewrote each row about
/// once per tier it climbed: at most `tiers * N * R` rows, as the policy predicts.
#[test]
fn write_amplification_stays_within_one_rewrite_per_tier() {
    let fault = FaultVfs::new(9);
    let tiers = CompactionConfig {
        base_rows: 4,
        tier_ratio: 4,
        min_merge: 4,
        max_merge: 4,
        ..CompactionConfig::default()
    };
    let engine = open(
        &fault,
        EngineConfig {
            compaction: tiers,
            ..config()
        },
    );
    let handle = create(&engine, usize::MAX, 4);
    let flushes = 64_u64;
    let rows = 4_u64;
    for index in 0..flushes {
        let ops = (0..rows)
            .map(|row| upsert(&format!("k{index}-{row}"), 1.0))
            .collect();
        write(&handle, ops);
        handle.flush_blocking().expect("flush");
    }
    wait_idle(&engine, &handle);
    let ingested = flushes * rows;
    let written = handle.maintenance_written();
    assert_eq!(written.flush_rows, ingested);
    // 4 -> 16 -> 64 -> 256: three rewrites per row, log4(256 / 4).
    assert_eq!(written.compaction_rows, 3 * ingested, "{written:?}");
    assert_eq!(segment_rows(&handle), [256]);
    assert!(written.compaction_bytes > 0);
}

/// Dropping a collection releases everything it holds at the scheduler: the permit and the
/// maintenance memory of a compaction granted just before the drop (the drop waits for it to
/// end), and the request of a flush still waiting for a permit, so other collections' jobs are
/// never held back by a dropped one.
#[test]
fn a_dropped_collection_releases_its_permits_and_maintenance_memory() {
    let fault = FaultVfs::new(10);
    let engine = open(
        &fault,
        EngineConfig {
            compaction: tiny_tiers(),
            ..config()
        },
    );
    let scheduler = engine.scheduler().clone();
    let handle = create(&engine, usize::MAX, 2);
    scheduler.pause();
    for id in ["a", "b"] {
        write(&handle, vec![upsert(id, 1.0)]);
        let flushed = {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || handle.flush_blocking())
        };
        wait_for("the flush to ask", || scheduler.stats().waiting == 1);
        scheduler.step(1);
        flushed
            .join()
            .expect("flush thread")
            .expect("flush should commit");
    }
    wait_for("the compaction to ask", || {
        handle.maintenance_status().pending == ["compact"]
    });
    scheduler.step(1);
    // A frozen memtable whose flush waits for a permit the paused scheduler never grants.
    write(&handle, vec![upsert("c", 1.0)]);
    let flushed = {
        let handle = Arc::clone(&handle);
        std::thread::spawn(move || handle.flush_blocking())
    };
    wait_for("the flush to ask", || {
        handle.maintenance_status().pending == ["flush"]
    });
    assert_eq!(scheduler.stats().compactions_granted, 1);

    engine
        .drop_collection_blocking(&CollectionRef::new_default(NAME))
        .expect("drop");
    assert!(
        flushed.join().expect("flush thread").is_err(),
        "the explicit flush fails with the drop"
    );
    let stats = scheduler.stats();
    assert_eq!(
        (stats.waiting, stats.running, stats.reserved_bytes),
        (0, 0, 0),
        "{stats:?}"
    );
    scheduler.resume();
}

/// Engine drop answers every request a writer holds: a write held back by a stall, an
/// explicit flush waiting for a permit the paused scheduler never grants, and the permit
/// requests themselves; none of them is applied, and the drop returns.
#[test]
fn engine_drop_answers_stalled_writes_and_waiting_maintenance() {
    let fault = FaultVfs::new(11);
    let engine = open(
        &fault,
        EngineConfig {
            memtable: MemtableConfig {
                max_frozen: 1,
                ..MemtableConfig::default()
            },
            ..config()
        },
    );
    let scheduler = engine.scheduler().clone();
    let handle = create(&engine, 2, usize::MAX);
    scheduler.pause();
    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    wait_for("the freeze", || handle.current().frozen.len() == 1);
    write(&handle, vec![upsert("c", 3.0), upsert("d", 4.0)]);
    let stalled = (0..3)
        .map(|index| {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || {
                handle.write_blocking(vec![upsert(&format!("late{index}"), 5.0)])
            })
        })
        .collect::<Vec<_>>();
    let flushed = {
        let handle = Arc::clone(&handle);
        std::thread::spawn(move || handle.flush_blocking())
    };
    std::thread::sleep(TICK_INTERVAL * 3);
    assert!(stalled.iter().all(|thread| !thread.is_finished()));
    assert!(!flushed.is_finished());
    assert_eq!(
        scheduler.stats().waiting,
        1,
        "the flush waits for its permit"
    );

    let dropped = std::thread::spawn(move || {
        drop(handle);
        drop(engine);
    });
    wait_for("the engine drop", || dropped.is_finished());
    dropped.join().expect("drop thread");
    wait_for("every waiting call to be answered", || {
        stalled.iter().all(std::thread::JoinHandle::is_finished) && flushed.is_finished()
    });
    for thread in stalled {
        let error = thread
            .join()
            .expect("writer thread")
            .expect_err("a write held by the stall is refused at shutdown");
        assert!(
            matches!(error, LogPoseError::Unavailable { .. }),
            "{error:?}"
        );
    }
    flushed
        .join()
        .expect("flush thread")
        .expect_err("the explicit flush is refused at shutdown");
    assert_eq!(scheduler.stats().waiting, 0);

    fault.crash();
    let engine = open(&fault, config());
    let rows = live(&reopen(&engine));
    assert_eq!(rows.len(), 4, "no stalled write was applied: {rows:?}");
}

/// Concurrent clients, several compactions of one collection at once, and crashes: a model of
/// every key that each client owns (so the model is exact under concurrency) and that crashes
/// widen only for writes that failed.
mod concurrent {
    use super::*;
    use std::sync::{Mutex, atomic::AtomicU64};

    /// The values a key may hold: one after an acknowledged write, and the old and new ones
    /// after a write that failed during a crash window, until recovery shows which.
    type Possible = Vec<Option<f32>>;

    /// One client's keys, locked while the client writes so that a check between its writes
    /// sees every write it had acknowledged, and none in flight.
    #[derive(Default)]
    pub(super) struct Client {
        keys: Mutex<BTreeMap<String, Possible>>,
    }

    /// Writes refused with `WriteStalled`, which must never be applied.
    pub(super) static STALLED: AtomicU64 = AtomicU64::new(0);

    /// Three compactions of one collection may run beside its flush; tiers of two rows and
    /// merges of two or three, so compactions run all the time; and a short stall timeout, so
    /// writes that a crashed disk stalls fail quickly.
    fn engine_config(threads: usize) -> EngineConfig {
        EngineConfig {
            runtime: RuntimeConfig {
                maintenance_threads: threads,
                writer_threads: 2,
                ..config().runtime
            },
            memtable: MemtableConfig {
                max_frozen: 2,
                write_stall_timeout: Duration::from_millis(200),
                ..MemtableConfig::default()
            },
            compaction: CompactionConfig {
                base_rows: 2,
                tier_ratio: 2,
                min_merge: 2,
                max_merge: 3,
                deleted_ratio: 0.3,
                max_jobs_per_collection: 3,
                ..CompactionConfig::default()
            },
            // Every flush and compaction builds SQ8 codes and a graph, so jobs build graphs
            // concurrently and crashes land in index builds.
            index: crate::IndexPolicy {
                graph_min_rows: 4,
                sq8_min_rows: 2,
                ..crate::IndexPolicy::default()
            },
            ..config()
        }
    }

    fn client_rows(
        handle: &CollectionHandle,
        client: usize,
        context: &str,
    ) -> BTreeMap<String, f32> {
        let prefix = format!("c{client}-");
        let version = handle.current();
        version
            .check_invariants()
            .map_err(|error| format!("{context}: {error}"))
            .expect("invariants hold");
        version
            .live_images()
            .into_iter()
            .map(|(_, image)| {
                let (id, vector, _) = flat_row(&version.schema, &image);
                (id, vector[0])
            })
            .filter(|(id, _)| id.starts_with(&prefix))
            .collect()
    }

    /// Check `client`'s keys against the model; with `settle`, pin each to what it holds.
    pub(super) fn check(
        handle: &CollectionHandle,
        client: usize,
        state: &Client,
        settle: bool,
        context: &str,
    ) {
        let mut keys = state.keys.lock().expect("model lock");
        let rows = client_rows(handle, client, context);
        for (key, possible) in keys.iter_mut() {
            let actual = rows.get(key).copied();
            assert!(
                possible.contains(&actual),
                "{context}: key {key} holds {actual:?}, expected one of {possible:?}"
            );
            if settle {
                *possible = vec![actual];
            }
        }
        for key in rows.keys() {
            assert!(keys.contains_key(key), "{context}: unknown key {key}");
        }
    }

    /// Each client writes `steps` random batches of upserts, partial updates, and deletes over
    /// its own ten keys. Outside a crash window every write must succeed or be refused with
    /// `WriteStalled` (which leaves the model alone); inside one, any failure is uncertain.
    pub(super) fn run_clients(
        handle: &Arc<CollectionHandle>,
        clients: &Arc<Vec<Client>>,
        seed: u64,
        round: u64,
        steps: u64,
        crashing: bool,
    ) {
        let threads = (0..clients.len())
            .map(|client| {
                let handle = Arc::clone(handle);
                let clients = Arc::clone(clients);
                std::thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(
                        seed.wrapping_mul(1_000) + round * 10 + client as u64,
                    );
                    let state = &clients[client];
                    for step in 0..steps {
                        let mut keys = state.keys.lock().expect("model lock");
                        let mut ops = Vec::new();
                        let mut effects = Vec::<(String, Option<f32>)>::new();
                        for _ in 0..rng.random_range(1..=3) {
                            let id = format!("c{client}-{}", rng.random_range(0..10));
                            if effects.iter().any(|(key, _)| *key == id) {
                                continue;
                            }
                            // Every value is written once, so a stray write is recognizable.
                            let x = (round * 100_000 + step * 10 + ops.len() as u64) as f32;
                            let present = keys
                                .get(&id)
                                .is_some_and(|possible| possible.iter().all(Option::is_some));
                            match rng.random_range(0..10) {
                                0..=4 => {
                                    ops.push(upsert(&id, x));
                                    effects.push((id, Some(x)));
                                }
                                5..=6 if present => {
                                    let mut update = PartialUpdate::new(id.as_str());
                                    update.vectors.insert("vector".to_owned(), vec![x, 1.0]);
                                    ops.push(ClientOp::Update(update));
                                    effects.push((id, Some(x)));
                                }
                                _ => {
                                    ops.push(delete(&id));
                                    effects.push((id, None));
                                }
                            }
                        }
                        match handle.write_blocking(ops) {
                            Ok(_) => {
                                for (id, value) in effects {
                                    keys.insert(id, vec![value]);
                                }
                            }
                            Err(LogPoseError::WriteStalled { .. }) => {
                                STALLED.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(error) => {
                                assert!(
                                    crashing,
                                    "seed {seed}, round {round}, client {client}, step {step}: \
                                     {error}"
                                );
                                for (id, value) in effects {
                                    let possible = keys.entry(id).or_insert_with(|| vec![None]);
                                    if !possible.contains(&value) {
                                        possible.push(value);
                                    }
                                }
                            }
                        }
                        drop(keys);
                        if !crashing && step % 7 == 0 {
                            check(
                                &handle,
                                client,
                                state,
                                false,
                                &format!(
                                    "seed {seed}, round {round}, client {client}, step {step}"
                                ),
                            );
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().expect("client thread");
        }
    }

    /// One seed: four rounds of concurrent writes beside a reader and explicit flushes and
    /// compactions, each ending in a crash while writes and background jobs run.
    pub(super) fn run(seed: u64) -> u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let fault = FaultVfs::new(seed);
        let threads = rng.random_range(2..6);
        let mut engine = open(&fault, engine_config(threads));
        let mut handle = create(&engine, 3, 2);
        let clients = Arc::new((0..3).map(|_| Client::default()).collect::<Vec<_>>());
        let mut compactions = 0;
        for round in 0..4 {
            let stop = Arc::new(AtomicBool::new(false));
            let reader = {
                let handle = Arc::clone(&handle);
                let stop = Arc::clone(&stop);
                let scheduler = engine.scheduler().clone();
                std::thread::spawn(move || {
                    let mut last = 0;
                    while !stop.load(Ordering::Acquire) {
                        let version = handle.current();
                        version
                            .check_invariants()
                            .expect("every version is consistent");
                        assert!(version.visible_seq_no >= last, "visibility went back");
                        last = version.visible_seq_no;
                        let stats = scheduler.stats();
                        assert!(stats.reserved_bytes <= stats.pool_bytes, "{stats:?}");
                        std::thread::yield_now();
                    }
                })
            };
            let explicit = {
                let handle = Arc::clone(&handle);
                let mut rng = StdRng::seed_from_u64(rng.random());
                std::thread::spawn(move || {
                    for _ in 0..3 {
                        std::thread::sleep(Duration::from_millis(rng.random_range(0..30)));
                        if rng.random_bool(0.5) {
                            handle.flush_blocking().expect("explicit flush");
                        } else {
                            handle.compact_blocking().expect("explicit compaction");
                        }
                    }
                })
            };
            run_clients(&handle, &clients, seed, round, 60, false);
            explicit.join().expect("explicit thread");
            stop.store(true, Ordering::Release);
            reader.join().expect("reader thread");
            for (client, state) in clients.iter().enumerate() {
                check(
                    &handle,
                    client,
                    state,
                    false,
                    &format!("seed {seed}, round {round}"),
                );
            }
            compactions += engine.scheduler().stats().compactions_granted;

            let tear = TearMode::ALL[rng.random_range(0..TearMode::ALL.len())];
            let ops = rng.random_range(0..80);
            fault.set_plan(FaultPlan {
                crash_after_ops: Some(fault.mutating_ops() + ops),
                tear,
                ..FaultPlan::default()
            });
            run_clients(&handle, &clients, seed, round + 50, 15, true);
            std::thread::sleep(Duration::from_millis(rng.random_range(0..20)));
            drop(handle);
            drop(engine);
            fault.crash();
            fault.set_plan(FaultPlan::default());
            engine = open(&fault, engine_config(threads));
            handle = reopen(&engine);
            handle.arm_maintenance();
            for (client, state) in clients.iter().enumerate() {
                check(
                    &handle,
                    client,
                    state,
                    true,
                    &format!("seed {seed}, after crash {round} ({tear:?} after {ops} ops)"),
                );
            }
        }
        compactions += engine.scheduler().stats().compactions_granted;
        compactions
    }
}

/// Several compactions of one collection at once, beside its flushes, three clients writing
/// upserts, partial updates, and deletes, a reader checking every version, explicit flushes and
/// compactions, and crashes (every tear mode) while writes and jobs run. Every key holds what
/// its client's model allows after every round and every recovery: nothing acknowledged is
/// lost, nothing refused with `WriteStalled` is applied, and nothing else appears.
/// `LOGPOSE_CONCURRENT_COMPACTION_SEEDS` sets the number of seeds (default 6) and
/// `LOGPOSE_CONCURRENT_COMPACTION_FIRST_SEED` the first.
#[test]
fn concurrent_compactions_flushes_writes_and_crashes_match_the_model() {
    let count = std::env::var("LOGPOSE_CONCURRENT_COMPACTION_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(6);
    let first = std::env::var("LOGPOSE_CONCURRENT_COMPACTION_FIRST_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| rand::rng().random_range(0..1_000_000_u64));
    let mut compactions = 0;
    for seed in first..first + count {
        compactions += concurrent::run(seed);
    }
    assert!(
        compactions >= count,
        "compactions must run (first seed {first}): {compactions}"
    );
}
