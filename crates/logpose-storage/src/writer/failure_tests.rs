//! Maintenance failures at the writer: flushes that keep failing poison the collection so
//! writes fail fast instead of stalling forever, a failing compaction backs off on its own
//! (exponentially, reset by a success) without holding back flushes, and deleted rows of a
//! lone or small segment are reclaimed.

use super::*;
use crate::{
    CompactionConfig, CreateCollectionRequest, Engine, EngineConfig, ManualClock, MemtableConfig,
    RuntimeConfig, legacy_view::legacy_put, test_support::ControlledVfs,
};
use jobs::FLUSH_RETRY_BACKOFF;
use logpose_types::{
    CollectionRef, DistanceMetric, ErrorCode, Snapshot,
    record::{PrimaryKey, Record},
};
use logpose_vfs::{CrashPoint, FaultVfs};
use logpose_wal::BootId;
use std::{thread::JoinHandle, time::Instant};

const ROOT: &str = "/storage";
const NAME: &str = "failing";

fn config(clock: &Arc<ManualClock>) -> EngineConfig {
    EngineConfig {
        boot_id: Some(BootId::new("boot")),
        runtime: RuntimeConfig {
            io_threads: 2,
            query_threads: 1,
            maintenance_threads: 1,
            writer_threads: 1,
            ..RuntimeConfig::default()
        },
        clock: Some(Arc::clone(clock) as Arc<dyn crate::Clock>),
        ..EngineConfig::default()
    }
}

/// Open an engine over `vfs`, whose failures the test chooses.
fn open(vfs: &Arc<ControlledVfs>, config: EngineConfig) -> Engine {
    Engine::open(vfs.clone(), ROOT, config).expect("engine should open")
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
        .create_collection(descriptor, None)
        .expect("collection should be created")
}

fn upsert(id: &str, x: f32) -> ClientOp {
    ClientOp::Upsert(Record::new(id).with_vector("vector", vec![x, 1.0]))
}

fn delete(id: &str) -> ClientOp {
    ClientOp::Delete(PrimaryKey::from(id))
}

fn write(handle: &CollectionHandle, ops: Vec<ClientOp>) {
    handle.write_blocking(ops).expect("write should commit");
}

/// Write `ops` on a thread of its own, for a write that may stall.
fn spawn_write(
    handle: &Arc<CollectionHandle>,
    ops: Vec<ClientOp>,
) -> JoinHandle<Result<CommitAck>> {
    let handle = Arc::clone(handle);
    std::thread::spawn(move || handle.write_blocking(ops))
}

/// The live rows' ids, after checking the version's invariants.
fn live(handle: &CollectionHandle) -> Vec<String> {
    let version = handle.current();
    version.check_invariants().expect("invariants hold");
    let mut ids = version
        .live_images()
        .into_iter()
        .map(|(_, image)| {
            let put = legacy_put(&version.schema, &image).expect("row should read");
            put.id.as_str().to_owned()
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn segment_rows(handle: &CollectionHandle) -> Vec<u32> {
    handle
        .current()
        .segments
        .iter()
        .map(|segment| segment.row_count())
        .collect()
}

/// Wait for `done`, failing the test (instead of hanging it) past a deadline.
fn wait_for(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Join `thread` within the deadline.
fn join<T>(what: &str, thread: JoinHandle<T>) -> T {
    wait_for(what, || thread.is_finished());
    thread.join().expect("the thread should not panic")
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
    wait_for("maintenance to settle", || {
        settled() && {
            std::thread::sleep(TICK_INTERVAL * 2);
            settled()
        }
    });
}

/// Failures in a row of the job kind `job` that the maintenance status reports last.
fn failures(handle: &CollectionHandle, job: &str) -> u32 {
    handle
        .maintenance_status()
        .last_error
        .filter(|error| error.job == job)
        .map_or(0, |error| error.consecutive_failures)
}

fn assert_poisoned(error: &LogPoseError) {
    assert!(
        matches!(error, LogPoseError::CollectionPoisoned { .. }),
        "{error:?}"
    );
    assert_eq!(error.code(), ErrorCode::FailedPrecondition);
    assert_eq!(
        error.retry_after(),
        None,
        "a poisoned collection is not retried"
    );
}

/// A device that takes WAL writes but no segment: every flush fails at its segment's sync.
/// Writes first stall (and time out with `WriteStalled`) while the flush is retried; once
/// `max_flush_failures` flushes failed in a row the collection is poisoned, so the stalled write
/// and every later one fail at once with `CollectionPoisoned`. Reads keep serving every
/// acknowledged row, and the stats report the failure.
#[test]
fn flushes_that_keep_failing_poison_the_collection_so_writes_fail_fast() {
    let fault = FaultVfs::new(1);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let engine = open(
        &vfs,
        EngineConfig {
            memtable: MemtableConfig {
                max_frozen: 1,
                write_stall_timeout: Duration::from_secs(5),
                max_flush_failures: 3,
                ..MemtableConfig::default()
            },
            ..config(&clock)
        },
    );
    let handle = create(&engine, 2, usize::MAX);
    vfs.fail_file_syncs_containing(".seg", u32::MAX);

    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    wait_for("the first failed flush", || failures(&handle, "flush") == 1);
    assert!(!handle.is_poisoned());
    // The active memtable reaches its trigger with one frozen: the next write stalls.
    write(&handle, vec![upsert("c", 3.0), upsert("d", 4.0)]);
    let stalled = spawn_write(&handle, vec![upsert("e", 5.0)]);
    std::thread::sleep(TICK_INTERVAL * 3);
    assert!(!stalled.is_finished(), "the write must wait for a flush");
    // Past the stall timeout it fails with `WriteStalled`: the flush may still recover.
    clock.advance(Duration::from_secs(5));
    let error = join("the stalled write", stalled).expect_err("the stall outlasted the timeout");
    assert!(
        matches!(error, LogPoseError::WriteStalled { .. }),
        "{error:?}"
    );
    // The flush backoff passed too, and the retry fails again.
    wait_for("the second failed flush", || {
        failures(&handle, "flush") == 2
    });
    assert!(!handle.is_poisoned());

    // A write stalled when the last allowed failure lands fails at once, well before its
    // timeout.
    let stalled = spawn_write(&handle, vec![upsert("f", 6.0)]);
    std::thread::sleep(TICK_INTERVAL * 3);
    assert!(!stalled.is_finished(), "the write must wait for a flush");
    clock.advance(FLUSH_RETRY_BACKOFF);
    let error = join("the stalled write", stalled).expect_err("the collection is poisoned");
    assert_poisoned(&error);
    assert!(handle.is_poisoned());
    assert_eq!(failures(&handle, "flush"), 3);

    // Later writes and explicit flushes fail fast too; no clock movement is needed.
    let started = Instant::now();
    let error = handle
        .write_blocking(vec![upsert("g", 7.0)])
        .expect_err("a poisoned collection refuses writes");
    assert_poisoned(&error);
    assert_poisoned(&handle.flush_blocking().expect_err("and flushes"));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "they never stall"
    );

    // Reads keep serving every acknowledged row.
    assert_eq!(live(&handle), ["a", "b", "c", "d"]);
    let records = engine
        .core()
        .scan_exact_internal(&handle, None::<Snapshot>, true, None)
        .expect("reads keep working");
    assert_eq!(records.len(), 4);

    // The stats report the failure that poisoned the collection.
    let stats = engine
        .core()
        .collection_stats(&handle, None::<Snapshot>)
        .expect("stats keep working");
    let error = stats.maintenance.last_error.expect("the last failure");
    assert_eq!(error.job, "flush");
    assert_eq!(error.consecutive_failures, 3);
    assert!(error.failed_at_unix_ms > 0);
    assert!(!error.message.is_empty());
    drop(engine);
}

/// A flush that fails because the device is full cannot succeed on a retry: the collection is
/// poisoned by the first failure.
#[test]
fn a_flush_on_a_full_device_poisons_the_collection_at_once() {
    let fault = FaultVfs::new(2);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let engine = open(&vfs, config(&clock));
    let handle = create(&engine, 2, usize::MAX);
    // Segment files cannot be created: ENOSPC.
    vfs.fail_creates_containing(".seg");

    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    wait_for("the collection to be poisoned", || handle.is_poisoned());
    assert_eq!(failures(&handle, "flush"), 1);
    let error = handle
        .write_blocking(vec![upsert("c", 3.0)])
        .expect_err("a poisoned collection refuses writes");
    assert_poisoned(&error);
    assert_eq!(live(&handle), ["a", "b"]);
}

/// Only failures in a row poison: a flush that succeeds resets the count and clears the error,
/// so round after round of fewer than `max_flush_failures` failures never poisons.
#[test]
fn a_flush_that_succeeds_resets_the_failures_in_a_row() {
    let fault = FaultVfs::new(6);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let engine = open(
        &vfs,
        EngineConfig {
            memtable: MemtableConfig {
                max_flush_failures: 3,
                ..MemtableConfig::default()
            },
            ..config(&clock)
        },
    );
    let handle = create(&engine, 2, usize::MAX);
    for round in 0..3_u64 {
        // The next two flushes fail at their segment's sync; the third succeeds.
        vfs.fail_file_syncs_containing(".seg", 2);
        let (first, second) = (format!("{round}a"), format!("{round}b"));
        write(&handle, vec![upsert(&first, 1.0), upsert(&second, 2.0)]);
        wait_for("the first failed flush", || failures(&handle, "flush") == 1);
        clock.advance(FLUSH_RETRY_BACKOFF);
        wait_for("the second failed flush", || {
            failures(&handle, "flush") == 2
        });
        assert!(!handle.is_poisoned(), "round {round}");
        clock.advance(FLUSH_RETRY_BACKOFF);
        wait_for("the flush to succeed", || {
            handle.current().checkpoint_seq_no == 2 * (round + 1)
        });
        wait_idle(&engine, &handle);
        assert_eq!(
            handle.maintenance_status().last_error,
            None,
            "round {round}"
        );
    }
    assert!(!handle.is_poisoned());
    assert_eq!(live(&handle).len(), 6);
}

/// A collection poisoned by failing flushes keeps every acknowledged write in its WAL: after a
/// reopen in the same process it holds exactly the acknowledged rows, none of the refused
/// ones, and flushes and writes work again.
#[test]
fn a_reopen_after_poisoning_recovers_exactly_the_acknowledged_writes() {
    let fault = FaultVfs::new(7);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let config = EngineConfig {
        memtable: MemtableConfig {
            max_flush_failures: 2,
            ..MemtableConfig::default()
        },
        ..config(&clock)
    };
    let engine = open(&vfs, config.clone());
    let handle = create(&engine, 2, usize::MAX);
    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    wait_for("the first flush", || {
        handle.current().checkpoint_seq_no == 2
    });

    vfs.fail_file_syncs_containing(".seg", u32::MAX);
    write(&handle, vec![upsert("c", 3.0), delete("a")]);
    wait_for("the first failed flush", || failures(&handle, "flush") == 1);
    // Acknowledged into the active memtable while the frozen one waits.
    write(&handle, vec![upsert("d", 4.0)]);
    clock.advance(FLUSH_RETRY_BACKOFF);
    wait_for("the collection to be poisoned", || handle.is_poisoned());
    for ops in [vec![upsert("e", 5.0)], vec![delete("b")]] {
        assert_poisoned(
            &handle
                .write_blocking(ops)
                .expect_err("a poisoned collection refuses writes"),
        );
    }
    assert_eq!(live(&handle), ["b", "c", "d"]);
    drop(handle);
    drop(engine);

    vfs.fail_file_syncs_containing(".seg", 0);
    let engine = open(&vfs, config);
    let handle = engine
        .collection(&CollectionRef::new_default(NAME))
        .expect("the collection reopens");
    assert!(!handle.is_poisoned());
    assert_eq!(live(&handle), ["b", "c", "d"]);
    handle.flush_blocking().expect("flushes work again");
    write(&handle, vec![upsert("e", 5.0)]);
    assert_eq!(live(&handle), ["b", "c", "d", "e"]);
}

/// A job that fails only because the collection is already poisoned does not replace the
/// error that poisoned it: a compaction that ends after the poisoning is refused, and the stats
/// keep reporting the flush failures.
#[test]
fn a_job_refused_by_a_poisoned_collection_keeps_the_error_that_poisoned_it() {
    let fault = FaultVfs::new(8);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let engine = open(
        &vfs,
        EngineConfig {
            memtable: MemtableConfig {
                max_flush_failures: 2,
                ..MemtableConfig::default()
            },
            ..config(&clock)
        },
    );
    let handle = create(&engine, 2, usize::MAX);
    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    write(&handle, vec![upsert("c", 3.0), upsert("d", 4.0)]);
    wait_idle(&engine, &handle);
    assert_eq!(segment_rows(&handle), [2, 2]);

    // A compaction, stepped by hand, builds before the device fails and commits after.
    let (mut ticket, start) = handle
        .begin_job(JobKind::Compact)
        .expect("compaction begins");
    let JobWork::Compact(work) = &start.work else {
        unreachable!("two segments to compact");
    };
    let commit = engine
        .core()
        .build_compaction(&handle, &start.version, start.unit, work, &mut ticket)
        .expect("the compaction builds");

    vfs.fail_file_syncs_containing(".seg", u32::MAX);
    write(&handle, vec![upsert("e", 5.0), upsert("f", 6.0)]);
    wait_for("the first failed flush", || failures(&handle, "flush") == 1);
    clock.advance(FLUSH_RETRY_BACKOFF);
    wait_for("the collection to be poisoned", || handle.is_poisoned());

    assert_poisoned(
        &ticket
            .commit(commit)
            .expect_err("a poisoned collection refuses the commit"),
    );
    drop(start);
    // The writer answers the commit before it ends the job.
    wait_for("the compaction to end", || {
        handle.maintenance_status().in_progress.is_none()
    });
    let error = handle
        .maintenance_status()
        .last_error
        .expect("the flush failure");
    assert_eq!(error.job, "flush");
    assert_eq!(error.consecutive_failures, 2);
}

/// A compaction that keeps failing is retried after a backoff of its own, which doubles with
/// each failure in a row and starts over after a success, and it never delays a flush: the
/// clock stays inside the compaction's backoff while a flush runs.
#[test]
fn a_failing_compaction_backs_off_on_its_own_and_never_delays_a_flush() {
    let fault = FaultVfs::new(3);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let engine = open(
        &vfs,
        EngineConfig {
            // Tiers of 2, 8, 32, ... live rows, merged two at a time.
            compaction: CompactionConfig {
                base_rows: 2,
                tier_ratio: 4,
                min_merge: 2,
                max_merge: 4,
                ..CompactionConfig::default()
            },
            // Fewer than the compaction failures below: they must not count.
            memtable: MemtableConfig {
                max_flush_failures: 2,
                ..MemtableConfig::default()
            },
            ..config(&clock)
        },
    );
    let handle = create(&engine, 2, 2);
    vfs.fail_crash_point(Some(CrashPoint::CompactionAfterOutputSync));

    // Two flushes make two tier-1 segments, and their merge fails.
    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    write(&handle, vec![upsert("c", 3.0), upsert("d", 4.0)]);
    wait_for("the first failed compaction", || {
        failures(&handle, "compact") == 1
    });
    // Inside the compaction's backoff, the next flush still runs.
    write(&handle, vec![upsert("e", 5.0), upsert("f", 6.0)]);
    wait_for("a flush during the compaction backoff", || {
        segment_rows(&handle) == [2, 2, 2] && handle.current().frozen.is_empty()
    });
    assert_eq!(handle.current().checkpoint_seq_no, 6);
    assert_eq!(
        failures(&handle, "compact"),
        1,
        "no retry within the backoff"
    );

    // The backoff doubles: 1 s after the first failure, then 2 s, then 4 s.
    clock.advance(Duration::from_secs(1));
    wait_for("the second failure", || failures(&handle, "compact") == 2);
    clock.advance(Duration::from_secs(1));
    std::thread::sleep(TICK_INTERVAL * 3);
    assert_eq!(
        failures(&handle, "compact"),
        2,
        "the second failure waits 2 s"
    );
    clock.advance(Duration::from_secs(1));
    wait_for("the third failure", || failures(&handle, "compact") == 3);
    clock.advance(Duration::from_secs(3));
    std::thread::sleep(TICK_INTERVAL * 3);
    assert_eq!(
        failures(&handle, "compact"),
        3,
        "the third failure waits 4 s"
    );
    assert!(!handle.is_poisoned(), "compaction failures never poison");

    // Once the device recovers, the retry succeeds and clears the error.
    vfs.fail_crash_point(None);
    clock.advance(Duration::from_secs(1));
    wait_for("the compaction to succeed", || segment_rows(&handle) == [6]);
    wait_idle(&engine, &handle);
    assert_eq!(handle.maintenance_status().last_error, None);

    // The backoff starts over: the next failure is the first in a row, retried after 1 s.
    vfs.fail_crash_point(Some(CrashPoint::CompactionAfterOutputSync));
    write(&handle, vec![upsert("g", 7.0), upsert("h", 8.0)]);
    wait_for("a new failed compaction", || {
        failures(&handle, "compact") == 1
    });
    clock.advance(Duration::from_secs(1));
    wait_for("its retry after 1 s", || failures(&handle, "compact") == 2);
    assert_eq!(live(&handle).len(), 8);
}

/// A flush that failed leaves its memtable frozen, and the background retry waits for the
/// flush backoff on the engine clock. `Engine::tick_writer` runs the writer's tick at once: one
/// just short of the backoff plans nothing, and the first one past it has requested the retry
/// by the time it returns, whichever real-time tick would have come first.
#[test]
fn a_tick_past_the_flush_backoff_requests_the_retry_before_it_returns() {
    let fault = FaultVfs::new(8);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let engine = open(&vfs, config(&clock));
    // No flush trigger: only the explicit flush and its retry flush.
    let handle = create(&engine, usize::MAX, usize::MAX);
    write(&handle, vec![upsert("a", 1.0), upsert("b", 2.0)]);
    vfs.fail_file_syncs_containing(".seg", 1);
    handle
        .flush_blocking()
        .expect_err("the flush fails at its segment's sync");
    assert_eq!(failures(&handle, "flush"), 1);
    assert_eq!(
        handle.current().frozen.len(),
        1,
        "the memtable stays frozen"
    );
    let granted = engine.scheduler().stats().flushes_granted;

    clock.advance(FLUSH_RETRY_BACKOFF - Duration::from_millis(1));
    engine
        .tick_writer(&handle, Duration::from_secs(30))
        .expect("the writer ticks");
    std::thread::sleep(TICK_INTERVAL * 2);
    assert_eq!(
        engine.scheduler().stats().flushes_granted,
        granted,
        "no retry inside the backoff"
    );
    assert_eq!(handle.current().frozen.len(), 1);

    clock.advance(Duration::from_millis(1));
    engine
        .tick_writer(&handle, Duration::from_secs(30))
        .expect("the writer ticks");
    assert_eq!(
        engine.scheduler().stats().flushes_granted,
        granted + 1,
        "the retry was granted before the tick returned"
    );
    wait_for("the retry to commit", || {
        handle.current().frozen.is_empty() && engine.scheduler().stats().running == 0
    });
    assert_eq!(segment_rows(&handle), [2]);
    assert_eq!(handle.maintenance_status().last_error, None);
    assert_eq!(live(&handle), ["a", "b"]);
}

/// An explicit compaction of a collection with one segment rewrites it when it has deleted
/// rows, so they are reclaimed; with none left, it changes nothing.
#[test]
fn an_explicit_compaction_reclaims_the_deleted_rows_of_a_single_segment() {
    let fault = FaultVfs::new(4);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let engine = open(&vfs, config(&clock));
    let handle = create(&engine, usize::MAX, usize::MAX);
    write(
        &handle,
        vec![upsert("a", 1.0), upsert("b", 2.0), upsert("c", 3.0)],
    );
    handle.flush_blocking().expect("flush");
    write(&handle, vec![delete("b")]);
    assert_eq!(segment_rows(&handle), [3]);

    handle.compact_blocking().expect("compaction");
    assert_eq!(segment_rows(&handle), [2], "the deleted row is gone");
    assert_eq!(live(&handle), ["a", "c"]);
    assert_eq!(handle.maintenance_written().compaction_rows, 2);

    let unit = handle.current().segments[0].unit;
    handle.compact_blocking().expect("compaction");
    assert_eq!(
        handle.current().segments[0].unit,
        unit,
        "a segment without deleted rows is left alone"
    );
    assert_eq!(handle.maintenance_written().compaction_rows, 2);
}

/// In the background, a segment below `base_rows` is rewritten for its deletions alone once
/// `small_deleted_rows` rows and `small_deleted_ratio` of it are deleted, and not before.
#[test]
fn a_small_segment_is_rewritten_in_the_background_once_half_deleted_and_past_the_floor() {
    let fault = FaultVfs::new(5);
    let vfs = ControlledVfs::wrap(fault.process());
    let clock = Arc::new(ManualClock::new());
    let engine = open(
        &vfs,
        EngineConfig {
            compaction: CompactionConfig {
                small_deleted_rows: 3,
                ..CompactionConfig::default()
            },
            ..config(&clock)
        },
    );
    // Flushes only on request; background compaction merges four segments of a tier.
    let handle = create(&engine, usize::MAX, 4);
    let ids = ["a", "b", "c", "d", "e", "f"];
    write(
        &handle,
        ids.iter()
            .enumerate()
            .map(|(x, id)| upsert(id, x as f32))
            .collect(),
    );
    handle.flush_blocking().expect("flush");
    assert_eq!(segment_rows(&handle), [6]);

    // Two of six deleted: below the floor and the ratio.
    write(&handle, vec![delete("a"), delete("b")]);
    wait_idle(&engine, &handle);
    assert_eq!(segment_rows(&handle), [6], "too few deleted rows");
    // Three of six: at the floor and at half.
    write(&handle, vec![delete("c")]);
    wait_for("the rewrite", || segment_rows(&handle) == [3]);
    wait_idle(&engine, &handle);
    assert_eq!(live(&handle), ["d", "e", "f"]);
    assert_eq!(handle.maintenance_written().compaction_rows, 3);
}

/// Flush backoff is fixed; compaction backoff doubles from 1 s up to 60 s, and a success
/// resets it.
#[test]
fn compaction_backoff_doubles_up_to_its_cap_and_resets_on_success() {
    let mut backoff = jobs::Backoff::compaction();
    let at = Duration::from_secs(100);
    assert!(backoff.ready(at));
    let waits = (1..=9)
        .map(|failure| {
            assert_eq!(backoff.failed(at), failure);
            backoff.retry_at().map(|retry| retry - at)
        })
        .collect::<Vec<_>>();
    let secs = |secs| Some(Duration::from_secs(secs));
    assert_eq!(
        waits,
        [
            secs(1),
            secs(2),
            secs(4),
            secs(8),
            secs(16),
            secs(32),
            secs(60),
            secs(60),
            secs(60)
        ]
    );
    assert!(!backoff.ready(at + Duration::from_secs(59)));
    assert!(backoff.ready(at + Duration::from_secs(60)));
    backoff.succeeded();
    assert!(backoff.ready(at));
    assert_eq!(backoff.failed(at), 1);
    assert_eq!(backoff.retry_at(), secs(101));

    let mut flush = jobs::Backoff::flush();
    for failure in 1..=4 {
        assert_eq!(flush.failed(at), failure);
        assert_eq!(flush.retry_at(), Some(at + FLUSH_RETRY_BACKOFF));
    }
    // Far more failures than a u32 shift allows stay capped.
    for _ in 0..100 {
        backoff.failed(at);
    }
    assert_eq!(backoff.retry_at(), secs(160));
}
