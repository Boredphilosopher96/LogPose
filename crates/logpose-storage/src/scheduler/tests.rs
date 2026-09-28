//! Scheduler unit tests: priorities, slots, and the maintenance-memory reservation.

use super::*;
use std::sync::mpsc;

/// A request whose permit lands in a channel, labelled for the order checks.
fn ask(
    scheduler: &MaintenanceScheduler,
    kind: JobKind,
    bytes: u64,
    label: &'static str,
    granted: &mpsc::Sender<(&'static str, Permit)>,
) -> Result<RequestId> {
    let granted = granted.clone();
    scheduler.request(kind, bytes, move |permit| {
        let _ = granted.send((label, permit));
    })
}

fn drain(received: &mpsc::Receiver<(&'static str, Permit)>) -> Vec<(&'static str, Permit)> {
    received.try_iter().collect()
}

fn labels(permits: &[(&'static str, Permit)]) -> Vec<&'static str> {
    permits.iter().map(|(label, _)| *label).collect()
}

#[test]
fn a_waiting_flush_is_granted_before_compactions_that_asked_earlier() {
    let scheduler = MaintenanceScheduler::new(2, 1000);
    let (granted, received) = mpsc::channel();
    scheduler.pause();
    ask(&scheduler, JobKind::Compact, 10, "compact-1", &granted).expect("request");
    ask(&scheduler, JobKind::Compact, 10, "compact-2", &granted).expect("request");
    ask(&scheduler, JobKind::Flush, 0, "flush", &granted).expect("request");
    assert!(drain(&received).is_empty(), "nothing while paused");

    scheduler.step(1);
    let first = drain(&received);
    assert_eq!(labels(&first), ["flush"]);
    scheduler.step(1);
    let second = drain(&received);
    assert_eq!(labels(&second), ["compact-1"]);
    assert_eq!(scheduler.stats().waiting, 1);
    assert_eq!(scheduler.stats().running, 2);
}

#[test]
fn compactions_never_take_the_slot_kept_for_flushes() {
    // Two slots: at most one compaction runs, so a flush always finds a slot.
    let scheduler = MaintenanceScheduler::new(2, 1000);
    let (granted, received) = mpsc::channel();
    ask(&scheduler, JobKind::Compact, 1, "compact-1", &granted).expect("request");
    ask(&scheduler, JobKind::Compact, 1, "compact-2", &granted).expect("request");
    let running = drain(&received);
    assert_eq!(
        labels(&running),
        ["compact-1"],
        "the second waits for the slot"
    );

    ask(&scheduler, JobKind::Flush, 0, "flush", &granted).expect("request");
    let flush = drain(&received);
    assert_eq!(
        labels(&flush),
        ["flush"],
        "the flush runs beside the compaction"
    );

    drop(running);
    let second = drain(&received);
    assert_eq!(labels(&second), ["compact-2"]);
    drop(flush);
    assert_eq!(scheduler.stats().running, 1);
    drop(second);
    assert_eq!(scheduler.stats().running, 0);
}

#[test]
fn a_slot_freed_while_both_wait_goes_to_the_flush() {
    let scheduler = MaintenanceScheduler::new(3, 1000);
    let (granted, received) = mpsc::channel();
    // Fill all four slots (three, plus the index-build slot, which flushes may use too): two
    // compactions and two flushes.
    ask(&scheduler, JobKind::Compact, 1, "compact-1", &granted).expect("request");
    ask(&scheduler, JobKind::Compact, 1, "compact-2", &granted).expect("request");
    ask(&scheduler, JobKind::Flush, 0, "flush-1", &granted).expect("request");
    ask(&scheduler, JobKind::Flush, 0, "flush-0", &granted).expect("request");
    let mut running = drain(&received);
    assert_eq!(
        labels(&running),
        ["compact-1", "compact-2", "flush-1", "flush-0"]
    );
    // A compaction, then a flush, wait.
    ask(&scheduler, JobKind::Compact, 1, "compact-3", &granted).expect("request");
    ask(&scheduler, JobKind::Flush, 0, "flush-2", &granted).expect("request");
    assert!(drain(&received).is_empty());

    // A compaction finishes: its slot goes to the flush that asked later.
    let finished = running.remove(0);
    drop(finished);
    assert_eq!(labels(&drain(&received)), ["flush-2"]);
}

#[test]
fn a_compaction_waits_until_its_memory_fits_and_never_overcommits_the_pool() {
    let scheduler = MaintenanceScheduler::new(4, 100);
    let (granted, received) = mpsc::channel();
    ask(&scheduler, JobKind::Compact, 60, "big", &granted).expect("request");
    ask(&scheduler, JobKind::Compact, 50, "waits", &granted).expect("request");
    ask(&scheduler, JobKind::Compact, 10, "small", &granted).expect("request");
    let big = drain(&received);
    assert_eq!(labels(&big), ["big"]);
    let stats = scheduler.stats();
    assert_eq!(stats.reserved_bytes, 60);
    assert_eq!(
        stats.waiting, 2,
        "the small one does not overtake the one that waits"
    );

    drop(big);
    let rest = drain(&received);
    assert_eq!(labels(&rest), ["waits", "small"]);
    let stats = scheduler.stats();
    assert_eq!(stats.reserved_bytes, 60);
    assert_eq!(stats.peak_reserved_bytes, 60);
    assert!(stats.peak_reserved_bytes <= stats.pool_bytes);
    drop(rest);
    assert_eq!(scheduler.stats().reserved_bytes, 0);
}

#[test]
fn a_compaction_larger_than_the_pool_is_declined() {
    let scheduler = MaintenanceScheduler::new(2, 100);
    let (granted, received) = mpsc::channel();
    let error = ask(&scheduler, JobKind::Compact, 101, "huge", &granted)
        .expect_err("more than the whole pool");
    assert!(
        matches!(
            error,
            LogPoseError::TooLarge {
                size: Some(101),
                limit: 100,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(scheduler.stats().declined, 1);
    assert!(drain(&received).is_empty());
    // A flush is never declined, whatever it reserves.
    ask(&scheduler, JobKind::Flush, 1_000_000, "flush", &granted).expect("request");
    let flush = drain(&received);
    assert_eq!(flush[0].1.bytes(), 1_000_000);
}

/// A flush reserves its build without waiting for the pool (it may overcommit it), and
/// compactions wait while it runs; compaction reservations alone never exceed the pool.
#[test]
fn a_running_flush_reservation_holds_back_compactions_but_never_waits() {
    let scheduler = MaintenanceScheduler::new(3, 100);
    let (granted, received) = mpsc::channel();
    ask(&scheduler, JobKind::Compact, 60, "compaction", &granted).expect("request");
    let compaction = drain(&received);
    ask(&scheduler, JobKind::Flush, 70, "flush", &granted).expect("request");
    let flush = drain(&received);
    assert_eq!(
        labels(&flush),
        ["flush"],
        "a flush never waits for the pool"
    );
    let stats = scheduler.stats();
    assert_eq!((stats.reserved_bytes, stats.flush_reserved_bytes), (60, 70));
    drop(compaction);
    ask(&scheduler, JobKind::Compact, 40, "waits", &granted).expect("request");
    assert!(drain(&received).is_empty(), "70 + 40 exceeds the pool");
    drop(flush);
    let waits = drain(&received);
    assert_eq!(labels(&waits), ["waits"]);
    let stats = scheduler.stats();
    assert_eq!((stats.reserved_bytes, stats.flush_reserved_bytes), (40, 0));
    assert!(stats.peak_reserved_bytes <= stats.pool_bytes);
}

#[test]
fn a_waiting_compaction_that_does_not_fit_never_blocks_a_flush() {
    let scheduler = MaintenanceScheduler::new(3, 100);
    let (granted, received) = mpsc::channel();
    ask(&scheduler, JobKind::Compact, 90, "holds", &granted).expect("request");
    let holds = drain(&received);
    ask(&scheduler, JobKind::Compact, 90, "waits", &granted).expect("request");
    ask(&scheduler, JobKind::Flush, 0, "flush", &granted).expect("request");
    assert_eq!(labels(&drain(&received)), ["flush"]);
    drop(holds);
    assert_eq!(labels(&drain(&received)), ["waits"]);
}

#[test]
fn a_cancelled_request_is_never_granted_and_an_undelivered_permit_is_released() {
    let scheduler = MaintenanceScheduler::new(2, 100);
    let (granted, received) = mpsc::channel();
    scheduler.pause();
    let id = ask(&scheduler, JobKind::Compact, 10, "cancelled", &granted).expect("request");
    assert!(scheduler.cancel(id));
    assert!(!scheduler.cancel(id), "already gone");
    scheduler.resume();
    assert!(drain(&received).is_empty());

    // A permit whose receiver is gone is dropped, which releases it.
    drop(received);
    ask(&scheduler, JobKind::Compact, 10, "undelivered", &granted).expect("request");
    let stats = scheduler.stats();
    assert_eq!(stats.running, 0);
    assert_eq!(stats.reserved_bytes, 0);
    assert_eq!(stats.compactions_granted, 1);
}

#[test]
fn one_thread_still_gets_a_flush_slot_a_compaction_slot_and_an_index_slot() {
    let scheduler = MaintenanceScheduler::new(1, 100);
    assert_eq!(scheduler.slots(), 3);
    let (granted, received) = mpsc::channel();
    ask(&scheduler, JobKind::Compact, 1, "compact", &granted).expect("request");
    ask(&scheduler, JobKind::Flush, 0, "flush", &granted).expect("request");
    ask(&scheduler, JobKind::Index, 1, "index", &granted).expect("request");
    assert_eq!(labels(&drain(&received)), ["compact", "flush", "index"]);
    assert_eq!(scheduler.stats().index_builds_granted, 1);
}

/// Index builds come last: a waiting compaction goes first even when the build asked earlier,
/// and one build at a time runs, in its own slot, so a long graph build never holds back a
/// compaction's slot.
#[test]
fn index_builds_wait_for_compactions_and_run_in_their_own_slot() {
    let scheduler = MaintenanceScheduler::new(2, 1000);
    let (granted, received) = mpsc::channel();
    scheduler.pause();
    ask(&scheduler, JobKind::Index, 10, "index-1", &granted).expect("request");
    ask(&scheduler, JobKind::Index, 10, "index-2", &granted).expect("request");
    ask(&scheduler, JobKind::Compact, 10, "compact-1", &granted).expect("request");
    scheduler.step(1);
    let compaction = drain(&received);
    assert_eq!(labels(&compaction), ["compact-1"]);
    scheduler.resume();
    let index = drain(&received);
    assert_eq!(labels(&index), ["index-1"], "one index build at a time");
    ask(&scheduler, JobKind::Compact, 10, "compact-2", &granted).expect("request");
    assert!(drain(&received).is_empty(), "the compaction slot is taken");
    // The compaction ends while the build runs: the next compaction takes its slot at once.
    drop(compaction);
    let compaction = drain(&received);
    assert_eq!(labels(&compaction), ["compact-2"]);
    assert_eq!(scheduler.stats().running, 2);
    drop(index);
    assert_eq!(labels(&drain(&received)), ["index-2"]);
    drop(compaction);
}

/// A waiting compaction holds back index builds even when it waits for memory, so builds never
/// starve it; an index build waits for its memory too, and one larger than the pool is declined.
#[test]
fn index_builds_reserve_maintenance_memory_after_compactions() {
    let scheduler = MaintenanceScheduler::new(4, 100);
    let (granted, received) = mpsc::channel();
    ask(&scheduler, JobKind::Compact, 60, "compact-1", &granted).expect("request");
    let first = drain(&received);
    ask(&scheduler, JobKind::Compact, 50, "compact-2", &granted).expect("request");
    ask(&scheduler, JobKind::Index, 10, "index", &granted).expect("request");
    assert!(
        drain(&received).is_empty(),
        "the index build waits behind the compaction that waits for memory"
    );
    drop(first);
    let running = drain(&received);
    assert_eq!(labels(&running), ["compact-2", "index"]);
    assert_eq!(scheduler.stats().reserved_bytes, 60);
    drop(running);

    let error = ask(&scheduler, JobKind::Index, 101, "huge", &granted)
        .expect_err("more than the whole pool");
    assert!(
        matches!(&error, LogPoseError::TooLarge { what, .. } if what == "index build memory"),
        "{error:?}"
    );
}
