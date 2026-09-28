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
    // Fill all three slots: two compactions and one flush.
    ask(&scheduler, JobKind::Compact, 1, "compact-1", &granted).expect("request");
    ask(&scheduler, JobKind::Compact, 1, "compact-2", &granted).expect("request");
    ask(&scheduler, JobKind::Flush, 0, "flush-1", &granted).expect("request");
    let mut running = drain(&received);
    assert_eq!(labels(&running), ["compact-1", "compact-2", "flush-1"]);
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
    // A flush reserves nothing, whatever it is asked for.
    ask(&scheduler, JobKind::Flush, 1_000_000, "flush", &granted).expect("request");
    let flush = drain(&received);
    assert_eq!(flush[0].1.bytes(), 0);
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
fn one_thread_still_gets_a_flush_slot_and_a_compaction_slot() {
    let scheduler = MaintenanceScheduler::new(1, 100);
    assert_eq!(scheduler.slots(), 2);
    let (granted, received) = mpsc::channel();
    ask(&scheduler, JobKind::Compact, 1, "compact", &granted).expect("request");
    ask(&scheduler, JobKind::Flush, 0, "flush", &granted).expect("request");
    assert_eq!(labels(&drain(&received)), ["compact", "flush"]);
}
