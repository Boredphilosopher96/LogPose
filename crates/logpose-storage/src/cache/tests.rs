//! Standalone tests of the buffer cache: accounting, CLOCK, floors, pins,
//! single flight, failures, invalidation, reports, warm-up, and a
//! concurrency stress test.

use super::{
    AlignedBytes, ArtifactClass, BudgetInputs, BufferCache, CacheConfig, CacheKey, CacheMode,
    CacheUnit, FetchReport, Fetched, FileId, InlineExecutor, LoadExecutor, LoadJob, PinSet,
    WarmUpItem, charge_for,
};
use crate::segment_v2::SegmentError;
use std::{
    future::Future,
    io,
    pin::pin,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

const UNIT: usize = 1000;

fn cache(budget: u64) -> BufferCache {
    BufferCache::new(CacheConfig::with_budget(budget))
}

/// A budget that holds exactly `entries` units of `UNIT` bytes.
fn budget_for(entries: u64) -> u64 {
    entries * charge_for(UNIT)
}

fn key(file: FileId, section: u32) -> CacheKey {
    CacheKey::section(file, section)
}

fn bytes(len: usize, fill: u8) -> AlignedBytes {
    AlignedBytes::copy_from(&vec![fill; len])
}

/// Load `key` with a fresh `UNIT`-byte buffer and drop the pin.
fn insert(cache: &BufferCache, key: CacheKey, class: ArtifactClass) -> Fetched {
    let (_, fetched) = cache
        .get_or_load_blocking(key, class, CacheMode::Normal, || Ok(bytes(UNIT, 1)))
        .expect("load succeeds");
    fetched
}

fn hit(cache: &BufferCache, key: CacheKey, class: ArtifactClass) -> Arc<AlignedBytes> {
    let (bytes, fetched) = cache
        .get_or_load_blocking(key, class, CacheMode::Normal, || {
            unreachable!("a resident key must not load")
        })
        .expect("hit");
    assert_eq!(fetched, Fetched::Hit);
    bytes
}

fn io_error() -> SegmentError {
    SegmentError::from(io::Error::other("disk on fire"))
}

/// Poll a future to completion on the current thread.
fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park_timeout(Duration::from_millis(10));
    }
}

/// Runs each job on a new thread. `join` waits for them: a job thread holds
/// its flight (whose result pins the bytes) until it returns, a moment
/// after its waiters wake, so tests that assert on eviction join first.
#[derive(Default)]
struct SpawnExecutor {
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl LoadExecutor for SpawnExecutor {
    fn execute(&self, job: LoadJob) {
        let thread = thread::spawn(move || job.run());
        self.threads.lock().expect("threads").push(thread);
    }
}

impl SpawnExecutor {
    fn join(&self) {
        let threads = std::mem::take(&mut *self.threads.lock().expect("threads"));
        for thread in threads {
            thread.join().expect("job thread");
        }
    }
}

/// Keeps jobs until the test runs or drops them.
#[derive(Default)]
struct HeldExecutor {
    jobs: Mutex<Vec<LoadJob>>,
}

impl LoadExecutor for HeldExecutor {
    fn execute(&self, job: LoadJob) {
        self.jobs.lock().expect("jobs").push(job);
    }
}

impl HeldExecutor {
    fn take(&self) -> Vec<LoadJob> {
        std::mem::take(&mut *self.jobs.lock().expect("jobs"))
    }
}

/// Drops every job, like an executor that has shut down.
struct ClosedExecutor;

impl LoadExecutor for ClosedExecutor {
    fn execute(&self, job: LoadJob) {
        drop(job);
    }
}

/// Wait until `condition` holds, failing after ten seconds.
fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn charges_every_entry_against_its_class() {
    let cache = cache(1 << 20);
    let file = FileId::next();
    insert(&cache, key(file, 0), ArtifactClass::GraphAndCodes);
    insert(&cache, key(file, 1), ArtifactClass::GraphAndCodes);
    cache
        .get_or_load_blocking(
            key(file, 2),
            ArtifactClass::RawVectors,
            CacheMode::Normal,
            || Ok(bytes(13, 2)),
        )
        .expect("load");
    let stats = cache.stats();
    assert_eq!(
        stats.used_by(ArtifactClass::GraphAndCodes),
        2 * charge_for(UNIT)
    );
    assert_eq!(stats.used_by(ArtifactClass::RawVectors), charge_for(13));
    assert_eq!(stats.used_total(), cache.used());
    assert_eq!(stats.entries, 3);
    assert_eq!(charge_for(13), 16 + super::ENTRY_OVERHEAD);
}

#[test]
fn evicts_down_to_the_budget() {
    let cache = cache(budget_for(4));
    let file = FileId::next();
    for section in 0..6 {
        insert(&cache, key(file, section), ArtifactClass::ScalarColumns);
        assert!(cache.used() <= cache.budget());
    }
    let stats = cache.stats();
    assert_eq!(stats.entries, 4);
    assert_eq!(stats.evictions, 2);
    assert_eq!(stats.evicted_bytes, 2 * charge_for(UNIT));
    assert_eq!(stats.overcommit_bytes, 0);
    // CLOCK without references evicts in insertion order.
    assert!(!cache.residency(&key(file, 0)));
    assert!(!cache.residency(&key(file, 1)));
    assert!((2..6).all(|section| cache.residency(&key(file, section))));
}

#[test]
fn clock_gives_referenced_entries_a_second_chance() {
    let cache = cache(budget_for(3));
    let file = FileId::next();
    let class = ArtifactClass::ScalarColumns;
    for section in 0..3 {
        insert(&cache, key(file, section), class);
    }
    drop(hit(&cache, key(file, 0), class));
    insert(&cache, key(file, 3), class);
    assert!(cache.residency(&key(file, 0)), "referenced entry survives");
    assert!(!cache.residency(&key(file, 1)), "unreferenced entry goes");
    assert!(cache.residency(&key(file, 2)));
    assert!(cache.residency(&key(file, 3)));
    // The hand moved past it to the back of the ring, and its reference is
    // spent: it goes after the two entries now ahead of it.
    insert(&cache, key(file, 4), class);
    assert!(!cache.residency(&key(file, 2)));
    insert(&cache, key(file, 5), class);
    assert!(!cache.residency(&key(file, 3)));
    insert(&cache, key(file, 6), class);
    assert!(!cache.residency(&key(file, 0)));
}

#[test]
fn never_evicts_pinned_entries() {
    let cache = cache(budget_for(2));
    let file = FileId::next();
    let class = ArtifactClass::RawVectors;
    let mut pins = PinSet::new();
    for section in 0..2 {
        let (bytes, _) = cache
            .get_or_load_blocking(key(file, section), class, CacheMode::Normal, || {
                Ok(bytes(UNIT, 3))
            })
            .expect("load");
        pins.insert(key(file, section), bytes);
    }
    let (third, _) = cache
        .get_or_load_blocking(key(file, 2), class, CacheMode::Normal, || {
            Ok(bytes(UNIT, 4))
        })
        .expect("load still succeeds over budget");
    for section in 0..3 {
        assert!(cache.residency(&key(file, section)), "section {section}");
    }
    let stats = cache.stats();
    assert_eq!(stats.evictions, 0);
    assert_eq!(stats.overcommits, 1);
    assert_eq!(stats.overcommit_bytes, charge_for(UNIT));
    assert_eq!(cache.used(), budget_for(3));
    // Unpinning the third makes it the only candidate.
    drop(third);
    cache.trim();
    assert!(!cache.residency(&key(file, 2)));
    assert!(cache.residency(&key(file, 0)) && cache.residency(&key(file, 1)));
    assert_eq!(cache.stats().overcommit_bytes, 0);
}

#[test]
fn dropping_a_pin_set_releases_its_pins() {
    let cache = cache(budget_for(4));
    let file = FileId::next();
    let class = ArtifactClass::PkIndex;
    let mut pins = PinSet::new();
    for section in 0..4 {
        let (bytes, _) = cache
            .get_or_load_blocking(key(file, section), class, CacheMode::Normal, || {
                Ok(bytes(UNIT, 5))
            })
            .expect("load");
        pins.insert(key(file, section), bytes);
    }
    assert_eq!(pins.len(), 4);
    assert_eq!(pins.bytes(), 4 * UNIT as u64);
    cache.set_budget(budget_for(1));
    assert_eq!(cache.stats().entries, 4, "everything is pinned");
    assert!(pins.release(&key(file, 3)));
    cache.trim();
    assert!(!cache.residency(&key(file, 3)));
    drop(pins);
    cache.trim();
    assert_eq!(cache.stats().entries, 1);
    assert!(cache.used() <= cache.budget());
}

#[test]
fn evicts_the_lowest_priority_class_first() {
    let cache = BufferCache::new(CacheConfig {
        budget: budget_for(4),
        floors: [0.0; ArtifactClass::COUNT],
    });
    let file = FileId::next();
    insert(&cache, key(file, 0), ArtifactClass::GraphAndCodes);
    insert(&cache, key(file, 1), ArtifactClass::DynamicJson);
    insert(&cache, key(file, 2), ArtifactClass::RawVectors);
    insert(&cache, key(file, 3), ArtifactClass::PkIndex);
    insert(&cache, key(file, 4), ArtifactClass::GraphAndCodes);
    assert!(!cache.residency(&key(file, 1)), "dynamic goes first");
    insert(&cache, key(file, 5), ArtifactClass::GraphAndCodes);
    assert!(!cache.residency(&key(file, 2)), "raw vectors next");
    insert(&cache, key(file, 6), ArtifactClass::GraphAndCodes);
    assert!(!cache.residency(&key(file, 3)), "then the pk index");
    assert!(cache.residency(&key(file, 0)));
}

#[test]
fn keeps_class_floors_under_pressure_from_higher_classes() {
    // Floors: raw vectors keep 25 percent of a 16-unit budget (4 units).
    let mut floors = [0.0; ArtifactClass::COUNT];
    floors[ArtifactClass::RawVectors.index()] = 0.25;
    let cache = BufferCache::new(CacheConfig {
        budget: budget_for(16),
        floors,
    });
    let file = FileId::next();
    for section in 0..10 {
        insert(&cache, key(file, section), ArtifactClass::RawVectors);
    }
    for section in 100..200 {
        insert(&cache, key(file, section), ArtifactClass::GraphAndCodes);
        assert!(cache.used() <= cache.budget());
    }
    let stats = cache.stats();
    assert_eq!(stats.used_by(ArtifactClass::RawVectors), budget_for(4));
    assert_eq!(stats.used_by(ArtifactClass::GraphAndCodes), budget_for(12));
}

#[test]
fn concurrent_blocking_misses_share_one_read() {
    const CALLERS: usize = 16;
    let cache = cache(1 << 20);
    let file = FileId::next();
    let reads = Arc::new(AtomicUsize::new(0));
    let (release, gate) = mpsc::channel::<()>();
    let gate = Arc::new(Mutex::new(gate));
    let barrier = Arc::new(Barrier::new(CALLERS));
    let callers = (0..CALLERS)
        .map(|_| {
            let (cache, reads, gate, barrier) = (
                cache.clone(),
                Arc::clone(&reads),
                Arc::clone(&gate),
                Arc::clone(&barrier),
            );
            thread::spawn(move || {
                barrier.wait();
                cache.get_or_load_blocking(
                    key(file, 7),
                    ArtifactClass::GraphAndCodes,
                    CacheMode::Normal,
                    || {
                        reads.fetch_add(1, Ordering::SeqCst);
                        gate.lock().expect("gate").recv().expect("released");
                        Ok(bytes(UNIT, 9))
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    wait_until("every caller to join the load", || {
        cache.stats().waits == CALLERS as u64 - 1
    });
    release.send(()).expect("loader waits");
    let results = callers
        .into_iter()
        .map(|caller| caller.join().expect("caller").expect("load"))
        .collect::<Vec<_>>();
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    let mut report = FetchReport::default();
    for (bytes, fetched) in &results {
        assert!(Arc::ptr_eq(bytes, &results[0].0));
        report.record(ArtifactClass::GraphAndCodes, *fetched);
    }
    assert_eq!((report.misses, report.waits, report.hits), (1, 15, 0));
    assert_eq!(report.bytes_read, UNIT as u64);
    assert!(cache.residency(&key(file, 7)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_async_misses_share_one_read() {
    const CALLERS: usize = 16;
    let cache = cache(1 << 20);
    let file = FileId::next();
    let reads = Arc::new(AtomicUsize::new(0));
    let open = Arc::new(AtomicBool::new(false));
    let fetches = (0..CALLERS)
        .map(|_| {
            let (reads, open) = (Arc::clone(&reads), Arc::clone(&open));
            let fetch = cache.get_or_load(
                key(file, 1),
                ArtifactClass::PkIndex,
                CacheMode::Normal,
                &SpawnExecutor::default(),
                move || {
                    reads.fetch_add(1, Ordering::SeqCst);
                    while !open.load(Ordering::SeqCst) {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Ok(bytes(UNIT, 4))
                },
            );
            tokio::spawn(fetch)
        })
        .collect::<Vec<_>>();
    open.store(true, Ordering::SeqCst);
    let mut report = FetchReport::default();
    let mut first: Option<Arc<AlignedBytes>> = None;
    for fetch in fetches {
        let (bytes, fetched) = fetch.await.expect("task").expect("load");
        report.record(ArtifactClass::PkIndex, fetched);
        let first = first.get_or_insert_with(|| Arc::clone(&bytes));
        assert!(Arc::ptr_eq(first, &bytes));
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!((report.misses, report.waits), (1, 15));
    assert_eq!(report.by_class[ArtifactClass::PkIndex.index()].misses, 1);
}

#[test]
fn failed_load_reaches_every_waiter_and_is_not_cached() {
    const CALLERS: usize = 8;
    let cache = cache(1 << 20);
    let file = FileId::next();
    let reads = Arc::new(AtomicUsize::new(0));
    let (release, gate) = mpsc::channel::<()>();
    let gate = Arc::new(Mutex::new(gate));
    let callers = (0..CALLERS)
        .map(|_| {
            let (cache, reads, gate) = (cache.clone(), Arc::clone(&reads), Arc::clone(&gate));
            thread::spawn(move || {
                cache.get_or_load_blocking(
                    key(file, 0),
                    ArtifactClass::ScalarIndex,
                    CacheMode::Normal,
                    || {
                        reads.fetch_add(1, Ordering::SeqCst);
                        gate.lock().expect("gate").recv().expect("released");
                        Err(io_error())
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    wait_until("every caller to join the load", || {
        cache.stats().waits == CALLERS as u64 - 1
    });
    release.send(()).expect("loader waits");
    for caller in callers {
        let error = caller.join().expect("caller").expect_err("load fails");
        assert!(matches!(error, SegmentError::Io(_)), "{error}");
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert!(!cache.residency(&key(file, 0)));
    assert_eq!(cache.used(), 0);
    assert_eq!(cache.stats().failed_loads, 1);
    // The next access loads again.
    assert!(matches!(
        insert(&cache, key(file, 0), ArtifactClass::ScalarIndex),
        Fetched::Loaded { .. }
    ));
}

#[test]
fn corrupt_load_returns_the_typed_error_and_is_not_cached() {
    let cache = cache(1 << 20);
    let file = FileId::next();
    let region = crate::segment_v2::Region::VectorPage { index: 3, page: 9 };
    let error = cache
        .get_or_load_blocking(
            CacheKey {
                file,
                section: 3,
                unit: CacheUnit::Page(9),
            },
            ArtifactClass::RawVectors,
            CacheMode::Normal,
            || Err(SegmentError::Checksum { region }),
        )
        .expect_err("corrupt");
    assert!(error.is_corruption());
    assert!(matches!(error, SegmentError::Checksum { region: found } if found == region));
    assert_eq!(cache.stats().entries, 0);
}

// The panic is the point: a panicking loader must not leave waiters hanging.
#[allow(clippy::panic)]
#[test]
fn panicking_loader_aborts_its_waiters_and_caches_nothing() {
    let cache = cache(1 << 20);
    let file = FileId::next();
    let started = Arc::new(Barrier::new(2));
    let (release, gate) = mpsc::channel::<()>();
    let leader = {
        let (cache, started) = (cache.clone(), Arc::clone(&started));
        thread::spawn(move || {
            cache.get_or_load_blocking(
                key(file, 0),
                ArtifactClass::PkIndex,
                CacheMode::Normal,
                || -> Result<AlignedBytes, SegmentError> {
                    started.wait();
                    gate.recv().expect("released");
                    panic!("loader failed hard")
                },
            )
        })
    };
    started.wait();
    let waiter = {
        let cache = cache.clone();
        thread::spawn(move || {
            cache.get_or_load_blocking(
                key(file, 0),
                ArtifactClass::PkIndex,
                CacheMode::Normal,
                || unreachable!("the waiter joins"),
            )
        })
    };
    wait_until("the waiter to join", || cache.stats().waits == 1);
    release.send(()).expect("loader waits");
    assert!(leader.join().is_err(), "the panic reaches the leader");
    let error = waiter.join().expect("waiter").expect_err("aborted");
    assert!(matches!(error, SegmentError::LoadAborted));
    assert!(!cache.residency(&key(file, 0)));
    assert!(matches!(
        insert(&cache, key(file, 0), ArtifactClass::PkIndex),
        Fetched::Loaded { .. }
    ));
}

#[test]
fn a_dropped_job_aborts_its_waiters() {
    let cache = cache(1 << 20);
    let file = FileId::next();
    let fetch = cache.get_or_load(
        key(file, 0),
        ArtifactClass::GraphAndCodes,
        CacheMode::Normal,
        &ClosedExecutor,
        || Ok(bytes(UNIT, 1)),
    );
    let error = block_on(fetch).expect_err("the executor dropped the job");
    assert!(matches!(error, SegmentError::LoadAborted));
    let (_, fetched) = block_on(cache.get_or_load(
        key(file, 0),
        ArtifactClass::GraphAndCodes,
        CacheMode::Normal,
        &InlineExecutor,
        || Ok(bytes(UNIT, 1)),
    ))
    .expect("a retry loads");
    assert!(matches!(fetched, Fetched::Loaded { .. }));
}

#[test]
fn a_blocking_caller_runs_a_queued_load_itself() {
    let cache = cache(1 << 20);
    let file = FileId::next();
    let executor = HeldExecutor::default();
    let reads = Arc::new(AtomicUsize::new(0));
    let queued = {
        let reads = Arc::clone(&reads);
        cache.get_or_load(
            key(file, 0),
            ArtifactClass::GraphAndCodes,
            CacheMode::Normal,
            &executor,
            move || {
                reads.fetch_add(1, Ordering::SeqCst);
                Ok(bytes(UNIT, 6))
            },
        )
    };
    // The job sits in the queue. A blocking caller must not wait for it.
    let (bytes, fetched) = cache
        .get_or_load_blocking(
            key(file, 0),
            ArtifactClass::GraphAndCodes,
            CacheMode::Normal,
            || unreachable!("the stored loader runs instead"),
        )
        .expect("load");
    assert_eq!(fetched, Fetched::Waited);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    let (queued_bytes, queued_fetched) = block_on(queued).expect("leader resolves");
    assert!(Arc::ptr_eq(&bytes, &queued_bytes));
    assert!(matches!(queued_fetched, Fetched::Loaded { .. }));
    for job in executor.take() {
        job.run();
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1, "the queued job is a no-op");
}

#[test]
fn bypass_reads_use_resident_entries_but_never_insert() {
    let cache = cache(1 << 20);
    let file = FileId::next();
    let (_, fetched) = cache
        .get_or_load_blocking(
            key(file, 0),
            ArtifactClass::ScalarColumns,
            CacheMode::Bypass,
            || Ok(bytes(UNIT, 1)),
        )
        .expect("load");
    assert!(matches!(fetched, Fetched::Loaded { .. }));
    assert!(!cache.residency(&key(file, 0)));
    let (_, fetched) = block_on(cache.get_or_load(
        key(file, 0),
        ArtifactClass::ScalarColumns,
        CacheMode::Bypass,
        &SpawnExecutor::default(),
        || Ok(bytes(UNIT, 1)),
    ))
    .expect("load");
    assert!(matches!(fetched, Fetched::Loaded { .. }));
    assert_eq!(cache.used(), 0);

    insert(&cache, key(file, 0), ArtifactClass::ScalarColumns);
    let (_, fetched) = cache
        .get_or_load_blocking(
            key(file, 0),
            ArtifactClass::ScalarColumns,
            CacheMode::Bypass,
            || unreachable!("resident"),
        )
        .expect("hit");
    assert_eq!(fetched, Fetched::Hit);
    // A bypass hit does not mark the entry referenced: it is the first
    // victim, ahead of an entry inserted after it.
    cache.set_budget(budget_for(2));
    insert(&cache, key(file, 1), ArtifactClass::ScalarColumns);
    insert(&cache, key(file, 2), ArtifactClass::ScalarColumns);
    assert!(!cache.residency(&key(file, 0)));
}

#[test]
fn invalidating_a_file_drops_its_entries_and_detaches_its_loads() {
    let cache = cache(1 << 20);
    let (dead, live) = (FileId::next(), FileId::next());
    for section in 0..3 {
        insert(&cache, key(dead, section), ArtifactClass::RawVectors);
        insert(&cache, key(live, section), ArtifactClass::RawVectors);
    }
    let executor = HeldExecutor::default();
    let in_flight = cache.get_or_load(
        key(dead, 9),
        ArtifactClass::RawVectors,
        CacheMode::Normal,
        &executor,
        || Ok(bytes(UNIT, 2)),
    );
    assert_eq!(cache.invalidate_file(dead), 3);
    assert_eq!(cache.used(), budget_for(3));
    assert!((0..3).all(|section| !cache.residency(&key(dead, section))));
    assert!((0..3).all(|section| cache.residency(&key(live, section))));
    for job in executor.take() {
        job.run();
    }
    let (bytes, _) = block_on(in_flight).expect("waiters still get the bytes");
    assert_eq!(bytes.len(), UNIT);
    assert!(
        !cache.residency(&key(dead, 9)),
        "a detached load is not cached"
    );
    assert_eq!(cache.stats().invalidated, 3);
    assert_eq!(cache.used(), budget_for(3));
}

#[test]
fn fetch_reports_count_hits_misses_waits_and_bytes() {
    let cache = cache(1 << 20);
    let file = FileId::next();
    let mut report = FetchReport::default();
    for (section, class, len) in [
        (0, ArtifactClass::GraphAndCodes, 100),
        (1, ArtifactClass::RawVectors, 40),
        (2, ArtifactClass::RawVectors, 60),
    ] {
        let (_, fetched) = cache
            .get_or_load_blocking(key(file, section), class, CacheMode::Normal, || {
                Ok(bytes(len, 0))
            })
            .expect("load");
        report.record(class, fetched);
    }
    let (_, fetched) = cache
        .get_or_load_blocking(
            key(file, 1),
            ArtifactClass::RawVectors,
            CacheMode::Normal,
            || unreachable!("resident"),
        )
        .expect("hit");
    report.record(ArtifactClass::RawVectors, fetched);
    report.record(ArtifactClass::RawVectors, Fetched::Waited);
    assert_eq!(report.hits, 1);
    assert_eq!(report.misses, 3);
    assert_eq!(report.waits, 1);
    assert_eq!(report.bytes_read, 200);
    assert_eq!(report.by_class[ArtifactClass::RawVectors.index()].misses, 2);
    assert_eq!(
        report.by_class[ArtifactClass::RawVectors.index()].bytes,
        100
    );
    assert!(report.is_cold());
    let mut total = FetchReport::default();
    total.merge(&report);
    total.merge(&report);
    assert_eq!(total.misses, 6);
    assert_eq!(
        total.by_class[ArtifactClass::GraphAndCodes.index()].bytes,
        200
    );
    let stats = cache.stats();
    assert_eq!((stats.hits, stats.misses), (1, 3));
}

fn warm_item(
    file: FileId,
    section: u32,
    class: ArtifactClass,
    order: &Arc<Mutex<Vec<u32>>>,
) -> WarmUpItem {
    let order = Arc::clone(order);
    WarmUpItem {
        key: key(file, section),
        class,
        bytes: UNIT as u64,
        load: Box::new(move || {
            order.lock().expect("order").push(section);
            Ok(bytes(UNIT, 7))
        }),
    }
}

#[test]
fn warm_up_loads_by_class_priority_and_stops_at_the_fill_limit() {
    // Room for ten units; warm-up stops at 90 percent.
    let cache = cache(budget_for(10));
    let file = FileId::next();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut items = Vec::new();
    for section in 20..25 {
        items.push(warm_item(file, section, ArtifactClass::ScalarIndex, &order));
    }
    for section in 10..14 {
        items.push(warm_item(file, section, ArtifactClass::PkIndex, &order));
    }
    for section in 0..3 {
        items.push(warm_item(
            file,
            section,
            ArtifactClass::GraphAndCodes,
            &order,
        ));
    }
    insert(&cache, key(file, 1), ArtifactClass::GraphAndCodes);
    let report = block_on(cache.warm_up(items, &InlineExecutor));
    // Loads not yet awaited count against the limit even when (as with the
    // inline executor) they are already resident, so the estimate is
    // conservative by at most one unit.
    assert_eq!(report.resident, 1);
    assert_eq!(report.loaded, 7, "{report:?}");
    assert_eq!(report.skipped, 4);
    assert_eq!(report.failed, 0);
    assert_eq!(report.bytes_read, 7 * UNIT as u64);
    assert_eq!(
        *order.lock().expect("order"),
        vec![0, 2, 10, 11, 12, 13, 20],
        "classes load in priority order and the lowest one is cut off"
    );
    assert!(cache.used() <= budget_for(9));
    assert_eq!(cache.stats().evictions, 0, "warm-up never evicts");
}

#[test]
fn warm_up_keeps_at_most_two_loads_in_flight_and_survives_failures() {
    let cache = cache(1 << 30);
    let file = FileId::next();
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let items = (0..12)
        .map(|section| {
            let (running, peak) = (Arc::clone(&running), Arc::clone(&peak));
            WarmUpItem {
                key: key(file, section),
                class: ArtifactClass::GraphAndCodes,
                bytes: UNIT as u64,
                load: Box::new(move || {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(5));
                    running.fetch_sub(1, Ordering::SeqCst);
                    if section % 4 == 3 {
                        Err(io_error())
                    } else {
                        Ok(bytes(UNIT, 1))
                    }
                }),
            }
        })
        .collect();
    let report = block_on(cache.warm_up(items, &SpawnExecutor::default()));
    assert_eq!(report.loaded, 9);
    assert_eq!(report.failed, 3);
    assert!((1..=2).contains(&peak.load(Ordering::SeqCst)));
    assert_eq!(cache.stats().entries, 9);
}

#[test]
fn shrinking_the_budget_evicts_immediately() {
    let cache = cache(budget_for(8));
    let file = FileId::next();
    for section in 0..8 {
        insert(&cache, key(file, section), ArtifactClass::ScalarColumns);
    }
    cache.set_budget(budget_for(3));
    assert_eq!(cache.stats().entries, 3);
    assert_eq!(cache.used(), budget_for(3));
}

#[test]
fn budget_is_the_memory_limit_minus_every_reservation() {
    let inputs = BudgetInputs {
        memory_limit: 16_000,
        pk_index_bytes: 1_000,
        memtable_bytes: 2_000,
        maintenance_bytes: 3_000,
    };
    assert_eq!(
        inputs.cache_budget(),
        16_000 - 1_000 - 2_000 - 1_600 - 3_000
    );
    let starved = BudgetInputs {
        memory_limit: 1_000,
        pk_index_bytes: 5_000,
        ..BudgetInputs::default()
    };
    assert_eq!(starved.cache_budget(), 0);
}

/// Deterministic per-thread randomness.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

#[test]
fn stress_stays_within_budget_beyond_pins_and_never_deadlocks() {
    const BLOCKING_THREADS: u64 = 6;
    const ASYNC_THREADS: u64 = 2;
    const THREADS: u64 = BLOCKING_THREADS + ASYNC_THREADS;
    const OPS: u64 = 3_000;
    const KEYS: u32 = 96;
    const MAX_PINS: usize = 2;
    let files = [FileId::next(), FileId::next()];
    let size_of = |section: u32| 200 + (section as usize % 7) * 300;
    let class_of = |section: u32| ArtifactClass::ALL[section as usize % ArtifactClass::COUNT];
    let max_charge = charge_for(size_of(6));
    let cache = cache(12 * max_charge);
    // Every thread holds at most MAX_PINS pins plus the unit it is loading,
    // and an inserted unit stays pinned by its flight until handed out.
    let slack = THREADS * (MAX_PINS as u64 + 2) * max_charge;
    let reads = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let checker = {
        let (cache, stop) = (cache.clone(), Arc::clone(&stop));
        thread::spawn(move || {
            let mut samples = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                let used = cache.used();
                assert!(
                    used <= cache.budget() + slack,
                    "used {used} exceeds budget {} plus pinned slack {slack}",
                    cache.budget()
                );
                samples += 1;
                thread::yield_now();
            }
            samples
        })
    };
    let executor = Arc::new(SpawnExecutor::default());
    let (done, finished) = mpsc::channel::<u64>();
    for thread_index in 0..THREADS {
        let (cache, reads, done, executor) = (
            cache.clone(),
            Arc::clone(&reads),
            done.clone(),
            Arc::clone(&executor),
        );
        thread::spawn(move || {
            let mut rng = SplitMix(thread_index * 7919 + 1);
            let mut pins: Vec<(CacheKey, Arc<AlignedBytes>)> = Vec::new();
            for op in 0..OPS {
                let section = u32::try_from(rng.below(u64::from(KEYS))).unwrap_or(0);
                let file = files[usize::from(rng.below(10) == 0)];
                let key = key(file, section);
                let class = class_of(section);
                let len = size_of(section);
                let fail = rng.below(50) == 0;
                let reads = Arc::clone(&reads);
                let load = move || {
                    reads.fetch_add(1, Ordering::Relaxed);
                    if fail {
                        Err(io_error())
                    } else {
                        Ok(bytes(len, u8::try_from(section % 251).unwrap_or(0)))
                    }
                };
                let result = if thread_index < BLOCKING_THREADS {
                    cache.get_or_load_blocking(key, class, CacheMode::Normal, load)
                } else {
                    block_on(cache.get_or_load(key, class, CacheMode::Normal, &*executor, load))
                };
                if let Ok((bytes, _)) = result {
                    assert_eq!(bytes.len(), len, "a key always maps to its own bytes");
                    if rng.below(3) == 0 {
                        pins.push((key, bytes));
                    }
                }
                while pins.len() > MAX_PINS {
                    pins.remove(0);
                }
                if op % 500 == 499 && thread_index == 0 {
                    cache.invalidate_file(files[1]);
                }
            }
            drop(pins);
            let _ = done.send(thread_index);
        });
    }
    drop(done);
    for _ in 0..THREADS {
        finished
            .recv_timeout(Duration::from_secs(120))
            .expect("every thread finishes: no deadlock");
    }
    stop.store(true, Ordering::Relaxed);
    let samples = checker.join().expect("the budget invariant held");
    executor.join();
    assert!(samples > 0);

    // Quiescent: nothing is pinned, so a trim reaches the budget, and the
    // accounting equals the charges of exactly the resident keys.
    cache.trim();
    assert!(cache.used() <= cache.budget());
    let resident_charge: u64 = files
        .iter()
        .flat_map(|file| (0..KEYS).map(move |section| (*file, section)))
        .filter(|(file, section)| cache.residency(&key(*file, *section)))
        .map(|(_, section)| charge_for(size_of(section)))
        .sum();
    assert_eq!(cache.used(), resident_charge);
    let stats = cache.stats();
    assert_eq!(
        stats.used_total(),
        resident_charge,
        "per-class counters agree with the entries"
    );
    assert_eq!(
        reads.load(Ordering::Relaxed),
        stats.misses,
        "every miss ran its loader exactly once"
    );
    assert!(stats.evictions > 0, "the budget was small enough to churn");
}

#[test]
fn invalidating_a_file_releases_its_clock_slots() {
    // Far under budget, so eviction never runs to skip dead slots.
    let cache = cache(budget_for(10_000));
    let kept = FileId::next();
    insert(&cache, key(kept, 0), ArtifactClass::PkIndex);
    for _ in 0..50 {
        let file = FileId::next();
        for section in 0..20 {
            insert(&cache, key(file, section), ArtifactClass::PkIndex);
        }
        assert_eq!(cache.invalidate_file(file), 20);
    }
    let slots: usize = cache
        .inner
        .clocks
        .iter()
        .map(|ring| super::lock(ring).len())
        .sum();
    assert_eq!(slots, 1, "only the live entry keeps a ring slot");
    assert_eq!(cache.used(), charge_for(UNIT));
    hit(&cache, key(kept, 0), ArtifactClass::PkIndex);
}

#[test]
fn pinned_higher_classes_do_not_evict_a_class_below_its_floor() {
    // Dynamic JSON keeps 4 of 16 units; it holds 3.
    let mut floors = [0.0; ArtifactClass::COUNT];
    floors[ArtifactClass::DynamicJson.index()] = 0.25;
    let cache = BufferCache::new(CacheConfig {
        budget: budget_for(16),
        floors,
    });
    let file = FileId::next();
    for section in 0..3 {
        insert(&cache, key(file, section), ArtifactClass::DynamicJson);
    }
    // Twenty pinned graph units overflow the budget with nothing of theirs
    // evictable.
    let pins: Vec<_> = (100..120)
        .map(|section| {
            cache
                .get_or_load_blocking(
                    key(file, section),
                    ArtifactClass::GraphAndCodes,
                    CacheMode::Normal,
                    || Ok(bytes(UNIT, 2)),
                )
                .expect("loads")
                .0
        })
        .collect();
    let stats = cache.stats();
    assert_eq!(
        stats.used_by(ArtifactClass::DynamicJson),
        budget_for(3),
        "the floor holds"
    );
    assert!(stats.overcommits > 0);
    assert_eq!(stats.overcommit_bytes, budget_for(7));
    // Released pins make the graph units evictable again.
    drop(pins);
    cache.trim();
    let stats = cache.stats();
    assert!(stats.used_total() <= cache.budget());
    assert_eq!(stats.used_by(ArtifactClass::DynamicJson), budget_for(3));
}

#[test]
fn floors_that_exceed_the_budget_still_evict() {
    // Every class claims the whole budget, so none is ever over its floor
    // and the lowest-priority non-empty class gives way.
    let cache = BufferCache::new(CacheConfig {
        budget: budget_for(4),
        floors: [1.0; ArtifactClass::COUNT],
    });
    let file = FileId::next();
    for section in 0..4 {
        insert(&cache, key(file, section), ArtifactClass::GraphAndCodes);
    }
    for section in 10..12 {
        insert(&cache, key(file, section), ArtifactClass::DynamicJson);
    }
    assert!(cache.used() <= cache.budget());
    assert_eq!(cache.stats().overcommits, 0);
}

/// The internal state agrees with itself: per-class usage is the sum of the
/// resident entries' charges, and every resident entry sits in its class's
/// ring exactly once (an entry missing from its ring could never be
/// evicted).
fn assert_consistent(cache: &BufferCache) {
    let inner = &cache.inner;
    let mut charges = [0_u64; ArtifactClass::COUNT];
    let mut resident = std::collections::HashMap::new();
    for shard in inner.shards.iter() {
        for (key, entry) in &super::lock(shard).entries {
            assert_eq!(*key, entry.key);
            assert_eq!(entry.charge, charge_for(entry.bytes.len()));
            charges[entry.class.index()] += entry.charge;
            resident.insert(*key, entry.class);
        }
    }
    let used: [u64; ArtifactClass::COUNT] = cache.stats().used;
    assert_eq!(used, charges, "per-class usage equals the resident charges");
    let mut in_rings = std::collections::HashMap::new();
    for class in ArtifactClass::ALL {
        for slot in super::lock(&inner.clocks[class.index()]).iter() {
            if let Some(entry) = slot.upgrade() {
                assert_eq!(entry.class, class, "an entry sits in its class's ring");
                *in_rings.entry(entry.key).or_insert(0) += 1;
            }
        }
    }
    for key in resident.keys() {
        assert_eq!(
            in_rings.get(key),
            Some(&1),
            "{key:?} is in its ring exactly once"
        );
    }
}

#[test]
fn stress_with_bypass_budget_changes_and_invalidation_keeps_exact_accounting() {
    const BLOCKING_THREADS: u64 = 5;
    const ASYNC_THREADS: u64 = 3;
    const THREADS: u64 = BLOCKING_THREADS + ASYNC_THREADS;
    const OPS: u64 = 2_500;
    const KEYS: u32 = 64;
    const MAX_PINS: usize = 3;
    let files: [FileId; 3] = std::array::from_fn(|_| FileId::next());
    let size_of = |section: u32| 64 + (section as usize % 5) * 700;
    let class_of = |section: u32| ArtifactClass::ALL[section as usize % ArtifactClass::COUNT];
    let max_charge = charge_for(size_of(4));
    let budgets = [0, 4 * max_charge, 10 * max_charge, 40 * max_charge];
    let cache = BufferCache::new(CacheConfig {
        budget: budgets[2],
        floors: [0.0, 0.0, 0.05, 0.1, 0.05, 0.1],
    });
    let slack = THREADS * (MAX_PINS as u64 + 2) * max_charge;
    let stop = Arc::new(AtomicBool::new(false));
    let checker = {
        let (cache, stop) = (cache.clone(), Arc::clone(&stop));
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // A budget change can be mid-eviction; allow the largest.
                let used = cache.used();
                assert!(
                    used <= budgets[3] + slack,
                    "used {used} exceeds every budget plus pinned slack"
                );
                thread::yield_now();
            }
        })
    };
    let executor = Arc::new(SpawnExecutor::default());
    let workers: Vec<_> = (0..THREADS)
        .map(|thread_index| {
            let (cache, executor) = (cache.clone(), Arc::clone(&executor));
            thread::spawn(move || {
                let mut rng = SplitMix(thread_index * 104_729 + 17);
                let mut pins: Vec<Arc<AlignedBytes>> = Vec::new();
                for _ in 0..OPS {
                    let section = u32::try_from(rng.below(u64::from(KEYS))).unwrap_or(0);
                    let file = files[usize::try_from(rng.below(3)).unwrap_or(0)];
                    let key = key(file, section);
                    let len = size_of(section);
                    let fill =
                        u8::try_from((file.get() * 31 + u64::from(section)) % 251).unwrap_or(0);
                    let fail = rng.below(40) == 0;
                    let mode = if rng.below(5) == 0 {
                        CacheMode::Bypass
                    } else {
                        CacheMode::Normal
                    };
                    let load = move || {
                        if fail {
                            Err(io_error())
                        } else {
                            Ok(bytes(len, fill))
                        }
                    };
                    let result = if thread_index < BLOCKING_THREADS {
                        cache.get_or_load_blocking(key, class_of(section), mode, load)
                    } else {
                        block_on(cache.get_or_load(key, class_of(section), mode, &*executor, load))
                    };
                    if let Ok((bytes, _)) = result {
                        assert_eq!(bytes.len(), len, "a key always maps to its own bytes");
                        assert!(bytes.iter().all(|byte| *byte == fill), "and to its content");
                        if rng.below(3) == 0 {
                            pins.push(bytes);
                        }
                    }
                    while pins.len() > MAX_PINS {
                        pins.swap_remove(0);
                    }
                    match rng.below(200) {
                        0 => {
                            cache
                                .invalidate_file(files[usize::try_from(rng.below(3)).unwrap_or(0)]);
                        }
                        1 => cache.set_budget(budgets[usize::try_from(rng.below(4)).unwrap_or(0)]),
                        2 => cache.trim(),
                        _ => {}
                    }
                }
            })
        })
        .collect();
    for worker in workers {
        worker
            .join()
            .expect("worker finishes without a failed assertion");
    }
    stop.store(true, Ordering::Relaxed);
    checker.join().expect("the budget invariant held");
    executor.join();

    assert_consistent(&cache);
    for budget in budgets.into_iter().rev() {
        cache.set_budget(budget);
        assert!(cache.used() <= budget, "nothing is pinned once quiescent");
        assert_consistent(&cache);
    }
    assert_eq!(cache.used(), 0);
    assert_eq!(cache.stats().entries, 0);
}
