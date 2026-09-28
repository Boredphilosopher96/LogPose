//! The engine-wide buffer cache of segment section bytes (D8: one memory
//! budget, positioned reads, no mmap).
//!
//! # Model
//!
//! - **Units.** An entry is one verified load unit of one segment file,
//!   named by a [`CacheKey`]: a whole section, the directory of a paged
//!   section (the `VectorF32` prefix or the `DynamicJson` block index), or
//!   one page or block. Bytes enter the cache only after their CRC matched.
//! - **Budget and classes.** Every entry is charged
//!   [`charge_for`]`(len)` bytes to one [`ArtifactClass`]. When the total
//!   exceeds the budget, the inserting thread evicts: from the
//!   lowest-priority class whose usage exceeds its floor, else from the
//!   lowest-priority non-empty class.
//! - **CLOCK.** One second-chance ring per class. A hit only sets the
//!   entry's `referenced` flag, so hits never contend on a list.
//! - **Pins.** A caller holds a unit by holding its `Arc<AlignedBytes>`
//!   (usually in a [`PinSet`]). An entry whose bytes are shared outside the
//!   cache is pinned and is never evicted; when nothing but pinned entries is
//!   left the insert still succeeds and [`CacheStats::overcommit_bytes`]
//!   records the excess.
//! - **Single flight.** Concurrent misses on one key share one load. A
//!   failed load (I/O error, CRC mismatch, panic) is never cached; every
//!   waiter gets the error and the next access loads again.
//! - **Invalidation.** [`BufferCache::invalidate_file`] drops a file's
//!   entries and detaches its loads in flight (their waiters still get the
//!   bytes, which are not inserted). A reader with a cache attached calls it
//!   when dropped.
//!
//! # Locking
//!
//! Shards and rings are `std::sync::Mutex`es held only for map and queue
//! operations, never across a load and never two at once (the design's
//! no-nested-locks rule): eviction pops a candidate from a ring, releases
//! the ring, then locks the candidate's shard.

mod bytes;
mod flight;
mod key;
mod report;
mod warm;

#[cfg(test)]
mod tests;

pub use bytes::AlignedBytes;
pub use flight::{Fetch, InlineExecutor, LoadExecutor, LoadJob, Loader};
pub use key::{ArtifactClass, CacheKey, CacheUnit, DEFAULT_FLOORS, FileId, KeyHasher, KeyMap};
pub use report::{CacheStats, ClassFetch, FetchReport, Fetched, PinSet};
pub use warm::{WARM_UP_FILL, WARM_UP_IN_FLIGHT, WarmUpItem, WarmUpReport};

use crate::segment_v2::SegmentError;
use flight::{Flight, FlightResult, Loaded};
use std::{
    collections::VecDeque,
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

/// Number of shards of the key maps.
const SHARDS: usize = 64;

/// Fixed bookkeeping charged per entry on top of its bytes: the map slot,
/// the entry, and its ring slot.
pub const ENTRY_OVERHEAD: u64 = 128;

/// Bytes charged against the budget for a unit of `len` bytes.
#[must_use]
pub fn charge_for(len: usize) -> u64 {
    (len as u64).div_ceil(8) * 8 + ENTRY_OVERHEAD
}

/// Whether a load may enter the cache.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CacheMode {
    /// Look up, share in-flight loads, and insert what is loaded.
    #[default]
    Normal,
    /// For full scans (compaction, rebuilds, `verify`): use a resident entry
    /// or join a load in flight, but never insert and never mark entries
    /// referenced, so a scan does not flush the hot set.
    Bypass,
}

/// Cache sizing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CacheConfig {
    /// Budget in bytes (see [`BudgetInputs::cache_budget`]).
    pub budget: u64,
    /// Per-class floors as fractions of the budget, indexed by
    /// [`ArtifactClass::index`].
    pub floors: [f32; ArtifactClass::COUNT],
}

impl CacheConfig {
    /// A configuration with `budget` bytes and the default floors.
    #[must_use]
    pub fn with_budget(budget: u64) -> Self {
        Self {
            budget,
            floors: DEFAULT_FLOORS,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self::with_budget(1 << 30)
    }
}

/// The reservations the cache budget is derived from.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BudgetInputs {
    /// `storage.memory_limit`.
    pub memory_limit: u64,
    /// Sum of the writers' primary-key index sizes.
    pub pk_index_bytes: u64,
    /// The memtable reservation (`global_fraction * memory_limit`).
    pub memtable_bytes: u64,
    /// The maintenance-memory reservation for flush and compaction builds.
    pub maintenance_bytes: u64,
}

impl BudgetInputs {
    /// Share of `memory_limit` kept for query working memory (bitmaps,
    /// heaps, visited sets).
    pub const QUERY_RESERVE: f64 = 0.10;

    /// `memory_limit - pk index - memtables - query reserve - maintenance`,
    /// saturating at zero.
    #[must_use]
    pub fn cache_budget(&self) -> u64 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let query_reserve = (self.memory_limit as f64 * Self::QUERY_RESERVE) as u64;
        self.memory_limit
            .saturating_sub(self.pk_index_bytes)
            .saturating_sub(self.memtable_bytes)
            .saturating_sub(query_reserve)
            .saturating_sub(self.maintenance_bytes)
    }
}

/// The engine-wide buffer cache. Cheap to clone; clones share the cache.
#[derive(Clone)]
pub struct BufferCache {
    inner: Arc<Inner>,
}

/// A handle that does not keep the cache alive.
#[derive(Clone, Debug)]
pub struct WeakBufferCache {
    inner: Weak<Inner>,
}

impl WeakBufferCache {
    /// The cache, if it still exists.
    #[must_use]
    pub fn upgrade(&self) -> Option<BufferCache> {
        self.inner.upgrade().map(|inner| BufferCache { inner })
    }
}

pub(crate) struct Inner {
    budget: AtomicU64,
    floors: [f32; ArtifactClass::COUNT],
    used: [AtomicU64; ArtifactClass::COUNT],
    shards: Box<[Mutex<Shard>]>,
    clocks: [Mutex<VecDeque<Weak<Entry>>>; ArtifactClass::COUNT],
    counters: Counters,
    /// Usage below which inserts skip their sweep, after a pass ended
    /// overcommitted; 0 when the last pass reached the budget.
    sweep_resume_at: AtomicU64,
}

#[derive(Default)]
struct Shard {
    entries: KeyMap<Arc<Entry>>,
    loading: KeyMap<Arc<Flight>>,
}

struct Entry {
    key: CacheKey,
    class: ArtifactClass,
    bytes: Arc<AlignedBytes>,
    /// Bytes charged to the class: the buffer, [`ENTRY_OVERHEAD`], and the decoded form once
    /// it is charged.
    charge: AtomicU64,
    /// Whether the decoded form's heap bytes are part of `charge`: set at insert when a loader
    /// attached one, and by [`BufferCache::charge_decoded`] for one attached on first use.
    decoded_charged: AtomicBool,
    referenced: AtomicBool,
}

impl Entry {
    /// Whether anyone outside the cache holds the bytes.
    fn is_pinned(&self) -> bool {
        Arc::strong_count(&self.bytes) > 1
    }
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    waits: AtomicU64,
    failed_loads: AtomicU64,
    evictions: AtomicU64,
    evicted_bytes: AtomicU64,
    invalidated: AtomicU64,
    overcommits: AtomicU64,
    overcommit_bytes: AtomicU64,
}

/// Lock a mutex, recovering from poisoning: every critical section is a
/// single map or queue operation, so a panic cannot leave one half done.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Outcome of the first, locked step of an access.
enum Lookup {
    Hit(Arc<AlignedBytes>),
    Join(Arc<Flight>),
    Lead(Arc<Flight>),
    Private,
}

impl BufferCache {
    /// An empty cache.
    #[must_use]
    pub fn new(config: CacheConfig) -> Self {
        let floors = config.floors.map(|floor| {
            if floor.is_finite() {
                floor.clamp(0.0, 1.0)
            } else {
                0.0
            }
        });
        Self {
            inner: Arc::new(Inner {
                budget: AtomicU64::new(config.budget),
                floors,
                used: Default::default(),
                shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
                clocks: Default::default(),
                counters: Counters::default(),
                sweep_resume_at: AtomicU64::new(0),
            }),
        }
    }

    /// A handle that does not keep the cache alive.
    #[must_use]
    pub fn downgrade(&self) -> WeakBufferCache {
        WeakBufferCache {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Current budget.
    #[must_use]
    pub fn budget(&self) -> u64 {
        self.inner.budget.load(Ordering::Relaxed)
    }

    /// Change the budget (the engine recomputes it as the pk index grows)
    /// and evict down to it.
    pub fn set_budget(&self, budget: u64) {
        self.inner.budget.store(budget, Ordering::Relaxed);
        self.inner.evict_to_budget();
    }

    /// Evict unpinned entries until the cache is within budget. Inserts do
    /// this themselves; call it after releasing many pins to return memory
    /// promptly.
    pub fn trim(&self) {
        self.inner.evict_to_budget();
    }

    /// Total charged bytes.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.inner.used_total()
    }

    /// Whether `key` is resident.
    #[must_use]
    pub fn residency(&self, key: &CacheKey) -> bool {
        lock(self.inner.shard(key)).entries.contains_key(key)
    }

    /// The bytes of `key` if it is resident, as a hit, without loading anything: for compute
    /// stages, which must not do I/O, to pin units that are already in memory. A unit that is
    /// not resident (or still loading) is `None` and counts nothing; the caller fetches it.
    #[must_use]
    pub fn get_resident(&self, key: &CacheKey) -> Option<Arc<AlignedBytes>> {
        let shard = lock(self.inner.shard(key));
        let entry = shard.entries.get(key)?;
        entry.referenced.store(true, Ordering::Relaxed);
        let bytes = Arc::clone(&entry.bytes);
        drop(shard);
        self.inner.counters.hits.fetch_add(1, Ordering::Relaxed);
        Some(bytes)
    }

    /// Charge the decoded form attached to `key`'s bytes after they were cached (a scalar
    /// column, a dynamic block, or a vector prefix decodes on first use) to its class, once, and
    /// evict if that took the cache over budget. Nothing happens for a key that is not resident,
    /// whose decoded form is already charged, or whose bytes carry none.
    pub fn charge_decoded(&self, key: &CacheKey) {
        let mut charged = false;
        {
            let shard = lock(self.inner.shard(key));
            if let Some(entry) = shard.entries.get(key) {
                let extra = entry.bytes.decoded_heap_bytes();
                if entry.bytes.has_decoded() && !entry.decoded_charged.swap(true, Ordering::Relaxed)
                {
                    entry.charge.fetch_add(extra, Ordering::Relaxed);
                    self.inner.used[entry.class.index()].fetch_add(extra, Ordering::Relaxed);
                    charged = extra > 0;
                }
            }
        }
        if charged {
            self.inner.evict_after_insert();
        }
    }

    /// Counters and usage.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        let inner = &self.inner;
        let counters = &inner.counters;
        let entries = inner
            .shards
            .iter()
            .map(|shard| lock(shard).entries.len() as u64)
            .sum();
        CacheStats {
            budget: self.budget(),
            used: std::array::from_fn(|class| inner.used[class].load(Ordering::Relaxed)),
            entries,
            hits: counters.hits.load(Ordering::Relaxed),
            misses: counters.misses.load(Ordering::Relaxed),
            waits: counters.waits.load(Ordering::Relaxed),
            failed_loads: counters.failed_loads.load(Ordering::Relaxed),
            evictions: counters.evictions.load(Ordering::Relaxed),
            evicted_bytes: counters.evicted_bytes.load(Ordering::Relaxed),
            invalidated: counters.invalidated.load(Ordering::Relaxed),
            overcommits: counters.overcommits.load(Ordering::Relaxed),
            overcommit_bytes: counters.overcommit_bytes.load(Ordering::Relaxed),
        }
    }

    /// Get `key`, loading it with `load` on the calling thread on a miss.
    ///
    /// For code that already runs on an I/O thread (segment readers, jobs,
    /// warm-up loaders). If another caller is loading the key, this waits
    /// for it; if that load is queued and has not started, this runs it
    /// here instead, so it never waits behind its own pool's queue.
    ///
    /// # Errors
    ///
    /// The load's error, shared with every caller that waited on it.
    pub fn get_or_load_blocking<F>(
        &self,
        key: CacheKey,
        class: ArtifactClass,
        mode: CacheMode,
        load: F,
    ) -> Result<(Arc<AlignedBytes>, Fetched), SegmentError>
    where
        F: FnOnce() -> Result<AlignedBytes, SegmentError>,
    {
        match self.inner.lookup(key, class, mode, None) {
            Lookup::Hit(bytes) => Ok((bytes, Fetched::Hit)),
            Lookup::Join(flight) => flight
                .wait_blocking(&self.inner)
                .map(|loaded| (loaded.bytes, Fetched::Waited)),
            Lookup::Lead(flight) => {
                flight::run(&self.inner, &flight, load);
                flight
                    .wait_blocking(&self.inner)
                    .map(|loaded| (Arc::clone(&loaded.bytes), flight::fetched(true, &loaded)))
            }
            Lookup::Private => {
                let flight = Flight::new(key, class, false, None);
                flight::run(&self.inner, &flight, load);
                flight
                    .wait_blocking(&self.inner)
                    .map(|loaded| (Arc::clone(&loaded.bytes), flight::fetched(true, &loaded)))
            }
        }
    }

    /// Get `key`; on a miss, run `load` on `executor` and resolve when it
    /// finishes.
    ///
    /// The lookup, the single-flight registration, and the hand-off to the
    /// executor all happen before this returns, so the returned future only
    /// waits: dropping it cancels nothing.
    pub fn get_or_load<F>(
        &self,
        key: CacheKey,
        class: ArtifactClass,
        mode: CacheMode,
        executor: &dyn LoadExecutor,
        load: F,
    ) -> Fetch
    where
        F: FnOnce() -> Result<AlignedBytes, SegmentError> + Send + 'static,
    {
        let mut load = Some(load);
        let lookup = self.inner.lookup(
            key,
            class,
            mode,
            Some(&mut || load.take().map(|load| Box::new(load) as Loader)),
        );
        match lookup {
            Lookup::Hit(bytes) => Fetch::ready(Ok((bytes, Fetched::Hit))),
            Lookup::Join(flight) => Fetch::waiting(flight, false),
            Lookup::Lead(flight) => {
                executor.execute(LoadJob::new(
                    Some(Arc::clone(&self.inner)),
                    Arc::clone(&flight),
                ));
                Fetch::waiting(flight, true)
            }
            Lookup::Private => {
                let loader = load.take().map(|load| Box::new(load) as Loader);
                let flight = Flight::new(key, class, false, loader);
                executor.execute(LoadJob::new(
                    Some(Arc::clone(&self.inner)),
                    Arc::clone(&flight),
                ));
                Fetch::waiting(flight, true)
            }
        }
    }

    /// Drop every entry of `file` and detach its loads in flight. Called
    /// when the file's last handle is dropped; file ids are never reused.
    ///
    /// Returns the number of entries removed.
    pub fn invalidate_file(&self, file: FileId) -> usize {
        let inner = &self.inner;
        let mut removed = 0;
        for shard in inner.shards.iter() {
            let mut shard = lock(shard);
            shard.loading.retain(|key, _| key.file != file);
            shard.entries.retain(|key, entry| {
                if key.file == file {
                    inner.used[entry.class.index()]
                        .fetch_sub(entry.charge.load(Ordering::Relaxed), Ordering::Relaxed);
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        }
        if removed > 0 {
            // The removed entries' ring slots are dead now. Eviction would
            // skip them, but it runs only over budget, so without this a
            // cache that never fills keeps one slot (and the entry's
            // allocation) for every unit ever loaded from a dropped file.
            for ring in &inner.clocks {
                lock(ring).retain(|slot| slot.strong_count() > 0);
            }
        }
        inner
            .counters
            .invalidated
            .fetch_add(removed as u64, Ordering::Relaxed);
        removed
    }

    /// Warm the cache after open: load `items` class by class, highest
    /// priority first, with at most [`WARM_UP_IN_FLIGHT`] loads on
    /// `executor` at once. A class stops at the first item that would take
    /// the cache past [`WARM_UP_FILL`] of its budget (counting loads in
    /// flight); the next class gets what room is left. Resident items are
    /// skipped and failures are counted, never fatal.
    pub async fn warm_up(
        &self,
        items: Vec<WarmUpItem>,
        executor: &dyn LoadExecutor,
    ) -> WarmUpReport {
        warm::warm_up(self, items, executor).await
    }
}

impl fmt::Debug for BufferCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BufferCache")
            .field("budget", &self.budget())
            .field("used", &self.used())
            .finish_non_exhaustive()
    }
}

impl Inner {
    fn shard(&self, key: &CacheKey) -> &Mutex<Shard> {
        let mut hasher = KeyHasher::default();
        key.hash(&mut hasher);
        // The multiply leaves the best-mixed bits at the top.
        let index = usize::try_from(hasher.finish() >> (64 - SHARDS.trailing_zeros())).unwrap_or(0)
            % SHARDS;
        &self.shards[index]
    }

    fn used_total(&self) -> u64 {
        self.used
            .iter()
            .map(|used| used.load(Ordering::Relaxed))
            .sum()
    }

    fn floor_bytes(&self, class: ArtifactClass, budget: u64) -> u64 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let floor = (budget as f64 * f64::from(self.floors[class.index()])) as u64;
        floor
    }

    /// The locked first step of an access. `loader` supplies the stored
    /// loader when this call starts a flight that runs on an executor.
    fn lookup(
        &self,
        key: CacheKey,
        class: ArtifactClass,
        mode: CacheMode,
        loader: Option<&mut dyn FnMut() -> Option<Loader>>,
    ) -> Lookup {
        let mut shard = lock(self.shard(&key));
        if let Some(entry) = shard.entries.get(&key) {
            if mode == CacheMode::Normal {
                entry.referenced.store(true, Ordering::Relaxed);
            }
            let bytes = Arc::clone(&entry.bytes);
            drop(shard);
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            return Lookup::Hit(bytes);
        }
        if let Some(flight) = shard.loading.get(&key) {
            let flight = Arc::clone(flight);
            drop(shard);
            self.counters.waits.fetch_add(1, Ordering::Relaxed);
            return Lookup::Join(flight);
        }
        self.counters.misses.fetch_add(1, Ordering::Relaxed);
        if mode == CacheMode::Bypass {
            return Lookup::Private;
        }
        let stored = loader.and_then(|loader| loader());
        let flight = Flight::new(key, class, true, stored);
        shard.loading.insert(key, Arc::clone(&flight));
        Lookup::Lead(flight)
    }

    /// Publish a flight's outcome: insert on success (if still registered),
    /// unregister, complete the flight, then evict.
    fn finish(&self, flight: &Arc<Flight>, result: Result<(AlignedBytes, u64), SegmentError>) {
        let outcome: FlightResult = match result {
            Ok((bytes, micros)) => Ok(Loaded {
                bytes: Arc::new(bytes),
                micros,
            }),
            Err(error) => {
                self.counters.failed_loads.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        };
        let mut inserted = None;
        if flight.registered {
            let mut shard = lock(self.shard(&flight.key));
            let current = shard
                .loading
                .get(&flight.key)
                .is_some_and(|registered| Arc::ptr_eq(registered, flight));
            if current {
                shard.loading.remove(&flight.key);
                if let Ok(loaded) = &outcome {
                    let decoded = loaded.bytes.decoded_heap_bytes();
                    let charge = charge_for(loaded.bytes.len()) + decoded;
                    let entry = Arc::new(Entry {
                        key: flight.key,
                        class: flight.class,
                        bytes: Arc::clone(&loaded.bytes),
                        charge: AtomicU64::new(charge),
                        decoded_charged: AtomicBool::new(loaded.bytes.has_decoded()),
                        referenced: AtomicBool::new(false),
                    });
                    self.used[entry.class.index()].fetch_add(charge, Ordering::Relaxed);
                    if let Some(old) = shard.entries.insert(flight.key, Arc::clone(&entry)) {
                        self.used[old.class.index()]
                            .fetch_sub(old.charge.load(Ordering::Relaxed), Ordering::Relaxed);
                    }
                    inserted = Some(entry);
                }
            }
        }
        if let Some(entry) = &inserted {
            lock(&self.clocks[entry.class.index()]).push_back(Arc::downgrade(entry));
        }
        flight.complete(outcome);
        if inserted.is_some() {
            self.evict_after_insert();
        }
    }

    /// The sweep an insert runs, with hysteresis: after a pass that ended
    /// overcommitted (only pinned entries were left to evict), inserts skip
    /// sweeping until usage grows by a sixty-fourth of the budget past where
    /// that pass ended. Without it, a query stage that pins a large set makes
    /// every insert rescan every ring (O(entries) per insert). Usage may
    /// therefore exceed `budget + pinned` by up to that margin until the next
    /// sweep; `trim` and `set_budget` always sweep.
    fn evict_after_insert(&self) {
        if self.used_total() < self.sweep_resume_at.load(Ordering::Relaxed) {
            return;
        }
        self.evict_to_budget();
    }

    /// Evict until within budget, or until nothing is evictable.
    fn evict_to_budget(&self) {
        let mut exhausted = [false; ArtifactClass::COUNT];
        loop {
            let budget = self.budget.load(Ordering::Relaxed);
            let used = self.used_total();
            if used <= budget {
                self.counters.overcommit_bytes.store(0, Ordering::Relaxed);
                self.sweep_resume_at.store(0, Ordering::Relaxed);
                return;
            }
            let Some(class) = self.victim_class(budget, &exhausted) else {
                self.counters.overcommits.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .overcommit_bytes
                    .store(used - budget, Ordering::Relaxed);
                self.sweep_resume_at
                    .store(used.saturating_add(budget / 64), Ordering::Relaxed);
                return;
            };
            if !self.evict_one(class) {
                exhausted[class.index()] = true;
            }
        }
    }

    /// The lowest-priority class whose usage exceeds its floor; if none,
    /// the lowest-priority non-empty class. Classes already swept without
    /// result are skipped.
    ///
    /// The fallback applies only when no class exceeds its floor (floors
    /// that add up to more than the budget). When the classes over their
    /// floors are all pinned, the pass ends overcommitted instead: evicting
    /// a class below its floor would let pressure from a higher class take
    /// the share the floor reserves.
    fn victim_class(
        &self,
        budget: u64,
        exhausted: &[bool; ArtifactClass::COUNT],
    ) -> Option<ArtifactClass> {
        let used = |class: ArtifactClass| self.used[class.index()].load(Ordering::Relaxed);
        let over_floor = |class: &ArtifactClass| used(*class) > self.floor_bytes(*class, budget);
        let lowest_first = || ArtifactClass::ALL.into_iter().rev();
        if lowest_first().any(|class| over_floor(&class)) {
            lowest_first()
                .filter(|class| !exhausted[class.index()])
                .find(over_floor)
        } else {
            lowest_first()
                .filter(|class| !exhausted[class.index()])
                .find(|class| used(*class) > 0)
        }
    }

    /// Advance `class`'s CLOCK hand until one entry is evicted. Returns
    /// `false` after a full sweep finds nothing evictable.
    ///
    /// The sweep visits each entry at most twice: on the first lap a
    /// `referenced` entry gets its second chance; on the second lap the flag
    /// is ignored, so hits racing with the sweep cannot keep an unpinned
    /// entry resident over budget.
    fn evict_one(&self, class: ArtifactClass) -> bool {
        let ring = &self.clocks[class.index()];
        let lap = lock(ring).len();
        let mut visits = 0;
        loop {
            let candidate = {
                let mut ring = lock(ring);
                let mut found = None;
                while visits < lap * 2 {
                    let Some(weak) = ring.pop_front() else {
                        break;
                    };
                    visits += 1;
                    let Some(entry) = weak.upgrade() else {
                        continue;
                    };
                    let second_lap = visits > lap;
                    if !second_lap && entry.referenced.swap(false, Ordering::Relaxed) {
                        ring.push_back(weak);
                        continue;
                    }
                    if entry.is_pinned() {
                        ring.push_back(weak);
                        continue;
                    }
                    found = Some(entry);
                    break;
                }
                found
            };
            let Some(entry) = candidate else {
                return false;
            };
            let mut shard = lock(self.shard(&entry.key));
            let resident = shard
                .entries
                .get(&entry.key)
                .is_some_and(|current| Arc::ptr_eq(current, &entry));
            if !resident {
                // Invalidated while we looked at it; its ring slot is gone.
                continue;
            }
            if entry.is_pinned() {
                drop(shard);
                lock(ring).push_back(Arc::downgrade(&entry));
                continue;
            }
            shard.entries.remove(&entry.key);
            let charge = entry.charge.load(Ordering::Relaxed);
            self.used[class.index()].fetch_sub(charge, Ordering::Relaxed);
            drop(shard);
            self.counters.evictions.fetch_add(1, Ordering::Relaxed);
            self.counters
                .evicted_bytes
                .fetch_add(charge, Ordering::Relaxed);
            return true;
        }
    }
}
