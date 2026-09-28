//! The engine-wide maintenance scheduler: permits for flush, compaction, and index-build jobs.
//!
//! A collection's writer decides that a job is due (a frozen memtable to flush, a set of
//! segments the compaction policy picked, or a segment whose vector graphs are missing) and
//! asks the scheduler for a permit. The scheduler grants permits in priority order and within
//! two limits:
//!
//! - **Slots.** At most `slots` jobs run at once, engine-wide, which is the number of job
//!   threads, so a granted job always starts right away. There are `max(2, threads) + 1`: one
//!   is always left for a flush, so a stream of long compactions never delays a flush, and
//!   with it the write stall a flush releases; one is the index-build slot, so a graph build
//!   (the longest job by far) never holds back a compaction; compactions may hold the rest.
//! - **Maintenance memory.** A compaction's or an index build's permit reserves the bytes its
//!   build holds (see the compaction policy's `build_bytes` and `index_build_bytes`) from the
//!   engine-wide pool of `maintenance_fraction * memory_limit`, and releases them when the
//!   permit is dropped. A job that does not fit in the free part of the pool waits; one larger
//!   than the whole pool is declined at once. A flush reserves what its build holds beyond the
//!   memtable (the builder's copy of the rows and its SQ8 and scalar index sections), but never
//!   waits for it and is never declined: its reservation only makes the other jobs wait while
//!   it runs, so the pool stays within its size whenever no flush runs.
//!
//! Waiting requests are granted flushes first, then compactions, then index builds, each in
//! request order. A waiting compaction that does not fit blocks the compactions behind it (so a
//! large job is never starved by smaller ones), but never a flush; index builds wait while any
//! compaction does, so graph builds use what memory and time compactions leave, and a graph is
//! built for the segments compaction settles on rather than for ones it is about to merge.
//!
//! The scheduler never calls into a collection while it holds its lock: a grant is delivered
//! (by sending the permit to the collection's writer) after the lock is released, and a permit
//! that cannot be delivered is simply dropped, which releases it.
//!
//! Tests can pause the scheduler: requests then queue until [`MaintenanceScheduler::step`]
//! grants them one at a time, which makes the order of grants observable and deterministic.

use crate::writer::JobKind;
use logpose_types::{LogPoseError, Result};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

/// Priority of a waiting request; lower is granted first.
fn priority(kind: JobKind) -> u8 {
    match kind {
        JobKind::Flush => 0,
        JobKind::Compact => 1,
        JobKind::Index => 2,
    }
}

/// Engine-wide maintenance permits. Cheap to clone; clones share the state.
#[derive(Clone)]
pub struct MaintenanceScheduler {
    shared: Arc<Shared>,
}

struct Shared {
    /// Jobs that may run at once, engine-wide.
    slots: usize,
    /// Of those, compactions.
    compaction_slots: usize,
    /// Of those, index builds.
    index_slots: usize,
    /// The maintenance-memory pool compactions and index builds reserve from.
    pool_bytes: u64,
    state: Mutex<State>,
}

type Deliver = Box<dyn FnOnce(Permit) + Send>;

struct Waiting {
    kind: JobKind,
    bytes: u64,
    deliver: Deliver,
}

#[derive(Default)]
struct State {
    /// Waiting requests by `(priority, request order)`.
    queue: BTreeMap<(u8, u64), Waiting>,
    next_request: u64,
    running_flushes: usize,
    running_compactions: usize,
    running_indexes: usize,
    /// Maintenance memory compaction and index-build permits reserve.
    reserved: u64,
    /// Maintenance memory flush permits reserve (granted without checking the pool).
    flush_reserved: u64,
    /// While paused, only [`MaintenanceScheduler::step`] grants, one request per step.
    paused: bool,
    steps: usize,
    stats: SchedulerStats,
}

/// Counters of what the scheduler did, for tests and diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SchedulerStats {
    /// Flush permits granted.
    pub flushes_granted: u64,
    /// Compaction permits granted.
    pub compactions_granted: u64,
    /// Index-build permits granted.
    pub index_builds_granted: u64,
    /// Compaction and index-build requests declined because they need more than the whole
    /// pool.
    pub declined: u64,
    /// Requests waiting now.
    pub waiting: usize,
    /// Jobs holding a permit now.
    pub running: usize,
    /// Maintenance memory compactions and index builds reserve now.
    pub reserved_bytes: u64,
    /// The most maintenance memory compactions and index builds ever reserved at once.
    pub peak_reserved_bytes: u64,
    /// Maintenance memory running flushes reserve now.
    pub flush_reserved_bytes: u64,
    /// The size of the maintenance-memory pool.
    pub pool_bytes: u64,
}

/// A granted permit. Dropping it releases its slot and its memory reservation, and grants the
/// next waiting request that now fits.
pub struct Permit {
    shared: Arc<Shared>,
    kind: JobKind,
    bytes: u64,
}

#[cfg(test)]
impl Permit {
    /// Maintenance memory the permit reserves.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl fmt::Debug for Permit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Permit")
            .field("kind", &self.kind)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        {
            let mut state = self.shared.lock();
            match self.kind {
                JobKind::Flush => {
                    state.running_flushes -= 1;
                    state.flush_reserved -= self.bytes;
                }
                JobKind::Compact => {
                    state.running_compactions -= 1;
                    state.reserved -= self.bytes;
                }
                JobKind::Index => {
                    state.running_indexes -= 1;
                    state.reserved -= self.bytes;
                }
            }
        }
        MaintenanceScheduler::pump(&self.shared);
    }
}

/// A request still waiting for its permit. Dropping the ticket does not cancel it; call
/// [`MaintenanceScheduler::cancel`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestId(u8, u64);

impl MaintenanceScheduler {
    /// A scheduler for `threads` maintenance threads (at least two: one is kept for flushes),
    /// plus one slot for index builds, and a maintenance-memory pool of `pool_bytes`.
    pub(crate) fn new(threads: usize, pool_bytes: u64) -> Self {
        let slots = threads.max(2);
        Self {
            shared: Arc::new(Shared {
                slots: slots + 1,
                compaction_slots: slots - 1,
                index_slots: 1,
                pool_bytes,
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Job threads the engine needs so that every granted job starts at once.
    pub(crate) fn slots(&self) -> usize {
        self.shared.slots
    }

    /// The maintenance-memory pool's size.
    #[must_use]
    pub fn pool_bytes(&self) -> u64 {
        self.shared.pool_bytes
    }

    /// Ask for a permit of `kind` reserving `bytes` of maintenance memory; `deliver` receives
    /// it once granted, on whatever thread released the resources (never under the scheduler's
    /// lock). Fails at once when a compaction or an index build needs more than the whole pool;
    /// a flush is granted whatever it reserves.
    pub(crate) fn request(
        &self,
        kind: JobKind,
        bytes: u64,
        deliver: impl FnOnce(Permit) + Send + 'static,
    ) -> Result<RequestId> {
        let id = {
            let mut state = self.shared.lock();
            if kind != JobKind::Flush && bytes > self.shared.pool_bytes {
                state.stats.declined += 1;
                return Err(LogPoseError::TooLarge {
                    what: match kind {
                        JobKind::Index => "index build memory",
                        _ => "compaction build memory",
                    }
                    .to_owned(),
                    size: Some(bytes),
                    limit: self.shared.pool_bytes,
                });
            }
            let id = RequestId(priority(kind), state.next_request);
            state.next_request += 1;
            state.queue.insert(
                (id.0, id.1),
                Waiting {
                    kind,
                    bytes,
                    deliver: Box::new(deliver),
                },
            );
            id
        };
        Self::pump(&self.shared);
        Ok(id)
    }

    /// Withdraw a request that has not been granted yet. Returns whether it was still waiting.
    pub(crate) fn cancel(&self, id: RequestId) -> bool {
        let removed = self.shared.lock().queue.remove(&(id.0, id.1));
        // Dropped outside the lock: the closure may own things whose drop takes other locks.
        removed.is_some()
    }

    /// Stop granting permits until [`resume`](Self::resume); [`step`](Self::step) grants one
    /// at a time meanwhile. For tests.
    pub fn pause(&self) {
        self.shared.lock().paused = true;
    }

    /// Grant permits freely again.
    pub fn resume(&self) {
        {
            let mut state = self.shared.lock();
            state.paused = false;
            state.steps = 0;
        }
        Self::pump(&self.shared);
    }

    /// While paused, allow `count` more grants (each to the first waiting request that fits,
    /// in priority order). Grants to requests made later use up the remaining steps.
    pub fn step(&self, count: usize) {
        self.shared.lock().steps += count;
        Self::pump(&self.shared);
    }

    /// What the scheduler did so far, and holds now.
    #[must_use]
    pub fn stats(&self) -> SchedulerStats {
        let state = self.shared.lock();
        SchedulerStats {
            waiting: state.queue.len(),
            running: state.running_flushes + state.running_compactions + state.running_indexes,
            reserved_bytes: state.reserved,
            flush_reserved_bytes: state.flush_reserved,
            pool_bytes: self.shared.pool_bytes,
            ..state.stats
        }
    }

    /// Grant every waiting request that fits, delivering each outside the lock.
    fn pump(shared: &Arc<Shared>) {
        while let Some((deliver, permit)) = Self::grant_one(shared) {
            deliver(permit);
        }
    }

    /// Take the first grantable request, if any, and account for its permit.
    fn grant_one(shared: &Arc<Shared>) -> Option<(Deliver, Permit)> {
        let mut state = shared.lock();
        if state.paused && state.steps == 0 {
            return None;
        }
        let running = state.running_flushes + state.running_compactions + state.running_indexes;
        if running >= shared.slots {
            return None;
        }
        let fits = |bytes: u64| state.reserved + state.flush_reserved + bytes <= shared.pool_bytes;
        let mut chosen = None;
        let mut compaction_seen = false;
        let mut index_seen = false;
        for (key, waiting) in &state.queue {
            match waiting.kind {
                JobKind::Flush => {
                    chosen = Some(*key);
                    break;
                }
                JobKind::Compact => {
                    // Only the first waiting compaction is a candidate: the ones behind it
                    // wait their turn even if they are smaller.
                    if compaction_seen {
                        continue;
                    }
                    compaction_seen = true;
                    if state.running_compactions < shared.compaction_slots && fits(waiting.bytes) {
                        chosen = Some(*key);
                        break;
                    }
                }
                JobKind::Index => {
                    // Index builds wait for every waiting compaction, and only the first
                    // waiting one is a candidate, like compactions.
                    if compaction_seen || index_seen {
                        break;
                    }
                    index_seen = true;
                    if state.running_indexes < shared.index_slots && fits(waiting.bytes) {
                        chosen = Some(*key);
                        break;
                    }
                }
            }
        }
        let waiting = state.queue.remove(&chosen?)?;
        if state.paused {
            state.steps -= 1;
        }
        match waiting.kind {
            JobKind::Flush => {
                state.running_flushes += 1;
                state.stats.flushes_granted += 1;
                state.flush_reserved += waiting.bytes;
            }
            JobKind::Compact => {
                state.running_compactions += 1;
                state.stats.compactions_granted += 1;
                state.reserved += waiting.bytes;
                state.stats.peak_reserved_bytes =
                    state.stats.peak_reserved_bytes.max(state.reserved);
            }
            JobKind::Index => {
                state.running_indexes += 1;
                state.stats.index_builds_granted += 1;
                state.reserved += waiting.bytes;
                state.stats.peak_reserved_bytes =
                    state.stats.peak_reserved_bytes.max(state.reserved);
            }
        }
        Some((
            waiting.deliver,
            Permit {
                shared: Arc::clone(shared),
                kind: waiting.kind,
                bytes: waiting.bytes,
            },
        ))
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Debug for MaintenanceScheduler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaintenanceScheduler")
            .field("slots", &self.shared.slots)
            .field("pool_bytes", &self.shared.pool_bytes)
            .field("stats", &self.stats())
            .finish()
    }
}

#[cfg(test)]
mod tests;
