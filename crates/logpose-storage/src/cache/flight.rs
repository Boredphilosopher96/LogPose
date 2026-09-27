//! Single-flight loads: one [`Flight`] per key being loaded, shared by every
//! caller that misses on the key while the load runs.
//!
//! A flight can be waited on from blocking code (a condition variable) and
//! from async code (wakers). Its loader either runs inline on the thread
//! that started it (the blocking path) or is stored in the flight and handed
//! to a [`LoadExecutor`] as a [`LoadJob`] (the async path). A blocking
//! caller that joins a flight whose stored loader has not started yet runs
//! it itself, so a blocking caller on an I/O thread can never wait for a job
//! queued behind it on the same pool.

use super::{AlignedBytes, ArtifactClass, CacheKey, Fetched, Inner, lock};
use crate::segment_v2::SegmentError;
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex, PoisonError},
    task::{Context, Poll, Waker},
    time::Instant,
};

/// A blocking load of one unit: read it, verify it, return its bytes.
pub type Loader = Box<dyn FnOnce() -> Result<AlignedBytes, SegmentError> + Send + 'static>;

/// What a finished load produced, as shared by its waiters.
#[derive(Clone, Debug)]
pub(super) struct Loaded {
    pub(super) bytes: Arc<AlignedBytes>,
    pub(super) micros: u64,
}

pub(super) type FlightResult = Result<Loaded, SegmentError>;

enum State {
    Pending(Vec<Waker>),
    Done(FlightResult),
}

/// One load in progress.
pub(super) struct Flight {
    pub(super) key: CacheKey,
    pub(super) class: ArtifactClass,
    /// Whether the flight is in its shard's `loading` map. A detached flight
    /// (a bypass load, or one whose file was invalidated) never inserts.
    pub(super) registered: bool,
    /// The loader, until some thread takes it to run it.
    loader: Mutex<Option<Loader>>,
    state: Mutex<State>,
    done: Condvar,
}

impl Flight {
    pub(super) fn new(
        key: CacheKey,
        class: ArtifactClass,
        registered: bool,
        loader: Option<Loader>,
    ) -> Arc<Self> {
        Arc::new(Self {
            key,
            class,
            registered,
            loader: Mutex::new(loader),
            state: Mutex::new(State::Pending(Vec::new())),
            done: Condvar::new(),
        })
    }

    fn take_loader(&self) -> Option<Loader> {
        lock(&self.loader).take()
    }

    /// Publish the result and wake every waiter. Only the first call has an
    /// effect.
    pub(super) fn complete(&self, result: FlightResult) {
        let wakers = {
            let mut state = lock(&self.state);
            match &mut *state {
                State::Done(_) => return,
                State::Pending(wakers) => {
                    let wakers = std::mem::take(wakers);
                    *state = State::Done(result);
                    wakers
                }
            }
        };
        self.done.notify_all();
        for waker in wakers {
            waker.wake();
        }
    }

    /// Wait from blocking code, running the stored loader if nobody has
    /// started it yet.
    pub(super) fn wait_blocking(self: &Arc<Self>, inner: &Arc<Inner>) -> FlightResult {
        if let Some(loader) = self.take_loader() {
            run(inner, self, loader);
        }
        let mut state = lock(&self.state);
        loop {
            match &*state {
                State::Done(result) => return result.clone(),
                State::Pending(_) => {
                    state = self
                        .done
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
    }

    fn poll_result(&self, waker: &Waker) -> Option<FlightResult> {
        let mut state = lock(&self.state);
        match &mut *state {
            State::Done(result) => Some(result.clone()),
            State::Pending(wakers) => {
                if !wakers.iter().any(|known| known.will_wake(waker)) {
                    wakers.push(waker.clone());
                }
                None
            }
        }
    }
}

/// Run `load` for `flight`, publish its result, and complete the flight.
/// If `load` panics, the guard completes the flight with
/// [`SegmentError::LoadAborted`] while the panic unwinds, so no waiter hangs.
pub(super) fn run<F>(inner: &Arc<Inner>, flight: &Arc<Flight>, load: F)
where
    F: FnOnce() -> Result<AlignedBytes, SegmentError>,
{
    let guard = AbortOnDrop { inner, flight };
    let start = Instant::now();
    let result = load();
    let micros = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
    std::mem::forget(guard);
    inner.finish(flight, result.map(|bytes| (bytes, micros)));
}

/// Completes a flight with [`SegmentError::LoadAborted`] unless forgotten.
struct AbortOnDrop<'a> {
    inner: &'a Arc<Inner>,
    flight: &'a Arc<Flight>,
}

impl Drop for AbortOnDrop<'_> {
    fn drop(&mut self) {
        self.inner.finish(self.flight, Err(SegmentError::LoadAborted));
    }
}

/// A queued load, handed to a [`LoadExecutor`].
///
/// Run it with [`run`](Self::run) on a thread that may block. Dropping it
/// without running it (an executor that has shut down) completes the load
/// with [`SegmentError::LoadAborted`], so its waiters do not hang.
#[must_use = "a load job must be run or dropped"]
pub struct LoadJob {
    inner: Option<Arc<Inner>>,
    flight: Arc<Flight>,
}

impl LoadJob {
    pub(super) fn new(inner: Option<Arc<Inner>>, flight: Arc<Flight>) -> Self {
        Self { inner, flight }
    }

    /// Run the load (read, verify, insert) on the current thread. Does
    /// nothing if a blocking caller already took the load over.
    pub fn run(self) {
        let Some(loader) = self.flight.take_loader() else {
            return;
        };
        match &self.inner {
            Some(inner) => run(inner, &self.flight, loader),
            None => run_detached(&self.flight, loader),
        }
    }
}

impl Drop for LoadJob {
    fn drop(&mut self) {
        if self.flight.take_loader().is_some() {
            match &self.inner {
                Some(inner) => inner.finish(&self.flight, Err(SegmentError::LoadAborted)),
                None => self.flight.complete(Err(SegmentError::LoadAborted)),
            }
        }
    }
}

impl fmt::Debug for LoadJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoadJob")
            .field("key", &self.flight.key)
            .field("class", &self.flight.class)
            .finish()
    }
}

/// Run a load that belongs to no cache.
fn run_detached(flight: &Arc<Flight>, loader: Loader) {
    struct Abort<'a>(&'a Flight);
    impl Drop for Abort<'_> {
        fn drop(&mut self) {
            self.0.complete(Err(SegmentError::LoadAborted));
        }
    }
    let guard = Abort(flight);
    let start = Instant::now();
    let result = loader();
    let micros = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
    std::mem::forget(guard);
    flight.complete(result.map(|bytes| Loaded {
        bytes: Arc::new(bytes),
        micros,
    }));
}

/// Where cache misses run their blocking reads: the engine's `IoPool`.
///
/// `execute` must eventually run the job on a thread that may block, or drop
/// it; it must not run it on a tokio worker or a rayon thread.
pub trait LoadExecutor: Send + Sync {
    /// Run or drop `job`.
    fn execute(&self, job: LoadJob);
}

/// Runs each job on the calling thread, before `execute` returns.
///
/// For tests, and for async callers that already run on an I/O thread.
#[derive(Clone, Copy, Debug, Default)]
pub struct InlineExecutor;

impl LoadExecutor for InlineExecutor {
    fn execute(&self, job: LoadJob) {
        job.run();
    }
}

/// The future returned by [`BufferCache::get_or_load`](super::BufferCache::get_or_load).
///
/// The load is registered and queued when `get_or_load` returns, not when
/// the future is first polled, so dropping the future never cancels a load
/// other callers wait on.
#[must_use = "futures do nothing unless polled"]
pub struct Fetch {
    state: FetchState,
}

enum FetchState {
    Ready(Option<Result<(Arc<AlignedBytes>, Fetched), SegmentError>>),
    Waiting { flight: Arc<Flight>, leader: bool },
    Finished,
}

impl Fetch {
    pub(super) fn ready(result: Result<(Arc<AlignedBytes>, Fetched), SegmentError>) -> Self {
        Self {
            state: FetchState::Ready(Some(result)),
        }
    }

    pub(super) fn waiting(flight: Arc<Flight>, leader: bool) -> Self {
        Self {
            state: FetchState::Waiting { flight, leader },
        }
    }

    /// Run `load` on `executor` outside any cache: nothing is looked up,
    /// shared, or inserted. Used by readers that have no cache attached.
    pub fn detached(
        executor: &dyn LoadExecutor,
        key: CacheKey,
        class: ArtifactClass,
        load: Loader,
    ) -> Self {
        let flight = Flight::new(key, class, false, Some(load));
        executor.execute(LoadJob::new(None, Arc::clone(&flight)));
        Self::waiting(flight, true)
    }
}

impl Future for Fetch {
    type Output = Result<(Arc<AlignedBytes>, Fetched), SegmentError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = match &mut this.state {
            FetchState::Ready(result) => result.take().unwrap_or(Err(SegmentError::LoadAborted)),
            FetchState::Waiting { flight, leader } => {
                let Some(result) = flight.poll_result(context.waker()) else {
                    return Poll::Pending;
                };
                let leader = *leader;
                result.map(|loaded| {
                    let how = fetched(leader, &loaded);
                    (loaded.bytes, how)
                })
            }
            FetchState::Finished => Err(SegmentError::LoadAborted),
        };
        this.state = FetchState::Finished;
        Poll::Ready(result)
    }
}

impl fmt::Debug for Fetch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.state {
            FetchState::Ready(_) => "ready",
            FetchState::Waiting { .. } => "waiting",
            FetchState::Finished => "finished",
        };
        formatter.debug_struct("Fetch").field("state", &state).finish()
    }
}

/// How a caller's report sees a finished flight: the caller that started it
/// missed and read the bytes; everyone else waited.
pub(super) fn fetched(leader: bool, loaded: &Loaded) -> Fetched {
    if leader {
        Fetched::Loaded {
            bytes: loaded.bytes.len() as u64,
            micros: loaded.micros,
        }
    } else {
        Fetched::Waited
    }
}
