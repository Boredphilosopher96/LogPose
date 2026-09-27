//! The engine's thread pools: the blocking [`IoPool`], the `query` and `maintenance` rayon pools,
//! and [`run_cpu`] to await rayon work from async code.
//!
//! Tokio workers only orchestrate. Blocking syscalls run on the `IoPool`, latency-sensitive CPU
//! work on the `query` pool, and long-running flush and compaction CPU work on the smaller
//! `maintenance` pool so that it cannot starve queries.

use crate::cache::{LoadExecutor, LoadJob};
use logpose_types::{LogPoseError, Result};
use std::{
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, PoisonError,
        mpsc::{self, Receiver, Sender},
    },
    thread,
};
use tokio::sync::{Semaphore, oneshot};

/// Sizes of the engine's thread pools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    /// Blocking I/O threads. Default 8.
    pub io_threads: usize,
    /// Jobs the I/O pool accepts before `IoPool::run` waits for a slot. Default 1024.
    pub io_queue_depth: usize,
    /// Threads of the `query` rayon pool. Default `available_parallelism`.
    pub query_threads: usize,
    /// Threads of the `maintenance` rayon pool. Default `max(1, available_parallelism / 4)`.
    pub maintenance_threads: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let parallelism = thread::available_parallelism().map_or(1, usize::from);
        Self {
            io_threads: 8,
            io_queue_depth: 1024,
            query_threads: parallelism,
            maintenance_threads: (parallelism / 4).max(1),
        }
    }
}

/// The engine's thread pools. One per [`Engine`](crate::Engine).
pub struct Runtime {
    /// Blocking file I/O.
    pub io: IoPool,
    /// Latency-sensitive CPU work: query scoring, bitmap compilation, batch preparation.
    pub query: rayon::ThreadPool,
    /// Long-running CPU work: flush and compaction encoding and index builds.
    pub maintenance: rayon::ThreadPool,
}

impl Runtime {
    /// Start every pool.
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        Ok(Self {
            io: IoPool::new("logpose-io", config.io_threads, config.io_queue_depth)?,
            query: rayon_pool("logpose-query", config.query_threads)?,
            maintenance: rayon_pool("logpose-maintenance", config.maintenance_threads)?,
        })
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Runtime")
            .field("io", &self.io)
            .field("query_threads", &self.query.current_num_threads())
            .field(
                "maintenance_threads",
                &self.maintenance.current_num_threads(),
            )
            .finish()
    }
}

fn rayon_pool(name: &'static str, threads: usize) -> Result<rayon::ThreadPool> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .thread_name(move |index| format!("{name}-{index}"))
        .build()
        .map_err(|error| {
            LogPoseError::internal(format!("failed to start the {name} pool: {error}"))
        })
}

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Fixed-size pool of blocking threads with a bounded queue.
///
/// A dedicated pool rather than `tokio::task::spawn_blocking`: `spawn_blocking` shares a
/// 512-thread default with everything else and exposes no queue depth. Dropping the pool closes
/// its queue; threads exit after the jobs already queued have run.
pub struct IoPool {
    name: &'static str,
    threads: usize,
    queue_depth: usize,
    sender: Sender<Job>,
    /// One permit per queue slot. `run` holds a permit from submission until its job finishes.
    permits: Arc<Semaphore>,
}

impl IoPool {
    /// Start `threads` threads named `<name>-<index>` behind a queue of `queue_depth` jobs.
    pub fn new(name: &'static str, threads: usize, queue_depth: usize) -> Result<Self> {
        let threads = threads.max(1);
        let queue_depth = queue_depth.max(1);
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..threads {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("{name}-{index}"))
                .spawn(move || worker(&receiver))
                .map_err(|error| {
                    LogPoseError::internal(format!("failed to start the {name} pool: {error}"))
                })?;
        }
        Ok(Self {
            name,
            threads,
            queue_depth,
            sender,
            permits: Arc::new(Semaphore::new(queue_depth)),
        })
    }

    /// Run `f` on an I/O thread; the returned future resolves with its result.
    ///
    /// Waits (asynchronously) for a queue slot when `queue_depth` jobs are already queued or
    /// running. A panic in `f` is reported as an error, and the thread keeps serving.
    pub fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = Result<T>> + Send + 'static {
        let permits = Arc::clone(&self.permits);
        let sender = self.sender.clone();
        let name = self.name;
        async move {
            let permit = permits
                .acquire_owned()
                .await
                .map_err(|_| pool_closed(name))?;
            let (result_sender, result) = oneshot::channel();
            sender
                .send(Box::new(move || {
                    let outcome = catch_unwind(AssertUnwindSafe(f));
                    drop(permit);
                    let _ = result_sender.send(outcome);
                }))
                .map_err(|_| pool_closed(name))?;
            match result.await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(_)) => Err(task_panicked(name)),
                Err(_) => Err(pool_closed(name)),
            }
        }
    }

    /// Queue `f` without waiting for a slot and without a result.
    ///
    /// For engine-internal fan-out whose width is already bounded by the caller, such as
    /// opening every collection at engine open.
    pub fn execute(&self, f: impl FnOnce() + Send + 'static) -> Result<()> {
        self.sender
            .send(Box::new(move || {
                let _ = catch_unwind(AssertUnwindSafe(f));
            }))
            .map_err(|_| pool_closed(self.name))
    }

    /// Number of threads.
    #[must_use]
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Jobs submitted through [`IoPool::run`] that are queued or running.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.queue_depth - self.permits.available_permits()
    }
}

impl fmt::Debug for IoPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IoPool")
            .field("name", &self.name)
            .field("threads", &self.threads)
            .field("queue_depth", &self.queue_depth)
            .field("in_flight", &self.in_flight())
            .finish()
    }
}

/// Buffer cache misses run on the I/O pool. A job the pool cannot accept (it has shut down) is
/// dropped, which fails the load with `SegmentError::LoadAborted` for every waiter.
impl LoadExecutor for IoPool {
    fn execute(&self, job: LoadJob) {
        let _ = IoPool::execute(self, move || job.run());
    }
}

fn worker(receiver: &Mutex<Receiver<Job>>) {
    loop {
        // The guard is dropped at the end of this statement, before the job runs.
        let job = receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .recv();
        match job {
            Ok(job) => job(),
            Err(_) => return,
        }
    }
}

/// Run `f` on a rayon pool and await it from async code.
///
/// `f` must not block on a future or do I/O: a rayon thread that blocks can deadlock work
/// stealing. A panic in `f` is reported as an error.
pub fn run_cpu<T: Send + 'static>(
    pool: &rayon::ThreadPool,
    f: impl FnOnce() -> T + Send + 'static,
) -> impl Future<Output = Result<T>> + Send + 'static {
    let (result_sender, result) = oneshot::channel();
    pool.spawn(move || {
        let _ = result_sender.send(catch_unwind(AssertUnwindSafe(f)));
    });
    async move {
        match result.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(task_panicked("rayon")),
            Err(_) => Err(pool_closed("rayon")),
        }
    }
}

fn pool_closed(name: &str) -> LogPoseError {
    LogPoseError::unavailable(format!("the {name} pool is shut down"))
}

fn task_panicked(name: &str) -> LogPoseError {
    LogPoseError::internal(format!("a task on the {name} pool panicked"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    #[tokio::test]
    async fn io_pool_runs_jobs_off_the_calling_thread() {
        let pool = IoPool::new("test-io", 2, 8).expect("pool should start");
        let caller = thread::current().id();
        let (worker_thread, name) = pool
            .run(|| {
                let current = thread::current();
                (current.id(), current.name().map(str::to_owned))
            })
            .await
            .expect("job should run");
        assert_ne!(worker_thread, caller);
        assert!(name.is_some_and(|name| name.starts_with("test-io-")));
    }

    #[tokio::test]
    async fn io_pool_runs_up_to_its_thread_count_concurrently() {
        let pool = IoPool::new("test-io", 3, 16).expect("pool should start");
        let barrier = Arc::new(Barrier::new(3));
        // Spawned so that all three are submitted before any is awaited.
        let jobs = (0..3)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                tokio::spawn(pool.run(move || barrier.wait().is_leader()))
            })
            .collect::<Vec<_>>();
        let mut leaders = 0;
        for job in jobs {
            if job
                .await
                .expect("task should join")
                .expect("job should run")
            {
                leaders += 1;
            }
        }
        assert_eq!(leaders, 1, "all three jobs must have met at the barrier");
        assert_eq!(pool.in_flight(), 0);
    }

    #[tokio::test]
    async fn io_pool_bounds_its_queue() {
        let pool = IoPool::new("test-io", 1, 2).expect("pool should start");
        let (release, gate) = mpsc::channel::<()>();
        let gate = Arc::new(Mutex::new(gate));
        let started = Arc::new(AtomicUsize::new(0));
        let blocked = (0..2)
            .map(|_| {
                let gate = Arc::clone(&gate);
                let started = Arc::clone(&started);
                tokio::spawn(pool.run(move || {
                    started.fetch_add(1, Ordering::SeqCst);
                    let _ = gate.lock().unwrap_or_else(PoisonError::into_inner).recv();
                }))
            })
            .collect::<Vec<_>>();
        while pool.in_flight() < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let third = tokio::time::timeout(Duration::from_millis(50), pool.run(|| ())).await;
        assert!(third.is_err(), "a third job must wait for a queue slot");

        release.send(()).expect("gate should be open");
        release.send(()).expect("gate should be open");
        for job in blocked {
            job.await
                .expect("task should join")
                .expect("job should run");
        }
        pool.run(|| ()).await.expect("a slot should be free again");
        assert_eq!(started.load(Ordering::SeqCst), 2);
    }

    // The panics are the point: a panicking job must be reported, not take the pool down.
    #[allow(clippy::panic)]
    #[tokio::test]
    async fn io_pool_reports_a_panicking_job_and_keeps_serving() {
        let pool = IoPool::new("test-io", 1, 4).expect("pool should start");
        let error = pool
            .run(|| -> usize { std::panic::panic_any("boom") })
            .await
            .expect_err("a panic should be reported");
        assert!(error.to_string().contains("panicked"), "{error}");
        assert_eq!(pool.run(|| 7).await.expect("pool should still serve"), 7);
    }

    #[allow(clippy::panic)]
    #[tokio::test]
    async fn run_cpu_runs_on_the_given_rayon_pool() {
        let pool = rayon_pool("test-cpu", 2).expect("pool should start");
        let name = run_cpu(&pool, || thread::current().name().map(str::to_owned))
            .await
            .expect("job should run");
        assert!(name.is_some_and(|name| name.starts_with("test-cpu-")));
        let error = run_cpu(&pool, || -> usize { std::panic::panic_any("boom") })
            .await
            .expect_err("a panic should be reported");
        assert!(error.to_string().contains("panicked"), "{error}");
    }

    #[tokio::test]
    async fn io_pool_runs_buffer_cache_misses() {
        use crate::cache::{
            AlignedBytes, ArtifactClass, BufferCache, CacheConfig, CacheKey, CacheMode, Fetched,
            FileId,
        };
        let pool = IoPool::new("test-io", 2, 8).expect("pool should start");
        let cache = BufferCache::new(CacheConfig::with_budget(1 << 20));
        let key = CacheKey::section(FileId::next(), 0);
        let caller = thread::current().id();
        let (bytes, fetched) = cache
            .get_or_load(
                key,
                ArtifactClass::PkIndex,
                CacheMode::Normal,
                &pool,
                move || {
                    assert_ne!(thread::current().id(), caller, "the miss runs on the pool");
                    Ok(AlignedBytes::copy_from(b"section"))
                },
            )
            .await
            .expect("load succeeds");
        assert_eq!(&**bytes, b"section");
        assert!(matches!(fetched, Fetched::Loaded { .. }));
        assert!(cache.residency(&key));
    }

    /// An I/O thread that needs a unit whose miss is queued behind it on its own pool (here the
    /// only thread, with the bounded queue full) runs the queued load itself instead of waiting
    /// for a thread that will never come.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_io_thread_runs_a_cache_load_queued_behind_it() {
        use crate::cache::{
            AlignedBytes, ArtifactClass, BufferCache, CacheConfig, CacheKey, CacheMode, Fetched,
            FileId,
        };
        let pool = IoPool::new("test-io", 1, 1).expect("pool should start");
        let cache = BufferCache::new(CacheConfig::with_budget(1 << 20));
        let key = CacheKey::section(FileId::next(), 0);
        let (started, started_receiver) = mpsc::channel();
        let (go, go_receiver) = mpsc::channel::<()>();
        let blocking = tokio::spawn(pool.run({
            let cache = cache.clone();
            move || {
                started.send(()).expect("test is waiting");
                go_receiver.recv().expect("test signals");
                cache.get_or_load_blocking(key, ArtifactClass::PkIndex, CacheMode::Normal, || {
                    Ok(AlignedBytes::copy_from(b"inline"))
                })
            }
        }));
        started_receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("the blocking job occupies the only I/O thread");
        let fetch = cache.get_or_load(
            key,
            ArtifactClass::PkIndex,
            CacheMode::Normal,
            &pool,
            || Ok(AlignedBytes::copy_from(b"queued")),
        );
        go.send(()).expect("job is waiting");
        let (bytes, fetched) = tokio::time::timeout(Duration::from_secs(10), blocking)
            .await
            .expect("the I/O thread must not wait for a job queued behind it")
            .expect("task should join")
            .expect("job should run")
            .expect("load succeeds");
        assert_eq!(&**bytes, b"queued", "it ran the queued loader");
        assert_eq!(fetched, Fetched::Waited);
        let (bytes, fetched) = tokio::time::timeout(Duration::from_secs(10), fetch)
            .await
            .expect("the async caller is woken")
            .expect("load succeeds");
        assert_eq!(&**bytes, b"queued");
        assert!(matches!(fetched, Fetched::Loaded { .. }));
    }

    #[test]
    fn runtime_config_defaults_follow_the_design() {
        let config = RuntimeConfig::default();
        assert_eq!(config.io_threads, 8);
        assert!(config.query_threads >= 1);
        assert!(config.maintenance_threads >= 1);
        assert!(config.maintenance_threads <= config.query_threads);
    }
}
