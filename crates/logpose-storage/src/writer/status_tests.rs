//! The maintenance status a request's caller reads once the request is answered: the writer
//! publishes a job's end (or a failed freeze) in the status before it answers any request the
//! job settles, so a caller whose explicit flush, compaction, or hand-stepped commit returned
//! never reads the job as pending or running, nor misses its failure.
//!
//! Reading the status after the answer arrived would race the writer. Instead each test reads
//! it inside the wake of the reply channel, which runs on the writer's thread as it sends the
//! answer: the status the caller could read at the earliest moment.

use super::*;
use crate::{
    CreateCollectionRequest, Engine, EngineConfig, RuntimeConfig, test_support::ControlledVfs,
};
use logpose_types::{DistanceMetric, MaintenanceStatus, Snapshot, record::Record};
use logpose_vfs::FaultVfs;
use logpose_wal::BootId;
use std::{
    future::Future,
    pin::Pin,
    sync::{Condvar, Mutex, PoisonError},
    task::{Context, Poll, Wake, Waker},
    time::Instant,
};

const ROOT: &str = "/storage";
const NAME: &str = "status";

fn open(vfs: &Arc<ControlledVfs>) -> Engine {
    let config = EngineConfig {
        boot_id: Some(BootId::new("boot")),
        runtime: RuntimeConfig {
            io_threads: 2,
            query_threads: 1,
            maintenance_threads: 1,
            writer_threads: 1,
            ..RuntimeConfig::default()
        },
        ..EngineConfig::default()
    };
    Engine::open(vfs.clone(), ROOT, config).expect("engine should open")
}

/// A collection with no flush trigger and no background compaction: only the requests the test
/// makes run jobs.
fn create(engine: &Engine) -> Arc<CollectionHandle> {
    let mut descriptor = engine
        .core()
        .plan_collection_descriptor(&CreateCollectionRequest::new(NAME, 2, DistanceMetric::Dot))
        .expect("descriptor should plan");
    descriptor.flush_threshold_ops = usize::MAX;
    descriptor.flush_threshold_bytes = usize::MAX;
    descriptor.compaction_threshold_segments = usize::MAX;
    engine
        .create_collection(descriptor, None)
        .expect("collection should be created")
}

fn write(handle: &CollectionHandle, id: &str, x: f32) {
    handle
        .write_blocking(vec![ClientOp::Upsert(
            Record::new(id).with_vector("vector", vec![x, 1.0]),
        )])
        .expect("write should commit");
}

/// A waker that records the collection's maintenance status when it is woken.
struct StatusAtWake {
    handle: Arc<CollectionHandle>,
    seen: Mutex<Option<MaintenanceStatus>>,
    woken: Condvar,
}

impl Wake for StatusAtWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let status = self.handle.maintenance_status();
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        seen.get_or_insert(status);
        self.woken.notify_all();
    }
}

/// Send the control message `message` builds around a reply channel, and return the answer
/// with the maintenance status at the moment the writer sent it.
fn answered_with_status(
    handle: &Arc<CollectionHandle>,
    message: impl FnOnce(SnapshotReply) -> ControlMsg,
) -> (Result<Snapshot>, MaintenanceStatus) {
    let (reply, mut replied) = oneshot::channel();
    let observer = Arc::new(StatusAtWake {
        handle: Arc::clone(handle),
        seen: Mutex::new(None),
        woken: Condvar::new(),
    });
    let waker = Waker::from(Arc::clone(&observer));
    let mut context = Context::from_waker(&waker);
    // Register the waker before the writer can answer.
    assert!(
        Pin::new(&mut replied).poll(&mut context).is_pending(),
        "nothing answered yet"
    );
    handle
        .control_sender()
        .send(message(reply))
        .expect("the writer runs");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = observer.seen.lock().unwrap_or_else(PoisonError::into_inner);
    while seen.is_none() {
        let now = Instant::now();
        assert!(now < deadline, "timed out waiting for the answer");
        seen = observer
            .woken
            .wait_timeout(seen, deadline - now)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
    let status = seen.take().expect("the wake recorded the status");
    drop(seen);
    let Poll::Ready(answer) = Pin::new(&mut replied).poll(&mut context) else {
        unreachable!("the wake came with the answer");
    };
    (answer.expect("the writer answered"), status)
}

fn flush(handle: &Arc<CollectionHandle>) -> (Result<Snapshot>, MaintenanceStatus) {
    answered_with_status(handle, |reply| ControlMsg::Flush { reply: Some(reply) })
}

#[test]
fn an_explicit_flush_answers_once_the_status_shows_it_ended() {
    let vfs = ControlledVfs::wrap(FaultVfs::new(8).process());
    let engine = open(&vfs);
    let handle = create(&engine);
    write(&handle, "a", 1.0);

    let (answer, status) = flush(&handle);
    let snapshot = answer.expect("the flush commits");
    assert_eq!(
        snapshot.manifest_generation,
        handle.current().manifest_generation
    );
    assert_eq!(status.in_progress, None, "the flush no longer runs");
    assert!(status.pending.is_empty(), "nothing waits: {status:?}");
    assert_eq!(status.completed_runs, 1);
    assert_eq!(status.last_error, None);
}

#[test]
fn an_explicit_compaction_answers_once_the_status_shows_it_ended() {
    let vfs = ControlledVfs::wrap(FaultVfs::new(8).process());
    let engine = open(&vfs);
    let handle = create(&engine);
    for (id, x) in [("a", 1.0), ("b", 2.0)] {
        write(&handle, id, x);
        handle.flush_blocking().expect("the flush commits");
    }
    assert_eq!(handle.current().segments.len(), 2);

    let (answer, status) = answered_with_status(&handle, |reply| ControlMsg::Compact { reply });
    answer.expect("the compaction commits");
    assert_eq!(handle.current().segments.len(), 1);
    assert_eq!(status.in_progress, None, "the compaction no longer runs");
    assert!(status.pending.is_empty(), "nothing waits: {status:?}");
    assert_eq!(status.completed_runs, 3, "two flushes and the compaction");
}

#[test]
fn a_hand_stepped_commit_answers_once_the_status_shows_it_ended() {
    let vfs = ControlledVfs::wrap(FaultVfs::new(8).process());
    let engine = open(&vfs);
    let handle = create(&engine);
    write(&handle, "a", 1.0);

    let (mut ticket, start) = handle.begin_job(JobKind::Flush).expect("the job begins");
    assert_eq!(
        handle.maintenance_status().in_progress.as_deref(),
        Some("flush")
    );
    let JobWork::Flush(work) = &start.work else {
        unreachable!("the flush has a memtable to write");
    };
    let commit = engine
        .core()
        .build_flush(&handle, &start.version, start.unit, work, &mut ticket)
        .expect("the flush builds");
    let job = start.job;
    drop(start);

    let (answer, status) = answered_with_status(&handle, |reply| ControlMsg::JobDone {
        job,
        result: Ok(commit),
        wrote_files: true,
        reply: Some(reply),
    });
    answer.expect("the flush commits");
    assert_eq!(status.in_progress, None, "the flush no longer runs");
    assert_eq!(status.completed_runs, 1);
    // The job is over: the ticket's end finds nothing left to end.
    drop(ticket);
    assert_eq!(handle.current().checkpoint_seq_no, 1);
}

#[test]
fn a_failed_explicit_flush_answers_once_the_status_reports_the_failure() {
    let vfs = ControlledVfs::wrap(FaultVfs::new(8).process());
    let engine = open(&vfs);
    let handle = create(&engine);
    write(&handle, "a", 1.0);
    vfs.fail_file_syncs_containing(".seg", 1);

    let (answer, status) = flush(&handle);
    answer.expect_err("the flush fails at its segment's sync");
    assert_eq!(status.in_progress, None, "the flush no longer runs");
    let error = status.last_error.expect("the failure is reported");
    assert_eq!(error.job, "flush");
    assert_eq!(error.consecutive_failures, 1);
}

#[test]
fn a_failed_freeze_answers_once_the_status_reports_the_failure() {
    let vfs = ControlledVfs::wrap(FaultVfs::new(8).process());
    let engine = open(&vfs);
    let handle = create(&engine);
    write(&handle, "a", 1.0);
    // The freeze before the flush rotates the WAL into a new file, whose creation fails.
    vfs.fail_creates_containing(logpose_wal::WAL_FILE_SUFFIX);

    let (answer, status) = flush(&handle);
    answer.expect_err("the freeze fails");
    assert!(handle.current().frozen.is_empty(), "nothing was frozen");
    assert_eq!(status.in_progress, None);
    assert!(
        status.pending.is_empty(),
        "no flush was planned: {status:?}"
    );
    let error = status.last_error.expect("the failure is reported");
    assert_eq!(error.job, "flush");
    assert_eq!(error.consecutive_failures, 1);
}
