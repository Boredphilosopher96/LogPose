//! Background maintenance: flush and compaction triggers, and each collection's job queue with
//! its persisted status.
//!
//! The queue and status live on the [`CollectionHandle`]; jobs run on the engine's job threads,
//! at most one queue loop per collection at a time, and the collection's writer task serializes
//! the jobs themselves (an explicit flush or compaction waits behind a queued one).
//! `maintenance.json` persists the status so that a job interrupted by a crash or a shutdown
//! resumes when the engine reopens. The file is written without holding the queue lock, so a
//! status read never waits for its fsync.

use crate::{
    durable_fs::path_exists,
    engine::{CoreRef, EngineCore},
    error::json_message,
    fs_util::{atomic_write_in_existing_dir, read_json},
    handle::CollectionHandle,
    version::Version,
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{MaintenanceStatus, Result, Snapshot};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, MutexGuard, PoisonError},
};

/// A background maintenance job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceOperation {
    Flush,
    Compact,
}

impl MaintenanceOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Flush => "flush",
            Self::Compact => "compact",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "flush" => Some(Self::Flush),
            "compact" => Some(Self::Compact),
            _ => None,
        }
    }
}

/// A collection's maintenance queue and the status it persists.
#[derive(Debug, Default)]
pub(crate) struct MaintenanceState {
    status: MaintenanceStatus,
    queue: VecDeque<MaintenanceOperation>,
    /// Whether a job loop is scheduled or running for the collection.
    running: bool,
    /// Bumped on every status change, so a persist can tell whether a newer status was already
    /// written.
    version: u64,
}

impl MaintenanceState {
    /// State recovered from a persisted status. A job that was in progress when the process
    /// stopped goes back to the front of the pending list. Returns the operations to resume and
    /// whether the status changed and must be persisted again.
    pub(crate) fn recovered(
        mut status: MaintenanceStatus,
    ) -> (Self, Vec<MaintenanceOperation>, bool) {
        let mut changed = false;
        if let Some(in_progress) = status.in_progress.take() {
            if !status.pending.iter().any(|pending| pending == &in_progress) {
                status.pending.insert(0, in_progress);
            }
            changed = true;
        }
        let resume = status
            .pending
            .iter()
            .filter_map(|label| MaintenanceOperation::from_str(label))
            .collect();
        (
            Self {
                status,
                ..Self::default()
            },
            resume,
            changed,
        )
    }

    /// The status this state persists.
    pub(crate) fn status(&self) -> &MaintenanceStatus {
        &self.status
    }
}

/// Whether the delta of `version` has reached a flush threshold.
pub(crate) fn should_flush(descriptor: &CollectionDescriptor, version: &Version) -> bool {
    version.counters.memtable_rows >= descriptor.flush_threshold_ops as u64
        || version.counters.memtable_bytes >= descriptor.flush_threshold_bytes as u64
}

/// Whether `version` has enough segments to compact.
pub(crate) fn should_compact(descriptor: &CollectionDescriptor, version: &Version) -> bool {
    version.counters.segment_count as usize >= descriptor.compaction_threshold_segments
}

impl EngineCore {
    pub(crate) fn maintenance_file_path(descriptor: &CollectionDescriptor) -> PathBuf {
        descriptor.root_path.join("maintenance.json")
    }

    pub(crate) fn load_maintenance_status(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        let path = Self::maintenance_file_path(descriptor);
        if !path_exists(self.vfs.as_ref(), &path)? {
            return Ok(MaintenanceStatus::default());
        }
        read_json(self.vfs.as_ref(), &path)
    }

    /// Persist `status`. The collection directory must exist: a job or a write that races a
    /// drop must not recreate the retired directory.
    pub(crate) fn persist_maintenance_status(
        &self,
        descriptor: &CollectionDescriptor,
        status: &MaintenanceStatus,
    ) -> Result<()> {
        atomic_write_in_existing_dir(
            self.vfs.as_ref(),
            &Self::maintenance_file_path(descriptor),
            serde_json::to_vec_pretty(status).map_err(json_message)?,
        )
    }

    /// The collection's maintenance status. Resident; no file read.
    pub(crate) fn maintenance_status(&self, handle: &CollectionHandle) -> MaintenanceStatus {
        lock_jobs(handle).status.clone()
    }

    /// Persist the handle's status if a newer one was not written yet. The queue lock is held
    /// only to copy the status, never across the file's fsync. A failure is recorded in the
    /// status rather than returned: the in-memory queue is authoritative while the engine runs,
    /// and the file only matters for resuming after a restart.
    fn persist_status(&self, handle: &CollectionHandle) {
        let mut written = handle
            .status_file
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (status, version) = {
            let state = lock_jobs(handle);
            (state.status.clone(), state.version)
        };
        if version <= *written {
            return;
        }
        match self.persist_maintenance_status(handle.descriptor(), &status) {
            Ok(()) => *written = version,
            Err(error) => {
                lock_jobs(handle).status.last_error =
                    Some(format!("failed to persist maintenance status: {error}"));
            }
        }
    }
}

fn lock_jobs(handle: &CollectionHandle) -> MutexGuard<'_, MaintenanceState> {
    handle.jobs.lock().unwrap_or_else(PoisonError::into_inner)
}

impl CoreRef {
    /// Queue `operations` for `handle` and make sure a job loop will run them.
    pub(crate) fn enqueue_maintenance(
        &self,
        handle: &Arc<CollectionHandle>,
        operations: Vec<MaintenanceOperation>,
    ) {
        if operations.is_empty() || handle.is_dropped() {
            return;
        }
        let start = {
            let mut state = lock_jobs(handle);
            for operation in &operations {
                let label = operation.as_str();
                if state.status.in_progress.as_deref() != Some(label)
                    && !state.status.pending.iter().any(|pending| pending == label)
                {
                    state.status.pending.push(label.to_owned());
                    state.version += 1;
                }
                if !state.queue.contains(operation) {
                    state.queue.push_back(*operation);
                }
            }
            !std::mem::replace(&mut state.running, true)
        };
        self.persist_status(handle);
        if start {
            let core = self.clone();
            let loop_handle = Arc::clone(handle);
            if self
                .jobs
                .execute(move || core.run_maintenance_queue(&loop_handle))
                .is_err()
            {
                lock_jobs(handle).running = false;
            }
        }
    }

    /// Resume the maintenance that recovery found pending, once, on the first data-plane
    /// access of `handle` (v1 semantics: metadata and status reads never start jobs).
    pub(crate) fn resume_armed_maintenance(&self, handle: &Arc<CollectionHandle>) {
        if handle.take_maintenance_resume() {
            self.resume_maintenance(handle);
        }
    }

    /// Resume the persisted pending operations of `handle` if its queue is idle.
    pub(crate) fn resume_maintenance(&self, handle: &Arc<CollectionHandle>) {
        let operations = {
            let state = lock_jobs(handle);
            if state.running {
                return;
            }
            state
                .status
                .pending
                .iter()
                .filter_map(|label| MaintenanceOperation::from_str(label))
                .collect::<Vec<_>>()
        };
        self.enqueue_maintenance(handle, operations);
    }

    /// Run the next queued job for `handle`, then, if more are queued, go to the back of the
    /// job threads' queue for the next one. Yielding after every job keeps the shared job
    /// threads fair: a collection whose writes keep refilling its queue cannot starve other
    /// collections or explicit flush and compaction requests. Stops when the queue is empty, the
    /// collection is dropped, or the engine shuts down; pending operations left behind stay
    /// persisted and resume on reopen.
    fn run_maintenance_queue(&self, handle: &Arc<CollectionHandle>) {
        let operation = {
            let mut state = lock_jobs(handle);
            let next = if self.is_shutting_down() || handle.is_dropped() {
                None
            } else {
                state.queue.pop_front()
            };
            let Some(operation) = next else {
                state.running = false;
                return;
            };
            let label = operation.as_str();
            state.status.pending.retain(|pending| pending != label);
            state.status.in_progress = Some(label.to_owned());
            state.version += 1;
            operation
        };
        self.persist_status(handle);

        let result = self.perform_maintenance(handle, operation);
        let follow_up = if result.is_ok() {
            let version = handle.current();
            let descriptor = handle.descriptor();
            let mut operations = Vec::new();
            if should_flush(descriptor, &version) {
                operations.push(MaintenanceOperation::Flush);
            }
            if should_compact(descriptor, &version) {
                operations.push(MaintenanceOperation::Compact);
            }
            operations
        } else {
            Vec::new()
        };

        {
            let mut state = lock_jobs(handle);
            state.status.in_progress = None;
            match result {
                Ok(_) => {
                    state.status.completed_runs += 1;
                    state.status.last_error = None;
                }
                Err(error) => state.status.last_error = Some(error.to_string()),
            }
            state.version += 1;
        }
        if !handle.is_dropped() {
            self.persist_status(handle);
        }
        // This job still owns the queue (`running` is set), so this only queues.
        self.enqueue_maintenance(handle, follow_up);

        let more = {
            let mut state = lock_jobs(handle);
            if state.queue.is_empty() {
                state.running = false;
            }
            state.running
        };
        if more {
            let core = self.clone();
            let next_handle = Arc::clone(handle);
            if self
                .jobs
                .execute(move || core.run_maintenance_queue(&next_handle))
                .is_err()
            {
                lock_jobs(handle).running = false;
            }
        }
    }

    /// Run one maintenance operation now, on the calling thread.
    pub(crate) fn perform_maintenance(
        &self,
        handle: &Arc<CollectionHandle>,
        operation: MaintenanceOperation,
    ) -> Result<Snapshot> {
        match operation {
            MaintenanceOperation::Flush => self.flush_collection(handle),
            MaintenanceOperation::Compact => self.compact_collection(handle),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CreateCollectionRequest, Engine, EngineConfig,
        test_support::{put, unique_temp_dir},
    };
    use logpose_types::{CollectionRef, DistanceMetric};
    use logpose_vfs::std_vfs;

    /// A write or job that checked the collection just before a concurrent drop still persists
    /// its status afterwards; that must not recreate the retired directory.
    #[test]
    fn a_status_persisted_after_a_drop_does_not_recreate_the_directory() {
        let root = unique_temp_dir("maintenance-after-drop");
        let engine =
            Engine::open(std_vfs(), &root, EngineConfig::default()).expect("engine should open");
        let descriptor = engine
            .core()
            .plan_collection_descriptor(&CreateCollectionRequest::new(
                "events",
                2,
                DistanceMetric::Dot,
            ))
            .expect("descriptor should plan");
        let handle = engine
            .create_collection(descriptor, None)
            .expect("collection should be created");
        engine
            .core()
            .write(&handle, vec![put("alpha", vec![1.0, 0.0])])
            .expect("write should succeed");
        engine
            .drop_collection(&CollectionRef::new_default("events"))
            .expect("drop should succeed");

        let status = MaintenanceStatus {
            pending: vec!["flush".to_owned()],
            ..MaintenanceStatus::default()
        };
        engine
            .core()
            .persist_maintenance_status(handle.descriptor(), &status)
            .expect_err("the retired directory is gone");
        assert!(
            !handle.meta().dir.exists(),
            "persisting a status must not resurrect a dropped collection's directory"
        );
    }

    #[test]
    fn recovered_state_resumes_the_interrupted_job_first() {
        let (state, resume, changed) = MaintenanceState::recovered(MaintenanceStatus {
            pending: vec!["compact".to_owned(), "bogus".to_owned()],
            in_progress: Some("flush".to_owned()),
            last_error: None,
            completed_runs: 3,
        });
        assert!(changed);
        assert_eq!(
            resume,
            vec![MaintenanceOperation::Flush, MaintenanceOperation::Compact]
        );
        assert_eq!(state.status.pending, vec!["flush", "compact", "bogus"]);
        assert_eq!(state.status.in_progress, None);
        assert_eq!(state.status.completed_runs, 3);
        assert!(!state.running);
    }

    #[test]
    fn recovered_state_without_an_interrupted_job_is_unchanged() {
        let status = MaintenanceStatus {
            pending: vec!["compact".to_owned()],
            ..MaintenanceStatus::default()
        };
        let (state, resume, changed) = MaintenanceState::recovered(status.clone());
        assert!(!changed);
        assert_eq!(resume, vec![MaintenanceOperation::Compact]);
        assert_eq!(state.status, status);
    }
}
