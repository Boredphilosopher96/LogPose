//! Background maintenance: flush and compaction triggers, the persisted maintenance status, and the per-collection worker queue.

use crate::{
    LocalStorageEngine,
    error::json_message,
    fs_util::{atomic_write, read_json},
    stats::approximate_record_bytes,
    wal_rotation::wal_rotation_lock,
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{MaintenanceStatus, Result, Snapshot};
use logpose_wal::WalRecord;
use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    thread,
};

impl LocalStorageEngine {
    pub(crate) fn should_flush(
        &self,
        descriptor: &CollectionDescriptor,
        delta: &[WalRecord],
    ) -> bool {
        if delta.len() >= descriptor.flush_threshold_ops {
            return true;
        }

        let approx_bytes = delta
            .iter()
            .map(|record| approximate_record_bytes(&record.op))
            .sum::<usize>();
        approx_bytes >= descriptor.flush_threshold_bytes
    }

    pub(crate) fn should_compact(
        &self,
        descriptor: &CollectionDescriptor,
        segment_count: usize,
    ) -> bool {
        segment_count >= descriptor.compaction_threshold_segments
    }

    pub(crate) fn load_maintenance_status(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        let path = Self::maintenance_file_path(descriptor);
        if !path.exists() {
            return Ok(MaintenanceStatus::default());
        }
        read_json(&path)
    }

    pub(crate) fn persist_maintenance_status(
        &self,
        descriptor: &CollectionDescriptor,
        status: &MaintenanceStatus,
    ) -> Result<()> {
        atomic_write(
            &Self::maintenance_file_path(descriptor),
            serde_json::to_vec_pretty(status).map_err(json_message)?,
        )
    }

    pub(crate) fn enqueue_maintenance(
        &self,
        descriptor: &CollectionDescriptor,
        operations: Vec<MaintenanceOperation>,
    ) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }

        let status_lock = maintenance_status_lock(&descriptor.root_path);
        {
            let _guard = status_lock
                .lock()
                .expect("maintenance status lock should not be poisoned");
            let mut persisted = self.load_maintenance_status(descriptor)?;
            for operation in &operations {
                let label = operation.as_str().to_owned();
                if persisted.in_progress.as_deref() == Some(label.as_str())
                    || persisted.pending.iter().any(|pending| pending == &label)
                {
                    continue;
                }
                persisted.pending.push(label);
            }
            self.persist_maintenance_status(descriptor, &persisted)?;
        }

        let key = descriptor.root_path.clone();
        let should_spawn = {
            let mut coordinator = maintenance_coordinator()
                .lock()
                .expect("maintenance coordinator lock should not be poisoned");
            let state = coordinator.entry(key.clone()).or_default();
            for operation in operations {
                if !state.queue.iter().any(|pending| pending == &operation) {
                    state.queue.push_back(operation);
                }
            }
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if should_spawn {
            let engine = self.clone();
            let collection_name = descriptor.lookup_name();
            thread::spawn(move || engine.run_maintenance_worker(collection_name, key));
        }
        Ok(())
    }

    pub(crate) fn recover_persisted_maintenance(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<()> {
        let key = descriptor.root_path.clone();
        {
            let coordinator = maintenance_coordinator()
                .lock()
                .expect("maintenance coordinator lock should not be poisoned");
            if coordinator.contains_key(&key) {
                return Ok(());
            }
        }

        let status_lock = maintenance_status_lock(&descriptor.root_path);
        let operations = {
            let _guard = status_lock
                .lock()
                .expect("maintenance status lock should not be poisoned");
            let mut status = self.load_maintenance_status(descriptor)?;
            let mut needs_persist = false;
            if let Some(in_progress) = status.in_progress.take() {
                if !status.pending.iter().any(|pending| pending == &in_progress) {
                    status.pending.insert(0, in_progress);
                }
                needs_persist = true;
            }
            let operations = status
                .pending
                .iter()
                .filter_map(|label| MaintenanceOperation::from_str(label))
                .collect::<Vec<_>>();
            if needs_persist {
                self.persist_maintenance_status(descriptor, &status)?;
            }
            operations
        };

        if operations.is_empty() {
            return Ok(());
        }

        self.enqueue_maintenance(descriptor, operations)
    }

    fn run_maintenance_worker(self, collection_name: String, coordinator_key: PathBuf) {
        loop {
            let operation = {
                let mut coordinator = maintenance_coordinator()
                    .lock()
                    .expect("maintenance coordinator lock should not be poisoned");
                let Some(state) = coordinator.get_mut(&coordinator_key) else {
                    return;
                };
                match state.queue.pop_front() {
                    Some(operation) => operation,
                    None => {
                        coordinator.remove(&coordinator_key);
                        return;
                    }
                }
            };

            let descriptor = match self.find_collection_descriptor(&collection_name) {
                Ok(descriptor) => descriptor,
                Err(_) => {
                    clear_maintenance_runtime_state(&coordinator_key);
                    return;
                }
            };

            let status_lock = maintenance_status_lock(&descriptor.root_path);
            if let Ok(_guard) = status_lock.lock()
                && let Ok(mut status) = self.load_maintenance_status(&descriptor)
            {
                let label = operation.as_str().to_owned();
                status.pending.retain(|pending| pending != &label);
                status.in_progress = Some(label);
                let _ = self.persist_maintenance_status(&descriptor, &status);
            }

            let result = self.perform_maintenance_operation(&collection_name, operation);

            let follow_up_operations = if result.is_ok() {
                self.load_collection_state(&collection_name, None)
                    .ok()
                    .map(|state| {
                        let mut operations = Vec::new();
                        if self.should_flush(&state.descriptor, &state.delta) {
                            operations.push(MaintenanceOperation::Flush);
                        }
                        if self.should_compact(&state.descriptor, state.manifest.segments.len()) {
                            operations.push(MaintenanceOperation::Compact);
                        }
                        operations
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            if let Ok(_guard) = status_lock.lock()
                && let Ok(mut status) = self.load_maintenance_status(&descriptor)
            {
                status.in_progress = None;
                match result {
                    Ok(_) => {
                        status.completed_runs += 1;
                        status.last_error = None;
                    }
                    Err(error) => {
                        status.last_error = Some(error.to_string());
                    }
                }
                let _ = self.persist_maintenance_status(&descriptor, &status);
            }

            if !follow_up_operations.is_empty() {
                let _ = self.enqueue_maintenance(&descriptor, follow_up_operations);
            }
        }
    }

    pub(crate) fn perform_maintenance_operation(
        &self,
        collection_name: &str,
        operation: MaintenanceOperation,
    ) -> Result<Snapshot> {
        let descriptor = self.find_collection_descriptor(collection_name)?;
        let manifest_lock = maintenance_operation_lock(&descriptor.root_path);
        match operation {
            MaintenanceOperation::Flush => {
                let _manifest_guard = manifest_lock
                    .lock()
                    .expect("maintenance operation lock should not be poisoned");
                let wal_lock = wal_rotation_lock(&descriptor.root_path);
                let _wal_guard = wal_lock
                    .lock()
                    .expect("wal rotation lock should not be poisoned");
                let state =
                    self.load_collection_state_descriptor_with_wal_lock(descriptor, None)?;
                self.flush_state(state)
            }
            MaintenanceOperation::Compact => {
                let _manifest_guard = manifest_lock
                    .lock()
                    .expect("maintenance operation lock should not be poisoned");
                let state = self.load_collection_state_descriptor(descriptor, None)?;
                self.compact_state(state)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceOperation {
    Flush,
    Compact,
}

impl MaintenanceOperation {
    fn as_str(&self) -> &'static str {
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

#[derive(Default)]
struct RuntimeMaintenanceState {
    running: bool,
    queue: VecDeque<MaintenanceOperation>,
}

fn maintenance_coordinator() -> &'static Mutex<BTreeMap<PathBuf, RuntimeMaintenanceState>> {
    static COORDINATOR: OnceLock<Mutex<BTreeMap<PathBuf, RuntimeMaintenanceState>>> =
        OnceLock::new();
    COORDINATOR.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn clear_maintenance_runtime_state(path: &Path) {
    let mut coordinator = maintenance_coordinator()
        .lock()
        .expect("maintenance coordinator lock should not be poisoned");
    coordinator.remove(path);
}

fn maintenance_operation_locks() -> &'static Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn maintenance_operation_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = maintenance_operation_locks()
        .lock()
        .expect("maintenance operation lock map should not be poisoned");
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn maintenance_status_locks() -> &'static Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn maintenance_status_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = maintenance_status_locks()
        .lock()
        .expect("maintenance status lock map should not be poisoned");
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::unique_temp_dir;

    #[test]
    fn maintenance_worker_clears_coordinator_on_descriptor_lookup_failure() {
        let root = unique_temp_dir("storage-maintenance-descriptor-failure");
        let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
        let coordinator_key = root.join("collections").join("missing-collection");

        {
            let mut coordinator = maintenance_coordinator()
                .lock()
                .expect("maintenance coordinator lock should not be poisoned");
            coordinator.insert(
                coordinator_key.clone(),
                RuntimeMaintenanceState {
                    running: true,
                    queue: VecDeque::from([MaintenanceOperation::Flush]),
                },
            );
        }

        engine.run_maintenance_worker("missing".to_owned(), coordinator_key.clone());

        let coordinator = maintenance_coordinator()
            .lock()
            .expect("maintenance coordinator lock should not be poisoned");
        assert!(
            !coordinator.contains_key(&coordinator_key),
            "descriptor lookup failure should clear runtime coordinator state"
        );
    }
}
