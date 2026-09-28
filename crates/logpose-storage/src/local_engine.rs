//! `LocalStorageEngine`: the `StorageEngine` implementation over an [`Engine`].
//!
//! Every trait method resolves its collection with a map lookup and runs its blocking work on
//! the engine's I/O pool (reads), never on a tokio worker. Writes, flushes, and compactions go
//! to the collection's writer task, which does its own I/O on the I/O pool and runs job builds
//! on the job threads. Reads of the current state use the published `Version` and read no
//! metadata files.

use crate::{
    BlobStore, CreateCollectionRequest, InspectReport, InspectTarget, StorageEngine,
    collections::collection_ref_from_lookup,
    durable_fs::path_exists,
    engine::{CoreRef, Engine, EngineConfig, not_found},
    handle::CollectionHandle,
    legacy_view::legacy_ops,
    read::{BoxFuture, CollectionReader, ReadOptions, ReadView},
    tokens::SnapshotToken,
};
use async_trait::async_trait;
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    ANONYMOUS_LOCAL_NODE_NAME, CollectionAssignment, CollectionRef, CollectionStats, CommitAck,
    LeadershipFence, LogPoseError, MaintenanceStatus, NodeRole, Result, Snapshot, WriteOperation,
    filter::FilterExpr,
    record::{ClientOp, PartialUpdate},
    schema::{CollectionSchema, SchemaChange},
};
use logpose_vfs::{Vfs, std_vfs};
use std::{path::Path, sync::Arc};

/// Local filesystem-backed storage engine: the [`StorageEngine`] trait over an [`Engine`].
///
/// Every file access goes through the engine's [`Vfs`]: [`StdVfs`](logpose_vfs::StdVfs) for
/// the convenience constructors, or any `Vfs` passed to [`LocalStorageEngine::with_vfs`] (tests
/// use [`FaultVfs`](logpose_vfs::FaultVfs) to inject crashes).
///
/// Opening claims the storage root exclusively: a second engine on the same root fails with
/// [`LogPoseError::StorageRootLocked`], in this process or another. Share one engine by cloning
/// it; the root is released when the last clone is dropped.
#[derive(Clone, Debug)]
pub struct LocalStorageEngine {
    engine: Engine,
}

impl LocalStorageEngine {
    /// Open a local storage engine rooted at the provided path on the real filesystem.
    ///
    /// Creates the root directory if needed and fails if another engine holds the root.
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        Self::with_blob_store(root, None)
    }

    /// Open a local storage engine on the real filesystem with `config`.
    ///
    /// Creates the root directory if needed and fails if another engine holds the root.
    pub fn with_config(root: impl AsRef<Path>, config: EngineConfig) -> Result<Self> {
        Ok(Self::from_engine(Engine::open(std_vfs(), root, config)?))
    }

    /// Open a local storage engine on the real filesystem that resolves delete-by-filter and
    /// update-by-filter requests with `resolver` (the service injects `logpose-query`'s).
    ///
    /// Creates the root directory if needed and fails if another engine holds the root.
    pub fn with_resolver(
        root: impl AsRef<Path>,
        resolver: Arc<dyn crate::read::RowSetResolver>,
    ) -> Result<Self> {
        let config = EngineConfig {
            resolver: Some(resolver),
            ..EngineConfig::default()
        };
        Ok(Self::from_engine(Engine::open(std_vfs(), root, config)?))
    }

    /// Open a local storage engine on the real filesystem with an optional blob-store
    /// implementation.
    ///
    /// Creates the root directory if needed and fails if another engine holds the root.
    pub fn with_blob_store(
        root: impl AsRef<Path>,
        blob_store: Option<Arc<dyn BlobStore>>,
    ) -> Result<Self> {
        Self::with_vfs(std_vfs(), root, blob_store)
    }

    /// Open a local storage engine that performs all file I/O through `vfs`.
    ///
    /// Creates the root directory if needed and fails if another engine holds the root.
    pub fn with_vfs(
        vfs: Arc<dyn Vfs>,
        root: impl AsRef<Path>,
        blob_store: Option<Arc<dyn BlobStore>>,
    ) -> Result<Self> {
        let config = EngineConfig {
            blob_store,
            ..EngineConfig::default()
        };
        Ok(Self::from_engine(Engine::open(vfs, root, config)?))
    }

    /// Serve the `StorageEngine` trait over an open engine.
    #[must_use]
    pub fn from_engine(engine: Engine) -> Self {
        Self { engine }
    }

    /// The engine behind this adapter.
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The filesystem this engine performs all I/O through.
    #[must_use]
    pub fn vfs(&self) -> &Arc<dyn Vfs> {
        self.engine.vfs()
    }

    /// Build the descriptor that would be persisted for one collection request.
    pub fn plan_collection_descriptor(
        &self,
        request: &CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        self.engine.core().plan_collection_descriptor(request)
    }

    /// Persist a collection using a previously planned descriptor. Blocking.
    pub fn create_collection_from_descriptor(
        &self,
        descriptor: CollectionDescriptor,
        assignment: Option<&CollectionAssignment>,
    ) -> Result<CollectionDescriptor> {
        self.engine
            .create_collection(descriptor, assignment)
            .map(|handle| handle.descriptor().clone())
    }

    /// [`LocalStorageEngine::create_collection_from_descriptor`] on the I/O pool, for async
    /// callers.
    pub async fn create_collection_from_descriptor_async(
        &self,
        descriptor: CollectionDescriptor,
        assignment: Option<CollectionAssignment>,
    ) -> Result<CollectionDescriptor> {
        self.engine
            .io(move |core| {
                core.create_collection(descriptor, assignment.as_ref())
                    .map(|handle| handle.descriptor().clone())
            })
            .await
    }

    /// Open a collection descriptor using an explicit database namespace.
    pub async fn open_collection_in_database(
        &self,
        database_name: &str,
        name: &str,
    ) -> Result<CollectionDescriptor> {
        self.engine
            .collection(&CollectionRef::new(database_name, name))
            .map(|handle| handle.descriptor().clone())
    }

    /// Pin the current state of `collection_name` for repeatable reads, returning the token and
    /// the snapshot it names. While the token lives, reads through it (and exact reads of that
    /// snapshot) see exactly this state, across flushes and compactions (I12).
    ///
    /// Fails with [`LogPoseError::TooManySnapshots`] when the collection already holds the most
    /// pins it allows or pinned snapshots exceed the engine's pinned-memory limit.
    pub fn pin_snapshot(&self, collection_name: &str) -> Result<(SnapshotToken, Snapshot)> {
        let handle = self.handle(collection_name)?;
        handle.ensure_open()?;
        let version = handle.current();
        let token = handle.pin_version(Arc::clone(&version))?;
        Ok((token, version.snapshot()))
    }

    /// Unpin `token`. Returns whether it was pinned.
    pub fn release_snapshot(&self, collection_name: &str, token: &SnapshotToken) -> Result<bool> {
        Ok(self.handle(collection_name)?.release_snapshot(token))
    }

    /// Statistics of the state `token` pins, extending the token's expiry.
    pub async fn stats_at_token(
        &self,
        collection_name: &str,
        token: SnapshotToken,
    ) -> Result<CollectionStats> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| {
            core.collection_stats(handle, token)
        })
        .await
    }

    fn handle(&self, name: &str) -> Result<Arc<CollectionHandle>> {
        self.engine.collection(&collection_ref_from_lookup(name))
    }

    /// Run data-plane work for `handle` on the I/O pool. The first data-plane access of a
    /// recovered collection lets its background maintenance run.
    async fn data_io<T: Send + 'static>(
        &self,
        handle: Arc<CollectionHandle>,
        f: impl FnOnce(&CoreRef, &Arc<CollectionHandle>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        handle.arm_maintenance();
        self.engine.io(move |core| f(core, &handle)).await
    }

    /// The handle serving `descriptor`, which must name the same collection (same id).
    fn handle_for(&self, descriptor: &CollectionDescriptor) -> Result<Arc<CollectionHandle>> {
        let reference = descriptor.collection_ref();
        let handle = self.engine.collection(&reference)?;
        if handle.meta().id != descriptor.collection_id {
            return Err(not_found(&reference));
        }
        Ok(handle)
    }

    async fn create(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
    ) -> Result<CollectionDescriptor> {
        self.engine
            .io(move |core| {
                let descriptor = core.plan_collection_descriptor(&request)?;
                core.create_collection(descriptor, Some(&assignment))
                    .map(|handle| handle.descriptor().clone())
            })
            .await
    }

    async fn stats_of(
        &self,
        handle: Arc<CollectionHandle>,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        self.data_io(handle, move |core, handle| {
            core.collection_stats(handle, snapshot)
        })
        .await
    }
}

impl CollectionReader for LocalStorageEngine {
    fn read_view<'a>(
        &'a self,
        collection: &'a CollectionRef,
        options: ReadOptions,
    ) -> BoxFuture<'a, Result<ReadView>> {
        Box::pin(async move {
            let handle = self.engine.collection(collection)?;
            handle.arm_maintenance();
            self.engine.core().read_view_of(&handle, &options)
        })
    }
}

#[async_trait]
impl StorageEngine for LocalStorageEngine {
    async fn engine_name(&self) -> &'static str {
        "local"
    }

    async fn create_collection(
        &self,
        request: CreateCollectionRequest,
    ) -> Result<CollectionDescriptor> {
        self.create(
            request,
            CollectionAssignment {
                assigned_node: ANONYMOUS_LOCAL_NODE_NAME.to_owned(),
                assigned_role: NodeRole::Data,
            },
        )
        .await
    }

    async fn create_collection_with_assignment(
        &self,
        request: CreateCollectionRequest,
        assignment: CollectionAssignment,
        _leader_fence: Option<LeadershipFence>,
    ) -> Result<CollectionDescriptor> {
        self.create(request, assignment).await
    }

    async fn open_collection(&self, name: &str) -> Result<CollectionDescriptor> {
        self.handle(name).map(|handle| handle.describe())
    }

    async fn has_local_collection(&self, name: &str) -> Result<bool> {
        Ok(self.handle(name).is_ok())
    }

    async fn local_collection_matches_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<bool> {
        match self.handle(&descriptor.lookup_name()) {
            Ok(handle) => Ok(handle.descriptor().matches_serving_identity(descriptor)),
            Err(error) if error.to_string().contains("does not exist") => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn list_collections(&self) -> Result<Vec<CollectionDescriptor>> {
        self.engine.core().list_descriptors()
    }

    async fn collection_assignment_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<CollectionAssignment> {
        self.handle_for(descriptor)?
            .meta()
            .assignment
            .clone()
            .ok_or_else(|| {
                LogPoseError::internal(format!(
                    "collection '{}' is missing placement metadata",
                    descriptor.name
                ))
            })
    }

    async fn drop_collection(
        &self,
        collection_name: &str,
        _leader_fence: Option<LeadershipFence>,
    ) -> Result<()> {
        let reference = collection_ref_from_lookup(collection_name);
        self.engine
            .io(move |core| core.drop_collection(&reference))
            .await
    }

    async fn schema(&self, collection_name: &str) -> Result<Arc<CollectionSchema>> {
        let handle = self.handle(collection_name)?;
        handle.ensure_open()?;
        Ok(Arc::clone(&handle.current().schema))
    }

    async fn alter_schema(&self, collection_name: &str, change: SchemaChange) -> Result<CommitAck> {
        let handle = self.handle(collection_name)?;
        handle.alter_schema(change).await
    }

    async fn write_batch(&self, collection_name: &str, ops: Vec<ClientOp>) -> Result<CommitAck> {
        let handle = self.handle(collection_name)?;
        handle.write(ops).await
    }

    async fn delete_by_filter(&self, collection_name: &str, filter: FilterExpr) -> Result<CommitAck> {
        self.handle(collection_name)?.delete_by_filter(filter).await
    }

    async fn update_by_filter(
        &self,
        collection_name: &str,
        filter: FilterExpr,
        patch: PartialUpdate,
    ) -> Result<CommitAck> {
        self.handle(collection_name)?
            .update_by_filter(filter, patch)
            .await
    }

    async fn write(
        &self,
        collection_name: &str,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        let handle = self.handle(collection_name)?;
        let ops = legacy_ops(operations)?;
        handle
            .write(ops)
            .await
            .map_err(|error| error.with_field_prefix("operations"))
    }

    async fn snapshot(&self, collection_name: &str) -> Result<Snapshot> {
        let handle = self.handle(collection_name)?;
        handle.ensure_open()?;
        Ok(handle.current().snapshot())
    }

    async fn flush(&self, collection_name: &str) -> Result<Snapshot> {
        self.handle(collection_name)?.flush().await
    }

    async fn compact(&self, collection_name: &str) -> Result<Snapshot> {
        self.handle(collection_name)?.compact().await
    }

    async fn stats(&self, collection_name: &str) -> Result<CollectionStats> {
        self.stats_snapshot(collection_name, None).await
    }

    async fn stats_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let handle = self.handle_for(descriptor)?;
        self.stats_of(handle, snapshot).await
    }

    async fn maintenance_status_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<MaintenanceStatus> {
        Ok(self.handle_for(descriptor)?.maintenance_status())
    }

    async fn stats_snapshot(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let handle = self.handle(collection_name)?;
        self.stats_of(handle, snapshot).await
    }

    async fn inspect(&self, collection_name: &str, target: InspectTarget) -> Result<InspectReport> {
        let handle = self.handle(collection_name)?;
        self.data_io(handle, move |core, handle| core.inspect(handle, target))
            .await
    }
}

impl CoreRef {
    /// Commit v1 `operations` as one batch through the collection's writer. Blocking; for
    /// threads outside any async runtime (tests, job threads).
    #[cfg(test)]
    pub(crate) fn write(
        &self,
        handle: &Arc<CollectionHandle>,
        operations: Vec<WriteOperation>,
    ) -> Result<CommitAck> {
        let ops = legacy_ops(operations)?;
        handle
            .write_blocking(ops)
            .map_err(|error| error.with_field_prefix("operations"))
    }
}

impl crate::engine::EngineCore {
    pub(crate) fn exists(&self, path: &Path) -> Result<bool> {
        path_exists(self.vfs.as_ref(), path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::unique_temp_dir;
    use std::fs;

    #[test]
    fn engine_open_fails_while_another_engine_holds_the_storage_root() {
        let root = unique_temp_dir("storage-root-lock");
        let first = LocalStorageEngine::new(&root).expect("first engine should open");
        let error = LocalStorageEngine::new(&root)
            .expect_err("a second engine on the same root must not open");
        assert!(
            matches!(error, LogPoseError::StorageRootLocked { .. }),
            "unexpected error: {error}"
        );
        let shared = first.clone();
        drop(first);
        assert!(
            LocalStorageEngine::new(&root).is_err(),
            "a clone keeps the root locked"
        );
        drop(shared);

        // An independent handle on LOCK is what another process looks like to the OS.
        let foreign = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join("LOCK"))
            .expect("engine should have created the lock file");
        foreign
            .try_lock()
            .expect("the root should be released once every clone is dropped");

        let error = LocalStorageEngine::new(&root)
            .expect_err("engine must not open a root held by another process");
        assert!(
            error
                .to_string()
                .contains("is already in use by another engine"),
            "unexpected error: {error}"
        );

        drop(foreign);
        LocalStorageEngine::new(&root).expect("engine should open after the holder exits");
    }
}
