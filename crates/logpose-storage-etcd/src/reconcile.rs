//! Collection creates and drops that complete even when their caller stops waiting, and the
//! reconciler that resolves the pending metadata of a create that stopped between its steps.
//!
//! A create writes pending metadata, creates the local collection, and marks the metadata
//! ready. It runs as a task of its own, so a caller that stops waiting (a client that hung up,
//! a request timeout) no longer leaves it half done. A create can still stop between its steps
//! when its process dies, etcd fails after the local create, or its node loses the leadership
//! that fences the create's last step. Its metadata then stays pending, and every node
//! describes the collection as needing reconciliation.
//!
//! [`EtcdCollectionCatalog::reconcile_pending`] resolves such metadata on the node the
//! collection is placed on (a create places the collection on the leader that runs it, and
//! only that node can see whether the local step happened), whether or not that node still
//! leads:
//!
//! - the local collection exists, opened, with the pending collection id: the create is rolled
//!   forward (the metadata is marked ready);
//! - otherwise it is rolled back: a local collection with that id that failed to open is
//!   dropped (the create was never acknowledged, so no write reached it), then the metadata is
//!   removed.
//!
//! Both changes are fenced by the node's membership lease (so a process that is no longer the
//! registered incarnation of the node changes nothing) and guarded by the metadata's mod
//! revisions, so a pass never races a create, drop, or placement change made elsewhere. The
//! leader's lease is not needed: no other node acts on pending metadata placed on this one,
//! since describes, drops, and creates of its name all refuse it while it is pending. Pending
//! metadata of a create or drop still in flight on this node, or placed on another node, is
//! left alone. Pending metadata placed on a node that never registers again stays pending
//! until an operator removes its keys.

use super::*;
use std::{future::Future, sync::PoisonError};

/// What one [`EtcdCollectionCatalog::reconcile_pending`] pass did, by collection
/// (`database/collection`).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReconcileReport {
    /// Pending creates marked ready: their local collection exists and opened.
    pub rolled_forward: Vec<String>,
    /// Pending creates whose metadata was removed, after dropping a local collection of theirs
    /// that failed to open.
    pub rolled_back: Vec<String>,
    /// Pending metadata left alone: a create or drop of the collection is in flight on this
    /// node, or the collection is placed on another node.
    pub skipped: Vec<String>,
    /// Pending metadata that could not be resolved, with the error. The next pass tries again.
    pub failed: Vec<String>,
}

/// One collection's metadata, read at one etcd revision.
pub(crate) struct CollectionMetadata {
    /// The collection, as `database/collection`.
    pub(crate) name: String,
    pub(crate) stored: StoredCollectionDescriptor,
    pub(crate) assignment: Option<CollectionAssignment>,
    pub(crate) revision: CollectionMetadataRevision,
}

/// How a pass resolved one pending create.
enum Resolution {
    RolledForward,
    RolledBack,
}

/// A create or drop in flight on this node; it leaves the in-flight map when dropped.
pub(crate) struct Operation {
    in_flight: Arc<Mutex<BTreeMap<String, usize>>>,
    name: String,
}

impl Drop for Operation {
    fn drop(&mut self) {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = in_flight.get_mut(&self.name) {
            *count -= 1;
            if *count == 0 {
                in_flight.remove(&self.name);
            }
        }
    }
}

/// Run `operation` as a task of its own and wait for its result. Dropping the returned future
/// stops the waiting, not the operation.
pub(crate) async fn run_to_completion<T: Send + 'static>(
    operation: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    tokio::spawn(operation).await.map_err(|error| {
        LogPoseError::internal(format!(
            "a collection metadata operation did not finish: {error}"
        ))
    })?
}

impl EtcdCollectionCatalog {
    /// Register a create or drop of `name` (`database/collection`) as in flight until the
    /// returned operation drops.
    pub(crate) fn begin_operation(&self, name: &str) -> Operation {
        let name = canonical_collection_lookup_name(name);
        *self
            .in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(name.clone())
            .or_default() += 1;
        Operation {
            in_flight: Arc::clone(&self.in_flight),
            name,
        }
    }

    fn in_flight(&self, name: &str) -> bool {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(name)
    }

    /// Resolve the pending metadata of creates that stopped between their steps and are placed
    /// on the node `member` names, this one. Each is rolled forward (marked ready) when the
    /// local collection exists, opened, with the pending collection id, and rolled back
    /// otherwise: a local collection of that id that failed to open is dropped (the create was
    /// never acknowledged, so no write reached it), then the metadata is removed. The etcd
    /// changes are fenced by the node's membership lease and guarded by the metadata's mod
    /// revisions; the node need not lead. Pending metadata placed on another node, whose local
    /// state only that node sees, and that of a create or drop in flight on this node are left
    /// alone. The coordination loop runs this when the node registers its membership and
    /// periodically while it stays registered.
    ///
    /// # Errors
    ///
    /// Etcd failures and undecodable metadata while listing it. A collection that fails to
    /// resolve is reported in [`ReconcileReport::failed`] instead.
    pub async fn reconcile_pending(&self, member: &MembershipFence) -> Result<ReconcileReport> {
        let mut report = ReconcileReport::default();
        let listed = self
            .etcd
            .list_collection_metadata()
            .await
            .inspect_err(|error| {
                tracing::warn!(%error, "could not list collection metadata to reconcile it");
            })?;
        for metadata in listed {
            if metadata.stored.ready {
                continue;
            }
            let name = metadata.name.clone();
            let placed_here = metadata
                .assignment
                .as_ref()
                .is_some_and(|assignment| assignment.assigned_node == member.node_id);
            if !placed_here || self.in_flight(&name) {
                report.skipped.push(name);
                continue;
            }
            match self.resolve_pending(&metadata, member).await {
                Ok(Resolution::RolledForward) => {
                    tracing::info!(collection = %name, "rolled a pending collection create forward");
                    report.rolled_forward.push(name);
                }
                Ok(Resolution::RolledBack) => {
                    tracing::info!(collection = %name, "rolled a pending collection create back");
                    report.rolled_back.push(name);
                }
                Err(error) => {
                    tracing::warn!(
                        collection = %name,
                        %error,
                        "could not resolve a pending collection create"
                    );
                    report.failed.push(format!("{name}: {error}"));
                }
            }
        }
        Ok(report)
    }

    async fn resolve_pending(
        &self,
        metadata: &CollectionMetadata,
        member: &MembershipFence,
    ) -> Result<Resolution> {
        let reference = collection_ref_from_lookup_name(&metadata.name);
        let collection_id = &metadata.stored.descriptor.collection_id;
        match self.engine.collection(&reference) {
            Ok(handle) if handle.meta().id == *collection_id => {
                self.etcd
                    .mark_collection_ready_if_revision_matches(
                        &metadata.name,
                        &metadata.stored.descriptor,
                        metadata.revision,
                        Fence::Member(member),
                    )
                    .await?;
                return Ok(Resolution::RolledForward);
            }
            // No local collection, or another one of the same name: the create never got to
            // its local step.
            Ok(_) | Err(LogPoseError::NotFound { .. }) => {}
            Err(error) => {
                // A local collection of this name that failed to open. When it is this
                // create's, it goes with the rollback. Its drop is a local decision about a
                // collection placed here that was never acknowledged, so it is not fenced; a
                // failed metadata removal after it leaves pending metadata without a local
                // collection, which the next pass rolls back.
                let failed_is_ours = self
                    .engine
                    .list_collections()
                    .map_err(|_| error.clone())?
                    .iter()
                    .any(|descriptor| {
                        descriptor.collection_ref() == reference
                            && descriptor.collection_id == *collection_id
                    });
                if failed_is_ours {
                    self.engine.drop_collection(&reference).await?;
                }
            }
        }
        self.etcd
            .delete_collection_metadata_if_revision_matches(
                &metadata.name,
                metadata.revision,
                Some(Fence::Member(member)),
            )
            .await?;
        Ok(Resolution::RolledBack)
    }
}

impl EtcdPlacementStore {
    /// Every collection's metadata that has a descriptor, pending or ready, from one range
    /// read, so each collection's keys and mod revisions come from one etcd revision. A missing
    /// assignment or owner key reads as revision 0, which is what etcd compares a missing key
    /// against.
    pub(crate) async fn list_collection_metadata(&self) -> Result<Vec<CollectionMetadata>> {
        #[derive(Default)]
        struct Keys {
            stored: Option<StoredCollectionDescriptor>,
            assignment: Option<CollectionAssignment>,
            descriptor_mod_revision: i64,
            assignment_mod_revision: i64,
            owner_mod_revision: i64,
        }
        let prefix = self.collections_prefix();
        let mut client = self.client().await?;
        let response = client
            .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
            .await
            .map_err(etcd_message)?;
        let mut collections = BTreeMap::<String, Keys>::new();
        for kv in response.kvs() {
            let key = std::str::from_utf8(kv.key()).map_err(|error| {
                LogPoseError::corrupt(
                    CorruptionKind::Metadata,
                    format!("failed to decode metadata key as utf-8: {error}"),
                )
            })?;
            let Some(rest) = key.strip_prefix(prefix.as_str()) else {
                continue;
            };
            let parts = rest.split('/').collect::<Vec<_>>();
            let [database, collection, field @ ..] = parts.as_slice() else {
                continue;
            };
            let keys = collections
                .entry(format!("{database}/{collection}"))
                .or_default();
            match field {
                ["descriptor"] => {
                    keys.stored =
                        Some(serde_json::from_slice(kv.value()).map_err(json_decode_message)?);
                    keys.descriptor_mod_revision = kv.mod_revision();
                }
                ["assignment"] => {
                    keys.assignment =
                        Some(serde_json::from_slice(kv.value()).map_err(json_decode_message)?);
                    keys.assignment_mod_revision = kv.mod_revision();
                }
                ["shards", "0", "owner"] => keys.owner_mod_revision = kv.mod_revision(),
                _ => {}
            }
        }
        Ok(collections
            .into_iter()
            .filter_map(|(name, keys)| {
                keys.stored.map(|stored| CollectionMetadata {
                    name,
                    stored,
                    assignment: keys.assignment,
                    revision: CollectionMetadataRevision {
                        assignment_mod_revision: keys.assignment_mod_revision,
                        descriptor_mod_revision: keys.descriptor_mod_revision,
                        owner_mod_revision: keys.owner_mod_revision,
                    },
                })
            })
            .collect())
    }
}

/// A step between which a create or drop can be interrupted, for tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Step {
    /// A create wrote its pending metadata.
    MetadataWritten,
    /// A create created, or a drop dropped, the local collection.
    LocalChanged,
}

/// An interruption the next create or drop to reach `step` takes.
#[cfg(test)]
pub(crate) struct Interrupt {
    step: Step,
    reached: Option<tokio::sync::oneshot::Sender<()>>,
    resume: tokio::sync::oneshot::Receiver<bool>,
}

#[cfg(test)]
impl EtcdCollectionCatalog {
    /// Interrupt the next create or drop that reaches `step`: the first receiver hears when it
    /// gets there, and it then waits for the sender. `true` resumes it; `false`, or dropping
    /// the sender, stops it there, as if its process had died.
    pub(crate) fn interrupt_at(
        &self,
        step: Step,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<bool>,
    ) {
        let (reached, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume) = tokio::sync::oneshot::channel();
        *self
            .interrupt
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Interrupt {
            step,
            reached: Some(reached),
            resume,
        });
        (reached_rx, resume_tx)
    }

    /// Where a create or drop takes an interruption the test set for `step`.
    pub(crate) async fn interruption_point(&self, step: Step) -> Result<()> {
        let interrupt = {
            let mut slot = self
                .interrupt
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if slot
                .as_ref()
                .is_some_and(|interrupt| interrupt.step == step)
            {
                slot.take()
            } else {
                None
            }
        };
        let Some(mut interrupt) = interrupt else {
            return Ok(());
        };
        if let Some(reached) = interrupt.reached.take() {
            let _ = reached.send(());
        }
        if interrupt.resume.await.unwrap_or(false) {
            Ok(())
        } else {
            Err(LogPoseError::internal(format!(
                "the operation stopped at {step:?} for a test"
            )))
        }
    }

    /// Creates and drops in flight on this node.
    pub(crate) fn operations_in_flight(&self) -> usize {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .sum()
    }
}

#[cfg(test)]
mod tests;
