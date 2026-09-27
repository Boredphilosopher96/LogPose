//! The logical state a read runs against (manifest, schema, and replayed delta), and snapshot
//! validation against it.

use crate::{
    manifest::Manifest,
    version::{DeltaLog, Version, visible_seq_no},
};
use logpose_types::{LogPoseError, Result, SeqNo, Snapshot, schema::CollectionSchema};
use std::sync::Arc;

/// A manifest and the WAL delta above its checkpoint: the current state (from the published
/// `Version`) or a historical one (loaded from disk for an older manifest generation).
#[derive(Clone, Debug)]
pub(crate) struct CollectionState {
    pub(crate) manifest: Arc<Manifest>,
    /// The schema as of the state's last operation; rows are read with it.
    pub(crate) schema: Arc<CollectionSchema>,
    pub(crate) delta: DeltaLog,
}

impl CollectionState {
    pub(crate) fn visible_seq_no(&self) -> SeqNo {
        visible_seq_no(&self.manifest, &self.delta)
    }
}

impl Version {
    /// The v1 state this version publishes.
    pub(crate) fn state(&self) -> CollectionState {
        CollectionState {
            manifest: Arc::clone(&self.manifest),
            schema: Arc::clone(&self.schema),
            delta: self.delta.clone(),
        }
    }

    /// The snapshot naming exactly this version's state.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            manifest_generation: self.manifest_generation,
            visible_seq_no: self.visible_seq_no,
        }
    }
}

pub(crate) fn resolve_snapshot(
    state: &CollectionState,
    snapshot: Option<Snapshot>,
) -> Result<Snapshot> {
    let snapshot = snapshot.unwrap_or(Snapshot {
        manifest_generation: state.manifest.generation,
        visible_seq_no: state.visible_seq_no(),
    });

    if snapshot.manifest_generation != state.manifest.generation {
        return Err(LogPoseError::invalid_field(
            "snapshot",
            format!(
                "invalid snapshot: manifest generation {} is unavailable",
                snapshot.manifest_generation
            ),
        ));
    }

    let max_visible = state.visible_seq_no();
    if snapshot.visible_seq_no > max_visible {
        return Err(LogPoseError::invalid_field(
            "snapshot",
            format!(
                "invalid snapshot: visible sequence {} exceeds maximum {} for manifest generation {}",
                snapshot.visible_seq_no, max_visible, snapshot.manifest_generation
            ),
        ));
    }
    if snapshot.visible_seq_no < state.manifest.checkpoint_seq_no {
        return Err(LogPoseError::invalid_field(
            "snapshot",
            format!(
                "invalid snapshot: visible sequence {} is below checkpoint {} for manifest generation {}",
                snapshot.visible_seq_no,
                state.manifest.checkpoint_seq_no,
                snapshot.manifest_generation
            ),
        ));
    }

    Ok(snapshot)
}
