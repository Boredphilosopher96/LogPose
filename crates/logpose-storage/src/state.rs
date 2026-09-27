//! Per-operation collection state: the descriptor, manifest and replayed WAL delta, and snapshot validation against it.

use crate::manifest::Manifest;
use logpose_catalog::CollectionDescriptor;
use logpose_types::{LogPoseError, Result, SeqNo, Snapshot};
use logpose_wal::WalRecord;

#[derive(Clone, Debug)]
pub(crate) struct CollectionState {
    pub(crate) descriptor: CollectionDescriptor,
    pub(crate) manifest: Manifest,
    pub(crate) delta: Vec<WalRecord>,
}

impl CollectionState {
    pub(crate) fn visible_seq_no(&self) -> SeqNo {
        self.delta.last().map(|record| record.seq_no).unwrap_or(
            self.manifest
                .checkpoint_seq_no
                .max(self.manifest.max_segment_seq_no()),
        )
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
        return Err(LogPoseError::Message(format!(
            "invalid snapshot: manifest generation {} is unavailable",
            snapshot.manifest_generation
        )));
    }

    let max_visible = state.visible_seq_no();
    if snapshot.visible_seq_no > max_visible {
        return Err(LogPoseError::Message(format!(
            "invalid snapshot: visible sequence {} exceeds maximum {} for manifest generation {}",
            snapshot.visible_seq_no, max_visible, snapshot.manifest_generation
        )));
    }
    if snapshot.visible_seq_no < state.manifest.checkpoint_seq_no {
        return Err(LogPoseError::Message(format!(
            "invalid snapshot: visible sequence {} is below checkpoint {} for manifest generation {}",
            snapshot.visible_seq_no, state.manifest.checkpoint_seq_no, snapshot.manifest_generation
        )));
    }

    Ok(snapshot)
}
