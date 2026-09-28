//! The logical state a read runs against (manifest, schema, and replayed delta), and exact
//! snapshot resolution.
//!
//! Every read is served from a resident `Version`; nothing is loaded from disk. An exact
//! [`Snapshot`] resolves to the current `Version` when it names the current manifest
//! generation, or else to a `Version` a snapshot token pins. A snapshot of any other state is no
//! longer retained and fails with [`LogPoseError::SnapshotExpired`]: repeatable reads across
//! flushes and compactions need a token (D7).

use crate::{
    engine::EngineCore,
    handle::CollectionHandle,
    manifest::Manifest,
    tokens::SnapshotToken,
    version::{DeltaLog, Version, visible_seq_no},
};
use logpose_types::{LogPoseError, Result, SeqNo, Snapshot, schema::CollectionSchema};
use std::sync::Arc;

/// A manifest and the WAL delta above its checkpoint, from one `Version`.
#[derive(Clone, Debug)]
pub(crate) struct CollectionState {
    pub(crate) manifest: Arc<Manifest>,
    /// The schema as of the state's last operation; rows are read with it.
    pub(crate) schema: Arc<CollectionSchema>,
    pub(crate) delta: DeltaLog,
    /// The version the state comes from. Held for the whole read, so the segment files the
    /// read opens by path stay on disk (I7).
    pub(crate) _version: Arc<Version>,
}

impl CollectionState {
    pub(crate) fn visible_seq_no(&self) -> SeqNo {
        visible_seq_no(&self.manifest, &self.delta)
    }
}

impl Version {
    /// The v1 state this version publishes.
    pub(crate) fn state(self: &Arc<Self>) -> CollectionState {
        CollectionState {
            manifest: Arc::clone(&self.manifest),
            schema: Arc::clone(&self.schema),
            delta: self.delta.clone(),
            _version: Arc::clone(self),
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

/// What state a read runs against.
#[derive(Clone, Debug)]
pub(crate) enum ReadAt {
    /// The current `Version`, or the state an exact legacy snapshot names.
    Snapshot(Option<Snapshot>),
    /// The `Version` a snapshot token pins.
    Token(SnapshotToken),
}

impl From<Option<Snapshot>> for ReadAt {
    fn from(snapshot: Option<Snapshot>) -> Self {
        Self::Snapshot(snapshot)
    }
}

impl From<SnapshotToken> for ReadAt {
    fn from(token: SnapshotToken) -> Self {
        Self::Token(token)
    }
}

impl EngineCore {
    /// The state a read runs against, and the snapshot naming what it reads. Reads no file.
    ///
    /// No snapshot reads the current `Version`. An exact snapshot of the current manifest
    /// generation reads the current `Version` up to its sequence number. An older generation
    /// is served only from a token-pinned `Version` that covers it, and fails with
    /// [`LogPoseError::SnapshotExpired`] otherwise. A token reads exactly the `Version` it pins
    /// and extends the token's expiry.
    pub(crate) fn read_state(
        &self,
        handle: &CollectionHandle,
        at: impl Into<ReadAt>,
    ) -> Result<(CollectionState, Snapshot)> {
        handle.ensure_open()?;
        let snapshot = match at.into() {
            ReadAt::Token(token) => {
                let version = handle.snapshot_version(&token)?;
                let snapshot = version.snapshot();
                return Ok((version.state(), snapshot));
            }
            ReadAt::Snapshot(snapshot) => snapshot,
        };
        let current = handle.current();
        let version = match &snapshot {
            Some(snapshot) if snapshot.manifest_generation != current.manifest_generation => {
                if snapshot.visible_seq_no > current.visible_seq_no
                    || snapshot.manifest_generation > current.manifest_generation
                {
                    return Err(LogPoseError::invalid_field(
                        "snapshot",
                        format!(
                            "invalid snapshot: manifest generation {}, visible sequence {} is \
                             ahead of the collection (generation {}, sequence {})",
                            snapshot.manifest_generation,
                            snapshot.visible_seq_no,
                            current.manifest_generation,
                            current.visible_seq_no
                        ),
                    ));
                }
                handle.pinned_version_for(snapshot).ok_or_else(|| {
                    LogPoseError::SnapshotExpired {
                        collection: handle.descriptor().lookup_name(),
                        reason: format!(
                            "manifest generation {} is no longer current (now {}) and no \
                             snapshot token pins it; pin a token for repeatable reads",
                            snapshot.manifest_generation, current.manifest_generation
                        ),
                    }
                })?
            }
            _ => current,
        };
        let state = version.state();
        let snapshot = resolve_snapshot(&state, snapshot)?;
        Ok((state, snapshot))
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
