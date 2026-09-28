//! Resolving a legacy read to the `Version` it runs against.
//!
//! Every read is served from a resident `Version`; nothing is loaded from disk. A read with no
//! snapshot uses the current `Version`. An exact [`Snapshot`] resolves only to a `Version` with
//! exactly its manifest generation and visible sequence number: the current one, one of the
//! latest versions of the current generation, or a token-pinned one. With deletion vectors a
//! `Version` cannot be reconstructed for an older sequence number, so any other snapshot fails
//! with [`LogPoseError::SnapshotExpired`]: repeatable reads need a token (D7).

use crate::{
    engine::EngineCore, handle::CollectionHandle, tokens::SnapshotToken, version::Version,
};
use logpose_types::{LogPoseError, Result, Snapshot};
use std::sync::Arc;

impl Version {
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
    /// The current `Version`, or the one an exact legacy snapshot names.
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
    /// The `Version` a read runs against, and the snapshot naming it. Reads no file.
    ///
    /// No snapshot reads the current `Version`. An exact snapshot reads the retained `Version`
    /// with exactly its generation and sequence number (see the module docs) and fails with
    /// [`LogPoseError::SnapshotExpired`] when none is retained. A snapshot ahead of the
    /// collection, or below the current generation's checkpoint, is invalid. A token reads
    /// exactly the `Version` it pins and extends the token's expiry.
    pub(crate) fn read_state(
        &self,
        handle: &CollectionHandle,
        at: impl Into<ReadAt>,
    ) -> Result<(Arc<Version>, Snapshot)> {
        handle.ensure_open()?;
        let snapshot = match at.into() {
            ReadAt::Token(token) => {
                let version = handle.snapshot_version(&token)?;
                let snapshot = version.snapshot();
                return Ok((version, snapshot));
            }
            ReadAt::Snapshot(None) => {
                let version = handle.current();
                let snapshot = version.snapshot();
                return Ok((version, snapshot));
            }
            ReadAt::Snapshot(Some(snapshot)) => snapshot,
        };
        let current = handle.current();
        if snapshot == current.snapshot() {
            return Ok((current, snapshot));
        }
        if snapshot.visible_seq_no > current.visible_seq_no
            || snapshot.manifest_generation > current.manifest_generation
        {
            return Err(LogPoseError::invalid_field(
                "snapshot",
                format!(
                    "invalid snapshot: manifest generation {}, visible sequence {} is ahead of \
                     the collection (generation {}, sequence {})",
                    snapshot.manifest_generation,
                    snapshot.visible_seq_no,
                    current.manifest_generation,
                    current.visible_seq_no
                ),
            ));
        }
        if snapshot.manifest_generation == current.manifest_generation
            && snapshot.visible_seq_no < current.checkpoint_seq_no
        {
            return Err(LogPoseError::invalid_field(
                "snapshot",
                format!(
                    "invalid snapshot: visible sequence {} is below checkpoint {} for manifest \
                     generation {}",
                    snapshot.visible_seq_no,
                    current.checkpoint_seq_no,
                    snapshot.manifest_generation
                ),
            ));
        }
        let version =
            handle
                .version_for(&snapshot)
                .ok_or_else(|| LogPoseError::SnapshotExpired {
                    collection: handle.descriptor().lookup_name(),
                    reason: format!(
                        "the state at manifest generation {}, sequence {} is no longer retained \
                     (now generation {}, sequence {}); pin a snapshot token for repeatable \
                     reads",
                        snapshot.manifest_generation,
                        snapshot.visible_seq_no,
                        current.manifest_generation,
                        current.visible_seq_no
                    ),
                })?;
        Ok((version, snapshot))
    }
}
