//! Manifest v1: the JSON manifest generations, the `CURRENT` pointer, and per-segment metadata.

use crate::{
    durable_fs::read_file,
    engine::EngineCore,
    error::json_message,
    fs_util::{AtomicWritePoints, atomic_write_with_points, read_json},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{LogPoseError, QueryUnitArtifactStats, Result, ScalarFieldStats, SeqNo};
use logpose_vfs::CrashPoint;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

impl EngineCore {
    pub(crate) fn load_manifest(
        &self,
        descriptor: &CollectionDescriptor,
        generation_override: Option<u64>,
    ) -> Result<Manifest> {
        let generation = match generation_override {
            Some(generation) => generation,
            None => self.read_current_generation(descriptor)?,
        };

        let path = Self::manifest_file_path(descriptor, generation);
        if !self.exists(&path)? {
            if generation_override.is_some() && generation != 0 {
                return Err(LogPoseError::Message(format!(
                    "invalid snapshot: manifest generation {} does not exist",
                    generation
                )));
            }
            return Ok(Manifest::empty(generation));
        }
        read_json(self.vfs.as_ref(), &path)
    }

    pub(crate) fn read_current_generation(&self, descriptor: &CollectionDescriptor) -> Result<u64> {
        let path = Self::current_manifest_pointer(descriptor);
        if !self.exists(&path)? {
            return Ok(0);
        }
        let contents = read_file(self.vfs.as_ref(), &path, "failed to read CURRENT pointer")?;
        String::from_utf8_lossy(&contents)
            .trim()
            .parse::<u64>()
            .map_err(|error| {
                LogPoseError::Message(format!(
                    "failed to parse CURRENT manifest generation: {error}"
                ))
            })
    }

    /// Durably write the manifest generation, then point `CURRENT` at it.
    pub(crate) fn publish_manifest(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
    ) -> Result<()> {
        let manifest_path = Self::manifest_file_path(descriptor, manifest.generation);
        atomic_write_with_points(
            self.vfs.as_ref(),
            &manifest_path,
            serde_json::to_vec_pretty(manifest).map_err(json_message)?,
            AtomicWritePoints {
                after_temp_sync: Some(CrashPoint::ManifestAfterFileSync),
                after_rename: None,
                after_dir_sync: Some(CrashPoint::ManifestAfterDirSync),
            },
        )?;
        atomic_write_with_points(
            self.vfs.as_ref(),
            &Self::current_manifest_pointer(descriptor),
            manifest.generation.to_string().into_bytes(),
            AtomicWritePoints {
                after_temp_sync: Some(CrashPoint::CurrentAfterTempSync),
                after_rename: Some(CrashPoint::CurrentAfterRename),
                after_dir_sync: Some(CrashPoint::CurrentAfterDirSync),
            },
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) generation: u64,
    pub(crate) checkpoint_seq_no: SeqNo,
    pub(crate) segments: Vec<SegmentMeta>,
}

impl Manifest {
    pub(crate) fn empty(generation: u64) -> Self {
        Self {
            generation,
            checkpoint_seq_no: 0,
            segments: Vec::new(),
        }
    }

    pub(crate) fn max_segment_seq_no(&self) -> SeqNo {
        self.segments
            .iter()
            .map(|segment| segment.max_seq_no)
            .max()
            .unwrap_or(0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SegmentMeta {
    pub(crate) segment_id: String,
    pub(crate) file_name: String,
    pub(crate) min_seq_no: SeqNo,
    pub(crate) max_seq_no: SeqNo,
    pub(crate) put_count: usize,
    pub(crate) delete_count: usize,
    pub(crate) dimensions: usize,
    pub(crate) checksum: u32,
    #[serde(default)]
    pub(crate) approx_bytes: usize,
    #[serde(default = "default_index_kind")]
    pub(crate) index_kind: String,
    #[serde(default)]
    pub(crate) scalar_fields: BTreeMap<String, ScalarFieldStats>,
    #[serde(default)]
    pub(crate) artifacts: Vec<QueryUnitArtifactStats>,
    #[serde(default)]
    pub(crate) component_bytes: BTreeMap<String, usize>,
    pub(crate) remote: Option<RemoteArtifact>,
}

fn default_index_kind() -> String {
    "hnsw".to_owned()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RemoteArtifact {
    pub(crate) key: String,
    pub(crate) status: RemoteSyncState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RemoteSyncState {
    PendingUpload,
    UploadSkipped,
}

pub(crate) fn segment_artifact_file_name<'a>(
    segment: &'a SegmentMeta,
    kind: &str,
) -> Option<&'a str> {
    segment
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == kind)
        .map(|artifact| artifact.file_name.as_str())
}
