//! WAL rotation protocol shared by flush and recovery: the `PENDING_ROTATION` marker.

#[cfg(test)]
use crate::failpoints;
use crate::{durable_fs::sync_parent_dir, engine::EngineCore, fs_util::remove_file_if_exists};
use logpose_catalog::CollectionDescriptor;
use logpose_types::Result;

impl EngineCore {
    /// Remove the pending-rotation marker and make the removal durable.
    pub(crate) fn clear_pending_rotation_marker(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> Result<()> {
        let marker_path = Self::pending_rotation_file_path(descriptor);
        #[cfg(test)]
        failpoints::check_marker_removal(&marker_path)?;
        remove_file_if_exists(
            self.vfs.as_ref(),
            &marker_path,
            "failed to clear pending WAL rotation marker",
        )?;
        sync_parent_dir(self.vfs.as_ref(), &marker_path)
    }
}
