//! Write-ahead log: frames, group commit, rollback, rotation, tail repair and replay.
//!
//! This is the WAL v2 format; the version 1 JSON WAL is gone and a directory that still holds
//! its `active.wal` fails to open with [`WalError::UnexpectedFile`]. [`codec`] holds the payload
//! types the engine puts into frames; this layer treats payloads as opaque bytes.
//!
//! A collection's WAL is a directory of files named by the first sequence number they may
//! contain (`00000000000000000001.wal`, ...). Only the highest-named file is appended to. Every
//! file is a sequence of 8-byte aligned frames with a 48-byte header (see [`FrameHeader`]); the
//! payload is opaque to this layer. The engine appends frames in fsync groups: one
//! [`WalWriter::append_group`] call is one `append` plus one `sync_data`, the last frame of the
//! group carries `GROUP_END`, and every frame of the group carries the same `group_no`.
//!
//! Durability rules this module enforces:
//!
//! - **Groups are atomic.** Recovery keeps a group only if its `GROUP_END` frame is complete and
//!   checksummed; the frames of an incomplete last group are truncated as a unit.
//! - **Torn tail versus corruption.** A damaged frame in the highest-named file is a torn tail
//!   only if every checksummed frame after it belongs to the damaged frame's own fsync group.
//!   Anything else means a later group was durably written after the damage, and
//!   [`WalRecovery::open`] fails with [`WalError::Corrupt`] without modifying anything. A bad
//!   frame in any other file is always corruption.
//! - **Every file starts with a checkpoint group.** The first frame of every file is a
//!   checkpoint frame that is a group of its own, synced before anything else is appended, so
//!   damage at offset 0 followed by any checksummed frame is corruption, never a torn tail.
//! - **Failed groups are rolled back.** If the append or the sync of a group fails, the writer
//!   truncates the file back to the end of the last synced group and syncs that, so the failed
//!   group is definitely absent after recovery ([`WriteOutcome::NotApplied`]). If the rollback
//!   fails too, the writer fences the directory with a `FSYNC_FAILED` marker holding the boot id
//!   ([`WriteOutcome::Unknown`]); opening the WAL again in the same boot is refused with
//!   [`WalError::FsyncFailedSameBoot`].
//! - **Rotation makes the new file durable before it is used.** The new file receives a
//!   checkpoint frame, is synced, and its directory is synced before any data group lands in it.
//! - **A frame is only valid in its own file.** The header checksum is salted with the first
//!   sequence number in the file's name, so a frame copied into user data (and so into another
//!   file's payload) never passes as a durable frame during the tail search.
//! - **Lengths from disk are validated before allocation.** A payload buffer is allocated only
//!   after the header checksum passes and `payload_len` is at most [`MAX_FRAME_PAYLOAD`] and
//!   fits in the file.
//!
//! Typical use by the engine:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use logpose_wal::codec::PayloadKind;
//! # use logpose_wal::{WalConfig, WalFrame, WalRecovery};
//! # fn run(vfs: Arc<dyn logpose_vfs::Vfs>) -> Result<(), logpose_wal::WalError> {
//! let dir = std::path::Path::new("/data/collection/wal");
//! let mut recovery = WalRecovery::open(vfs, dir, WalConfig::default(), 0)?;
//! while let Some(frame) = recovery.next_frame()? {
//!     // decode `frame.payload` and apply it
//!     let _ = (frame.header.kind, frame.payload);
//! }
//! // Used only if the writer has to start an empty file; the engine encodes the durable
//! // manifest's generation and checkpoint into the payload.
//! let mut writer = recovery.into_writer(&WalFrame::checkpoint(0, Vec::new())?)?;
//! let seq = writer.next_seq_no();
//! let frame = WalFrame::new(PayloadKind::WriteBatch, seq, seq, b"payload".to_vec())?;
//! writer.append_group(&[frame])?;
//! # Ok(())
//! # }
//! ```

pub mod codec;
mod error;
mod fence;
mod files;
mod frame;
mod reader;
mod recovery;
mod scan;
mod writer;

#[cfg(test)]
mod tests;

pub use error::WalError;
pub use fence::{BootId, FENCE_FILE_NAME, FenceMarker, clear_fence, read_fence};
pub use files::{WAL_FILE_SUFFIX, parse_wal_file_name, wal_file_name};
pub use frame::{
    FORMAT_VERSION, FRAME_ALIGN, FRAME_HEADER_LEN, FRAME_MAGIC, FrameHeader, MAX_FRAME_PAYLOAD,
    WalFrame, frame_len,
};
pub use logpose_types::WriteOutcome;
pub use reader::read_committed;
pub use recovery::{RecoveryReport, ReplayFrame, TailRepair, WalRecovery};
pub use writer::{GroupCommit, WalWriter};

use logpose_types::SeqNo;

/// Ownership epoch written into every frame header. Always 0 until replication (Phase 7).
pub type Epoch = u64;

/// Default size at which [`WalWriter::should_rotate`] asks for a new file: 64 MiB.
pub const DEFAULT_WAL_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Settings shared by [`WalRecovery`] and the [`WalWriter`] it produces.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalConfig {
    /// Size of the active file at which [`WalWriter::should_rotate`] returns `true`.
    pub file_bytes: u64,
    /// Ownership epoch written into every frame header.
    pub epoch: Epoch,
    /// Identity of the current boot, written into and compared against the `FSYNC_FAILED`
    /// fence marker. Tests inject distinct values to simulate reboots.
    pub boot_id: BootId,
}

impl WalConfig {
    /// A configuration with the default file size and epoch 0 for the given boot.
    #[must_use]
    pub fn new(boot_id: BootId) -> Self {
        Self {
            file_bytes: DEFAULT_WAL_FILE_BYTES,
            epoch: 0,
            boot_id,
        }
    }
}

impl Default for WalConfig {
    /// The default file size, epoch 0 and [`BootId::current`].
    fn default() -> Self {
        Self::new(BootId::current())
    }
}

/// Sequence number range `first..=last` of the data frames in a group, if it has any.
fn data_range<'a>(headers: impl IntoIterator<Item = &'a FrameHeader>) -> Option<(SeqNo, SeqNo)> {
    headers
        .into_iter()
        .filter(|header| header.is_data())
        .fold(None, |range, header| match range {
            None => Some((header.first_seq_no, header.last_seq_no)),
            Some((first, _)) => Some((first, header.last_seq_no)),
        })
}
