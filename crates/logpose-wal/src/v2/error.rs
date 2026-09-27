//! Typed WAL v2 errors.

use logpose_types::{CorruptionKind, LogPoseError, SeqNo};
use std::{io, path::PathBuf};
use thiserror::Error;

/// What a failed [`append_group`](super::WalWriter::append_group) or
/// [`rotate`](super::WalWriter::rotate) means for the frames it tried to write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteOutcome {
    /// The file was truncated back to the last synced group and the truncation was synced: the
    /// failed frames are absent after any crash and will never be replayed.
    NotApplied,
    /// The rollback failed, so the failed frames may or may not be replayed later. Clients must
    /// treat this like a timeout.
    Unknown {
        /// Whether the `FSYNC_FAILED` fence marker was written durably. When it was not, a later
        /// open in the same boot cannot notice the hazard, so the process must stop.
        fenced: bool,
    },
}

/// Errors of the WAL v2 frame layer.
#[derive(Debug, Error)]
pub enum WalError {
    /// A filesystem operation failed outside the append path.
    #[error("WAL I/O error: {context} '{}': {source}", path.display())]
    Io {
        /// What the WAL was doing.
        context: &'static str,
        /// The file or directory involved.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: io::Error,
    },
    /// A WAL file holds bytes that no crash can produce: a damaged frame followed by a later
    /// durable group, a bad frame in a file other than the highest-named one, or checksummed
    /// frames that break the sequence or group rules. Nothing was modified.
    #[error("WAL file '{}' is corrupt at offset {offset}: {reason}", file.display())]
    Corrupt {
        /// The corrupt file.
        file: PathBuf,
        /// Byte offset of the first bad frame.
        offset: u64,
        /// What is wrong.
        reason: String,
    },
    /// A checksummed frame header carries a `format_version` this build cannot read.
    #[error(
        "WAL file '{}' has a frame with unsupported format version {version} at offset {offset}",
        file.display()
    )]
    UnsupportedFormatVersion {
        /// The file.
        file: PathBuf,
        /// Byte offset of the frame.
        offset: u64,
        /// The version found.
        version: u16,
    },
    /// The WAL directory holds a `.wal` file whose name is not a 20-digit sequence number, such
    /// as the version 1 `active.wal`.
    #[error("unexpected WAL file '{}'", path.display())]
    UnexpectedFile {
        /// The unexpected file.
        path: PathBuf,
    },
    /// The WAL directory is fenced by an `FSYNC_FAILED` marker from the current boot: a WAL
    /// fsync and its rollback failed, so the page cache may hold frames that never reached the
    /// device. Restart the host, or clear the marker with [`clear_fence`](super::clear_fence)
    /// after checking the device.
    #[error(
        "WAL '{}' is fenced: an fsync and its rollback failed in this boot ({boot_id}) for sequence numbers {first_seq_no}..={last_seq_no}",
        marker.display()
    )]
    FsyncFailedSameBoot {
        /// Path of the marker.
        marker: PathBuf,
        /// Boot id recorded in the marker.
        boot_id: String,
        /// First sequence number of the failed group.
        first_seq_no: SeqNo,
        /// Last sequence number of the failed group.
        last_seq_no: SeqNo,
    },
    /// An `FSYNC_FAILED` marker exists but cannot be parsed. It is treated as a marker from the
    /// current boot.
    #[error("WAL fence marker '{}' is unreadable: {reason}", marker.display())]
    FenceUnreadable {
        /// Path of the marker.
        marker: PathBuf,
        /// Why it could not be read.
        reason: String,
    },
    /// Appending or syncing frames failed. `outcome` says whether the frames can come back.
    #[error("WAL write failed ({outcome:?}): {source}")]
    WriteFailed {
        /// Whether the failed frames were durably rolled back.
        outcome: WriteOutcome,
        /// The error that failed the write.
        #[source]
        source: io::Error,
    },
    /// The writer refused the call because an earlier write failed or was interrupted.
    #[error("WAL writer is failed after an earlier write error ({outcome:?})")]
    WriterFailed {
        /// Outcome of the write that failed the writer.
        outcome: WriteOutcome,
    },
    /// The caller passed frames that break the frame or sequence rules. Nothing was written.
    #[error("invalid WAL frame: {reason}")]
    InvalidFrame {
        /// What is wrong.
        reason: String,
    },
    /// A frame payload exceeds [`MAX_FRAME_PAYLOAD`](super::MAX_FRAME_PAYLOAD).
    #[error("WAL frame payload of {len} bytes exceeds the {max}-byte limit")]
    FrameTooLarge {
        /// Payload length.
        len: usize,
        /// The limit.
        max: u32,
    },
}

impl WalError {
    pub(super) fn io(context: &'static str, path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            context,
            path: path.into(),
            source,
        }
    }

    pub(super) fn corrupt(
        file: impl Into<PathBuf>,
        offset: u64,
        reason: impl Into<String>,
    ) -> Self {
        Self::Corrupt {
            file: file.into(),
            offset,
            reason: reason.into(),
        }
    }

    pub(super) fn invalid(reason: impl Into<String>) -> Self {
        Self::InvalidFrame {
            reason: reason.into(),
        }
    }

    /// Whether this error reports damaged or inconsistent WAL contents.
    #[must_use]
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            Self::Corrupt { .. } | Self::UnsupportedFormatVersion { .. }
        )
    }

    /// The underlying I/O error, if this error wraps one.
    #[must_use]
    pub fn io_error(&self) -> Option<&io::Error> {
        match self {
            Self::Io { source, .. } | Self::WriteFailed { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<WalError> for LogPoseError {
    fn from(error: WalError) -> Self {
        let message = error.to_string();
        match error {
            WalError::Corrupt { file, .. }
            | WalError::UnsupportedFormatVersion { file, .. }
            | WalError::UnexpectedFile { path: file } => LogPoseError::Corrupt {
                kind: CorruptionKind::Wal,
                location: Some(file.display().to_string()),
                message,
            },
            WalError::FenceUnreadable { marker, .. } => LogPoseError::Corrupt {
                kind: CorruptionKind::Wal,
                location: Some(marker.display().to_string()),
                message,
            },
            WalError::Io {
                context,
                path,
                source,
            } => LogPoseError::io(
                format!("WAL I/O error: {context} '{}'", path.display()),
                source,
            ),
            WalError::WriteFailed { source, .. } => LogPoseError::io(
                message
                    .strip_suffix(&format!(": {source}"))
                    .unwrap_or("WAL write failed")
                    .to_owned(),
                source,
            ),
            WalError::FrameTooLarge { len, max } => LogPoseError::TooLarge {
                what: "WAL frame payload".to_owned(),
                size: u64::try_from(len).ok(),
                limit: u64::from(max),
            },
            WalError::FsyncFailedSameBoot { .. }
            | WalError::WriterFailed { .. }
            | WalError::InvalidFrame { .. } => LogPoseError::internal(message),
        }
    }
}
