//! Conversions from lower-level errors into [`LogPoseError`].

use logpose_types::{CorruptionKind, LogPoseError};
use std::path::Path;

pub(crate) fn io_message(context: &str, error: std::io::Error) -> LogPoseError {
    LogPoseError::io(context, error)
}

/// A caller-supplied descriptor (principal or policy) failed validation.
pub(crate) fn invalid_descriptor(error: String) -> LogPoseError {
    LogPoseError::invalid_argument(error)
}

/// Encoding an in-memory value as JSON failed; this is a bug, not bad input.
pub(crate) fn json_message(error: serde_json::Error) -> LogPoseError {
    LogPoseError::internal(format!("failed to serialize JSON: {error}"))
}

/// A stored JSON file of `kind` does not decode.
pub(crate) fn json_corrupt(
    kind: CorruptionKind,
    path: &Path,
    error: &serde_json::Error,
) -> LogPoseError {
    LogPoseError::Corrupt {
        kind,
        location: Some(path.display().to_string()),
        message: format!("failed to decode JSON file '{}': {error}", path.display()),
    }
}
