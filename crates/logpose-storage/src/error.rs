//! Conversions from lower-level errors into [`LogPoseError`].

use logpose_types::LogPoseError;

pub(crate) fn io_message(context: &str, error: std::io::Error) -> LogPoseError {
    LogPoseError::Message(format!("{context}: {error}"))
}

pub(crate) fn string_message(error: String) -> LogPoseError {
    LogPoseError::Message(error)
}

pub(crate) fn json_message(error: serde_json::Error) -> LogPoseError {
    LogPoseError::Message(format!("failed to serialize or deserialize JSON: {error}"))
}
