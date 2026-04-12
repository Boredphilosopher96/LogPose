//! Write-ahead log interfaces.

use crc32fast::hash;
use logpose_types::{LogPoseError, Result, SeqNo, WriteOperation};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const WAL_MAGIC: u32 = 0x4c50_5741;
const FRAME_HEADER_BYTES: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u64>();
const FRAME_TRAILER_BYTES: usize = std::mem::size_of::<u32>();

/// WAL policy scaffold for future durability strategies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalMode {
    /// Favor local development simplicity.
    Development,
    /// Favor strict durability defaults for production.
    Production,
}

/// Durable WAL frame persisted for a single write operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    /// Monotonic sequence number assigned by the storage engine.
    pub seq_no: SeqNo,
    /// Durable operation payload.
    pub op: WriteOperation,
}

/// Append-only writer for an active WAL file.
pub struct WalWriter {
    file: File,
    path: PathBuf,
}

impl WalWriter {
    /// Open or create an active WAL file for durable appends.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| io_message("failed to create WAL parent directory", error))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|error| io_message("failed to open WAL file", error))?;

        Ok(Self { file, path })
    }

    /// Return the active WAL path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a durable frame and sync it to the local filesystem.
    pub fn append(&mut self, seq_no: SeqNo, operation: &WriteOperation) -> Result<()> {
        let record = WalRecord {
            seq_no,
            op: operation.clone(),
        };
        let payload = serde_json::to_vec(&record).map_err(|error| {
            LogPoseError::Message(format!("failed to serialize WAL record: {error}"))
        })?;
        let checksum = hash(&payload);
        let mut frame =
            Vec::with_capacity(FRAME_HEADER_BYTES + payload.len() + FRAME_TRAILER_BYTES);
        frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&checksum.to_le_bytes());

        self.file
            .write_all(&frame)
            .map_err(|error| io_message("failed to append WAL frame", error))?;
        self.file
            .sync_data()
            .map_err(|error| io_message("failed to fsync WAL data", error))?;

        Ok(())
    }

    /// Truncate the active WAL after a successful checkpoint.
    pub fn truncate(&mut self) -> Result<()> {
        self.file
            .set_len(0)
            .map_err(|error| io_message("failed to truncate WAL", error))?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|error| io_message("failed to rewind WAL", error))?;
        self.file
            .sync_all()
            .map_err(|error| io_message("failed to fsync truncated WAL", error))?;
        Ok(())
    }
}

/// Replay a single WAL file, ignoring a truncated trailing frame.
pub fn replay_file(path: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
    let path = path.as_ref();
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_message("failed to read WAL file", error)),
    };

    let mut offset = 0usize;
    let mut records = Vec::new();

    while offset < bytes.len() {
        if bytes.len() - offset < FRAME_HEADER_BYTES {
            break;
        }

        let magic = u32::from_le_bytes(
            bytes[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .expect("header slice should have the right size"),
        );
        offset += std::mem::size_of::<u32>();
        if magic != WAL_MAGIC {
            return Err(LogPoseError::Message(format!(
                "invalid WAL magic at byte offset {}",
                offset - std::mem::size_of::<u32>()
            )));
        }

        let payload_len = u64::from_le_bytes(
            bytes[offset..offset + std::mem::size_of::<u64>()]
                .try_into()
                .expect("length slice should have the right size"),
        ) as usize;
        offset += std::mem::size_of::<u64>();

        if bytes.len() - offset < payload_len + FRAME_TRAILER_BYTES {
            break;
        }

        let payload_end = offset + payload_len;
        let payload = &bytes[offset..payload_end];
        offset = payload_end;

        let checksum = u32::from_le_bytes(
            bytes[offset..offset + FRAME_TRAILER_BYTES]
                .try_into()
                .expect("checksum slice should have the right size"),
        );
        offset += FRAME_TRAILER_BYTES;

        let actual_checksum = hash(payload);
        if checksum != actual_checksum {
            return Err(LogPoseError::Message(format!(
                "checksum mismatch while replaying WAL: expected {checksum}, got {actual_checksum}"
            )));
        }

        let record = serde_json::from_slice::<WalRecord>(payload).map_err(|error| {
            LogPoseError::Message(format!("failed to deserialize WAL payload: {error}"))
        })?;
        records.push(record);
    }

    Ok(records)
}

/// Replay all WAL files in a directory, processing rolled files before the active file.
pub fn replay_dir(path: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut entries = fs::read_dir(path)
        .map_err(|error| io_message("failed to list WAL directory", error))?
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .collect::<Vec<_>>();

    entries.sort_by_key(|path| wal_sort_key(path));

    let mut replayed = Vec::new();
    for entry in entries {
        replayed.extend(replay_file(&entry)?);
    }
    Ok(replayed)
}

/// Rotate the active WAL to a rolled filename and create a new empty active file.
pub fn rotate_active(active_path: impl AsRef<Path>, rolled_path: impl AsRef<Path>) -> Result<()> {
    let active_path = active_path.as_ref();
    let rolled_path = rolled_path.as_ref();
    if let Some(parent) = rolled_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| io_message("failed to create WAL rotation directory", error))?;
    }

    if active_path.exists() {
        fs::rename(active_path, rolled_path)
            .map_err(|error| io_message("failed to rotate active WAL", error))?;
    }

    let mut writer = WalWriter::open(active_path)?;
    writer.truncate()?;
    Ok(())
}

fn wal_sort_key(path: &Path) -> (u8, String) {
    let name = path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();
    let priority = u8::from(name == "active.wal");
    (priority, name)
}

fn io_message(context: &str, error: std::io::Error) -> LogPoseError {
    LogPoseError::Message(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_types::{PutRecord, RecordId, WriteOperation};
    use serde_json::json;
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn replay_returns_appended_records_in_order() {
        let dir = unique_temp_dir("wal-replay-order");
        let path = dir.join("active.wal");

        let mut writer = WalWriter::open(&path).expect("writer should open");
        writer
            .append(
                1,
                &WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 2.0],
                    metadata: json!({"color":"blue"}),
                }),
            )
            .expect("append should succeed");
        writer
            .append(
                2,
                &WriteOperation::Delete(logpose_types::DeleteRecord {
                    id: RecordId::new("alpha"),
                }),
            )
            .expect("append should succeed");

        let replayed = replay_file(&path).expect("replay should succeed");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].seq_no, 1);
        assert_eq!(replayed[1].seq_no, 2);
    }

    #[test]
    fn replay_stops_cleanly_on_partial_trailing_frame() {
        let dir = unique_temp_dir("wal-partial-frame");
        let path = dir.join("active.wal");

        let mut writer = WalWriter::open(&path).expect("writer should open");
        writer
            .append(
                7,
                &WriteOperation::Put(PutRecord {
                    id: RecordId::new("beta"),
                    vector: vec![3.0, 4.0],
                    metadata: json!({"size":"large"}),
                }),
            )
            .expect("append should succeed");

        let bytes = fs::read(&path).expect("wal file should exist");
        fs::write(&path, &bytes[..bytes.len() - 5]).expect("truncate should succeed");

        let replayed = replay_file(&path).expect("replay should succeed");
        assert_eq!(replayed.len(), 0);
    }

    #[test]
    fn replay_rejects_corrupt_checksum() {
        let dir = unique_temp_dir("wal-checksum");
        let path = dir.join("active.wal");

        let mut writer = WalWriter::open(&path).expect("writer should open");
        writer
            .append(
                11,
                &WriteOperation::Put(PutRecord {
                    id: RecordId::new("gamma"),
                    vector: vec![9.0, 8.0],
                    metadata: json!({"tier":"gold"}),
                }),
            )
            .expect("append should succeed");

        let mut bytes = fs::read(&path).expect("wal file should exist");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, bytes).expect("corruption write should succeed");

        let error = replay_file(&path).expect_err("checksum mismatch should fail");
        assert!(error.to_string().contains("checksum"));
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
        fs::create_dir_all(&dir).expect("temp dir should be created");
        dir
    }

    #[allow(dead_code)]
    fn _assert_path(_: &Path) {}
}
