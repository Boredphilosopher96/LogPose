//! Write-ahead log interfaces.
//!
//! A WAL file is a sequence of frames. Each frame holds exactly one [`WalBatch`], so a
//! write batch is committed with a single append and a single fsync, and replay applies
//! it all or nothing:
//!
//! ```text
//! magic: u32 LE | payload_len: u64 LE | payload: [u8; payload_len] | crc32: u32 LE
//! ```
//!
//! The CRC covers the little-endian length bytes followed by the payload. The payload is
//! the JSON encoding of a [`WalBatch`].
//!
//! Every file is read through one frame scanner. The scanner yields the longest prefix of
//! fully valid frames and describes why it stopped, if it stopped before the end of the
//! file. The active WAL treats an invalid tail as a torn write: replay ignores it and
//! [`WalWriter::open`] truncates it before appending. Rolled WAL files are strict.

pub mod codec;

use crc32fast::Hasher;
use logpose_types::{LogPoseError, Result, SeqNo, WriteOperation};
use logpose_vfs::{CrashPoint, OpenMode, Vfs, VfsFile, parent_dir, read_file};
use serde::{Deserialize, Serialize};
use std::{
    io::IoSlice,
    path::{Path, PathBuf},
    sync::Arc,
};

/// File name of the WAL that receives appends. Every other `*.wal` file is rolled.
pub const ACTIVE_WAL_FILE_NAME: &str = "active.wal";

const WAL_MAGIC: u32 = 0x4c50_5742;
const MAGIC_BYTES: usize = std::mem::size_of::<u32>();
const LENGTH_BYTES: usize = std::mem::size_of::<u64>();
const FRAME_HEADER_BYTES: usize = MAGIC_BYTES + LENGTH_BYTES;
const FRAME_TRAILER_BYTES: usize = std::mem::size_of::<u32>();

/// WAL policy scaffold for future durability strategies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalMode {
    /// Favor local development simplicity.
    Development,
    /// Favor strict durability defaults for production.
    Production,
}

/// One operation inside a WAL batch, tagged with its sequence number.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    /// Monotonic sequence number assigned by the storage engine.
    pub seq_no: SeqNo,
    /// Durable operation payload.
    pub op: WriteOperation,
}

/// An atomic group of operations persisted as exactly one WAL frame.
///
/// A batch is never empty and its sequence numbers are contiguous and start above zero.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalBatch {
    records: Vec<WalRecord>,
}

impl WalBatch {
    /// Build a batch, rejecting empty batches and non-contiguous sequence numbers.
    pub fn new(records: Vec<WalRecord>) -> Result<Self> {
        let batch = Self { records };
        batch.validate()?;
        Ok(batch)
    }

    /// Records in commit order.
    #[must_use]
    pub fn records(&self) -> &[WalRecord] {
        &self.records
    }

    /// Consume the batch and return its records in commit order.
    #[must_use]
    pub fn into_records(self) -> Vec<WalRecord> {
        self.records
    }

    /// Sequence number of the first record.
    #[must_use]
    pub fn first_seq_no(&self) -> SeqNo {
        self.records.first().map_or(0, |record| record.seq_no)
    }

    /// Sequence number of the last record.
    #[must_use]
    pub fn last_seq_no(&self) -> SeqNo {
        self.records.last().map_or(0, |record| record.seq_no)
    }

    fn validate(&self) -> Result<()> {
        let Some(first) = self.records.first() else {
            return Err(LogPoseError::Message(
                "WAL batch must contain at least one record".to_owned(),
            ));
        };
        if first.seq_no == 0 {
            return Err(LogPoseError::Message(
                "WAL batch sequence numbers must start above zero".to_owned(),
            ));
        }
        for pair in self.records.windows(2) {
            if pair[0].seq_no.checked_add(1) != Some(pair[1].seq_no) {
                return Err(LogPoseError::Message(format!(
                    "WAL batch sequence numbers must be contiguous: {} is followed by {}",
                    pair[0].seq_no, pair[1].seq_no
                )));
            }
        }
        Ok(())
    }
}

/// How a WAL file is interpreted when its frames are scanned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalFileKind {
    /// The file that receives appends. An invalid tail is a torn write and is ignored.
    Active,
    /// A rotated file. Rotation repairs the tail before renaming, so any defect errors.
    Rolled,
}

impl WalFileKind {
    /// Classify a WAL file by its name.
    #[must_use]
    pub fn from_path(path: &Path) -> Self {
        if path
            .file_name()
            .is_some_and(|name| name == ACTIVE_WAL_FILE_NAME)
        {
            Self::Active
        } else {
            Self::Rolled
        }
    }
}

/// Append-only writer for an active WAL file.
pub struct WalWriter {
    vfs: Arc<dyn Vfs>,
    file: Arc<dyn VfsFile>,
    path: PathBuf,
    /// Length of the fully valid, synced frame prefix. A failed append rolls back to it.
    len: u64,
}

impl WalWriter {
    /// Open or create an active WAL file for durable appends.
    ///
    /// Any bytes after the last fully valid frame are a torn write from a crash. They are
    /// truncated and synced before the writer is returned, so the next append never lands
    /// after garbage. A defect with any valid frame after it is not a torn tail; that is
    /// reported as an error instead of discarding acknowledged batches.
    ///
    /// A newly created file is made durable by syncing its directory before this returns. A
    /// newly created parent directory is not; the caller owns the layout above the WAL file.
    pub fn open(vfs: Arc<dyn Vfs>, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        vfs.create_dir_all(parent_dir(&path))
            .map_err(|error| io_message("failed to create WAL parent directory", error))?;
        let file = match vfs.open(&path, OpenMode::Append) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let file = vfs
                    .open(&path, OpenMode::CreateNew)
                    .map_err(|error| io_message("failed to create WAL file", error))?;
                // An acknowledged append to a file whose name is not durable could vanish with
                // the file, so a created WAL is made durable before it is used.
                sync_parent_dir(vfs.as_ref(), &path)?;
                file
            }
            Err(error) => return Err(io_message("failed to open WAL file", error)),
        };

        let bytes = read_whole(file.as_ref())
            .map_err(|error| io_message("failed to read WAL file", error))?;
        let scan = scan_frames(&bytes);
        let valid_len = scan.accepted_len(&path, WalFileKind::Active)?;
        if valid_len < bytes.len() {
            file.set_len(valid_len as u64)
                .map_err(|error| io_message("failed to truncate torn WAL tail", error))?;
            file.sync_all()
                .map_err(|error| io_message("failed to fsync repaired WAL", error))?;
            vfs.crash_point(CrashPoint::RecoveryAfterTailRepair)
                .map_err(|error| io_message("WAL tail repair interrupted", error))?;
            tracing::warn!(
                path = %path.display(),
                valid_bytes = valid_len,
                discarded_bytes = bytes.len() - valid_len,
                defect = ?scan.stop.map(|stop| stop.defect),
                "truncated torn tail of active WAL"
            );
        }

        Ok(Self {
            vfs,
            file,
            path,
            len: valid_len as u64,
        })
    }

    /// Return the active WAL path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a batch as one frame and sync it to the local filesystem.
    ///
    /// If the write or the fsync fails, the file is truncated back to its length before the
    /// append, so a batch reported as failed is never replayed later.
    pub fn append_batch(&mut self, batch: &WalBatch) -> Result<()> {
        let payload = serde_json::to_vec(batch).map_err(|error| {
            LogPoseError::Message(format!("failed to serialize WAL batch: {error}"))
        })?;
        let frame = encode_frame(&payload);

        if let Err(error) = self.file.append(&[IoSlice::new(&frame)]) {
            return Err(self.roll_back_failed_append("failed to append WAL frame", error));
        }
        self.vfs
            .crash_point(CrashPoint::WalAfterAppend)
            .map_err(|error| io_message("WAL append interrupted", error))?;
        if let Err(error) = self.file.sync_data() {
            return Err(self.roll_back_failed_append("failed to fsync WAL data", error));
        }
        self.len += frame.len() as u64;
        self.vfs
            .crash_point(CrashPoint::WalAfterSync)
            .map_err(|error| io_message("WAL append interrupted after its fsync", error))?;

        Ok(())
    }

    fn roll_back_failed_append(&mut self, context: &str, error: std::io::Error) -> LogPoseError {
        match self
            .file
            .set_len(self.len)
            .and_then(|()| self.file.sync_all())
            .and_then(|()| self.vfs.crash_point(CrashPoint::WalAfterRollback))
        {
            Ok(()) => io_message(context, error),
            Err(rollback_error) => LogPoseError::Message(format!(
                "{context}: {error}; rolling the WAL back to {} bytes also failed: {rollback_error}",
                self.len
            )),
        }
    }

    /// Truncate the active WAL after a successful checkpoint.
    pub fn truncate(&mut self) -> Result<()> {
        self.file
            .set_len(0)
            .map_err(|error| io_message("failed to truncate WAL", error))?;
        self.file
            .sync_all()
            .map_err(|error| io_message("failed to fsync truncated WAL", error))?;
        self.len = 0;
        Ok(())
    }
}

/// Replay the batches of a single WAL file.
///
/// A missing file replays as empty. See [`WalFileKind`] for how an invalid tail is treated.
pub fn replay_file(
    vfs: &dyn Vfs,
    path: impl AsRef<Path>,
    kind: WalFileKind,
) -> Result<Vec<WalBatch>> {
    let path = path.as_ref();
    let bytes = match read_file(vfs, path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_message("failed to read WAL file", error)),
    };

    let scan = scan_frames(&bytes);
    scan.accepted_len(path, kind)?;
    scan.payloads
        .iter()
        .map(|payload| decode_batch(path, payload))
        .collect()
}

/// Replay all WAL files in a directory, processing rolled files before the active file.
pub fn replay_dir(vfs: &dyn Vfs, path: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
    replay_dir_after_checkpoint(vfs, path, 0)
}

/// Replay the records above the manifest checkpoint from every relevant WAL file.
///
/// Batches at or below the checkpoint are dropped whole. A batch whose sequence range
/// straddles the checkpoint means the checkpoint split an atomic batch, and a batch that
/// does not start above the previous replayed batch means a sequence number was written
/// twice. Both are invariant violations and are reported as errors.
pub fn replay_dir_after_checkpoint(
    vfs: &dyn Vfs,
    path: impl AsRef<Path>,
    checkpoint_seq_no: SeqNo,
) -> Result<Vec<WalRecord>> {
    let path = path.as_ref();
    let listed = match vfs.list(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_message("failed to list WAL directory", error)),
    };

    let mut entries = listed
        .into_iter()
        .filter(|entry| !entry.is_dir)
        .map(|entry| path.join(entry.name))
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .collect::<Vec<_>>();

    entries.retain(|path| wal_may_contain_records_after_checkpoint(path, checkpoint_seq_no));

    entries.sort_by_key(|path| wal_sort_key(path));

    let mut replayed = Vec::new();
    for entry in entries {
        for batch in replay_file(vfs, &entry, WalFileKind::from_path(&entry))? {
            if batch.last_seq_no() <= checkpoint_seq_no {
                continue;
            }
            if batch.first_seq_no() <= checkpoint_seq_no {
                return Err(LogPoseError::Message(format!(
                    "WAL batch {}..={} in {} straddles checkpoint {checkpoint_seq_no}",
                    batch.first_seq_no(),
                    batch.last_seq_no(),
                    entry.display()
                )));
            }
            if let Some(previous) = replayed.last().map(|record: &WalRecord| record.seq_no)
                && batch.first_seq_no() <= previous
            {
                return Err(LogPoseError::Message(format!(
                    "WAL batch {}..={} in {} does not follow sequence number {previous}",
                    batch.first_seq_no(),
                    batch.last_seq_no(),
                    entry.display()
                )));
            }
            replayed.extend(batch.into_records());
        }
    }
    Ok(replayed)
}

/// Rotate the active WAL to a rolled filename and create a new empty active file.
///
/// A torn tail on the active WAL is repaired before the rename, so rolled files only
/// ever contain fully valid frames. The rename and the new active file are made durable by
/// syncing their directories before this returns.
pub fn rotate_active(
    vfs: &Arc<dyn Vfs>,
    active_path: impl AsRef<Path>,
    rolled_path: impl AsRef<Path>,
) -> Result<()> {
    let active_path = active_path.as_ref();
    let rolled_path = rolled_path.as_ref();
    vfs.create_dir_all(parent_dir(rolled_path))
        .map_err(|error| io_message("failed to create WAL rotation directory", error))?;

    let active_exists = logpose_vfs::exists(vfs.as_ref(), active_path)
        .map_err(|error| io_message("failed to look up active WAL", error))?;
    if active_exists {
        drop(WalWriter::open(Arc::clone(vfs), active_path)?);
        vfs.rename(active_path, rolled_path)
            .map_err(|error| io_message("failed to rotate active WAL", error))?;
    }

    let mut writer = WalWriter::open(Arc::clone(vfs), active_path)?;
    writer.truncate()?;
    // The rename and the new active file exist only in the directory until it is fsynced.
    sync_parent_dir(vfs.as_ref(), active_path)?;
    if parent_dir(rolled_path) != parent_dir(active_path) {
        sync_parent_dir(vfs.as_ref(), rolled_path)?;
    }
    vfs.crash_point(CrashPoint::WalAfterRotateCreate)
        .map_err(|error| io_message("WAL rotation interrupted", error))
}

/// Fsync the directory containing `path` so entries created or renamed in it survive power loss.
fn sync_parent_dir(vfs: &dyn Vfs, path: &Path) -> Result<()> {
    vfs.sync_dir(parent_dir(path))
        .map_err(|error| io_message("failed to fsync WAL directory", error))
}

fn read_whole(file: &dyn VfsFile) -> std::io::Result<Vec<u8>> {
    let len = usize::try_from(file.len()?).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "WAL file is too large")
    })?;
    let mut bytes = vec![0u8; len];
    file.read_exact_at(&mut bytes, 0)?;
    Ok(bytes)
}

/// Why a frame scan stopped before the end of the file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameDefect {
    /// Fewer bytes remain than the header or the declared frame length needs.
    Truncated,
    /// The frame does not start with the WAL magic.
    BadMagic,
    /// The frame is complete but its checksum does not match.
    BadChecksum,
}

#[derive(Clone, Copy, Debug)]
struct ScanStop {
    offset: usize,
    defect: FrameDefect,
    /// True when a fully valid frame starts anywhere after the defect, which rules out a
    /// torn tail: appends are sequential and a torn tail is truncated before the next
    /// append, so only the final frame can be torn. The search is not limited to the
    /// defective frame's declared end, because a corrupt magic or length gives no
    /// trustworthy end.
    followed_by_valid_frame: bool,
}

/// Result of scanning a WAL file: the valid frame prefix and where it ends.
struct FrameScan<'a> {
    payloads: Vec<&'a [u8]>,
    valid_len: usize,
    stop: Option<ScanStop>,
}

impl FrameScan<'_> {
    /// Return how many bytes of the file a reader of `kind` accepts, or an error when the
    /// defect that stopped the scan is not acceptable for that kind of file.
    fn accepted_len(&self, path: &Path, kind: WalFileKind) -> Result<usize> {
        let Some(stop) = self.stop else {
            return Ok(self.valid_len);
        };
        let acceptable = kind == WalFileKind::Active && !stop.followed_by_valid_frame;
        if acceptable {
            return Ok(self.valid_len);
        }
        let detail = match stop.defect {
            FrameDefect::Truncated => "truncated frame",
            FrameDefect::BadMagic => "invalid WAL magic",
            FrameDefect::BadChecksum => "checksum mismatch",
        };
        let position = if stop.followed_by_valid_frame {
            " before later valid frames"
        } else {
            ""
        };
        Err(LogPoseError::Message(format!(
            "corrupt WAL {}: {detail} at byte offset {}{position}",
            path.display(),
            stop.offset
        )))
    }
}

enum ParsedFrame<'a> {
    Valid { payload: &'a [u8], end: usize },
    Invalid(FrameDefect),
}

/// The single frame scanner shared by replay and by tail repair in [`WalWriter::open`].
fn scan_frames(bytes: &[u8]) -> FrameScan<'_> {
    let mut payloads = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        match parse_frame(bytes, offset) {
            ParsedFrame::Valid { payload, end } => {
                payloads.push(payload);
                offset = end;
            }
            ParsedFrame::Invalid(defect) => {
                let followed_by_valid_frame = (offset + 1..bytes.len()).any(|candidate| {
                    matches!(parse_frame(bytes, candidate), ParsedFrame::Valid { .. })
                });
                return FrameScan {
                    payloads,
                    valid_len: offset,
                    stop: Some(ScanStop {
                        offset,
                        defect,
                        followed_by_valid_frame,
                    }),
                };
            }
        }
    }
    FrameScan {
        payloads,
        valid_len: offset,
        stop: None,
    }
}

fn parse_frame(bytes: &[u8], offset: usize) -> ParsedFrame<'_> {
    let truncated = ParsedFrame::Invalid(FrameDefect::Truncated);
    let Some(magic) = read_array::<MAGIC_BYTES>(bytes, offset).map(u32::from_le_bytes) else {
        return truncated;
    };
    if magic != WAL_MAGIC {
        return ParsedFrame::Invalid(FrameDefect::BadMagic);
    }
    let length_offset = offset + MAGIC_BYTES;
    let Some(length_bytes) = read_array::<LENGTH_BYTES>(bytes, length_offset) else {
        return truncated;
    };
    let payload_start = offset + FRAME_HEADER_BYTES;
    let Some(payload_end) = usize::try_from(u64::from_le_bytes(length_bytes))
        .ok()
        .and_then(|payload_len| payload_start.checked_add(payload_len))
    else {
        return truncated;
    };
    let Some(end) = payload_end.checked_add(FRAME_TRAILER_BYTES) else {
        return truncated;
    };
    let Some(stored_checksum) =
        read_array::<FRAME_TRAILER_BYTES>(bytes, payload_end).map(u32::from_le_bytes)
    else {
        return truncated;
    };
    let payload = &bytes[payload_start..payload_end];
    if frame_checksum(&length_bytes, payload) != stored_checksum {
        return ParsedFrame::Invalid(FrameDefect::BadChecksum);
    }
    ParsedFrame::Valid { payload, end }
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Option<[u8; N]> {
    bytes
        .get(offset..offset.checked_add(N)?)
        .and_then(|slice| slice.try_into().ok())
}

fn frame_checksum(length_bytes: &[u8; LENGTH_BYTES], payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(length_bytes);
    hasher.update(payload);
    hasher.finalize()
}

fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let length_bytes = (payload.len() as u64).to_le_bytes();
    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len() + FRAME_TRAILER_BYTES);
    frame.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    frame.extend_from_slice(&length_bytes);
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&frame_checksum(&length_bytes, payload).to_le_bytes());
    frame
}

/// Decode a checksum-valid payload. Failures here are never treated as a torn tail: the
/// bytes are exactly what was written, so an undecodable batch is a hard error.
fn decode_batch(path: &Path, payload: &[u8]) -> Result<WalBatch> {
    let batch = serde_json::from_slice::<WalBatch>(payload).map_err(|error| {
        LogPoseError::Message(format!(
            "failed to deserialize WAL batch in {}: {error}",
            path.display()
        ))
    })?;
    batch.validate()?;
    Ok(batch)
}

fn wal_sort_key(path: &Path) -> (u8, String) {
    let name = path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();
    let priority = u8::from(name == ACTIVE_WAL_FILE_NAME);
    (priority, name)
}

fn wal_may_contain_records_after_checkpoint(path: &Path, checkpoint_seq_no: SeqNo) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return true;
    };

    if name == ACTIVE_WAL_FILE_NAME {
        return true;
    }

    rolled_wal_checkpoint_seq_no(name)
        .is_none_or(|file_checkpoint| file_checkpoint > checkpoint_seq_no)
}

fn rolled_wal_checkpoint_seq_no(file_name: &str) -> Option<SeqNo> {
    file_name.strip_suffix(".wal")?.parse().ok()
}

fn io_message(context: &str, error: std::io::Error) -> LogPoseError {
    LogPoseError::Message(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_types::{DeleteRecord, PutRecord, RecordId, WriteOperation};
    use logpose_vfs::{StdVfs, std_vfs};
    use serde_json::json;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn put(seq_no: SeqNo, id: &str) -> WalRecord {
        WalRecord {
            seq_no,
            op: WriteOperation::Put(PutRecord {
                id: RecordId::new(id),
                vector: vec![1.0, 2.0],
                metadata: json!({"id": id}),
            }),
        }
    }

    fn batch(records: Vec<WalRecord>) -> WalBatch {
        WalBatch::new(records).expect("batch should be valid")
    }

    fn append(path: &Path, records: Vec<WalRecord>) {
        let mut writer = WalWriter::open(std_vfs(), path).expect("writer should open");
        writer
            .append_batch(&batch(records))
            .expect("append should succeed");
    }

    fn seq_nos(batches: &[WalBatch]) -> Vec<Vec<SeqNo>> {
        batches
            .iter()
            .map(|batch| batch.records().iter().map(|record| record.seq_no).collect())
            .collect()
    }

    fn append_garbage(path: &Path, garbage: &[u8]) {
        let mut bytes = fs::read(path).expect("wal file should exist");
        bytes.extend_from_slice(garbage);
        fs::write(path, bytes).expect("garbage should be written");
    }

    #[test]
    fn replay_returns_appended_batches_in_order() {
        let dir = unique_temp_dir("wal-replay-order");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);

        let mut writer = WalWriter::open(std_vfs(), &path).expect("writer should open");
        writer
            .append_batch(&batch(vec![put(1, "alpha")]))
            .expect("append should succeed");
        writer
            .append_batch(&batch(vec![WalRecord {
                seq_no: 2,
                op: WriteOperation::Delete(DeleteRecord {
                    id: RecordId::new("alpha"),
                }),
            }]))
            .expect("append should succeed");

        let replayed =
            replay_file(&StdVfs, &path, WalFileKind::Active).expect("replay should succeed");
        assert_eq!(seq_nos(&replayed), vec![vec![1], vec![2]]);
    }

    #[test]
    fn multi_op_batch_is_one_frame_and_replays_whole() {
        let dir = unique_temp_dir("wal-multi-op");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);

        append(&path, vec![put(1, "a"), put(2, "b"), put(3, "c")]);

        let scan_bytes = fs::read(&path).expect("wal file should exist");
        assert_eq!(scan_frames(&scan_bytes).payloads.len(), 1);
        let replayed =
            replay_file(&StdVfs, &path, WalFileKind::Active).expect("replay should succeed");
        assert_eq!(seq_nos(&replayed), vec![vec![1, 2, 3]]);
        assert_eq!(replayed[0].first_seq_no(), 1);
        assert_eq!(replayed[0].last_seq_no(), 3);
    }

    #[test]
    fn batch_rejects_empty_and_non_contiguous_sequences() {
        assert!(WalBatch::new(Vec::new()).is_err());
        assert!(WalBatch::new(vec![put(0, "zero")]).is_err());
        assert!(WalBatch::new(vec![put(1, "a"), put(3, "b")]).is_err());
        assert!(WalBatch::new(vec![put(2, "a"), put(1, "b")]).is_err());
    }

    #[test]
    fn frame_truncated_mid_batch_hides_the_whole_batch() {
        let dir = unique_temp_dir("wal-partial-batch");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);

        append(&path, vec![put(1, "keep")]);
        let committed_len = fs::metadata(&path).expect("metadata").len();
        append(&path, vec![put(2, "a"), put(3, "b"), put(4, "c")]);
        let full = fs::read(&path).expect("wal file should exist");

        for cut in (committed_len as usize + 1)..full.len() {
            fs::write(&path, &full[..cut]).expect("truncate should succeed");
            let replayed =
                replay_file(&StdVfs, &path, WalFileKind::Active).expect("replay should succeed");
            assert_eq!(seq_nos(&replayed), vec![vec![1]], "cut at byte {cut}");
        }
    }

    #[test]
    fn active_replay_ignores_a_torn_checksum_at_the_tail() {
        let dir = unique_temp_dir("wal-torn-checksum");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&path, vec![put(1, "keep")]);
        append(&path, vec![put(2, "torn")]);

        let mut bytes = fs::read(&path).expect("wal file should exist");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, bytes).expect("corruption write should succeed");

        let replayed =
            replay_file(&StdVfs, &path, WalFileKind::Active).expect("replay should succeed");
        assert_eq!(seq_nos(&replayed), vec![vec![1]]);
    }

    #[test]
    fn rolled_replay_rejects_corrupt_checksum() {
        let dir = unique_temp_dir("wal-checksum");
        let path = dir.join("00000000000000000011.wal");
        append(&path, vec![put(11, "gamma")]);

        let mut bytes = fs::read(&path).expect("wal file should exist");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, bytes).expect("corruption write should succeed");

        let error =
            replay_file(&StdVfs, &path, WalFileKind::Rolled).expect_err("mismatch should fail");
        assert!(error.to_string().contains("checksum"), "{error}");
    }

    #[test]
    fn rolled_replay_rejects_garbage_tail() {
        let dir = unique_temp_dir("wal-rolled-garbage");
        let path = dir.join("00000000000000000001.wal");
        append(&path, vec![put(1, "alpha")]);
        append_garbage(&path, b"garbage after a valid frame");

        let error =
            replay_file(&StdVfs, &path, WalFileKind::Rolled).expect_err("garbage should fail");
        assert!(error.to_string().contains("magic"), "{error}");
    }

    #[test]
    fn corruption_before_a_valid_frame_is_not_a_torn_tail() {
        let dir = unique_temp_dir("wal-mid-corruption");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&path, vec![put(1, "a")]);
        let first_len = fs::metadata(&path).expect("metadata").len() as usize;
        append(&path, vec![put(2, "b")]);

        let mut bytes = fs::read(&path).expect("wal file should exist");
        bytes[first_len - 1] ^= 0xFF;
        fs::write(&path, &bytes).expect("corruption write should succeed");

        let replay_error = replay_file(&StdVfs, &path, WalFileKind::Active)
            .expect_err("mid-log corruption should fail");
        assert!(
            replay_error
                .to_string()
                .contains("before later valid frames")
        );
        let open_error = WalWriter::open(std_vfs(), &path)
            .err()
            .expect("writer must not discard acknowledged frames");
        assert!(open_error.to_string().contains("checksum mismatch"));
        assert_eq!(
            fs::read(&path).expect("wal file should exist"),
            bytes,
            "the file must be left untouched"
        );
    }

    /// Three acknowledged frames; `corrupt` damages the first one in place.
    fn corrupt_first_of_three(prefix: &str, corrupt: impl Fn(&mut Vec<u8>)) -> (PathBuf, Vec<u8>) {
        let dir = unique_temp_dir(prefix);
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&path, vec![put(1, "a")]);
        append(&path, vec![put(2, "b")]);
        append(&path, vec![put(3, "c")]);
        let mut bytes = fs::read(&path).expect("wal file should exist");
        corrupt(&mut bytes);
        fs::write(&path, &bytes).expect("corruption write should succeed");
        (path, bytes)
    }

    fn assert_refuses_to_discard_later_frames(path: &Path, bytes: &[u8]) {
        let replay_error = replay_file(&StdVfs, path, WalFileKind::Active)
            .expect_err("replay must not silently drop acknowledged frames");
        assert!(
            replay_error
                .to_string()
                .contains("before later valid frames"),
            "{replay_error}"
        );
        assert!(
            WalWriter::open(std_vfs(), path).is_err(),
            "writer must not truncate acknowledged frames"
        );
        assert_eq!(
            fs::read(path).expect("wal file should exist"),
            bytes,
            "the file must be left untouched"
        );
    }

    #[test]
    fn corrupt_magic_before_valid_frames_is_not_a_torn_tail() {
        let (path, bytes) = corrupt_first_of_three("wal-mid-magic", |bytes| bytes[0] ^= 0xFF);
        assert_refuses_to_discard_later_frames(&path, &bytes);
    }

    #[test]
    fn corrupt_length_before_valid_frames_is_not_a_torn_tail() {
        // A flipped high bit makes the first frame claim to run past the end of the file.
        let (path, bytes) = corrupt_first_of_three("wal-mid-length", |bytes| {
            bytes[MAGIC_BYTES + LENGTH_BYTES - 1] ^= 0x80;
        });
        assert_refuses_to_discard_later_frames(&path, &bytes);
    }

    #[test]
    fn two_corrupt_frames_before_a_valid_frame_are_not_a_torn_tail() {
        let dir = unique_temp_dir("wal-two-corrupt");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&path, vec![put(1, "a")]);
        let first_end = fs::metadata(&path).expect("metadata").len() as usize;
        append(&path, vec![put(2, "b")]);
        let second_end = fs::metadata(&path).expect("metadata").len() as usize;
        append(&path, vec![put(3, "c")]);
        let mut bytes = fs::read(&path).expect("wal file should exist");
        bytes[first_end - 1] ^= 0xFF;
        bytes[second_end - 1] ^= 0xFF;
        fs::write(&path, &bytes).expect("corruption write should succeed");
        assert_refuses_to_discard_later_frames(&path, &bytes);
    }

    #[test]
    fn rolled_replay_rejects_a_truncated_final_frame() {
        let dir = unique_temp_dir("wal-rolled-truncated");
        let path = dir.join("00000000000000000002.wal");
        append(&path, vec![put(1, "a")]);
        append(&path, vec![put(2, "b")]);
        let bytes = fs::read(&path).expect("wal file should exist");
        fs::write(&path, &bytes[..bytes.len() - 1]).expect("truncation should succeed");

        let error = replay_file(&StdVfs, &path, WalFileKind::Rolled)
            .expect_err("rotation only rolls repaired files, so a torn rolled file is corrupt");
        assert!(error.to_string().contains("truncated frame"), "{error}");
    }

    #[test]
    fn failed_append_rolls_back_to_the_last_synced_frame() {
        let dir = unique_temp_dir("wal-append-rollback");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&path, vec![put(1, "a")]);
        let committed = fs::read(&path).expect("wal file should exist");

        let mut writer = WalWriter::open(std_vfs(), &path).expect("writer should open");
        // Stand in for a frame whose write succeeded but whose fsync then failed.
        writer
            .file
            .append(&[IoSlice::new(&encode_frame(b"{\"records\":[]}"))])
            .expect("write should succeed");
        let error = writer
            .roll_back_failed_append("failed to fsync WAL data", std::io::Error::other("EIO"));

        assert!(
            error.to_string().contains("failed to fsync WAL data: EIO"),
            "{error}"
        );
        assert_eq!(fs::read(&path).expect("wal file should exist"), committed);
        writer
            .append_batch(&batch(vec![put(2, "b")]))
            .expect("append after rollback should succeed");
        let replayed =
            replay_file(&StdVfs, &path, WalFileKind::Rolled).expect("replay should succeed");
        assert_eq!(seq_nos(&replayed), vec![vec![1], vec![2]]);
    }

    #[test]
    fn replay_dir_rejects_a_sequence_number_written_twice() {
        let dir = unique_temp_dir("wal-duplicate-seq");
        let active_path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&active_path, vec![put(1, "a"), put(2, "b")]);
        append(&active_path, vec![put(2, "again")]);

        let error = replay_dir(&StdVfs, &dir).expect_err("a reused sequence number should fail");
        assert!(
            error
                .to_string()
                .contains("does not follow sequence number 2"),
            "{error}"
        );
    }

    #[test]
    fn open_truncates_garbage_tail() {
        let dir = unique_temp_dir("wal-garbage-tail");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&path, vec![put(1, "alpha"), put(2, "beta")]);
        let valid_len = fs::metadata(&path).expect("metadata").len();
        append_garbage(&path, b"\0\0\0\0garbage from a torn write");

        drop(WalWriter::open(std_vfs(), &path).expect("writer should open"));

        assert_eq!(fs::metadata(&path).expect("metadata").len(), valid_len);
        let replayed =
            replay_file(&StdVfs, &path, WalFileKind::Active).expect("replay should succeed");
        assert_eq!(seq_nos(&replayed), vec![vec![1, 2]]);
    }

    #[test]
    fn torn_tail_then_append_then_reopen_keeps_every_acknowledged_batch() {
        let dir = unique_temp_dir("wal-torn-append");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&path, vec![put(1, "a"), put(2, "b")]);

        // Simulate a crash while appending batch 3..=4: only part of its frame landed.
        let before_torn = fs::metadata(&path).expect("metadata").len() as usize;
        append(&path, vec![put(3, "lost"), put(4, "lost")]);
        let bytes = fs::read(&path).expect("wal file should exist");
        fs::write(
            &path,
            &bytes[..before_torn + (bytes.len() - before_torn) / 2],
        )
        .expect("tear should succeed");

        append(&path, vec![put(3, "c")]);
        append(&path, vec![put(4, "d"), put(5, "e")]);

        let replayed =
            replay_file(&StdVfs, &path, WalFileKind::Active).expect("replay should succeed");
        assert_eq!(seq_nos(&replayed), vec![vec![1, 2], vec![3], vec![4, 5]]);
        let ids = replayed
            .iter()
            .flat_map(|batch| batch.records().iter().map(|record| record.op.id().as_str()))
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["a", "b", "c", "d", "e"]);
        assert_eq!(
            replay_file(&StdVfs, &path, WalFileKind::Rolled)
                .expect("a repaired file is valid under strict replay")
                .len(),
            3
        );
    }

    #[test]
    fn open_repairs_file_that_is_only_garbage() {
        let dir = unique_temp_dir("wal-only-garbage");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        fs::write(&path, b"not a wal").expect("garbage should be written");

        drop(WalWriter::open(std_vfs(), &path).expect("writer should open"));
        assert_eq!(fs::metadata(&path).expect("metadata").len(), 0);
    }

    #[test]
    fn huge_declared_length_is_treated_as_truncated() {
        let mut bytes = WAL_MAGIC.to_le_bytes().to_vec();
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        bytes.extend_from_slice(b"tail");
        let scan = scan_frames(&bytes);
        assert_eq!(scan.valid_len, 0);
        assert_eq!(
            scan.stop.map(|stop| stop.defect),
            Some(FrameDefect::Truncated)
        );
    }

    #[test]
    fn replay_dir_after_checkpoint_skips_checkpointed_rolled_logs() {
        let dir = unique_temp_dir("wal-checkpoint-filter");
        let active_path = dir.join(ACTIVE_WAL_FILE_NAME);
        let rolled_path = dir.join("00000000000000000001.wal");

        append(&active_path, vec![put(1, "alpha")]);
        rotate_active(&std_vfs(), &active_path, &rolled_path).expect("rotation should succeed");

        fs::write(&rolled_path, b"checkpointed garbage").expect("corruption should be written");

        append(&active_path, vec![put(2, "beta")]);

        let replayed =
            replay_dir_after_checkpoint(&StdVfs, &dir, 1).expect("replay should succeed");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].seq_no, 2);
        assert_eq!(replayed[0].op.id().as_str(), "beta");
    }

    #[test]
    fn replay_dir_drops_whole_batches_at_or_below_the_checkpoint() {
        let dir = unique_temp_dir("wal-checkpoint-batches");
        let active_path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&active_path, vec![put(1, "a"), put(2, "b")]);
        append(&active_path, vec![put(3, "c"), put(4, "d")]);

        let replayed =
            replay_dir_after_checkpoint(&StdVfs, &dir, 2).expect("replay should succeed");
        assert_eq!(
            replayed
                .iter()
                .map(|record| record.seq_no)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        let all = replay_dir(&StdVfs, &dir).expect("replay should succeed");
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn replay_dir_rejects_a_batch_straddling_the_checkpoint() {
        let dir = unique_temp_dir("wal-checkpoint-straddle");
        let active_path = dir.join(ACTIVE_WAL_FILE_NAME);
        append(&active_path, vec![put(1, "a"), put(2, "b"), put(3, "c")]);

        let error = replay_dir_after_checkpoint(&StdVfs, &dir, 2)
            .expect_err("straddling batch should fail");
        assert!(
            error.to_string().contains("straddles checkpoint 2"),
            "{error}"
        );
    }

    #[test]
    fn rotation_repairs_torn_tail_before_rolling() {
        let dir = unique_temp_dir("wal-rotate-torn");
        let active_path = dir.join(ACTIVE_WAL_FILE_NAME);
        let rolled_path = dir.join("00000000000000000002.wal");
        append(&active_path, vec![put(1, "a"), put(2, "b")]);
        append_garbage(&active_path, b"torn");

        rotate_active(&std_vfs(), &active_path, &rolled_path).expect("rotation should succeed");

        let rolled = replay_file(&StdVfs, &rolled_path, WalFileKind::Rolled)
            .expect("rolled file should be clean");
        assert_eq!(seq_nos(&rolled), vec![vec![1, 2]]);
        assert_eq!(fs::metadata(&active_path).expect("metadata").len(), 0);
    }

    #[test]
    fn undecodable_checksum_valid_frame_is_a_hard_error() {
        let dir = unique_temp_dir("wal-undecodable");
        let path = dir.join(ACTIVE_WAL_FILE_NAME);
        fs::write(&path, encode_frame(b"{\"records\":[]}")).expect("frame should be written");

        let error =
            replay_file(&StdVfs, &path, WalFileKind::Active).expect_err("empty batch is invalid");
        assert!(error.to_string().contains("at least one record"), "{error}");
        drop(WalWriter::open(std_vfs(), &path).expect("writer should open"));
        assert!(
            fs::metadata(&path).expect("metadata").len() > 0,
            "checksum-valid frames are never truncated"
        );
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

    mod crash {
        use super::*;
        use logpose_vfs::{FaultPlan, FaultVfs, TearMode, is_crashed};

        const WAL_DIR: &str = "/wal";

        fn active_path() -> PathBuf {
            Path::new(WAL_DIR).join(ACTIVE_WAL_FILE_NAME)
        }

        /// A filesystem with a durable, empty active WAL.
        fn fresh_vfs(seed: u64) -> Arc<FaultVfs> {
            let vfs = FaultVfs::new(seed);
            let process = vfs.process();
            drop(WalWriter::open(Arc::clone(&process), active_path()).expect("writer should open"));
            process.sync_dir(Path::new("/")).expect("root sync");
            process.sync_dir(Path::new(WAL_DIR)).expect("wal dir sync");
            vfs
        }

        /// Batches of different sizes so torn frames land at different offsets.
        fn scenario_batches() -> Vec<WalBatch> {
            vec![
                batch(vec![put(1, "a")]),
                batch(vec![put(2, "b"), put(3, "c"), put(4, "d")]),
                batch(vec![put(5, &"e".repeat(5000))]),
                batch(vec![put(6, "f")]),
            ]
        }

        /// Append every batch, stopping at the first failure. Returns how many were acknowledged.
        fn append_until_failure(vfs: Arc<dyn Vfs>, batches: &[WalBatch]) -> usize {
            let Ok(mut writer) = WalWriter::open(vfs, active_path()) else {
                return 0;
            };
            batches
                .iter()
                .take_while(|batch| writer.append_batch(batch).is_ok())
                .count()
        }

        fn replayed_seq_nos(vfs: &dyn Vfs) -> Vec<Vec<SeqNo>> {
            seq_nos(
                &replay_file(vfs, active_path(), WalFileKind::Active)
                    .expect("recovered WAL should replay"),
            )
        }

        #[test]
        #[allow(clippy::panic)]
        fn acknowledged_batches_survive_a_crash_at_every_operation() {
            let batches = scenario_batches();
            let expected = seq_nos(&batches);

            let clean = fresh_vfs(0);
            assert_eq!(
                append_until_failure(clean.process(), &batches),
                batches.len()
            );
            let total_ops = clean.mutating_ops();

            for tear in TearMode::ALL {
                for crash_after_ops in 0..=total_ops {
                    let seed = crash_after_ops * 31 + tear as u64;
                    let vfs = fresh_vfs(seed);
                    vfs.set_plan(FaultPlan {
                        crash_after_ops: Some(crash_after_ops),
                        tear,
                        ..FaultPlan::default()
                    });
                    let acked = append_until_failure(vfs.process(), &batches);
                    vfs.crash();

                    let context = format!("tear={tear:?} crash_after_ops={crash_after_ops}");
                    let recovered = replayed_seq_nos(vfs.process().as_ref());
                    assert!(
                        recovered.len() >= acked && recovered.len() <= acked + 1,
                        "{context}: acked {acked} batches but recovered {recovered:?}"
                    );
                    assert_eq!(recovered, expected[..recovered.len()], "{context}");

                    // Reopening repairs any torn tail, and the log keeps accepting batches.
                    let process = vfs.process();
                    let mut writer = WalWriter::open(Arc::clone(&process), active_path())
                        .unwrap_or_else(|error| panic!("{context}: reopen failed: {error}"));
                    let next = recovered
                        .last()
                        .and_then(|last| last.last())
                        .map_or(1, |seq| seq + 1);
                    writer
                        .append_batch(&batch(vec![put(next, "after")]))
                        .unwrap_or_else(|error| panic!("{context}: append failed: {error}"));
                    let mut with_next = recovered.clone();
                    with_next.push(vec![next]);
                    assert_eq!(
                        seq_nos(
                            &replay_file(process.as_ref(), active_path(), WalFileKind::Rolled)
                                .expect("a repaired WAL is valid under strict replay")
                        ),
                        with_next,
                        "{context}"
                    );
                }
            }
        }

        #[test]
        fn failed_fsync_rolls_back_and_the_batch_never_reappears() {
            let vfs = fresh_vfs(7);
            let process = vfs.process();
            let mut writer =
                WalWriter::open(Arc::clone(&process), active_path()).expect("writer should open");
            writer
                .append_batch(&batch(vec![put(1, "a")]))
                .expect("first append should succeed");
            // The first append used file sync 0; fail the next one.
            vfs.set_plan(FaultPlan {
                fail_sync: Some(1),
                tear: TearMode::KeepRandomPrefix,
                ..FaultPlan::default()
            });
            let error = writer
                .append_batch(&batch(vec![put(2, "lost")]))
                .expect_err("the fsync failure should fail the append");
            assert!(
                error.to_string().contains("failed to fsync WAL data"),
                "{error}"
            );
            assert_eq!(
                vfs.crash_points_hit().last(),
                Some(&CrashPoint::WalAfterRollback)
            );
            writer
                .append_batch(&batch(vec![put(2, "b")]))
                .expect("append after rollback should succeed");

            vfs.crash();
            let recovered = replay_file(vfs.process().as_ref(), active_path(), WalFileKind::Rolled)
                .expect("rolled-back WAL should be strictly valid");
            let ids = recovered
                .iter()
                .flat_map(|batch| batch.records().iter().map(|record| record.op.id().as_str()))
                .collect::<Vec<_>>();
            assert_eq!(ids, vec!["a", "b"]);
        }

        #[test]
        fn named_crash_points_in_append_and_rotation() {
            // Crash after the append but before its fsync: the batch was never acknowledged
            // and, with unsynced data dropped, is gone.
            let vfs = fresh_vfs(1);
            vfs.set_plan(FaultPlan {
                crash_at: Some(CrashPoint::WalAfterAppend),
                ..FaultPlan::default()
            });
            assert_eq!(append_until_failure(vfs.process(), &scenario_batches()), 0);
            assert_eq!(vfs.crash().triggered_at, Some(CrashPoint::WalAfterAppend));
            assert!(replayed_seq_nos(vfs.process().as_ref()).is_empty());

            // Crash after the fsync: unacknowledged, but durable.
            let vfs = fresh_vfs(2);
            vfs.set_plan(FaultPlan {
                crash_at: Some(CrashPoint::WalAfterSync),
                ..FaultPlan::default()
            });
            assert_eq!(append_until_failure(vfs.process(), &scenario_batches()), 0);
            vfs.crash();
            assert_eq!(replayed_seq_nos(vfs.process().as_ref()), vec![vec![1]]);

            // Crash right after rotation: both the rolled file and the new active file are
            // durable, and the rolled file is strictly valid.
            let vfs = fresh_vfs(3);
            assert_eq!(
                append_until_failure(vfs.process(), &scenario_batches()[..2]),
                2
            );
            vfs.set_plan(FaultPlan {
                crash_at: Some(CrashPoint::WalAfterRotateCreate),
                ..FaultPlan::default()
            });
            let rolled = Path::new(WAL_DIR).join("00000000000000000004.wal");
            let error = rotate_active(&vfs.process(), active_path(), &rolled)
                .expect_err("rotation should crash at its crash point");
            assert!(error.to_string().contains("interrupted"), "{error}");
            vfs.crash();
            let process = vfs.process();
            assert_eq!(
                seq_nos(
                    &replay_file(process.as_ref(), &rolled, WalFileKind::Rolled)
                        .expect("rolled file should be durable and valid")
                ),
                vec![vec![1], vec![2, 3, 4]]
            );
            assert!(replayed_seq_nos(process.as_ref()).is_empty());
        }

        #[test]
        fn crash_after_tail_repair_keeps_the_repair() {
            let vfs = fresh_vfs(4);
            assert_eq!(
                append_until_failure(vfs.process(), &scenario_batches()[..1]),
                1
            );
            let process = vfs.process();
            let file = process
                .open(&active_path(), OpenMode::Append)
                .expect("active WAL should open");
            file.append(&[IoSlice::new(b"torn frame bytes")])
                .expect("garbage append should succeed");
            file.sync_data().expect("garbage sync should succeed");

            vfs.set_plan(FaultPlan {
                crash_at: Some(CrashPoint::RecoveryAfterTailRepair),
                ..FaultPlan::default()
            });
            let error = WalWriter::open(Arc::clone(&process), active_path())
                .err()
                .expect("open should crash after repairing the tail");
            assert!(
                error.to_string().contains("tail repair interrupted"),
                "{error}"
            );
            assert!(
                is_crashed(&process.list(Path::new("/")).expect_err("process is halted")),
                "every later call fails"
            );
            vfs.crash();
            assert_eq!(
                seq_nos(
                    &replay_file(vfs.process().as_ref(), active_path(), WalFileKind::Rolled)
                        .expect("the synced repair must survive the crash")
                ),
                vec![vec![1]]
            );
        }
    }
}
