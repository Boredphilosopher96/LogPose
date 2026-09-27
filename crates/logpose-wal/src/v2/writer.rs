//! The WAL v2 writer: group append, failed-group rollback, rotation and checkpoint truncation.

use super::{
    FenceMarker, FrameHeader, WalConfig, WalError, WalFrame, WriteOutcome, data_range,
    fence::write_fence,
    files::WalFile,
    frame::{FRAME_HEADER_LEN, ZERO_PADDING, padding_len},
    recovery::{ResumeState, wal_path},
};
use logpose_types::SeqNo;
use logpose_vfs::{CrashPoint, OpenMode, Vfs, VfsFile};
use std::{
    io::{self, IoSlice},
    path::{Path, PathBuf},
    sync::Arc,
};

/// Where a successfully synced group landed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupCommit {
    /// The group's number (low 32 bits of the group counter).
    pub group_no: u32,
    /// Sequence range of the group's data frames, if it has any.
    pub seq_range: Option<(SeqNo, SeqNo)>,
    /// The file the group was appended to.
    pub file: PathBuf,
    /// Offset of the group's first frame.
    pub start_offset: u64,
    /// Offset just past the group's last frame: the file's synced length.
    pub end_offset: u64,
}

/// Appends fsync groups to the highest-named WAL file.
///
/// Obtained from [`WalRecovery::into_writer`](super::WalRecovery::into_writer). Every method is
/// blocking and must run on the I/O pool. After a failed append, sync or rotation whose outcome
/// is not a clean rollback, or after an interrupted write, the writer refuses every further
/// write with [`WalError::WriterFailed`].
pub struct WalWriter {
    vfs: Arc<dyn Vfs>,
    dir: PathBuf,
    config: WalConfig,
    /// Every WAL file in sequence order; the last one is active.
    files: Vec<WalFile>,
    file: Arc<dyn VfsFile>,
    /// End of the last group whose sync returned `Ok`. Rollback truncates to it.
    synced_len: u64,
    next_seq_no: SeqNo,
    next_group_no: u32,
    /// Whether the active file holds a data frame (its first must match the file name).
    active_has_data: bool,
    failed: Option<WriteOutcome>,
}

impl WalWriter {
    pub(super) fn resume(state: ResumeState) -> Self {
        Self {
            vfs: state.vfs,
            dir: state.dir,
            config: state.config,
            files: state.files,
            file: state.file,
            synced_len: state.len,
            next_seq_no: state.next_seq_no,
            next_group_no: state.next_group_no,
            active_has_data: state.active_has_data,
            failed: None,
        }
    }

    /// The sequence number the next data frame must start at.
    #[must_use]
    pub fn next_seq_no(&self) -> SeqNo {
        self.next_seq_no
    }

    /// The group number the next group gets.
    #[must_use]
    pub fn next_group_no(&self) -> u32 {
        self.next_group_no
    }

    /// Length of the active file up to the end of the last synced group.
    #[must_use]
    pub fn synced_len(&self) -> u64 {
        self.synced_len
    }

    /// Path of the active file.
    #[must_use]
    pub fn active_path(&self) -> &Path {
        self.files
            .last()
            .map_or(self.dir.as_path(), |file| file.path.as_path())
    }

    /// Paths of every WAL file, oldest first; the last one is active.
    #[must_use]
    pub fn files(&self) -> Vec<PathBuf> {
        self.files.iter().map(|file| file.path.clone()).collect()
    }

    /// The WAL directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The outcome of the write that failed this writer, if one did.
    #[must_use]
    pub fn failure(&self) -> Option<WriteOutcome> {
        self.failed
    }

    /// Whether the active file reached the configured size and the caller should
    /// [`rotate`](Self::rotate) between groups.
    #[must_use]
    pub fn should_rotate(&self) -> bool {
        self.synced_len >= self.config.file_bytes
    }

    /// Append one fsync group: every frame in one `append` call, then one `sync_data`.
    ///
    /// The frames get this group's number, and the last one gets `GROUP_END`. Data frames must
    /// continue the sequence exactly at [`next_seq_no`](Self::next_seq_no); checkpoint frames
    /// may appear anywhere.
    ///
    /// On an append or sync error the group is rolled back: the file is truncated to the end of
    /// the last synced group and synced. If that succeeds the error carries
    /// [`WriteOutcome::NotApplied`]: the group will never be replayed, the writer stays usable,
    /// and the next group reuses this group's number and sequence numbers. If the rollback
    /// fails, the directory is fenced with `FSYNC_FAILED` and the error carries
    /// [`WriteOutcome::Unknown`]; the writer is failed for good, and when the marker could not
    /// be written either (`fenced: false`) the caller must stop the process.
    pub fn append_group(&mut self, frames: &[WalFrame]) -> Result<GroupCommit, WalError> {
        self.check_usable()?;
        let next_seq_no = self.validate_group(frames)?;
        let group_no = self.next_group_no;
        let headers: Vec<FrameHeader> = frames
            .iter()
            .enumerate()
            .map(|(index, frame)| {
                frame.header(self.config.epoch, group_no, index + 1 == frames.len())
            })
            .collect();
        let encoded: Vec<[u8; FRAME_HEADER_LEN]> =
            headers.iter().map(FrameHeader::encode).collect();
        let mut slices = Vec::with_capacity(frames.len() * 3);
        let mut group_len = 0u64;
        for (frame, header) in frames.iter().zip(&encoded) {
            slices.push(IoSlice::new(header));
            slices.push(IoSlice::new(frame.payload()));
            let padding = padding_len(frame.payload().len());
            if padding > 0 {
                slices.push(IoSlice::new(&ZERO_PADDING[..padding]));
            }
            group_len += frame.encoded_len();
        }
        let start_offset = self.synced_len;
        let end_offset = start_offset + group_len;
        let seq_range = data_range(&headers);

        match self.file.append(&slices) {
            Ok(len) if len == end_offset => {}
            Ok(len) => {
                let error = io::Error::other(format!(
                    "WAL file is {len} bytes after appending a group that should end at {end_offset}"
                ));
                return Err(self.roll_back(error, seq_range));
            }
            Err(error) => return Err(self.roll_back(error, seq_range)),
        }
        self.crash_point(CrashPoint::WalAfterAppend)?;
        if let Err(error) = self.file.sync_data() {
            return Err(self.roll_back(error, seq_range));
        }
        self.synced_len = end_offset;
        self.next_seq_no = next_seq_no;
        self.next_group_no = group_no.wrapping_add(1);
        self.active_has_data |= seq_range.is_some();
        self.crash_point(CrashPoint::WalAfterSync)?;
        Ok(GroupCommit {
            group_no,
            seq_range,
            file: self.active_path().to_path_buf(),
            start_offset,
            end_offset,
        })
    }

    /// Check a group against the sequence rules. Returns the next sequence number after it.
    fn validate_group(&self, frames: &[WalFrame]) -> Result<SeqNo, WalError> {
        if frames.is_empty() {
            return Err(WalError::invalid("a group must hold at least one frame"));
        }
        let mut next = self.next_seq_no;
        for frame in frames.iter().filter(|frame| frame.is_data()) {
            if frame.first_seq_no() != next {
                return Err(WalError::invalid(format!(
                    "data frame starts at sequence number {} but the log continues at {next}",
                    frame.first_seq_no()
                )));
            }
            next = frame
                .last_seq_no()
                .checked_add(1)
                .ok_or_else(|| WalError::invalid("sequence numbers are exhausted"))?;
        }
        Ok(next)
    }

    /// Roll the active file back to `synced_len` after `error` failed a group, fencing the
    /// directory if the rollback fails. Returns the error to report.
    fn roll_back(&mut self, error: io::Error, seq_range: Option<(SeqNo, SeqNo)>) -> WalError {
        let rollback = self
            .file
            .set_len(self.synced_len)
            .and_then(|()| self.file.sync_all());
        let outcome = match rollback {
            Ok(()) => {
                if let Err(crash) = self.vfs.crash_point(CrashPoint::WalAfterRollback) {
                    // The process "died" after a durable rollback; nothing may follow.
                    self.failed = Some(WriteOutcome::NotApplied);
                    return WalError::WriteFailed {
                        outcome: WriteOutcome::NotApplied,
                        source: crash,
                    };
                }
                tracing::warn!(
                    file = %self.active_path().display(),
                    synced_len = self.synced_len,
                    ?seq_range,
                    %error,
                    "WAL group failed and was rolled back"
                );
                WriteOutcome::NotApplied
            }
            Err(rollback_error) => {
                let outcome = WriteOutcome::Unknown {
                    fenced: self.fence(seq_range, &rollback_error),
                };
                self.failed = Some(outcome);
                outcome
            }
        };
        WalError::WriteFailed {
            outcome,
            source: error,
        }
    }

    /// Write the `FSYNC_FAILED` marker after a failed rollback. Returns whether it is durable.
    fn fence(&self, seq_range: Option<(SeqNo, SeqNo)>, rollback_error: &io::Error) -> bool {
        let (first_seq_no, last_seq_no) = seq_range.unwrap_or((self.next_seq_no, self.next_seq_no));
        let marker = FenceMarker {
            boot_id: self.config.boot_id.clone(),
            first_seq_no,
            last_seq_no,
        };
        match write_fence(self.vfs.as_ref(), &self.dir, &marker) {
            Ok(()) => {
                tracing::error!(
                    file = %self.active_path().display(),
                    first_seq_no,
                    last_seq_no,
                    %rollback_error,
                    "WAL rollback after a failed write also failed; fenced the WAL for this boot"
                );
                true
            }
            Err(fence_error) => {
                tracing::error!(
                    file = %self.active_path().display(),
                    first_seq_no,
                    last_seq_no,
                    %rollback_error,
                    %fence_error,
                    "WAL rollback and the fence marker both failed; the process must stop"
                );
                false
            }
        }
    }

    /// Start a new active file named [`next_seq_no`](Self::next_seq_no), between groups.
    ///
    /// Steps: create the file (`CreateNew`), append `checkpoint` as its own group and sync it,
    /// sync the directory, `crash_point(WalAfterRotateCreate)`, then switch. The old file is
    /// never appended again. Returns `false` without doing anything when the active file holds
    /// no data frame yet, because the new file would have the same name.
    ///
    /// If the file cannot be created, the writer stays on the old file and remains usable. Any
    /// later failure fails the writer: the new file is rolled back to empty (or the directory
    /// fenced, as for [`append_group`](Self::append_group)), because appending more to the old
    /// file while a newer one may exist would break the file-order rules.
    pub fn rotate(&mut self, checkpoint: &WalFrame) -> Result<bool, WalError> {
        self.check_usable()?;
        if checkpoint.is_data() {
            return Err(WalError::invalid(format!(
                "rotation needs a checkpoint frame, got a {} frame",
                checkpoint.kind()
            )));
        }
        if !self.active_has_data {
            return Ok(false);
        }
        let first_seq_no = self.next_seq_no;
        let path = wal_path(&self.dir, first_seq_no);
        let file = self
            .vfs
            .open(&path, OpenMode::CreateNew)
            .map_err(|error| WalError::io("failed to create WAL file", &path, error))?;

        let group_no = self.next_group_no;
        let header = checkpoint
            .header(self.config.epoch, group_no, true)
            .encode();
        let padding = &ZERO_PADDING[..padding_len(checkpoint.payload().len())];
        let written = file
            .append(&[
                IoSlice::new(&header),
                IoSlice::new(checkpoint.payload()),
                IoSlice::new(padding),
            ])
            .and_then(|_| file.sync_data());
        if let Err(error) = written {
            let outcome = match file.set_len(0).and_then(|()| file.sync_all()) {
                Ok(()) => WriteOutcome::NotApplied,
                Err(rollback_error) => WriteOutcome::Unknown {
                    fenced: self.fence(None, &rollback_error),
                },
            };
            self.failed = Some(outcome);
            return Err(WalError::WriteFailed {
                outcome,
                source: error,
            });
        }
        if let Err(error) = self.vfs.sync_dir(&self.dir) {
            self.failed = Some(WriteOutcome::NotApplied);
            return Err(WalError::WriteFailed {
                outcome: WriteOutcome::NotApplied,
                source: error,
            });
        }
        self.crash_point(CrashPoint::WalAfterRotateCreate)?;

        tracing::debug!(
            old = %self.active_path().display(),
            new = %path.display(),
            "rotated the WAL"
        );
        self.files.push(WalFile { first_seq_no, path });
        self.file = file;
        self.synced_len = checkpoint.encoded_len();
        self.next_group_no = group_no.wrapping_add(1);
        self.active_has_data = false;
        Ok(true)
    }

    /// Delete every file that holds only operations at or below `checkpoint_seq_no`, that is,
    /// every file whose successor starts at or below `checkpoint_seq_no + 1`, then sync the
    /// directory. The active file is never deleted. Returns the deleted paths.
    ///
    /// Call only after a manifest with this checkpoint is durable. Recovery skips such files
    /// anyway, so a crash that brings some of them back is harmless.
    pub fn remove_checkpointed(
        &mut self,
        checkpoint_seq_no: SeqNo,
    ) -> Result<Vec<PathBuf>, WalError> {
        let obsolete = self
            .files
            .windows(2)
            .take_while(|pair| {
                pair[1]
                    .first_seq_no
                    .checked_sub(1)
                    .is_some_and(|last| last <= checkpoint_seq_no)
            })
            .count();
        let mut removed = Vec::with_capacity(obsolete);
        let mut result = Ok(());
        for file in &self.files[..obsolete] {
            match self.vfs.remove_file(&file.path) {
                Ok(()) => removed.push(file.path.clone()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    removed.push(file.path.clone());
                }
                Err(error) => {
                    result = Err(WalError::io(
                        "failed to remove checkpointed WAL file",
                        &file.path,
                        error,
                    ));
                    break;
                }
            }
        }
        self.files.drain(..removed.len());
        result?;
        if !removed.is_empty() {
            self.vfs
                .sync_dir(&self.dir)
                .map_err(|error| WalError::io("failed to sync WAL directory", &self.dir, error))?;
        }
        Ok(removed)
    }

    fn check_usable(&self) -> Result<(), WalError> {
        match self.failed {
            Some(outcome) => Err(WalError::WriterFailed { outcome }),
            None => Ok(()),
        }
    }

    /// Report a named crash point. A crash there halts the writer: nothing after it may run.
    fn crash_point(&mut self, point: CrashPoint) -> Result<(), WalError> {
        self.vfs.crash_point(point).map_err(|error| {
            self.failed = Some(WriteOutcome::Unknown { fenced: false });
            WalError::io("WAL write interrupted", self.active_path(), error)
        })
    }
}
