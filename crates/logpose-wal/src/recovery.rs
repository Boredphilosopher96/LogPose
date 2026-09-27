//! Opening a WAL directory: fence check, durability barrier, tail repair and replay.

use crate::{
    FenceMarker, FrameHeader, WalConfig, WalError, WalFrame, WalWriter, data_range,
    fence::{fence_path, read_marker},
    files::{WalFile, list_dir, wal_file_name},
    scan::{Continuity, Probe, TornDefect, probe, scan_tail},
};
use logpose_types::SeqNo;
use logpose_vfs::{CrashPoint, OpenMode, Vfs, VfsFile, parent_dir};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// One frame yielded by replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayFrame {
    /// The frame header.
    pub header: FrameHeader,
    /// The payload bytes, checksum-verified.
    pub payload: Vec<u8>,
    /// The WAL file the frame is in.
    pub file: Arc<Path>,
    /// Byte offset of the frame in that file.
    pub offset: u64,
}

/// What tail repair removed from the highest-named file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TailRepair {
    /// The repaired file.
    pub file: PathBuf,
    /// Length before repair.
    pub original_len: u64,
    /// Length after repair: the end of the last complete group.
    pub repaired_len: u64,
    /// Checksummed frames that were discarded because their group was incomplete.
    pub discarded_frames: usize,
    /// Sequence range of the discarded data frames, if any.
    pub discarded_seq: Option<(SeqNo, SeqNo)>,
    /// Offset and description of the first damaged frame, if the file did not simply end
    /// inside a group.
    pub damage: Option<(u64, &'static str)>,
}

/// What [`WalRecovery`] found and did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    /// Every WAL file, in sequence order.
    pub files: Vec<PathBuf>,
    /// Files skipped because all of their operations are at or below the checkpoint. The
    /// writer's [`remove_checkpointed`](WalWriter::remove_checkpointed) deletes them.
    pub skipped_files: Vec<PathBuf>,
    /// The torn tail that was truncated, if any.
    pub tail_repair: Option<TailRepair>,
    /// A fence marker left by an earlier boot. [`WalRecovery::into_writer`] removes it.
    pub stale_fence: Option<FenceMarker>,
}

/// Recovery of one WAL directory.
///
/// [`open`](Self::open) runs everything that may modify the directory before replay; then
/// [`next_frame`](Self::next_frame) streams the frames above the checkpoint in order, one
/// payload in memory at a time, and [`into_writer`](Self::into_writer) returns the writer that
/// continues the log.
pub struct WalRecovery {
    vfs: Arc<dyn Vfs>,
    dir: PathBuf,
    config: WalConfig,
    checkpoint_seq_no: SeqNo,
    files: Vec<WalFile>,
    report: RecoveryReport,
    /// The highest-named file, open for appending, and its length after repair.
    active: Option<(Arc<dyn VfsFile>, u64)>,
    cursor: Cursor,
}

struct Cursor {
    /// Index into `files` of the file being read.
    file_index: usize,
    /// The open file, its path, its length and its salt (the first sequence number in its name).
    file: Option<(Arc<dyn VfsFile>, Arc<Path>, u64, SeqNo)>,
    offset: u64,
    continuity: Continuity,
    finished: bool,
}

impl WalRecovery {
    /// Open the WAL in `dir` for recovery over a durable checkpoint `checkpoint_seq_no`.
    ///
    /// In order:
    ///
    /// 1. **Fence check.** An `FSYNC_FAILED` marker from the current boot fails with
    ///    [`WalError::FsyncFailedSameBoot`] (an unparsable one with
    ///    [`WalError::FenceUnreadable`]) before anything is touched. A marker from an earlier
    ///    boot is reported and removed by [`into_writer`](Self::into_writer).
    /// 2. **Durability barrier.** Every WAL file is `sync_all`ed and the directory synced, so
    ///    recovery reasons only about bytes on disk (after an in-process reopen the page cache
    ///    may hold more).
    /// 3. **Tail repair** of the highest-named file: an incomplete last group is truncated and
    ///    the truncation synced; damage followed by a later durable group fails with
    ///    [`WalError::Corrupt`] and changes nothing.
    ///
    /// Files whose successor starts at or below `checkpoint_seq_no + 1` hold only checkpointed
    /// operations; they are neither read nor validated. A missing directory recovers as an
    /// empty WAL.
    pub fn open(
        vfs: Arc<dyn Vfs>,
        dir: impl AsRef<Path>,
        config: WalConfig,
        checkpoint_seq_no: SeqNo,
    ) -> Result<Self, WalError> {
        let dir = dir.as_ref().to_path_buf();
        let listing = list_dir(vfs.as_ref(), &dir)?;
        let mut report = RecoveryReport::default();

        if let Some(len) = listing.fence_len {
            let marker = read_marker(vfs.as_ref(), &dir, len)?;
            if marker.boot_id == config.boot_id {
                return Err(WalError::FsyncFailedSameBoot {
                    marker: fence_path(&dir),
                    boot_id: marker.boot_id.to_string(),
                    first_seq_no: marker.first_seq_no,
                    last_seq_no: marker.last_seq_no,
                });
            }
            tracing::warn!(
                dir = %dir.display(),
                marker_boot = %marker.boot_id,
                first_seq_no = marker.first_seq_no,
                last_seq_no = marker.last_seq_no,
                "WAL fence marker from an earlier boot: recovering from the on-disk state"
            );
            report.stale_fence = Some(marker);
        }

        let files = listing.files;
        report.files = files.iter().map(|file| file.path.clone()).collect();

        // Durability barrier.
        let mut active = None;
        for (index, wal_file) in files.iter().enumerate() {
            let is_last = index + 1 == files.len();
            let mode = if is_last {
                OpenMode::Append
            } else {
                OpenMode::Read
            };
            let file = vfs
                .open(&wal_file.path, mode)
                .map_err(|error| WalError::io("failed to open WAL file", &wal_file.path, error))?;
            file.sync_all()
                .map_err(|error| WalError::io("failed to sync WAL file", &wal_file.path, error))?;
            if is_last {
                active = Some(file);
            }
        }
        if listing.exists {
            vfs.sync_dir(&dir)
                .map_err(|error| WalError::io("failed to sync WAL directory", &dir, error))?;
        }

        // Files entirely at or below the checkpoint are skipped: file i holds only operations
        // below the first sequence number of file i + 1.
        let first_read = files
            .windows(2)
            .take_while(|pair| {
                pair[1]
                    .first_seq_no
                    .checked_sub(1)
                    .is_some_and(|last| last <= checkpoint_seq_no)
            })
            .count();
        report.skipped_files = files[..first_read]
            .iter()
            .map(|file| file.path.clone())
            .collect();
        if let Some(first) = files.get(first_read)
            && first.first_seq_no > checkpoint_seq_no.saturating_add(1)
        {
            return Err(WalError::corrupt(
                &first.path,
                0,
                format!(
                    "the oldest needed WAL file starts at sequence number {} but the checkpoint is {checkpoint_seq_no}: operations in between are missing",
                    first.first_seq_no
                ),
            ));
        }

        let active = match (active, files.last()) {
            (Some(file), Some(last)) => {
                let len = file
                    .len()
                    .map_err(|error| WalError::io("failed to stat WAL file", &last.path, error))?;
                let len = repair_tail(vfs.as_ref(), file.as_ref(), last, len, &mut report)?;
                Some((file, len))
            }
            _ => None,
        };

        Ok(Self {
            vfs,
            dir,
            config,
            checkpoint_seq_no,
            files,
            report,
            active,
            cursor: Cursor {
                file_index: first_read,
                file: None,
                offset: 0,
                continuity: Continuity::default(),
                finished: false,
            },
        })
    }

    /// What recovery found and did so far.
    #[must_use]
    pub fn report(&self) -> &RecoveryReport {
        &self.report
    }

    /// The checkpoint this recovery replays above.
    #[must_use]
    pub fn checkpoint_seq_no(&self) -> SeqNo {
        self.checkpoint_seq_no
    }

    /// The next frame whose `last_seq_no` is above the checkpoint, in log order, or `None` at the
    /// end of the log.
    ///
    /// Every frame read is checked strictly: any damage, a sequence gap, a group that does not
    /// continue the previous one, or a data frame that straddles the checkpoint is
    /// [`WalError::Corrupt`]. Tail repair already ran, so the highest-named file is held to the
    /// same standard.
    pub fn next_frame(&mut self) -> Result<Option<ReplayFrame>, WalError> {
        loop {
            if self.cursor.finished {
                return Ok(None);
            }
            let Some((file, path, len, salt)) = self.cursor.file.clone() else {
                if !self.open_next_file()? {
                    self.cursor.finished = true;
                    return Ok(None);
                }
                continue;
            };
            let offset = self.cursor.offset;
            let mut payload = Vec::new();
            match probe(file.as_ref(), &path, salt, offset, len, &mut payload)? {
                Probe::End => {
                    self.cursor.file = None;
                    self.cursor.file_index += 1;
                }
                Probe::Torn(defect) => {
                    return Err(WalError::corrupt(
                        path.as_ref(),
                        offset,
                        format!("{} in a WAL file that must be intact", defect.describe()),
                    ));
                }
                Probe::Frame(header) => {
                    self.cursor
                        .continuity
                        .frame(&header)
                        .map_err(|reason| WalError::corrupt(path.as_ref(), offset, reason))?;
                    self.cursor.offset = offset + header.frame_len();
                    if header.last_seq_no <= self.checkpoint_seq_no {
                        continue;
                    }
                    if header.is_data() && header.first_seq_no <= self.checkpoint_seq_no {
                        return Err(WalError::corrupt(
                            path.as_ref(),
                            offset,
                            format!(
                                "frame {}..={} straddles checkpoint {}",
                                header.first_seq_no, header.last_seq_no, self.checkpoint_seq_no
                            ),
                        ));
                    }
                    return Ok(Some(ReplayFrame {
                        header,
                        payload,
                        file: path,
                        offset,
                    }));
                }
            }
        }
    }

    /// Open the next file to read. Returns `false` when there is none.
    fn open_next_file(&mut self) -> Result<bool, WalError> {
        let index = self.cursor.file_index;
        let Some(wal_file) = self.files.get(index) else {
            if let Some((group_no, false)) = self.cursor.continuity.last_frame {
                // Tail repair leaves the last file on a group boundary, so this cannot happen
                // unless the file changed underneath recovery.
                let last = self.files.last().map(|file| file.path.clone());
                return Err(WalError::corrupt(
                    last.unwrap_or_else(|| self.dir.clone()),
                    0,
                    format!("the WAL ends inside fsync group {group_no}"),
                ));
            }
            return Ok(false);
        };
        let path: Arc<Path> = Arc::from(wal_file.path.as_path());
        self.cursor
            .continuity
            .start_file(wal_file.first_seq_no)
            .map_err(|reason| WalError::corrupt(path.as_ref(), 0, reason))?;
        let (file, len) = if index + 1 == self.files.len() {
            match &self.active {
                Some((file, len)) => (Arc::clone(file), *len),
                None => return Ok(false),
            }
        } else {
            let file = self
                .vfs
                .open(&wal_file.path, OpenMode::Read)
                .map_err(|error| WalError::io("failed to open WAL file", &wal_file.path, error))?;
            let len = file
                .len()
                .map_err(|error| WalError::io("failed to stat WAL file", &wal_file.path, error))?;
            (file, len)
        };
        self.cursor.file = Some((file, path, len, wal_file.first_seq_no));
        self.cursor.offset = 0;
        Ok(true)
    }

    /// Finish replay and return the writer that continues the log.
    ///
    /// Frames not yet consumed with [`next_frame`](Self::next_frame) are read and validated
    /// first. The writer appends to the highest-named file; if the directory has no WAL file, it
    /// creates the directory (made durable in its parent) and a file named
    /// `checkpoint_seq_no + 1`. A fence marker from an earlier boot is removed durably.
    ///
    /// Every WAL file starts with a checkpoint frame that is a group of its own. When the file
    /// the writer continues is empty (new, or truncated to nothing by tail repair), `checkpoint`
    /// is appended and synced as that group before the writer is returned; otherwise it is not
    /// used. It must be a checkpoint frame for the durable manifest's checkpoint.
    pub fn into_writer(mut self, checkpoint: &WalFrame) -> Result<WalWriter, WalError> {
        if checkpoint.is_data() {
            return Err(WalError::invalid(format!(
                "a new WAL file must start with a checkpoint frame, got a {} frame",
                checkpoint.kind()
            )));
        }
        while self.next_frame()?.is_some() {}
        let last_data_seq = self.cursor.continuity.last_data_seq;
        let next_seq_no = last_data_seq
            .unwrap_or(0)
            .max(self.checkpoint_seq_no)
            .checked_add(1)
            .ok_or_else(|| WalError::invalid("sequence numbers are exhausted"))?;
        let next_group_no = self
            .cursor
            .continuity
            .last_frame
            .map_or(0, |(group_no, _)| group_no.wrapping_add(1));

        if self.report.stale_fence.is_some() {
            crate::clear_fence(self.vfs.as_ref(), &self.dir)?;
        }

        let Some((file, len)) = self.active.take() else {
            create_wal_dir(self.vfs.as_ref(), &self.dir)?;
            let path = wal_path(&self.dir, next_seq_no);
            let file = self
                .vfs
                .open(&path, OpenMode::CreateNew)
                .map_err(|error| WalError::io("failed to create WAL file", &path, error))?;
            self.vfs
                .sync_dir(&self.dir)
                .map_err(|error| WalError::io("failed to sync WAL directory", &self.dir, error))?;
            return WalWriter::resume(ResumeState {
                vfs: self.vfs,
                dir: self.dir,
                config: self.config,
                files: vec![WalFile {
                    first_seq_no: next_seq_no,
                    path,
                }],
                file,
                len: 0,
                next_seq_no,
                next_group_no,
                active_has_data: false,
            })
            .start_file(checkpoint);
        };
        let Some(active) = self.files.last().cloned() else {
            return Err(WalError::invalid("the active WAL file vanished"));
        };
        // The active file's data must end exactly where the writer continues: a checkpoint
        // beyond the end of the log would otherwise leave a gap inside the file.
        let active_has_data = self.cursor.continuity.file_has_data();
        let expected_next = match last_data_seq {
            Some(last) if active_has_data => last + 1,
            _ => active.first_seq_no,
        };
        if expected_next != next_seq_no {
            return Err(WalError::corrupt(
                &active.path,
                len,
                format!(
                    "the WAL continues at sequence number {expected_next} but the checkpoint is {}",
                    self.checkpoint_seq_no
                ),
            ));
        }
        let writer = WalWriter::resume(ResumeState {
            vfs: self.vfs,
            dir: self.dir,
            config: self.config,
            files: self.files,
            file,
            len,
            next_seq_no,
            next_group_no,
            active_has_data,
        });
        if len == 0 {
            writer.start_file(checkpoint)
        } else {
            Ok(writer)
        }
    }
}

/// Everything a writer needs to continue a recovered log.
pub(crate) struct ResumeState {
    pub(crate) vfs: Arc<dyn Vfs>,
    pub(crate) dir: PathBuf,
    pub(crate) config: WalConfig,
    pub(crate) files: Vec<WalFile>,
    pub(crate) file: Arc<dyn VfsFile>,
    pub(crate) len: u64,
    pub(crate) next_seq_no: SeqNo,
    pub(crate) next_group_no: u32,
    pub(crate) active_has_data: bool,
}

/// Tail repair of the highest-named file. Returns its length afterwards.
fn repair_tail(
    vfs: &dyn Vfs,
    file: &dyn VfsFile,
    wal_file: &WalFile,
    len: u64,
    report: &mut RecoveryReport,
) -> Result<u64, WalError> {
    let scan = scan_tail(file, &wal_file.path, wal_file.first_seq_no, len)?;
    if scan.committed_end == len {
        return Ok(len);
    }
    let repair = TailRepair {
        file: wal_file.path.clone(),
        original_len: len,
        repaired_len: scan.committed_end,
        discarded_frames: scan.discarded.len(),
        discarded_seq: data_range(&scan.discarded),
        damage: scan
            .damage
            .map(|(offset, defect): (u64, TornDefect)| (offset, defect.describe())),
    };
    file.set_len(scan.committed_end)
        .map_err(|error| WalError::io("failed to truncate torn WAL tail", &wal_file.path, error))?;
    file.sync_all()
        .map_err(|error| WalError::io("failed to sync repaired WAL file", &wal_file.path, error))?;
    tracing::warn!(
        file = %repair.file.display(),
        original_len = repair.original_len,
        repaired_len = repair.repaired_len,
        discarded_frames = repair.discarded_frames,
        discarded_seq = ?repair.discarded_seq,
        damage = ?repair.damage,
        "truncated the torn tail of the WAL"
    );
    report.tail_repair = Some(repair);
    vfs.crash_point(CrashPoint::RecoveryAfterTailRepair)
        .map_err(|error| WalError::io("WAL tail repair interrupted", &wal_file.path, error))?;
    Ok(scan.committed_end)
}

/// Create `dir` and make it durable in its parent.
pub(crate) fn create_wal_dir(vfs: &dyn Vfs, dir: &Path) -> Result<(), WalError> {
    vfs.create_dir_all(dir)
        .map_err(|error| WalError::io("failed to create WAL directory", dir, error))?;
    let parent = parent_dir(dir);
    vfs.sync_dir(parent)
        .map_err(|error| WalError::io("failed to sync WAL parent directory", parent, error))
}

/// Path of the WAL file starting at `first_seq_no`.
pub(crate) fn wal_path(dir: &Path, first_seq_no: SeqNo) -> PathBuf {
    dir.join(wal_file_name(first_seq_no))
}
