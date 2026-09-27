//! Read-only access to the committed frames of a live WAL directory.

use crate::{
    ReplayFrame, WalError,
    files::list_dir,
    scan::{Continuity, Probe, probe},
};
use logpose_types::SeqNo;
use logpose_vfs::{OpenMode, Vfs};
use std::{path::Path, sync::Arc};

/// Read the data frames (write batches and schema changes) that cover the sequence numbers
/// `after + 1..=through`, in log order, without modifying anything.
///
/// Unlike [`WalRecovery`](crate::WalRecovery), this runs no fence check, no durability barrier
/// and no tail repair, so it is safe on a directory whose writer is appending concurrently. The
/// caller must only ask for sequence numbers that were already made durable (for the engine:
/// that some published state includes), so every frame read is complete and checksummed. The
/// read stops at the frame that ends at `through` and never looks at the bytes after it.
///
/// Files whose successor starts at or below `after + 1` are skipped, as in recovery.
///
/// # Errors
///
/// [`WalError::Corrupt`] when a needed frame is damaged or missing, when the frames break the
/// sequence or group rules, or when a frame straddles `after` or `through`; [`WalError::Io`] for
/// filesystem errors.
pub fn read_committed(
    vfs: &dyn Vfs,
    dir: &Path,
    after: SeqNo,
    through: SeqNo,
) -> Result<Vec<ReplayFrame>, WalError> {
    let mut frames = Vec::new();
    if through <= after {
        return Ok(frames);
    }
    let files = list_dir(vfs, dir)?.files;
    let first_read = files
        .windows(2)
        .take_while(|pair| {
            pair[1]
                .first_seq_no
                .checked_sub(1)
                .is_some_and(|last| last <= after)
        })
        .count();
    let mut continuity = Continuity::default();
    let mut payload = Vec::new();
    for wal_file in &files[first_read..] {
        let path: Arc<Path> = Arc::from(wal_file.path.as_path());
        continuity
            .start_file(wal_file.first_seq_no)
            .map_err(|reason| WalError::corrupt(path.as_ref(), 0, reason))?;
        let file = vfs
            .open(&wal_file.path, OpenMode::Read)
            .map_err(|error| WalError::io("failed to open WAL file", &wal_file.path, error))?;
        let len = file
            .len()
            .map_err(|error| WalError::io("failed to stat WAL file", &wal_file.path, error))?;
        let mut offset = 0;
        loop {
            let header = match probe(
                file.as_ref(),
                &path,
                wal_file.first_seq_no,
                offset,
                len,
                &mut payload,
            )? {
                Probe::End => break,
                Probe::Torn(defect) => {
                    return Err(WalError::corrupt(
                        path.as_ref(),
                        offset,
                        format!(
                            "{} before committed sequence number {through}",
                            defect.describe()
                        ),
                    ));
                }
                Probe::Frame(header) => header,
            };
            continuity
                .frame(&header)
                .map_err(|reason| WalError::corrupt(path.as_ref(), offset, reason))?;
            let frame_offset = offset;
            offset += header.frame_len();
            if !header.is_data() || header.last_seq_no <= after {
                continue;
            }
            if header.first_seq_no <= after || header.last_seq_no > through {
                return Err(WalError::corrupt(
                    path.as_ref(),
                    frame_offset,
                    format!(
                        "frame {}..={} straddles the requested range {}..={through}",
                        header.first_seq_no,
                        header.last_seq_no,
                        after + 1
                    ),
                ));
            }
            let done = header.last_seq_no == through;
            frames.push(ReplayFrame {
                header,
                payload: std::mem::take(&mut payload),
                file: Arc::clone(&path),
                offset: frame_offset,
            });
            if done {
                return Ok(frames);
            }
        }
    }
    let reached = continuity.last_data_seq.unwrap_or(after);
    Err(WalError::corrupt(
        dir,
        0,
        format!(
            "the WAL ends at sequence number {reached}, before committed sequence number {through}"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BootId, WalConfig, WalFrame, WalRecovery};
    use logpose_vfs::FaultVfs;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn reads_exactly_the_requested_range_across_files() -> TestResult {
        let vfs = FaultVfs::new(3);
        vfs.create_dir_all(Path::new("/c"))?;
        let dir = Path::new("/c/wal");
        let config = WalConfig {
            file_bytes: 1 << 20,
            epoch: 0,
            boot_id: BootId::new("boot"),
        };
        let recovery = WalRecovery::open(vfs.process(), dir, config, 0)?;
        let mut writer = recovery.into_writer(&WalFrame::checkpoint(0, Vec::new())?)?;
        writer.append_group(&[
            WalFrame::write_batch(1, 2, b"a".to_vec())?,
            WalFrame::schema_change(3, b"s".to_vec())?,
        ])?;
        writer.rotate(&WalFrame::checkpoint(0, Vec::new())?)?;
        writer.append_group(&[WalFrame::write_batch(4, 4, b"b".to_vec())?])?;
        writer.append_group(&[WalFrame::write_batch(5, 6, b"c".to_vec())?])?;

        let seqs = |frames: Vec<ReplayFrame>| {
            frames
                .into_iter()
                .map(|frame| (frame.header.first_seq_no, frame.header.last_seq_no))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            seqs(read_committed(vfs.as_ref(), dir, 0, 6)?),
            vec![(1, 2), (3, 3), (4, 4), (5, 6)]
        );
        assert_eq!(seqs(read_committed(vfs.as_ref(), dir, 3, 4)?), vec![(4, 4)]);
        assert_eq!(seqs(read_committed(vfs.as_ref(), dir, 2, 2)?), vec![]);
        assert!(matches!(
            read_committed(vfs.as_ref(), dir, 1, 4),
            Err(WalError::Corrupt { .. })
        ));
        assert!(matches!(
            read_committed(vfs.as_ref(), dir, 0, 5),
            Err(WalError::Corrupt { .. })
        ));
        assert!(matches!(
            read_committed(vfs.as_ref(), dir, 0, 9),
            Err(WalError::Corrupt { .. })
        ));
        Ok(())
    }
}
