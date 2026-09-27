//! Frame scanning: reading one frame, the sequence and group rules, and tail classification.

use super::{
    WalError,
    frame::{FRAME_ALIGN, FRAME_HEADER_LEN, FrameHeader, HeaderDefect, MAGIC_BYTES, crc32c},
};
use logpose_types::SeqNo;
use logpose_vfs::VfsFile;
use std::path::Path;

/// Why the bytes at an offset are not a complete, checksummed frame. Each of these is what a
/// torn write can leave behind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TornDefect {
    /// Fewer than 48 bytes remain.
    ShortHeader,
    /// The magic does not match.
    BadMagic,
    /// The header checksum does not match.
    BadHeaderChecksum,
    /// The header is valid but the frame, `frame_len` bytes long, extends past the end of the
    /// file.
    PastEndOfFile {
        /// Length the checksummed header claims.
        frame_len: u64,
    },
    /// The header is valid but the payload checksum of the `frame_len`-byte frame does not
    /// match.
    BadPayloadChecksum {
        /// Length the checksummed header claims.
        frame_len: u64,
    },
}

impl TornDefect {
    pub(super) fn describe(self) -> &'static str {
        match self {
            Self::ShortHeader => "short frame header",
            Self::BadMagic => "bad frame magic",
            Self::BadHeaderChecksum => "bad header checksum",
            Self::PastEndOfFile { .. } => "frame extends past the end of the file",
            Self::BadPayloadChecksum { .. } => "bad payload checksum",
        }
    }

    /// The frame length a checksummed header claims, if the header was checksummed.
    fn claimed_len(self) -> Option<u64> {
        match self {
            Self::PastEndOfFile { frame_len } | Self::BadPayloadChecksum { frame_len } => {
                Some(frame_len)
            }
            _ => None,
        }
    }
}

/// What is at an offset of a file.
#[derive(Debug)]
pub(super) enum Probe {
    /// The offset is the end of the file.
    End,
    /// A complete frame whose header and payload checksums match. Its payload is in the buffer
    /// passed to [`probe`].
    Frame(FrameHeader),
    /// Not a complete frame.
    Torn(TornDefect),
}

/// Read the frame at `offset` of a file of length `len`, putting its payload into `payload`.
///
/// Lengths from disk are validated before anything is allocated: the payload buffer is sized
/// only after the header checksum matched, `payload_len` is at most the frame limit, and the
/// frame fits in the file. A header whose checksum matches but whose fields break the frame
/// rules is an error, never a torn frame: a crash cannot produce it.
pub(super) fn probe(
    file: &dyn VfsFile,
    path: &Path,
    offset: u64,
    len: u64,
    payload: &mut Vec<u8>,
) -> Result<Probe, WalError> {
    if offset >= len {
        return Ok(Probe::End);
    }
    if len - offset < FRAME_HEADER_LEN as u64 {
        return Ok(Probe::Torn(TornDefect::ShortHeader));
    }
    let mut bytes = [0u8; FRAME_HEADER_LEN];
    file.read_exact_at(&mut bytes, offset)
        .map_err(|error| WalError::io("failed to read WAL frame header", path, error))?;
    let header = match FrameHeader::decode(&bytes) {
        Ok(header) => header,
        Err(HeaderDefect::BadMagic) => return Ok(Probe::Torn(TornDefect::BadMagic)),
        Err(HeaderDefect::BadChecksum) => return Ok(Probe::Torn(TornDefect::BadHeaderChecksum)),
        Err(HeaderDefect::UnsupportedVersion(version)) => {
            return Err(WalError::UnsupportedFormatVersion {
                file: path.to_path_buf(),
                offset,
                version,
            });
        }
        Err(HeaderDefect::Invalid(reason)) => {
            return Err(WalError::corrupt(
                path,
                offset,
                format!("checksummed header breaks the frame rules: {reason}"),
            ));
        }
    };
    if header.frame_len() > len - offset {
        return Ok(Probe::Torn(TornDefect::PastEndOfFile {
            frame_len: header.frame_len(),
        }));
    }
    payload.clear();
    payload.resize(header.payload_len as usize, 0);
    file.read_exact_at(payload, offset + FRAME_HEADER_LEN as u64)
        .map_err(|error| WalError::io("failed to read WAL frame payload", path, error))?;
    if crc32c(payload) != header.payload_crc {
        return Ok(Probe::Torn(TornDefect::BadPayloadChecksum {
            frame_len: header.frame_len(),
        }));
    }
    Ok(Probe::Frame(header))
}

/// The sequence and group rules between consecutive checksummed frames, across files.
#[derive(Clone, Debug, Default)]
pub(super) struct Continuity {
    /// Last sequence number of the last data frame seen.
    pub(super) last_data_seq: Option<SeqNo>,
    /// `(group_no, group_end)` of the last frame seen.
    pub(super) last_frame: Option<(u32, bool)>,
    /// First sequence number in the current file's name.
    file_first_seq: SeqNo,
    /// Whether the current file had a data frame yet.
    file_has_data: bool,
}

impl Continuity {
    /// Start a new file. Files must continue the sequence exactly where the previous file's data
    /// ended and the previous file must end on a group boundary.
    pub(super) fn start_file(&mut self, first_seq_no: SeqNo) -> Result<(), String> {
        if let Some((group_no, false)) = self.last_frame {
            return Err(format!(
                "the previous WAL file ends inside fsync group {group_no}"
            ));
        }
        if let Some(last) = self.last_data_seq
            && Some(first_seq_no) != last.checked_add(1)
        {
            return Err(format!(
                "file starts at sequence number {first_seq_no} but the previous file ends at {last}"
            ));
        }
        self.file_first_seq = first_seq_no;
        self.file_has_data = false;
        Ok(())
    }

    /// Whether the current file has a data frame so far.
    pub(super) fn file_has_data(&self) -> bool {
        self.file_has_data
    }

    /// Check one checksummed frame against the frames before it, then record it.
    pub(super) fn frame(&mut self, header: &FrameHeader) -> Result<(), String> {
        match self.last_frame {
            Some((group_no, true)) if header.group_no != group_no.wrapping_add(1) => {
                return Err(format!(
                    "frame of group {} follows the end of group {group_no}",
                    header.group_no
                ));
            }
            Some((group_no, false)) if header.group_no != group_no => {
                return Err(format!(
                    "frame of group {} interrupts unfinished group {group_no}",
                    header.group_no
                ));
            }
            _ => {}
        }
        if header.is_data() {
            if !self.file_has_data && header.first_seq_no != self.file_first_seq {
                return Err(format!(
                    "first data frame starts at sequence number {} but the file name says {}",
                    header.first_seq_no, self.file_first_seq
                ));
            }
            if let Some(last) = self.last_data_seq
                && Some(header.first_seq_no) != last.checked_add(1)
            {
                return Err(format!(
                    "sequence discontinuity: frame starts at {} after {last}",
                    header.first_seq_no
                ));
            }
            self.last_data_seq = Some(header.last_seq_no);
            self.file_has_data = true;
        }
        self.last_frame = Some((header.group_no, header.group_end));
        Ok(())
    }
}

/// The result of scanning the highest-named file for tail repair.
#[derive(Debug)]
pub(super) struct TailScan {
    /// End offset of the last complete group: everything after it is discarded.
    pub(super) committed_end: u64,
    /// Checksummed frames after `committed_end` that are discarded with it: the frames of an
    /// incomplete last group, and frames of the damaged group found past the damage.
    pub(super) discarded: Vec<FrameHeader>,
    /// The first damaged frame, if the scan stopped before the end of the file.
    pub(super) damage: Option<(u64, TornDefect)>,
}

/// Scan the highest-named WAL file and classify its tail.
///
/// Frames are read from offset 0 until the end of the file or the first frame that is not
/// complete and checksummed. A checksummed frame that breaks the sequence or group rules is
/// corruption. Past a damaged frame, the rest of the file is searched at 8-byte steps for
/// checksummed frames; the tail is torn only if all of them belong to the damaged frame's own
/// fsync group (the group after the last `GROUP_END`) and only the last of them may carry
/// `GROUP_END`. Anything else means a later group was durably written after the damage, so it
/// is corruption and the caller must not truncate.
pub(super) fn scan_tail(
    file: &dyn VfsFile,
    path: &Path,
    first_seq_no: SeqNo,
    len: u64,
) -> Result<TailScan, WalError> {
    let mut continuity = Continuity::default();
    continuity
        .start_file(first_seq_no)
        .map_err(|reason| WalError::corrupt(path, 0, reason))?;
    let mut payload = Vec::new();
    let mut offset = 0;
    let mut committed_end = 0;
    let mut open_group = Vec::new();
    let damage = loop {
        match probe(file, path, offset, len, &mut payload)? {
            Probe::End => break None,
            Probe::Torn(defect) => break Some((offset, defect)),
            Probe::Frame(header) => {
                continuity
                    .frame(&header)
                    .map_err(|reason| WalError::corrupt(path, offset, reason))?;
                offset += header.frame_len();
                if header.group_end {
                    committed_end = offset;
                    open_group.clear();
                } else {
                    open_group.push(header);
                }
            }
        }
    };
    let mut discarded = open_group;
    if let Some((damaged_at, defect)) = damage {
        // The damaged frame's group: the unfinished group it interrupts, or the group after the
        // last complete one. Unknown only when the very first frame of the file is damaged.
        let damaged_group = continuity.last_frame.map(|(group_no, group_end)| {
            if group_end {
                group_no.wrapping_add(1)
            } else {
                group_no
            }
        });
        // A damaged frame whose header is checksummed has a trustworthy extent: bytes inside
        // it are its payload, never frames. Otherwise search right after its start.
        let search_from = damaged_at + defect.claimed_len().unwrap_or(FRAME_ALIGN);
        let later = find_frames_after(file, path, search_from, len)?;
        // When the first frame of the file is damaged the group is unknown, and every later
        // frame must agree with the first one found.
        let expected = damaged_group.or_else(|| later.first().map(|(_, header)| header.group_no));
        for (index, (at, header)) in later.iter().enumerate() {
            let is_last = index + 1 == later.len();
            if Some(header.group_no) != expected || (header.group_end && !is_last) {
                return Err(WalError::corrupt(
                    path,
                    damaged_at,
                    format!(
                        "{} at offset {damaged_at}, but a checksummed frame of group {}{} at offset {at} follows it, so a later group was durably written after the damage",
                        defect.describe(),
                        header.group_no,
                        if header.group_end { " (GROUP_END)" } else { "" },
                    ),
                ));
            }
        }
        discarded.extend(later.into_iter().map(|(_, header)| header));
    }
    Ok(TailScan {
        committed_end,
        discarded,
        damage,
    })
}

/// Checksummed frames in `from..len`, found by searching for the magic at 8-byte steps. A found
/// frame's extent is skipped, so frames embedded in a valid frame's payload are not reported.
fn find_frames_after(
    file: &dyn VfsFile,
    path: &Path,
    from: u64,
    len: u64,
) -> Result<Vec<(u64, FrameHeader)>, WalError> {
    const WINDOW: u64 = 1 << 20;
    let mut found = Vec::new();
    let mut payload = Vec::new();
    let mut window = Vec::new();
    let mut base = from.next_multiple_of(FRAME_ALIGN);
    while base + FRAME_HEADER_LEN as u64 <= len {
        let size = WINDOW.min(len - base);
        // `size` is at most WINDOW (1 MiB), so this cast never truncates.
        window.resize(size as usize, 0);
        file.read_exact_at(&mut window, base)
            .map_err(|error| WalError::io("failed to read WAL tail", path, error))?;
        let mut next_base = base + size;
        let mut index = 0;
        while index + MAGIC_BYTES.len() <= window.len() {
            if window[index..index + MAGIC_BYTES.len()] == MAGIC_BYTES {
                let at = base + index as u64;
                if let Probe::Frame(header) = probe(file, path, at, len, &mut payload)? {
                    found.push((at, header));
                    next_base = at + header.frame_len();
                    break;
                }
            }
            index += FRAME_ALIGN as usize;
        }
        base = next_base;
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::WalFrame;
    use logpose_vfs::{FaultVfs, OpenMode, Vfs};
    use std::{io::IoSlice, sync::Arc};

    fn file_with(bytes: &[u8]) -> Result<Arc<dyn VfsFile>, Box<dyn std::error::Error>> {
        let vfs = FaultVfs::new(0);
        let file = vfs.open(Path::new("/f.wal"), OpenMode::CreateNew)?;
        file.append(&[IoSlice::new(bytes)])?;
        Ok(file)
    }

    #[test]
    fn probe_reports_each_torn_defect() -> Result<(), Box<dyn std::error::Error>> {
        let frame = WalFrame::write_batch(1, 1, vec![7; 20])?.encode(0, 0, true);
        let path = Path::new("/f.wal");
        let mut payload = Vec::new();

        let file = file_with(&frame[..40])?;
        assert!(matches!(
            probe(file.as_ref(), path, 0, 40, &mut payload)?,
            Probe::Torn(TornDefect::ShortHeader)
        ));

        let file = file_with(&frame[..60])?;
        assert!(matches!(
            probe(file.as_ref(), path, 0, 60, &mut payload)?,
            Probe::Torn(TornDefect::PastEndOfFile { frame_len: 72 })
        ));

        let mut damaged = frame.clone();
        damaged[50] ^= 1;
        let file = file_with(&damaged)?;
        let len = damaged.len() as u64;
        assert!(matches!(
            probe(file.as_ref(), path, 0, len, &mut payload)?,
            Probe::Torn(TornDefect::BadPayloadChecksum { frame_len: 72 })
        ));

        damaged = frame.clone();
        damaged[20] ^= 1;
        let file = file_with(&damaged)?;
        assert!(matches!(
            probe(file.as_ref(), path, 0, len, &mut payload)?,
            Probe::Torn(TornDefect::BadHeaderChecksum)
        ));

        let file = file_with(&frame)?;
        assert!(matches!(
            probe(file.as_ref(), path, 0, len, &mut payload)?,
            Probe::Frame(_)
        ));
        assert_eq!(payload, vec![7; 20]);
        assert!(matches!(
            probe(file.as_ref(), path, len, len, &mut payload)?,
            Probe::End
        ));
        Ok(())
    }

    #[test]
    fn probe_does_not_allocate_for_a_header_claiming_a_huge_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        // A checksummed header whose payload would run far past the end of the file is torn,
        // and the payload buffer stays untouched.
        let header = FrameHeader {
            kind: crate::codec::PayloadKind::WriteBatch,
            group_end: true,
            epoch: 0,
            first_seq_no: 1,
            last_seq_no: 1,
            payload_len: 60 << 20,
            payload_crc: 0,
            group_no: 0,
        }
        .encode();
        let file = file_with(&header)?;
        let mut payload = Vec::new();
        assert!(matches!(
            probe(file.as_ref(), Path::new("/f.wal"), 0, 48, &mut payload)?,
            Probe::Torn(TornDefect::PastEndOfFile { .. })
        ));
        assert_eq!(payload.capacity(), 0);
        Ok(())
    }

    #[test]
    fn continuity_enforces_sequence_and_group_rules() -> Result<(), WalError> {
        let header = |first, last, group_no, group_end| {
            WalFrame::write_batch(first, last, Vec::new())
                .map(|frame| frame.header(0, group_no, group_end))
        };
        let mut continuity = Continuity::default();
        assert!(continuity.start_file(5).is_ok());
        assert!(continuity.frame(&header(6, 6, 0, true)?).is_err());
        assert!(continuity.frame(&header(5, 6, 0, false)?).is_ok());
        // Same group continues; a new group number here is an error.
        assert!(continuity.clone().frame(&header(7, 7, 1, true)?).is_err());
        assert!(continuity.frame(&header(7, 7, 0, true)?).is_ok());
        // After GROUP_END the next frame must be the next group.
        assert!(continuity.clone().frame(&header(8, 8, 0, true)?).is_err());
        assert!(continuity.clone().frame(&header(9, 9, 1, true)?).is_err());
        assert!(continuity.frame(&header(8, 8, 1, true)?).is_ok());
        // The next file must start right after the last sequence number.
        assert!(continuity.clone().start_file(10).is_err());
        assert!(continuity.start_file(9).is_ok());
        // Group numbers wrap.
        let mut wrapping = Continuity::default();
        assert!(wrapping.start_file(1).is_ok());
        assert!(wrapping.frame(&header(1, 1, u32::MAX, true)?).is_ok());
        assert!(wrapping.frame(&header(2, 2, 0, true)?).is_ok());
        Ok(())
    }
}
