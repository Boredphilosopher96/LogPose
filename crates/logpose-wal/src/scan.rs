//! Frame scanning: reading one frame, the sequence and group rules, and tail classification.

use crate::codec::PayloadKind;
use crate::{
    WalError,
    frame::{FRAME_ALIGN, FRAME_HEADER_LEN, FrameHeader, HeaderDefect, MAGIC_BYTES, crc32c},
};
use logpose_types::SeqNo;
use logpose_vfs::VfsFile;
use std::path::Path;

/// Why the bytes at an offset are not a complete, checksummed frame. Each of these is what a
/// torn write can leave behind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TornDefect {
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
    pub(crate) fn describe(self) -> &'static str {
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
pub(crate) enum Probe {
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
pub(crate) fn probe(
    file: &dyn VfsFile,
    path: &Path,
    salt: u64,
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
    let header = match FrameHeader::decode(&bytes, salt) {
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
pub(crate) struct Continuity {
    /// Last sequence number of the last data frame seen.
    pub(crate) last_data_seq: Option<SeqNo>,
    /// `(group_no, group_end)` of the last frame seen.
    pub(crate) last_frame: Option<(u32, bool)>,
    /// First sequence number in the current file's name.
    file_first_seq: SeqNo,
    /// Whether the current file had a data frame yet.
    file_has_data: bool,
    /// Whether the current file had any frame yet.
    file_has_frame: bool,
    /// Whether a file was started, so the next file must continue it.
    in_file: bool,
}

impl Continuity {
    /// Start a new file. Files must continue the sequence exactly where the previous file's data
    /// ended and the previous file must end on a group boundary. A file followed by another one
    /// must hold a data frame: rotation never leaves one without, so an older file with none has
    /// lost the operations between its name and the next file's.
    pub(crate) fn start_file(&mut self, first_seq_no: SeqNo) -> Result<(), String> {
        if let Some((group_no, false)) = self.last_frame {
            return Err(format!(
                "the previous WAL file ends inside fsync group {group_no}"
            ));
        }
        if self.in_file && !self.file_has_data {
            return Err(format!(
                "the previous WAL file, named for sequence number {}, holds no data frame, so sequence numbers {}..{first_seq_no} are missing",
                self.file_first_seq, self.file_first_seq
            ));
        }
        if let Some(last) = self.last_data_seq
            && Some(first_seq_no) != last.checked_add(1)
        {
            return Err(format!(
                "file starts at sequence number {first_seq_no} but the previous file ends at {last}"
            ));
        }
        self.in_file = true;
        self.file_first_seq = first_seq_no;
        self.file_has_data = false;
        self.file_has_frame = false;
        Ok(())
    }

    /// Whether the current file has a data frame so far.
    pub(crate) fn file_has_data(&self) -> bool {
        self.file_has_data
    }

    /// Check one checksummed frame against the frames before it, then record it.
    ///
    /// Every file starts with a checkpoint frame that is a group of its own, synced before
    /// anything else is appended. Tail repair relies on this: damage at offset 0 is then damage
    /// to a durable group, never part of a torn one.
    pub(crate) fn frame(&mut self, header: &FrameHeader) -> Result<(), String> {
        if !self.file_has_frame && (header.kind != PayloadKind::Checkpoint || !header.group_end) {
            return Err(format!(
                "the first frame of a WAL file must be a checkpoint frame that ends its own group, got a {} frame{}",
                header.kind,
                if header.group_end {
                    ""
                } else {
                    " without GROUP_END"
                }
            ));
        }
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
        self.file_has_frame = true;
        Ok(())
    }
}

/// The result of scanning the highest-named file for tail repair.
#[derive(Debug)]
pub(crate) struct TailScan {
    /// End offset of the last complete group: everything after it is discarded.
    pub(crate) committed_end: u64,
    /// Checksummed frames after `committed_end` that are discarded with it: the frames of an
    /// incomplete last group, and frames of the damaged group found past the damage.
    pub(crate) discarded: Vec<FrameHeader>,
    /// The first damaged frame, if the scan stopped before the end of the file.
    pub(crate) damage: Option<(u64, TornDefect)>,
}

/// Scan the highest-named WAL file and classify its tail.
///
/// Frames are read from offset 0 until the end of the file or the first frame that is not
/// complete and checksummed. A checksummed frame that breaks the sequence or group rules is
/// corruption. Past a damaged frame, the rest of the file is searched at 8-byte steps for
/// checksummed frames; the tail is torn only if all of them belong to the damaged frame's own
/// fsync group (the group after the last `GROUP_END`) and only the last of them may carry
/// `GROUP_END`. When the damaged frame is the file's first frame, its checkpoint group, any
/// later checksummed frame at all is corruption. Anything else means a later group was durably
/// written after the damage, so it is corruption and the caller must not truncate.
pub(crate) fn scan_tail(
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
        match probe(file, path, first_seq_no, offset, len, &mut payload)? {
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
        // last complete one. `None` when the first frame of the file is damaged: that is the
        // file's checkpoint group, one frame synced before anything else was appended, so no
        // later frame can belong to it.
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
        // The first frame that breaks the rule ends the search, so a damaged frame early in a
        // large file costs one pass, not one per frame.
        let mut seen_group_end = false;
        find_frames_after(file, path, first_seq_no, search_from, len, |at, header| {
            if Some(header.group_no) != damaged_group || seen_group_end {
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
            seen_group_end = header.group_end;
            discarded.push(header);
            Ok(())
        })?;
    }
    Ok(TailScan {
        committed_end,
        discarded,
        damage,
    })
}

/// Visit the checksummed frames in `from..len`, found by searching for the magic at 8-byte
/// steps. A found frame's extent is skipped, so frames embedded in a valid frame's payload are
/// not reported. `visit` returning an error stops the search.
///
/// The file is read in windows of at most 1 MiB, and each byte is read into a window at most
/// once: after a found frame the search continues inside the current window when the frame
/// ends there.
fn find_frames_after(
    file: &dyn VfsFile,
    path: &Path,
    salt: u64,
    from: u64,
    len: u64,
    mut visit: impl FnMut(u64, FrameHeader) -> Result<(), WalError>,
) -> Result<(), WalError> {
    const WINDOW: u64 = 1 << 20;
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
                if let Probe::Frame(header) = probe(file, path, salt, at, len, &mut payload)? {
                    visit(at, header)?;
                    let end = at + header.frame_len();
                    // Frames are 8-byte aligned and sized, so `end - base` stays aligned.
                    match usize::try_from(end - base) {
                        Ok(next_index) if next_index < window.len() => {
                            index = next_index;
                            continue;
                        }
                        _ => {
                            next_base = end;
                            break;
                        }
                    }
                }
            }
            index += FRAME_ALIGN as usize;
        }
        base = next_base;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WalFrame;
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
        let frame = WalFrame::write_batch(1, 1, vec![7; 20])?.encode(1, 0, 0, true);
        let path = Path::new("/f.wal");
        let mut payload = Vec::new();

        let file = file_with(&frame[..40])?;
        assert!(matches!(
            probe(file.as_ref(), path, 1, 0, 40, &mut payload)?,
            Probe::Torn(TornDefect::ShortHeader)
        ));

        let file = file_with(&frame[..60])?;
        assert!(matches!(
            probe(file.as_ref(), path, 1, 0, 60, &mut payload)?,
            Probe::Torn(TornDefect::PastEndOfFile { frame_len: 72 })
        ));

        let mut damaged = frame.clone();
        damaged[50] ^= 1;
        let file = file_with(&damaged)?;
        let len = damaged.len() as u64;
        assert!(matches!(
            probe(file.as_ref(), path, 1, 0, len, &mut payload)?,
            Probe::Torn(TornDefect::BadPayloadChecksum { frame_len: 72 })
        ));

        damaged = frame.clone();
        damaged[20] ^= 1;
        let file = file_with(&damaged)?;
        assert!(matches!(
            probe(file.as_ref(), path, 1, 0, len, &mut payload)?,
            Probe::Torn(TornDefect::BadHeaderChecksum)
        ));

        let file = file_with(&frame)?;
        assert!(matches!(
            probe(file.as_ref(), path, 1, 0, len, &mut payload)?,
            Probe::Frame(_)
        ));
        assert_eq!(payload, vec![7; 20]);
        assert!(matches!(
            probe(file.as_ref(), path, 1, len, len, &mut payload)?,
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
        .encode(1);
        let file = file_with(&header)?;
        let mut payload = Vec::new();
        assert!(matches!(
            probe(file.as_ref(), Path::new("/f.wal"), 1, 0, 48, &mut payload)?,
            Probe::Torn(TornDefect::PastEndOfFile { .. })
        ));
        assert_eq!(payload.capacity(), 0);
        Ok(())
    }

    /// A read-only in-memory file that counts the bytes read from it.
    struct CountingFile {
        bytes: Vec<u8>,
        read: std::sync::atomic::AtomicU64,
    }

    impl CountingFile {
        fn read_bytes(&self) -> u64 {
            self.read.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl VfsFile for CountingFile {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(self.bytes.len());
            let count = buf.len().min(self.bytes.len() - start);
            buf[..count].copy_from_slice(&self.bytes[start..start + count]);
            self.read
                .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
            Ok(count)
        }
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
            if self.read_at(buf, offset)? == buf.len() {
                Ok(())
            } else {
                Err(std::io::ErrorKind::UnexpectedEof.into())
            }
        }
        fn append(&self, _bufs: &[IoSlice<'_>]) -> std::io::Result<u64> {
            Err(std::io::Error::other("read-only"))
        }
        fn sync_data(&self) -> std::io::Result<()> {
            Ok(())
        }
        fn sync_all(&self) -> std::io::Result<()> {
            Ok(())
        }
        fn len(&self) -> std::io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }
        fn set_len(&self, _len: u64) -> std::io::Result<()> {
            Err(std::io::Error::other("read-only"))
        }
    }

    /// A lone checkpoint group, then `frames` single-operation frames. With `one_group`, they
    /// form one group whose first frame's magic is destroyed (a torn group); otherwise every
    /// frame is its own group and the first one's magic is destroyed (mid-log damage).
    fn damaged_log(frames: u64, one_group: bool) -> Result<CountingFile, WalError> {
        let mut bytes = WalFrame::checkpoint(0, Vec::new())?.encode(1, 0, 0, true);
        let damaged_at = bytes.len();
        for seq in 1..=frames {
            let (group_no, group_end) = if one_group {
                (1, seq == frames)
            } else {
                (u32::try_from(seq).unwrap_or(u32::MAX), true)
            };
            bytes.extend(
                WalFrame::write_batch(seq, seq, vec![7; 8])?.encode(1, 0, group_no, group_end),
            );
        }
        bytes[damaged_at..damaged_at + 4].copy_from_slice(b"XXXX");
        Ok(CountingFile {
            bytes,
            read: std::sync::atomic::AtomicU64::new(0),
        })
    }

    #[test]
    fn tail_search_reads_each_byte_about_once() -> Result<(), WalError> {
        // A torn group of many small frames: every one of them is found and discarded, and the
        // search must not re-read a window per found frame.
        let file = damaged_log(4000, true)?;
        let len = file.bytes.len() as u64;
        let scan = scan_tail(&file, Path::new("/f.wal"), 1, len)?;
        assert_eq!(scan.committed_end, 48);
        assert_eq!(scan.discarded.len(), 3999);
        assert!(
            file.read_bytes() <= 3 * len,
            "read {} bytes of a {len}-byte file",
            file.read_bytes()
        );
        Ok(())
    }

    #[test]
    fn tail_search_stops_at_the_first_frame_of_a_later_group() -> Result<(), WalError> {
        let file = damaged_log(4000, false)?;
        let len = file.bytes.len() as u64;
        let result = scan_tail(&file, Path::new("/f.wal"), 1, len);
        assert!(
            matches!(result, Err(WalError::Corrupt { offset: 48, .. })),
            "{result:?}"
        );
        assert!(
            file.read_bytes() <= 2 * len,
            "read {} bytes of a {len}-byte file",
            file.read_bytes()
        );
        Ok(())
    }

    #[test]
    fn continuity_enforces_sequence_and_group_rules() -> Result<(), WalError> {
        let header = |first, last, group_no, group_end| {
            WalFrame::write_batch(first, last, Vec::new())
                .map(|frame| frame.header(0, group_no, group_end))
        };
        let checkpoint = |seq, group_no, group_end| {
            WalFrame::checkpoint(seq, Vec::new()).map(|frame| frame.header(0, group_no, group_end))
        };
        let mut continuity = Continuity::default();
        assert!(continuity.start_file(5).is_ok());
        // A file starts with a checkpoint frame that is a group of its own.
        assert!(continuity.clone().frame(&header(5, 5, 0, true)?).is_err());
        assert!(continuity.clone().frame(&checkpoint(4, 0, false)?).is_err());
        assert!(continuity.frame(&checkpoint(4, 0, true)?).is_ok());
        assert!(continuity.frame(&header(6, 6, 1, true)?).is_err());
        assert!(continuity.frame(&header(5, 6, 1, false)?).is_ok());
        // Same group continues; a new group number here is an error.
        assert!(continuity.clone().frame(&header(7, 7, 2, true)?).is_err());
        assert!(continuity.frame(&header(7, 7, 1, true)?).is_ok());
        // After GROUP_END the next frame must be the next group.
        assert!(continuity.clone().frame(&header(8, 8, 1, true)?).is_err());
        assert!(continuity.clone().frame(&header(9, 9, 2, true)?).is_err());
        assert!(continuity.frame(&header(8, 8, 2, true)?).is_ok());
        // The next file must start right after the last sequence number, again with a
        // checkpoint group.
        assert!(continuity.clone().start_file(10).is_err());
        assert!(continuity.start_file(9).is_ok());
        assert!(continuity.clone().frame(&header(9, 9, 3, true)?).is_err());
        assert!(continuity.frame(&checkpoint(8, 3, true)?).is_ok());
        // A file followed by another must hold a data frame.
        let mut empty = Continuity::default();
        assert!(empty.start_file(1).is_ok());
        assert!(empty.frame(&checkpoint(0, 0, true)?).is_ok());
        assert!(empty.start_file(5).is_err());
        // Group numbers wrap.
        let mut wrapping = Continuity::default();
        assert!(wrapping.start_file(1).is_ok());
        assert!(wrapping.frame(&checkpoint(0, u32::MAX, true)?).is_ok());
        assert!(wrapping.frame(&header(1, 1, 0, true)?).is_ok());
        Ok(())
    }
}
