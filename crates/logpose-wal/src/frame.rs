//! WAL v2 frame header layout and outgoing frames.
//!
//! All integers are little-endian and every checksum is CRC-32C:
//!
//! ```text
//! offset size field
//!      0    4 magic          0x3257_504C ("LPW2")
//!      4    4 header_crc     crc32c(file salt || bytes 8..48), see below
//!      8    4 payload_len    bytes of payload, <= MAX_FRAME_PAYLOAD (64 MiB)
//!     12    1 frame_type     1 = WriteBatch, 2 = SchemaChange, 3 = Checkpoint
//!     13    1 flags          bit 0 reserved for compression (0); bit 1 GROUP_END
//!     14    2 format_version 1
//!     16    8 epoch          ownership epoch, 0 until Phase 7
//!     24    8 first_seq_no
//!     32    8 last_seq_no
//!     40    4 payload_crc    crc32c(payload)
//!     44    4 group_no       low 32 bits of the writer's fsync-group counter
//!     48    n payload        opaque to this layer
//!   48+n    p padding        zeros so the next frame starts 8-byte aligned (not CRC-covered)
//! ```
//!
//! The header checksum is salted with the file: it covers the first sequence number in the
//! file's name (8 bytes, little-endian) followed by header bytes 8..48. A frame is therefore
//! only valid in the file it was written to. Without the salt, the resync scan after a torn
//! header could find a checksum-valid frame inside user data (a payload that holds the raw bytes
//! of a frame from another WAL, for example as vector components), take it for a durable later
//! group, and fail a crash-recovery open with a spurious corruption error.

use crate::codec::PayloadKind;
use crate::{Epoch, WalError};
use logpose_types::SeqNo;

/// Frame magic, `"LPW2"` in little-endian byte order.
pub const FRAME_MAGIC: u32 = 0x3257_504C;
/// Header length in bytes.
pub const FRAME_HEADER_LEN: usize = 48;
/// The only `format_version` this build reads and writes.
pub const FORMAT_VERSION: u16 = 1;
/// Largest payload a frame may carry: 64 MiB.
pub const MAX_FRAME_PAYLOAD: u32 = 64 * 1024 * 1024;
/// Every frame starts at a multiple of this many bytes.
pub const FRAME_ALIGN: u64 = 8;

/// Reserved for compression; must be 0.
const FLAG_COMPRESSED: u8 = 1 << 0;
/// Set on the last frame of every fsync group.
const FLAG_GROUP_END: u8 = 1 << 1;

pub(crate) const MAGIC_BYTES: [u8; 4] = FRAME_MAGIC.to_le_bytes();
pub(crate) const ZERO_PADDING: [u8; FRAME_ALIGN as usize] = [0; FRAME_ALIGN as usize];

/// Total on-disk length of a frame with a `payload_len`-byte payload, including padding.
#[must_use]
pub fn frame_len(payload_len: u32) -> u64 {
    FRAME_HEADER_LEN as u64 + u64::from(payload_len).next_multiple_of(FRAME_ALIGN)
}

/// Padding bytes after a `payload_len`-byte payload.
pub(crate) fn padding_len(payload_len: usize) -> usize {
    payload_len.next_multiple_of(FRAME_ALIGN as usize) - payload_len
}

/// CRC-32C (Castagnoli), hardware accelerated where available.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// The header checksum: CRC-32C over `salt` (little-endian) followed by header bytes 8..48.
/// `salt` is the first sequence number in the name of the file that holds the frame.
pub(crate) fn header_crc(salt: SeqNo, bytes: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&salt.to_le_bytes()), bytes)
}

/// A decoded, validated frame header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    /// The `frame_type`.
    pub kind: PayloadKind,
    /// Whether this is the last frame of its fsync group.
    pub group_end: bool,
    /// Ownership epoch.
    pub epoch: Epoch,
    /// First sequence number covered (the checkpoint for a checkpoint frame).
    pub first_seq_no: SeqNo,
    /// Last sequence number covered (equal to `first_seq_no` except for write batches).
    pub last_seq_no: SeqNo,
    /// Payload length in bytes.
    pub payload_len: u32,
    /// CRC-32C of the payload.
    pub payload_crc: u32,
    /// Low 32 bits of the writer's fsync-group counter.
    pub group_no: u32,
}

/// Why header bytes did not decode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HeaderDefect {
    /// The magic does not match: not a frame start, or a torn write.
    BadMagic,
    /// The header checksum does not match: a torn or damaged header.
    BadChecksum,
    /// The checksum matches but the version is unknown.
    UnsupportedVersion(u16),
    /// The checksum matches but a field breaks the frame rules. No crash produces this.
    Invalid(String),
}

impl FrameHeader {
    /// Whether this frame consumes sequence numbers (write batch or schema change).
    #[must_use]
    pub fn is_data(&self) -> bool {
        self.kind != PayloadKind::Checkpoint
    }

    /// Total on-disk length of this frame, including padding.
    #[must_use]
    pub fn frame_len(&self) -> u64 {
        frame_len(self.payload_len)
    }

    /// Encode the header for the file named `salt` (its first sequence number), computing the
    /// salted `header_crc`.
    #[must_use]
    pub fn encode(&self, salt: SeqNo) -> [u8; FRAME_HEADER_LEN] {
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        bytes[0..4].copy_from_slice(&MAGIC_BYTES);
        bytes[8..12].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes[12] = self.kind.frame_type();
        bytes[13] = if self.group_end { FLAG_GROUP_END } else { 0 };
        bytes[14..16].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.epoch.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.first_seq_no.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.last_seq_no.to_le_bytes());
        bytes[40..44].copy_from_slice(&self.payload_crc.to_le_bytes());
        bytes[44..48].copy_from_slice(&self.group_no.to_le_bytes());
        let crc = header_crc(salt, &bytes[8..]);
        bytes[4..8].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    /// Decode and validate header bytes read from the file named `salt`. The checksum is
    /// verified before any field is trusted.
    pub(crate) fn decode(
        bytes: &[u8; FRAME_HEADER_LEN],
        salt: SeqNo,
    ) -> Result<Self, HeaderDefect> {
        if bytes[0..4] != MAGIC_BYTES {
            return Err(HeaderDefect::BadMagic);
        }
        if u32_at(bytes, 4) != header_crc(salt, &bytes[8..]) {
            return Err(HeaderDefect::BadChecksum);
        }
        let version = u16::from_le_bytes([bytes[14], bytes[15]]);
        if version != FORMAT_VERSION {
            return Err(HeaderDefect::UnsupportedVersion(version));
        }
        let kind = PayloadKind::from_frame_type(bytes[12])
            .ok_or_else(|| HeaderDefect::Invalid(format!("unknown frame type {}", bytes[12])))?;
        let flags = bytes[13];
        if flags & FLAG_COMPRESSED != 0 {
            return Err(HeaderDefect::Invalid(
                "compressed frames are not supported".to_owned(),
            ));
        }
        if flags & !FLAG_GROUP_END != 0 {
            return Err(HeaderDefect::Invalid(format!(
                "reserved flag bits set: {flags:#04x}"
            )));
        }
        let header = Self {
            kind,
            group_end: flags & FLAG_GROUP_END != 0,
            epoch: u64_at(bytes, 16),
            first_seq_no: u64_at(bytes, 24),
            last_seq_no: u64_at(bytes, 32),
            payload_len: u32_at(bytes, 8),
            payload_crc: u32_at(bytes, 40),
            group_no: u32_at(bytes, 44),
        };
        validate_fields(
            kind,
            header.first_seq_no,
            header.last_seq_no,
            header.payload_len as usize,
        )
        .map_err(HeaderDefect::Invalid)?;
        Ok(header)
    }
}

/// The frame rules that do not depend on neighboring frames.
fn validate_fields(
    kind: PayloadKind,
    first_seq_no: SeqNo,
    last_seq_no: SeqNo,
    payload_len: usize,
) -> Result<(), String> {
    if payload_len > MAX_FRAME_PAYLOAD as usize {
        return Err(format!(
            "payload_len {payload_len} exceeds the {MAX_FRAME_PAYLOAD}-byte limit"
        ));
    }
    match kind {
        PayloadKind::WriteBatch if first_seq_no == 0 || first_seq_no > last_seq_no => Err(format!(
            "write batch covers invalid sequence range {first_seq_no}..={last_seq_no}"
        )),
        PayloadKind::SchemaChange if first_seq_no == 0 || first_seq_no != last_seq_no => {
            Err(format!(
                "schema change must consume exactly one sequence number, got {first_seq_no}..={last_seq_no}"
            ))
        }
        PayloadKind::Checkpoint if first_seq_no != last_seq_no => Err(format!(
            "checkpoint frame must have first_seq_no == last_seq_no, got {first_seq_no}..={last_seq_no}"
        )),
        _ => Ok(()),
    }
}

fn u32_at(bytes: &[u8; FRAME_HEADER_LEN], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn u64_at(bytes: &[u8; FRAME_HEADER_LEN], at: usize) -> u64 {
    let mut word = [0u8; 8];
    word.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(word)
}

/// A frame ready to append: its payload is encoded and checksummed.
///
/// Building frames (and their payload checksums) is CPU work the engine does while the previous
/// group's I/O is in flight. The group number and `GROUP_END` flag are assigned by
/// [`WalWriter::append_group`](crate::WalWriter::append_group), which also builds the header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalFrame {
    kind: PayloadKind,
    first_seq_no: SeqNo,
    last_seq_no: SeqNo,
    payload: Vec<u8>,
    payload_crc: u32,
}

impl WalFrame {
    /// Build a frame, validating the sequence rules for `kind` and the payload size.
    ///
    /// - A write batch covers `first_seq_no..=last_seq_no` with `1 <= first_seq_no`.
    /// - A schema change consumes exactly one sequence number.
    /// - A checkpoint frame consumes none and carries `first_seq_no == last_seq_no ==
    ///   checkpoint_seq_no`.
    pub fn new(
        kind: PayloadKind,
        first_seq_no: SeqNo,
        last_seq_no: SeqNo,
        payload: Vec<u8>,
    ) -> Result<Self, WalError> {
        if payload.len() > MAX_FRAME_PAYLOAD as usize {
            return Err(WalError::FrameTooLarge {
                len: payload.len(),
                max: MAX_FRAME_PAYLOAD,
            });
        }
        validate_fields(kind, first_seq_no, last_seq_no, payload.len())
            .map_err(WalError::invalid)?;
        let payload_crc = crc32c(&payload);
        Ok(Self {
            kind,
            first_seq_no,
            last_seq_no,
            payload,
            payload_crc,
        })
    }

    /// A write-batch frame covering `first_seq_no..=last_seq_no`.
    pub fn write_batch(
        first_seq_no: SeqNo,
        last_seq_no: SeqNo,
        payload: Vec<u8>,
    ) -> Result<Self, WalError> {
        Self::new(PayloadKind::WriteBatch, first_seq_no, last_seq_no, payload)
    }

    /// A schema-change frame consuming `seq_no`.
    pub fn schema_change(seq_no: SeqNo, payload: Vec<u8>) -> Result<Self, WalError> {
        Self::new(PayloadKind::SchemaChange, seq_no, seq_no, payload)
    }

    /// A checkpoint frame for `checkpoint_seq_no`.
    pub fn checkpoint(checkpoint_seq_no: SeqNo, payload: Vec<u8>) -> Result<Self, WalError> {
        Self::new(
            PayloadKind::Checkpoint,
            checkpoint_seq_no,
            checkpoint_seq_no,
            payload,
        )
    }

    /// The frame type.
    #[must_use]
    pub fn kind(&self) -> PayloadKind {
        self.kind
    }

    /// Whether this frame consumes sequence numbers.
    #[must_use]
    pub fn is_data(&self) -> bool {
        self.kind != PayloadKind::Checkpoint
    }

    /// First sequence number covered.
    #[must_use]
    pub fn first_seq_no(&self) -> SeqNo {
        self.first_seq_no
    }

    /// Last sequence number covered.
    #[must_use]
    pub fn last_seq_no(&self) -> SeqNo {
        self.last_seq_no
    }

    /// The payload.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// On-disk length of this frame, including header and padding.
    #[must_use]
    pub fn encoded_len(&self) -> u64 {
        FRAME_HEADER_LEN as u64 + (self.payload.len() as u64).next_multiple_of(FRAME_ALIGN)
    }

    /// The header this frame gets as a member of group `group_no`.
    pub(crate) fn header(&self, epoch: Epoch, group_no: u32, group_end: bool) -> FrameHeader {
        FrameHeader {
            kind: self.kind,
            group_end,
            epoch,
            first_seq_no: self.first_seq_no,
            last_seq_no: self.last_seq_no,
            // `new` bounds the payload by MAX_FRAME_PAYLOAD, so this never saturates.
            payload_len: u32::try_from(self.payload.len()).unwrap_or(u32::MAX),
            payload_crc: self.payload_crc,
            group_no,
        }
    }

    /// Encode the whole frame for the file named `salt` (its first sequence number) into one
    /// buffer. Used by tests and golden files; the writer uses vectored appends instead.
    #[must_use]
    pub fn encode(&self, salt: SeqNo, epoch: Epoch, group_no: u32, group_end: bool) -> Vec<u8> {
        let header = self.header(epoch, group_no, group_end).encode(salt);
        let mut bytes = Vec::with_capacity(usize::try_from(self.encoded_len()).unwrap_or(0));
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&self.payload);
        bytes.extend_from_slice(&ZERO_PADDING[..padding_len(self.payload.len())]);
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The salt of the file the test frames live in.
    const SALT: SeqNo = 1;

    fn header_bytes(frame: &WalFrame) -> [u8; FRAME_HEADER_LEN] {
        frame.header(7, 42, true).encode(SALT)
    }

    #[test]
    fn header_round_trips_through_encode_and_decode() -> Result<(), WalError> {
        let frame = WalFrame::write_batch(5, 9, b"hello".to_vec())?;
        let header = frame.header(7, 42, true);
        let decoded = FrameHeader::decode(&header.encode(SALT), SALT);
        assert_eq!(decoded, Ok(header));
        assert_eq!(header.frame_len(), 48 + 8);
        Ok(())
    }

    #[test]
    fn encoding_matches_golden_bytes() -> Result<(), WalError> {
        let frame = WalFrame::write_batch(1, 2, vec![0xAB, 0xCD, 0xEF])?;
        let bytes = frame.encode(77, 0, 3, true);
        let payload_crc = crc32c(&[0xAB, 0xCD, 0xEF]);
        let mut expected = Vec::new();
        expected.extend_from_slice(b"LPW2");
        expected.extend_from_slice(&[0; 4]); // header_crc, patched below
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(&[1, 0b10]);
        expected.extend_from_slice(&1u16.to_le_bytes());
        expected.extend_from_slice(&0u64.to_le_bytes());
        expected.extend_from_slice(&1u64.to_le_bytes());
        expected.extend_from_slice(&2u64.to_le_bytes());
        expected.extend_from_slice(&payload_crc.to_le_bytes());
        expected.extend_from_slice(&3u32.to_le_bytes());
        // The header checksum covers the file's salt (77, the first sequence number in its
        // name) and then header bytes 8..48.
        let mut salted = 77u64.to_le_bytes().to_vec();
        salted.extend_from_slice(&expected[8..48]);
        let header_crc = crc32c(&salted);
        expected[4..8].copy_from_slice(&header_crc.to_le_bytes());
        expected.extend_from_slice(&[0xAB, 0xCD, 0xEF, 0, 0, 0, 0, 0]);
        assert_eq!(bytes, expected);
        assert_eq!(bytes.len() as u64, frame.encoded_len());
        // Pin the exact bytes too, so a change to the salt or the layout is deliberate.
        assert_eq!(
            bytes[..8],
            [0x4C, 0x50, 0x57, 0x32, 0xFC, 0xA6, 0x91, 0x83],
            "magic and salted header checksum"
        );
        // Pin the checksum algorithm itself: CRC-32C of "123456789" is 0xE3069283.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        Ok(())
    }

    #[test]
    fn frames_are_padded_to_eight_bytes() -> Result<(), WalError> {
        for len in 0..=17 {
            let frame = WalFrame::write_batch(1, 1, vec![1; len])?;
            let bytes = frame.encode(SALT, 0, 0, true);
            assert_eq!(bytes.len() % 8, 0);
            assert_eq!(bytes.len() as u64, frame_len(len as u32));
        }
        Ok(())
    }

    #[test]
    fn any_flipped_header_bit_is_detected() -> Result<(), WalError> {
        let frame = WalFrame::write_batch(3, 3, b"x".to_vec())?;
        let bytes = header_bytes(&frame);
        for byte in 0..FRAME_HEADER_LEN {
            for bit in 0..8 {
                let mut damaged = bytes;
                damaged[byte] ^= 1 << bit;
                assert!(
                    FrameHeader::decode(&damaged, SALT).is_err(),
                    "flip of byte {byte} bit {bit} went unnoticed"
                );
            }
        }
        Ok(())
    }

    fn resealed(mut bytes: [u8; FRAME_HEADER_LEN]) -> [u8; FRAME_HEADER_LEN] {
        let crc = header_crc(SALT, &bytes[8..]);
        bytes[4..8].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    #[test]
    fn a_header_only_validates_in_the_file_it_was_written_for() {
        let frame = WalFrame::write_batch(3, 3, b"x".to_vec());
        assert!(frame.is_ok());
        let Ok(frame) = frame else { return };
        let bytes = frame.header(0, 5, true).encode(40);
        assert!(FrameHeader::decode(&bytes, 40).is_ok());
        for other in [0, 1, 39, 41, u64::MAX] {
            assert_eq!(
                FrameHeader::decode(&bytes, other),
                Err(HeaderDefect::BadChecksum),
                "a frame from file 40 must not validate in file {other}"
            );
        }
    }

    #[test]
    fn rejects_oversized_payload_len_even_with_a_valid_checksum() -> Result<(), WalError> {
        let frame = WalFrame::write_batch(3, 3, b"x".to_vec())?;
        let mut bytes = header_bytes(&frame);
        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            FrameHeader::decode(&resealed(bytes), SALT),
            Err(HeaderDefect::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_unknown_version_type_and_flags() -> Result<(), WalError> {
        let frame = WalFrame::write_batch(3, 3, b"x".to_vec())?;
        let base = header_bytes(&frame);

        let mut version = base;
        version[14] = 2;
        assert_eq!(
            FrameHeader::decode(&resealed(version), SALT),
            Err(HeaderDefect::UnsupportedVersion(2))
        );

        let mut kind = base;
        kind[12] = 9;
        assert!(matches!(
            FrameHeader::decode(&resealed(kind), SALT),
            Err(HeaderDefect::Invalid(_))
        ));

        for flags in [0b01, 0b100, 0x80] {
            let mut flagged = base;
            flagged[13] = flags;
            assert!(matches!(
                FrameHeader::decode(&resealed(flagged), SALT),
                Err(HeaderDefect::Invalid(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn frame_constructors_enforce_sequence_rules() {
        assert!(WalFrame::write_batch(0, 0, Vec::new()).is_err());
        assert!(WalFrame::write_batch(5, 4, Vec::new()).is_err());
        assert!(WalFrame::write_batch(5, 5, Vec::new()).is_ok());
        assert!(WalFrame::new(PayloadKind::SchemaChange, 5, 6, Vec::new()).is_err());
        assert!(WalFrame::schema_change(0, Vec::new()).is_err());
        assert!(WalFrame::new(PayloadKind::Checkpoint, 5, 6, Vec::new()).is_err());
        assert!(WalFrame::checkpoint(0, Vec::new()).is_ok());
    }

    #[test]
    fn rejects_payloads_above_the_limit() {
        let payload = vec![0u8; MAX_FRAME_PAYLOAD as usize + 1];
        assert!(matches!(
            WalFrame::write_batch(1, 1, payload),
            Err(WalError::FrameTooLarge { .. })
        ));
    }
}
