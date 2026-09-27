//! Tests for segment v2: round trips, corruption, allocation bounds, and the
//! golden file.

mod corruption;
mod fixture;
mod golden;
mod hashes;
mod random;
mod roundtrip;

use super::{MemorySource, SegmentReader, SegmentRow, format::crc};
use logpose_wal::codec::RowImage;

/// Open and fully verify `bytes`.
pub(super) fn open_verified(bytes: &[u8]) -> SegmentReader<MemorySource> {
    let reader = SegmentReader::open(MemorySource::new(bytes.to_vec())).expect("segment opens");
    reader.verify().expect("segment verifies");
    reader
}

/// Rows as `(seq_no, image)` pairs, in row order.
pub(super) fn rows_of(reader: &SegmentReader<MemorySource>) -> Vec<(u64, RowImage)> {
    reader
        .read_rows()
        .expect("rows read back")
        .into_iter()
        .map(|SegmentRow { seq_no, image }| (seq_no, image))
        .collect()
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
}

fn write_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

/// Byte-level editing of a valid segment that keeps every CRC consistent,
/// for tests that must get past the checksums.
pub(super) struct Patcher {
    pub(super) bytes: Vec<u8>,
}

impl Patcher {
    pub(super) fn new(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
        }
    }

    pub(super) fn footer_at(&self) -> usize {
        self.bytes.len() - 64
    }

    pub(super) fn table_offset(&self) -> usize {
        usize::try_from(read_u64(&self.bytes, self.footer_at())).expect("offset fits")
    }

    pub(super) fn section_count(&self) -> usize {
        read_u32(&self.bytes, self.footer_at() + 8) as usize
    }

    pub(super) fn entry_at(&self, index: usize) -> usize {
        self.table_offset() + 64 * index
    }

    /// `(kind, offset, length)` of section `index`.
    pub(super) fn entry(&self, index: usize) -> (u16, usize, usize) {
        let at = self.entry_at(index);
        let kind = u16::from_le_bytes([self.bytes[at], self.bytes[at + 1]]);
        let offset = usize::try_from(read_u64(&self.bytes, at + 8)).expect("fits");
        let length = usize::try_from(read_u64(&self.bytes, at + 16)).expect("fits");
        (kind, offset, length)
    }

    /// Index of the first section of `kind`.
    pub(super) fn find(&self, kind: u16) -> usize {
        (0..self.section_count())
            .find(|index| self.entry(*index).0 == kind)
            .expect("section of that kind exists")
    }

    pub(super) fn set_u32(&mut self, at: usize, value: u32) -> &mut Self {
        write_u32(&mut self.bytes, at, value);
        self
    }

    pub(super) fn set_u64(&mut self, at: usize, value: u64) -> &mut Self {
        write_u64(&mut self.bytes, at, value);
        self
    }

    pub(super) fn set_u16(&mut self, at: usize, value: u16) -> &mut Self {
        self.bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
        self
    }

    /// Recompute the footer CRC.
    pub(super) fn seal_footer(&mut self) -> &mut Self {
        let at = self.footer_at();
        let footer_crc = crc(&self.bytes[at..at + 52]);
        write_u32(&mut self.bytes, at + 52, footer_crc);
        self
    }

    /// Recompute the header CRC and its copy in the footer.
    pub(super) fn seal_header(&mut self) -> &mut Self {
        let header_crc = crc(&self.bytes[..124]);
        write_u32(&mut self.bytes, 124, header_crc);
        let at = self.footer_at();
        write_u32(&mut self.bytes, at + 24, header_crc);
        self.seal_footer()
    }

    /// Recompute the table CRC.
    pub(super) fn seal_table(&mut self) -> &mut Self {
        let start = self.table_offset();
        let end = start + 64 * self.section_count();
        let table_crc = crc(&self.bytes[start..end]);
        let at = self.footer_at();
        write_u32(&mut self.bytes, at + 12, table_crc);
        self.seal_footer()
    }

    /// Recompute the CRC of section `index`.
    pub(super) fn seal_section(&mut self, index: usize) -> &mut Self {
        let (_, offset, length) = self.entry(index);
        let section_crc = crc(&self.bytes[offset..offset + length]);
        let at = self.entry_at(index);
        write_u32(&mut self.bytes, at + 24, section_crc);
        self.seal_table()
    }
}
