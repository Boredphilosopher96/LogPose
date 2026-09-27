//! Segment v1 (`LPS1`) file format: header, entry table and footer types shared by the reader and writer.

use logpose_types::SeqNo;
use logpose_vfs::CrashPoint;
use serde::{Deserialize, Serialize};

mod reader;
mod writer;

pub(crate) use reader::read_segment_file;

/// Why a segment is written; selects the crash points reported while publishing it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentPurpose {
    /// A flush of the mutable delta.
    Flush,
    /// The replacement segment of a compaction.
    Compaction,
}

impl SegmentPurpose {
    /// Reported after the segment file itself is written and synced.
    fn after_file_sync(self) -> Option<CrashPoint> {
        match self {
            Self::Flush => Some(CrashPoint::FlushAfterSegmentSync),
            Self::Compaction => None,
        }
    }

    /// Reported after the segment and sidecars are renamed into place and their directories
    /// synced.
    fn after_dir_sync(self) -> CrashPoint {
        match self {
            Self::Flush => CrashPoint::FlushAfterSegmentsDirSync,
            Self::Compaction => CrashPoint::CompactionAfterOutputSync,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SegmentHeader {
    version: u16,
    dimensions: usize,
    entry_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SegmentFooter {
    payload_checksum: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SegmentEntry {
    seq_no: SeqNo,
    record_id_offset: u64,
    record_id_len: u32,
    kind: SegmentEntryKind,
    vector_offset: u64,
    vector_dimensions: u32,
    metadata_offset: u64,
    metadata_len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SegmentEntryKind {
    Put,
    Delete,
}
