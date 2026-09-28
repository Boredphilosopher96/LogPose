//! Deletion vectors: the two-tier copy-on-write bitmap they and memtable index postings share,
//! the per-`Version` map of deletion vectors, and the DV file codec.
//!
//! A row is never removed from a unit in place: an upsert, update, or delete of a key whose live
//! row is at `RowAddr { unit, row }` sets `row` in `deletes[unit]`. Bits are only ever set for a
//! given unit (I10); flush drops a memtable's deleted slots and compaction drops its inputs'
//! deleted rows, and each starts a fresh unit.
//!
//! Between checkpoints the WAL makes the bits durable. At a checkpoint (a flush commit) every
//! segment whose deletion vector grew gets a new immutable DV file,
//! `segments/<unit:08x>.dv.<generation:016x>`, and the manifest names the generation in force; a
//! compaction commit writes one for its output when reconciliation set any bit.

use crate::paths::SEGMENTS_DIR;
use logpose_types::{CorruptionKind, LogPoseError, Result, RowAddr, RowId, SeqNo, UnitId};
use logpose_vfs::{OpenMode, Vfs};
use roaring::RoaringBitmap;
use std::{
    fmt,
    io::IoSlice,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Most entries the recent tier of a [`CowBitmap`] holds before it is folded into the base.
pub(crate) const RECENT_MAX: u64 = 4096;

/// An append-mostly bitmap with cheap copy-on-write, shared by deletion vectors and memtable
/// index postings.
///
/// Cloning is two `Arc` increments. The first insert after a clone copies only the `recent`
/// tier (at most [`RECENT_MAX`] entries, about 8 KiB); `base` is copied once per fold, so a
/// published `Version` keeps sharing it while the writer keeps setting bits. Copy-on-write keys
/// on sharing (`Arc::make_mut`), never on a published flag.
#[derive(Clone, Default)]
pub(crate) struct CowBitmap {
    /// Large, rarely copied.
    base: Arc<RoaringBitmap>,
    /// Recent insertions, disjoint from `base`.
    recent: Arc<RoaringBitmap>,
    /// `base.len() + recent.len()`.
    len: u64,
}

impl CowBitmap {
    /// A bitmap holding exactly `bitmap`.
    pub(crate) fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        let len = bitmap.len();
        Self {
            base: Arc::new(bitmap),
            recent: Arc::default(),
            len,
        }
    }

    pub(crate) fn contains(&self, row: RowId) -> bool {
        self.base.contains(row) || self.recent.contains(row)
    }

    /// Set `row`; false if it was already set.
    pub(crate) fn insert(&mut self, row: RowId) -> bool {
        if self.contains(row) {
            return false;
        }
        Arc::make_mut(&mut self.recent).insert(row);
        self.len += 1;
        if self.recent.len() > RECENT_MAX {
            let recent = std::mem::take(&mut self.recent);
            *Arc::make_mut(&mut self.base) |= recent.as_ref();
        }
        true
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `bitmap := bitmap AND NOT self`.
    #[allow(
        dead_code,
        reason = "readers subtract deletion vectors once the read path lands"
    )]
    pub(crate) fn subtract_from(&self, bitmap: &mut RoaringBitmap) {
        *bitmap -= self.base.as_ref();
        *bitmap -= self.recent.as_ref();
    }

    /// `bitmap := bitmap OR self`.
    pub(crate) fn union_into(&self, bitmap: &mut RoaringBitmap) {
        *bitmap |= self.base.as_ref();
        *bitmap |= self.recent.as_ref();
    }

    /// `base OR recent`, for serialization.
    pub(crate) fn to_bitmap(&self) -> RoaringBitmap {
        let mut bitmap = self.base.as_ref().clone();
        bitmap |= self.recent.as_ref();
        bitmap
    }

    /// Set bits in ascending order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = RowId> + '_ {
        MergeAscending {
            left: self.base.iter().peekable(),
            right: self.recent.iter().peekable(),
        }
    }
}

/// Merge of two disjoint ascending iterators.
struct MergeAscending<L: Iterator<Item = u32>, R: Iterator<Item = u32>> {
    left: std::iter::Peekable<L>,
    right: std::iter::Peekable<R>,
}

impl<L: Iterator<Item = u32>, R: Iterator<Item = u32>> Iterator for MergeAscending<L, R> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        match (self.left.peek(), self.right.peek()) {
            (Some(left), Some(right)) if right < left => self.right.next(),
            (Some(_), _) => self.left.next(),
            (None, _) => self.right.next(),
        }
    }
}

impl PartialEq for CowBitmap {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.to_bitmap() == other.to_bitmap()
    }
}

impl fmt::Debug for CowBitmap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CowBitmap")
            .field("len", &self.len)
            .field("recent", &self.recent.len())
            .finish()
    }
}

/// Deleted rows of one unit.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DeletionVector(pub(crate) CowBitmap);

impl DeletionVector {
    pub(crate) fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self(CowBitmap::from_bitmap(bitmap))
    }

    /// Set `row`; false if it was already deleted.
    pub(crate) fn mark(&mut self, row: RowId) -> bool {
        self.0.insert(row)
    }

    pub(crate) fn contains(&self, row: RowId) -> bool {
        self.0.contains(row)
    }

    pub(crate) fn len(&self) -> u64 {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn to_bitmap(&self) -> RoaringBitmap {
        self.0.to_bitmap()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = RowId> + '_ {
        self.0.iter()
    }
}

/// The deletion vector of every unit that has a deleted row. Persistent: clone is O(1) and
/// setting a bit path-copies O(log units) nodes plus the unit's recent tier.
#[derive(Clone, Default)]
pub(crate) struct DeletionMap(imbl::OrdMap<UnitId, DeletionVector>);

impl DeletionMap {
    pub(crate) fn get(&self, unit: UnitId) -> Option<&DeletionVector> {
        self.0.get(&unit)
    }

    /// Whether `addr` is deleted.
    pub(crate) fn is_deleted(&self, addr: RowAddr) -> bool {
        self.get(addr.unit).is_some_and(|dv| dv.contains(addr.row))
    }

    /// Set `addr`'s bit; false if it was already set.
    pub(crate) fn mark(&mut self, addr: RowAddr) -> bool {
        match self.0.get_mut(&addr.unit) {
            Some(dv) => dv.mark(addr.row),
            None => {
                let mut dv = DeletionVector::default();
                dv.mark(addr.row);
                self.0.insert(addr.unit, dv);
                true
            }
        }
    }

    /// Replace `unit`'s deletion vector; an empty one removes the entry.
    pub(crate) fn set(&mut self, unit: UnitId, dv: DeletionVector) {
        if dv.is_empty() {
            self.0.remove(&unit);
        } else {
            self.0.insert(unit, dv);
        }
    }

    /// Remove `unit`'s deletion vector and return it.
    pub(crate) fn remove(&mut self, unit: UnitId) -> Option<DeletionVector> {
        self.0.remove(&unit)
    }

    /// Cardinality of `unit`'s deletion vector.
    pub(crate) fn len_of(&self, unit: UnitId) -> u64 {
        self.get(unit).map_or(0, DeletionVector::len)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&UnitId, &DeletionVector)> + '_ {
        self.0.iter()
    }

    /// Sum of every cardinality.
    pub(crate) fn total(&self) -> u64 {
        self.0.values().map(DeletionVector::len).sum()
    }
}

impl fmt::Debug for DeletionMap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_map()
            .entries(self.0.iter().map(|(unit, dv)| (unit, dv.len())))
            .finish()
    }
}

/// First bytes of every DV file.
const DV_MAGIC: [u8; 8] = *b"LPDV\x00\x00\x02\x00";
/// Bytes before the bitmap.
const DV_HEADER_LEN: usize = 40;
/// Largest bitmap a DV file may declare (a sanity bound on corrupt lengths): a bitmap of every
/// possible row id serializes to about 512 MiB.
const MAX_DV_BITMAP: u64 = 1 << 30;
/// The infix of a DV file name: `<unit:08x>.dv.<generation:016x>`.
const DV_INFIX: &str = ".dv.";

/// One DV file's contents.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DvFile {
    pub(crate) unit: UnitId,
    /// The segment's row count, for validation.
    pub(crate) row_count: u32,
    pub(crate) generation: u64,
    /// Every deletion with a sequence number at or below this is included.
    pub(crate) covered_seq_no: SeqNo,
    pub(crate) bitmap: RoaringBitmap,
}

impl DvFile {
    /// The file layout: magic, unit id, row count, generation, covered sequence number, bitmap
    /// length, the bitmap in the portable roaring format, and a CRC-32C of everything before.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let bitmap_len = self.bitmap.serialized_size();
        let mut bytes = Vec::with_capacity(DV_HEADER_LEN + bitmap_len + 4);
        bytes.extend_from_slice(&DV_MAGIC);
        bytes.extend_from_slice(&self.unit.0.to_le_bytes());
        bytes.extend_from_slice(&self.row_count.to_le_bytes());
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&self.covered_seq_no.to_le_bytes());
        bytes.extend_from_slice(&(bitmap_len as u64).to_le_bytes());
        // Writing into a Vec cannot fail.
        let _ = self.bitmap.serialize_into(&mut bytes);
        let crc = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    /// Decode and verify a DV file read from `path`. Every defect is
    /// `Corrupt { kind: DeletionVector }`.
    pub(crate) fn decode(bytes: &[u8], path: &Path) -> Result<Self> {
        let corrupt = |message: &str| dv_corrupt(path, message.to_owned());
        if bytes.len() < DV_HEADER_LEN + 4 {
            return Err(corrupt("the file is shorter than its header and checksum"));
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32c::crc32c(body) != le_u32(crc) {
            return Err(corrupt("checksum mismatch"));
        }
        if body[..8] != DV_MAGIC {
            return Err(corrupt("bad magic"));
        }
        let bitmap_len = le_u64(&body[32..40]);
        let bitmap_bytes = &body[DV_HEADER_LEN..];
        if bitmap_len > MAX_DV_BITMAP || bitmap_bytes.len() as u64 != bitmap_len {
            return Err(corrupt("the bitmap length does not match the file"));
        }
        let bitmap = RoaringBitmap::deserialize_from(bitmap_bytes)
            .map_err(|error| dv_corrupt(path, format!("the bitmap does not decode: {error}")))?;
        if bitmap.serialized_size() != bitmap_bytes.len() {
            return Err(corrupt("the bitmap is not in canonical form"));
        }
        Ok(Self {
            unit: UnitId(le_u32(&body[8..12])),
            row_count: le_u32(&body[12..16]),
            generation: le_u64(&body[16..24]),
            covered_seq_no: le_u64(&body[24..32]),
            bitmap,
        })
    }

    /// Check the file against what the manifest expects of it.
    pub(crate) fn check(
        &self,
        path: &Path,
        unit: UnitId,
        row_count: u32,
        generation: u64,
        cardinality: u32,
    ) -> Result<()> {
        let mismatch = |what: String| Err(dv_corrupt(path, what));
        if self.unit != unit || self.generation != generation {
            return mismatch(format!(
                "the file names unit {} generation {}, but the manifest expects unit {unit} \
                 generation {generation}",
                self.unit, self.generation
            ));
        }
        if self.row_count != row_count {
            return mismatch(format!(
                "the file is for a segment of {} rows, but the segment has {row_count}",
                self.row_count
            ));
        }
        if self.bitmap.len() != u64::from(cardinality) {
            return mismatch(format!(
                "the file holds {} deleted rows, but the manifest records {cardinality}",
                self.bitmap.len()
            ));
        }
        if self.bitmap.max().is_some_and(|max| max >= row_count) {
            return mismatch(format!(
                "a deleted row is at or beyond the segment's {row_count} rows"
            ));
        }
        Ok(())
    }
}

fn le_u32(bytes: &[u8]) -> u32 {
    let mut array = [0; 4];
    array.copy_from_slice(&bytes[..4]);
    u32::from_le_bytes(array)
}

fn le_u64(bytes: &[u8]) -> u64 {
    let mut array = [0; 8];
    array.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(array)
}

pub(crate) fn dv_corrupt(path: &Path, message: String) -> LogPoseError {
    LogPoseError::Corrupt {
        kind: CorruptionKind::DeletionVector,
        location: Some(path.display().to_string()),
        message: format!("deletion vector file '{}': {message}", path.display()),
    }
}

/// `segments/<unit:08x>.dv.<generation:016x>` of the collection in `dir`.
pub(crate) fn dv_path(dir: &Path, unit: UnitId, generation: u64) -> PathBuf {
    dir.join(SEGMENTS_DIR).join(dv_file_name(unit, generation))
}

pub(crate) fn dv_file_name(unit: UnitId, generation: u64) -> String {
    format!("{unit}{DV_INFIX}{generation:016x}")
}

/// The unit and generation a DV file name encodes, if it is one.
pub(crate) fn parse_dv_file_name(name: &str) -> Option<(UnitId, u64)> {
    let (unit, rest) = name.split_at_checked(8)?;
    let generation = rest.strip_prefix(DV_INFIX)?;
    let lower_hex = |text: &str| {
        text.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    if generation.len() != 16 || !lower_hex(unit) || !lower_hex(generation) {
        return None;
    }
    Some((
        UnitId(u32::from_str_radix(unit, 16).ok()?),
        u64::from_str_radix(generation, 16).ok()?,
    ))
}

/// Durably write `file` as a new file at `path`: create, one append, `sync_all`. The caller
/// syncs `segments/` before a manifest names the file.
pub(crate) fn write_dv_file(vfs: &dyn Vfs, path: &Path, file: &DvFile) -> Result<()> {
    let io = |what: &str, error| LogPoseError::io(format!("{what} '{}'", path.display()), error);
    let bytes = file.encode();
    let handle = vfs
        .open(path, OpenMode::CreateNew)
        .map_err(|error| io("failed to create", error))?;
    handle
        .append(&[IoSlice::new(&bytes)])
        .map_err(|error| io("failed to write", error))?;
    handle
        .sync_all()
        .map_err(|error| io("failed to sync", error))
}

/// Read, verify, and check the DV file the manifest names for a segment.
pub(crate) fn load_dv_file(
    vfs: &dyn Vfs,
    path: &Path,
    unit: UnitId,
    row_count: u32,
    generation: u64,
    cardinality: u32,
) -> Result<RoaringBitmap> {
    let bytes = logpose_vfs::read_file(vfs, path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            dv_corrupt(
                path,
                "the file the manifest names does not exist".to_owned(),
            )
        } else {
            LogPoseError::io(format!("failed to read '{}'", path.display()), error)
        }
    })?;
    let file = DvFile::decode(&bytes, path)?;
    file.check(path, unit, row_count, generation, cardinality)?;
    Ok(file.bitmap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clone_never_sees_later_inserts_and_the_recent_tier_folds() {
        let mut bitmap = CowBitmap::default();
        let mut snapshots = Vec::new();
        for row in 0..(3 * RECENT_MAX as u32 + 17) {
            if row % 997 == 0 {
                snapshots.push((bitmap.clone(), row));
            }
            assert!(bitmap.insert(row * 3));
            assert!(!bitmap.insert(row * 3), "a second insert reports a set bit");
            assert!(bitmap.recent.len() <= RECENT_MAX);
        }
        assert_eq!(bitmap.len(), 3 * RECENT_MAX + 17);
        assert!(
            bitmap
                .iter()
                .eq((0..(3 * RECENT_MAX as u32 + 17)).map(|row| row * 3))
        );
        for (snapshot, rows) in snapshots {
            assert_eq!(snapshot.len(), u64::from(rows));
            assert!(snapshot.iter().eq((0..rows).map(|row| row * 3)));
        }
    }

    #[test]
    fn subtract_and_union_cover_both_tiers() {
        let mut bitmap = CowBitmap::from_bitmap((0..10).collect());
        bitmap.insert(20);
        let mut all: RoaringBitmap = (0..30).collect();
        bitmap.subtract_from(&mut all);
        assert_eq!(all, (10..30).filter(|row| *row != 20).collect());
        let mut none = RoaringBitmap::new();
        bitmap.union_into(&mut none);
        assert_eq!(none, bitmap.to_bitmap());
        assert_eq!(none.len(), 11);
    }

    #[test]
    fn the_deletion_map_marks_once_and_shares_with_clones() {
        let mut map = DeletionMap::default();
        let addr = RowAddr {
            unit: UnitId(3),
            row: 7,
        };
        let before = map.clone();
        assert!(map.mark(addr));
        assert!(!map.mark(addr));
        assert!(map.is_deleted(addr));
        assert!(!before.is_deleted(addr));
        assert_eq!(map.total(), 1);
        map.set(UnitId(3), DeletionVector::default());
        assert!(map.get(UnitId(3)).is_none());
    }

    fn sample() -> DvFile {
        DvFile {
            unit: UnitId(0x2a),
            row_count: 100,
            generation: 9,
            covered_seq_no: 1234,
            bitmap: [1, 5, 99].into_iter().collect(),
        }
    }

    #[test]
    fn dv_files_round_trip_and_every_flipped_byte_is_corruption() {
        let file = sample();
        let path = Path::new("/c/segments/0000002a.dv.0000000000000009");
        let bytes = file.encode();
        assert_eq!(DvFile::decode(&bytes, path).expect("decode"), file);
        for index in 0..bytes.len() {
            let mut damaged = bytes.clone();
            damaged[index] ^= 0x40;
            let error = DvFile::decode(&damaged, path).expect_err("damage is detected");
            assert!(
                matches!(
                    error,
                    LogPoseError::Corrupt {
                        kind: CorruptionKind::DeletionVector,
                        ..
                    }
                ),
                "byte {index}: {error:?}"
            );
        }
        for len in 0..bytes.len() {
            assert!(DvFile::decode(&bytes[..len], path).is_err(), "{len}");
        }
    }

    #[test]
    fn dv_files_are_checked_against_the_manifest() {
        let file = sample();
        let path = Path::new("x");
        file.check(path, UnitId(0x2a), 100, 9, 3).expect("matches");
        for (unit, rows, generation, cardinality) in [
            (UnitId(0x2b), 100, 9, 3),
            (UnitId(0x2a), 101, 9, 3),
            (UnitId(0x2a), 100, 8, 3),
            (UnitId(0x2a), 100, 9, 2),
        ] {
            assert!(
                file.check(path, unit, rows, generation, cardinality)
                    .is_err()
            );
        }
        let mut beyond = file.clone();
        beyond.row_count = 99;
        assert!(beyond.check(path, UnitId(0x2a), 99, 9, 3).is_err());
    }

    #[test]
    fn dv_file_names_round_trip_and_reject_foreign_names() {
        let name = dv_file_name(UnitId(0x2a), 0x1f);
        assert_eq!(name, "0000002a.dv.000000000000001f");
        assert_eq!(parse_dv_file_name(&name), Some((UnitId(0x2a), 0x1f)));
        for foreign in [
            "0000002a.seg",
            "0000002a.dv.1f",
            "0000002A.dv.000000000000001f",
            "0000002a.dv.000000000000001F",
            "0000002a.dv.000000000000001f.tmp",
        ] {
            assert_eq!(parse_dv_file_name(foreign), None, "{foreign}");
        }
    }
}
