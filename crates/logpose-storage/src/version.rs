//! `Version`: an immutable, self-consistent view of one collection, and the v1 state inside it.
//!
//! Until the memtable and segment v2 land, a `Version` holds the v1 manifest plus the mutable
//! delta replayed from (and appended to) the v1 WAL. Everything reachable from a published
//! `Version` is immutable; the writer builds the next one with `Arc` clones and swaps it in.

use crate::{handle::CollectionMeta, manifest::Manifest, stats::approximate_record_bytes};
use logpose_types::{LogPoseError, Result, SeqNo};
use logpose_wal::WalRecord;
use serde::{Serialize, Serializer, ser::SerializeSeq};
use std::{fmt, sync::Arc};

/// Identifier of a published [`Version`]. Strictly increasing per collection (I6).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VersionId(pub u64);

/// Counters maintained incrementally so that statistics and flush triggers are O(1).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VersionCounters {
    /// Segments in the manifest.
    pub segment_count: u32,
    /// Operations in the mutable delta.
    pub memtable_rows: u64,
    /// Approximate bytes of the mutable delta, as the flush trigger measures them.
    pub memtable_bytes: u64,
}

/// A complete, immutable, self-consistent view of one collection.
///
/// Readers `load_full()` it from the collection handle and hold the `Arc` for a whole request;
/// only the collection's writer publishes new ones.
pub struct Version {
    /// Strictly increasing per collection (I6).
    pub id: VersionId,
    /// Identity and configuration of the collection.
    pub meta: Arc<CollectionMeta>,
    /// Last sequence number of the last batch included (I3).
    pub visible_seq_no: SeqNo,
    /// Durable manifest generation at publish time.
    pub manifest_generation: u64,
    /// Every operation at or below this sequence number is in a segment.
    pub checkpoint_seq_no: SeqNo,
    /// Incrementally maintained counters.
    pub counters: VersionCounters,
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) delta: DeltaLog,
}

impl Version {
    /// The first version of a recovered or created collection.
    pub(crate) fn initial(meta: Arc<CollectionMeta>, manifest: Manifest, delta: DeltaLog) -> Self {
        Self::build(VersionId(1), meta, Arc::new(manifest), delta)
    }

    fn build(
        id: VersionId,
        meta: Arc<CollectionMeta>,
        manifest: Arc<Manifest>,
        delta: DeltaLog,
    ) -> Self {
        let visible_seq_no = visible_seq_no(&manifest, &delta);
        Self {
            id,
            meta,
            visible_seq_no,
            manifest_generation: manifest.generation,
            checkpoint_seq_no: manifest.checkpoint_seq_no,
            counters: VersionCounters {
                segment_count: u32::try_from(manifest.segments.len()).unwrap_or(u32::MAX),
                memtable_rows: delta.len() as u64,
                memtable_bytes: delta.bytes(),
            },
            manifest,
            delta,
        }
    }

    /// The successor of this version with `records` (one committed batch) appended.
    pub(crate) fn with_batch(&self, records: Vec<WalRecord>) -> Self {
        Self::build(
            self.next_id(),
            Arc::clone(&self.meta),
            Arc::clone(&self.manifest),
            self.delta.appended(records),
        )
    }

    /// The successor of this version over a new manifest and delta (flush, compaction, or a
    /// reload from disk).
    pub(crate) fn with_state(&self, manifest: Manifest, delta: DeltaLog) -> Self {
        Self::build(
            self.next_id(),
            Arc::clone(&self.meta),
            Arc::new(manifest),
            delta,
        )
    }

    /// The successor of this version over a new manifest, keeping the delta (compaction).
    pub(crate) fn with_manifest(&self, manifest: Manifest) -> Self {
        Self::build(
            self.next_id(),
            Arc::clone(&self.meta),
            Arc::new(manifest),
            self.delta.clone(),
        )
    }

    fn next_id(&self) -> VersionId {
        VersionId(self.id.0 + 1)
    }

    /// Check the invariants every published version must satisfy: `visible_seq_no` is the last
    /// sequence number included (I3), the delta is contiguous above the checkpoint, and the
    /// counters equal the values computed from the state (I13).
    pub fn check_invariants(&self) -> Result<()> {
        let fail = |message: String| {
            Err(LogPoseError::internal(format!(
                "version {} of collection '{}' violates an invariant: {message}",
                self.id.0,
                self.meta.descriptor.lookup_name()
            )))
        };
        let mut expected = None::<SeqNo>;
        for record in self.delta.iter() {
            if record.seq_no <= self.checkpoint_seq_no {
                return fail(format!(
                    "delta record {} is at or below checkpoint {}",
                    record.seq_no, self.checkpoint_seq_no
                ));
            }
            if let Some(expected) = expected
                && record.seq_no != expected
            {
                return fail(format!(
                    "delta is not contiguous: expected {expected}, found {}",
                    record.seq_no
                ));
            }
            expected = Some(record.seq_no + 1);
        }
        if self.visible_seq_no != visible_seq_no(&self.manifest, &self.delta) {
            return fail(format!("visible_seq_no {} is stale", self.visible_seq_no));
        }
        if self.manifest_generation != self.manifest.generation
            || self.checkpoint_seq_no != self.manifest.checkpoint_seq_no
        {
            return fail("manifest summary does not match the manifest".to_owned());
        }
        let recomputed = DeltaLog::from_records(self.delta.iter().cloned().collect());
        let counters = VersionCounters {
            segment_count: u32::try_from(self.manifest.segments.len()).unwrap_or(u32::MAX),
            memtable_rows: recomputed.len() as u64,
            memtable_bytes: recomputed.bytes(),
        };
        if counters != self.counters {
            return fail(format!(
                "counters {:?} differ from recomputed {counters:?}",
                self.counters
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Version")
            .field("id", &self.id)
            .field("collection", &self.meta.reference)
            .field("visible_seq_no", &self.visible_seq_no)
            .field("manifest_generation", &self.manifest_generation)
            .field("checkpoint_seq_no", &self.checkpoint_seq_no)
            .field("counters", &self.counters)
            .finish()
    }
}

/// The last sequence number reflected by `manifest` plus `delta`.
pub(crate) fn visible_seq_no(manifest: &Manifest, delta: &DeltaLog) -> SeqNo {
    delta.last_seq_no().unwrap_or_else(|| {
        manifest
            .checkpoint_seq_no
            .max(manifest.max_segment_seq_no())
    })
}

/// Batches per sealed chunk of a [`DeltaLog`].
const CHUNK_BATCHES: usize = 64;

/// The mutable delta: WAL records above the manifest checkpoint, oldest first.
///
/// A persistent append-only log. Cloning is O(1) and appending a batch copies at most
/// `CHUNK_BATCHES` pointers plus one pointer per sealed chunk, so the writer can derive the next
/// `Version` while readers keep iterating older ones.
#[derive(Clone, Default)]
pub(crate) struct DeltaLog {
    /// Full chunks of `CHUNK_BATCHES` batches each.
    sealed: Arc<Vec<Arc<[Arc<[WalRecord]>]>>>,
    /// The chunk being filled; fewer than `CHUNK_BATCHES` batches.
    open: Arc<Vec<Arc<[WalRecord]>>>,
    len: usize,
    bytes: u64,
}

impl DeltaLog {
    /// A log holding `records` as one batch.
    pub(crate) fn from_records(records: Vec<WalRecord>) -> Self {
        Self::default().appended(records)
    }

    /// This log with `records` appended as one batch. An empty batch is ignored.
    pub(crate) fn appended(&self, records: Vec<WalRecord>) -> Self {
        if records.is_empty() {
            return self.clone();
        }
        let mut next = self.clone();
        next.len += records.len();
        next.bytes += records
            .iter()
            .map(|record| approximate_record_bytes(&record.op) as u64)
            .sum::<u64>();
        let open = Arc::make_mut(&mut next.open);
        open.push(Arc::from(records));
        if open.len() == CHUNK_BATCHES {
            let chunk = Arc::from(std::mem::take(open));
            Arc::make_mut(&mut next.sealed).push(chunk);
        }
        next
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Approximate bytes of every record, as the flush trigger measures them.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn last_seq_no(&self) -> Option<SeqNo> {
        self.iter().next_back().map(|record| record.seq_no)
    }

    /// Records oldest first; reversible for newest-first resolution.
    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = &WalRecord> + '_ {
        self.sealed
            .iter()
            .flat_map(|chunk| chunk.iter())
            .chain(self.open.iter())
            .flat_map(|batch| batch.iter())
    }

    /// A copy of every record, oldest first.
    pub(crate) fn to_vec(&self) -> Vec<WalRecord> {
        self.iter().cloned().collect()
    }
}

impl fmt::Debug for DeltaLog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaLog")
            .field("len", &self.len)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Serialize for DeltaLog {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.len))?;
        for record in self.iter() {
            seq.serialize_element(record)?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::put;

    fn records(first: SeqNo, count: usize) -> Vec<WalRecord> {
        (first..)
            .take(count)
            .map(|seq_no| WalRecord {
                seq_no,
                op: put(&format!("id-{seq_no}"), vec![1.0, 0.0]),
            })
            .collect()
    }

    #[test]
    fn delta_log_appends_without_changing_earlier_snapshots() {
        let mut log = DeltaLog::default();
        let mut snapshots = Vec::new();
        let mut next = 1;
        for batch in 0..(CHUNK_BATCHES * 2 + 5) {
            let size = batch % 3 + 1;
            snapshots.push((log.clone(), next - 1));
            log = log.appended(records(next, size));
            next += size as SeqNo;
        }
        let all = log.iter().map(|record| record.seq_no).collect::<Vec<_>>();
        assert_eq!(all, (1..next).collect::<Vec<_>>());
        assert_eq!(log.len(), all.len());
        assert_eq!(log.last_seq_no(), Some(next - 1));
        let reversed = log.iter().rev().map(|record| record.seq_no);
        assert!(reversed.eq((1..next).rev()));

        for (snapshot, last) in snapshots {
            let seen = snapshot.iter().map(|record| record.seq_no);
            assert!(seen.eq(1..=last), "snapshot at {last} must be unchanged");
            assert_eq!(snapshot.len() as SeqNo, last);
        }
    }

    #[test]
    fn delta_log_tracks_bytes_like_the_flush_trigger() {
        let batch = records(1, 3);
        let expected = batch
            .iter()
            .map(|record| approximate_record_bytes(&record.op) as u64)
            .sum::<u64>();
        let log = DeltaLog::default().appended(batch).appended(Vec::new());
        assert_eq!(log.bytes(), expected);
        assert_eq!(log.len(), 3);
        assert!(DeltaLog::default().is_empty());
        assert_eq!(DeltaLog::default().last_seq_no(), None);
    }
}
