//! `Version`: an immutable, self-consistent view of one collection, and the mutable delta inside
//! it.
//!
//! Until the memtable and segment v2 land (PR 10), a `Version` holds the v1 manifest plus the
//! delta: the WAL operations above the manifest checkpoint, in the v2 data model (`FieldId`-keyed
//! row images, key deletes, and schema changes), exactly as the writer's `apply` produced them.
//! It also holds the [`FileHandle`] of every segment of its manifest, so a segment file stays on
//! disk while any `Version` that contains it is alive (I7). Everything reachable from a
//! published `Version` is immutable; the writer builds the next one with `Arc` clones and swaps
//! it in.

use crate::{gc::FileHandle, handle::CollectionMeta, manifest::Manifest};
use logpose_types::{LogPoseError, Result, SeqNo, schema::CollectionSchema};
use logpose_wal::codec::{RowImage, WirePk};
use std::{fmt, sync::Arc};

/// Identifier of a published [`Version`]. Strictly increasing per collection (I6).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VersionId(pub u64);

/// Counters maintained incrementally so that statistics and flush triggers are O(1).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VersionCounters {
    /// Segments in the manifest.
    pub segment_count: u32,
    /// Operations in the mutable delta (one per sequence number).
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
    /// The schema as of `visible_seq_no`. Readers map names to field ids with this schema only.
    pub schema: Arc<CollectionSchema>,
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
    /// The files of every segment of `manifest`, ascending by unit.
    pub(crate) files: Arc<[Arc<FileHandle>]>,
}

impl Version {
    /// A version over `manifest` and `delta`.
    pub(crate) fn build(
        id: VersionId,
        meta: Arc<CollectionMeta>,
        schema: Arc<CollectionSchema>,
        manifest: Arc<Manifest>,
        delta: DeltaLog,
        files: Arc<[Arc<FileHandle>]>,
    ) -> Self {
        let visible_seq_no = visible_seq_no(&manifest, &delta);
        Self {
            id,
            meta,
            schema,
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
            files,
        }
    }

    /// The operations above the checkpoint.
    #[must_use]
    pub fn delta_len(&self) -> usize {
        self.delta.len()
    }

    /// The latest row image for `pk` in the mutable delta as of this version, with its sequence
    /// number, if the key's last operation there is a put. `None` when the key was deleted or has
    /// no operation in the delta.
    #[cfg(test)]
    pub(crate) fn delta_image(&self, pk: &WirePk) -> Option<(SeqNo, &RowImage)> {
        self.delta
            .iter()
            .rev()
            .find_map(|record| match &record.op {
                DeltaOp::Put(image) if &image.pk == pk => Some(Some((record.seq_no, image))),
                DeltaOp::Delete(deleted) if deleted == pk => Some(None),
                _ => None,
            })?
    }

    /// Check the invariants every published version must satisfy: `visible_seq_no` is the last
    /// sequence number included (I3), the delta is contiguous above the checkpoint, the schema
    /// changes in the delta end at the version's schema, and the counters equal the values
    /// computed from the state (I13).
    pub fn check_invariants(&self) -> Result<()> {
        let fail = |message: String| {
            Err(LogPoseError::Message(format!(
                "version {} of collection '{}' violates an invariant: {message}",
                self.id.0,
                self.meta.descriptor.lookup_name()
            )))
        };
        let mut expected = None::<SeqNo>;
        let mut schema_version = None;
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
            if let DeltaOp::SchemaChange {
                schema_version: version,
            } = record.op
            {
                schema_version = Some(version);
            }
        }
        if let Some(version) = schema_version
            && version > self.schema.schema_version()
        {
            return fail(format!(
                "the delta changes the schema to version {version}, but the version's schema is \
                 at {}",
                self.schema.schema_version()
            ));
        }
        if self.visible_seq_no != visible_seq_no(&self.manifest, &self.delta) {
            return fail(format!("visible_seq_no {} is stale", self.visible_seq_no));
        }
        if self.manifest_generation != self.manifest.generation
            || self.checkpoint_seq_no != self.manifest.checkpoint_seq_no
        {
            return fail("manifest summary does not match the manifest".to_owned());
        }
        if !self
            .files
            .iter()
            .map(|file| file.unit())
            .eq(self.manifest.units())
        {
            return fail("segment file handles do not match the manifest's segments".to_owned());
        }
        let mut recomputed = DeltaLog::default();
        for batch in self.delta.batches() {
            recomputed.append(batch.to_vec());
        }
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
            .field("schema_version", &self.schema.schema_version())
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

/// One operation of the mutable delta, in the v2 data model.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DeltaOp {
    /// A complete row image, keyed by field id.
    Put(RowImage),
    /// A delete by key.
    Delete(WirePk),
    /// A schema change to `schema_version`. Consumes its sequence number and changes no row;
    /// the schema itself is on the `Version`.
    SchemaChange {
        /// The schema version the change produced.
        schema_version: u64,
    },
}

impl DeltaOp {
    /// Approximate bytes, as the flush trigger measures them.
    fn approximate_bytes(&self) -> u64 {
        let pk_len = |pk: &WirePk| match pk {
            WirePk::Int64(_) => 8,
            WirePk::String(value) => value.len() as u64,
        };
        match self {
            Self::Put(image) => {
                pk_len(&image.pk)
                    + image
                        .vectors
                        .iter()
                        .map(|(_, vector)| vector.as_bytes().len() as u64)
                        .sum::<u64>()
                    + image
                        .scalars
                        .iter()
                        .map(|(_, value)| value.as_bytes().len() as u64)
                        .sum::<u64>()
                    + image
                        .dynamic
                        .as_ref()
                        .map_or(0, |dynamic| dynamic.as_bytes().len() as u64)
                    + 32
            }
            Self::Delete(pk) => pk_len(pk) + 16,
            Self::SchemaChange { .. } => 16,
        }
    }
}

/// One delta operation and its sequence number.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeltaRecord {
    pub(crate) seq_no: SeqNo,
    pub(crate) op: DeltaOp,
}

/// Batches per sealed chunk of a [`DeltaLog`].
const CHUNK_BATCHES: usize = 64;

/// The mutable delta: operations above the manifest checkpoint, oldest first, grouped by the
/// atomic batch (one WAL frame) they were committed in.
///
/// A persistent append-only log. Cloning is O(1) and appending a batch copies at most
/// `CHUNK_BATCHES` pointers plus one pointer per sealed chunk, and only when the log is shared
/// (copy on write keyed on sharing, never on a published flag), so the writer can derive the
/// next `Version` while readers keep iterating older ones.
#[derive(Clone, Default)]
pub(crate) struct DeltaLog {
    /// Full chunks of `CHUNK_BATCHES` batches each.
    sealed: Arc<Vec<SealedChunk>>,
    /// The chunk being filled; fewer than `CHUNK_BATCHES` batches.
    open: Arc<Vec<Arc<[DeltaRecord]>>>,
    len: usize,
    bytes: u64,
}

impl DeltaLog {
    /// Append `records` as one batch. An empty batch is ignored.
    pub(crate) fn append(&mut self, records: Vec<DeltaRecord>) {
        if records.is_empty() {
            return;
        }
        self.len += records.len();
        self.bytes += batch_bytes(&records);
        self.push_batch(Arc::from(records));
    }

    /// Append one non-empty batch.
    fn push_batch(&mut self, batch: Arc<[DeltaRecord]>) {
        let open = Arc::make_mut(&mut self.open);
        open.push(batch);
        if open.len() == CHUNK_BATCHES {
            let batches: Arc<[Arc<[DeltaRecord]>]> = Arc::from(std::mem::take(open));
            let chunk = SealedChunk {
                bytes: batches.iter().map(|batch| batch_bytes(batch)).sum(),
                first_seq_no: first_seq_no(&batches),
                last_seq_no: last_seq_no(&batches),
                batches,
            };
            Arc::make_mut(&mut self.sealed).push(chunk);
        }
    }

    /// The log without the batches at or below `seq_no`. Batches never straddle a checkpoint,
    /// so a batch is kept or dropped whole.
    pub(crate) fn after(&self, seq_no: SeqNo) -> Self {
        let mut kept = Self::default();
        for batch in self.batches() {
            if batch.last().is_some_and(|record| record.seq_no > seq_no) {
                kept.len += batch.len();
                kept.bytes += batch_bytes(batch);
                kept.push_batch(Arc::clone(batch));
            }
        }
        kept
    }

    /// Approximate bytes of the batches whose last sequence number is in `after + 1..=through`.
    /// O(chunks) plus the batches of at most two partly covered chunks.
    pub(crate) fn bytes_in(&self, after: SeqNo, through: SeqNo) -> u64 {
        let in_range = |batch: &Arc<[DeltaRecord]>| {
            batch
                .last()
                .is_some_and(|record| record.seq_no > after && record.seq_no <= through)
        };
        let mut total = 0;
        for chunk in self.sealed.iter() {
            if chunk.last_seq_no <= after || chunk.first_seq_no > through {
                continue;
            }
            if chunk.first_seq_no > after && chunk.last_seq_no <= through {
                total += chunk.bytes;
            } else {
                total += chunk
                    .batches
                    .iter()
                    .filter(|batch| in_range(batch))
                    .map(|batch| batch_bytes(batch))
                    .sum::<u64>();
            }
        }
        total
            + self
                .open
                .iter()
                .filter(|batch| in_range(batch))
                .map(|batch| batch_bytes(batch))
                .sum::<u64>()
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

    /// Batches oldest first.
    pub(crate) fn batches(&self) -> impl DoubleEndedIterator<Item = &Arc<[DeltaRecord]>> + '_ {
        self.sealed
            .iter()
            .flat_map(|chunk| chunk.batches.iter())
            .chain(self.open.iter())
    }

    /// Records oldest first; reversible for newest-first resolution.
    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = &DeltaRecord> + '_ {
        self.batches().flat_map(|batch| batch.iter())
    }
}

/// A full chunk of a [`DeltaLog`] with its byte total and sequence range, so range sums skip it
/// whole.
#[derive(Clone)]
struct SealedChunk {
    batches: Arc<[Arc<[DeltaRecord]>]>,
    bytes: u64,
    /// Last sequence number of the chunk's first batch.
    first_seq_no: SeqNo,
    /// Last sequence number of the chunk's last batch.
    last_seq_no: SeqNo,
}

fn batch_bytes(batch: &[DeltaRecord]) -> u64 {
    batch
        .iter()
        .map(|record| record.op.approximate_bytes())
        .sum()
}

fn first_seq_no(batches: &[Arc<[DeltaRecord]>]) -> SeqNo {
    batches
        .first()
        .and_then(|batch| batch.last())
        .map_or(0, |record| record.seq_no)
}

fn last_seq_no(batches: &[Arc<[DeltaRecord]>]) -> SeqNo {
    batches
        .last()
        .and_then(|batch| batch.last())
        .map_or(0, |record| record.seq_no)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn records(first: SeqNo, count: usize) -> Vec<DeltaRecord> {
        (first..)
            .take(count)
            .map(|seq_no| DeltaRecord {
                seq_no,
                op: DeltaOp::Delete(WirePk::String(format!("id-{seq_no}"))),
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
            log.append(records(next, size));
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
            .map(|record| record.op.approximate_bytes())
            .sum::<u64>();
        let mut log = DeltaLog::default();
        log.append(batch);
        log.append(Vec::new());
        assert_eq!(log.bytes(), expected);
        assert_eq!(log.len(), 3);
        assert!(DeltaLog::default().is_empty());
        assert_eq!(DeltaLog::default().last_seq_no(), None);
    }

    #[test]
    fn bytes_in_sums_exactly_the_batches_in_range_across_chunks() {
        let mut log = DeltaLog::default();
        let mut next = 1;
        let mut ends = Vec::new();
        for batch in 0..(CHUNK_BATCHES * 3 + 7) {
            let size = batch % 4 + 1;
            log.append(records(next, size));
            next += size as SeqNo;
            ends.push(next - 1);
        }
        let brute = |after: SeqNo, through: SeqNo| {
            log.batches()
                .filter(|batch| {
                    batch
                        .last()
                        .is_some_and(|record| record.seq_no > after && record.seq_no <= through)
                })
                .map(|batch| batch_bytes(batch))
                .sum::<u64>()
        };
        let last = next - 1;
        for (after, through) in [
            (0, last),
            (0, 0),
            (5, 17),
            (ends[CHUNK_BATCHES - 1], ends[CHUNK_BATCHES * 2 - 1]),
            (ends[10], ends[CHUNK_BATCHES * 2 + 3]),
            (last, last + 10),
            (3, 2),
        ] {
            assert_eq!(
                log.bytes_in(after, through),
                brute(after, through),
                "{after}..={through}"
            );
        }
        assert_eq!(log.bytes_in(0, last), log.bytes());
    }

    #[test]
    fn after_keeps_whole_batches_above_the_checkpoint() {
        let mut log = DeltaLog::default();
        let mut next = 1;
        for size in [2, 3, 1, 4] {
            log.append(records(next, size));
            next += size as SeqNo;
        }
        let kept = log.after(5);
        assert_eq!(
            kept.iter().map(|record| record.seq_no).collect::<Vec<_>>(),
            (6..=10).collect::<Vec<_>>()
        );
        assert_eq!(kept.len(), 5);
        let mut expected = DeltaLog::default();
        expected.append(records(6, 1));
        expected.append(records(7, 4));
        assert_eq!(kept.bytes(), expected.bytes());
        assert!(log.after(10).is_empty());
        assert_eq!(log.after(0).len(), 10);
    }
}
