//! Open and recover: build a collection's first `Version` from its manifest, its segments and
//! DV files, and the WAL above the manifest's checkpoint, and start its writer task.
//!
//! `open_collection` runs, in order:
//!
//! 1. The **durability barrier**: sync the collection directory and its data directories, so
//!    recovery reasons only about what is on disk (the WAL layer syncs `wal/` and its files
//!    inside `WalRecovery::open`, after its fence check).
//! 2. Read `CURRENT` and load the manifest it names; a version 1 layout fails here.
//! 3. **Orphan cleanup** relative to that manifest, before any id can be issued again.
//! 4. Open every segment of the manifest (header, footer, section table, and schema snapshot,
//!    checked against the manifest entry), and load and verify each DV file it names.
//! 5. **Rebuild the primary-key index** from each segment's key and row-meta sections minus its
//!    deletion vector. A key with two live rows (an I5 violation the engine prevents) keeps the
//!    row with the higher sequence number and deletes the other in memory; in strict mode it
//!    fails the open.
//! 6. WAL recovery: fence check, WAL barrier, tail repair, and replay of every frame above the
//!    checkpoint through the same `apply` the writer uses, into a fresh active memtable,
//!    starting from the manifest's schema and following the `SchemaChange` frames in the log,
//!    so each batch is read with the schema that was current at its sequence number. WAL files
//!    at or below the checkpoint are then deleted.
//! 7. `Version` 1 and the writer task, seeded with the id counters.
//!
//! Every file change before the writer starts (orphan removal, tail truncation, WAL cleanup)
//! removes or adds only bytes no durable state references, and each runs after the barrier, so
//! recovery that crashes at any point and runs again converges to the same state (I11).

use crate::{
    dv::{DeletionMap, DeletionVector, dv_path, load_dv_file},
    engine::{CoreRef, EngineCore},
    fs_util::read_json,
    gc::{durability_barrier, remove_orphans},
    handle::{CollectionHandle, CollectionMeta},
    manifest::{Manifest, load_manifest, manifest_corrupt, read_current},
    memtable::MemtableData,
    segment::SegmentHandle,
    version::{VersionCounters, VersionId},
    writer::{self, LogicalState, PkIndex, WriterSeed, checkpoint_frame, replay_frame},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{
    CollectionRef, CorruptionKind, LogPoseError, Result, RowAddr, SeqNo, UnitId,
    record::PrimaryKey, schema::CollectionSchema,
};
use logpose_wal::{WalRecovery, WalWriter};
use std::{collections::HashMap, path::Path, sync::Arc};

/// The outcome of recovering one collection directory at engine open.
pub(crate) enum RecoveredCollection {
    /// Recovered and ready to serve.
    Open(Arc<CollectionHandle>),
    /// The descriptor names the collection, but it cannot be served.
    Failed {
        reference: CollectionRef,
        /// The descriptor, when it is valid.
        descriptor: Option<Box<CollectionDescriptor>>,
        error: LogPoseError,
    },
    /// The descriptor cannot be parsed, so the collection is not even known by name.
    Unreadable { error: String },
}

/// What a collection's writer starts from besides its WAL and state.
pub(crate) struct DurableStart {
    pub(crate) manifest: Arc<Manifest>,
    /// The manifest generation kept beside the durable one, if any.
    pub(crate) previous_generation: Option<u64>,
    /// The first manifest generation the writer may issue.
    pub(crate) next_manifest_gen: u64,
    /// The first unit id the writer may issue.
    pub(crate) next_unit_id: u32,
    /// The first DV file generation the writer may issue.
    pub(crate) next_dv_gen: u64,
}

impl CoreRef {
    /// Recover the collection in `dir`: read its descriptor and placement,
    /// then recover its files (see the module docs), build `Version` 1, and start the writer.
    pub(crate) fn recover_collection(&self, dir: &Path) -> RecoveredCollection {
        let descriptor_path = dir.join("descriptor.json");
        let mut descriptor =
            match read_json::<CollectionDescriptor>(self.vfs.as_ref(), &descriptor_path) {
                Ok(descriptor) => descriptor,
                Err(error) => {
                    return RecoveredCollection::Unreadable {
                        error: error.to_string(),
                    };
                }
            };
        // The collection lives where its descriptor was found. The stored path is the one it
        // was created at, which differs once a storage root is moved or a collection's files
        // are copied to another node.
        descriptor.root_path = dir.to_path_buf();
        let reference = descriptor.collection_ref();
        if let Err(error) = descriptor.validate() {
            return RecoveredCollection::Failed {
                reference,
                descriptor: None,
                error: LogPoseError::corrupt(
                    CorruptionKind::Descriptor,
                    format!(
                        "collection descriptor in '{}' is invalid: {error}",
                        dir.display()
                    ),
                ),
            };
        }
        match self.open_collection(descriptor.clone()) {
            Ok(handle) => RecoveredCollection::Open(handle),
            Err(error) => RecoveredCollection::Failed {
                reference,
                descriptor: Some(Box::new(descriptor)),
                error,
            },
        }
    }

    fn open_collection(&self, descriptor: CollectionDescriptor) -> Result<Arc<CollectionHandle>> {
        let assignment = self.load_collection_assignment(&descriptor)?;
        let mut durable = self.recover_manifest(&descriptor)?;
        let state = self.recover_segments(&descriptor, &mut durable)?;
        let (state, mut wal) = self.recover_wal(&descriptor, &durable.manifest, state)?;
        // Every operation in these files is in a segment of the durable manifest.
        wal.remove_checkpointed(durable.manifest.checkpoint_seq_no)?;
        let meta = Arc::new(CollectionMeta::new(descriptor, assignment));
        // Maintenance starts on the first data-plane access, not here: a node that only
        // reports status for a collection it does not serve must never run its jobs. The first
        // tick after that plans whatever the recovered state is due (a replayed memtable over
        // a flush trigger, segments the policy would merge).
        self.start_collection(meta, durable, state, wal, false)
    }

    /// The durability barrier, the manifest `CURRENT` names, and orphan cleanup relative to it.
    fn recover_manifest(&self, descriptor: &CollectionDescriptor) -> Result<DurableStart> {
        let dir = &descriptor.root_path;
        let vfs = self.vfs.as_ref();
        durability_barrier(vfs, dir)?;
        let generation = read_current(vfs, dir)?;
        let manifest = load_manifest(vfs, dir, generation)?;
        if manifest.collection_id != descriptor.collection_id {
            return Err(manifest_corrupt(
                &dir.join("CURRENT"),
                format!(
                    "manifest {generation} belongs to collection {}, not {}",
                    manifest.collection_id, descriptor.collection_id
                ),
            ));
        }
        let cleanup = remove_orphans(vfs, dir, &manifest)?;
        if !cleanup.removed.is_empty() {
            tracing::info!(
                collection = %descriptor.lookup_name(),
                removed = cleanup.removed.len(),
                "recovery removed files no durable manifest references"
            );
        }
        Ok(DurableStart {
            manifest: Arc::new(manifest),
            previous_generation: cleanup.previous_generation,
            next_manifest_gen: cleanup.next_manifest_gen,
            next_unit_id: cleanup.next_unit_id,
            next_dv_gen: cleanup.next_dv_gen,
        })
    }

    /// Open every segment of the durable manifest, load its DV file, rebuild the primary-key
    /// index, and start an empty active memtable (taking a unit id from `durable`) for WAL
    /// replay.
    fn recover_segments(
        &self,
        descriptor: &CollectionDescriptor,
        durable: &mut DurableStart,
    ) -> Result<LogicalState> {
        let dir = &descriptor.root_path;
        let vfs = self.vfs.as_ref();
        let manifest = &durable.manifest;
        let mut segments = Vec::with_capacity(manifest.segments.len());
        let mut deletes = DeletionMap::default();
        for entry in &manifest.segments {
            let segment = SegmentHandle::open(
                vfs,
                dir,
                &manifest.collection_id,
                entry.clone(),
                self.buffer_cache(),
                self.gc.clone(),
            )?;
            if let Some(dv) = entry.dv {
                let bitmap = load_dv_file(
                    vfs,
                    &dv_path(dir, entry.unit, dv.generation),
                    entry.unit,
                    entry.row_count,
                    dv.generation,
                    dv.cardinality,
                )?;
                deletes.set(entry.unit, DeletionVector::from_bitmap(bitmap));
            }
            segments.push(Arc::new(segment));
        }
        let pk = rebuild_pk_index(
            &segments,
            &mut deletes,
            self.strict_invariants,
            &descriptor.lookup_name(),
        )?;
        let unit = UnitId(durable.next_unit_id);
        durable.next_unit_id = durable
            .next_unit_id
            .checked_add(1)
            .ok_or_else(|| LogPoseError::internal("the collection has used every unit id"))?;
        let segments: Arc<[Arc<SegmentHandle>]> = Arc::from(segments);
        Ok(new_state(
            Arc::new(manifest.schema.clone()),
            unit,
            manifest.checkpoint_seq_no + 1,
            self.tokens.clock.now(),
            segments,
            deletes,
            pk,
            self.strict_invariants,
        ))
    }

    /// Publish `Version` 1 over the recovered state and start the writer task.
    pub(crate) fn start_collection(
        &self,
        meta: Arc<CollectionMeta>,
        durable: DurableStart,
        state: LogicalState,
        wal: WalWriter,
        armed: bool,
    ) -> Result<Arc<CollectionHandle>> {
        let DurableStart {
            manifest,
            previous_generation,
            next_manifest_gen,
            next_unit_id,
            next_dv_gen,
        } = durable;
        let version = state.version(VersionId(1), Arc::clone(&meta), Arc::clone(&manifest));
        if self.strict_invariants {
            version.check_invariants()?;
        }
        let next_seq_no = wal.next_seq_no();
        if version.visible_seq_no.checked_add(1) != Some(next_seq_no) {
            return Err(LogPoseError::Corrupt {
                kind: CorruptionKind::Wal,
                location: Some(wal.active_path().display().to_string()),
                message: format!(
                    "the WAL continues at sequence number {next_seq_no}, but the recovered state \
                     ends at {}",
                    version.visible_seq_no
                ),
            });
        }
        let (channels, inbox) = writer::channels(&self.group_commit);
        let handle = Arc::new(CollectionHandle::new(
            version,
            channels,
            armed,
            Arc::clone(&self.tokens),
        ));
        writer::spawn(
            self.clone(),
            Arc::clone(&handle),
            inbox,
            WriterSeed {
                wal,
                state,
                manifest,
                next_seq_no,
                version_id: VersionId(1),
                previous_generation,
                next_manifest_gen,
                next_unit_id,
                next_dv_gen,
            },
        );
        Ok(handle)
    }
}

impl EngineCore {
    /// Recover the WAL of a collection above `manifest`'s checkpoint into `state` and return
    /// the replayed state and the writer that continues the log.
    ///
    /// The WAL layer runs the fence check, the durability barrier for `wal/`, and tail repair,
    /// then streams the frames above the checkpoint; each one is applied with the writer's
    /// `apply`. Checkpoint frames are cross-checked against the manifest.
    pub(crate) fn recover_wal(
        &self,
        descriptor: &CollectionDescriptor,
        manifest: &Manifest,
        mut state: LogicalState,
    ) -> Result<(LogicalState, WalWriter)> {
        let checkpoint = manifest.checkpoint_seq_no;
        let mut recovery = WalRecovery::open(
            Arc::clone(&self.vfs),
            Self::wal_dir(descriptor),
            self.wal_config(),
            checkpoint,
        )?;
        while let Some(frame) = recovery.next_frame()? {
            replay_frame(&mut state, checkpoint, frame)?;
        }
        if let Some(repair) = &recovery.report().tail_repair {
            tracing::warn!(
                collection = %descriptor.lookup_name(),
                file = %repair.file.display(),
                discarded_bytes = repair.original_len - repair.repaired_len,
                discarded_seq = ?repair.discarded_seq,
                "recovery truncated an unacknowledged WAL tail"
            );
        }
        let wal = recovery.into_writer(&checkpoint_frame(manifest)?)?;
        Ok((state, wal))
    }
}

/// A writer state with an empty active memtable `unit` whose operations start at
/// `first_seq_no`, over `segments` with their deletion vectors and the key index built from
/// them.
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_state(
    schema: Arc<CollectionSchema>,
    unit: UnitId,
    first_seq_no: SeqNo,
    now: std::time::Duration,
    segments: Arc<[Arc<SegmentHandle>]>,
    deletes: DeletionMap,
    pk: PkIndex,
    strict: bool,
) -> LogicalState {
    let counters = VersionCounters {
        total_rows: segments
            .iter()
            .map(|segment| u64::from(segment.row_count()))
            .sum(),
        deleted_rows: deletes.total(),
        segment_count: u32::try_from(segments.len()).unwrap_or(u32::MAX),
        memtable_rows: 0,
        memtable_bytes: 0,
    };
    LogicalState {
        active: MemtableData::new(unit, Arc::clone(&schema), first_seq_no, now),
        schema,
        frozen: Vec::new(),
        segments,
        deletes,
        counters,
        pk,
        strict,
    }
}

/// Rebuild the primary-key index from the segments' keys minus their deletion vectors, in
/// ascending unit order. A key with two live rows keeps the one with the higher sequence number
/// and deletes the other in `deletes`; in strict mode it fails instead.
fn rebuild_pk_index(
    segments: &[Arc<SegmentHandle>],
    deletes: &mut DeletionMap,
    strict: bool,
    collection: &str,
) -> Result<PkIndex> {
    let live_rows = segments
        .iter()
        .map(|segment| u64::from(segment.row_count()).saturating_sub(deletes.len_of(segment.unit)))
        .sum::<u64>();
    let mut rows: HashMap<PrimaryKey, (RowAddr, SeqNo)> =
        HashMap::with_capacity(usize::try_from(live_rows).unwrap_or(0));
    let mut repaired = 0_u64;
    for segment in segments {
        let (pks, seqs) = segment.keys()?;
        for row in 0..segment.row_count() {
            let addr = RowAddr {
                unit: segment.unit,
                row,
            };
            if deletes.is_deleted(addr) {
                continue;
            }
            let (Some(pk), Some(&seq_no)) = (pks.get(row as usize), seqs.get(row as usize)) else {
                return Err(LogPoseError::corrupt(
                    CorruptionKind::Segment,
                    format!("segment {} is missing the key of row {row}", segment.unit),
                ));
            };
            let Some((other, other_seq)) = rows.insert(pk.clone(), (addr, seq_no)) else {
                continue;
            };
            if strict {
                return Err(LogPoseError::internal(format!(
                    "collection '{collection}': key {pk} has live rows at {other} and at {addr} \
                     (I5)"
                )));
            }
            repaired += 1;
            tracing::error!(
                collection,
                %pk,
                %other,
                %addr,
                "repairing a key with two live rows: the older one is deleted in memory"
            );
            if other_seq > seq_no {
                rows.insert(pk, (other, other_seq));
                deletes.mark(addr);
            } else {
                deletes.mark(other);
            }
        }
    }
    if repaired > 0 {
        tracing::error!(collection, repaired, "pk_duplicates_repaired");
    }
    let mut pk = PkIndex::with_capacity(rows.len());
    for (key, (addr, _)) in rows {
        pk.insert(key, addr);
    }
    Ok(pk)
}

#[cfg(test)]
mod tests;
