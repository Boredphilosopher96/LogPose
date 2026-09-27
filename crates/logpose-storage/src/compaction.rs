//! Compaction: rewrite every immutable segment into one replacement segment.

use crate::{
    LocalStorageEngine,
    manifest::Manifest,
    resolve::{ResolvedState, resolve_latest_from_segments},
    state::CollectionState,
};
use logpose_types::{PutRecord, Result, Snapshot, WriteOperation};
use logpose_wal::WalRecord;

impl LocalStorageEngine {
    pub(crate) fn compact_state(&self, state: CollectionState) -> Result<Snapshot> {
        if state.manifest.segments.len() <= 1 {
            return Ok(Snapshot {
                manifest_generation: state.manifest.generation,
                visible_seq_no: state.visible_seq_no(),
            });
        }

        let resolved = resolve_latest_from_segments(&state.descriptor, &state.manifest)?;
        let mut compacted_records = resolved
            .into_values()
            .map(|state| match state {
                ResolvedState::Visible(record) => WalRecord {
                    seq_no: record.seq_no,
                    op: WriteOperation::Put(PutRecord {
                        id: record.id,
                        vector: record.vector,
                        metadata: record.metadata,
                    }),
                },
                ResolvedState::Deleted { id, seq_no } => WalRecord {
                    seq_no,
                    op: WriteOperation::Delete(logpose_types::DeleteRecord { id }),
                },
            })
            .collect::<Vec<_>>();
        compacted_records.sort_by_key(|record| record.seq_no);

        let replacement = self.write_segment_file(&state.descriptor, &compacted_records)?;
        let next_manifest = Manifest {
            generation: state.manifest.generation + 1,
            checkpoint_seq_no: state.manifest.checkpoint_seq_no,
            segments: vec![replacement],
        };
        self.publish_manifest(&state.descriptor, &next_manifest)?;

        Ok(Snapshot {
            manifest_generation: next_manifest.generation,
            visible_seq_no: state.visible_seq_no(),
        })
    }
}
