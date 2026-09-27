//! Collection and query-unit statistics and the size estimates behind them.

use crate::{
    LocalStorageEngine,
    manifest::SegmentMeta,
    resolve::{ResolvedState, resolve_latest_state_selected},
    state::{CollectionState, resolve_snapshot},
};
use logpose_index::{FlatIndexEntrySource, HnswIndexSidecar, build_flat_index};
use logpose_types::{
    CollectionStats, QueryUnitArtifactStats, QueryUnitStats, Result, Snapshot, WriteOperation,
};
use logpose_wal::WalRecord;
use std::collections::BTreeMap;

impl LocalStorageEngine {
    pub(crate) fn collection_stats_from_state(
        &self,
        state: CollectionState,
        snapshot: Option<Snapshot>,
    ) -> Result<CollectionStats> {
        let effective_snapshot = resolve_snapshot(&state, snapshot)?;
        let resolved = resolve_latest_state_selected(
            self.vfs.as_ref(),
            &state,
            effective_snapshot.visible_seq_no,
            true,
            None,
        )?;
        let mut live_record_count = 0usize;
        let mut deleted_record_count = 0usize;
        for value in resolved.values() {
            match value {
                ResolvedState::Visible(_) => live_record_count += 1,
                ResolvedState::Deleted { .. } => deleted_record_count += 1,
            }
        }
        let maintenance = self.load_maintenance_status(&state.descriptor)?;
        let delta_records = state
            .delta
            .iter()
            .filter(|record| record.seq_no <= effective_snapshot.visible_seq_no)
            .cloned()
            .collect::<Vec<_>>();
        let mut query_units = vec![mutable_query_unit(&delta_records)];
        query_units.extend(state.manifest.segments.iter().map(QueryUnitStats::from));

        Ok(CollectionStats {
            collection_id: state.descriptor.collection_id.clone(),
            database_name: state.descriptor.database_name.clone(),
            collection_name: state.descriptor.name.clone(),
            manifest_generation: effective_snapshot.manifest_generation,
            visible_seq_no: effective_snapshot.visible_seq_no,
            mutable_op_count: delta_records.len(),
            segment_count: state.manifest.segments.len(),
            live_record_count,
            deleted_record_count,
            maintenance,
            query_units,
        })
    }
}

impl From<&SegmentMeta> for QueryUnitStats {
    fn from(segment: &SegmentMeta) -> Self {
        Self {
            unit_id: segment.segment_id.clone(),
            tier: "immutable".to_owned(),
            index_kind: segment.index_kind.clone(),
            min_seq_no: segment.min_seq_no,
            max_seq_no: segment.max_seq_no,
            put_count: segment.put_count,
            delete_count: segment.delete_count,
            approx_bytes: segment.approx_bytes,
            scalar_fields: segment.scalar_fields.clone(),
            artifact_stats: segment.artifacts.clone(),
            component_bytes: segment.component_bytes.clone(),
        }
    }
}

fn mutable_query_unit(delta: &[WalRecord]) -> QueryUnitStats {
    let sidecar = build_flat_index(
        "mutable-delta",
        &delta
            .iter()
            .map(|record| match &record.op {
                WriteOperation::Put(put) => FlatIndexEntrySource {
                    is_put: true,
                    record_id_offset: 0,
                    vector_offset: 0,
                    metadata_offset: 0,
                    vector: Some(put.vector.clone()),
                    metadata: Some(put.metadata.clone()),
                },
                WriteOperation::Delete(_) => FlatIndexEntrySource {
                    is_put: false,
                    record_id_offset: 0,
                    vector_offset: 0,
                    metadata_offset: 0,
                    vector: None,
                    metadata: None,
                },
            })
            .collect::<Vec<_>>(),
    );

    QueryUnitStats {
        unit_id: "mutable-delta".to_owned(),
        tier: "mutable".to_owned(),
        index_kind: "raw".to_owned(),
        min_seq_no: delta.first().map(|record| record.seq_no).unwrap_or(0),
        max_seq_no: delta.last().map(|record| record.seq_no).unwrap_or(0),
        put_count: sidecar.put_count,
        delete_count: sidecar.delete_count,
        approx_bytes: delta
            .iter()
            .map(|record| approximate_record_bytes(&record.op))
            .sum(),
        scalar_fields: sidecar.scalar_fields,
        artifact_stats: vec![QueryUnitArtifactStats {
            kind: "mutable_delta".to_owned(),
            file_name: String::new(),
            approx_bytes: delta
                .iter()
                .map(|record| approximate_record_bytes(&record.op))
                .sum(),
        }],
        component_bytes: BTreeMap::from([(
            "mutable_delta".to_owned(),
            delta
                .iter()
                .map(|record| approximate_record_bytes(&record.op))
                .sum(),
        )]),
    }
}

pub(crate) fn segment_component_bytes(
    hnsw_index: &HnswIndexSidecar,
    raw_segment_bytes: usize,
    flat_bytes: usize,
) -> BTreeMap<String, usize> {
    let ann_vectors = hnsw_index
        .nodes
        .iter()
        .map(|node| node.record.vector.len() * std::mem::size_of::<f32>())
        .sum::<usize>();
    let ann_metadata = hnsw_index
        .nodes
        .iter()
        .map(|node| node.record.metadata.to_string().len())
        .sum::<usize>();
    let ann_graph = hnsw_index
        .nodes
        .iter()
        .map(|node| {
            std::mem::size_of::<u32>()
                + std::mem::size_of::<u8>()
                + node.neighbors_by_level.len() * std::mem::size_of::<u32>()
                + node
                    .neighbors_by_level
                    .iter()
                    .map(|neighbors| neighbors.len() * std::mem::size_of::<u32>())
                    .sum::<usize>()
        })
        .sum::<usize>();

    BTreeMap::from([
        ("raw_segment".to_owned(), raw_segment_bytes),
        ("exact_flat".to_owned(), flat_bytes),
        ("ann_graph".to_owned(), ann_graph),
        ("ann_vectors".to_owned(), ann_vectors),
        ("ann_metadata".to_owned(), ann_metadata),
    ])
}

pub(crate) fn approximate_record_bytes(operation: &WriteOperation) -> usize {
    match operation {
        WriteOperation::Put(put) => {
            put.id.as_str().len()
                + put.vector.len() * std::mem::size_of::<f32>()
                + serde_json::to_vec(&put.metadata)
                    .map(|value| value.len())
                    .unwrap_or(0)
                + 32
        }
        WriteOperation::Delete(delete) => delete.id.as_str().len() + 16,
    }
}
