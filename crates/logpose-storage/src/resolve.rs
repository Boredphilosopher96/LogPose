//! Latest-visible resolution over the mutable delta and immutable segments, and the exact scan built on it.

use crate::{
    LocalStorageEngine,
    manifest::Manifest,
    segment_v1::read_segment_file,
    state::{CollectionState, resolve_snapshot},
};
use logpose_catalog::CollectionDescriptor;
use logpose_types::{RecordId, Result, SeqNo, Snapshot, VisibleRecord, WriteOperation};
use logpose_vfs::Vfs;
use logpose_wal::WalRecord;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub(crate) enum ResolvedState {
    Visible(VisibleRecord),
    Deleted { id: RecordId, seq_no: SeqNo },
}

pub(crate) fn resolve_latest_state_selected(
    vfs: &dyn Vfs,
    state: &CollectionState,
    visible_seq_no: SeqNo,
    include_mutable: bool,
    immutable_unit_ids: Option<std::collections::BTreeSet<String>>,
) -> Result<BTreeMap<RecordId, ResolvedState>> {
    let mut resolved = BTreeMap::new();

    if include_mutable {
        for record in state
            .delta
            .iter()
            .rev()
            .filter(|record| record.seq_no <= visible_seq_no)
        {
            apply_resolved_record(&mut resolved, record.clone());
        }
    }

    for segment in state.manifest.segments.iter().rev().filter(|segment| {
        immutable_unit_ids
            .as_ref()
            .is_none_or(|selected| selected.contains(&segment.segment_id))
    }) {
        let records = read_segment_file(
            vfs,
            &state
                .descriptor
                .root_path
                .join("segments")
                .join(&segment.file_name),
        )?;
        for record in records
            .into_iter()
            .rev()
            .filter(|record| record.seq_no <= visible_seq_no)
        {
            apply_resolved_record(&mut resolved, record);
        }
    }

    Ok(resolved)
}

pub(crate) fn resolve_latest_from_segments(
    vfs: &dyn Vfs,
    descriptor: &CollectionDescriptor,
    manifest: &Manifest,
) -> Result<BTreeMap<RecordId, ResolvedState>> {
    let mut resolved = BTreeMap::new();
    for segment in manifest.segments.iter().rev() {
        let records = read_segment_file(
            vfs,
            &descriptor
                .root_path
                .join("segments")
                .join(&segment.file_name),
        )?;
        for record in records.into_iter().rev() {
            apply_resolved_record(&mut resolved, record);
        }
    }
    Ok(resolved)
}

pub(crate) fn resolve_latest_state_for_ids_selected(
    vfs: &dyn Vfs,
    state: &CollectionState,
    visible_seq_no: SeqNo,
    wanted_ids: &BTreeSet<RecordId>,
    include_mutable: bool,
    immutable_unit_ids: Option<BTreeSet<String>>,
) -> Result<BTreeMap<RecordId, ResolvedState>> {
    let mut resolved = BTreeMap::new();

    if include_mutable {
        for record in state
            .delta
            .iter()
            .rev()
            .filter(|record| record.seq_no <= visible_seq_no)
        {
            if wanted_ids.contains(record.op.id()) {
                apply_resolved_record(&mut resolved, record.clone());
            }
            if resolved.len() == wanted_ids.len() {
                return Ok(resolved);
            }
        }
    }

    for segment in state.manifest.segments.iter().rev().filter(|segment| {
        immutable_unit_ids
            .as_ref()
            .is_none_or(|selected| selected.contains(&segment.segment_id))
    }) {
        let records = read_segment_file(
            vfs,
            &state
                .descriptor
                .root_path
                .join("segments")
                .join(&segment.file_name),
        )?;
        for record in records
            .into_iter()
            .rev()
            .filter(|record| record.seq_no <= visible_seq_no && wanted_ids.contains(record.op.id()))
        {
            apply_resolved_record(&mut resolved, record);
        }
        if resolved.len() == wanted_ids.len() {
            return Ok(resolved);
        }
    }

    Ok(resolved)
}

fn apply_resolved_record(resolved: &mut BTreeMap<RecordId, ResolvedState>, record: WalRecord) {
    let id = record.op.id().clone();
    if resolved.contains_key(&id) {
        return;
    }

    match record.op {
        WriteOperation::Put(put) => {
            resolved.insert(
                id,
                ResolvedState::Visible(VisibleRecord {
                    id: put.id,
                    vector: put.vector,
                    metadata: put.metadata,
                    seq_no: record.seq_no,
                }),
            );
        }
        WriteOperation::Delete(delete) => {
            resolved.insert(
                id,
                ResolvedState::Deleted {
                    id: delete.id,
                    seq_no: record.seq_no,
                },
            );
        }
    }
}

impl LocalStorageEngine {
    pub(crate) fn scan_exact_internal(
        &self,
        collection_name: &str,
        snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Option<std::collections::BTreeSet<String>>,
    ) -> Result<Vec<VisibleRecord>> {
        let state = self.load_collection_state(
            collection_name,
            snapshot.as_ref().map(|value| value.manifest_generation),
        )?;
        let snapshot = resolve_snapshot(&state, snapshot)?;

        let resolved = resolve_latest_state_selected(
            self.vfs.as_ref(),
            &state,
            snapshot.visible_seq_no,
            include_mutable,
            immutable_unit_ids,
        )?;
        let mut records = resolved
            .into_values()
            .filter_map(|state| match state {
                ResolvedState::Visible(record) => Some(record),
                ResolvedState::Deleted { .. } => None,
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(records)
    }
}
