//! Collection statistics and `inspect` reports over one `Version`.
//!
//! Statistics are O(units): row counts come from the `Version`'s counters and deletion
//! vectors, and each segment's field summaries from its manifest entry's zone maps, so no row
//! is read. The memtables are reported as one mutable unit, `mutable-delta`. A segment's
//! `index_kind` names the vector index its search actually uses: `hnsw` with a graph, `sq8`
//! with codes only, `flat` otherwise.

use crate::{
    handle::CollectionHandle, segment::SegmentHandle, state::ReadAt, tokens::SnapshotToken,
    version::Version,
};
use logpose_types::{
    CollectionStats, LogPoseError, MaintenanceStatus, QueryUnitArtifactStats, QueryUnitStats,
    ResourceKind, Result, RowAddr, ScalarFieldStats, ScalarMetadataValue, SeqNo, Snapshot,
    record::PrimaryKey,
    schema::{FieldId, FieldRef},
};
use logpose_wal::codec::{RowImage, ValueBytes};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::{collections::BTreeMap, sync::Arc};

/// The query-unit id of the memtables, reported as one mutable unit.
const MUTABLE_UNIT_ID: &str = "mutable-delta";

/// What [`CollectionHandle::inspect`] reports on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InspectTarget {
    /// The manifest of the current state.
    Manifest,
    /// The rows written since the checkpoint: the memtables, which the WAL above the
    /// checkpoint rebuilds on recovery.
    Wal,
    /// The collection's maintenance status.
    Maintenance,
    /// One segment by its unit id: its manifest entry, section table, and rows.
    Segment(String),
}

/// A JSON inspection report for operators and the CLI.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InspectReport {
    /// The inspected target: `manifest`, `wal`, `maintenance`, or `segment:<unit>`.
    pub target: String,
    /// JSON payload describing the target.
    pub payload: JsonValue,
}

impl CollectionHandle {
    /// Statistics of the current state, or of the exact `snapshot` (see
    /// [`ReadOptions::snapshot`](crate::ReadOptions::snapshot)). O(units): no row is read.
    ///
    /// # Errors
    ///
    /// The collection is dropped, or `snapshot` is invalid or no longer retained
    /// (`SnapshotExpired`).
    pub fn stats(&self, snapshot: Option<Snapshot>) -> Result<CollectionStats> {
        self.stats_at(snapshot)
    }

    /// Statistics of the state `token` pins, extending the token's expiry.
    ///
    /// # Errors
    ///
    /// The collection is dropped, or the token expired, was released, or is unknown
    /// (`SnapshotExpired`).
    pub fn stats_at_token(&self, token: &SnapshotToken) -> Result<CollectionStats> {
        self.stats_at(token.clone())
    }

    fn stats_at(&self, at: impl Into<ReadAt>) -> Result<CollectionStats> {
        self.arm_maintenance();
        let (version, snapshot) = self.read_state(at)?;
        let descriptor = self.descriptor();
        let mut query_units = vec![mutable_unit_stats(&version)];
        for segment in version.segments.iter() {
            query_units.push(segment_unit_stats(&version, segment));
        }
        Ok(CollectionStats {
            collection_id: descriptor.collection_id.clone(),
            database_name: descriptor.database_name.clone(),
            collection_name: descriptor.name.clone(),
            manifest_generation: snapshot.manifest_generation,
            visible_seq_no: snapshot.visible_seq_no,
            mutable_op_count: usize::try_from(version.visible_seq_no - version.checkpoint_seq_no)
                .unwrap_or(usize::MAX),
            segment_count: version.segments.len(),
            live_record_count: usize::try_from(version.counters.live_rows()).unwrap_or(usize::MAX),
            deleted_record_count: usize::try_from(version.counters.deleted_rows)
                .unwrap_or(usize::MAX),
            maintenance: self.maintenance_status(),
            query_units,
        })
    }

    /// The `inspect` report of `target` over the current state. Runs on the engine's I/O pool:
    /// a segment report reads the segment's rows.
    ///
    /// # Errors
    ///
    /// The collection is dropped, the engine is shut down, an unknown segment (`NotFound`),
    /// or I/O and typed corruption reading a segment.
    pub async fn inspect(&self, target: InspectTarget) -> Result<InspectReport> {
        self.ensure_open()?;
        self.arm_maintenance();
        let version = self.current();
        let status = self.maintenance_status();
        self.run_io(move || inspect_version(&version, status, target))
            .await
    }
}

/// The `inspect` report of `target` over `version`.
fn inspect_version(
    version: &Version,
    status: MaintenanceStatus,
    target: InspectTarget,
) -> Result<InspectReport> {
    match target {
        InspectTarget::Manifest => Ok(InspectReport {
            target: "manifest".to_owned(),
            payload: version.manifest.inspect_json(),
        }),
        InspectTarget::Wal => {
            // The memtables hold every row written since the checkpoint; deletes live only in
            // deletion vectors (and the WAL files).
            let mut records = Vec::new();
            for memtable in version.memtables() {
                for slot in 0..memtable.slot_count() {
                    let image = memtable.row_image(slot).map_err(LogPoseError::internal)?;
                    records.push(json!({
                        "seq_no": memtable.seq_no(slot).unwrap_or_default(),
                        "op": "put",
                        "pk": PrimaryKey::from(image.pk.clone()).to_json(),
                        "record": record_json(version, &image)?,
                        "unit": memtable.unit.to_string(),
                        "deleted": version.is_deleted(RowAddr {
                            unit: memtable.unit,
                            row: slot,
                        }),
                    }));
                }
            }
            records.sort_by_key(|record| record["seq_no"].as_u64().unwrap_or_default());
            Ok(InspectReport {
                target: "wal".to_owned(),
                payload: json!({
                    "checkpoint_seq_no": version.checkpoint_seq_no,
                    "visible_seq_no": version.visible_seq_no,
                    "schema_version": version.schema.schema_version(),
                    "memtables": version.memtables().map(|memtable| json!({
                        "unit": memtable.unit.to_string(),
                        "first_seq_no": memtable.first_seq_no,
                        "last_seq_no": memtable.last_seq_no,
                        "slots": memtable.slot_count(),
                        "bytes": memtable.bytes().total(),
                    })).collect::<Vec<_>>(),
                    "records": records,
                }),
            })
        }
        InspectTarget::Maintenance => Ok(InspectReport {
            target: "maintenance".to_owned(),
            payload: serde_json::to_value(status).map_err(crate::error::json_message)?,
        }),
        InspectTarget::Segment(segment_id) => {
            let segment = version
                .segments
                .iter()
                .find(|segment| segment.unit.to_string() == segment_id)
                .ok_or_else(|| {
                    LogPoseError::not_found(ResourceKind::Segment, segment_id.clone())
                })?;
            inspect_segment(version, segment)
        }
    }
}

/// `image` as a reader of `version` sees it, as the record's JSON document.
fn record_json(version: &Version, image: &RowImage) -> Result<JsonValue> {
    let record = image.to_record(&version.schema).map_err(|error| {
        LogPoseError::internal(format!(
            "row {} cannot be read with schema version {}: {error}",
            PrimaryKey::from(image.pk.clone()).label(),
            version.schema.schema_version()
        ))
    })?;
    Ok(record.to_json(&version.schema))
}

/// The `inspect segment` report: the manifest entry, the section table, and every row.
fn inspect_segment(version: &Version, segment: &Arc<SegmentHandle>) -> Result<InspectReport> {
    let reader = segment.reader();
    let deleted = version.deletes.len_of(segment.unit);
    let sections = reader
        .sections()
        .iter()
        .map(|section| {
            json!({
                "kind": section
                    .section_kind()
                    .map_or_else(|| "unknown".to_owned(), |kind| format!("{kind:?}")),
                "field_id": section.field.map(|field| field.0),
                "encoding": section.encoding,
                "offset": section.offset,
                "length": section.length,
            })
        })
        .collect::<Vec<_>>();
    let mut records = Vec::new();
    for (row, stored) in (0_u32..).zip(segment.read_rows()?) {
        records.push(json!({
            "row": row,
            "seq_no": stored.seq_no,
            "op": "put",
            "pk": PrimaryKey::from(stored.image.pk.clone()).to_json(),
            "deleted": version.is_deleted(RowAddr {
                unit: segment.unit,
                row,
            }),
        }));
    }
    let header = reader.header();
    Ok(InspectReport {
        target: format!("segment:{}", segment.unit),
        payload: json!({
            "segment": {
                "segment_id": segment.unit.to_string(),
                "unit": segment.unit.to_string(),
                "file_name": segment
                    .path()
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned()),
                "format_version": crate::segment_v2::FORMAT_VERSION,
                "file_len": reader.file_len(),
                "row_count": header.row_count,
                "deleted_rows": deleted,
                "live_rows": u64::from(header.row_count).saturating_sub(deleted),
                "schema_version": header.schema_version,
                "min_seq_no": header.min_seq_no,
                "max_seq_no": header.max_seq_no,
                "tier": segment.entry.tier,
                "index_kind": index_kind(segment),
            },
            "sections": sections,
            "records": records,
        }),
    })
}

/// The one mutable query unit: every memtable of `version`.
fn mutable_unit_stats(version: &Version) -> QueryUnitStats {
    let mut live = 0_usize;
    let mut dead = 0_usize;
    let mut min_seq_no = SeqNo::MAX;
    let mut max_seq_no = 0;
    for memtable in version.memtables() {
        for slot in 0..memtable.slot_count() {
            if version.is_deleted(RowAddr {
                unit: memtable.unit,
                row: slot,
            }) {
                dead += 1;
            } else {
                live += 1;
                let seq_no = memtable.seq_no(slot).unwrap_or_default();
                min_seq_no = min_seq_no.min(seq_no);
                max_seq_no = max_seq_no.max(seq_no);
            }
        }
    }
    let bytes = usize::try_from(
        version
            .memtables()
            .map(|memtable| memtable.bytes().total())
            .sum::<u64>(),
    )
    .unwrap_or(usize::MAX);
    QueryUnitStats {
        unit_id: MUTABLE_UNIT_ID.to_owned(),
        tier: "mutable".to_owned(),
        index_kind: "raw".to_owned(),
        min_seq_no: if live == 0 { 0 } else { min_seq_no },
        max_seq_no,
        put_count: live,
        delete_count: dead,
        approx_bytes: bytes,
        scalar_fields: BTreeMap::new(),
        artifact_stats: vec![QueryUnitArtifactStats {
            kind: "mutable_delta".to_owned(),
            file_name: String::new(),
            approx_bytes: bytes,
        }],
        component_bytes: BTreeMap::from([("mutable_delta".to_owned(), bytes)]),
    }
}

/// The vector index a segment's search uses.
fn index_kind(segment: &SegmentHandle) -> &'static str {
    let vectors = &segment.entry.vectors;
    if vectors.iter().any(|vector| vector.has_graph) {
        "hnsw"
    } else if vectors.iter().any(|vector| vector.has_sq8) {
        "sq8"
    } else {
        "flat"
    }
}

/// The query unit of one segment, from its manifest entry: typed scalar fields report their
/// zone maps (rows, nulls, distinct count, bounds); `$extra` keys have none.
fn segment_unit_stats(version: &Version, segment: &Arc<SegmentHandle>) -> QueryUnitStats {
    let deleted = version.deletes.len_of(segment.unit);
    let rows = segment.row_count();
    let mut scalar_fields = BTreeMap::new();
    for zone in &segment.entry.zones {
        let Some(FieldRef::Scalar(field)) = version.schema.field_by_id(FieldId(zone.field_id))
        else {
            continue;
        };
        let bound = |bytes: &Option<Vec<u8>>| {
            let value = ValueBytes::from_encoded(bytes.clone()?)
                .decode(field.field_type)
                .ok()?;
            ScalarMetadataValue::from_json(&value.into_json())
        };
        scalar_fields.insert(
            field.name.clone(),
            ScalarFieldStats {
                present_count: rows.saturating_sub(zone.null_count) as usize,
                null_count: zone.null_count as usize,
                distinct_count: zone.distinct_estimate as usize,
                min: bound(&zone.min),
                max: bound(&zone.max),
                value_counts: BTreeMap::new(),
            },
        );
    }
    let file_name = segment
        .path()
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let bytes = usize::try_from(segment.entry.file_len).unwrap_or(usize::MAX);
    QueryUnitStats {
        unit_id: segment.unit.to_string(),
        tier: "immutable".to_owned(),
        index_kind: index_kind(segment).to_owned(),
        min_seq_no: segment.entry.min_seq_no,
        max_seq_no: segment.entry.max_seq_no,
        put_count: usize::try_from(u64::from(rows).saturating_sub(deleted)).unwrap_or(usize::MAX),
        delete_count: usize::try_from(deleted).unwrap_or(usize::MAX),
        approx_bytes: bytes,
        scalar_fields,
        artifact_stats: vec![QueryUnitArtifactStats {
            kind: "segment".to_owned(),
            file_name,
            approx_bytes: bytes,
        }],
        component_bytes: BTreeMap::from([("segment".to_owned(), bytes)]),
    }
}
