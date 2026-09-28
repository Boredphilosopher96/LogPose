//! The legacy read adapter: the `StorageEngine` read paths (exact scans, latest-visible lookups,
//! ANN candidates, statistics, and inspection) over the v2 units of one `Version`.
//!
//! Every read resolves to one `Version` and reads its live rows: a row is live exactly when its
//! bit is not set in its unit's deletion vector, so a read never resolves "the latest version
//! of a key" across units (I5 guarantees one live row per key). Rows are read with the
//! `Version`'s schema and flattened to v1 records (see `legacy_view`). ANN candidates are
//! served by an exact scan of the selected segments, which the query layer reranks as before;
//! the vector index sections that make it approximate arrive with the read path (PR 12).
//!
//! Segment rows are read around the buffer cache, as every v1 read read whole segment files.
//! Each segment's planner statistics are computed from its rows once per process and cached on
//! its handle; the memtables' are computed on every call, as the v1 delta's were.

use crate::{
    engine::EngineCore,
    handle::CollectionHandle,
    legacy_view::{legacy_id, legacy_put},
    metric::{storage_metric_compare, storage_metric_value},
    segment::SegmentHandle,
    state::ReadAt,
    version::{UnitRef, Version},
};
use logpose_types::{
    AnnCandidate, AnnSearchRequest, CollectionStats, LogPoseError, QueryUnitArtifactStats,
    QueryUnitStats, RecordId, ResourceKind, Result, RowAddr, ScalarFieldStats, ScalarMetadataValue,
    SeqNo, VisibleRecord,
};
use logpose_wal::codec::RowImage;
use serde_json::{Value, json};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// A metadata filter over record metadata, as the query layer passes it down.
pub(crate) type MetadataFilter = Arc<dyn for<'a> Fn(&'a Value) -> bool + Send + Sync>;

/// The query-unit id of the memtables, which the legacy planner treats as one mutable unit.
const MUTABLE_UNIT_ID: &str = "mutable-delta";

/// The `index_kind` of a segment unit: it serves ANN candidates by an exact scan.
const EXACT_SCAN_INDEX_KIND: &str = "exact";

/// Planner statistics of one segment, over every row it stores (deleted ones included, so the
/// statistics stay a conservative superset as rows are deleted).
#[derive(Debug)]
pub(crate) struct UnitStats {
    scalar_fields: BTreeMap<String, ScalarFieldStats>,
}

/// Which units a legacy read covers.
struct Selection<'a> {
    /// Whether the memtables are read.
    mutable: bool,
    /// The segments read, by query-unit id; `None` reads every segment.
    segments: Option<&'a BTreeSet<String>>,
}

impl Selection<'_> {
    fn covers(&self, unit: &UnitRef<'_>) -> bool {
        match unit {
            UnitRef::Memtable(_) => self.mutable,
            UnitRef::Segment(segment) => self
                .segments
                .is_none_or(|selected| selected.contains(&segment.unit.to_string())),
        }
    }
}

/// One live row of a `Version`.
struct LiveRow {
    seq_no: SeqNo,
    image: RowImage,
}

/// The live rows of `unit` in `version`.
fn live_rows(version: &Version, unit: &UnitRef<'_>) -> Result<Vec<LiveRow>> {
    let id = unit.id();
    let live = |row: u32| !version.is_deleted(RowAddr { unit: id, row });
    match unit {
        UnitRef::Segment(segment) => Ok((0_u32..)
            .zip(segment.read_rows()?)
            .filter(|(row, _)| live(*row))
            .map(|(_, stored)| LiveRow {
                seq_no: stored.seq_no,
                image: stored.image,
            })
            .collect()),
        UnitRef::Memtable(memtable) => (0..memtable.slot_count())
            .filter(|slot| live(*slot))
            .map(|slot| {
                Ok(LiveRow {
                    seq_no: memtable.seq_no(slot).unwrap_or_default(),
                    image: memtable.row_image(slot).map_err(LogPoseError::internal)?,
                })
            })
            .collect(),
    }
}

/// The v1 record of a live row, read with `version`'s schema.
fn visible_record(version: &Version, row: &LiveRow) -> Result<VisibleRecord> {
    let put = legacy_put(&version.schema, &row.image)?;
    Ok(VisibleRecord {
        id: put.id,
        vector: put.vector,
        metadata: put.metadata,
        seq_no: row.seq_no,
    })
}

/// The live records of the selected units, optionally only those with `wanted` ids, sorted by
/// id.
fn visible_records(
    version: &Version,
    selection: &Selection<'_>,
    wanted: Option<&BTreeSet<RecordId>>,
) -> Result<Vec<VisibleRecord>> {
    let mut records = Vec::new();
    for unit in version.units().filter(|unit| selection.covers(unit)) {
        for row in live_rows(version, &unit)? {
            if wanted.is_some_and(|wanted| !wanted.contains(&legacy_id(&row.image.pk))) {
                continue;
            }
            records.push(visible_record(version, &row)?);
        }
    }
    records.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(records)
}

impl EngineCore {
    /// Every live record of the selected units of the state `at` names, sorted by id.
    pub(crate) fn scan_exact_internal(
        &self,
        handle: &CollectionHandle,
        at: impl Into<ReadAt>,
        include_mutable: bool,
        immutable_unit_ids: Option<BTreeSet<String>>,
    ) -> Result<Vec<VisibleRecord>> {
        let (version, _) = self.read_state(handle, at)?;
        visible_records(
            &version,
            &Selection {
                mutable: include_mutable,
                segments: immutable_unit_ids.as_ref(),
            },
            None,
        )
    }

    /// The live records with `record_ids` among the selected units, sorted by id.
    pub(crate) fn latest_visible_selected(
        &self,
        handle: &CollectionHandle,
        at: impl Into<ReadAt>,
        record_ids: Vec<RecordId>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> Result<Vec<VisibleRecord>> {
        let (version, _) = self.read_state(handle, at)?;
        let selected = immutable_unit_ids.into_iter().collect::<BTreeSet<_>>();
        let wanted = record_ids.into_iter().collect::<BTreeSet<_>>();
        visible_records(
            &version,
            &Selection {
                mutable: include_mutable,
                segments: Some(&selected),
            },
            Some(&wanted),
        )
    }

    /// The best `candidate_budget` live rows of the selected segments by exact score, admitted
    /// by `filter`.
    pub(crate) fn ann_search_selected(
        &self,
        handle: &CollectionHandle,
        at: impl Into<ReadAt>,
        immutable_unit_ids: Vec<String>,
        request: &AnnSearchRequest,
        filter: Option<MetadataFilter>,
    ) -> Result<Vec<AnnCandidate>> {
        let (version, _) = self.read_state(handle, at)?;
        // The legacy read paths search the first vector field of the reading schema, the one
        // `legacy_put` returns as the record's vector.
        let metric = version
            .schema
            .vectors()
            .first()
            .map(|field| field.metric)
            .ok_or_else(|| LogPoseError::internal("a collection schema has no vector field"))?;
        let selected = immutable_unit_ids.into_iter().collect::<BTreeSet<_>>();
        let selection = Selection {
            mutable: false,
            segments: Some(&selected),
        };
        let budget = request.candidate_budget.max(request.top_k);
        let mut candidates = Vec::new();
        for unit in version.units().filter(|unit| selection.covers(unit)) {
            for row in live_rows(&version, &unit)? {
                let record = visible_record(&version, &row)?;
                if filter
                    .as_ref()
                    .is_some_and(|filter| !filter(&record.metadata))
                {
                    continue;
                }
                candidates.push(AnnCandidate {
                    unit_id: unit.id().to_string(),
                    value: storage_metric_value(metric, &request.vector, &record.vector)?,
                    record_id: record.id,
                    seq_no: record.seq_no,
                });
            }
        }
        candidates.sort_by(|left, right| {
            storage_metric_compare(metric, right.value, left.value)
                .then(right.seq_no.cmp(&left.seq_no))
                .then(left.record_id.cmp(&right.record_id))
        });
        candidates.truncate(budget);
        Ok(candidates)
    }

    /// Collection statistics of the state `at` names. The counts come from the `Version`'s
    /// counters; the query units carry the planner statistics.
    pub(crate) fn collection_stats(
        &self,
        handle: &CollectionHandle,
        at: impl Into<ReadAt>,
    ) -> Result<CollectionStats> {
        let (version, snapshot) = self.read_state(handle, at)?;
        let descriptor = handle.descriptor();
        let mut query_units = vec![mutable_unit_stats(&version)?];
        for segment in version.segments.iter() {
            query_units.push(segment_unit_stats(&version, segment)?);
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
            maintenance: self.maintenance_status(handle),
            query_units,
        })
    }

    /// The `inspect` report of `target` over the current `Version`.
    pub(crate) fn inspect(
        &self,
        handle: &CollectionHandle,
        target: crate::InspectTarget,
    ) -> Result<crate::InspectReport> {
        use crate::{InspectReport, InspectTarget};
        handle.ensure_open()?;
        let version = handle.current();
        match target {
            InspectTarget::Manifest => Ok(InspectReport {
                target: "manifest".to_owned(),
                payload: version.manifest.inspect_json(),
            }),
            InspectTarget::Wal => {
                // The memtables hold every row written since the checkpoint; deletes live only
                // in deletion vectors (and the WAL files).
                let mut records = Vec::new();
                for memtable in version.memtables() {
                    for slot in 0..memtable.slot_count() {
                        let image = memtable.row_image(slot).map_err(LogPoseError::internal)?;
                        let put = legacy_put(&version.schema, &image)?;
                        records.push(json!({
                            "seq_no": memtable.seq_no(slot).unwrap_or_default(),
                            "op": "put",
                            "id": put.id,
                            "vector": put.vector,
                            "metadata": put.metadata,
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
                payload: serde_json::to_value(self.maintenance_status(handle))
                    .map_err(crate::error::json_message)?,
            }),
            InspectTarget::Segment(segment_id) => {
                let segment = version
                    .segments
                    .iter()
                    .find(|segment| segment.unit.to_string() == segment_id)
                    .ok_or_else(|| {
                        LogPoseError::not_found(ResourceKind::Segment, segment_id.clone())
                    })?;
                inspect_segment(&version, segment)
            }
        }
    }
}

/// The `inspect segment` report: the manifest entry, the section table, and every row.
fn inspect_segment(
    version: &Version,
    segment: &Arc<SegmentHandle>,
) -> Result<crate::InspectReport> {
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
            "id": legacy_id(&stored.image.pk),
            "deleted": version.is_deleted(RowAddr {
                unit: segment.unit,
                row,
            }),
        }));
    }
    let header = reader.header();
    Ok(crate::InspectReport {
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
            },
            "sections": sections,
            "records": records,
        }),
    })
}

/// The one mutable query unit: every memtable of `version`.
fn mutable_unit_stats(version: &Version) -> Result<QueryUnitStats> {
    let mut scalar_fields = BTreeMap::new();
    let mut live = 0_usize;
    let mut dead = 0_usize;
    let mut min_seq_no = SeqNo::MAX;
    let mut max_seq_no = 0;
    for memtable in version.memtables() {
        let unit = UnitRef::Memtable(memtable);
        for slot in 0..memtable.slot_count() {
            if version.is_deleted(RowAddr {
                unit: memtable.unit,
                row: slot,
            }) {
                dead += 1;
            }
        }
        for row in live_rows(version, &unit)? {
            live += 1;
            min_seq_no = min_seq_no.min(row.seq_no);
            max_seq_no = max_seq_no.max(row.seq_no);
            let put = legacy_put(&version.schema, &row.image)?;
            update_scalar_field_stats(&mut scalar_fields, &put.metadata);
        }
    }
    let bytes = usize::try_from(
        version
            .memtables()
            .map(|memtable| memtable.bytes().total())
            .sum::<u64>(),
    )
    .unwrap_or(usize::MAX);
    Ok(QueryUnitStats {
        unit_id: MUTABLE_UNIT_ID.to_owned(),
        tier: "mutable".to_owned(),
        index_kind: "raw".to_owned(),
        min_seq_no: if live == 0 { 0 } else { min_seq_no },
        max_seq_no,
        put_count: live,
        delete_count: dead,
        approx_bytes: bytes,
        scalar_fields,
        artifact_stats: vec![QueryUnitArtifactStats {
            kind: "mutable_delta".to_owned(),
            file_name: String::new(),
            approx_bytes: bytes,
        }],
        component_bytes: BTreeMap::from([("mutable_delta".to_owned(), bytes)]),
    })
}

/// The query unit of one segment. `index_kind` is `exact`: the segment serves ANN candidates,
/// but by an exact scan of its live rows until the vector index sections land (PR 12). The
/// planner treats it as an ANN unit, so query plans do not change when the index arrives.
fn segment_unit_stats(version: &Version, segment: &Arc<SegmentHandle>) -> Result<QueryUnitStats> {
    let stats = match segment.legacy_stats.get() {
        Some(stats) => Arc::clone(stats),
        None => {
            let mut scalar_fields = BTreeMap::new();
            for stored in segment.read_rows()? {
                let put = legacy_put(&version.schema, &stored.image)?;
                update_scalar_field_stats(&mut scalar_fields, &put.metadata);
            }
            Arc::clone(
                segment
                    .legacy_stats
                    .get_or_init(|| Arc::new(UnitStats { scalar_fields })),
            )
        }
    };
    let deleted = version.deletes.len_of(segment.unit);
    let file_name = segment
        .path()
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let bytes = usize::try_from(segment.entry.file_len).unwrap_or(usize::MAX);
    Ok(QueryUnitStats {
        unit_id: segment.unit.to_string(),
        tier: "immutable".to_owned(),
        index_kind: EXACT_SCAN_INDEX_KIND.to_owned(),
        min_seq_no: segment.entry.min_seq_no,
        max_seq_no: segment.entry.max_seq_no,
        put_count: usize::try_from(u64::from(segment.row_count()).saturating_sub(deleted))
            .unwrap_or(usize::MAX),
        delete_count: usize::try_from(deleted).unwrap_or(usize::MAX),
        approx_bytes: bytes,
        scalar_fields: stats.scalar_fields.clone(),
        artifact_stats: vec![QueryUnitArtifactStats {
            kind: "segment".to_owned(),
            file_name,
            approx_bytes: bytes,
        }],
        component_bytes: BTreeMap::from([("segment".to_owned(), bytes)]),
    })
}

/// Fold one record's top-level metadata into per-field planner statistics.
fn update_scalar_field_stats(
    scalar_fields: &mut BTreeMap<String, ScalarFieldStats>,
    metadata: &Value,
) {
    let Value::Object(fields) = metadata else {
        return;
    };
    for (field, value) in fields {
        let stats = scalar_fields
            .entry(field.clone())
            .or_insert_with(|| ScalarFieldStats {
                present_count: 0,
                null_count: 0,
                distinct_count: 0,
                min: None,
                max: None,
                value_counts: BTreeMap::new(),
            });
        stats.present_count += 1;
        let Some(scalar) = ScalarMetadataValue::from_json(value) else {
            continue;
        };
        if scalar == ScalarMetadataValue::Null {
            stats.null_count += 1;
        }
        *stats.value_counts.entry(scalar.summary_key()).or_insert(0) += 1;
        stats.distinct_count = stats.value_counts.len();
        if scalar != ScalarMetadataValue::Null {
            if stats
                .min
                .as_ref()
                .is_none_or(|current| compare_scalars(&scalar, current) == Ordering::Less)
            {
                stats.min = Some(scalar.clone());
            }
            if stats
                .max
                .as_ref()
                .is_none_or(|current| compare_scalars(&scalar, current) == Ordering::Greater)
            {
                stats.max = Some(scalar);
            }
        }
    }
}

fn compare_scalars(left: &ScalarMetadataValue, right: &ScalarMetadataValue) -> Ordering {
    use ScalarMetadataValue::{Bool, Null, Number, String};
    match (left, right) {
        (String(left), String(right)) => left.cmp(right),
        (Bool(left), Bool(right)) => left.cmp(right),
        (Number(left), Number(right)) => compare_numbers(left, right),
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(_), Number(_) | String(_)) | (Number(_), String(_)) => Ordering::Less,
        (Number(_) | String(_), Bool(_)) | (String(_), Number(_)) => Ordering::Greater,
    }
}

fn compare_numbers(left: &serde_json::Number, right: &serde_json::Number) -> Ordering {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return left.cmp(&right);
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return left.cmp(&right);
    }
    let left = left.as_f64().unwrap_or_default();
    let right = right.as_f64().unwrap_or_default();
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}
