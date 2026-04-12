//! Exact flat index sidecars for immutable units.

use logpose_types::{ScalarFieldStats, ScalarMetadataValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{cmp::Ordering, collections::BTreeMap, fs, io, path::Path};

/// Index family available for a queryable unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    /// Hierarchical navigable small world graph.
    Hnsw,
    /// Inverted file with product quantization.
    IvfPq,
    /// Brute-force exact search path.
    Flat,
}

impl IndexKind {
    /// Render the index kind as a stable string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hnsw => "hnsw",
            Self::IvfPq => "ivf_pq",
            Self::Flat => "flat",
        }
    }
}

/// File-backed exact sidecar for immutable flat retrieval.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlatIndexSidecar {
    /// Sidecar version.
    pub version: u16,
    /// Segment identifier this sidecar belongs to.
    pub segment_id: String,
    /// Index family represented by the sidecar.
    pub index_kind: IndexKind,
    /// Total segment entry count, including tombstones.
    pub entry_count: usize,
    /// Number of put entries represented by the sidecar.
    pub put_count: usize,
    /// Number of delete entries represented by the sidecar.
    pub delete_count: usize,
    /// Stable offsets into the segment payload sections.
    pub entry_offsets: Vec<FlatIndexOffset>,
    /// Precomputed vector norms for put entries.
    pub vector_norms: Vec<Option<f32>>,
    /// Scalar field summaries over top-level metadata fields.
    pub scalar_fields: BTreeMap<String, ScalarFieldStats>,
}

/// Offsets into the immutable segment payload sections.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FlatIndexOffset {
    /// Offset into the record id section.
    pub record_id_offset: u64,
    /// Offset into the vector section.
    pub vector_offset: u64,
    /// Offset into the metadata section.
    pub metadata_offset: u64,
}

/// Builder input for one segment entry.
#[derive(Clone, Debug)]
pub struct FlatIndexEntrySource {
    /// Whether the segment entry is a put.
    pub is_put: bool,
    /// Offset into the record id section.
    pub record_id_offset: u64,
    /// Offset into the vector section.
    pub vector_offset: u64,
    /// Offset into the metadata section.
    pub metadata_offset: u64,
    /// Raw vector for put entries.
    pub vector: Option<Vec<f32>>,
    /// Top-level metadata JSON for put entries.
    pub metadata: Option<Value>,
}

/// Build a persisted flat exact sidecar from segment entries.
#[must_use]
pub fn build_flat_index(
    segment_id: impl Into<String>,
    entries: &[FlatIndexEntrySource],
) -> FlatIndexSidecar {
    let mut put_count = 0usize;
    let mut delete_count = 0usize;
    let mut vector_norms = Vec::with_capacity(entries.len());
    let mut entry_offsets = Vec::with_capacity(entries.len());
    let mut scalar_fields = BTreeMap::<String, ScalarFieldStats>::new();

    for entry in entries {
        entry_offsets.push(FlatIndexOffset {
            record_id_offset: entry.record_id_offset,
            vector_offset: entry.vector_offset,
            metadata_offset: entry.metadata_offset,
        });

        if entry.is_put {
            put_count += 1;
            vector_norms.push(entry.vector.as_ref().map(|vector| vector_norm(vector)));
            if let Some(metadata) = &entry.metadata {
                update_scalar_field_stats(&mut scalar_fields, metadata);
            }
        } else {
            delete_count += 1;
            vector_norms.push(None);
        }
    }

    FlatIndexSidecar {
        version: 1,
        segment_id: segment_id.into(),
        index_kind: IndexKind::Flat,
        entry_count: entries.len(),
        put_count,
        delete_count,
        entry_offsets,
        vector_norms,
        scalar_fields,
    }
}

/// Persist a flat exact sidecar to disk.
pub fn write_flat_index(path: &Path, sidecar: &FlatIndexSidecar) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(sidecar)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?,
    )
}

/// Load a flat exact sidecar from disk.
pub fn read_flat_index(path: &Path) -> io::Result<FlatIndexSidecar> {
    serde_json::from_slice(&fs::read(path)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn vector_norm(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum::<f32>().sqrt()
}

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

        let summary_key = scalar.summary_key();
        let next_count = stats.value_counts.entry(summary_key).or_insert(0);
        *next_count += 1;
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
    match (left, right) {
        (ScalarMetadataValue::String(left), ScalarMetadataValue::String(right)) => left.cmp(right),
        (ScalarMetadataValue::Bool(left), ScalarMetadataValue::Bool(right)) => left.cmp(right),
        (ScalarMetadataValue::Number(left), ScalarMetadataValue::Number(right)) => {
            compare_numbers(left, right)
        }
        (ScalarMetadataValue::Null, ScalarMetadataValue::Null) => Ordering::Equal,
        (ScalarMetadataValue::Null, _) => Ordering::Less,
        (_, ScalarMetadataValue::Null) => Ordering::Greater,
        (
            ScalarMetadataValue::Bool(_),
            ScalarMetadataValue::Number(_) | ScalarMetadataValue::String(_),
        ) => Ordering::Less,
        (ScalarMetadataValue::Number(_), ScalarMetadataValue::String(_)) => Ordering::Less,
        (
            ScalarMetadataValue::Number(_) | ScalarMetadataValue::String(_),
            ScalarMetadataValue::Bool(_),
        ) => Ordering::Greater,
        (ScalarMetadataValue::String(_), ScalarMetadataValue::Number(_)) => Ordering::Greater,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_flat_index_tracks_norms_offsets_and_scalar_stats() {
        let sidecar = build_flat_index(
            "segment-1",
            &[
                FlatIndexEntrySource {
                    is_put: true,
                    record_id_offset: 0,
                    vector_offset: 0,
                    metadata_offset: 0,
                    vector: Some(vec![3.0, 4.0]),
                    metadata: Some(json!({
                        "topic":"intro",
                        "rank":2,
                        "active":true,
                        "details":{"chapter":1}
                    })),
                },
                FlatIndexEntrySource {
                    is_put: false,
                    record_id_offset: 4,
                    vector_offset: 0,
                    metadata_offset: 0,
                    vector: None,
                    metadata: None,
                },
            ],
        );

        assert_eq!(sidecar.index_kind, IndexKind::Flat);
        assert_eq!(sidecar.put_count, 1);
        assert_eq!(sidecar.delete_count, 1);
        assert_eq!(sidecar.vector_norms, vec![Some(5.0), None]);
        assert_eq!(sidecar.entry_offsets[0].record_id_offset, 0);
        assert_eq!(sidecar.scalar_fields["topic"].present_count, 1);
        assert_eq!(sidecar.scalar_fields["rank"].distinct_count, 1);
        assert_eq!(sidecar.scalar_fields["details"].present_count, 1);
        assert!(sidecar.scalar_fields["details"].value_counts.is_empty());
    }
}
