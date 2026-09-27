//! Segment v1 encoding and publication of the segment file with its flat and HNSW sidecars.

use super::{SegmentEntry, SegmentEntryKind, SegmentFooter, SegmentHeader, SegmentPurpose};
use crate::engine::EngineCore;
use crate::segment_v1::SegmentRecord;
use crate::{
    durable_fs::{create_dir_all_synced, sync_parent_dir, write_file_synced},
    error::{io_message, json_message},
    fs_util::{cleanup_file, crash_point},
    manifest::{RemoteArtifact, RemoteSyncState, SegmentMeta},
    stats::segment_component_bytes,
};
use crc32fast::hash;
use logpose_catalog::CollectionDescriptor;
use logpose_index::{
    FlatIndexEntrySource, HnswBuildParams, HnswIndexEntrySource, build_flat_index,
    build_hnsw_index, encode_flat_index, encode_hnsw_index,
};
use logpose_types::{QueryUnitArtifactStats, Result, WriteOperation};
use logpose_vfs::Vfs;
use std::{collections::BTreeSet, io, path::Path};
use uuid::Uuid;

impl EngineCore {
    pub(crate) fn write_segment_file(
        &self,
        descriptor: &CollectionDescriptor,
        records: &[SegmentRecord],
        purpose: SegmentPurpose,
    ) -> Result<SegmentMeta> {
        let segment_id = Uuid::new_v4().to_string();
        let temp_path = descriptor
            .root_path
            .join("tmp")
            .join(format!("{segment_id}.lps.tmp"));
        let final_path = descriptor
            .root_path
            .join("segments")
            .join(format!("{segment_id}.lps"));
        let sidecar_temp_path = descriptor
            .root_path
            .join("tmp")
            .join(format!("{segment_id}.flat.json.tmp"));
        let sidecar_path = Self::flat_index_file_path(descriptor, &segment_id);
        let hnsw_temp_path = descriptor
            .root_path
            .join("tmp")
            .join(format!("{segment_id}.hnsw.bin.tmp"));
        let hnsw_path = Self::hnsw_index_file_path(descriptor, &segment_id);

        let mut ids = Vec::new();
        let mut vectors = Vec::new();
        let mut metadata = Vec::new();
        let mut entries = Vec::new();
        let mut sidecar_entries = Vec::new();
        let mut hnsw_entry_sources = Vec::new();
        let mut put_count = 0usize;
        let mut delete_count = 0usize;
        let mut min_seq_no = u64::MAX;
        let mut max_seq_no = 0u64;

        for record in records {
            min_seq_no = min_seq_no.min(record.seq_no);
            max_seq_no = max_seq_no.max(record.seq_no);

            let id_offset = ids.len() as u64;
            let id_bytes = record.op.id().as_str().as_bytes();
            ids.extend_from_slice(id_bytes);

            match &record.op {
                WriteOperation::Put(put) => {
                    put_count += 1;
                    let vector_offset = vectors.len() as u64;
                    for value in &put.vector {
                        vectors.extend_from_slice(&value.to_le_bytes());
                    }
                    let metadata_offset = metadata.len() as u64;
                    let metadata_bytes = serde_json::to_vec(&put.metadata).map_err(json_message)?;
                    metadata.extend_from_slice(&metadata_bytes);

                    entries.push(SegmentEntry {
                        seq_no: record.seq_no,
                        record_id_offset: id_offset,
                        record_id_len: id_bytes.len() as u32,
                        kind: SegmentEntryKind::Put,
                        vector_offset,
                        vector_dimensions: put.vector.len() as u32,
                        metadata_offset,
                        metadata_len: metadata_bytes.len() as u32,
                    });
                    sidecar_entries.push(FlatIndexEntrySource {
                        is_put: true,
                        record_id_offset: id_offset,
                        vector_offset,
                        metadata_offset,
                        vector: Some(put.vector.clone()),
                        metadata: Some(put.metadata.clone()),
                    });
                    hnsw_entry_sources.push(Some(HnswIndexEntrySource {
                        entry_offset_index: entries.len() - 1,
                        record_id: put.id.clone(),
                        seq_no: record.seq_no,
                        vector: put.vector.clone(),
                        metadata: put.metadata.clone(),
                    }));
                }
                WriteOperation::Delete(_) => {
                    delete_count += 1;
                    entries.push(SegmentEntry {
                        seq_no: record.seq_no,
                        record_id_offset: id_offset,
                        record_id_len: id_bytes.len() as u32,
                        kind: SegmentEntryKind::Delete,
                        vector_offset: 0,
                        vector_dimensions: 0,
                        metadata_offset: 0,
                        metadata_len: 0,
                    });
                    sidecar_entries.push(FlatIndexEntrySource {
                        is_put: false,
                        record_id_offset: id_offset,
                        vector_offset: 0,
                        metadata_offset: 0,
                        vector: None,
                        metadata: None,
                    });
                    hnsw_entry_sources.push(None);
                }
            }
        }

        if records.is_empty() {
            min_seq_no = 0;
        }

        let header = SegmentHeader {
            version: 1,
            dimensions: descriptor.dimensions,
            entry_count: entries.len(),
        };
        let footer = SegmentFooter {
            payload_checksum: hash(
                &[ids.as_slice(), vectors.as_slice(), metadata.as_slice()].concat(),
            ),
        };

        let header_bytes = serde_json::to_vec(&header).map_err(json_message)?;
        let entry_bytes = serde_json::to_vec(&entries).map_err(json_message)?;
        let footer_bytes = serde_json::to_vec(&footer).map_err(json_message)?;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LPS1");
        for len in [
            header_bytes.len(),
            entry_bytes.len(),
            ids.len(),
            vectors.len(),
            metadata.len(),
            footer_bytes.len(),
        ] {
            bytes.extend_from_slice(&(len as u64).to_le_bytes());
        }
        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(&entry_bytes);
        bytes.extend_from_slice(&ids);
        bytes.extend_from_slice(&vectors);
        bytes.extend_from_slice(&metadata);
        bytes.extend_from_slice(&footer_bytes);

        let flat_index = build_flat_index(segment_id.clone(), &sidecar_entries);
        let visible_hnsw_entries =
            visible_hnsw_entries(records, &hnsw_entry_sources, descriptor.dimensions)
                .map_err(|error| io_message("failed to build hnsw sidecar", error))?;
        let hnsw_index = build_hnsw_index(
            segment_id.clone(),
            descriptor.metric,
            HnswBuildParams::default(),
            &visible_hnsw_entries,
        )
        .map_err(|error| io_message("failed to build hnsw sidecar", error))?;
        let flat_bytes = encode_flat_index(&flat_index)
            .map_err(|error| io_message("failed to encode flat index sidecar", error))?;
        let hnsw_bytes = encode_hnsw_index(&hnsw_index)
            .map_err(|error| io_message("failed to encode hnsw sidecar", error))?;
        let (segment_len, flat_len, hnsw_len) = (bytes.len(), flat_bytes.len(), hnsw_bytes.len());
        publish_segment_artifacts(
            self.vfs.as_ref(),
            SegmentArtifactPaths {
                segment_temp_path: &temp_path,
                segment_path: &final_path,
                flat_temp_path: &sidecar_temp_path,
                flat_path: &sidecar_path,
                hnsw_temp_path: &hnsw_temp_path,
                hnsw_path: &hnsw_path,
            },
            SegmentArtifactBytes {
                segment: &bytes,
                flat: &flat_bytes,
                hnsw: &hnsw_bytes,
            },
            purpose,
        )?;

        let segment_bytes = segment_len;
        let flat_bytes = flat_len;
        let hnsw_bytes = hnsw_len;
        let artifacts = vec![
            QueryUnitArtifactStats {
                kind: "flat_exact".to_owned(),
                file_name: sidecar_path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_else(|| format!("{segment_id}.flat.json")),
                approx_bytes: flat_bytes,
            },
            QueryUnitArtifactStats {
                kind: "hnsw".to_owned(),
                file_name: hnsw_path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_else(|| format!("{segment_id}.hnsw.bin")),
                approx_bytes: hnsw_bytes,
            },
        ];
        let component_bytes = segment_component_bytes(&hnsw_index, segment_bytes, flat_bytes);

        let remote = descriptor
            .remote_blob
            .as_ref()
            .map(|config| RemoteArtifact {
                key: format!(
                    "{}/collections/{}/segments/{}.lps",
                    config.prefix, descriptor.collection_id, segment_id
                ),
                status: if self.blob_store.is_some() {
                    RemoteSyncState::PendingUpload
                } else {
                    RemoteSyncState::UploadSkipped
                },
            });

        Ok(SegmentMeta {
            segment_id: segment_id.clone(),
            file_name: final_path
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| "segment.lps".to_owned()),
            min_seq_no,
            max_seq_no,
            put_count,
            delete_count,
            dimensions: descriptor.dimensions,
            checksum: footer.payload_checksum,
            approx_bytes: segment_bytes + flat_bytes + hnsw_bytes,
            index_kind: hnsw_index.index_kind.as_str().to_owned(),
            scalar_fields: flat_index.scalar_fields,
            artifacts,
            component_bytes,
            remote,
        })
    }
}

struct SegmentArtifactPaths<'a> {
    segment_temp_path: &'a Path,
    segment_path: &'a Path,
    flat_temp_path: &'a Path,
    flat_path: &'a Path,
    hnsw_temp_path: &'a Path,
    hnsw_path: &'a Path,
}

struct SegmentArtifactBytes<'a> {
    segment: &'a [u8],
    flat: &'a [u8],
    hnsw: &'a [u8],
}

/// Write the segment and both sidecars to temp files, sync them, rename them into place, and
/// sync the destination directories, cleaning up on failure.
///
/// The sidecars are encoded by `logpose-index` and written here, so every byte goes through the
/// engine's `Vfs`.
fn publish_segment_artifacts(
    vfs: &dyn Vfs,
    paths: SegmentArtifactPaths<'_>,
    bytes: SegmentArtifactBytes<'_>,
    purpose: SegmentPurpose,
) -> Result<()> {
    let cleanup = |files: &[&Path]| {
        for file in files {
            cleanup_file(vfs, file);
        }
    };
    if let Some(parent) = paths.segment_temp_path.parent() {
        create_dir_all_synced(vfs, parent)?;
    }
    if let Err(error) = write_file_synced(vfs, paths.segment_temp_path, bytes.segment) {
        cleanup(&[paths.segment_temp_path]);
        return Err(error);
    }
    crash_point(vfs, purpose.after_file_sync())?;
    if let Err(error) = write_file_synced(vfs, paths.flat_temp_path, bytes.flat) {
        cleanup(&[
            paths.segment_temp_path,
            paths.flat_temp_path,
            paths.hnsw_temp_path,
        ]);
        return Err(prefixed("failed to publish flat index sidecar", error));
    }
    if let Err(error) = write_file_synced(vfs, paths.hnsw_temp_path, bytes.hnsw) {
        cleanup(&[
            paths.segment_temp_path,
            paths.flat_temp_path,
            paths.hnsw_temp_path,
        ]);
        return Err(prefixed("failed to publish hnsw sidecar", error));
    }
    if let Err(error) = vfs.rename(paths.segment_temp_path, paths.segment_path) {
        cleanup(&[
            paths.segment_temp_path,
            paths.flat_temp_path,
            paths.hnsw_temp_path,
        ]);
        return Err(io_message("failed to publish segment file", error));
    }
    if let Err(error) = vfs.rename(paths.flat_temp_path, paths.flat_path) {
        cleanup(&[
            paths.segment_path,
            paths.flat_temp_path,
            paths.hnsw_temp_path,
        ]);
        return Err(io_message("failed to publish flat index sidecar", error));
    }
    if let Err(error) = vfs.rename(paths.hnsw_temp_path, paths.hnsw_path) {
        cleanup(&[
            paths.segment_path,
            paths.flat_path,
            paths.flat_temp_path,
            paths.hnsw_temp_path,
        ]);
        return Err(io_message("failed to publish hnsw sidecar", error));
    }
    // The renames above are durable only once their directories are synced, and the manifest
    // that references these files must not be published before that.
    sync_parent_dir(vfs, paths.segment_path)?;
    sync_parent_dir(vfs, paths.flat_path)?;
    if paths.hnsw_path.parent() != paths.flat_path.parent() {
        sync_parent_dir(vfs, paths.hnsw_path)?;
    }
    crash_point(vfs, Some(purpose.after_dir_sync()))
}

fn prefixed(context: &str, error: logpose_types::LogPoseError) -> logpose_types::LogPoseError {
    match error {
        logpose_types::LogPoseError::Io {
            context: inner,
            source,
        } => logpose_types::LogPoseError::Io {
            context: format!("{context}: {inner}"),
            source,
        },
        other => other,
    }
}

fn visible_hnsw_entries(
    records: &[SegmentRecord],
    entry_sources: &[Option<HnswIndexEntrySource>],
    dimensions: usize,
) -> io::Result<Vec<HnswIndexEntrySource>> {
    let mut seen = BTreeSet::new();
    let mut visible = Vec::new();
    for (index, record) in records.iter().enumerate().rev() {
        let record_id = record.op.id().clone();
        if !seen.insert(record_id) {
            continue;
        }
        let Some(entry) = entry_sources.get(index).and_then(|entry| entry.clone()) else {
            continue;
        };
        if entry.vector.len() != dimensions {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "stored vector '{}' expected {} dimensions but found {}",
                    entry.record_id,
                    dimensions,
                    entry.vector.len()
                ),
            ));
        }
        visible.push(entry);
    }
    visible.reverse();
    Ok(visible)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CreateCollectionRequest, LocalStorageEngine, StorageEngine, test_support::unique_temp_dir,
    };
    use logpose_types::{DistanceMetric, PutRecord, RecordId};
    use serde_json::json;
    use std::fs;

    #[test]
    fn visible_hnsw_entries_ignore_shadowed_dimension_mismatches() {
        let records = vec![
            SegmentRecord {
                seq_no: 1,
                op: WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![9.0],
                    metadata: json!({"version":1}),
                }),
            },
            SegmentRecord {
                seq_no: 2,
                op: WriteOperation::Put(PutRecord {
                    id: RecordId::new("alpha"),
                    vector: vec![1.0, 0.0],
                    metadata: json!({"version":2}),
                }),
            },
        ];
        let entries = vec![
            Some(HnswIndexEntrySource {
                entry_offset_index: 0,
                record_id: RecordId::new("alpha"),
                seq_no: 1,
                vector: vec![9.0],
                metadata: json!({"version":1}),
            }),
            Some(HnswIndexEntrySource {
                entry_offset_index: 1,
                record_id: RecordId::new("alpha"),
                seq_no: 2,
                vector: vec![1.0, 0.0],
                metadata: json!({"version":2}),
            }),
        ];

        let visible =
            visible_hnsw_entries(&records, &entries, 2).expect("latest visible record is valid");

        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].seq_no, 2);
        assert_eq!(visible[0].vector, vec![1.0, 0.0]);
    }

    #[test]
    fn write_segment_file_rejects_visible_dimension_mismatches() {
        let root = unique_temp_dir("storage-visible-dimension-mismatch");
        let runtime = tokio::runtime::Runtime::new().expect("runtime should build");

        let result = runtime.block_on(async {
            let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
            let descriptor = engine
                .create_collection(CreateCollectionRequest::new(
                    "broken",
                    2,
                    DistanceMetric::Dot,
                ))
                .await
                .expect("collection should be created");

            engine.engine().core().write_segment_file(
                &descriptor,
                &[SegmentRecord {
                    seq_no: 1,
                    op: WriteOperation::Put(PutRecord {
                        id: RecordId::new("alpha"),
                        vector: vec![1.0],
                        metadata: json!({"kind":"broken"}),
                    }),
                }],
                SegmentPurpose::Flush,
            )
        });

        let error = result.expect_err("visible dimension mismatch should fail segment build");
        assert!(
            error
                .to_string()
                .contains("failed to build hnsw sidecar: stored vector 'alpha' expected 2 dimensions but found 1"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn sidecar_publish_failure_cleans_up_published_segment_file() {
        let root = unique_temp_dir("storage-sidecar-cleanup");
        let temp_path = root.join("tmp").join("segment.lps.tmp");
        let final_path = root.join("segments").join("segment.lps");
        let sidecar_temp_path = root.join("tmp").join("segment.flat.json.tmp");
        let sidecar_path = root.join("indexes").join("segment.flat.json");
        let hnsw_temp_path = root.join("tmp").join("segment.hnsw.bin.tmp");
        let hnsw_path = root.join("indexes").join("segment.hnsw.bin");
        fs::create_dir_all(final_path.parent().expect("segment parent should exist"))
            .expect("segment parent should be created");
        fs::create_dir_all(sidecar_path.parent().expect("index parent should exist"))
            .expect("index parent should be created");
        fs::create_dir_all(&sidecar_path).expect("directory should force sidecar publish failure");

        let flat_index = build_flat_index(
            "segment",
            &[FlatIndexEntrySource {
                is_put: true,
                record_id_offset: 0,
                vector_offset: 0,
                metadata_offset: 0,
                vector: Some(vec![1.0, 0.0]),
                metadata: Some(json!({"kind":"keep"})),
            }],
        );
        let hnsw_index = build_hnsw_index(
            "segment",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[HnswIndexEntrySource {
                entry_offset_index: 0,
                record_id: RecordId::new("alpha"),
                seq_no: 1,
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            }],
        )
        .expect("hnsw index should build");

        let result = publish_segment_artifacts(
            &logpose_vfs::StdVfs,
            SegmentArtifactPaths {
                segment_temp_path: &temp_path,
                segment_path: &final_path,
                flat_temp_path: &sidecar_temp_path,
                flat_path: &sidecar_path,
                hnsw_temp_path: &hnsw_temp_path,
                hnsw_path: &hnsw_path,
            },
            SegmentArtifactBytes {
                segment: b"segment-bytes",
                flat: &encode_flat_index(&flat_index).expect("flat sidecar should encode"),
                hnsw: &encode_hnsw_index(&hnsw_index).expect("hnsw sidecar should encode"),
            },
            SegmentPurpose::Flush,
        );

        assert!(result.is_err(), "sidecar publish should fail");
        assert!(
            !final_path.exists(),
            "segment file should be removed after sidecar publish failure"
        );
        assert!(
            !temp_path.exists(),
            "temporary segment file should be cleaned up after failure"
        );
        assert!(
            !sidecar_temp_path.exists(),
            "temporary sidecar file should be cleaned up after failure"
        );
        assert!(
            !hnsw_temp_path.exists(),
            "temporary hnsw file should be cleaned up after failure"
        );
    }
}
