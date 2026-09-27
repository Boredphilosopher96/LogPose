//! Segment v1 decoding.

use super::{SegmentEntry, SegmentEntryKind, SegmentFooter, SegmentHeader};
use crate::segment_v1::SegmentRecord;
use crate::{durable_fs::read_file, error::json_message};
use crc32fast::hash;
use logpose_types::{LogPoseError, PutRecord, RecordId, Result, WriteOperation};
use logpose_vfs::Vfs;
use std::path::Path;

pub(crate) fn read_segment_file(vfs: &dyn Vfs, path: &Path) -> Result<Vec<SegmentRecord>> {
    let bytes = read_file(vfs, path, "failed to read segment file")?;
    if bytes.len() < 4 || &bytes[..4] != b"LPS1" {
        return Err(LogPoseError::Message(format!(
            "invalid segment magic in '{}'",
            path.display()
        )));
    }

    let mut offset = 4usize;
    let read_len = |bytes: &[u8], offset: &mut usize| -> Result<usize> {
        let slice = checked_slice(bytes, *offset, 8, "segment length header")?;
        let value = u64::from_le_bytes(
            slice
                .try_into()
                .expect("segment length slice should fit after bounds check"),
        ) as usize;
        *offset += 8;
        Ok(value)
    };
    let header_len = read_len(&bytes, &mut offset)?;
    let entry_len = read_len(&bytes, &mut offset)?;
    let ids_len = read_len(&bytes, &mut offset)?;
    let vectors_len = read_len(&bytes, &mut offset)?;
    let metadata_len = read_len(&bytes, &mut offset)?;
    let footer_len = read_len(&bytes, &mut offset)?;

    let header: SegmentHeader =
        serde_json::from_slice(checked_slice(&bytes, offset, header_len, "segment header")?)
            .map_err(json_message)?;
    offset += header_len;
    let entries: Vec<SegmentEntry> = serde_json::from_slice(checked_slice(
        &bytes,
        offset,
        entry_len,
        "segment entry table",
    )?)
    .map_err(json_message)?;
    offset += entry_len;

    let ids = checked_slice(&bytes, offset, ids_len, "segment id section")?;
    offset += ids_len;
    let vectors = checked_slice(&bytes, offset, vectors_len, "segment vector section")?;
    offset += vectors_len;
    let metadata = checked_slice(&bytes, offset, metadata_len, "segment metadata section")?;
    offset += metadata_len;
    let footer: SegmentFooter =
        serde_json::from_slice(checked_slice(&bytes, offset, footer_len, "segment footer")?)
            .map_err(json_message)?;

    let actual_checksum = hash(&[ids, vectors, metadata].concat());
    if actual_checksum != footer.payload_checksum {
        return Err(LogPoseError::Message(format!(
            "checksum mismatch while reading segment '{}': expected {}, got {}",
            path.display(),
            footer.payload_checksum,
            actual_checksum
        )));
    }

    let mut records = Vec::with_capacity(header.entry_count);
    for entry in entries {
        let id_slice = checked_slice(
            ids,
            entry.record_id_offset as usize,
            entry.record_id_len as usize,
            "segment record id",
        )?;
        let id = RecordId::new(std::str::from_utf8(id_slice).map_err(|error| {
            LogPoseError::Message(format!("failed to decode record id from segment: {error}"))
        })?);

        let op = match entry.kind {
            SegmentEntryKind::Put => {
                let mut vector = Vec::with_capacity(entry.vector_dimensions as usize);
                let vector_start = entry.vector_offset as usize;
                let vector_end = vector_start + entry.vector_dimensions as usize * 4;
                for chunk in checked_slice(
                    vectors,
                    vector_start,
                    vector_end.saturating_sub(vector_start),
                    "segment vector payload",
                )?
                .chunks_exact(4)
                {
                    vector.push(f32::from_le_bytes(
                        chunk.try_into().expect("vector chunk should be four bytes"),
                    ));
                }
                let metadata_start = entry.metadata_offset as usize;
                let metadata_end = metadata_start + entry.metadata_len as usize;
                let metadata_value = serde_json::from_slice(checked_slice(
                    metadata,
                    metadata_start,
                    metadata_end.saturating_sub(metadata_start),
                    "segment metadata payload",
                )?)
                .map_err(json_message)?;
                WriteOperation::Put(PutRecord {
                    id,
                    vector,
                    metadata: metadata_value,
                })
            }
            SegmentEntryKind::Delete => WriteOperation::Delete(logpose_types::DeleteRecord { id }),
        };

        records.push(SegmentRecord {
            seq_no: entry.seq_no,
            op,
        });
    }
    Ok(records)
}

fn checked_slice<'a>(bytes: &'a [u8], start: usize, len: usize, label: &str) -> Result<&'a [u8]> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| LogPoseError::Message(format!("overflow while reading {label}")))?;
    if end > bytes.len() {
        return Err(LogPoseError::Message(format!(
            "truncated segment while reading {label}: need {end} bytes but file has {}",
            bytes.len()
        )));
    }
    Ok(&bytes[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CreateCollectionRequest, InspectTarget, LocalStorageEngine, StorageEngine,
        test_support::unique_temp_dir,
    };
    use logpose_types::DistanceMetric;
    use serde_json::json;
    use std::fs;

    #[test]
    fn truncated_segment_returns_error_instead_of_panicking() {
        let root = unique_temp_dir("storage-truncated-segment");
        let runtime = tokio::runtime::Runtime::new().expect("runtime should build");

        let segment_path = runtime.block_on(async {
            let engine = LocalStorageEngine::new(&root).expect("storage engine should open");
            let descriptor = engine
                .create_collection(CreateCollectionRequest::new(
                    "broken",
                    2,
                    DistanceMetric::Cosine,
                ))
                .await
                .expect("collection should be created");

            engine
                .write(
                    "broken",
                    vec![WriteOperation::Put(PutRecord {
                        id: RecordId::new("id-1"),
                        vector: vec![1.0, 1.0],
                        metadata: json!({"status":"ok"}),
                    })],
                )
                .await
                .expect("write should succeed");
            engine.flush("broken").await.expect("flush should succeed");

            let manifest = engine
                .inspect("broken", InspectTarget::Manifest)
                .await
                .expect("inspect should succeed");
            let segment_file = manifest.payload["segments"][0]["file_name"]
                .as_str()
                .expect("segment file should exist");
            descriptor.root_path.join("segments").join(segment_file)
        });

        let bytes = fs::read(&segment_path).expect("segment file should exist");
        fs::write(&segment_path, &bytes[..10]).expect("truncate should succeed");

        let result =
            std::panic::catch_unwind(|| read_segment_file(&logpose_vfs::StdVfs, &segment_path));
        assert!(result.is_ok(), "truncated segment should not panic");
        assert!(result.expect("result should exist").is_err());
    }
}
