//! Manifest v2 and the `CURRENT` protocol.
//!
//! A manifest generation is one immutable file, `manifests/<generation:020>.mf`: a 32-byte
//! header (magic `LPMANIF2`, generation, payload length, payload CRC, header CRC) followed by
//! the postcard-encoded [`Manifest`]. `CURRENT` is the 21-byte text file `<generation:020>\n`
//! naming the durable generation. [`publish_manifest`] runs the atomic publish protocol; its
//! commit point is the directory sync after `CURRENT` is renamed into place.
//!
//! The manifest records the collection's schema at its commit, which may be newer than its
//! checkpoint: schema changes live in the WAL until a manifest records them, and replay skips
//! `SchemaChange` frames the manifest's schema already reflects. It also records the id
//! counters (`next_unit_id`, `next_dv_gen`), so ids are never reissued across restarts.
//!
//! Each [`ManifestSegment`] describes one segment v2 file (`segments/<unit:08x>.seg`): its
//! length and footer CRC (checked when the segment is opened), row count, sequence range, size
//! tier, per-field summaries, the deletion-vector file generation in force for it, if any, and
//! its index sidecar (`segments/<unit:08x>.idx.<sidecar unit:08x>`, the vector graphs an
//! index-build job added after the segment was written), if its build ran.

use crate::{durable_fs::read_file, fs_util::crash_point};
use logpose_types::{
    CollectionId, CorruptionKind, LogPoseError, Result, SeqNo, UnitId, schema::CollectionSchema,
};
use logpose_vfs::{CrashPoint, OpenMode, Vfs};
use serde::{Deserialize, Serialize};
use std::{
    io::{ErrorKind, IoSlice},
    path::{Path, PathBuf},
};

/// The manifest format this build writes and reads. Version 3 added index sidecars.
pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 3;
/// First bytes of every manifest file.
const MANIFEST_MAGIC: &[u8; 8] = b"LPMANIF2";
/// Bytes before the payload.
const MANIFEST_HEADER_LEN: usize = 32;
/// Largest payload a manifest file may declare (a sanity bound on corrupt lengths).
const MAX_MANIFEST_PAYLOAD: u64 = 1 << 30;
/// Length of `CURRENT`: twenty digits and a newline.
const CURRENT_LEN: usize = 21;
/// Name of the manifest pointer.
pub(crate) const CURRENT_FILE: &str = "CURRENT";
/// Name of the pointer being written, before its rename.
pub(crate) const CURRENT_TEMP_FILE: &str = "CURRENT.tmp";
/// Directory of manifest generations, inside the collection directory.
pub(crate) const MANIFESTS_DIR: &str = "manifests";
/// Extension of a manifest generation file.
const MANIFEST_EXTENSION: &str = ".mf";

/// One durable state of a collection's files.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    /// Always [`MANIFEST_FORMAT_VERSION`].
    pub(crate) format_version: u32,
    /// The collection this manifest belongs to; checked against the descriptor at open.
    pub(crate) collection_id: CollectionId,
    pub(crate) generation: u64,
    /// Ownership epoch; always 0 until replication (Phase 7).
    pub(crate) epoch: u64,
    /// Every operation at or below this sequence number is reflected in the segments (I9).
    pub(crate) checkpoint_seq_no: SeqNo,
    /// The writer's schema when this manifest was committed.
    pub(crate) schema: CollectionSchema,
    /// Next unit id to allocate. Every unit id below it was issued (and maybe burned).
    pub(crate) next_unit_id: u32,
    /// Next deletion-vector file generation; one counter for all segments.
    pub(crate) next_dv_gen: u64,
    /// Ascending by unit.
    pub(crate) segments: Vec<ManifestSegment>,
    pub(crate) totals: ManifestTotals,
}

/// One segment of a manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ManifestSegment {
    pub(crate) unit: UnitId,
    /// Length of the segment file in bytes.
    pub(crate) file_len: u64,
    /// The segment's footer CRC, checked when the segment is opened.
    pub(crate) footer_crc: u32,
    /// Rows in the file, deleted ones included.
    pub(crate) row_count: u32,
    /// Schema version the segment was written with. `u64`, like `CollectionSchema` and the
    /// segment v2 header.
    pub(crate) schema_version: u64,
    pub(crate) min_seq_no: SeqNo,
    pub(crate) max_seq_no: SeqNo,
    pub(crate) origin: SegmentOrigin,
    /// Size tier of the segment's row count when it was written.
    pub(crate) tier: u8,
    /// The deletion vector file in force, if the segment had deleted rows at a checkpoint.
    pub(crate) dv: Option<DvRef>,
    /// The segment's index sidecar, once its index build ran (it may hold no graph, when no
    /// field had enough distinct vectors). `None` until then: the segment is searched without a
    /// graph, and the writer plans the build.
    pub(crate) index: Option<IndexRef>,
    /// Per vector field summary.
    pub(crate) vectors: Vec<VectorSummary>,
    /// Per scalar field zone map.
    pub(crate) zones: Vec<FieldZone>,
}

/// How a segment was made.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum SegmentOrigin {
    /// A flush of the operations in `first_seq_no..=last_seq_no`.
    Flush {
        first_seq_no: SeqNo,
        last_seq_no: SeqNo,
    },
    /// A compaction of these units.
    Compaction { inputs: Vec<UnitId> },
}

/// A deletion vector file named by the manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DvRef {
    pub(crate) generation: u64,
    pub(crate) cardinality: u32,
    pub(crate) covered_seq_no: SeqNo,
}

/// An index sidecar named by the manifest: `segments/<segment:08x>.idx.<unit:08x>`, where
/// `unit` is the unit id the index-build job that wrote it was allocated (so no attempt ever
/// reuses a name). Its length and footer CRC are checked when the segment is opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexRef {
    pub(crate) unit: UnitId,
    pub(crate) file_len: u64,
    pub(crate) footer_crc: u32,
}

/// Per vector field summary of a segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct VectorSummary {
    pub(crate) field_id: u32,
    pub(crate) has_graph: bool,
    pub(crate) has_sq8: bool,
    pub(crate) non_null: u32,
}

/// Per scalar field zone map of a segment. Bounds are canonical value-codec bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct FieldZone {
    pub(crate) field_id: u32,
    pub(crate) min: Option<Vec<u8>>,
    pub(crate) max: Option<Vec<u8>>,
    pub(crate) null_count: u32,
    pub(crate) distinct_estimate: u32,
}

/// Totals over every segment of a manifest.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ManifestTotals {
    pub(crate) rows: u64,
    pub(crate) deleted_rows: u64,
    pub(crate) segment_bytes: u64,
}

impl Manifest {
    /// Generation 0 of a new collection.
    pub(crate) fn empty(collection_id: CollectionId, schema: CollectionSchema) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            collection_id,
            generation: 0,
            epoch: 0,
            checkpoint_seq_no: 0,
            schema,
            next_unit_id: 0,
            next_dv_gen: 0,
            segments: Vec::new(),
            totals: ManifestTotals::default(),
        }
    }

    /// Recompute `totals` from `segments`.
    pub(crate) fn with_totals(mut self) -> Self {
        self.totals = ManifestTotals {
            rows: self
                .segments
                .iter()
                .map(|segment| u64::from(segment.row_count))
                .sum(),
            deleted_rows: self
                .segments
                .iter()
                .filter_map(|segment| segment.dv.map(|dv| u64::from(dv.cardinality)))
                .sum(),
            segment_bytes: self.segments.iter().map(|segment| segment.file_len).sum(),
        };
        self
    }

    /// The units of every segment.
    pub(crate) fn units(&self) -> impl Iterator<Item = UnitId> + '_ {
        self.segments.iter().map(|segment| segment.unit)
    }

    /// The manifest as JSON for `inspect`.
    pub(crate) fn inspect_json(&self) -> serde_json::Value {
        let segments = self
            .segments
            .iter()
            .map(|segment| {
                serde_json::json!({
                    "unit": segment.unit.to_string(),
                    "segment_id": segment.unit.to_string(),
                    "file_len": segment.file_len,
                    "footer_crc": segment.footer_crc,
                    "row_count": segment.row_count,
                    "deleted_rows": segment.dv.map_or(0, |dv| dv.cardinality),
                    "schema_version": segment.schema_version,
                    "min_seq_no": segment.min_seq_no,
                    "max_seq_no": segment.max_seq_no,
                    "origin": serde_json::to_value(&segment.origin).unwrap_or_default(),
                    "tier": segment.tier,
                    "dv": segment.dv.map(|dv| serde_json::json!({
                        "generation": dv.generation,
                        "cardinality": dv.cardinality,
                        "covered_seq_no": dv.covered_seq_no,
                    })),
                    "index": segment.index.map(|index| serde_json::json!({
                        "unit": index.unit.to_string(),
                        "file_len": index.file_len,
                        "footer_crc": index.footer_crc,
                    })),
                    "vectors": segment.vectors.iter().map(|vector| serde_json::json!({
                        "field_id": vector.field_id,
                        "has_graph": vector.has_graph,
                        "has_sq8": vector.has_sq8,
                        "non_null": vector.non_null,
                    })).collect::<Vec<_>>(),
                    "zones": segment.zones.iter().map(|zone| serde_json::json!({
                        "field_id": zone.field_id,
                        "null_count": zone.null_count,
                        "distinct_estimate": zone.distinct_estimate,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "format_version": self.format_version,
            "collection_id": self.collection_id,
            "generation": self.generation,
            "epoch": self.epoch,
            "checkpoint_seq_no": self.checkpoint_seq_no,
            "schema_version": self.schema.schema_version(),
            "next_unit_id": self.next_unit_id,
            "next_dv_gen": self.next_dv_gen,
            "totals": {
                "rows": self.totals.rows,
                "deleted_rows": self.totals.deleted_rows,
                "segment_bytes": self.totals.segment_bytes,
            },
            "segments": segments,
        })
    }

    /// Encode as a manifest file: header, then payload.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let payload = postcard::to_allocvec(self).map_err(|error| {
            LogPoseError::internal(format!("failed to encode manifest: {error}"))
        })?;
        let mut bytes = Vec::with_capacity(MANIFEST_HEADER_LEN + payload.len());
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        let header_crc = crc32c::crc32c(&bytes[..28]);
        bytes.extend_from_slice(&header_crc.to_le_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    /// Decode and verify a manifest file read from `path`.
    pub(crate) fn decode(bytes: &[u8], path: &Path) -> Result<Self> {
        let corrupt = |message: String| manifest_corrupt(path, message);
        if bytes.len() < MANIFEST_HEADER_LEN {
            return Err(corrupt(format!(
                "manifest file is {} bytes, shorter than its {MANIFEST_HEADER_LEN}-byte header",
                bytes.len()
            )));
        }
        if &bytes[..8] != MANIFEST_MAGIC {
            return Err(corrupt("manifest file has a bad magic".to_owned()));
        }
        let header_crc = le_u32(&bytes[28..32]);
        if crc32c::crc32c(&bytes[..28]) != header_crc {
            return Err(corrupt("manifest header checksum mismatch".to_owned()));
        }
        let generation = le_u64(&bytes[8..16]);
        let payload_len = le_u64(&bytes[16..24]);
        let payload_crc = le_u32(&bytes[24..28]);
        let payload = &bytes[MANIFEST_HEADER_LEN..];
        if payload_len > MAX_MANIFEST_PAYLOAD || payload.len() as u64 != payload_len {
            return Err(corrupt(format!(
                "manifest declares a {payload_len}-byte payload but holds {} bytes",
                payload.len()
            )));
        }
        if crc32c::crc32c(payload) != payload_crc {
            return Err(corrupt("manifest payload checksum mismatch".to_owned()));
        }
        let manifest = postcard::from_bytes::<Self>(payload)
            .map_err(|error| corrupt(format!("manifest payload does not decode: {error}")))?;
        if manifest.format_version != MANIFEST_FORMAT_VERSION {
            return Err(corrupt(format!(
                "unsupported manifest format version {}",
                manifest.format_version
            )));
        }
        if manifest.generation != generation {
            return Err(corrupt(format!(
                "manifest header names generation {generation} but the payload holds {}",
                manifest.generation
            )));
        }
        if manifest
            .segments
            .windows(2)
            .any(|pair| pair[0].unit >= pair[1].unit)
            || manifest
                .segments
                .iter()
                .any(|segment| segment.unit.0 >= manifest.next_unit_id)
        {
            return Err(corrupt(
                "manifest segments are not ascending below next_unit_id".to_owned(),
            ));
        }
        Ok(manifest)
    }
}

fn le_u64(bytes: &[u8]) -> u64 {
    let mut array = [0; 8];
    array.copy_from_slice(bytes);
    u64::from_le_bytes(array)
}

fn le_u32(bytes: &[u8]) -> u32 {
    let mut array = [0; 4];
    array.copy_from_slice(bytes);
    u32::from_le_bytes(array)
}

pub(crate) fn manifest_corrupt(path: &Path, message: String) -> LogPoseError {
    LogPoseError::Corrupt {
        kind: CorruptionKind::Manifest,
        location: Some(path.display().to_string()),
        message,
    }
}

/// `manifests/` of the collection in `dir`.
pub(crate) fn manifests_dir(dir: &Path) -> PathBuf {
    dir.join(MANIFESTS_DIR)
}

/// The file of manifest `generation` of the collection in `dir`.
pub(crate) fn manifest_path(dir: &Path, generation: u64) -> PathBuf {
    manifests_dir(dir).join(manifest_file_name(generation))
}

/// `<generation:020>.mf`.
pub(crate) fn manifest_file_name(generation: u64) -> String {
    format!("{generation:020}{MANIFEST_EXTENSION}")
}

/// The generation a manifest file name encodes, if it is one.
pub(crate) fn parse_manifest_file_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(MANIFEST_EXTENSION)?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Read the generation `CURRENT` names in the collection directory `dir`.
pub(crate) fn read_current(vfs: &dyn Vfs, dir: &Path) -> Result<u64> {
    let path = dir.join(CURRENT_FILE);
    let bytes = read_file(vfs, &path, "failed to read CURRENT")?;
    let digits = bytes
        .strip_suffix(b"\n")
        .filter(|_| bytes.len() == CURRENT_LEN)
        .filter(|digits| digits.iter().all(u8::is_ascii_digit))
        .ok_or_else(|| {
            manifest_corrupt(
                &path,
                "CURRENT is not a 21-byte manifest v2 pointer (a version 1 layout is not \
                 supported)"
                    .to_owned(),
            )
        })?;
    std::str::from_utf8(digits)
        .ok()
        .and_then(|digits| digits.parse().ok())
        .ok_or_else(|| manifest_corrupt(&path, "CURRENT does not parse".to_owned()))
}

/// Load and verify manifest `generation` of the collection in `dir`.
pub(crate) fn load_manifest(vfs: &dyn Vfs, dir: &Path, generation: u64) -> Result<Manifest> {
    let path = manifest_path(dir, generation);
    let bytes = logpose_vfs::read_file(vfs, &path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            manifest_corrupt(
                &path,
                format!("manifest generation {generation} named by CURRENT does not exist"),
            )
        } else {
            LogPoseError::io(
                format!("failed to read manifest '{}'", path.display()),
                error,
            )
        }
    })?;
    Manifest::decode(&bytes, &path)
}

/// Why a manifest publish failed, and whether the durable `CURRENT` may have changed.
#[derive(Debug)]
pub(crate) struct ManifestPublishError {
    /// What failed.
    pub(crate) error: LogPoseError,
    /// Whether the failure came at or after the rename of `CURRENT`, so that the durable
    /// `CURRENT` may name either generation. The writer poisons the collection then.
    pub(crate) current_unknown: bool,
}

/// Durably write `manifest` into the collection directory `dir` and point `CURRENT` at it.
///
/// Precondition: every file the manifest references is synced, and so are the directories
/// they were created in. The protocol:
///
/// 1. `manifests/<g>.mf` with `CreateNew`, append, `sync_all` (`ManifestAfterFileSync`).
/// 2. `sync_dir(manifests/)` (`ManifestAfterDirSync`).
/// 3. Remove a stale `CURRENT.tmp`; write `CURRENT.tmp` with `CreateNew`, `sync_all`
///    (`CurrentAfterTempSync`).
/// 4. Rename `CURRENT.tmp` to `CURRENT` (`CurrentAfterRename`).
/// 5. `sync_dir(dir)` (`CurrentAfterDirSync`): the commit point.
///
/// A failure in steps 1 to 3 leaves the durable `CURRENT` unchanged; the caller abandons the
/// commit, burns the generation, and collects the partial `<g>.mf`. A failure in step 4 or 5
/// leaves it unknown ([`ManifestPublishError::current_unknown`]).
pub(crate) fn publish_manifest(
    vfs: &dyn Vfs,
    dir: &Path,
    manifest: &Manifest,
) -> std::result::Result<(), ManifestPublishError> {
    let unchanged = |error| ManifestPublishError {
        error,
        current_unknown: false,
    };
    let unknown = |error| ManifestPublishError {
        error,
        current_unknown: true,
    };
    let bytes = manifest.encode().map_err(unchanged)?;
    let manifest_file = manifest_path(dir, manifest.generation);
    write_new_synced(vfs, &manifest_file, &bytes).map_err(unchanged)?;
    crash_point(vfs, Some(CrashPoint::ManifestAfterFileSync)).map_err(unchanged)?;
    let manifests = manifests_dir(dir);
    vfs.sync_dir(&manifests)
        .map_err(|error| unchanged(io_error("failed to sync", &manifests, error)))?;
    crash_point(vfs, Some(CrashPoint::ManifestAfterDirSync)).map_err(unchanged)?;

    let temp = dir.join(CURRENT_TEMP_FILE);
    match vfs.remove_file(&temp) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(unchanged(io_error("failed to remove stale", &temp, error)));
        }
    }
    let pointer = format!("{:020}\n", manifest.generation);
    write_new_synced(vfs, &temp, pointer.as_bytes()).map_err(unchanged)?;
    crash_point(vfs, Some(CrashPoint::CurrentAfterTempSync)).map_err(unchanged)?;

    let current = dir.join(CURRENT_FILE);
    vfs.rename(&temp, &current)
        .map_err(|error| unknown(io_error("failed to rename into place", &current, error)))?;
    crash_point(vfs, Some(CrashPoint::CurrentAfterRename)).map_err(unknown)?;
    vfs.sync_dir(dir)
        .map_err(|error| unknown(io_error("failed to sync", dir, error)))?;
    crash_point(vfs, Some(CrashPoint::CurrentAfterDirSync)).map_err(unknown)
}

/// Create `path` (which must not exist), append `bytes`, and `sync_all`.
fn write_new_synced(vfs: &dyn Vfs, path: &Path, bytes: &[u8]) -> Result<()> {
    let file = vfs
        .open(path, OpenMode::CreateNew)
        .map_err(|error| io_error("failed to create", path, error))?;
    file.append(&[IoSlice::new(bytes)])
        .map_err(|error| io_error("failed to write", path, error))?;
    file.sync_all()
        .map_err(|error| io_error("failed to sync", path, error))
}

fn io_error(what: &str, path: &Path, error: std::io::Error) -> LogPoseError {
    LogPoseError::io(format!("{what} '{}'", path.display()), error)
}

#[cfg(test)]
mod tests;
