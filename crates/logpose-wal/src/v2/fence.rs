//! The `FSYNC_FAILED` fence marker.
//!
//! When a WAL group's append or sync fails and the rollback that should make the group
//! definitely absent fails too, the page cache may hold frames that never reached the device,
//! and Linux may report a later fsync of that file as successful without writing them. The
//! writer then records the current boot id in `wal/FSYNC_FAILED`. Recovery checks the marker
//! before anything else: a marker from the current boot refuses the open, because the page
//! cache is still the one that lied; a marker from an earlier boot means the page cache was
//! cleared by the reboot, so the on-disk bytes are the truth and recovery proceeds and removes
//! the marker.

use super::WalError;
use logpose_types::SeqNo;
use logpose_vfs::{OpenMode, Vfs, parent_dir, read_file};
use std::{
    fmt, io,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

/// File name of the fence marker inside the WAL directory.
pub const FENCE_FILE_NAME: &str = "FSYNC_FAILED";
const FENCE_TEMP_NAME: &str = "FSYNC_FAILED.tmp";
const FENCE_HEADER: &str = "logpose wal fence v1";
/// Last line of a marker, so that a truncated marker never parses.
const FENCE_END: &str = "end";
/// Markers are a few dozen bytes; anything larger is not a marker this build wrote.
const MAX_FENCE_BYTES: u64 = 4096;
const LINUX_BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// Identity of one boot of the host.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BootId(String);

impl BootId {
    /// A boot id with the given value. Tests use distinct values to simulate reboots.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The current boot id: `/proc/sys/kernel/random/boot_id` on Linux. Where that is not
    /// readable, a value derived from this process's id and start time, which is stable for the
    /// life of the process (so a fence from this process is always recognized) and differs from
    /// every later process.
    ///
    /// This reads the kernel's pseudo-file directly rather than through a
    /// [`Vfs`](logpose_vfs::Vfs): it is host identity, not engine state.
    #[must_use]
    pub fn current() -> Self {
        static CURRENT: OnceLock<BootId> = OnceLock::new();
        CURRENT
            .get_or_init(|| {
                let from_kernel = std::fs::read_to_string(LINUX_BOOT_ID_PATH)
                    .ok()
                    .map(|value| value.trim().to_owned())
                    .filter(|value| is_marker_token(value));
                Self(from_kernel.unwrap_or_else(|| {
                    let started = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |elapsed| elapsed.as_nanos());
                    format!("process-{}-{started}", std::process::id())
                }))
            })
            .clone()
    }

    /// The boot id as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BootId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Contents of an `FSYNC_FAILED` marker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FenceMarker {
    /// Boot in which the failure happened.
    pub boot_id: BootId,
    /// First sequence number of the group whose rollback failed.
    pub first_seq_no: SeqNo,
    /// Last sequence number of the group whose rollback failed.
    pub last_seq_no: SeqNo,
}

impl FenceMarker {
    fn encode(&self) -> String {
        format!(
            "{FENCE_HEADER}\nboot_id={}\nfirst_seq_no={}\nlast_seq_no={}\n{FENCE_END}\n",
            self.boot_id, self.first_seq_no, self.last_seq_no
        )
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(bytes).map_err(|_| "marker is not UTF-8".to_owned())?;
        let mut lines = text.lines();
        if lines.next() != Some(FENCE_HEADER) {
            return Err("missing marker header".to_owned());
        }
        let mut field = |name: &str| -> Result<String, String> {
            lines
                .next()
                .and_then(|line| line.strip_prefix(name)?.strip_prefix('='))
                .map(str::to_owned)
                .ok_or_else(|| format!("missing field '{name}'"))
        };
        let boot_id = field("boot_id")?;
        if !is_marker_token(&boot_id) {
            return Err("malformed boot id".to_owned());
        }
        let first_seq_no = field("first_seq_no")?
            .parse()
            .map_err(|_| "malformed first_seq_no".to_owned())?;
        let last_seq_no = field("last_seq_no")?
            .parse()
            .map_err(|_| "malformed last_seq_no".to_owned())?;
        if lines.next() != Some(FENCE_END) || lines.next().is_some() {
            return Err("missing end line or trailing data".to_owned());
        }
        Ok(Self {
            boot_id: BootId(boot_id),
            first_seq_no,
            last_seq_no,
        })
    }
}

/// A boot id must be one non-empty line so the marker stays parseable.
fn is_marker_token(value: &str) -> bool {
    !value.is_empty() && !value.contains(['\n', '\r'])
}

/// Read the fence marker in `dir`, if there is one.
///
/// A marker that cannot be parsed (for example one torn by a crash while it was written) is
/// reported as [`WalError::FenceUnreadable`], which callers must treat like a marker from the
/// current boot.
pub fn read_fence(vfs: &dyn Vfs, dir: &Path) -> Result<Option<FenceMarker>, WalError> {
    let listing = super::files::list_dir(vfs, dir)?;
    match listing.fence_len {
        None => Ok(None),
        Some(len) => read_marker(vfs, dir, len).map(Some),
    }
}

pub(super) fn read_marker(vfs: &dyn Vfs, dir: &Path, len: u64) -> Result<FenceMarker, WalError> {
    let marker = dir.join(FENCE_FILE_NAME);
    if len > MAX_FENCE_BYTES {
        return Err(WalError::FenceUnreadable {
            marker,
            reason: format!("marker is {len} bytes, larger than any marker this build writes"),
        });
    }
    let bytes = read_file(vfs, &marker)
        .map_err(|error| WalError::io("failed to read WAL fence marker", &marker, error))?;
    FenceMarker::decode(&bytes).map_err(|reason| WalError::FenceUnreadable { marker, reason })
}

/// Durably write the fence marker: write a temp file, sync it, rename it over the marker and
/// sync the directory. Any existing marker is replaced.
pub(super) fn write_fence(vfs: &dyn Vfs, dir: &Path, marker: &FenceMarker) -> io::Result<()> {
    let temp = dir.join(FENCE_TEMP_NAME);
    match vfs.remove_file(&temp) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let file = vfs.open(&temp, OpenMode::CreateNew)?;
    file.append(&[io::IoSlice::new(marker.encode().as_bytes())])?;
    file.sync_all()?;
    vfs.rename(&temp, &dir.join(FENCE_FILE_NAME))?;
    vfs.sync_dir(dir)
}

/// Remove the fence marker in `dir` durably. Returns whether a marker existed.
///
/// This is the operator's acknowledgement that the device was checked after a failed WAL fsync
/// (log it). Recovery also calls it for a marker left by an earlier boot.
pub fn clear_fence(vfs: &dyn Vfs, dir: &Path) -> Result<bool, WalError> {
    let marker = dir.join(FENCE_FILE_NAME);
    match vfs.remove_file(&marker) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(WalError::io(
                "failed to remove WAL fence marker",
                marker,
                error,
            ));
        }
    }
    vfs.sync_dir(parent_dir(&marker))
        .map_err(|error| WalError::io("failed to sync WAL directory", dir, error))?;
    Ok(true)
}

/// Path of the fence marker in `dir`.
pub(super) fn fence_path(dir: &Path) -> PathBuf {
    dir.join(FENCE_FILE_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_vfs::FaultVfs;

    #[test]
    fn marker_round_trips_and_rejects_damage() {
        let marker = FenceMarker {
            boot_id: BootId::new("7f1c-boot"),
            first_seq_no: 10,
            last_seq_no: 12,
        };
        let encoded = marker.encode();
        assert_eq!(FenceMarker::decode(encoded.as_bytes()), Ok(marker));
        for cut in 0..encoded.len() - 1 {
            assert!(
                FenceMarker::decode(&encoded.as_bytes()[..cut]).is_err(),
                "a marker cut at {cut} bytes decoded"
            );
        }
    }

    #[test]
    fn write_read_and_clear_a_marker() -> Result<(), Box<dyn std::error::Error>> {
        let vfs = FaultVfs::new(1);
        let dir = Path::new("/wal");
        vfs.create_dir_all(dir)?;
        vfs.sync_dir(Path::new("/"))?;
        assert_eq!(read_fence(vfs.as_ref(), dir)?, None);
        let marker = FenceMarker {
            boot_id: BootId::new("a"),
            first_seq_no: 1,
            last_seq_no: 3,
        };
        write_fence(vfs.as_ref(), dir, &marker)?;
        // A second failure replaces the marker instead of failing on the existing file.
        let newer = FenceMarker {
            boot_id: BootId::new("b"),
            first_seq_no: 4,
            last_seq_no: 4,
        };
        write_fence(vfs.as_ref(), dir, &newer)?;
        vfs.crash();
        assert_eq!(read_fence(vfs.as_ref(), dir)?, Some(newer));
        assert!(clear_fence(vfs.as_ref(), dir)?);
        vfs.crash();
        assert_eq!(read_fence(vfs.as_ref(), dir)?, None);
        assert!(!clear_fence(vfs.as_ref(), dir)?);
        Ok(())
    }

    #[test]
    fn current_boot_id_is_stable_within_a_process() {
        let first = BootId::current();
        assert!(!first.as_str().is_empty());
        assert_eq!(first, BootId::current());
    }
}
