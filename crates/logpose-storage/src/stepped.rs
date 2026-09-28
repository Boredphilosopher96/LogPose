//! Maintenance jobs stepped by hand: the scheduler's manual mode for job phases.
//!
//! [`MaintenanceScheduler::pause`](crate::MaintenanceScheduler::pause) and
//! [`step`](crate::MaintenanceScheduler::step) decide when background jobs get their permits;
//! a [`SteppedJob`] goes one level further and lets the caller place each phase of one job
//! (begin, build, commit) between its own writes, so a test can enumerate every position of
//! every phase relative to a short write sequence. The job runs exactly the code a background
//! job runs: the writer begins it (capturing the frozen memtable, the deletion-vector snapshots,
//! and the job's unit), [`SteppedJob::build`] writes its files on the calling thread, and
//! [`SteppedJob::commit`] hands the result to the writer, which reconciles deletions, publishes
//! the manifest, and installs the new `Version`.

use crate::{
    Engine,
    engine::CoreRef,
    handle::{CollectionHandle, JobTicket},
    version::Version,
    writer::{JobCommit, JobStart, JobWork},
};
use logpose_types::{LogPoseError, Result, Snapshot};
use std::{fmt, sync::Arc};

pub use crate::writer::JobKind;

/// A flush or compaction whose phases the caller runs one at a time.
///
/// Begun by [`Engine::begin_job`]. Dropping it (or [`abandon`](Self::abandon)) ends the job
/// without a change, as a failed background job ends: its unit is burned and any file it wrote is
/// removed. It holds a reference to the engine's state, so it must be dropped before the last
/// [`Engine`] clone is.
pub struct SteppedJob {
    kind: JobKind,
    handle: Arc<CollectionHandle>,
    /// The begin's capture; released once the build ran, as a background build releases it.
    start: Option<JobStart>,
    /// What the build produced.
    built: Option<JobCommit>,
    ticket: Option<JobTicket>,
    core: CoreRef,
}

impl fmt::Debug for SteppedJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SteppedJob")
            .field("kind", &self.kind)
            .field("collection", &self.handle.meta().reference)
            .field("has_work", &self.has_work())
            .field("built", &self.built.is_some())
            .finish()
    }
}

impl Engine {
    /// Begin a maintenance job of `kind` on `handle` without a scheduler permit, drained and
    /// captured by the writer exactly as a background job's begin is. A flush takes the oldest
    /// frozen memtable (freezing the active one if none is frozen) and waits for a flush already
    /// running; a compaction takes every unreserved segment. Blocking.
    ///
    /// # Errors
    ///
    /// The collection is poisoned, dropped, or its writer stopped.
    pub fn begin_job(&self, handle: &Arc<CollectionHandle>, kind: JobKind) -> Result<SteppedJob> {
        let core = self.core();
        let (ticket, start) = handle.begin_job(kind)?;
        Ok(SteppedJob {
            kind,
            handle: Arc::clone(handle),
            start: Some(start),
            built: None,
            ticket: Some(ticket),
            core,
        })
    }
}

impl SteppedJob {
    /// The job's kind.
    #[must_use]
    pub fn kind(&self) -> JobKind {
        self.kind
    }

    /// Whether the begin found anything to do: a flush with no frozen operation, or a
    /// compaction of fewer than two segments, has nothing to build and commits as a no-op.
    #[must_use]
    pub fn has_work(&self) -> bool {
        self.built.is_some()
            || self
                .start
                .as_ref()
                .is_some_and(|start| !matches!(start.work, JobWork::Nothing))
    }

    /// The published version the job began from, until the build released it.
    #[must_use]
    pub fn begin_version(&self) -> Option<&Arc<Version>> {
        self.start.as_ref().map(|start| &start.version)
    }

    /// Whether [`build`](Self::build) ran.
    #[must_use]
    pub fn is_built(&self) -> bool {
        self.built.is_some()
    }

    /// Build and write the job's files (the segment and DV files of a flush; the output segment
    /// of a compaction) on the calling thread, then release what the begin captured. Does
    /// nothing for a job without work, or when called twice. Blocking.
    ///
    /// # Errors
    ///
    /// The build's I/O and corruption errors. The job is then over: it can only be dropped.
    pub fn build(&mut self) -> Result<()> {
        if self.built.is_some() {
            return Ok(());
        }
        let Some(start) = self.start.take() else {
            return Err(LogPoseError::internal(
                "the stepped job's build already failed",
            ));
        };
        let Some(ticket) = self.ticket.as_mut() else {
            return Err(LogPoseError::internal("the stepped job already ended"));
        };
        let commit = match &start.work {
            JobWork::Nothing => {
                self.start = Some(start);
                return Ok(());
            }
            JobWork::Flush(work) => {
                self.core
                    .build_flush(&self.handle, &start.version, start.unit, work, ticket)
            }
            JobWork::Compact(work) => {
                self.core
                    .build_compaction(&self.handle, &start.version, start.unit, work, ticket)
            }
        };
        // A background build drops its captured inputs before it hands its result over, so the
        // last holder of a retired segment removes its file once the commit publishes.
        drop(start);
        self.built = Some(commit?);
        Ok(())
    }

    /// Build if not built yet, then have the writer commit the job and wait for it. A job
    /// without work ends without a change and returns the current snapshot. Blocking.
    ///
    /// # Errors
    ///
    /// The build's errors, and the commit's: a failed manifest publish before the `CURRENT`
    /// rename abandons the job; one at or after it poisons the collection.
    pub fn commit(mut self) -> Result<Snapshot> {
        self.build()?;
        let Some(ticket) = self.ticket.take() else {
            return Err(LogPoseError::internal("the stepped job already ended"));
        };
        match self.built.take() {
            Some(commit) => ticket.commit(commit),
            None => {
                drop(ticket);
                let version = self.handle.current();
                Ok(Snapshot {
                    manifest_generation: version.manifest_generation,
                    visible_seq_no: version.visible_seq_no,
                })
            }
        }
    }

    /// End the job without committing it; the same as dropping it.
    pub fn abandon(self) {}
}
