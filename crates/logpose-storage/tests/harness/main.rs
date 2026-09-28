//! Harness v2: the storage engine checked against a model, as the engine design's "Testing
//! Strategy" describes.
//!
//! - [`random`]: randomized model checking over the full action table ([`actions`]), in every
//!   maintenance mode (hand-stepped jobs, background jobs stepped through the paused
//!   scheduler, free-running background jobs) and on both `FaultVfs` and the real filesystem.
//! - [`crash`]: exhaustive crash enumeration of the design's scenarios under every tear mode
//!   (I8), readers' observed states against the recovered one (I14), and recovery crashed at
//!   every operation against a clean recovery of the same disk image (I11).
//! - [`interleave`]: every position of each job phase relative to a short write sequence
//!   (deletion-vector reconciliation, flush during compaction).
//! - [`stress`]: concurrent writers and invariant-checking readers beside background
//!   maintenance, time-bounded.
//!
//! Every run is seeded. See each module for the environment variables that size it.

use arc_swap as _;
use async_trait as _;
use bytemuck as _;
use crc32c as _;
use imbl as _;
use logpose_auth as _;
use logpose_catalog as _;
use logpose_index as _;
use postcard as _;
use rayon as _;
use roaring as _;
use serde as _;
use thiserror as _;
use tracing as _;
use twox_hash as _;
use uuid as _;

mod actions;
mod crash;
mod generate;
mod interleave;
mod model;
mod random;
mod session;
mod stress;
