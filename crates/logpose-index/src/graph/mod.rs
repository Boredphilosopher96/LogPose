//! Row-id HNSW graph for the v2 vector engine.
//!
//! The graph stores adjacency only. Nodes are dense `u32` row ids `0..n`, and
//! every distance comes from a caller-supplied source:
//!
//! - [`VectorSource`] answers row-to-row distances while building, so f32
//!   vectors, SQ8 codes or any future codec can drive construction.
//! - [`QueryDistance`] answers query-to-row distances while searching.
//!
//! Lower distance always means closer. [`F32Vectors`] is a plain row-major
//! implementation for tests and small tools.
//!
//! # Build
//!
//! [`HnswGraph::build`] inserts rows sequentially and is deterministic for a
//! given [`HnswParams::seed`]. [`HnswGraph::build_parallel`] uses `rayon` with
//! lock-free atomic link reads and a write mutex per node, in safe Rust. Both
//! use the neighbor-selection heuristic of Malkov and
//! Yashunin (algorithm 4, with `keepPrunedConnections`), `M` links on upper
//! layers, `2M` on layer 0, and levels drawn with `mL = 1 / ln(M)` from a
//! counter-based RNG keyed by `(seed, row)`, with no artificial level cap.
//! [`HnswGraph::insert`] appends one row incrementally.
//!
//! # Search
//!
//! Searches run on a reusable [`SearchScratch`] (binary-heap queues and a
//! generation-stamped visited array, so steady-state queries do not allocate
//! scratch memory). A [`SearchCursor`] keeps the frontier, the rows it pruned
//! and the visited set, so a search can be extended to a larger `ef` without
//! restarting. [`SearchStatus`] separates "fewer than `k` so far" from
//! "nothing left to explore".
//!
//! Filtered search takes a [`RowFilter`] and a [`FilterStrategy`]:
//! [`FilterStrategy::Admit`] walks the whole graph and admits only matching
//! rows into the results, and [`FilterStrategy::Acorn`] walks only matching
//! rows, bridging gaps through two-hop neighborhoods (ACORN-1 style). Exact
//! brute force over a filter is the caller's job.
//!
//! # Persistence
//!
//! [`HnswGraph::to_bytes`] and [`HnswGraph::from_bytes`] use a compact,
//! versioned, CRC32-protected CSR layout.

mod build;
mod distance;
mod error;
mod filter;
mod hnsw;
mod search;
mod serialize;

pub use distance::{F32Metric, F32Query, F32Vectors, QueryDistance, VectorSource};
pub use error::GraphError;
pub use filter::{AllRows, FilterStrategy, RowBitset, RowFilter};
pub use hnsw::{DEFAULT_EF_CONSTRUCTION, DEFAULT_M, HnswGraph, HnswParams, MAX_M};
pub use search::{Neighbor, SearchCursor, SearchOutput, SearchScratch, SearchStats, SearchStatus};
