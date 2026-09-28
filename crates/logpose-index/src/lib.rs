//! Vector and scalar indexes over segment row ids: SIMD distance kernels, SQ8 codes, the HNSW
//! graph, and scalar inverted and sorted indexes. This crate does no I/O: it builds structures
//! from rows and (de)serializes them to and from byte buffers that `logpose-storage` stores as
//! segment sections.

pub mod graph;
pub mod kernels;
pub mod scalar;
pub mod sq8;

#[cfg(test)]
use criterion as _;
