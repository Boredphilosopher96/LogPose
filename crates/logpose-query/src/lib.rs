//! Query planning abstractions.

/// Query execution profile selected for a request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryProfile {
    /// Optimized for low latency.
    Latency,
    /// Optimized for balanced resource usage.
    Balanced,
    /// Optimized for high recall.
    Recall,
}
