//! Authentication and authorization foundations.

/// Access model scaffold used to grow operator and service permissions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessTier {
    /// Full cluster administration privileges.
    Operator,
    /// Standard application workload privileges.
    Service,
    /// Read-only observability and metadata access.
    Observer,
}
