//! Write-ahead log interfaces.

/// WAL policy scaffold for future durability strategies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalMode {
    /// Favor local development simplicity.
    Development,
    /// Favor strict durability defaults for production.
    Production,
}
