//! Storage engine abstractions.

use async_trait::async_trait;

/// Durable storage surface for future engine implementations.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Return a short identifier for the engine implementation.
    async fn engine_name(&self) -> &'static str;
}

/// Default scaffold storage engine used for bootstrap.
pub struct MemoryStorage;

#[async_trait]
impl StorageEngine for MemoryStorage {
    async fn engine_name(&self) -> &'static str {
        "memory"
    }
}
