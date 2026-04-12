//! Client-side request and connection scaffolding.

use serde::{Deserialize, Serialize};

/// Client connection settings shared across tools and SDKs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    /// REST endpoint base URL.
    pub endpoint: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8080".to_owned(),
        }
    }
}
