//! Telemetry bootstrap for services and tools.

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize workspace logging and tracing.
pub fn init(log_filter: &str) {
    let env_filter = EnvFilter::try_new(log_filter)
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("fallback filter should be valid");

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().with_target(true))
        .try_init()
        .ok();
}
