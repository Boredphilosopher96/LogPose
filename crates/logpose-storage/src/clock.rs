//! The engine-wide clock: monotonic time for snapshot-token expiry, injectable so tests can
//! advance it deterministically.

use std::{
    fmt,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

/// A monotonic clock. [`Clock::now`] is the time since an arbitrary fixed origin and never goes
/// backwards.
pub trait Clock: Send + Sync + fmt::Debug {
    /// The current time since the clock's origin.
    fn now(&self) -> Duration;
}

/// The real monotonic clock, measured from its creation.
#[derive(Debug)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    /// A clock whose origin is now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// A clock that moves only when [`ManualClock::advance`] is called. For tests.
#[derive(Debug, Default)]
pub struct ManualClock {
    now: Mutex<Duration>,
}

impl ManualClock {
    /// A clock at time zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Move the clock forward by `by`.
    pub fn advance(&self, by: Duration) {
        let mut now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        *now = now.saturating_add(by);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_moves_only_when_advanced() {
        let clock = ManualClock::new();
        assert_eq!(clock.now(), Duration::ZERO);
        clock.advance(Duration::from_secs(3));
        clock.advance(Duration::from_millis(500));
        assert_eq!(clock.now(), Duration::from_millis(3500));
    }

    #[test]
    fn system_clock_never_goes_backwards() {
        let clock = SystemClock::new();
        let first = clock.now();
        assert!(clock.now() >= first);
    }
}
