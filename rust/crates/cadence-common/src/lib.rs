//! Shared utilities for the Cadence Rust port.
//!
//! - [`backoff`] — exponential retry policy (mirrors `go/common/backoff`).
//! - [`cron`] — cron schedules and next-run backoff
//!   (`go/common/backoff/cron.go`, `github.com/robfig/cron/v3`).
//! - [`jitter`] — bounded random jitter (mirrors `go/common/backoff`).
//! - [`retry`] — retry-with-backoff driver.
//! - [`semaphore`] — semaphore owner-ID codec (`go/common/semaphore`).

pub mod backoff;
pub mod cron;
pub mod jitter;
pub mod retry;
pub mod semaphore;

pub use backoff::{ExponentialRetryPolicy, Retrier};
