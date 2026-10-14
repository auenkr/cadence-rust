//! Shared utilities for the Cadence Rust port.
//!
//! - [`backoff`] — exponential retry policy (mirrors `go/common/backoff`).
//! - [`jitter`] — bounded random jitter (mirrors `go/common/backoff`).
//! - [`retry`] — retry-with-backoff driver.

pub mod backoff;
pub mod jitter;
pub mod retry;

pub use backoff::{ExponentialRetryPolicy, Retrier};
