//! Shared utilities for the Cadence Rust port.
//!
//! - [`backoff`] — exponential retry policy (mirrors `go/common/backoff`).
//! - [`cron`] — cron schedules and next-run backoff
//!   (`go/common/backoff/cron.go`, `github.com/robfig/cron/v3`).
//! - [`jitter`] — bounded random jitter (mirrors `go/common/backoff`).
//! - [`retry`] — retry-with-backoff driver.
//! - [`semaphore`] — semaphore owner-ID codec (`go/common/semaphore`).

pub mod backoff;
pub mod cluster;
pub mod cron;
pub mod duration;
pub mod go_string;
pub mod jitter;
pub mod retry;
pub mod semaphore;
pub mod semaphore_bytes;

pub use backoff::{ExponentialRetryPolicy, Retrier};

pub mod ack_manager;
pub mod task_isolation;
pub mod task_list;

pub mod poller;
pub mod poller_bytes;

pub mod liveness;

pub mod matching_rate;
pub mod qps;
pub mod qps_bytes;
pub mod rate_limiter;

pub mod membership;
pub mod ringpop_bootstrap_selection;
pub mod ringpop_disseminator;
pub mod ringpop_member;
pub mod ringpop_memberlist;
pub mod ringpop_state_transitions;
pub mod ringpop_wire;
