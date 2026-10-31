//! Cadence Worker service and the system-workflow contracts it serves.
//!
//! Go workers consume workflow histories with the Cadence SDK's deterministic
//! replay engine. Until that replay engine and each activity's dependencies
//! are ported, this crate deliberately exposes the stable wire contracts but
//! does not poll and discard production tasks.

pub mod archiver;
pub mod asyncworkflow;
pub mod batcher;
pub mod client;
pub mod config;
pub mod diagnostics;
pub mod domaindeprecation;
pub mod esanalyzer;
pub mod failovermanager;
pub mod indexer;
pub mod launcher;
pub mod parentclosepolicy;
pub mod replicator;
pub mod runtime;
pub mod scanner;
pub mod scheduler;
pub mod service;
pub mod workercommon;

/// Service name as used by the shared Cadence CLI.
pub const SERVICE_NAME: &str = "worker";
