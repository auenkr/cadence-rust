//! Port of `go/service/history/replication`.
//!
//! One Rust module per Go file. Collaborators owned by modules that are being
//! ported in parallel (shard context, engine, execution cache, persistence
//! managers, ndc history resender, client bean, metrics) are expressed as small
//! traits in [`deps`].
#![allow(clippy::result_large_err)]
pub mod deps;
pub mod dlq_handler;
pub mod dynamic_task_batch_sizer;
pub mod metrics_emitter;
pub mod task_ack_manager;
pub mod task_executor;
pub mod task_fetcher;
pub mod task_hydrator;
pub mod task_processor;
pub mod task_reader;
pub mod task_store;

pub use deps::ReplicationError;
pub use dlq_handler::{DlqHandler, DlqHandlerImpl};
pub use dynamic_task_batch_sizer::{DynamicTaskBatchSizer, DynamicTaskBatchSizerImpl};
pub use metrics_emitter::MetricsEmitterImpl;
pub use task_ack_manager::TaskAckManager;
pub use task_executor::{TaskExecutor, TaskExecutorImpl};
pub use task_fetcher::{TaskFetcher, TaskFetchers, TaskFetchersImpl};
pub use task_hydrator::TaskHydrator;
pub use task_processor::{TaskProcessor, TaskProcessorImpl};
pub use task_reader::TaskReader;
pub use task_store::TaskStore;

use std::time::Duration;

/// `replicationTimeout` (task_processor.go).
pub const REPLICATION_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod batch_sizer_tests;
#[cfg(test)]
mod reader_tests;
#[cfg(test)]
mod dlq_tests;
#[cfg(test)]
mod ack_tests;
#[cfg(test)]
mod fetcher_tests;
