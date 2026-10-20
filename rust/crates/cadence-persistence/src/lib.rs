//! Persistence layer for the Cadence Rust port.
//!
//! Mirrors the `go/common/persistence` manager semantics. The default
//! implementation is in-memory so the Rust server runs standalone; durable
//! backends (SQL/NoSQL) implement the same traits in follow-ups.

pub mod domain_replication;
pub mod domain_update;
pub mod domains;
pub mod dynamic_config;
pub mod executions;
pub mod history_branch;
pub mod raw_history;
pub mod schedules;
pub mod semaphores;
pub mod version_history;
pub mod visibility;

pub use domains::{DomainRecord, FailoverError, InMemoryDomainStore};
pub use executions::{
    AsyncRequest, AsyncRequestKind, ExecutionRecord, HistoryPageError, InMemoryExecutionStore,
    MutableStateView, QueryError, RecordError, RecordedActivity, RecordedDecision, ResetError,
    StartError, StoredPartitionConfig, TaskError, WorkflowMutationError,
};
pub use schedules::{ScheduleError, ScheduleRecord};
pub use semaphores::{
    AcquireError, GrantOutcome, SemaphoreError, SemaphoreHold, SemaphoreMetadata, SemaphoreRow,
};

pub mod matching_blob;
pub mod matching_mysql;
pub mod matching_postgres;
pub mod matching_queue;
pub mod matching_sqlite;
pub mod matching_task_store;
pub mod matching_writer_actor;

pub mod matching_gc;

pub mod matching_domain_metadata;
pub mod matching_domain_mysql;
pub mod matching_retry;
