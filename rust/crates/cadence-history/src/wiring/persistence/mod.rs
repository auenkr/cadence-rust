//! SQL-backed adapters for History persistence traits (one file per group).
#![allow(clippy::result_large_err)]
pub mod shard_events;
pub mod engine_ndc;
pub mod queues;
pub mod replication;
pub mod domain_dlq;
pub mod visibility;
pub mod es_visibility;
