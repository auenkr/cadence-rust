//! History service assembly: shared environment, shard factory, per-shard
//! engine factory, shard registry, queues, replication and failover.
pub mod controller;
pub mod engine_cache;
pub mod engine_close;
pub mod engine_decision;
pub mod engine_events;
pub mod engine_factory;
pub mod engine_start;
pub mod engine_state;
pub mod engine_tasks;
pub mod env;
pub mod failover;
pub mod queue_exec;
pub mod queues;
pub mod replication;
pub mod replication_builders;
pub mod resender;
pub mod shard_adapters;
pub mod shard_factory;
