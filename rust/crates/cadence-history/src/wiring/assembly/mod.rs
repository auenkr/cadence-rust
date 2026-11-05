//! History service assembly (unit D1): shared environment, shard factory,
//! per-shard engine factory and the shard registry. Other units add
//! `handler`, `queues` and `replication` next to these modules.
pub mod controller;
pub mod engine_cache;
pub mod engine_close;
pub mod engine_decision;
pub mod engine_events;
pub mod engine_factory;
pub mod engine_state;
pub mod engine_tasks;
pub mod env;
pub mod shard_factory;
