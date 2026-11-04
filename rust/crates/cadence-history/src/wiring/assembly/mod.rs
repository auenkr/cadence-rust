//! History service assembly (unit D1): shared environment, shard factory,
//! per-shard engine factory and the shard registry. Other units add
//! `handler`, `queues` and `replication` next to these modules.
pub mod controller;
pub mod engine_factory;
pub mod env;
pub mod shard_factory;
