//! History service assembly: shared environment, shard factory, per-shard
//! engine factory, shard registry, queues, replication and failover.
pub mod controller;
pub mod engine_factory;
pub mod env;
pub mod failover;
pub mod queues;
pub mod replication;
pub mod shard_adapters;
pub mod shard_factory;
