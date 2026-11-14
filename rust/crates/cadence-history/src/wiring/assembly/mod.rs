//! History service assembly: shared environment, shard factory, per-shard
//! engine factory, shard registry, queues, replication and failover.
pub mod active_cluster_manager;
pub mod archiver_client;
pub mod archiver_wiring;
pub mod visibility_wiring;
pub mod controller;
pub mod engine_cache;
pub mod engine_checksum;
pub mod engine_close;
pub mod engine_failover;
pub mod engine_continue;
pub mod engine_decision;
pub mod engine_decision_ops;
pub mod engine_events;
pub mod engine_events_transfer;
pub mod engine_factory;
pub mod engine_start;
pub mod engine_refresher;
pub mod engine_resetter;
pub mod engine_semaphore;
pub mod engine_state;
pub mod engine_tasks;
pub mod engine_timer;
pub mod env;
pub mod failover;
pub mod failover_client;
pub mod ndc_adapters;
pub mod parent_close_policy_client;
#[cfg(test)]
pub mod real_engine_harness;
#[cfg(test)]
mod real_engine_tests;
#[cfg(test)]
mod real_engine_v2_tests;
pub mod ndc_context_hooks;
pub mod ndc_rebuild;
pub mod ndc_state;
pub mod ndc_workflow;
pub mod queue_deps;
pub mod queue_deps_standby;
pub mod queue_deps_timer_active;
pub mod queue_deps_transfer_active;
pub mod queue_exec;
pub mod queue_factory;
pub mod queue_policy;
pub mod queue_provider;
pub mod queue_allocator_adapters;
pub mod queue_state_string;
pub mod queues;
pub mod replication;
pub mod replication_builders;
pub mod resender;
pub mod shard_adapters;
pub mod shard_factory;
