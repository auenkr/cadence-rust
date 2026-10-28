//! Cadence Matching service.
// Shared modules keep their historical `crate::<module>` paths.
use cadence_service_core::admin;
use cadence_service_core::config;
use cadence_service_core::grpc_shard;
use cadence_service_core::grpc_tls;
use cadence_service_core::handlers;
use cadence_service_core::matching_metrics;
use cadence_service_core::matching_metrics_runtime;
use cadence_service_core::matching_quota;
use cadence_service_core::ringpop_discovery;
use cadence_service_core::ringpop_tchannel;
use cadence_service_core::service;
use cadence_service_core::tchannel_json_client;
use cadence_service_core::yarpc;

pub mod launcher;
// Tonic service signatures require Result<_, Status>; boxing changes the API.
#[allow(clippy::result_large_err)]
pub mod grpc;
#[allow(clippy::result_large_err)]
pub mod grpc_admin;
#[allow(clippy::result_large_err)]
mod grpc_convert;
#[allow(clippy::result_large_err)]
mod grpc_decisions;
#[allow(clippy::result_large_err)]
mod grpc_history;
#[allow(clippy::result_large_err)]
pub mod grpc_internal;
#[allow(clippy::result_large_err)]
pub mod grpc_matching;
#[allow(clippy::result_large_err)]
pub mod matching_history;
#[allow(clippy::result_large_err)]
pub mod matching_history_tchannel;
pub mod ringpop_provider;
pub mod ringpop_runtime;
pub mod tchannel;
// The resolver carries tonic Status through matching placement call paths.
#[allow(clippy::result_large_err)]
pub mod matching_dispatch;
#[allow(clippy::result_large_err)]
pub mod matching_dispatch_native;
pub mod matching_forward;
#[allow(clippy::result_large_err)]
pub mod matching_forward_native;
pub mod matching_history_membership;
#[allow(clippy::result_large_err)]
pub mod matching_history_peer_resolver;
#[allow(clippy::result_large_err)]
pub mod matching_standby;
#[allow(clippy::result_large_err)]
pub mod matching_standby_native;
// Injected client faults are tonic Status, like the forwarding call sites.
mod matching_blocking;
pub mod matching_config;
pub mod matching_domain_cache;
pub mod matching_domain_uuid;
#[cfg(test)]
mod matching_dyncfg_defaults_tests;
pub mod matching_fatal;
#[allow(clippy::result_large_err)]
pub mod matching_fault_injection;
pub mod matching_handler;
#[allow(clippy::result_large_err)]
pub mod matching_handler_native;
pub mod matching_host_limiter;
pub mod matching_limiter;
#[allow(clippy::result_large_err)]
pub mod matching_membership;
pub mod matching_membership_lifecycle;
pub mod matching_native_status;
pub mod matching_panic;
#[allow(clippy::result_large_err)]
pub mod matching_partition;
pub mod matching_partition_native;
pub mod matching_peer_provider;
pub mod matching_persistence;
pub mod matching_persistence_limiter;
#[allow(clippy::result_large_err)]
pub mod matching_query;
#[allow(clippy::result_large_err)]
pub mod matching_query_native;
pub mod matching_reader;
pub mod matching_reader_native;
#[allow(clippy::result_large_err)]
pub mod matching_registry;
#[allow(clippy::result_large_err)]
pub mod matching_registry_native;
pub mod matching_ringpop_provider;
pub mod matching_runtime;
pub mod matching_scaler;
pub mod matching_semaphore;
#[allow(clippy::result_large_err)]
pub mod matching_shard_distributor;
#[allow(clippy::result_large_err)]
pub mod matching_shard_processor;
#[allow(clippy::result_large_err)]
pub mod matching_shard_routing;
pub mod matching_sync;
pub mod matching_sync_native;
pub mod matching_task_info_log;
pub mod matching_task_store;
pub mod matching_throttled_logger;
#[allow(clippy::result_large_err)]
pub mod matching_writer;
#[allow(clippy::result_large_err)]
pub mod matching_writer_native;
#[allow(clippy::result_large_err)]
pub mod thrift_matching;
#[allow(dead_code)] // Native adapters await native Matching handler integration.
pub mod thrift_matching_native;
#[allow(dead_code)] // Native request adapters await shared handler integration.
pub mod thrift_matching_native_extra;
#[allow(dead_code)] // Native response adapters await shared handler integration.
pub mod thrift_matching_native_responses;
