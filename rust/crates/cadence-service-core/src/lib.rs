//! Shared building blocks of every Cadence service: configuration, transports,
//! metrics sinks, lifecycle helpers and the legacy in-process `AppState`.
pub mod cli;
pub mod admin;
pub mod config;
pub mod grpc_tls;
#[allow(clippy::result_large_err)]
pub mod grpc_shard;
pub mod handlers;
pub mod frontend_lifecycle;
pub mod ringpop_discovery;
pub mod ringpop_tchannel;
pub mod service;
pub mod tchannel_json_client;
pub mod yarpc;
pub mod matching_metrics;
mod matching_metrics_rollup;
pub mod matching_metrics_prometheus;
pub mod matching_metrics_statsd;
pub mod matching_metrics_m3;
pub mod matching_metrics_runtime;
pub mod matching_metrics_migration;
pub mod matching_quota;
pub mod openfeature;
