//! Shared building blocks of every Cadence service: configuration, transports,
//! metrics sinks, lifecycle helpers and the legacy in-process `AppState`.
pub mod admin;
pub mod admin_replication;
pub mod cli;
pub mod config;
pub mod es_mapping;
pub mod frontend_lifecycle;
pub mod frontend_remote;
#[allow(clippy::result_large_err)]
pub mod grpc_shard;
pub mod grpc_tls;
pub mod handlers;
pub mod matching_metrics;
pub mod matching_metrics_m3;
pub mod matching_metrics_migration;
pub mod matching_metrics_prometheus;
mod matching_metrics_rollup;
pub mod matching_metrics_runtime;
pub mod matching_metrics_statsd;
pub mod matching_quota;
pub mod openfeature;
pub mod replication_convert;
pub mod ringpop_discovery;
pub mod ringpop_tchannel;
pub mod schedule_flow;
pub mod scheduler_wire;
pub mod service;
pub mod tchannel_json_client;
pub mod workflow_api;
pub mod yarpc;
