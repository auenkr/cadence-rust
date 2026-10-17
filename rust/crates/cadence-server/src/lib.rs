pub mod admin;
pub mod config;
// Tonic service signatures require Result<_, Status>; boxing changes the API.
#[allow(clippy::result_large_err)]
pub mod grpc;
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
pub mod grpc_admin;
#[allow(clippy::result_large_err)]
pub mod grpc_shard;
pub mod handlers;
pub mod service;
pub mod tchannel;
pub mod yarpc;

#[allow(clippy::result_large_err)]
pub mod matching_history;

#[allow(clippy::result_large_err)]
pub mod matching_dispatch;

#[allow(clippy::result_large_err)]
pub mod matching_registry;
#[allow(clippy::result_large_err)]
pub mod matching_partition;
pub mod matching_sync;
pub mod matching_forward;
pub mod matching_semaphore;
pub mod matching_scaler;

#[allow(clippy::result_large_err)]
pub mod matching_writer;

pub mod matching_limiter;

#[allow(clippy::result_large_err)]
pub mod matching_query;

pub mod matching_reader;
