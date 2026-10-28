//! Byte-preserving building blocks for the native Matching RPC handler.
//!
//! These are engine-facing operations, not a `NativeMatchingApi` implementation.
//! The caller must perform the handler's startup, admission, domain lookup,
//! metrics and ownership checks in their Go order before invoking them. In
//! particular, `get` alone is not a substitute for the handler's ownership
//! check. See memory/matching-native-handler-integration-20261028.md.
use cadence_common::{go_string::GoString, task_list::TaskListNameBytes};
use cadence_persistence::matching_native_task_store::TaskListKeyBytes;
use cadence_proto::native::{api, matching};
use tonic::{Code, Status};

use crate::{
    matching_native_status::raw_status,
    matching_partition_native::NativeStoredPartitionConfig,
    matching_query_native::{QueryOutcome, WorkerAnswer},
    matching_registry_native::{NativeQueueHandle, NativeQueueRegistry},
};

/// Parse an identifier without losing the bytes embedded in Go's error.
/// Requires `RawStatusService` on the native transport.
pub fn task_list_name(name: &GoString) -> Result<TaskListNameBytes, Status> {
    TaskListNameBytes::parse(name.as_bytes()).map_err(|message| {
        raw_status(Code::Internal, &message, tonic::codegen::Bytes::new())
    })
}

/// Go's proto mapper reads a missing or unknown task-list kind as normal.
pub fn persistence_kind(api_kind: i32) -> i16 {
    match api_kind {
        2 => 1,
        3 => 2,
        _ => 0,
    }
}

/// Go's mapper fills default partitions when the count and map length differ.
pub fn partition_config(config: api::TaskListPartitionConfig) -> NativeStoredPartitionConfig {
    let convert = |count, partitions| {
        cadence_common::task_list::partition_map::<api::TaskListPartition>(count, partitions)
            .into_iter()
            .map(|(id, partition)| (id, partition.isolation_groups))
            .collect()
    };
    NativeStoredPartitionConfig {
        version: config.version,
        read_partitions: convert(config.num_read_partitions, config.read_partitions),
        write_partitions: convert(config.num_write_partitions, config.write_partitions),
    }
}

/// Mapper panic occurs before admission if a present completion has no result.
/// Call this before any handler side effects.
pub fn validate_query_completion_mapper(
    request: &matching::RespondQueryTaskCompletedRequest,
) -> Result<(), Status> {
    if request.request.as_ref().is_some_and(|nested| nested.result.is_none()) {
        return Err(Status::unknown(
            "panic: runtime error: invalid memory address or nil pointer dereference",
        ));
    }
    Ok(())
}

/// Deliver to the engine-wide task-ID index, including when the owner unloaded.
/// Domain and task-list fields are deliberately not used for this lookup.
pub fn deliver_query_completion(
    registry: &NativeQueueRegistry,
    request: matching::RespondQueryTaskCompletedRequest,
) -> Result<matching::RespondQueryTaskCompletedResponse, Status> {
    let payload = request.request.unwrap_or_default();
    registry.deliver_query_result(
        &request.task_id,
        QueryOutcome::Worker(WorkerAnswer {
            result: payload.result.unwrap_or_default(),
            worker_version_info: payload.worker_version_info,
        }),
    )?;
    Ok(matching::RespondQueryTaskCompletedResponse::default())
}

/// Cancel by raw poller ID on an already ownership-checked and loaded queue.
/// A missing poller is a successful cancellation, as in Go.
pub fn cancel_outstanding_poll(queue: &NativeQueueHandle, poller_id: &GoString) {
    queue.pollers.lock().expect("poller history lock").cancel(poller_id.as_bytes());
}

/// Validate the engine's manual-update checks after the scaler check and
/// before loading the queue. `config` stays native throughout.
pub fn validate_partition_update(
    task_list: &api::TaskList,
    config: Option<api::TaskListPartitionConfig>,
) -> Result<NativeStoredPartitionConfig, Status> {
    if matches!(task_list.kind, 2 | 3) {
        return Err(Status::invalid_argument(
            "Only normal tasklist's partition config can be updated.",
        ));
    }
    let config = config.ok_or_else(|| {
        Status::invalid_argument("Task list partition config is not set in the request.")
    })?;
    if !task_list_name(&task_list.name)?.is_root() {
        return Err(Status::invalid_argument(
            "Only root partition's partition config can be updated.",
        ));
    }
    Ok(partition_config(config))
}

/// Validate refresh before loading the queue, preserving nil versus present.
pub fn validate_partition_refresh(
    task_list: &api::TaskList,
    config: Option<api::TaskListPartitionConfig>,
) -> Result<Option<NativeStoredPartitionConfig>, Status> {
    if matches!(task_list.kind, 2 | 3) {
        return Err(Status::invalid_argument(
            "Only normal tasklist's partition config can be updated.",
        ));
    }
    if task_list_name(&task_list.name)?.is_root() && config.is_some() {
        return Err(Status::invalid_argument("PartitionConfig must be nil for root partition."));
    }
    Ok(config.map(partition_config))
}

/// Persist and notify using the native registry's cancellation-aware update.
/// `key` and `queue` must refer to the same ownership-checked manager.
pub async fn update_partition_config(
    registry: &NativeQueueRegistry,
    key: TaskListKeyBytes,
    queue: NativeQueueHandle,
    config: NativeStoredPartitionConfig,
    now: i64,
    deadline: Option<tokio::time::Instant>,
    metric_domain: String,
) -> Result<(), Status> {
    registry
        .update_partition_config_for_domain(key, queue, config, now, deadline, metric_domain)
        .await
}

/// Push a supplied config or refresh the root config from persistence.
pub async fn refresh_partition_config(
    registry: &NativeQueueRegistry,
    key: TaskListKeyBytes,
    queue: NativeQueueHandle,
    config: Option<NativeStoredPartitionConfig>,
    deadline: Option<tokio::time::Instant>,
) -> Result<(), Status> {
    registry
        .refresh_partition_config_with_deadline(key, queue, config, deadline)
        .await
}
