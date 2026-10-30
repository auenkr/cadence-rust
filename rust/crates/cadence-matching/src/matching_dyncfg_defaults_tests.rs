//! Pins every Matching dynamic-config default to the Go value in
//! `go/common/dynamicconfig/dynamicproperties/constants.go`
//! (see memory/matching-dyncfg-key-matrix-20261023.md). Defaults for the
//! `task_list_*` readers are inline closures, so this checks the source of the
//! reader that follows each key; the shared constants are pinned separately.

use crate::matching_runtime::live_shutdown_drain_duration;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const WINDOW: usize = 700;

/// Production files can have top-level test-only helpers before later production items, such as
/// `QueryDispatchFault` in `grpc_matching.rs`. Stop at the test modules instead of the first
/// top-level `#[cfg(test)]`; indented test-only fields, statements, and methods are also retained.
fn production(source: &'static str) -> &'static str {
    let path_module = source.find("\n#[cfg(test)]\n#[path =");
    let inline_module = source.find("\n#[cfg(test)]\nmod ");
    let end = path_module.into_iter().chain(inline_module).min();
    end.map_or(source, |end| &source[..end])
}

fn assert_default(source: &'static str, key: &str, expected: &str) {
    let quoted = format!("\"{key}\"");
    let start = production(source)
        .find(&quoted)
        .unwrap_or_else(|| panic!("{key}: no Rust reader found"));
    let window = &source[start..(start + WINDOW).min(source.len())];
    assert!(window.contains(expected), "{key}: expected default `{expected}` near reader");
}

#[test]
fn reader_defaults_match_go_table() {
    let table: &[(&str, &str, &str)] = &[
        (include_str!("grpc_matching.rs"), "matching.numTasklistWritePartitions", "1,"),
        (include_str!("grpc_matching.rs"), "matching.numTasklistReadPartitions", "1,"),
        (include_str!("grpc_matching.rs"), "matching.localTaskWaitTime", "10_000_000"),
        (include_str!("grpc_matching.rs"), "matching.localPollWaitTime", "10_000_000"),
        (include_str!("grpc_matching.rs"), "matching.enableGetNumberOfPartitionsFromCache", "false"),
        (include_str!("grpc_matching.rs"), "matching.qpsTrackerInterval", "DEFAULT_QPS_TRACKER_INTERVAL_NANOS"),
        (include_str!("grpc_matching.rs"), "matching.forwarderMaxChildrenPerNode", "20"),
        (include_str!("grpc_matching.rs"), "matching.forwarderMaxOutstandingPolls", "1,"),
        (include_str!("grpc_matching.rs"), "matching.forwarderMaxOutstandingTasks", "1,"),
        (include_str!("grpc_matching.rs"), "matching.forwarderMaxRatePerSecond", "10,"),
        (include_str!("grpc_matching.rs"), "matching.getTasksBatchSize", "DEFAULT_TASKS_BATCH_SIZE"),
        (include_str!("grpc_matching.rs"), "matching.readRangeSize", "DEFAULT_READ_RANGE_SIZE"),
        (include_str!("grpc_matching.rs"), "matching.asyncTaskDispatchTimeout", "DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS"),
        (include_str!("grpc_matching.rs"), "matching.appendTaskTimeout", "5_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.maxTaskBatchSize", "100"),
        (include_str!("grpc_matching.rs"), "matching.outstandingTaskAppendsThreshold", "250"),
        (include_str!("grpc_matching.rs"), "matching.maxTaskDeleteBatchSize", "100"),
        (include_str!("grpc_matching.rs"), "matching.updateAckInterval", "DEFAULT_UPDATE_ACK_INTERVAL_NANOS"),
        (include_str!("grpc_matching.rs"), "matching.recordTaskStartedTimeout", "DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS"),
        (include_str!("grpc_matching.rs"), "matching.minTaskThrottlingBurstSize", "MIN_TASK_THROTTLING_BURST_SIZE"),
        (include_str!("grpc_matching.rs"), "matching.idleTasklistCheckInterval", "300_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.adaptiveScalerUpdateInterval", "15_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.enableAdaptiveScaler", "false"),
        (include_str!("grpc_matching.rs"), "matching.partitionUpscaleRPS", "200"),
        (include_str!("grpc_matching.rs"), "matching.partitionDownscaleFactor", "0.75"),
        (include_str!("grpc_matching.rs"), "matching.partitionUpscaleSustainedDuration", "60_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.partitionDownscaleSustainedDuration", "120_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.taskListMinimumWritePartitions", "1"),
        (include_str!("grpc_matching.rs"), "matching.enablePartitionIsolationGroupAssignment", "false"),
        (include_str!("grpc_matching.rs"), "matching.enablePartitionEmptyCheck", "false"),
        (include_str!("grpc_matching.rs"), "matching.isolationGroupUpscaleSustainedDuration", "60_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.isolationGroupDownscaleSustainedDuration", "120_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.isolationGroupHasPollersSustainedDuration", "60_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.isolationGroupNoPollersSustainedDuration", "60_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.isolationGroupsPerPartition", "2"),
        (include_str!("grpc_matching.rs"), "matching.taskIsolationPollerWindow", "2_000_000_000"),
        (include_str!("grpc_matching.rs"), "matching.taskIsolationDuration", "200_000_000"),
        (include_str!("grpc_matching.rs"), "matching.enableStandbyTaskCompletion", "true"),
        (include_str!("grpc_matching.rs"), "matching.enableClientAutoConfig", "false"),
        (include_str!("grpc_matching.rs"), "matching.overrideTaskListRps", "0.0"),
        (include_str!("grpc_matching.rs"), "matching.matchingReturnAllTaskListKinds", "false"),
        (include_str!("grpc_matching.rs"), "system.enableTasklistIsolation", "false"),
        (include_str!("grpc_matching.rs"), "system.enableDistributedSemaphore", "false"),
        (include_str!("launcher.rs"), "matching.excludeShortLivedTaskListsFromShardManager", "true"),
        (include_str!("launcher.rs"), "matching.percentageOnboardedToShardManager", "0"),
        (include_str!("launcher.rs"), "system.enableDebugMode", "false"),
        (include_str!("matching_persistence_limiter.rs"), "matching.persistenceMaxQPS", "HOST_QPS_DEFAULT"),
        (include_str!("matching_persistence_limiter.rs"), "matching.persistenceGlobalMaxQPS", "GLOBAL_QPS_DEFAULT"),
        (include_str!("matching_throttled_logger.rs"), "matching.throttledLogRPS", "None, 20"),
        (include_str!("matching_task_info_log.rs"), "matching.enableTaskInfoLogByDomainID", "false"),
    ];
    for (source, key, expected) in table {
        assert_default(source, key, expected);
    }
}

#[test]
fn shared_default_constants_match_go() {
    assert_eq!(crate::matching_reader::DEFAULT_TASKS_BATCH_SIZE, 1000);
    assert_eq!(crate::matching_reader::DEFAULT_READ_RANGE_SIZE, 50000);
    assert_eq!(crate::matching_reader::DEFAULT_ASYNC_TASK_DISPATCH_TIMEOUT_NANOS, 3_000_000_000);
    assert_eq!(crate::matching_reader::DEFAULT_UPDATE_ACK_INTERVAL_NANOS, 60_000_000_000);
    assert_eq!(crate::matching_history::DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS, 1_000_000_000);
    assert_eq!(crate::matching_limiter::MIN_TASK_THROTTLING_BURST_SIZE, 1);
    assert_eq!(crate::matching_registry::DEFAULT_QPS_TRACKER_INTERVAL_NANOS, 10_000_000_000);
    let limiter = include_str!("matching_persistence_limiter.rs");
    assert!(limiter.contains("const HOST_QPS_DEFAULT: i64 = 3000;"));
    assert!(limiter.contains("const GLOBAL_QPS_DEFAULT: i64 = 0;"));
}

#[test]
fn shutdown_drain_duration_defaults_to_zero() {
    let overlay = Arc::new(Mutex::new(crate::admin::DynamicConfigOverlay::default()));
    assert_eq!((live_shutdown_drain_duration(overlay))(), Duration::ZERO);
}
