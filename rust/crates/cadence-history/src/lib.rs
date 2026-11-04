//! History service bootstrap and shard ownership primitives.

pub mod config;
pub mod engine;
pub mod execution;
pub mod mutable_state;
pub mod mutable_state_activity;
pub mod queue;
pub mod shard;
pub mod ndc;

pub const SERVICE_NAME: &str = "history";

/// Exact RPC method identifiers present in Go's `HistoryService` Thrift IDL.
/// The shared server's HistoryAPI gRPC service is generated from the upstream
/// IDL and registered by the shared launcher.
pub const THRIFT_HISTORY_METHODS: &[&str] = &[
    "StartWorkflowExecution",
    "GetMutableState",
    "PollMutableState",
    "ResetStickyTaskList",
    "RecordDecisionTaskStarted",
    "RecordActivityTaskStarted",
    "RespondDecisionTaskCompleted",
    "RespondDecisionTaskFailed",
    "RecordActivityTaskHeartbeat",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskFailed",
    "RespondActivityTaskCanceled",
    "SignalWorkflowExecution",
    "SignalWithStartWorkflowExecution",
    "RemoveSignalMutableState",
    "TerminateWorkflowExecution",
    "ResetWorkflowExecution",
    "RequestCancelWorkflowExecution",
    "ScheduleDecisionTask",
    "RecordChildExecutionCompleted",
    "DescribeWorkflowExecution",
    "ReplicateEventsV2",
    "SyncShardStatus",
    "SyncActivity",
    "DescribeMutableState",
    "DescribeHistoryHost",
    "CloseShard",
    "RemoveTask",
    "ResetQueue",
    "DescribeQueue",
    "GetReplicationMessages",
    "GetDLQReplicationMessages",
    "QueryWorkflow",
    "ReapplyEvents",
    "RefreshWorkflowTasks",
    "ReadDLQMessages",
    "PurgeDLQMessages",
    "MergeDLQMessages",
    "NotifyFailoverMarkers",
    "GetCrossClusterTasks",
    "RespondCrossClusterTasksCompleted",
    "GetFailoverInfo",
    "RatelimitUpdate",
];

/// Exact gRPC methods in `uber.cadence.history.v1.HistoryAPI`.
pub const GRPC_HISTORY_METHODS: &[&str] = &[
    "StartWorkflowExecution",
    "SignalWorkflowExecution",
    "SignalWithStartWorkflowExecution",
    "ResetWorkflowExecution",
    "TerminateWorkflowExecution",
    "DescribeWorkflowExecution",
    "QueryWorkflow",
    "ResetStickyTaskList",
    "GetMutableState",
    "PollMutableState",
    "RecordDecisionTaskStarted",
    "RespondDecisionTaskCompleted",
    "RespondDecisionTaskFailed",
    "RecordActivityTaskStarted",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskFailed",
    "RespondActivityTaskCanceled",
    "RecordActivityTaskHeartbeat",
    "RequestCancelWorkflowExecution",
    "RemoveSignalMutableState",
    "ScheduleDecisionTask",
    "RecordChildExecutionCompleted",
    "ReplicateEventsV2",
    "SyncShardStatus",
    "SyncActivity",
    "DescribeMutableState",
    "DescribeHistoryHost",
    "CloseShard",
    "RemoveTask",
    "ResetQueue",
    "DescribeQueue",
    "GetReplicationMessages",
    "GetDLQReplicationMessages",
    "ReapplyEvents",
    "RefreshWorkflowTasks",
    "CountDLQMessages",
    "ReadDLQMessages",
    "PurgeDLQMessages",
    "MergeDLQMessages",
    "NotifyFailoverMarkers",
    "GetCrossClusterTasks",
    "RespondCrossClusterTasksCompleted",
    "GetFailoverInfo",
    "RatelimitUpdate",
];

#[cfg(test)]
mod rpc_tests {
    use super::{GRPC_HISTORY_METHODS, THRIFT_HISTORY_METHODS};
    #[test]
    fn history_thrift_wire_names_are_unique() {
        let mut names = THRIFT_HISTORY_METHODS.to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), THRIFT_HISTORY_METHODS.len());
        let mut grpc = GRPC_HISTORY_METHODS.to_vec();
        grpc.sort_unstable();
        grpc.dedup();
        assert_eq!(grpc.len(), GRPC_HISTORY_METHODS.len());
    }
}
pub mod handler;
pub mod wrappers;
pub mod decision;
pub mod events;
pub mod query;
pub mod reset;
pub mod workflowcache;
pub mod queuev2;
pub mod failover;
pub mod replication;
