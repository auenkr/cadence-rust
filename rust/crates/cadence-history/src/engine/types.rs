//! History-service internal request/response types.
//!
//! Port of `go/common/types/history.go` (the `History*Request` wrappers that
//! the public `cadence-api` crate does not yet expose). Field names and JSON
//! tags follow the Go struct definitions.
// TODO(link): move into cadence-api once the types package is ported.

use std::collections::HashMap;

use cadence_api::admin::{ReapplyEventsRequest, RefreshWorkflowTasksRequest};
use cadence_api::enums::ContinueAsNewInitiator;
use cadence_api::service_types::{
    DescribeWorkflowExecutionRequest, RequestCancelWorkflowExecutionRequest,
    SignalWithStartWorkflowExecutionRequest,
};
use cadence_api::tasks::{
    PollForActivityTaskRequest, PollForDecisionTaskRequest, QueryWorkflowRequest,
    QueryWorkflowResponse, RecordActivityTaskHeartbeatRequest, RespondActivityTaskCanceledRequest,
    RespondActivityTaskCompletedRequest, RespondActivityTaskFailedRequest,
    RespondDecisionTaskCompletedRequest, RespondDecisionTaskFailedRequest, WorkflowQuery,
};
use cadence_api::types::{
    DataBlob, FailureOptions, ResetWorkflowExecutionRequest, SignalWorkflowExecutionRequest,
    StartWorkflowExecutionRequest, TaskList, TerminateWorkflowExecutionRequest, VersionHistories,
    VersionHistory, VersionHistoryItem, WorkflowExecution, WorkflowType,
};
use cadence_api::service_types::HistoryEvent;
use serde::{Deserialize, Serialize};

macro_rules! wire {
    ($(#[$m:meta])* pub struct $name:ident { $($(#[$fm:meta])* pub $f:ident : $t:ty,)* }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Default, Serialize, Deserialize)]
        pub struct $name { $($(#[$fm])* #[serde(default)] pub $f: $t,)* }
    };
}

wire! { pub struct DescribeMutableStateRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "execution")] pub execution: Option<WorkflowExecution>,
} }
wire! { pub struct DescribeMutableStateResponse {
    #[serde(rename = "mutableStateInCache")] pub mutable_state_in_cache: String,
    #[serde(rename = "mutableStateInDatabase")] pub mutable_state_in_database: String,
} }
wire! { pub struct HistoryDescribeWorkflowExecutionRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "request")] pub request: Option<DescribeWorkflowExecutionRequest>,
} }
wire! { pub struct GetMutableStateRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "execution")] pub execution: Option<WorkflowExecution>,
    #[serde(rename = "expectedNextEventId")] pub expected_next_event_id: i64,
    #[serde(rename = "currentBranchToken")] pub current_branch_token: Vec<u8>,
    #[serde(rename = "versionHistoryItem")] pub version_history_item: Option<VersionHistoryItem>,
} }
wire! { pub struct GetMutableStateResponse {
    #[serde(rename = "execution")] pub execution: Option<WorkflowExecution>,
    #[serde(rename = "workflowType")] pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "NextEventId")] pub next_event_id: i64,
    #[serde(rename = "PreviousStartedEventId")] pub previous_started_event_id: Option<i64>,
    #[serde(rename = "LastFirstEventId")] pub last_first_event_id: i64,
    #[serde(rename = "taskList")] pub task_list: Option<TaskList>,
    #[serde(rename = "stickyTaskList")] pub sticky_task_list: Option<TaskList>,
    #[serde(rename = "clientLibraryVersion")] pub client_library_version: String,
    #[serde(rename = "clientFeatureVersion")] pub client_feature_version: String,
    #[serde(rename = "clientImpl")] pub client_impl: String,
    #[serde(rename = "isWorkflowRunning")] pub is_workflow_running: bool,
    #[serde(rename = "stickyTaskListScheduleToStartTimeout")] pub sticky_task_list_schedule_to_start_timeout: Option<i32>,
    #[serde(rename = "eventStoreVersion")] pub event_store_version: i32,
    #[serde(rename = "currentBranchToken")] pub current_branch_token: Vec<u8>,
    #[serde(rename = "workflowState")] pub workflow_state: Option<i32>,
    #[serde(rename = "workflowCloseState")] pub workflow_close_state: Option<i32>,
    #[serde(rename = "versionHistories")] pub version_histories: Option<VersionHistories>,
    #[serde(rename = "isStickyTaskListEnabled")] pub is_sticky_task_list_enabled: bool,
    #[serde(rename = "historySize")] pub history_size: i64,
} }
wire! { pub struct PollMutableStateRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "execution")] pub execution: Option<WorkflowExecution>,
    #[serde(rename = "expectedNextEventId")] pub expected_next_event_id: i64,
    #[serde(rename = "currentBranchToken")] pub current_branch_token: Vec<u8>,
    #[serde(rename = "versionHistoryItem")] pub version_history_item: Option<VersionHistoryItem>,
} }
wire! { pub struct PollMutableStateResponse {
    #[serde(rename = "execution")] pub execution: Option<WorkflowExecution>,
    #[serde(rename = "workflowType")] pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "NextEventId")] pub next_event_id: i64,
    #[serde(rename = "PreviousStartedEventId")] pub previous_started_event_id: Option<i64>,
    #[serde(rename = "LastFirstEventId")] pub last_first_event_id: i64,
    #[serde(rename = "taskList")] pub task_list: Option<TaskList>,
    #[serde(rename = "stickyTaskList")] pub sticky_task_list: Option<TaskList>,
    #[serde(rename = "clientLibraryVersion")] pub client_library_version: String,
    #[serde(rename = "clientFeatureVersion")] pub client_feature_version: String,
    #[serde(rename = "clientImpl")] pub client_impl: String,
    #[serde(rename = "stickyTaskListScheduleToStartTimeout")] pub sticky_task_list_schedule_to_start_timeout: Option<i32>,
    #[serde(rename = "currentBranchToken")] pub current_branch_token: Vec<u8>,
    #[serde(rename = "versionHistories")] pub version_histories: Option<VersionHistories>,
    #[serde(rename = "workflowState")] pub workflow_state: Option<i32>,
    #[serde(rename = "workflowCloseState")] pub workflow_close_state: Option<i32>,
} }
wire! { pub struct HistoryQueryWorkflowRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "request")] pub request: Option<QueryWorkflowRequest>,
} }
wire! { pub struct HistoryQueryWorkflowResponse {
    #[serde(rename = "response")] pub response: Option<QueryWorkflowResponse>,
} }
wire! { pub struct HistoryReapplyEventsRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "request")] pub request: Option<ReapplyEventsRequest>,
} }
wire! { pub struct HistoryRecordActivityTaskHeartbeatRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "heartbeatRequest")] pub heartbeat_request: Option<RecordActivityTaskHeartbeatRequest>,
} }
wire! { pub struct RecordActivityTaskStartedRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "workflowExecution")] pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "scheduleId")] pub schedule_id: i64,
    #[serde(rename = "taskId")] pub task_id: i64,
    #[serde(rename = "requestId")] pub request_id: String,
    #[serde(rename = "pollRequest")] pub poll_request: Option<PollForActivityTaskRequest>,
} }
wire! { pub struct RecordActivityTaskStartedResponse {
    #[serde(rename = "scheduledEvent")] pub scheduled_event: Option<HistoryEvent>,
    #[serde(rename = "startedTimestamp")] pub started_timestamp: Option<i64>,
    #[serde(rename = "attempt")] pub attempt: i64,
    #[serde(rename = "scheduledTimestampOfThisAttempt")] pub scheduled_timestamp_of_this_attempt: Option<i64>,
    #[serde(rename = "heartbeatDetails")] pub heartbeat_details: Vec<u8>,
    #[serde(rename = "workflowType")] pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "workflowDomain")] pub workflow_domain: String,
} }
wire! { pub struct RecordChildExecutionCompletedRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "workflowExecution")] pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "initiatedId")] pub initiated_id: i64,
    #[serde(rename = "completedExecution")] pub completed_execution: Option<WorkflowExecution>,
    #[serde(rename = "completionEvent")] pub completion_event: Option<HistoryEvent>,
    #[serde(rename = "startedId")] pub started_id: i64,
} }
wire! { pub struct RecordDecisionTaskStartedRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "workflowExecution")] pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "scheduleId")] pub schedule_id: i64,
    #[serde(rename = "taskId")] pub task_id: i64,
    #[serde(rename = "requestId")] pub request_id: String,
    #[serde(rename = "pollRequest")] pub poll_request: Option<PollForDecisionTaskRequest>,
} }
wire! { pub struct TransientDecisionInfo {
    #[serde(rename = "scheduledEvent")] pub scheduled_event: Option<HistoryEvent>,
    #[serde(rename = "startedEvent")] pub started_event: Option<HistoryEvent>,
} }
wire! { pub struct RecordDecisionTaskStartedResponse {
    #[serde(rename = "workflowType")] pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "previousStartedEventId")] pub previous_started_event_id: Option<i64>,
    #[serde(rename = "scheduledEventId")] pub scheduled_event_id: i64,
    #[serde(rename = "startedEventId")] pub started_event_id: i64,
    #[serde(rename = "nextEventId")] pub next_event_id: i64,
    #[serde(rename = "attempt")] pub attempt: i64,
    #[serde(rename = "stickyExecutionEnabled")] pub sticky_execution_enabled: bool,
    #[serde(rename = "decisionInfo")] pub decision_info: Option<TransientDecisionInfo>,
    #[serde(rename = "WorkflowExecutionTaskList")] pub workflow_execution_task_list: Option<TaskList>,
    #[serde(rename = "eventStoreVersion")] pub event_store_version: i32,
    #[serde(rename = "branchToken")] pub branch_token: Vec<u8>,
    #[serde(rename = "scheduledTimestamp")] pub scheduled_timestamp: Option<i64>,
    #[serde(rename = "startedTimestamp")] pub started_timestamp: Option<i64>,
    #[serde(rename = "queries")] pub queries: HashMap<String, WorkflowQuery>,
    #[serde(rename = "historySize")] pub history_size: i64,
} }
wire! { pub struct HistoryRefreshWorkflowTasksRequest {
    #[serde(rename = "domainUIID")] pub domain_uuid: String,
    #[serde(rename = "request")] pub request: Option<RefreshWorkflowTasksRequest>,
} }
wire! { pub struct RemoveSignalMutableStateRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "workflowExecution")] pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "requestId")] pub request_id: String,
} }
wire! { pub struct ReplicateEventsV2Request {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "workflowExecution")] pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "versionHistoryItems")] pub version_history_items: Vec<VersionHistoryItem>,
    #[serde(rename = "events")] pub events: Option<DataBlob>,
    #[serde(rename = "newRunEvents")] pub new_run_events: Option<DataBlob>,
} }
wire! { pub struct HistoryRequestCancelWorkflowExecutionRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "cancelRequest")] pub cancel_request: Option<RequestCancelWorkflowExecutionRequest>,
    #[serde(rename = "externalInitiatedEventId")] pub external_initiated_event_id: Option<i64>,
    #[serde(rename = "externalWorkflowExecution")] pub external_workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "childWorkflowOnly")] pub child_workflow_only: bool,
} }
wire! { pub struct HistoryResetStickyTaskListRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "execution")] pub execution: Option<WorkflowExecution>,
} }
wire! { pub struct HistoryResetStickyTaskListResponse {} }
wire! { pub struct HistoryResetWorkflowExecutionRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "resetRequest")] pub reset_request: Option<ResetWorkflowExecutionRequest>,
} }
wire! { pub struct HistoryRespondActivityTaskCanceledRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "cancelRequest")] pub cancel_request: Option<RespondActivityTaskCanceledRequest>,
} }
wire! { pub struct HistoryRespondActivityTaskCompletedRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "completeRequest")] pub complete_request: Option<RespondActivityTaskCompletedRequest>,
} }
wire! { pub struct HistoryRespondActivityTaskFailedRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "failedRequest")] pub failed_request: Option<RespondActivityTaskFailedRequest>,
} }
wire! { pub struct HistoryRespondDecisionTaskCompletedRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "completeRequest")] pub complete_request: Option<RespondDecisionTaskCompletedRequest>,
} }
wire! { pub struct ActivityLocalDispatchInfo {
    #[serde(rename = "activityId")] pub activity_id: String,
    #[serde(rename = "scheduledTimestamp")] pub scheduled_timestamp: Option<i64>,
    #[serde(rename = "startedTimestamp")] pub started_timestamp: Option<i64>,
    #[serde(rename = "scheduledTimestampOfThisAttempt")] pub scheduled_timestamp_of_this_attempt: Option<i64>,
    #[serde(rename = "taskToken")] pub task_token: Vec<u8>,
} }
wire! { pub struct HistoryRespondDecisionTaskCompletedResponse {
    #[serde(rename = "startedResponse")] pub started_response: Option<RecordDecisionTaskStartedResponse>,
    #[serde(rename = "activitiesToDispatchLocally")] pub activities_to_dispatch_locally: HashMap<String, ActivityLocalDispatchInfo>,
} }
wire! { pub struct HistoryRespondDecisionTaskFailedRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "failedRequest")] pub failed_request: Option<RespondDecisionTaskFailedRequest>,
} }
wire! { pub struct ScheduleDecisionTaskRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "workflowExecution")] pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "isFirstDecision")] pub is_first_decision: bool,
} }
wire! { pub struct HistorySignalWithStartWorkflowExecutionRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "signalWithStartRequest")] pub signal_with_start_request: Option<SignalWithStartWorkflowExecutionRequest>,
    #[serde(rename = "PartitionConfig")] pub partition_config: HashMap<String, String>,
} }
wire! { pub struct HistorySignalWorkflowExecutionRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "signalRequest")] pub signal_request: Option<SignalWorkflowExecutionRequest>,
    #[serde(rename = "externalWorkflowExecution")] pub external_workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "childWorkflowOnly")] pub child_workflow_only: bool,
} }
wire! { pub struct ParentExecutionInfo {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "domain")] pub domain: String,
    #[serde(rename = "execution")] pub execution: Option<WorkflowExecution>,
    #[serde(rename = "initiatedId")] pub initiated_id: i64,
} }
wire! { pub struct HistoryStartWorkflowExecutionRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "startRequest")] pub start_request: Option<StartWorkflowExecutionRequest>,
    #[serde(rename = "parentExecutionInfo")] pub parent_execution_info: Option<ParentExecutionInfo>,
    #[serde(rename = "attempt")] pub attempt: i32,
    #[serde(rename = "expirationTimestamp")] pub expiration_timestamp: Option<i64>,
    #[serde(rename = "continueAsNewInitiator")] pub continue_as_new_initiator: Option<ContinueAsNewInitiator>,
    #[serde(rename = "continuedFailureReason")] pub continued_failure_reason: Option<String>,
    #[serde(rename = "continuedFailureDetails")] pub continued_failure_details: Vec<u8>,
    #[serde(rename = "lastCompletionResult")] pub last_completion_result: Vec<u8>,
    #[serde(rename = "firstDecisionTaskBackoffSeconds")] pub first_decision_task_backoff_seconds: Option<i32>,
    #[serde(rename = "PartitionConfig")] pub partition_config: HashMap<String, String>,
} }
wire! { pub struct SyncActivityRequest {
    #[serde(rename = "domainId")] pub domain_id: String,
    #[serde(rename = "workflowId")] pub workflow_id: String,
    #[serde(rename = "runId")] pub run_id: String,
    #[serde(rename = "version")] pub version: i64,
    #[serde(rename = "scheduledId")] pub scheduled_id: i64,
    #[serde(rename = "scheduledTime")] pub scheduled_time: Option<i64>,
    #[serde(rename = "startedId")] pub started_id: i64,
    #[serde(rename = "startedTime")] pub started_time: Option<i64>,
    #[serde(rename = "lastHeartbeatTime")] pub last_heartbeat_time: Option<i64>,
    #[serde(rename = "details")] pub details: Vec<u8>,
    #[serde(rename = "attempt")] pub attempt: i32,
    #[serde(rename = "lastFailureReason")] pub last_failure_reason: Option<String>,
    #[serde(rename = "lastWorkerIdentity")] pub last_worker_identity: String,
    #[serde(rename = "lastFailureDetails")] pub last_failure_details: Vec<u8>,
    #[serde(rename = "lastFailureOptions")] pub last_failure_options: Option<FailureOptions>,
    #[serde(rename = "versionHistory")] pub version_history: Option<VersionHistory>,
} }
wire! { pub struct SyncShardStatusRequest {
    #[serde(rename = "sourceCluster")] pub source_cluster: String,
    #[serde(rename = "shardId")] pub shard_id: i64,
    #[serde(rename = "timestamp")] pub timestamp: Option<i64>,
} }
wire! { pub struct HistoryTerminateWorkflowExecutionRequest {
    #[serde(rename = "domainUUID")] pub domain_uuid: String,
    #[serde(rename = "terminateRequest")] pub terminate_request: Option<TerminateWorkflowExecutionRequest>,
    #[serde(rename = "externalWorkflowExecution")] pub external_workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "childWorkflowOnly")] pub child_workflow_only: bool,
} }
wire! { pub struct FailoverMarkerToken {
    #[serde(rename = "shardIDs")] pub shard_ids: Vec<i32>,
    #[serde(rename = "failoverMarker")] pub failover_marker: Option<cadence_api::types::FailoverMarkerAttributes>,
} }
wire! { pub struct NotifyFailoverMarkersRequest {
    #[serde(rename = "failoverMarkerTokens")] pub failover_marker_tokens: Vec<FailoverMarkerToken>,
} }
wire! { pub struct GetFailoverInfoRequest {
    #[serde(rename = "domainID")] pub domain_id: String,
} }
wire! { pub struct GetFailoverInfoResponse {
    #[serde(rename = "completedShardCount")] pub completed_shard_count: i32,
    #[serde(rename = "pendingShards")] pub pending_shards: Vec<i32>,
} }
// Go `types.Any` payload (`Any{ValueType string; Value []byte}`).
wire! { pub struct Any {
    #[serde(rename = "valueType")] pub value_type: String,
    #[serde(rename = "value")] pub value: Vec<u8>,
} }
wire! { pub struct RatelimitUpdateRequest {
    #[serde(rename = "any")] pub any: Option<Any>,
} }
wire! { pub struct RatelimitUpdateResponse {
    #[serde(rename = "any")] pub any: Option<Any>,
} }
wire! { pub struct HistoryCountDLQMessagesResponse {
    #[serde(rename = "entries")] pub entries: Vec<cadence_api::admin::HistoryDLQCountEntry>,
} }
wire! { pub struct HealthStatus {
    #[serde(rename = "ok")] pub ok: bool,
    #[serde(rename = "msg")] pub msg: String,
} }
