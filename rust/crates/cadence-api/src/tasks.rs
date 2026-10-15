//! Task-list (matching) and decision types ported from
//! `go/common/types/shared.go`, `matching.go`.
//!
//! `serde` renames match the Go `json` tags exactly.

use serde::{Deserialize, Serialize};

use crate::enums::*;
use crate::types::*;

// ---------------------------------------------------------------------------
// Decision attributes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScheduleActivityTaskDecisionAttributes {
    #[serde(
        rename = "activityId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub activity_id: String,
    #[serde(
        rename = "activityType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_type: Option<ActivityType>,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "scheduleToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schedule_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "scheduleToStartTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schedule_to_start_timeout_seconds: Option<i32>,
    #[serde(
        rename = "startToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "heartbeatTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub heartbeat_timeout_seconds: Option<i32>,
    #[serde(
        rename = "retryPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteWorkflowExecutionDecisionAttributes {
    #[serde(
        rename = "result",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub result: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailWorkflowExecutionDecisionAttributes {
    #[serde(rename = "reason", default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMarkerDecisionAttributes {
    #[serde(
        rename = "markerName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub marker_name: String,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartTimerDecisionAttributes {
    #[serde(rename = "timerId", default, skip_serializing_if = "String::is_empty")]
    pub timer_id: String,
    #[serde(
        rename = "startToFireTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_to_fire_timeout_seconds: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelTimerDecisionAttributes {
    #[serde(rename = "timerId", default, skip_serializing_if = "String::is_empty")]
    pub timer_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCancelActivityTaskDecisionAttributes {
    #[serde(
        rename = "activityId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub activity_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CancelWorkflowExecutionDecisionAttributes {
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    #[serde(
        rename = "decisionType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub decision_type: Option<DecisionType>,
    #[serde(
        rename = "scheduleActivityTaskDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schedule_activity_task_decision_attributes: Option<ScheduleActivityTaskDecisionAttributes>,
    #[serde(
        rename = "startTimerDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_timer_decision_attributes: Option<StartTimerDecisionAttributes>,
    #[serde(
        rename = "completeWorkflowExecutionDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub complete_workflow_execution_decision_attributes:
        Option<CompleteWorkflowExecutionDecisionAttributes>,
    #[serde(
        rename = "failWorkflowExecutionDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub fail_workflow_execution_decision_attributes:
        Option<FailWorkflowExecutionDecisionAttributes>,
    #[serde(
        rename = "requestCancelActivityTaskDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub request_cancel_activity_task_decision_attributes:
        Option<RequestCancelActivityTaskDecisionAttributes>,
    #[serde(
        rename = "cancelTimerDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cancel_timer_decision_attributes: Option<CancelTimerDecisionAttributes>,
    #[serde(
        rename = "cancelWorkflowExecutionDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cancel_workflow_execution_decision_attributes:
        Option<CancelWorkflowExecutionDecisionAttributes>,
    #[serde(
        rename = "recordMarkerDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub record_marker_decision_attributes: Option<RecordMarkerDecisionAttributes>,
    #[serde(
        rename = "continueAsNewWorkflowExecutionDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub continue_as_new_workflow_execution_decision_attributes:
        Option<ContinueAsNewWorkflowExecutionDecisionAttributes>,
    #[serde(
        rename = "startChildWorkflowExecutionDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_child_workflow_execution_decision_attributes:
        Option<StartChildWorkflowExecutionDecisionAttributes>,
    #[serde(
        rename = "signalExternalWorkflowExecutionDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub signal_external_workflow_execution_decision_attributes:
        Option<SignalExternalWorkflowExecutionDecisionAttributes>,
    #[serde(
        rename = "upsertWorkflowSearchAttributesDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub upsert_workflow_search_attributes_decision_attributes:
        Option<UpsertWorkflowSearchAttributesDecisionAttributes>,
    #[serde(
        rename = "requestCancelExternalWorkflowExecutionDecisionAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub request_cancel_external_workflow_execution_decision_attributes:
        Option<RequestCancelExternalWorkflowExecutionDecisionAttributes>,
}

// ---------------------------------------------------------------------------
// Decision-task poll / respond
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollForDecisionTaskRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "binaryChecksum",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub binary_checksum: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PollForDecisionTaskResponse {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(
        rename = "previousStartedEventId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_started_event_id: Option<i64>,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(rename = "attempt", default)]
    pub attempt: i64,
    #[serde(rename = "backlogCountHint", default)]
    pub backlog_count_hint: i64,
    #[serde(rename = "nextEventId", default)]
    pub next_event_id: i64,
    #[serde(
        rename = "scheduledTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scheduled_timestamp: Option<i64>,
    #[serde(
        rename = "startedTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub started_timestamp: Option<i64>,
    #[serde(rename = "history", default, skip_serializing_if = "Option::is_none")]
    pub history: Option<crate::service_types::History>,
    #[serde(
        rename = "nextPageToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
    #[serde(
        rename = "workflowExecutionTaskList",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_task_list: Option<TaskList>,
    #[serde(rename = "query", default, skip_serializing_if = "Option::is_none")]
    pub query: Option<WorkflowQuery>,
    /// Extension for the embedded-query flow: matches the buffered query
    /// so workers can echo answers in `queryResults`. Omitted when empty.
    #[serde(rename = "queryId", default, skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
    #[serde(rename = "queries", default, skip_serializing_if = "Option::is_none")]
    pub queries: Option<std::collections::HashMap<String, WorkflowQuery>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RespondDecisionTaskCompletedRequest {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(rename = "decisions", default, skip_serializing_if = "Vec::is_empty")]
    pub decisions: Vec<Decision>,
    #[serde(
        rename = "executionContext",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub execution_context: Vec<u8>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "binaryChecksum",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub binary_checksum: String,
    #[serde(
        rename = "queryResults",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub query_results: Option<std::collections::HashMap<String, WorkflowQueryResult>>,
    #[serde(
        rename = "stickyAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub sticky_attributes: Option<StickyExecutionAttributes>,
    #[serde(rename = "returnNewDecisionTask", default)]
    pub return_new_decision_task: bool,
    #[serde(rename = "forceCreateNewDecisionTask", default)]
    pub force_create_new_decision_task: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RespondDecisionTaskCompletedResponse {
    #[serde(
        rename = "decisionTask",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub decision_task: Option<PollForDecisionTaskResponse>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespondDecisionTaskFailedRequest {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(rename = "cause", default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<DecisionTaskFailedCause>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "binaryChecksum",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub binary_checksum: String,
}

// ---------------------------------------------------------------------------
// Activity-task poll / respond
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PollForActivityTaskRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "taskListMetadata",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub task_list_metadata: Option<TaskListMetadata>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PollForActivityTaskResponse {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "activityId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub activity_id: String,
    #[serde(
        rename = "activityType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_type: Option<ActivityType>,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "scheduledTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scheduled_timestamp: Option<i64>,
    #[serde(
        rename = "scheduleToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schedule_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "startedTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub started_timestamp: Option<i64>,
    #[serde(
        rename = "startToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "heartbeatTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub heartbeat_timeout_seconds: Option<i32>,
    #[serde(rename = "attempt", default)]
    pub attempt: i32,
    #[serde(
        rename = "scheduledTimestampOfThisAttempt",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scheduled_timestamp_of_this_attempt: Option<i64>,
    #[serde(
        rename = "heartbeatDetails",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub heartbeat_details: Vec<u8>,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(
        rename = "workflowDomain",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_domain: String,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespondActivityTaskCompletedRequest {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(
        rename = "result",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub result: Vec<u8>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RespondActivityTaskFailedRequest {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(rename = "reason", default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "failureOptions",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub failure_options: Option<FailureOptions>,
    #[serde(
        rename = "heartbeatDetails",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub heartbeat_details: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Task event attributes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionTaskScheduledEventAttributes {
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(
        rename = "startToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_to_close_timeout_seconds: Option<i32>,
    #[serde(rename = "attempt", default)]
    pub attempt: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionTaskStartedEventAttributes {
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "requestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub request_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionTaskCompletedEventAttributes {
    #[serde(
        rename = "executionContext",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub execution_context: Vec<u8>,
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "binaryChecksum",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub binary_checksum: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivityTaskScheduledEventAttributes {
    #[serde(
        rename = "activityId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub activity_id: String,
    #[serde(
        rename = "activityType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_type: Option<ActivityType>,
    #[serde(rename = "domain", default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "scheduleToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schedule_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "scheduleToStartTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schedule_to_start_timeout_seconds: Option<i32>,
    #[serde(
        rename = "startToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "heartbeatTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub heartbeat_timeout_seconds: Option<i32>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(
        rename = "retryPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTaskStartedEventAttributes {
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "requestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub request_id: String,
    #[serde(rename = "attempt", default)]
    pub attempt: i32,
    #[serde(
        rename = "lastFailureReason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_failure_reason: Option<String>,
    #[serde(
        rename = "lastFailureDetails",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub last_failure_details: Vec<u8>,
    #[serde(
        rename = "lastFailureOptions",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_failure_options: Option<FailureOptions>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTaskCompletedEventAttributes {
    #[serde(
        rename = "result",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub result: Vec<u8>,
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTaskFailedEventAttributes {
    #[serde(rename = "reason", default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(
        rename = "failureOptions",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub failure_options: Option<FailureOptions>,
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTaskCancelRequestedEventAttributes {
    #[serde(
        rename = "activityId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub activity_id: String,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionCompletedEventAttributes {
    #[serde(
        rename = "result",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub result: Vec<u8>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionFailedEventAttributes {
    #[serde(rename = "reason", default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionCanceledEventAttributes {
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerStartedEventAttributes {
    #[serde(rename = "timerId", default, skip_serializing_if = "String::is_empty")]
    pub timer_id: String,
    #[serde(
        rename = "startToFireTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_to_fire_timeout_seconds: Option<i64>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelTimerFailedEventAttributes {
    #[serde(rename = "timerId", default)]
    pub timer_id: String,
    #[serde(default)]
    pub cause: String,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(default)]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerCanceledEventAttributes {
    #[serde(rename = "timerId", default, skip_serializing_if = "String::is_empty")]
    pub timer_id: String,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkerRecordedEventAttributes {
    #[serde(
        rename = "markerName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub marker_name: String,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionTaskFailedEventAttributes {
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(rename = "cause", default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<DecisionTaskFailedCause>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(rename = "reason", default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(
        rename = "binaryChecksum",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub binary_checksum: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_names_match_go() {
        let d = Decision {
            decision_type: Some(DecisionType::ScheduleActivityTask),
            schedule_activity_task_decision_attributes: Some(
                ScheduleActivityTaskDecisionAttributes {
                    activity_id: "a1".into(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        let v: serde_json::Value = serde_json::to_value(&d).unwrap();
        assert_eq!(v["decisionType"], 0);
        assert_eq!(
            v["scheduleActivityTaskDecisionAttributes"]["activityId"],
            "a1"
        );
    }

    #[test]
    fn poll_response_token_is_base64_like_go_bytes() {
        let r = PollForDecisionTaskResponse {
            task_token: b"token-bytes".to_vec(),
            started_event_id: 4,
            ..Default::default()
        };
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["startedEventId"], 4);
        assert!(v["taskToken"].is_string());
        let back: PollForDecisionTaskResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }
}

// ---------------------------------------------------------------------------
// Cross-workflow decisions and events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContinueAsNewWorkflowExecutionDecisionAttributes {
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "executionStartToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "taskStartToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub task_start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "backoffStartIntervalInSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub backoff_start_interval_in_seconds: Option<i32>,
    #[serde(
        rename = "retryPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(rename = "initiator", default, skip_serializing_if = "Option::is_none")]
    pub initiator: Option<ContinueAsNewInitiator>,
    #[serde(
        rename = "failureReason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub failure_reason: Option<String>,
    #[serde(
        rename = "failureDetails",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub failure_details: Vec<u8>,
    #[serde(
        rename = "lastCompletionResult",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub last_completion_result: Vec<u8>,
    #[serde(
        rename = "cronSchedule",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cron_schedule: String,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
    #[serde(
        rename = "jitterStartSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub jitter_start_seconds: Option<i32>,
    #[serde(
        rename = "cronOverlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_overlap_policy: Option<CronOverlapPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StartChildWorkflowExecutionDecisionAttributes {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "executionStartToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "taskStartToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub task_start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "parentClosePolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_close_policy: Option<ParentClosePolicy>,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
    #[serde(
        rename = "workflowIdReusePolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_id_reuse_policy: Option<WorkflowIDReusePolicy>,
    #[serde(
        rename = "retryPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
    #[serde(
        rename = "cronSchedule",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cron_schedule: String,
    #[serde(
        rename = "cronOverlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_overlap_policy: Option<CronOverlapPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalExternalWorkflowExecutionDecisionAttributes {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "execution", default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<WorkflowExecution>,
    #[serde(
        rename = "signalName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub signal_name: String,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
    #[serde(rename = "childWorkflowOnly", default)]
    pub child_workflow_only: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpsertWorkflowSearchAttributesDecisionAttributes {
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCancelExternalWorkflowExecutionDecisionAttributes {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(rename = "runId", default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
    #[serde(rename = "childWorkflowOnly", default)]
    pub child_workflow_only: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowExecutionContinuedAsNewEventAttributes {
    #[serde(
        rename = "newExecutionRunId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub new_execution_run_id: String,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "executionStartToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "taskStartToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub task_start_to_close_timeout_seconds: Option<i32>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(
        rename = "backoffStartIntervalInSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub backoff_start_interval_in_seconds: Option<i32>,
    #[serde(rename = "initiator", default, skip_serializing_if = "Option::is_none")]
    pub initiator: Option<ContinueAsNewInitiator>,
    #[serde(
        rename = "failureReason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub failure_reason: Option<String>,
    #[serde(
        rename = "failureDetails",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub failure_details: Vec<u8>,
    #[serde(
        rename = "lastCompletionResult",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub last_completion_result: Vec<u8>,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
    #[serde(
        rename = "cronOverlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_overlap_policy: Option<CronOverlapPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StartChildWorkflowExecutionInitiatedEventAttributes {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "executionStartToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "taskStartToCloseTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub task_start_to_close_timeout_seconds: Option<i32>,
    #[serde(
        rename = "parentClosePolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_close_policy: Option<ParentClosePolicy>,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(
        rename = "workflowIdReusePolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_id_reuse_policy: Option<WorkflowIDReusePolicy>,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
    #[serde(
        rename = "retryPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(
        rename = "cronSchedule",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cron_schedule: String,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
    #[serde(
        rename = "delayStartSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub delay_start_seconds: Option<i32>,
    #[serde(
        rename = "jitterStartSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub jitter_start_seconds: Option<i32>,
    #[serde(
        rename = "firstRunAtTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub first_run_at_timestamp: Option<i64>,
    #[serde(
        rename = "cronOverlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_overlap_policy: Option<CronOverlapPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartChildWorkflowExecutionFailedEventAttributes {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "cause", default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<ChildWorkflowExecutionFailedCause>,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildWorkflowExecutionStartedEventAttributes {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildWorkflowExecutionCompletedEventAttributes {
    #[serde(
        rename = "result",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub result: Vec<u8>,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildWorkflowExecutionFailedEventAttributes {
    #[serde(rename = "reason", default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildWorkflowExecutionCanceledEventAttributes {
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildWorkflowExecutionTerminatedEventAttributes {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalExternalWorkflowExecutionInitiatedEventAttributes {
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "signalName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub signal_name: String,
    #[serde(
        rename = "input",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub input: Vec<u8>,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalExternalWorkflowExecutionFailedEventAttributes {
    #[serde(rename = "cause", default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<SignalExternalWorkflowExecutionFailedCause>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
    #[serde(rename = "childWorkflowOnly", default)]
    pub child_workflow_only: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCancelExternalWorkflowExecutionFailedEventAttributes {
    #[serde(rename = "cause", default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<CancelExternalWorkflowExecutionFailedCause>,
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpsertWorkflowSearchAttributesEventAttributes {
    #[serde(rename = "decisionTaskCompletedEventId", default)]
    pub decision_task_completed_event_id: i64,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerFiredEventAttributes {
    #[serde(rename = "timerId", default, skip_serializing_if = "String::is_empty")]
    pub timer_id: String,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
}

/// Recorded on the source when a signal is delivered to one of its
/// child workflows, mirroring Go history.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalWorkflowExecutionSignaledEventAttributes {
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
}

/// Recorded on the source when a cancel is delivered to one of its
/// child workflows, mirroring Go history.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalWorkflowExecutionCancelRequestedEventAttributes {
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowQuery {
    #[serde(
        rename = "queryType",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub query_type: String,
    #[serde(
        rename = "queryArgs",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub query_args: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowQueryResult {
    #[serde(
        rename = "resultType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub result_type: Option<QueryResultType>,
    #[serde(
        rename = "answer",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub answer: Vec<u8>,
    #[serde(
        rename = "errorMessage",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub error_message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryWorkflowRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "execution", default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<WorkflowExecution>,
    #[serde(rename = "query", default, skip_serializing_if = "Option::is_none")]
    pub query: Option<WorkflowQuery>,
    #[serde(
        rename = "queryRejectCondition",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub query_reject_condition: Option<QueryRejectCondition>,
    #[serde(
        rename = "queryConsistencyLevel",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub query_consistency_level: Option<QueryConsistencyLevel>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryWorkflowResponse {
    #[serde(
        rename = "queryResult",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub query_result: Vec<u8>,
    #[serde(
        rename = "queryRejected",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub query_rejected: Option<crate::errors::QueryRejected>,
}

/// Mirrors Go `types.RespondQueryTaskCompletedRequest` (`shared.thrift`
/// field IDs 10/20/30/40/50). Workers answer a directly-dispatched query
/// task with this; it unblocks the matching `QueryWorkflow` wait.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespondQueryTaskCompletedRequest {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(
        rename = "completedType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub completed_type: Option<QueryTaskCompletedType>,
    #[serde(
        rename = "queryResult",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub query_result: Vec<u8>,
    #[serde(
        rename = "errorMessage",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub error_message: String,
    #[serde(
        rename = "workerVersionInfo",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub worker_version_info: Option<crate::types::WorkerVersionInfo>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespondQueryTaskCompletedResponse {}

// ---------------------------------------------------------------------------
// Pending info (describe responses)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PendingActivityInfo {
    #[serde(
        rename = "activityID",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub activity_id: String,
    #[serde(
        rename = "activityType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_type: Option<ActivityType>,
    #[serde(rename = "state", default, skip_serializing_if = "Option::is_none")]
    pub state: Option<PendingActivityState>,
    #[serde(rename = "attempt", default)]
    pub attempt: i32,
    #[serde(
        rename = "scheduledTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scheduled_timestamp: Option<i64>,
    #[serde(
        rename = "lastStartedTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_started_timestamp: Option<i64>,
    #[serde(
        rename = "heartbeatDetails",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub heartbeat_details: Vec<u8>,
    #[serde(rename = "maximumAttempts", default)]
    pub maximum_attempts: i32,
    #[serde(rename = "scheduleID", default)]
    pub schedule_id: i64,
    #[serde(
        rename = "lastFailureReason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_failure_reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingDecisionInfo {
    #[serde(rename = "state", default, skip_serializing_if = "Option::is_none")]
    pub state: Option<PendingDecisionState>,
    #[serde(
        rename = "scheduledTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scheduled_timestamp: Option<i64>,
    #[serde(
        rename = "startedTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub started_timestamp: Option<i64>,
    #[serde(rename = "attempt", default)]
    pub attempt: i64,
    #[serde(rename = "scheduleID", default)]
    pub schedule_id: i64,
    #[serde(
        rename = "originalScheduledTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub original_scheduled_timestamp: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingChildExecutionInfo {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowID",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(rename = "runID", default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(
        rename = "workflowTypName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_type_name: String,
    #[serde(rename = "initiatedID", default)]
    pub initiated_id: i64,
    #[serde(
        rename = "parentClosePolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_close_policy: Option<ParentClosePolicy>,
}

// ---------------------------------------------------------------------------
// Task-list describe / search attributes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeTaskListRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(
        rename = "taskListType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub task_list_type: Option<TaskListType>,
    #[serde(rename = "includeTaskListStatus", default)]
    pub include_task_list_status: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DescribeTaskListResponse {
    #[serde(rename = "pollers", default, skip_serializing_if = "Vec::is_empty")]
    pub pollers: Vec<PollerInfo>,
    #[serde(
        rename = "taskListStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub task_list_status: Option<TaskListStatus>,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetSearchAttributesResponse {
    #[serde(rename = "keys", default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<std::collections::HashMap<String, IndexedValueType>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GetTaskListsByDomainRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GetTaskListsByDomainResponse {
    #[serde(
        rename = "decisionTaskListMap",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub decision_task_list_map: Option<std::collections::HashMap<String, DescribeTaskListResponse>>,
    #[serde(
        rename = "activityTaskListMap",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_task_list_map: Option<std::collections::HashMap<String, DescribeTaskListResponse>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListTaskListPartitionsRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskListPartitionMetadata {
    #[serde(rename = "key", default, skip_serializing_if = "String::is_empty")]
    pub key: String,
    #[serde(
        rename = "ownerHostName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub owner_host_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTaskListPartitionsResponse {
    #[serde(
        rename = "activityTaskListPartitions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub activity_task_list_partitions: Vec<TaskListPartitionMetadata>,
    #[serde(
        rename = "decisionTaskListPartitions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub decision_task_list_partitions: Vec<TaskListPartitionMetadata>,
}

// ---------------------------------------------------------------------------
// Activity heartbeat / cancel
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordActivityTaskHeartbeatRequest {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordActivityTaskHeartbeatResponse {
    #[serde(rename = "cancelRequested", default)]
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespondActivityTaskCanceledRequest {
    #[serde(
        rename = "taskToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub task_token: Vec<u8>,
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTaskCanceledEventAttributes {
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "latestCancelRequestedEventId", default)]
    pub latest_cancel_requested_event_id: i64,
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionTimedOutEventAttributes {
    #[serde(
        rename = "timeoutType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_type: Option<TimeoutType>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionTaskTimedOutEventAttributes {
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(
        rename = "timeoutType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_type: Option<TimeoutType>,
    #[serde(
        rename = "baseRunId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub base_run_id: String,
    #[serde(rename = "newRunId", default, skip_serializing_if = "String::is_empty")]
    pub new_run_id: String,
    #[serde(rename = "forkEventVersion", default)]
    pub fork_event_version: i64,
    #[serde(rename = "reason", default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(rename = "cause", default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<DecisionTaskTimedOutCause>,
    #[serde(
        rename = "requestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub request_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTaskTimedOutEventAttributes {
    #[serde(
        rename = "details",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub details: Vec<u8>,
    #[serde(rename = "scheduledEventId", default)]
    pub scheduled_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
    #[serde(
        rename = "timeoutType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_type: Option<TimeoutType>,
    #[serde(
        rename = "lastFailureReason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_failure_reason: Option<String>,
    #[serde(
        rename = "lastFailureDetails",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub last_failure_details: Vec<u8>,
    #[serde(
        rename = "lastFailureOptions",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_failure_options: Option<FailureOptions>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildWorkflowExecutionTimedOutEventAttributes {
    #[serde(
        rename = "timeoutType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_type: Option<TimeoutType>,
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "initiatedEventId", default)]
    pub initiated_event_id: i64,
    #[serde(rename = "startedEventId", default)]
    pub started_event_id: i64,
}
