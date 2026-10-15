//! Schedule API types ported from `go/common/types/schedule.go` (Thrift wire
//! shapes from `go/idls/thrift/shared.thrift`).
//!
//! Field IDs and JSON names match Go exactly. Times are nanoseconds since the
//! epoch and durations whole seconds, per the Thrift IDL conventions.

use serde::{Deserialize, Serialize};

use crate::enums::{ScheduleCatchUpPolicy, ScheduleOverlapPolicy};
use crate::types::{Memo, RetryPolicy, SearchAttributes, TaskList, WorkflowType};

// ---------------------------------------------------------------------------
// Spec / action / policies
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleSpec {
    #[serde(
        rename = "cronExpression",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cron_expression: String,
    #[serde(rename = "startTime", default, skip_serializing_if = "Option::is_none")]
    pub start_time_nano: Option<i64>,
    #[serde(rename = "endTime", default, skip_serializing_if = "Option::is_none")]
    pub end_time_nano: Option<i64>,
    #[serde(rename = "jitter", default, skip_serializing_if = "Option::is_none")]
    pub jitter_in_seconds: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScheduleStartWorkflowAction {
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
        rename = "workflowIdPrefix",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id_prefix: String,
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
        rename = "retryPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScheduleAction {
    #[serde(
        rename = "startWorkflow",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_workflow: Option<ScheduleStartWorkflowAction>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulePolicies {
    #[serde(
        rename = "overlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub overlap_policy: Option<ScheduleOverlapPolicy>,
    #[serde(
        rename = "catchUpPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub catch_up_policy: Option<ScheduleCatchUpPolicy>,
    #[serde(
        rename = "catchUpWindow",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub catch_up_window_in_seconds: Option<i32>,
    #[serde(rename = "pauseOnFailure", default)]
    pub pause_on_failure: bool,
    #[serde(rename = "bufferLimit", default)]
    pub buffer_limit: i32,
    #[serde(rename = "concurrencyLimit", default)]
    pub concurrency_limit: i32,
}

// ---------------------------------------------------------------------------
// State / info
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulePauseInfo {
    #[serde(rename = "reason", default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(rename = "pausedTime", default, skip_serializing_if = "Option::is_none")]
    pub paused_time_nano: Option<i64>,
    #[serde(
        rename = "pausedBy",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub paused_by: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleState {
    #[serde(rename = "paused", default)]
    pub paused: bool,
    #[serde(rename = "pauseInfo", default, skip_serializing_if = "Option::is_none")]
    pub pause_info: Option<SchedulePauseInfo>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillInfo {
    #[serde(
        rename = "backfillId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub backfill_id: String,
    #[serde(
        rename = "startTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_time_nano: Option<i64>,
    #[serde(rename = "endTime", default, skip_serializing_if = "Option::is_none")]
    pub end_time_nano: Option<i64>,
    #[serde(rename = "runsCompleted", default)]
    pub runs_completed: i32,
    #[serde(rename = "runsTotal", default)]
    pub runs_total: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleInfo {
    #[serde(
        rename = "lastRunTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_run_time_nano: Option<i64>,
    #[serde(
        rename = "nextRunTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub next_run_time_nano: Option<i64>,
    #[serde(rename = "totalRuns", default)]
    pub total_runs: i64,
    #[serde(rename = "createTime", default, skip_serializing_if = "Option::is_none")]
    pub create_time_nano: Option<i64>,
    #[serde(
        rename = "lastUpdateTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_update_time_nano: Option<i64>,
    #[serde(
        rename = "ongoingBackfills",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub ongoing_backfills: Vec<BackfillInfo>,
    #[serde(rename = "missedRuns", default)]
    pub missed_runs: i64,
    #[serde(rename = "skippedRuns", default)]
    pub skipped_runs: i64,
    #[serde(rename = "bufferedFireCount", default)]
    pub buffered_fire_count: i64,
    #[serde(rename = "runningWorkflowCount", default)]
    pub running_workflow_count: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScheduleListEntry {
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "state", default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ScheduleState>,
    #[serde(
        rename = "cronExpression",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cron_expression: String,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
}

// ---------------------------------------------------------------------------
// Requests / responses (Thrift field IDs in convert.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CreateScheduleRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
    #[serde(rename = "spec", default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ScheduleSpec>,
    #[serde(rename = "action", default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ScheduleAction>,
    #[serde(
        rename = "policies",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub policies: Option<SchedulePolicies>,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
    #[serde(rename = "state", default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ScheduleState>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateScheduleResponse {
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeScheduleRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DescribeScheduleResponse {
    #[serde(rename = "spec", default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ScheduleSpec>,
    #[serde(rename = "action", default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ScheduleAction>,
    #[serde(
        rename = "policies",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub policies: Option<SchedulePolicies>,
    #[serde(rename = "state", default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ScheduleState>,
    #[serde(rename = "info", default, skip_serializing_if = "Option::is_none")]
    pub info: Option<ScheduleInfo>,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListSchedulesRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "pageSize", default)]
    pub page_size: i32,
    #[serde(
        rename = "nextPageToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListSchedulesResponse {
    #[serde(rename = "schedules", default, skip_serializing_if = "Vec::is_empty")]
    pub schedules: Vec<ScheduleListEntry>,
    #[serde(
        rename = "nextPageToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteScheduleRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteScheduleResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PauseScheduleRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
    #[serde(rename = "reason", default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(
        rename = "identity",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PauseScheduleResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnpauseScheduleRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
    #[serde(rename = "reason", default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(
        rename = "catchUpPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub catch_up_policy: Option<ScheduleCatchUpPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnpauseScheduleResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillScheduleRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
    #[serde(
        rename = "startTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_time_nano: Option<i64>,
    #[serde(rename = "endTime", default, skip_serializing_if = "Option::is_none")]
    pub end_time_nano: Option<i64>,
    #[serde(
        rename = "overlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub overlap_policy: Option<ScheduleOverlapPolicy>,
    #[serde(
        rename = "backfillId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub backfill_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillScheduleResponse {}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UpdateScheduleRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "scheduleId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub schedule_id: String,
    #[serde(rename = "spec", default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ScheduleSpec>,
    #[serde(rename = "action", default, skip_serializing_if = "Option::is_none")]
    pub action: Option<ScheduleAction>,
    #[serde(
        rename = "policies",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub policies: Option<SchedulePolicies>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateScheduleResponse {}
