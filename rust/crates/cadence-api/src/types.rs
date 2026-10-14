//! Core Cadence API structs ported from `go/common/types/shared.go`.
//!
//! `serde` renames match the Go `json` tags exactly (note: Thrift-generated
//! Go tags mix `camelCase` and `PascalCase`; this is preserved here for
//! wire compatibility).

use serde::{Deserialize, Serialize};

use crate::enums::*;

// ---------------------------------------------------------------------------
// Primitive / identity types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecution {
    #[serde(
        rename = "workflowId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(rename = "runId", default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
}

impl WorkflowExecution {
    #[must_use]
    pub fn new(workflow_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowType {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityType {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskList {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(rename = "kind", default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<TaskListKind>,
    #[serde(rename = "baseName", default, skip_serializing_if = "String::is_empty")]
    pub base_name: String,
}

impl TaskList {
    #[must_use]
    pub fn kind_or_default(&self) -> TaskListKind {
        self.kind.unwrap_or(TaskListKind::Normal)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskListMetadata {
    #[serde(
        rename = "maxTasksPerSecond",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub max_tasks_per_second: Option<f64>,
}

// ---------------------------------------------------------------------------
// Payload / blob types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataBlob {
    #[serde(
        rename = "EncodingType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub encoding_type: Option<EncodingType>,
    #[serde(
        rename = "Data",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub data: Vec<u8>,
}

impl DataBlob {
    #[must_use]
    pub fn new(encoding_type: EncodingType, data: Vec<u8>) -> Self {
        Self {
            encoding_type: Some(encoding_type),
            data,
        }
    }

    #[must_use]
    pub fn deep_copy(&self) -> Self {
        self.clone()
    }

    #[must_use]
    pub fn encoding_or_default(&self) -> EncodingType {
        self.encoding_type.unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    #[serde(
        rename = "fields",
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::b64::serialize_b64_map",
        deserialize_with = "crate::b64::deserialize_b64_map"
    )]
    pub fields: Option<std::collections::HashMap<String, Vec<u8>>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memo {
    #[serde(
        rename = "fields",
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::b64::serialize_b64_map",
        deserialize_with = "crate::b64::deserialize_b64_map"
    )]
    pub fields: Option<std::collections::HashMap<String, Vec<u8>>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchAttributes {
    #[serde(
        rename = "indexedFields",
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::b64::serialize_b64_map",
        deserialize_with = "crate::b64::deserialize_b64_map"
    )]
    pub indexed_fields: Option<std::collections::HashMap<String, Vec<u8>>>,
}

// ---------------------------------------------------------------------------
// Failure / retry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureOptions {
    #[serde(
        rename = "failureCategory",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub failure_category: Option<FailureCategory>,
    #[serde(
        rename = "nextRetryIntervalSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub next_retry_interval_seconds: Option<i32>,
}

impl FailureOptions {
    /// Mirrors Go `GetFailureCategory`: nil/unspecified defaults to Standard.
    #[must_use]
    pub fn failure_category_or_default(&self) -> FailureCategory {
        self.failure_category.unwrap_or(FailureCategory::Standard)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(rename = "initialIntervalInSeconds", default)]
    pub initial_interval_in_seconds: i32,
    #[serde(rename = "backoffCoefficient", default)]
    pub backoff_coefficient: f64,
    #[serde(rename = "maximumIntervalInSeconds", default)]
    pub maximum_interval_in_seconds: i32,
    #[serde(rename = "maximumAttempts", default)]
    pub maximum_attempts: i32,
    #[serde(
        rename = "nonRetriableErrorReasons",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub non_retriable_error_reasons: Vec<String>,
    #[serde(rename = "expirationIntervalInSeconds", default)]
    pub expiration_interval_in_seconds: i32,
    #[serde(rename = "jitterCoefficient", default)]
    pub jitter_coefficient: f64,
}

impl RetryPolicy {
    /// Mirrors Go validation defaults used by the server when fields are unset.
    #[must_use]
    pub fn effective_backoff_coefficient(&self) -> f64 {
        if self.backoff_coefficient == 0.0 {
            2.0
        } else {
            self.backoff_coefficient
        }
    }

    #[must_use]
    pub fn is_retriable_reason(&self, reason: &str) -> bool {
        !self.non_retriable_error_reasons.iter().any(|r| r == reason)
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BadBinaryInfo {
    #[serde(rename = "reason", default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(rename = "operator", default, skip_serializing_if = "String::is_empty")]
    pub operator: String,
    #[serde(
        rename = "createdTimeNano",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub created_time_nano: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BadBinaries {
    #[serde(rename = "binaries", default, skip_serializing_if = "Option::is_none")]
    pub binaries: Option<std::collections::HashMap<String, Option<BadBinaryInfo>>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainInfo {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(rename = "status", default, skip_serializing_if = "Option::is_none")]
    pub status: Option<DomainStatus>,
    #[serde(
        rename = "description",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub description: String,
    #[serde(
        rename = "ownerEmail",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub owner_email: String,
    #[serde(rename = "data", default, skip_serializing_if = "Option::is_none")]
    pub data: Option<std::collections::HashMap<String, String>>,
    #[serde(rename = "uuid", default, skip_serializing_if = "String::is_empty")]
    pub uuid: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainConfiguration {
    #[serde(rename = "workflowExecutionRetentionPeriodInDays", default)]
    pub workflow_execution_retention_period_in_days: i32,
    #[serde(rename = "emitMetric", default)]
    pub emit_metric: bool,
    #[serde(
        rename = "badBinaries",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub bad_binaries: Option<BadBinaries>,
    #[serde(
        rename = "historyArchivalStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub history_archival_status: Option<ArchivalStatus>,
    #[serde(
        rename = "historyArchivalURI",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub history_archival_uri: String,
    #[serde(
        rename = "visibilityArchivalStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub visibility_archival_status: Option<ArchivalStatus>,
    #[serde(
        rename = "visibilityArchivalURI",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub visibility_archival_uri: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterReplicationConfiguration {
    #[serde(
        rename = "clusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cluster_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveClusterInfo {
    #[serde(
        rename = "activeClusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub active_cluster_name: String,
    #[serde(rename = "failoverVersion", default)]
    pub failover_version: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterAttributeScope {
    #[serde(
        rename = "clusterAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cluster_attributes: Option<std::collections::HashMap<String, ActiveClusterInfo>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveClusters {
    #[serde(
        rename = "activeClustersByRegion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub active_clusters_by_region: Option<std::collections::HashMap<String, ActiveClusterInfo>>,
    #[serde(
        rename = "activeClustersByClusterAttribute",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub active_clusters_by_cluster_attribute:
        Option<std::collections::HashMap<String, ClusterAttributeScope>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainReplicationConfiguration {
    #[serde(
        rename = "activeClusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub active_cluster_name: String,
    #[serde(rename = "clusters", default, skip_serializing_if = "Vec::is_empty")]
    pub clusters: Vec<ClusterReplicationConfiguration>,
    #[serde(
        rename = "activeClusters",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub active_clusters: Option<ActiveClusters>,
}

// ---------------------------------------------------------------------------
// History types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryBranchRange {
    #[serde(rename = "branchID", default, skip_serializing_if = "String::is_empty")]
    pub branch_id: String,
    #[serde(rename = "beginNodeID", default)]
    pub begin_node_id: i64,
    #[serde(rename = "endNodeID", default)]
    pub end_node_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryBranch {
    #[serde(rename = "treeID", default, skip_serializing_if = "String::is_empty")]
    pub tree_id: String,
    #[serde(rename = "branchID", default, skip_serializing_if = "String::is_empty")]
    pub branch_id: String,
    #[serde(rename = "ancestors", default, skip_serializing_if = "Vec::is_empty")]
    pub ancestors: Vec<HistoryBranchRange>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionHistoryItem {
    #[serde(rename = "eventID", default)]
    pub event_id: i64,
    #[serde(rename = "version", default)]
    pub version: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionHistory {
    #[serde(rename = "branchToken", default, skip_serializing_if = "Vec::is_empty")]
    pub branch_token: Vec<u8>,
    #[serde(rename = "items", default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<VersionHistoryItem>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionHistories {
    #[serde(rename = "currentVersionHistoryIndex", default)]
    pub current_version_history_index: i32,
    #[serde(rename = "histories", default, skip_serializing_if = "Vec::is_empty")]
    pub histories: Vec<VersionHistory>,
}

// ---------------------------------------------------------------------------
// Poll / task types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StickyExecutionAttributes {
    #[serde(
        rename = "workerTaskList",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub worker_task_list: Option<TaskList>,
    #[serde(
        rename = "scheduleToStartTimeoutSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schedule_to_start_timeout_seconds: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PollerInfo {
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "lastAccessTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_access_time: Option<i64>,
    #[serde(rename = "ratePerSecond", default)]
    pub rate_per_second: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskListStatus {
    #[serde(rename = "backlogCountHint", default)]
    pub backlog_count_hint: i64,
    #[serde(rename = "readLevel", default)]
    pub read_level: i64,
    #[serde(rename = "ackLevel", default)]
    pub ack_level: i64,
    #[serde(rename = "ratePerSecond", default)]
    pub rate_per_second: f64,
    #[serde(
        rename = "taskIDBlock",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub task_id_block: Option<TaskIDBlock>,
    #[serde(rename = "newTasksPerSecond", default)]
    pub new_tasks_per_second: f64,
    #[serde(rename = "empty", default)]
    pub empty: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskIDBlock {
    #[serde(rename = "startID", default)]
    pub start_id: i64,
    #[serde(rename = "endID", default)]
    pub end_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportedClientVersions {
    #[serde(rename = "goSdk", default, skip_serializing_if = "String::is_empty")]
    pub go_sdk: String,
    #[serde(rename = "javaSdk", default, skip_serializing_if = "String::is_empty")]
    pub java_sdk: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerVersionInfo {
    #[serde(rename = "impl", default, skip_serializing_if = "String::is_empty")]
    pub impl_: String,
    #[serde(
        rename = "featureVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub feature_version: String,
}

// ---------------------------------------------------------------------------
// Request / response types (most-used surface)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StartWorkflowExecutionRequest {
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
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "requestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub request_id: String,
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
    #[serde(rename = "header", default, skip_serializing_if = "Option::is_none")]
    pub header: Option<Header>,
    #[serde(
        rename = "cronOverlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_overlap_policy: Option<CronOverlapPolicy>,
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartWorkflowExecutionResponse {
    #[serde(rename = "runId", default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalWorkflowExecutionRequest {
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
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "requestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub request_id: String,
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
pub struct TerminateWorkflowExecutionRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "reason", default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
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
        rename = "firstExecutionRunID",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub first_execution_run_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterDomainRequest {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(
        rename = "description",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub description: String,
    #[serde(
        rename = "ownerEmail",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub owner_email: String,
    #[serde(rename = "workflowExecutionRetentionPeriodInDays", default)]
    pub workflow_execution_retention_period_in_days: i32,
    #[serde(
        rename = "emitMetric",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub emit_metric: Option<bool>,
    #[serde(rename = "clusters", default, skip_serializing_if = "Vec::is_empty")]
    pub clusters: Vec<ClusterReplicationConfiguration>,
    #[serde(
        rename = "activeClusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub active_cluster_name: String,
    #[serde(rename = "data", default, skip_serializing_if = "Option::is_none")]
    pub data: Option<std::collections::HashMap<String, String>>,
    #[serde(rename = "isGlobalDomain", default)]
    pub is_global_domain: bool,
    #[serde(
        rename = "securityToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub security_token: String,
    #[serde(
        rename = "activeClusters",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub active_clusters: Option<ActiveClusters>,
    #[serde(
        rename = "historyArchivalStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub history_archival_status: Option<ArchivalStatus>,
    #[serde(
        rename = "historyArchivalURI",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub history_archival_uri: String,
    #[serde(
        rename = "visibilityArchivalStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub visibility_archival_status: Option<ArchivalStatus>,
    #[serde(
        rename = "visibilityArchivalURI",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub visibility_archival_uri: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_blob_json_names_match_go_tags() {
        let b = DataBlob::new(EncodingType::JSON, b"{}".to_vec());
        let v: serde_json::Value = serde_json::to_value(&b).unwrap();
        assert_eq!(v["EncodingType"], 1);
        assert!(v.get("Data").is_some());
    }

    #[test]
    fn failure_category_defaults_to_standard() {
        let o = FailureOptions::default();
        assert_eq!(o.failure_category_or_default(), FailureCategory::Standard);
        let o2 = FailureOptions {
            failure_category: Some(FailureCategory::Fatal),
            next_retry_interval_seconds: None,
        };
        assert_eq!(o2.failure_category_or_default(), FailureCategory::Fatal);
    }

    #[test]
    fn retry_policy_retriable_reasons() {
        let p = RetryPolicy {
            non_retriable_error_reasons: vec!["BadInput".to_string()],
            ..Default::default()
        };
        assert!(!p.is_retriable_reason("BadInput"));
        assert!(p.is_retriable_reason("Timeout"));
    }

    #[test]
    fn task_list_kind_defaults_to_normal() {
        let t = TaskList::default();
        assert_eq!(t.kind_or_default(), TaskListKind::Normal);
    }
}
