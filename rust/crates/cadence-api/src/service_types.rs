//! Service request/response types ported from `go/common/types/shared.go`.
//!
//! `serde` renames match the Go `json` tags exactly.

use serde::{Deserialize, Serialize};

use crate::enums::*;
use crate::types::*;

// ---------------------------------------------------------------------------
// Domain CRUD
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeDomainRequest {
    #[serde(rename = "name", default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "uuid", default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeDomainResponse {
    #[serde(
        rename = "domainInfo",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub domain_info: Option<DomainInfo>,
    #[serde(
        rename = "configuration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub configuration: Option<DomainConfiguration>,
    #[serde(
        rename = "replicationConfiguration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replication_configuration: Option<DomainReplicationConfiguration>,
    #[serde(rename = "failoverVersion", default)]
    pub failover_version: i64,
    #[serde(rename = "isGlobalDomain", default)]
    pub is_global_domain: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListDomainsRequest {
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListDomainsResponse {
    #[serde(rename = "domains", default, skip_serializing_if = "Vec::is_empty")]
    pub domains: Vec<DescribeDomainResponse>,
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
pub struct DeprecateDomainRequest {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(
        rename = "securityToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub security_token: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteDomainRequest {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(
        rename = "securityToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub security_token: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountWorkflowExecutionsResponse {
    #[serde(rename = "count", default)]
    pub count: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListWorkflowExecutionsRequest {
    #[serde(default)]
    pub domain: String,
    #[serde(rename = "pageSize", default)]
    pub page_size: i32,
    #[serde(
        rename = "nextPageToken",
        default,
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
    #[serde(default)]
    pub query: String,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListArchivedWorkflowExecutionsRequest {
    #[serde(default)]
    pub domain: String,
    #[serde(rename = "pageSize", default)]
    pub page_size: i32,
    #[serde(
        rename = "nextPageToken",
        default,
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
    #[serde(default)]
    pub query: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListArchivedWorkflowExecutionsResponse {
    #[serde(default)]
    pub executions: Vec<WorkflowExecutionInfo>,
    #[serde(
        rename = "nextPageToken",
        default,
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListWorkflowExecutionsResponse {
    #[serde(default)]
    pub executions: Vec<WorkflowExecutionInfo>,
    #[serde(
        rename = "nextPageToken",
        default,
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
}
pub type ScanWorkflowExecutionsRequest = ListWorkflowExecutionsRequest;
pub type ScanWorkflowExecutionsResponse = ListWorkflowExecutionsResponse;
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountWorkflowExecutionsRequest {
    #[serde(default)]
    pub domain: String,
    #[serde(default)]
    pub query: String,
}

/// Optional fields retain update presence; response configuration uses scalars.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainConfigurationUpdate {
    #[serde(
        rename = "workflowExecutionRetentionPeriodInDays",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_retention_period_in_days: Option<i32>,
    #[serde(
        rename = "emitMetric",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub emit_metric: Option<bool>,
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
        skip_serializing_if = "Option::is_none"
    )]
    pub history_archival_uri: Option<String>,
    #[serde(
        rename = "visibilityArchivalStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub visibility_archival_status: Option<ArchivalStatus>,
    #[serde(
        rename = "visibilityArchivalURI",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub visibility_archival_uri: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainReplicationConfigurationUpdate {
    #[serde(
        rename = "activeClusterName",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub active_cluster_name: Option<String>,
    #[serde(rename = "clusters", default, skip_serializing_if = "Option::is_none")]
    pub clusters: Option<Vec<ClusterReplicationConfiguration>>,
    #[serde(
        rename = "activeClusters",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub active_clusters: Option<ActiveClusters>,
}

/// Wire shape of the Thrift `UpdateDomainRequest` (field IDs 10-70).
/// The config-update predicate lives on a lighter struct in [`crate::domain`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDomainInfo {
    #[serde(
        rename = "description",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<String>,
    #[serde(
        rename = "ownerEmail",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub owner_email: Option<String>,
    #[serde(rename = "data", default, skip_serializing_if = "Option::is_none")]
    pub data: Option<std::collections::HashMap<String, String>>,
}

/// Wire shape of the Thrift `UpdateDomainRequest` (field IDs 10-70).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDomainRequest {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(
        rename = "updatedInfo",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_info: Option<UpdateDomainInfo>,
    #[serde(
        rename = "configuration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub configuration: Option<DomainConfigurationUpdate>,
    #[serde(
        rename = "replicationConfiguration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replication_configuration: Option<DomainReplicationConfigurationUpdate>,
    #[serde(
        rename = "securityToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub security_token: String,
    #[serde(
        rename = "deleteBadBinary",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub delete_bad_binary: Option<String>,
    #[serde(
        rename = "failoverTimeoutInSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub failover_timeout_in_seconds: Option<i32>,
}

impl UpdateDomainRequest {
    pub fn is_config_update(&self) -> bool {
        self.updated_info
            .as_ref()
            .is_some_and(|i| i.description.is_some() || i.owner_email.is_some() || i.data.is_some())
            || self.configuration.as_ref().is_some_and(|c| {
                c.emit_metric.is_some()
                    || c.workflow_execution_retention_period_in_days.is_some()
                    || c.bad_binaries.is_some()
                    || c.history_archival_status.is_some()
                    || c.history_archival_uri.is_some()
                    || c.visibility_archival_status.is_some()
                    || c.visibility_archival_uri.is_some()
            })
            || self.delete_bad_binary.is_some()
    }
    pub fn is_failover(&self) -> bool {
        self.replication_configuration
            .as_ref()
            .is_some_and(|r| r.active_cluster_name.is_some() || r.active_clusters.is_some())
    }
}

/// Wire shape of the Thrift `UpdateDomainResponse`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDomainResponse {
    #[serde(
        rename = "domainInfo",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub domain_info: Option<DomainInfo>,
    #[serde(
        rename = "configuration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub configuration: Option<DomainConfiguration>,
    #[serde(
        rename = "replicationConfiguration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replication_configuration: Option<DomainReplicationConfiguration>,
    #[serde(rename = "failoverVersion", default)]
    pub failover_version: i64,
    #[serde(rename = "isGlobalDomain", default)]
    pub is_global_domain: bool,
}

// ---------------------------------------------------------------------------
// Workflow describe / list
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionConfiguration {
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionInfo {
    #[serde(rename = "execution", default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<WorkflowExecution>,
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub workflow_type: Option<WorkflowType>,
    #[serde(rename = "startTime", default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    #[serde(rename = "closeTime", default, skip_serializing_if = "Option::is_none")]
    pub close_time: Option<i64>,
    #[serde(
        rename = "closeStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub close_status: Option<WorkflowExecutionCloseStatus>,
    #[serde(rename = "historyLength", default)]
    pub history_length: i64,
    #[serde(rename = "memo", default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<Memo>,
    #[serde(
        rename = "parentDomain",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_domain: Option<String>,
    #[serde(
        rename = "parentExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "parentInitiatedID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_initiated_id: Option<i64>,
    #[serde(
        rename = "executionTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_time: Option<i64>,
    #[serde(
        rename = "searchAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attributes: Option<SearchAttributes>,
    #[serde(rename = "taskList", default, skip_serializing_if = "Option::is_none")]
    pub task_list: Option<TaskList>,
    #[serde(rename = "isCron", default)]
    pub is_cron: bool,
    #[serde(
        rename = "cronSchedule",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_schedule: Option<String>,
    #[serde(
        rename = "cronOverlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_overlap_policy: Option<CronOverlapPolicy>,
    #[serde(
        rename = "updateTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub update_time: Option<i64>,
    #[serde(
        rename = "partitionConfig",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub partition_config: Option<std::collections::HashMap<String, String>>,
    #[serde(
        rename = "executionStatus",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_status: Option<WorkflowExecutionStatus>,
    #[serde(
        rename = "scheduledExecutionTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scheduled_execution_time: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeWorkflowExecutionRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "execution", default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<WorkflowExecution>,
    #[serde(
        rename = "queryConsistencyLevel",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub query_consistency_level: Option<QueryConsistencyLevel>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DescribeWorkflowExecutionResponse {
    #[serde(
        rename = "executionConfiguration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_configuration: Option<WorkflowExecutionConfiguration>,
    #[serde(
        rename = "workflowExecutionInfo",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_info: Option<WorkflowExecutionInfo>,
    #[serde(
        rename = "pendingActivities",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub pending_activities: Vec<crate::tasks::PendingActivityInfo>,
    #[serde(
        rename = "pendingChildren",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub pending_children: Vec<crate::tasks::PendingChildExecutionInfo>,
    #[serde(
        rename = "pendingDecision",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub pending_decision: Option<crate::tasks::PendingDecisionInfo>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartTimeFilter {
    #[serde(
        rename = "earliestTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub earliest_time: Option<i64>,
    #[serde(
        rename = "latestTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub latest_time: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionFilter {
    #[serde(
        rename = "workflowId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(rename = "runId", default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTypeFilter {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

fn b64_token_default() -> Vec<u8> {
    Vec::new()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListOpenWorkflowExecutionsRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "maximumPageSize", default)]
    pub maximum_page_size: i32,
    #[serde(
        rename = "nextPageToken",
        default = "b64_token_default",
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
    #[serde(
        rename = "StartTimeFilter",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_time_filter: Option<StartTimeFilter>,
    #[serde(
        rename = "executionFilter",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_filter: Option<WorkflowExecutionFilter>,
    #[serde(
        rename = "typeFilter",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub type_filter: Option<WorkflowTypeFilter>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListOpenWorkflowExecutionsResponse {
    #[serde(rename = "executions", default, skip_serializing_if = "Vec::is_empty")]
    pub executions: Vec<WorkflowExecutionInfo>,
    #[serde(
        rename = "nextPageToken",
        default = "b64_token_default",
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListClosedWorkflowExecutionsRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "maximumPageSize", default)]
    pub maximum_page_size: i32,
    #[serde(
        rename = "nextPageToken",
        default = "b64_token_default",
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
    #[serde(
        rename = "StartTimeFilter",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_time_filter: Option<StartTimeFilter>,
    #[serde(
        rename = "executionFilter",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_filter: Option<WorkflowExecutionFilter>,
    #[serde(
        rename = "typeFilter",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub type_filter: Option<WorkflowTypeFilter>,
    #[serde(
        rename = "statusFilter",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub status_filter: Option<WorkflowExecutionCloseStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListClosedWorkflowExecutionsResponse {
    #[serde(rename = "executions", default, skip_serializing_if = "Vec::is_empty")]
    pub executions: Vec<WorkflowExecutionInfo>,
    #[serde(
        rename = "nextPageToken",
        default = "b64_token_default",
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCancelWorkflowExecutionRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "requestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub request_id: String,
    #[serde(rename = "cause", default, skip_serializing_if = "String::is_empty")]
    pub cause: String,
    #[serde(
        rename = "firstExecutionRunID",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub first_execution_run_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SignalWithStartWorkflowExecutionAsyncRequest {
    #[serde(rename = "request", default, skip_serializing_if = "Option::is_none")]
    pub request: Option<SignalWithStartWorkflowExecutionRequest>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalWithStartWorkflowExecutionAsyncResponse {}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SignalWithStartWorkflowExecutionRequest {
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
        rename = "control",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub control: Vec<u8>,
    #[serde(
        rename = "signalName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub signal_name: String,
    #[serde(
        rename = "signalInput",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub signal_input: Vec<u8>,
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

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowExecutionStartedEventAttributes {
    #[serde(
        rename = "workflowType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_type: Option<WorkflowType>,
    #[serde(
        rename = "parentWorkflowDomain",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_workflow_domain: Option<String>,
    #[serde(
        rename = "parentWorkflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_workflow_execution: Option<WorkflowExecution>,
    #[serde(
        rename = "parentInitiatedEventId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_initiated_event_id: Option<i64>,
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
        rename = "continuedExecutionRunId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub continued_execution_run_id: String,
    #[serde(rename = "initiator", default, skip_serializing_if = "Option::is_none")]
    pub initiator: Option<ContinueAsNewInitiator>,
    #[serde(
        rename = "continuedFailureReason",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub continued_failure_reason: String,
    #[serde(
        rename = "continuedFailureDetails",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub continued_failure_details: Vec<u8>,
    #[serde(
        rename = "lastCompletionResult",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub last_completion_result: Vec<u8>,
    #[serde(
        rename = "originalExecutionRunId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub original_execution_run_id: String,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "firstExecutionRunId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub first_execution_run_id: String,
    #[serde(
        rename = "firstScheduledTimeNano",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub first_scheduled_time_nano: Option<i64>,
    #[serde(
        rename = "retryPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_policy: Option<RetryPolicy>,
    #[serde(rename = "attempt", default)]
    pub attempt: i32,
    #[serde(
        rename = "expirationTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub expiration_timestamp: Option<i64>,
    #[serde(
        rename = "cronSchedule",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cron_schedule: String,
    #[serde(
        rename = "firstDecisionTaskBackoffSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub first_decision_task_backoff_seconds: Option<i32>,
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
        rename = "partitionConfig",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub partition_config: Option<std::collections::HashMap<String, String>>,
    #[serde(
        rename = "requestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub request_id: String,
    #[serde(
        rename = "cronOverlapPolicy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_overlap_policy: Option<CronOverlapPolicy>,
    // Present in Go `common/types` but absent from the Thrift wire IDL, so
    // this never appears in Thrift conversions. Kept for restart/cron
    // fidelity in JSON snapshots and the gRPC path.
    #[serde(
        rename = "jitterStartSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub jitter_start_seconds: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionSignaledEventAttributes {
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionTerminatedEventAttributes {
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowExecutionCancelRequestedEventAttributes {
    #[serde(rename = "cause", default, skip_serializing_if = "String::is_empty")]
    pub cause: String,
    #[serde(
        rename = "externalInitiatedEventId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub external_initiated_event_id: Option<i64>,
    #[serde(
        rename = "externalWorkflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub external_workflow_execution: Option<WorkflowExecution>,
    #[serde(rename = "identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
    #[serde(
        rename = "requestId",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub request_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HistoryEvent {
    #[serde(rename = "eventId", default)]
    pub event_id: i64,
    #[serde(rename = "timestamp", default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
    #[serde(rename = "eventType", default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<EventType>,
    #[serde(rename = "version", default)]
    pub version: i64,
    #[serde(rename = "taskId", default)]
    pub task_id: i64,
    #[serde(
        rename = "workflowExecutionStartedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_started_event_attributes:
        Option<WorkflowExecutionStartedEventAttributes>,
    #[serde(
        rename = "workflowExecutionSignaledEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_signaled_event_attributes:
        Option<WorkflowExecutionSignaledEventAttributes>,
    #[serde(
        rename = "workflowExecutionTerminatedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_terminated_event_attributes:
        Option<WorkflowExecutionTerminatedEventAttributes>,
    #[serde(
        rename = "workflowExecutionCancelRequestedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_cancel_requested_event_attributes:
        Option<WorkflowExecutionCancelRequestedEventAttributes>,
    #[serde(
        rename = "workflowExecutionCompletedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_completed_event_attributes:
        Option<crate::tasks::WorkflowExecutionCompletedEventAttributes>,
    #[serde(
        rename = "workflowExecutionFailedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_failed_event_attributes:
        Option<crate::tasks::WorkflowExecutionFailedEventAttributes>,
    #[serde(
        rename = "workflowExecutionCanceledEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_canceled_event_attributes:
        Option<crate::tasks::WorkflowExecutionCanceledEventAttributes>,
    #[serde(
        rename = "decisionTaskScheduledEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub decision_task_scheduled_event_attributes:
        Option<crate::tasks::DecisionTaskScheduledEventAttributes>,
    #[serde(
        rename = "decisionTaskStartedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub decision_task_started_event_attributes:
        Option<crate::tasks::DecisionTaskStartedEventAttributes>,
    #[serde(
        rename = "decisionTaskCompletedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub decision_task_completed_event_attributes:
        Option<crate::tasks::DecisionTaskCompletedEventAttributes>,
    #[serde(
        rename = "decisionTaskFailedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub decision_task_failed_event_attributes:
        Option<crate::tasks::DecisionTaskFailedEventAttributes>,
    #[serde(
        rename = "activityTaskScheduledEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_task_scheduled_event_attributes:
        Option<crate::tasks::ActivityTaskScheduledEventAttributes>,
    #[serde(
        rename = "activityTaskStartedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_task_started_event_attributes:
        Option<crate::tasks::ActivityTaskStartedEventAttributes>,
    #[serde(
        rename = "activityTaskCompletedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_task_completed_event_attributes:
        Option<crate::tasks::ActivityTaskCompletedEventAttributes>,
    #[serde(
        rename = "activityTaskFailedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_task_failed_event_attributes:
        Option<crate::tasks::ActivityTaskFailedEventAttributes>,
    #[serde(
        rename = "activityTaskCancelRequestedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_task_cancel_requested_event_attributes:
        Option<crate::tasks::ActivityTaskCancelRequestedEventAttributes>,
    #[serde(
        rename = "activityTaskCanceledEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_task_canceled_event_attributes:
        Option<crate::tasks::ActivityTaskCanceledEventAttributes>,
    #[serde(
        rename = "timerStartedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timer_started_event_attributes: Option<crate::tasks::TimerStartedEventAttributes>,
    #[serde(
        rename = "timerCanceledEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timer_canceled_event_attributes: Option<crate::tasks::TimerCanceledEventAttributes>,
    #[serde(
        rename = "cancelTimerFailedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub cancel_timer_failed_event_attributes:
        Option<crate::tasks::CancelTimerFailedEventAttributes>,
    #[serde(
        rename = "markerRecordedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub marker_recorded_event_attributes: Option<crate::tasks::MarkerRecordedEventAttributes>,
    #[serde(
        rename = "workflowExecutionContinuedAsNewEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_continued_as_new_event_attributes:
        Option<crate::tasks::WorkflowExecutionContinuedAsNewEventAttributes>,
    #[serde(
        rename = "startChildWorkflowExecutionInitiatedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_child_workflow_execution_initiated_event_attributes:
        Option<crate::tasks::StartChildWorkflowExecutionInitiatedEventAttributes>,
    #[serde(
        rename = "startChildWorkflowExecutionFailedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_child_workflow_execution_failed_event_attributes:
        Option<crate::tasks::StartChildWorkflowExecutionFailedEventAttributes>,
    #[serde(
        rename = "childWorkflowExecutionStartedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub child_workflow_execution_started_event_attributes:
        Option<crate::tasks::ChildWorkflowExecutionStartedEventAttributes>,
    #[serde(
        rename = "childWorkflowExecutionCompletedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub child_workflow_execution_completed_event_attributes:
        Option<crate::tasks::ChildWorkflowExecutionCompletedEventAttributes>,
    #[serde(
        rename = "childWorkflowExecutionFailedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub child_workflow_execution_failed_event_attributes:
        Option<crate::tasks::ChildWorkflowExecutionFailedEventAttributes>,
    #[serde(
        rename = "childWorkflowExecutionCanceledEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub child_workflow_execution_canceled_event_attributes:
        Option<crate::tasks::ChildWorkflowExecutionCanceledEventAttributes>,
    #[serde(
        rename = "childWorkflowExecutionTerminatedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub child_workflow_execution_terminated_event_attributes:
        Option<crate::tasks::ChildWorkflowExecutionTerminatedEventAttributes>,
    #[serde(
        rename = "signalExternalWorkflowExecutionInitiatedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub signal_external_workflow_execution_initiated_event_attributes:
        Option<crate::tasks::SignalExternalWorkflowExecutionInitiatedEventAttributes>,
    #[serde(
        rename = "signalExternalWorkflowExecutionFailedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub signal_external_workflow_execution_failed_event_attributes:
        Option<crate::tasks::SignalExternalWorkflowExecutionFailedEventAttributes>,
    #[serde(
        rename = "requestCancelExternalWorkflowExecutionInitiatedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub request_cancel_external_workflow_execution_initiated_event_attributes:
        Option<crate::tasks::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes>,
    #[serde(
        rename = "requestCancelExternalWorkflowExecutionFailedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub request_cancel_external_workflow_execution_failed_event_attributes:
        Option<crate::tasks::RequestCancelExternalWorkflowExecutionFailedEventAttributes>,
    #[serde(
        rename = "upsertWorkflowSearchAttributesEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub upsert_workflow_search_attributes_event_attributes:
        Option<crate::tasks::UpsertWorkflowSearchAttributesEventAttributes>,
    #[serde(
        rename = "timerFiredEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub timer_fired_event_attributes: Option<crate::tasks::TimerFiredEventAttributes>,
    #[serde(
        rename = "externalWorkflowExecutionSignaledEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub external_workflow_execution_signaled_event_attributes:
        Option<crate::tasks::ExternalWorkflowExecutionSignaledEventAttributes>,
    #[serde(
        rename = "externalWorkflowExecutionCancelRequestedEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub external_workflow_execution_cancel_requested_event_attributes:
        Option<crate::tasks::ExternalWorkflowExecutionCancelRequestedEventAttributes>,
    #[serde(
        rename = "workflowExecutionTimedOutEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution_timed_out_event_attributes:
        Option<crate::tasks::WorkflowExecutionTimedOutEventAttributes>,

    #[serde(
        rename = "decisionTaskTimedOutEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub decision_task_timed_out_event_attributes:
        Option<crate::tasks::DecisionTaskTimedOutEventAttributes>,

    #[serde(
        rename = "activityTaskTimedOutEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub activity_task_timed_out_event_attributes:
        Option<crate::tasks::ActivityTaskTimedOutEventAttributes>,

    #[serde(
        rename = "childWorkflowExecutionTimedOutEventAttributes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub child_workflow_execution_timed_out_event_attributes:
        Option<crate::tasks::ChildWorkflowExecutionTimedOutEventAttributes>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct History {
    #[serde(rename = "events", default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<HistoryEvent>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetWorkflowExecutionHistoryRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "execution", default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<WorkflowExecution>,
    #[serde(rename = "maximumPageSize", default)]
    pub maximum_page_size: i32,
    #[serde(
        rename = "nextPageToken",
        default = "b64_token_default",
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
    #[serde(rename = "waitForNewEvent", default)]
    pub wait_for_new_event: bool,
    #[serde(
        rename = "HistoryEventFilterType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub history_event_filter_type: Option<HistoryEventFilterType>,
    #[serde(rename = "skipArchival", default)]
    pub skip_archival: bool,
    #[serde(
        rename = "queryConsistencyLevel",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub query_consistency_level: Option<QueryConsistencyLevel>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GetWorkflowExecutionHistoryResponse {
    #[serde(rename = "history", default, skip_serializing_if = "Option::is_none")]
    pub history: Option<History>,
    #[serde(
        rename = "nextPageToken",
        default = "b64_token_default",
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
    #[serde(rename = "archived", default)]
    pub archived: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_domain_request_names_match_go() {
        let r = DescribeDomainRequest {
            name: Some("d".into()),
            uuid: None,
        };
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["name"], "d");
        assert!(v.get("uuid").is_none());
    }

    #[test]
    fn history_event_names_match_go() {
        let e = HistoryEvent {
            event_id: 1,
            event_type: Some(EventType::WorkflowExecutionStarted),
            ..Default::default()
        };
        let v: serde_json::Value = serde_json::to_value(&e).unwrap();
        assert_eq!(v["eventId"], 1);
        assert_eq!(v["eventType"], 0);
    }

    #[test]
    fn execution_info_type_key_is_go_literal() {
        let i = WorkflowExecutionInfo {
            workflow_type: Some(WorkflowType { name: "wt".into() }),
            ..Default::default()
        };
        let v: serde_json::Value = serde_json::to_value(&i).unwrap();
        assert_eq!(v["type"]["name"], "wt");
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetStickyTaskListRequest {
    #[serde(default)]
    pub domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<WorkflowExecution>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetStickyTaskListResponse {}
