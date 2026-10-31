//! Admin API types ported from `go/idls/proto/uber/cadence/admin/v1/`
//! (service, cluster, history, queue and replication protos) with JSON names
//! matching Go's `common/types` tags.
//!
//! This module grows as AdminAPI adapters land; unported methods remain
//! listed in `memory/migration-inventory.md`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostInfo {
    // Go's tag is capital-I `Identity`.
    #[serde(rename = "Identity", default, skip_serializing_if = "String::is_empty")]
    pub identity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RingInfo {
    #[serde(rename = "role", default, skip_serializing_if = "String::is_empty")]
    pub role: String,
    #[serde(rename = "memberCount", default)]
    pub member_count: i32,
    #[serde(rename = "members", default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<HostInfo>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipInfo {
    #[serde(
        rename = "currentHost",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub current_host: Option<HostInfo>,
    #[serde(
        rename = "reachableMembers",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub reachable_members: Vec<String>,
    #[serde(rename = "rings", default, skip_serializing_if = "Vec::is_empty")]
    pub rings: Vec<RingInfo>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceFeature {
    #[serde(rename = "key", default, skip_serializing_if = "String::is_empty")]
    pub key: String,
    #[serde(rename = "enabled", default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceInfo {
    #[serde(rename = "backend", default)]
    pub backend: String,
    #[serde(rename = "features", default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<PersistenceFeature>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeClusterResponse {
    #[serde(
        rename = "supportedClientVersions",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub supported_client_versions: Option<crate::types::SupportedClientVersions>,
    #[serde(
        rename = "membershipInfo",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub membership_info: Option<MembershipInfo>,
    #[serde(
        rename = "persistenceInfo",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub persistence_info: Option<std::collections::HashMap<String, PersistenceInfo>>,
}

// ---------------------------------------------------------------------------
// Workflow maintenance
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteWorkflowRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<crate::types::WorkflowExecution>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteWorkflowResponse {
    #[serde(rename = "historyDeleted", default)]
    pub history_deleted: bool,
    #[serde(rename = "executionsDeleted", default)]
    pub executions_deleted: bool,
    #[serde(rename = "visibilityDeleted", default)]
    pub visibility_deleted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RefreshWorkflowTasksRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(rename = "domainId", default, skip_serializing_if = "String::is_empty")]
    pub domain_id: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<crate::types::WorkflowExecution>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshWorkflowTasksResponse {}

// ---------------------------------------------------------------------------
// Search attributes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AddSearchAttributeRequest {
    #[serde(
        rename = "searchAttribute",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub search_attribute: Option<std::collections::HashMap<String, crate::enums::IndexedValueType>>,
    #[serde(
        rename = "securityToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub security_token: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddSearchAttributeResponse {}

// ---------------------------------------------------------------------------
// Dynamic config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DynamicConfigFilter {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(rename = "value", default, skip_serializing_if = "Option::is_none")]
    pub value: Option<crate::types::DataBlob>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DynamicConfigValue {
    #[serde(rename = "value", default, skip_serializing_if = "Option::is_none")]
    pub value: Option<crate::types::DataBlob>,
    #[serde(rename = "filters", default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<DynamicConfigFilter>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DynamicConfigEntry {
    #[serde(rename = "name", default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(rename = "values", default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<DynamicConfigValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GetDynamicConfigRequest {
    #[serde(
        rename = "configName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub config_name: String,
    #[serde(rename = "filters", default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<DynamicConfigFilter>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GetDynamicConfigResponse {
    #[serde(rename = "value", default, skip_serializing_if = "Option::is_none")]
    pub value: Option<crate::types::DataBlob>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UpdateDynamicConfigRequest {
    #[serde(
        rename = "configName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub config_name: String,
    #[serde(
        rename = "configValues",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub config_values: Vec<DynamicConfigValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDynamicConfigResponse {}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RestoreDynamicConfigRequest {
    #[serde(
        rename = "configName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub config_name: String,
    #[serde(rename = "filters", default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<DynamicConfigFilter>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreDynamicConfigResponse {}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListDynamicConfigRequest {
    #[serde(
        rename = "configName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub config_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListDynamicConfigResponse {
    #[serde(rename = "entries", default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<DynamicConfigEntry>,
}

// The operational-config Thrift messages have the same fields as their
// regular-config counterparts; Go distinguishes them by the backing client
// and namespace rather than by a different wire shape.
pub type GetOperationalDynamicConfigRequest = GetDynamicConfigRequest;
pub type GetOperationalDynamicConfigResponse = GetDynamicConfigResponse;
pub type UpdateOperationalDynamicConfigRequest = UpdateDynamicConfigRequest;
pub type UpdateOperationalDynamicConfigResponse = UpdateDynamicConfigResponse;
pub type RestoreOperationalDynamicConfigRequest = RestoreDynamicConfigRequest;
pub type RestoreOperationalDynamicConfigResponse = RestoreDynamicConfigResponse;
pub type ListOperationalDynamicConfigRequest = ListDynamicConfigRequest;
pub type ListOperationalDynamicConfigResponse = ListDynamicConfigResponse;

// ---------------------------------------------------------------------------
// Isolation groups
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetGlobalIsolationGroupsRequest {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetGlobalIsolationGroupsResponse {
    #[serde(
        rename = "isolationGroups",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub isolation_groups: Option<crate::types::IsolationGroupConfiguration>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateGlobalIsolationGroupsRequest {
    #[serde(
        rename = "isolationGroups",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub isolation_groups: Option<crate::types::IsolationGroupConfiguration>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateGlobalIsolationGroupsResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetDomainIsolationGroupsRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetDomainIsolationGroupsResponse {
    #[serde(
        rename = "isolationGroups",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub isolation_groups: Option<crate::types::IsolationGroupConfiguration>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDomainIsolationGroupsRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "isolationGroups",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub isolation_groups: Option<crate::types::IsolationGroupConfiguration>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDomainIsolationGroupsResponse {}

// ---------------------------------------------------------------------------
// Async workflow configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetDomainAsyncWorkflowConfiguratonRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GetDomainAsyncWorkflowConfiguratonResponse {
    #[serde(
        rename = "configuration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub configuration: Option<crate::types::AsyncWorkflowConfiguration>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UpdateDomainAsyncWorkflowConfiguratonRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "configuration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub configuration: Option<crate::types::AsyncWorkflowConfiguration>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDomainAsyncWorkflowConfiguratonResponse {}

// ---------------------------------------------------------------------------
// Shards and queues (single-host semantics)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeHistoryHostRequest {
    #[serde(
        rename = "hostAddress",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub host_address: Option<String>,
    #[serde(
        rename = "shardIdForHost",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub shard_id_for_host: Option<i32>,
    #[serde(
        rename = "executionForHost",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_for_host: Option<crate::types::WorkflowExecution>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeHistoryHostResponse {
    #[serde(rename = "numberOfShards", default)]
    pub number_of_shards: i32,
    #[serde(rename = "shardIds", default, skip_serializing_if = "Vec::is_empty")]
    pub shard_ids: Vec<i32>,
    #[serde(rename = "domainCache", default, skip_serializing_if = "Option::is_none")]
    pub domain_cache: Option<DomainCacheInfo>,
    #[serde(
        rename = "shardControllerStatus",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub shard_controller_status: String,
    #[serde(rename = "address", default, skip_serializing_if = "String::is_empty")]
    pub address: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainCacheInfo {
    #[serde(rename = "numOfItemsInCacheById", default)]
    pub num_of_items_in_cache_by_id: i64,
    #[serde(rename = "numOfItemsInCacheByName", default)]
    pub num_of_items_in_cache_by_name: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeShardDistributionResponse {
    #[serde(rename = "numberOfShards", default)]
    pub number_of_shards: i32,
    #[serde(rename = "shards", default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<std::collections::HashMap<i32, String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseShardRequest {
    #[serde(rename = "shardID", default)]
    pub shard_id: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseShardResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveTaskRequest {
    #[serde(rename = "shardID", default)]
    pub shard_id: i32,
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub task_type: Option<i32>,
    #[serde(rename = "taskID", default)]
    pub task_id: i64,
    #[serde(
        rename = "visibilityTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub visibility_timestamp: Option<i64>,
    #[serde(
        rename = "clusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cluster_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveTaskResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetQueueRequest {
    #[serde(rename = "shardID", default)]
    pub shard_id: i32,
    #[serde(
        rename = "clusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cluster_name: String,
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub task_type: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetQueueResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeQueueRequest {
    #[serde(rename = "shardID", default)]
    pub shard_id: i32,
    #[serde(
        rename = "clusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cluster_name: String,
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub task_type: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeQueueResponse {
    #[serde(
        rename = "processingQueueStates",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub processing_queue_states: Vec<String>,
}

// ---------------------------------------------------------------------------
// Raw history and admin describe
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetWorkflowExecutionRawHistoryV2Request {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "execution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution: Option<crate::types::WorkflowExecution>,
    #[serde(
        rename = "startEventId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_event_id: Option<i64>,
    #[serde(
        rename = "startEventVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_event_version: Option<i64>,
    #[serde(rename = "endEventId", default, skip_serializing_if = "Option::is_none")]
    pub end_event_id: Option<i64>,
    #[serde(
        rename = "endEventVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub end_event_version: Option<i64>,
    #[serde(rename = "maximumPageSize", default)]
    pub maximum_page_size: i32,
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
pub struct GetWorkflowExecutionRawHistoryV2Response {
    #[serde(
        rename = "nextPageToken",
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "crate::b64::serialize_bytes",
        deserialize_with = "crate::b64::deserialize_bytes"
    )]
    pub next_page_token: Vec<u8>,
    #[serde(
        rename = "historyBatches",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub history_batches: Vec<crate::types::DataBlob>,
    #[serde(
        rename = "versionHistory",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub version_history: Option<crate::types::VersionHistory>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDescribeWorkflowExecutionRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "execution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution: Option<crate::types::WorkflowExecution>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDescribeWorkflowExecutionResponse {
    #[serde(rename = "shardId", default)]
    pub shard_id: i32,
    #[serde(
        rename = "historyAddr",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub history_addr: String,
    #[serde(
        rename = "mutableStateInCache",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub mutable_state_in_cache: String,
    #[serde(
        rename = "mutableStateInDatabase",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub mutable_state_in_database: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintainCorruptWorkflowRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "execution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub execution: Option<crate::types::WorkflowExecution>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintainCorruptWorkflowResponse {
    #[serde(rename = "historyDeleted", default)]
    pub history_deleted: bool,
    #[serde(rename = "executionsDeleted", default)]
    pub executions_deleted: bool,
    #[serde(rename = "visibilityDeleted", default)]
    pub visibility_deleted: bool,
}

// ---------------------------------------------------------------------------
// Event reapply, replication, DLQ and cross-cluster (single-host semantics)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReapplyEventsRequest {
    #[serde(rename = "domain", default, skip_serializing_if = "String::is_empty")]
    pub domain: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<crate::types::WorkflowExecution>,
    #[serde(rename = "events", default, skip_serializing_if = "Option::is_none")]
    pub events: Option<crate::types::DataBlob>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReapplyEventsResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationToken {
    #[serde(rename = "shardId", default)]
    pub shard_id: i32,
    #[serde(rename = "lastRetrievedMessageId", default)]
    pub last_retrieved_message_id: i64,
    #[serde(rename = "lastProcessedMessageId", default)]
    pub last_processed_message_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetReplicationMessagesRequest {
    #[serde(rename = "tokens", default, skip_serializing_if = "Vec::is_empty")]
    pub tokens: Vec<ReplicationToken>,
    #[serde(
        rename = "clusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cluster_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationTaskInfo {
    #[serde(rename = "domainId", default, skip_serializing_if = "String::is_empty")]
    pub domain_id: String,
    #[serde(
        rename = "workflowExecution",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_execution: Option<crate::types::WorkflowExecution>,
    #[serde(rename = "taskType", default)]
    pub task_type: i32,
    #[serde(rename = "taskId", default)]
    pub task_id: i64,
    #[serde(rename = "version", default)]
    pub version: i64,
    #[serde(rename = "firstEventId", default)]
    pub first_event_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetDLQReplicationMessagesRequest {
    #[serde(rename = "taskInfos", default, skip_serializing_if = "Vec::is_empty")]
    pub task_infos: Vec<ReplicationTaskInfo>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetDomainReplicationMessagesRequest {
    #[serde(
        rename = "lastRetrievedMessageId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_retrieved_message_id: Option<i64>,
    #[serde(
        rename = "lastProcessedMessageId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_processed_message_id: Option<i64>,
    #[serde(
        rename = "clusterName",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub cluster_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountDLQMessagesRequest {
    #[serde(rename = "forceFetch", default)]
    pub force_fetch: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryDLQCountEntry {
    #[serde(rename = "sourceCluster", default, skip_serializing_if = "String::is_empty")]
    pub source_cluster: String,
    #[serde(rename = "messageCount", default)]
    pub message_count: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountDLQMessagesResponse {
    #[serde(rename = "history", default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<HistoryDLQCountEntry>,
    #[serde(rename = "domain", default)]
    pub domain: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadDLQMessagesRequest {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub dlq_type: Option<crate::enums::DLQType>,
    #[serde(rename = "shardID", default)]
    pub shard_id: i32,
    #[serde(
        rename = "sourceCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub source_cluster: String,
    #[serde(
        rename = "inclusiveEndMessageId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub inclusive_end_message_id: Option<i64>,
    #[serde(rename = "maximumPageSize", default)]
    pub maximum_page_size: i32,
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
pub struct PurgeDLQMessagesRequest {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub dlq_type: Option<crate::enums::DLQType>,
    #[serde(rename = "shardID", default)]
    pub shard_id: i32,
    #[serde(
        rename = "sourceCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub source_cluster: String,
    #[serde(
        rename = "inclusiveEndMessageId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub inclusive_end_message_id: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeDLQMessagesResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeDLQMessagesRequest {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub dlq_type: Option<crate::enums::DLQType>,
    #[serde(rename = "shardID", default)]
    pub shard_id: i32,
    #[serde(
        rename = "sourceCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub source_cluster: String,
    #[serde(
        rename = "inclusiveEndMessageId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub inclusive_end_message_id: Option<i64>,
    #[serde(rename = "maximumPageSize", default)]
    pub maximum_page_size: i32,
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
pub struct MergeDLQMessagesResponse {
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
pub struct ResendReplicationTasksRequest {
    #[serde(rename = "domainID", default, skip_serializing_if = "String::is_empty")]
    pub domain_id: String,
    #[serde(
        rename = "workflowID",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub workflow_id: String,
    #[serde(rename = "runID", default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(
        rename = "remoteCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub remote_cluster: String,
    #[serde(
        rename = "startEventID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_event_id: Option<i64>,
    #[serde(
        rename = "startVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub start_version: Option<i64>,
    #[serde(rename = "endEventID", default, skip_serializing_if = "Option::is_none")]
    pub end_event_id: Option<i64>,
    #[serde(
        rename = "endVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub end_version: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResendReplicationTasksResponse {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetCrossClusterTasksRequest {
    #[serde(rename = "shardIDs", default, skip_serializing_if = "Vec::is_empty")]
    pub shard_ids: Vec<i32>,
    #[serde(
        rename = "targetCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub target_cluster: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespondCrossClusterTasksCompletedRequest {
    #[serde(rename = "shardId", default)]
    pub shard_id: i32,
    #[serde(
        rename = "targetCluster",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub target_cluster: String,
    #[serde(rename = "fetchNewTasks", default)]
    pub fetch_new_tasks: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespondCrossClusterTasksCompletedResponse {}
