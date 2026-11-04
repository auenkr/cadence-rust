//! Typed persistence contract mirroring Go `data_store_interfaces.go` and
//! `data_manager_interfaces.go`. Payloads are kept losslessly as blobs; SQL
//! implementations must serialize the typed task and mutable-state records.
use async_trait::async_trait;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DataBlob { pub encoding: String, pub data: Vec<u8> }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CreateWorkflowMode { BrandNew=0, WorkflowIdReuse=1, ContinueAsNew=2, Zombie=3 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum UpdateWorkflowMode { UpdateCurrent=0, BypassCurrent=1, IgnoreCurrent=2 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ConflictResolveWorkflowMode { UpdateCurrent=0, BypassCurrent=1 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum WorkflowRequestMode { New=0, Replicated=1 }

#[derive(Debug, Clone, PartialEq, Default)]
pub struct WorkflowExecutionInfo {
    pub domain_id: String, pub workflow_id: String, pub run_id: String,
    pub first_execution_run_id:String, pub parent_domain_id:String, pub parent_workflow_id:String, pub parent_run_id:String,
    pub initiated_id:i64, pub completion_event_batch_id:i64,
    pub create_request_id: String, pub state: i32, pub close_status: i32,
    pub next_event_id: i64, pub last_first_event_id: i64, pub last_event_task_id: i64,
    pub last_processed_event:i64, pub last_write_version: i64, pub start_time_unix_nanos: i64, pub last_updated_time_unix_nanos:i64,
    pub workflow_timeout_nanos:i64, pub decision_start_to_close_timeout_nanos:i64,
    pub execution_context:Vec<u8>, pub signal_count:i32, pub decision_version:i64, pub decision_schedule_id:i64,
    pub decision_started_id:i64, pub decision_request_id:String, pub decision_timeout_nanos:i64, pub decision_attempt:i64,
    pub decision_started_time_unix_nanos:i64, pub decision_scheduled_time_unix_nanos:i64,
    pub decision_original_scheduled_time_unix_nanos:i64, pub cancel_requested:bool, pub cancel_request_id:String,
    pub task_list: String, pub task_list_kind:i32, pub workflow_type_name:String, pub sticky_task_list: String,
    pub sticky_schedule_to_start_timeout_nanos:i64, pub client_library_version: String,
    pub client_feature_version: String, pub client_impl: String, pub cron_schedule: String,
    pub cron_overlap_policy:i32, pub expiration_interval_nanos:i64,
    pub completion_event: Option<DataBlob>, pub auto_reset_points: Option<DataBlob>, pub branch_token:Vec<u8>,
    pub attempt:i32, pub has_retry_policy:bool, pub initial_interval_nanos:i64, pub backoff_coefficient:f64,
    pub jitter_coefficient:f64, pub maximum_interval_nanos:i64, pub expiration_time_unix_nanos:i64,
    pub maximum_attempts:i32, pub non_retriable_errors:Vec<String>, pub memo:BTreeMap<String,Vec<u8>>,
    pub search_attributes: BTreeMap<String, Vec<u8>>, pub partition_config: BTreeMap<String,String>,
    pub active_cluster_selection_policy:Option<DataBlob>,
    pub history_size: i64, pub is_cron: bool,
}

/// Typed mutable-state activity row decoded from a SQL sqlblobs value.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ActivityInfo {
    pub version:i64, pub scheduled_event_batch_id:i64, pub scheduled_event:Vec<u8>, pub scheduled_event_encoding:String,
    pub scheduled_time_unix_nanos:i64, pub started_id:i64, pub started_event:Vec<u8>, pub started_event_encoding:String,
    pub started_time_unix_nanos:i64, pub activity_id:String, pub request_id:String,
    pub schedule_to_start_timeout_seconds:i32, pub schedule_to_close_timeout_seconds:i32,
    pub start_to_close_timeout_seconds:i32, pub heartbeat_timeout_seconds:i32,
    pub cancel_requested:bool, pub cancel_request_id:i64, pub timer_task_status:i32, pub attempt:i32,
    pub task_list:String, pub task_list_kind:i32, pub started_identity:String, pub has_retry_policy:bool,
    pub retry_initial_interval_seconds:i32, pub retry_maximum_interval_seconds:i32, pub retry_maximum_attempts:i32,
    pub retry_expiration_time_unix_nanos:i64, pub retry_backoff_coefficient:f64, pub retry_jitter_coefficient:f64,
    pub retry_non_retriable_errors:Vec<String>, pub retry_last_failure_reason:String, pub retry_last_worker_identity:String,
    pub retry_last_failure_details:Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChildExecutionInfo { pub version:i64, pub initiated_event_batch_id:i64, pub started_id:i64, pub initiated_event:Vec<u8>, pub initiated_event_encoding:String, pub started_workflow_id:String, pub started_run_id:Vec<u8>, pub started_event:Vec<u8>, pub started_event_encoding:String, pub create_request_id:String, pub domain_id:String, pub domain_name:String, pub workflow_type_name:String, pub parent_close_policy:i32 }
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SignalInfo { pub version:i64, pub initiated_event_batch_id:i64, pub request_id:String, pub name:String, pub input:Vec<u8>, pub control:Vec<u8> }
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RequestCancelInfo { pub version:i64, pub initiated_event_batch_id:i64, pub cancel_request_id:String }
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TimerInfo { pub version:i64, pub started_id:i64, pub expiry_time_unix_nanos:i64, pub task_id:i64 }
/// SQL sqlblobs WorkflowExecutionInfo; distinct from the persistence-facing type above.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SqlWorkflowExecutionInfo { pub parent_domain_id:Vec<u8>, pub parent_workflow_id:String, pub parent_run_id:Vec<u8>, pub initiated_id:i64, pub completion_event_batch_id:i64, pub completion_event:Vec<u8>, pub completion_event_encoding:String, pub task_list:String, pub task_list_kind:i32, pub workflow_type_name:String, pub workflow_timeout_seconds:i32, pub decision_task_timeout_seconds:i32, pub execution_context:Vec<u8>, pub state:i32, pub close_status:i32, pub start_version:i64, pub last_write_event_id:i64, pub last_event_task_id:i64, pub last_first_event_id:i64, pub last_processed_event:i64, pub start_time_unix_nanos:i64, pub last_updated_time_unix_nanos:i64, pub decision_version:i64, pub decision_schedule_id:i64, pub decision_started_id:i64, pub decision_timeout_seconds:i32, pub decision_attempt:i64, pub decision_started_timestamp_unix_nanos:i64, pub decision_scheduled_timestamp_unix_nanos:i64, pub cancel_requested:bool, pub decision_original_scheduled_timestamp_unix_nanos:i64, pub create_request_id:String, pub decision_request_id:String, pub cancel_request_id:String, pub sticky_task_list:String, pub sticky_schedule_to_start_timeout_seconds:i64, pub retry_attempt:i64, pub retry_initial_interval_seconds:i32, pub retry_maximum_interval_seconds:i32, pub retry_maximum_attempts:i32, pub retry_expiration_seconds:i32, pub retry_backoff_coefficient:f64, pub retry_jitter_coefficient:f64, pub retry_expiration_time_unix_nanos:i64, pub retry_non_retryable_errors:Vec<String>, pub has_retry_policy:bool, pub cron_schedule:String, pub event_store_version:i32, pub event_branch_token:Vec<u8>, pub signal_count:i64, pub history_size:i64, pub client_library_version:String, pub client_feature_version:String, pub client_impl:String, pub auto_reset_points:Vec<u8>, pub auto_reset_points_encoding:String, pub search_attributes:BTreeMap<String,Vec<u8>>, pub memo:BTreeMap<String,Vec<u8>>, pub version_histories:Vec<u8>, pub version_histories_encoding:String, pub first_execution_run_id:Vec<u8>, pub partition_config:BTreeMap<String,String>, pub checksum:Vec<u8>, pub checksum_encoding:String, pub cron_overlap_policy:i32, pub active_cluster_selection_policy:Vec<u8>, pub active_cluster_selection_policy_encoding:String }
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WorkflowSnapshot {
    pub activity_heartbeats: BTreeMap<i64, ActivityHeartbeat>, pub buffered_events: Vec<DataBlob>,
    pub execution_info: WorkflowExecutionInfo, pub version_histories: Option<DataBlob>,
    pub start_version: i64, pub last_write_version: i64,
    pub activity_infos: Vec<KeyedBlob<i64>>, pub timer_infos: Vec<KeyedBlob<String>>,
    pub workflow_timer_tasks: Vec<HistoryTaskKey>, pub child_execution_infos: Vec<KeyedBlob<i64>>,
    pub request_cancel_infos: Vec<KeyedBlob<i64>>, pub signal_infos: Vec<KeyedBlob<i64>>,
    pub semaphore_infos: Vec<DataBlob>, pub signal_requested_ids: Vec<String>,
    pub tasks_by_category: BTreeMap<HistoryTaskCategory, Vec<Task>>, pub workflow_requests: Vec<WorkflowRequest>,
    pub condition: i64, pub checksum: Option<DataBlob>,
}
/// A Go workflow-state map value keeps its identity in SQL key columns and
/// serializes only the value into `data`/`data_encoding`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedBlob<K> { pub key:K, pub value:DataBlob }
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WorkflowMutation {
    pub activity_heartbeats: BTreeMap<i64, ActivityHeartbeat>,
    pub execution_info: WorkflowExecutionInfo, pub version_histories: Option<DataBlob>,
    pub start_version: i64, pub last_write_version: i64,
    pub upsert_activity_infos: Vec<KeyedBlob<i64>>, pub delete_activity_infos: Vec<i64>, pub rewrite_activity_infos: Vec<KeyedBlob<i64>>,
    pub upsert_timer_infos: Vec<KeyedBlob<String>>, pub delete_timer_infos: Vec<String>, pub rewrite_timer_infos: Vec<KeyedBlob<String>>,
    pub sentinel_write_enabled: bool, pub workflow_timer_tasks: Vec<HistoryTaskKey>,
    pub upsert_child_execution_infos: Vec<KeyedBlob<i64>>, pub delete_child_execution_infos: Vec<i64>,
    pub upsert_request_cancel_infos: Vec<KeyedBlob<i64>>, pub delete_request_cancel_infos: Vec<i64>,
    pub upsert_signal_infos: Vec<KeyedBlob<i64>>, pub delete_signal_infos: Vec<i64>,
    pub upsert_semaphore_infos: Vec<DataBlob>, pub delete_semaphore_infos: Vec<i64>,
    pub upsert_signal_requested_ids: Vec<String>, pub delete_signal_requested_ids: Vec<String>,
    pub new_buffered_events: Option<DataBlob>, pub clear_buffered_events: bool,
    pub tasks_by_category: BTreeMap<HistoryTaskCategory, Vec<Task>>, pub workflow_requests: Vec<WorkflowRequest>,
    pub condition: i64, pub checksum: Option<DataBlob>,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct HistoryTaskCategory { pub category_type: i32, pub category_id: i32 }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Task { pub visibility_timestamp_unix_nanos: i64, pub task_type: i32, pub task_id: i64, pub data: DataBlob }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkflowRequest { pub request_id: String, pub data: DataBlob }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryTaskKey { pub task_id: i64, pub version: i64 }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransferTask {
    pub domain_id:String, pub workflow_id:String, pub run_id:String, pub visibility_time_unix_nanos:i64,
    pub task_id:i64, pub target_domain_id:String, pub target_domain_ids:Vec<String>, pub target_workflow_id:String,
    pub target_run_id:String, pub target_child_workflow_only:bool, pub task_list:String, pub task_type:i32,
    pub schedule_id:i64, pub version:i64, pub record_visibility:bool, pub original_task_list:String,
    pub original_task_list_kind:i32,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TimerTask {
    pub domain_id:String, pub workflow_id:String, pub run_id:String, pub visibility_timestamp_unix_nanos:i64,
    pub task_id:i64, pub task_type:i32, pub timeout_type:i32, pub event_id:i64, pub schedule_attempt:i64,
    pub version:i64, pub task_list:String,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationTask {
    pub domain_id:String, pub workflow_id:String, pub run_id:String, pub task_id:i64, pub task_type:i32,
    pub first_event_id:i64, pub next_event_id:i64, pub version:i64, pub scheduled_id:i64,
    pub event_store_version:i32, pub new_run_event_store_version:i32,
    pub branch_token:Vec<u8>, pub new_run_branch_token:Vec<u8>, pub creation_time_unix_nanos:i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CrossClusterTask { pub transfer_task:TransferTask, pub target_cluster:String }

#[derive(Debug, Clone, PartialEq)]
pub struct CreateWorkflowExecutionRequest { pub shard_id:i32, pub range_id:i64, pub mode:CreateWorkflowMode, pub previous_run_id:String, pub previous_last_write_version:i64, pub new_workflow_snapshot:WorkflowSnapshot, pub workflow_request_mode:WorkflowRequestMode, pub current_timestamp_unix_nanos:i64 }
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateWorkflowExecutionRequest { pub shard_id:i32, pub range_id:i64, pub mode:UpdateWorkflowMode, pub update_workflow_mutation:WorkflowMutation, pub new_workflow_snapshot:Option<WorkflowSnapshot>, pub workflow_request_mode:WorkflowRequestMode, pub current_timestamp_unix_nanos:i64 }
#[derive(Debug, Clone, PartialEq)]
pub struct ConflictResolveWorkflowExecutionRequest { pub shard_id:i32, pub range_id:i64, pub mode:ConflictResolveWorkflowMode, pub reset_workflow_snapshot:WorkflowSnapshot, pub new_workflow_snapshot:Option<WorkflowSnapshot>, pub current_workflow_mutation:Option<WorkflowMutation>, pub workflow_request_mode:WorkflowRequestMode, pub current_timestamp_unix_nanos:i64 }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CurrentExecution { pub run_id:String, pub create_request_id:String, pub state:i32, pub close_status:i32, pub start_version:i64, pub last_write_version:i64 }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExecutionListFilter { pub shard_id:i32, pub domain_id:String, pub workflow_id:Option<String>, pub page_size:i32, pub next_page_token:Vec<u8> }
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExecutionPage { pub executions:Vec<WorkflowSnapshot>, pub next_page_token:Vec<u8> }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskRange { pub shard_id:i32, pub min_task_id:i64, pub max_task_id:i64, pub batch_size:i32 }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TimerTaskRange { pub shard_id:i32, pub min_visibility_time_unix_nanos:i64, pub min_task_id:i64, pub max_visibility_time_unix_nanos:i64, pub batch_size:i32 }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistenceError { ShardOwnershipLost{shard_id:i32,expected_range_id:i64,actual_range_id:i64}, ConditionFailed(String), WorkflowAlreadyStarted{message:String,start_request_id:String,run_id:String,state:i32,close_status:i32,last_write_version:i64}, CurrentWorkflowConditionFailed(String), EntityNotExists(String), AlreadyExists(String), InvalidPersistenceRequest(String), InternalDataInconsistency(String), InvalidArgument(String), Internal(String) }
impl std::fmt::Display for PersistenceError { fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result { write!(f,"{self:?}") } }
impl std::error::Error for PersistenceError {}

/// ExecutionStore surface includes every Go SQL execution operation, including task APIs.
#[async_trait]
pub trait GoExecutionStore: Send+Sync {
    async fn create_workflow_execution(&self, request:CreateWorkflowExecutionRequest)->Result<CurrentExecution,PersistenceError>;
    async fn get_workflow_execution(&self, shard_id:i32, domain_id:String, workflow_id:String, run_id:String, range_id:i64)->Result<WorkflowSnapshot,PersistenceError>;
    async fn update_workflow_execution(&self, request:UpdateWorkflowExecutionRequest)->Result<(),PersistenceError>;
    async fn conflict_resolve_workflow_execution(&self, request:ConflictResolveWorkflowExecutionRequest)->Result<(),PersistenceError>;
    async fn delete_workflow_execution(&self, shard_id:i32, domain_id:String, workflow_id:String, run_id:String)->Result<(),PersistenceError>;
    async fn delete_current_workflow_execution(&self, shard_id:i32, domain_id:String, workflow_id:String, run_id:String)->Result<(),PersistenceError>;
    async fn get_current_execution(&self, shard_id:i32, domain_id:String, workflow_id:String)->Result<CurrentExecution,PersistenceError>;
    async fn is_workflow_execution_exists(&self, shard_id:i32, domain_id:String, workflow_id:String, run_id:String)->Result<bool,PersistenceError>;
    async fn list_concrete_executions(&self, filter:ExecutionListFilter)->Result<ExecutionPage,PersistenceError>;
    async fn list_current_executions(&self, filter:ExecutionListFilter)->Result<ExecutionPage,PersistenceError>;
    async fn create_transfer_tasks(&self, shard_id:i32, tasks:Vec<TransferTask>)->Result<(),PersistenceError>;
    async fn get_transfer_tasks(&self, shard_id:i32, min_id:i64, max_id:i64, batch_size:i32)->Result<Vec<TransferTask>,PersistenceError>;
    async fn complete_transfer_task(&self, shard_id:i32, task_id:i64)->Result<(),PersistenceError>;
    async fn create_failover_marker_tasks(&self, shard_id:i32, tasks:Vec<Task>)->Result<(),PersistenceError>;
    async fn create_history_tasks(&self, shard_id:i32, tasks:Vec<Task>)->Result<(),PersistenceError>;
    async fn create_timer_tasks(&self, shard_id:i32, tasks:Vec<TimerTask>)->Result<(),PersistenceError>;
    async fn select_timer_tasks(&self, shard_id:i32, min_timestamp:i64, max_timestamp:i64, batch_size:i32)->Result<Vec<TimerTask>,PersistenceError>;
    async fn complete_timer_task(&self, shard_id:i32, visibility_timestamp:i64, task_id:i64)->Result<(),PersistenceError>;
    async fn select_workflow_timer_tasks(&self, range:TimerTaskRange)->Result<Vec<HistoryTaskKey>,PersistenceError>;
    async fn create_replication_tasks(&self, shard_id:i32, tasks:Vec<ReplicationTask>)->Result<(),PersistenceError>;
    async fn get_replication_tasks(&self, shard_id:i32, min_id:i64, max_id:i64, batch_size:i32)->Result<Vec<ReplicationTask>,PersistenceError>;
    async fn complete_replication_task(&self, shard_id:i32, task_id:i64)->Result<(),PersistenceError>;
    async fn get_replication_dlq_size(&self, source_cluster:String)->Result<i64,PersistenceError>;
    async fn delete_replication_task_from_dlq(&self, source_cluster:String, task_id:i64)->Result<(),PersistenceError>;
    async fn put_replication_task_to_dlq(&self, source_cluster:String, task:ReplicationTask)->Result<(),PersistenceError>;
    async fn get_replication_tasks_from_dlq(&self, source_cluster:String, min_id:i64, max_id:i64, batch_size:i32)->Result<Vec<ReplicationTask>,PersistenceError>;
    async fn range_delete_replication_tasks_from_dlq(&self, source_cluster:String, inclusive_max_id:i64, page_size:i32)->Result<u64,PersistenceError>;
    async fn create_cross_cluster_tasks(&self, shard_id:i32, tasks:Vec<CrossClusterTask>)->Result<(),PersistenceError>;
    async fn get_cross_cluster_tasks(&self, shard_id:i32, min_id:i64, max_id:i64, batch_size:i32)->Result<Vec<CrossClusterTask>,PersistenceError>;
    async fn complete_cross_cluster_task(&self, shard_id:i32, task_id:i64)->Result<(),PersistenceError>;
    async fn create_replication_tasks_from_dlq(&self, source_cluster:String, tasks:Vec<ReplicationTask>)->Result<(),PersistenceError>;
    async fn get_history_tasks(&self, range:TaskRange, category:HistoryTaskCategory)->Result<Vec<Task>,PersistenceError>;
    async fn complete_history_task(&self, shard_id:i32, category:HistoryTaskCategory, task_id:i64)->Result<(),PersistenceError>;
    async fn range_complete_history_tasks(&self, range:TaskRange, category:HistoryTaskCategory)->Result<u64,PersistenceError>;
    async fn get_replication_dlq_page(&self, request:crate::history_task_sql::ReplicationDlqQuery)->Result<crate::history_task_sql::ReplicationDlqPage,PersistenceError>;
    async fn range_delete_replication_dlq_page(&self, shard_id:i32, source_cluster:String, inclusive_min_id:i64, exclusive_max_id:i64, page_size:i32)->Result<u64,PersistenceError>;
    async fn get_history_task_page(&self, request:crate::history_task_sql::HistoryTaskQuery)->Result<crate::history_task_sql::HistoryTaskPage,PersistenceError>;
    async fn complete_history_task_batch(&self, shard_id:i32, category:HistoryTaskCategory, keys:Vec<(i64,i64)>)->Result<(),PersistenceError>;
    async fn range_complete_history_task_page(&self, request:crate::history_task_sql::HistoryTaskQuery)->Result<u64,PersistenceError>;
    async fn create_failover_markers_with_range(&self, shard_id:i32, range_id:i64, markers:Vec<Task>)->Result<(),PersistenceError>;
    async fn get_active_cluster_selection_policy(&self, shard_id:i32, domain_id:String, workflow_id:String, run_id:String)->Result<Option<DataBlob>,PersistenceError>;
    async fn delete_active_cluster_selection_policy(&self, shard_id:i32, domain_id:String, workflow_id:String, run_id:String)->Result<(),PersistenceError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryBranchRange { pub branch_id:String, pub begin_node_id:i64, pub end_node_id:i64 }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryTreeInfo { pub created_time_unix_nanos:i64, pub ancestors:Vec<HistoryBranchRange>, pub info:String }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryBranchRequest {
    pub shard_id:i32, pub tree_id:String, pub branch_id:String, pub node_id:i64, pub transaction_id:i64,
    pub data:DataBlob, pub page_size:i32, pub next_page_token:Vec<u8>,
    pub is_new_branch:bool, pub info:String, pub ancestors:Vec<HistoryBranchRange>,
    pub min_node_id:i64, pub max_node_id:i64, pub last_node_id:i64, pub last_transaction_id:i64,
    pub fork_node_id:i64, pub source_branch_id:String, pub current_timestamp_unix_nanos:i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryBranchPage { pub nodes:Vec<DataBlob>, pub next_page_token:Vec<u8>, pub last_node_id:i64, pub last_transaction_id:i64 }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryTreeBranch { pub shard_id:i32, pub tree_id:String, pub branch_id:String, pub fork_time_unix_nanos:i64, pub info:String, pub ancestors:Vec<HistoryBranchRange> }
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryTreePage { pub branches:Vec<HistoryTreeBranch>, pub next_page_token:Vec<u8> }
#[async_trait]
pub trait GoHistoryV2Store: Send+Sync {
    async fn append_history_nodes(&self, request:HistoryBranchRequest)->Result<(),PersistenceError>;
    async fn read_history_branch(&self, request:HistoryBranchRequest)->Result<HistoryBranchPage,PersistenceError>;
    async fn fork_history_branch(&self, request:HistoryBranchRequest, fork_node_id:i64, new_branch_id:String)->Result<HistoryBranchRequest,PersistenceError>;
    async fn delete_history_branch(&self, request:HistoryBranchRequest)->Result<(),PersistenceError>;
    async fn get_history_tree(&self, request:HistoryBranchRequest)->Result<Vec<HistoryTreeBranch>,PersistenceError>;
    async fn get_all_history_tree_branches(&self, shard_id:i32, page_size:i32, next_page_token:Vec<u8>)->Result<HistoryTreePage,PersistenceError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActivityHeartbeat { pub details: Vec<u8>, pub updated_time_unix_nanos: i64 }
