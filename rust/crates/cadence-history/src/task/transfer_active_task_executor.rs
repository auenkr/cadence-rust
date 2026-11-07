//! Port of go/service/history/task/transfer_active_task_executor.go.
use std::{collections::{BTreeMap,HashSet},future::Future,sync::{Arc,Mutex},time::Duration};
use async_trait::async_trait;
use super::{deps::*,interface::*,task_util::verify_task_version,history_task::get_or_create_domain_tagged_scope,transfer_task_executor_base::*,task_util::get_transfer_task_metrics_scope};

/// `resetWorkflowTimeout`
pub const RESET_WORKFLOW_TIMEOUT:Duration=Duration::from_secs(30);
/// `execution.IdentityHistoryService`
pub const IDENTITY_HISTORY_SERVICE:&str="history-service";

/// `ErrMissingRequestCancelInfo`
pub fn err_missing_request_cancel_info()->TaskError{TaskError::InternalService("unable to get request cancel info".into())}
/// `ErrMissingSignalInfo`
pub fn err_missing_signal_info()->TaskError{TaskError::InternalService("unable to get signal info".into())}
/// `errUnknownTransferTask`
pub fn err_unknown_transfer_task()->TaskError{TaskError::Other("unknown transfer task".into())}
/// `errWorkflowBusy`
pub fn err_workflow_busy()->TaskError{TaskError::WorkflowBusy}
/// `errWorkflowRateLimited`
pub fn err_workflow_rate_limited()->TaskError{TaskError::WorkflowRateLimited}

// ---------------------------------------------------------------------------
// value types handed over by the (not yet linked) execution / history client packages
// ---------------------------------------------------------------------------
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ParentClosePolicy {Abandon,RequestCancel,Terminate,Unknown(i32)}
#[derive(Clone,Debug,Default,PartialEq,Eq)]
pub struct WorkflowExecutionRef {pub workflow_id:String,pub run_id:String}
#[derive(Clone,Debug)]
pub struct ActiveActivityInfo {pub version:i64,pub schedule_to_start_timeout:i32,pub task_list:String,pub task_list_kind:TaskListKind}
#[derive(Clone,Debug)]
pub struct ActiveDecisionInfo {pub version:i64}
#[derive(Clone,Debug)]
pub struct RequestCancelInfo {pub version:i64,pub cancel_request_id:String}
#[derive(Clone,Debug)]
pub struct SignalInfo {pub version:i64,pub signal_name:String,pub signal_request_id:String,pub input:Vec<u8>,pub control:Vec<u8>}
#[derive(Clone,Debug)]
pub struct ChildExecutionInfo {pub version:i64,pub started_id:i64,pub started_workflow_id:String,pub started_run_id:String,pub create_request_id:String,pub parent_close_policy:ParentClosePolicy,pub domain_id:String,pub domain_name:String}
#[derive(Clone,Debug,Default)]
pub struct ActiveExecutionInfo {
 pub domain_id:String,pub workflow_id:String,pub run_id:String,pub task_list:String,pub task_list_kind:TaskListKind,
 pub workflow_timeout:i32,pub sticky_schedule_to_start_timeout:i32,pub partition_config:BytesMap,
 pub parent_domain_id:String,pub parent_workflow_id:String,pub parent_run_id:String,pub initiated_id:i64,
 pub workflow_type_name:String,pub close_status:i32,pub is_continued_as_new:bool,pub cron_schedule:String,
 pub memo:Option<BytesMap>,pub search_attributes:BytesMap,pub active_cluster_selection_policy:Option<ActiveClusterSelectionPolicy>,
}
/// Close event kinds the executor distinguishes (`types.EventTypeWorkflowExecution*`).
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum CloseEventType {Completed,Failed,Canceled,Terminated,TimedOut,ContinuedAsNew,Other}
#[derive(Clone,Debug)]
pub struct CloseEvent {pub event_id:i64,pub event_type:CloseEventType,pub timestamp:i64,pub continued_as_new_attributes_present:bool,pub continued_as_new_failure_reason:String,
 /// Full completion event (needed by RecordChildExecutionCompleted).
 pub full_event:Option<Arc<cadence_api::service_types::HistoryEvent>>}
#[derive(Clone,Debug,Default)]
pub struct StartChildRetryPolicy {pub initial_interval_seconds:i32,pub backoff_coefficient:f64,pub maximum_interval_seconds:i32,pub maximum_attempts:i32,pub non_retriable_error_reasons:Vec<String>,pub expiration_interval_seconds:i32}
/// `types.StartChildWorkflowExecutionInitiatedEventAttributes`
#[derive(Clone,Debug,Default)]
pub struct StartChildAttributes {
 pub domain:String,pub workflow_id:String,pub workflow_type:String,pub task_list:String,pub input:Vec<u8>,pub header:Option<BytesMap>,
 pub execution_start_to_close_timeout_seconds:Option<i32>,pub task_start_to_close_timeout_seconds:Option<i32>,
 pub workflow_id_reuse_policy:Option<i32>,pub retry_policy:Option<StartChildRetryPolicy>,pub cron_schedule:String,
 pub cron_overlap_policy:Option<i32>,pub memo:Option<BytesMap>,pub search_attributes:Option<BytesMap>,
 pub delay_start_seconds:Option<i32>,pub jitter_start_seconds:Option<i32>,pub first_run_at_timestamp:Option<i64>,
}
/// `types.StartWorkflowExecutionRequest`
#[derive(Clone,Debug,Default)]
pub struct FrontendStartRequest {pub domain:String,pub attributes:StartChildAttributes,pub request_id:String,pub active_cluster_selection_policy:Option<ActiveClusterSelectionPolicy>}
#[derive(Clone,Debug)]
pub struct ParentExecutionInfo {pub domain_uuid:String,pub domain:String,pub execution:WorkflowExecutionRef,pub initiated_id:i64}
/// `types.HistoryStartWorkflowExecutionRequest`
#[derive(Clone,Debug)]
pub struct HistoryStartWorkflowRequest {pub domain_uuid:String,pub request:FrontendStartRequest,pub now:Time,pub partition_config:BytesMap,pub parent_execution_info:Option<ParentExecutionInfo>}
#[derive(Clone,Debug)]
pub struct RecordChildExecutionCompletedRequest {pub domain_uuid:String,pub workflow_execution:WorkflowExecutionRef,pub initiated_id:i64,pub completed_execution:WorkflowExecutionRef,pub completion_event:CloseEvent}
#[derive(Clone,Debug)]
pub struct HistoryRequestCancelRequest {
 pub domain_uuid:String,pub domain:String,pub workflow_execution:WorkflowExecutionRef,pub identity:String,pub request_id:String,
 pub first_execution_run_id:String,pub external_initiated_event_id:Option<i64>,pub external_workflow_execution:Option<WorkflowExecutionRef>,pub child_workflow_only:bool,
}
#[derive(Clone,Debug)]
pub struct HistorySignalRequest {
 pub domain_uuid:String,pub domain:String,pub workflow_execution:WorkflowExecutionRef,pub identity:String,pub signal_name:String,pub input:Vec<u8>,
 pub request_id:String,pub control:Vec<u8>,pub external_workflow_execution:Option<WorkflowExecutionRef>,pub child_workflow_only:bool,
}
#[derive(Clone,Debug)]
pub struct RemoveSignalMutableStateRequest {pub domain_uuid:String,pub workflow_execution:WorkflowExecutionRef,pub request_id:String}
#[derive(Clone,Debug)]
pub struct HistoryTerminateRequest {
 pub domain_uuid:String,pub domain:String,pub workflow_execution:WorkflowExecutionRef,pub reason:String,pub identity:String,
 pub first_execution_run_id:String,pub external_workflow_execution:Option<WorkflowExecutionRef>,pub child_workflow_only:bool,
}
#[derive(Clone,Debug)]
pub struct ScheduleDecisionTaskRequest {pub domain_uuid:String,pub workflow_execution:WorkflowExecutionRef,pub is_first_decision:bool}
/// `parentclosepolicy.RequestDetail`
#[derive(Clone,Debug)]
pub struct ParentClosePolicyRequestDetail {pub domain_id:String,pub domain_name:String,pub workflow_id:String,pub run_id:String,pub policy:ParentClosePolicy}
/// `parentclosepolicy.Request`
#[derive(Clone,Debug)]
pub struct ParentClosePolicyRequest {pub domain_name:String,pub executions:Vec<ParentClosePolicyRequestDetail>}
/// `types.ResetPointInfo`
#[derive(Clone,Debug)]
pub struct ResetPointInfo {pub run_id:String,pub binary_checksum:String,pub first_decision_completed_id:i64}
/// `execution.VersionHistory` subset used by resetWorkflow.
pub trait ResetVersionHistory:Send+Sync {
 fn get_event_version(&self,event_id:i64)->Result<i64>;
 fn get_branch_token(&self)->Vec<u8>;
}
/// Arguments of `reset.WorkflowResetter.ResetWorkflow`.
#[derive(Clone)]
pub struct WorkflowResetRequest {
 pub domain_id:String,pub workflow_id:String,pub base_run_id:String,pub base_branch_token:Vec<u8>,pub base_rebuild_last_event_id:i64,
 pub base_rebuild_last_event_version:i64,pub base_next_event_id:i64,pub reset_run_id:String,pub reset_request_id:String,
 pub current_context:Arc<dyn ActiveTransferContext>,pub current_mutable_state:Arc<dyn ActiveMutableState>,pub current_logger:Logger,
 pub reset_reason:String,pub skip_signal_reapply:bool,
}

/// TODO(link): replace by execution::MutableState (only the accessors used by transferActiveTaskExecutor).
#[async_trait]
pub trait ActiveMutableState:DecisionState {
 fn is_workflow_execution_running(&self)->bool;
 fn domain_name(&self)->String;
 fn execution_info(&self)->ActiveExecutionInfo;
 fn next_event_id(&self)->i64;
 fn activity_info(&self,schedule_id:i64)->Option<ActiveActivityInfo>;
 fn decision_info(&self,schedule_id:i64)->Option<ActiveDecisionInfo>;
 fn request_cancel_info(&self,initiated_id:i64)->Option<RequestCancelInfo>;
 fn signal_info(&self,initiated_id:i64)->Option<SignalInfo>;
 fn child_execution_info(&self,initiated_id:i64)->Option<ChildExecutionInfo>;
 fn pending_child_execution_infos(&self)->BTreeMap<i64,ChildExecutionInfo>;
 fn has_parent_execution(&self)->bool;
 fn last_write_version(&self)->Result<i64>;
 fn start_version(&self)->Result<i64>;
 fn current_version_history(&self)->Result<Option<Arc<dyn ResetVersionHistory>>>;
 /// `execution.FindAutoResetPoint(timeSource, &domainEntry.GetConfig().BadBinaries, executionInfo.AutoResetPoints)`
 fn find_auto_reset_point(&self,time_source:&dyn TimeSource,domain:&DomainCacheEntry)->Option<(String,ResetPointInfo)>;
 fn add_child_workflow_execution_started_event(&self,domain:&str,execution:&WorkflowExecutionRef,attributes:&StartChildAttributes,initiated_id:i64)->Result<()>;
 fn add_start_child_workflow_execution_failed_event(&self,initiated_id:i64,attributes:&StartChildAttributes)->Result<()>;
 fn add_external_workflow_execution_cancel_requested(&self,initiated_id:i64,domain:&str,workflow_id:&str,run_id:&str)->Result<()>;
 fn add_external_workflow_execution_signaled(&self,initiated_id:i64,domain:&str,workflow_id:&str,run_id:&str,control:&[u8])->Result<()>;
 fn add_request_cancel_external_workflow_execution_failed_event(&self,initiated_id:i64,domain:&str,workflow_id:&str,run_id:&str)->Result<()>;
 fn add_signal_external_workflow_execution_failed_event(&self,initiated_id:i64,domain:&str,workflow_id:&str,run_id:&str,control:&[u8])->Result<()>;
 /// `execution.ScheduleDecision`
 fn schedule_decision(&self)->Result<()>;
 async fn completion_event(&self)->Result<CloseEvent>;
 async fn start_event(&self)->Result<StartEvent>;
 async fn child_execution_initiated_event(&self,initiated_id:i64)->Result<StartChildAttributes>;
}

/// TODO(link): replace by execution::Context together with its release function.
#[async_trait]
pub trait ActiveTransferContext:Send+Sync {
 /// Go `release(err)`; implementations must tolerate repeated calls.
 fn release(&self,error:Option<TaskError>);
 /// Go `loadMutableState`
 async fn load_mutable_state(&self,task:&dyn PersistenceTask,scope:&Arc<dyn MetricsScope>,logger:&Logger,event_id:i64)->Result<Option<Arc<dyn ActiveMutableState>>>;
 async fn load_workflow_execution(&self)->Result<Arc<dyn ActiveMutableState>>;
 async fn update_workflow_execution_as_active(&self,now:Time)->Result<()>;
}

/// TODO(link): replace by execution::Cache, history.Client, parentclosepolicy.Client, reset.WorkflowResetter,
/// workflowcache.WFCache, persistence.ExecutionManager, simulation and config::Config.
#[async_trait]
pub trait TransferActiveDependencies:Send+Sync {
 // execution.Cache
 async fn get_or_create_workflow_execution_with_timeout(&self,domain_id:&str,workflow_id:&str,run_id:&str,timeout:Duration)->Result<Arc<dyn ActiveTransferContext>>;
 // workflowcache.WFCache
 fn allow_internal(&self,domain_id:&str,workflow_id:&str)->bool;
 // history.Client
 async fn record_child_execution_completed(&self,request:RecordChildExecutionCompletedRequest)->Result<()>;
 async fn request_cancel_workflow_execution(&self,request:HistoryRequestCancelRequest)->Result<()>;
 async fn signal_workflow_execution(&self,request:HistorySignalRequest)->Result<()>;
 async fn remove_signal_mutable_state(&self,request:RemoveSignalMutableStateRequest)->Result<()>;
 /// Returns the started run id (`StartWorkflowExecutionResponse.GetRunID`).
 async fn start_workflow_execution(&self,request:HistoryStartWorkflowRequest)->Result<String>;
 async fn schedule_decision_task(&self,request:ScheduleDecisionTaskRequest)->Result<()>;
 async fn terminate_workflow_execution(&self,request:HistoryTerminateRequest)->Result<()>;
 // common.CreateHistoryStartWorkflowRequest
 fn create_history_start_workflow_request(&self,target_domain_id:&str,request:FrontendStartRequest,now:Time,partition_config:BytesMap)->Result<HistoryStartWorkflowRequest>;
 // parentclosepolicy.Client
 async fn send_parent_close_policy_request(&self,request:ParentClosePolicyRequest)->Result<()>;
 // reset.WorkflowResetter
 async fn reset_workflow(&self,request:WorkflowResetRequest)->Result<()>;
 // persistence.ExecutionManager.GetCurrentExecution, returns the current run id
 async fn get_current_execution(&self,domain_id:&str,workflow_id:&str,domain_name:&str,shard_id:i32)->Result<String>;
 // execution.GetChildExecutionDomainID
 fn get_child_execution_domain_id(&self,child:&ChildExecutionInfo,parent_domain:&DomainCacheEntry)->Result<String>;
 // config.Config
 fn advanced_visibility_writing_enabled(&self)->bool;
 fn enable_parent_close_policy_worker(&self)->bool;
 fn parent_close_policy_threshold(&self,domain:&str)->usize;
 fn parent_close_policy_batch_size(&self,domain:&str)->usize;
 // simulation.LogEvents(EventNameExecuteHistoryTask)
 fn log_execute_history_task(&self,shard_id:i32,task:&dyn PersistenceTask);
}

pub struct TransferActiveTaskExecutor {pub base:TransferTaskExecutorBase,pub dependencies:Arc<dyn TransferActiveDependencies>}
/// `NewTransferActiveTaskExecutor`
pub fn new_transfer_active_task_executor(base:TransferTaskExecutorBase,dependencies:Arc<dyn TransferActiveDependencies>)->Arc<dyn Executor>{Arc::new(TransferActiveTaskExecutor {base,dependencies})}

fn tg(key:&'static str,value:&str)->Tag{Tag {key,value:value.to_string()}}
fn ignore_completed(result:Result<()>)->Result<()> {
 match result {Err(TaskError::EntityNotExists(_))|Err(TaskError::WorkflowExecutionAlreadyCompleted(_))=>Ok(()),other=>other}
}
/// `common.IsExpectedError`
fn is_expected_error(e:&TaskError)->bool{e.is_service_transient()||matches!(e,TaskError::EntityNotExists(_)|TaskError::WorkflowExecutionAlreadyCompleted(_))}
fn execution_ref(workflow_id:&str,run_id:&str)->WorkflowExecutionRef{WorkflowExecutionRef {workflow_id:workflow_id.into(),run_id:run_id.into()}}

struct ResetPlan {
 domain_name:String,reason:String,reset_point:ResetPointInfo,base_context:Arc<dyn ActiveTransferContext>,base_mutable_state:Arc<dyn ActiveMutableState>,
 current_mutable_state:Arc<dyn ActiveMutableState>,logger:Logger,
}

impl TransferActiveTaskExecutor {
 fn queue_scope(&self)->Arc<dyn MetricsScope>{self.base.shard.get_metrics_client().scope("TransferQueueProcessorScope",vec![])}
 fn now(&self)->Time{self.base.shard.get_time_source().now()}
 /// `GetOrCreateWorkflowExecutionWithTimeout` without error mapping.
 async fn get_context_raw(&self,domain_id:&str,workflow_id:&str,run_id:&str)->Result<Arc<dyn ActiveTransferContext>> {
  self.dependencies.get_or_create_workflow_execution_with_timeout(domain_id,workflow_id,run_id,TASK_GET_EXECUTION_CONTEXT_TIMEOUT).await
 }
 /// `GetOrCreateWorkflowExecutionWithTimeout` mapping `context.DeadlineExceeded` to `errWorkflowBusy`.
 async fn get_context(&self,domain_id:&str,workflow_id:&str,run_id:&str)->Result<Arc<dyn ActiveTransferContext>> {
  self.get_context_raw(domain_id,workflow_id,run_id).await.map_err(|e|if e==TaskError::ContextDeadlineExceeded{err_workflow_busy()}else{e})
 }
 /// Bounds an RPC with `taskRPCCallTimeout` and retries it with `taskRetryPolicy`/`IsServiceTransientError`.
 async fn rpc_with_retry<F,Fut>(&self,op:F)->Result<()> where F:FnMut()->Fut+Send,Fut:Future<Output=Result<()>>+Send {
  tokio::time::timeout(TASK_RPC_CALL_TIMEOUT,self.base.throttle_retry.do_op(op)).await.unwrap_or(Err(TaskError::ContextDeadlineExceeded))
 }
 /// Bounds a single RPC with `taskRPCCallTimeout`.
 async fn rpc_with_timeout<Fut>(&self,fut:Fut)->Result<()> where Fut:Future<Output=Result<()>>+Send {
  tokio::time::timeout(TASK_RPC_CALL_TIMEOUT,fut).await.unwrap_or(Err(TaskError::ContextDeadlineExceeded))
 }
 fn record_schedule_to_start(&self,domain_name:&str,task_list:&TaskList,scope_idx:ScopeIdx,task:&dyn PersistenceTask) {
  let scope=self.base.shard.get_metrics_client().scope(scope_idx,vec![domain_tag(domain_name),get_task_list_tag(&task_list.name,task_list.kind)]);
  let latency=self.now().sub(task.get_visibility_timestamp());
  scope.record_timer("ScheduleToStartHistoryQueueLatencyPerTaskList",latency);
  scope.exponential_histogram("ScheduleToStartHistoryQueueLatencyPerTaskListHistogram",latency);
 }
 fn allow_task(&self,task:&dyn PersistenceTask)->bool{self.dependencies.allow_internal(task.get_domain_id(),task.get_workflow_id())}
 fn blank_record(&self,task:&dyn PersistenceTask)->VisibilityRecord {
  VisibilityRecord {
   domain_id:task.get_domain_id().into(),domain:String::new(),workflow_id:task.get_workflow_id().into(),run_id:task.get_run_id().into(),workflow_type_name:String::new(),
   start_timestamp:0,execution_timestamp:0,close_timestamp:0,workflow_timeout:0,task_id:task.get_task_id(),memo:None,task_list:String::new(),is_cron:false,
   cron_schedule:String::new(),num_clusters:0,cluster_attribute:None,update_timestamp:0,search_attributes:BytesMap::new(),headers:BytesMap::new(),shard_id:0,
   execution_status:WorkflowExecutionStatus::Started,scheduled_execution_timestamp:0,close_status:0,history_length:0,retention_seconds:0,
  }
 }

 // -------------------------------------------------------------------------
 // processActivityTask / processDecisionTask
 // -------------------------------------------------------------------------
 pub async fn process_activity_task(&self,task:&ActivityTask)->Result<()> {
  if !self.allow_task(task){return Err(err_workflow_rate_limited());}
  let ctx=self.get_context(task.domain_id(),task.workflow_id(),task.run_id()).await?;
  let result=self.process_activity_task_locked(ctx.as_ref(),task).await;
  ctx.release(result.clone().err());result
 }
 async fn process_activity_task_locked(&self,ctx:&dyn ActiveTransferContext,task:&ActivityTask)->Result<()> {
  let Some(ms)=ctx.load_mutable_state(task,&self.queue_scope(),&self.base.logger,task.schedule_id).await? else{return Ok(());};
  if !ms.is_workflow_execution_running(){return Ok(());}
  let domain_name=ms.domain_name();
  let Some(ai)=ms.activity_info(task.schedule_id) else{
   self.base.logger.debug("Potentially duplicate ",&[tag::task_id(task.task_data.task_id),tag::workflow_schedule_id(task.schedule_id),tag::task_type(ptask_type::TRANSFER_TASK_TYPE_ACTIVITY_TASK)]);
   return Ok(());
  };
  if !verify_task_version(&self.base.shard,&self.base.logger,task.domain_id(),ai.version,task.get_version(),task)?{return Ok(());}
  let timeout=ai.schedule_to_start_timeout.min(MAX_TASK_TIMEOUT);
  let mut task_list=TaskList {name:ai.task_list.clone(),kind:ai.task_list_kind};
  if task_list.name.is_empty(){task_list.name=task.task_list.clone();}
  // release the context lock since we no longer need mutable state builder and
  // the rest of logic is making RPC call, which takes time.
  ctx.release(None);
  let info=PushMatchingInfo {task_list:task_list.clone(),schedule_to_start_timeout:timeout,partition_config:ms.execution_info().partition_config};
  let result=self.base.push_activity(task,&info).await;
  if result.is_ok(){self.record_schedule_to_start(&domain_name,&task_list,metrics::TRANSFER_ACTIVE_TASK_ACTIVITY_SCOPE,task);}
  result
 }
 pub async fn process_decision_task(&self,task:&DecisionTask)->Result<()> {
  let ctx=self.get_context(task.domain_id(),task.workflow_id(),task.run_id()).await?;
  let result=self.process_decision_task_locked(ctx.as_ref(),task).await;
  ctx.release(result.clone().err());result
 }
 async fn process_decision_task_locked(&self,ctx:&dyn ActiveTransferContext,task:&DecisionTask)->Result<()> {
  let Some(ms)=ctx.load_mutable_state(task,&self.queue_scope(),&self.base.logger,task.schedule_id).await? else{return Ok(());};
  if !ms.is_workflow_execution_running(){return Ok(());}
  let Some(decision)=ms.decision_info(task.schedule_id) else{
   self.base.logger.debug("Potentially duplicate ",&[tag::task_id(task.task_data.task_id),tag::workflow_schedule_id(task.schedule_id),tag::task_type(ptask_type::TRANSFER_TASK_TYPE_DECISION_TASK)]);
   return Ok(());
  };
  if !verify_task_version(&self.base.shard,&self.base.logger,task.domain_id(),decision.version,task.get_version(),task)?{return Ok(());}
  let domain_name=ms.domain_name();
  let execution_info=ms.execution_info();
  let mut decision_timeout=execution_info.workflow_timeout.min(MAX_TASK_TIMEOUT);
  // NOTE: previously this section check whether mutable state has enabled sticky decision ...
  // the correct logic should check whether the decision task is a sticky decision task or not.
  let mut task_list=TaskList {name:task.task_list.clone(),kind:execution_info.task_list_kind};
  if ms.execution_info().task_list!=task.task_list {
   // this decision is an sticky decision, there shall already be an timer set
   task_list.kind=TaskListKind::Sticky;
   decision_timeout=execution_info.sticky_schedule_to_start_timeout;
  }
  // release the context lock since we no longer need mutable state builder and
  // the rest of logic is making RPC call, which takes time.
  ctx.release(None);
  // Rate limiting task processing requests
  if !self.allow_task(task){return Err(err_workflow_rate_limited());}
  let info=PushMatchingInfo {task_list:task_list.clone(),schedule_to_start_timeout:decision_timeout,partition_config:ms.execution_info().partition_config};
  let mut result=self.base.push_decision(task,&info).await;
  if matches!(result,Err(TaskError::StickyWorkerUnavailable(_))) {
   // sticky worker is unavailable, switch to non-sticky task list
   task_list=TaskList {name:ms.execution_info().task_list,kind:TaskListKind::Normal};
   // Continue to use sticky schedule_to_start timeout as TTL for the matching task.
   let info=PushMatchingInfo {task_list:task_list.clone(),schedule_to_start_timeout:decision_timeout,partition_config:ms.execution_info().partition_config};
   result=self.base.push_decision(task,&info).await;
  }
  if result.is_ok(){self.record_schedule_to_start(&domain_name,&task_list,metrics::TRANSFER_ACTIVE_TASK_DECISION_SCOPE,task);}
  result
 }

 // -------------------------------------------------------------------------
 // close execution
 // -------------------------------------------------------------------------
 pub async fn process_close_execution(&self,task:&CloseExecutionTask)->Result<()>{self.process_close_execution_task_helper(task,true,true,true).await}
 pub async fn process_record_workflow_closed(&self,task:&RecordWorkflowClosedTask)->Result<()>{self.process_close_execution_task_helper(task,true,false,false).await}
 pub async fn process_record_child_execution_completed(&self,task:&RecordChildExecutionCompletedTask)->Result<()>{self.process_close_execution_task_helper(task,false,true,false).await}
 /// This helper performs three operations: publish workflow closed visibility record, reply to the parent
 /// workflow and apply parent close policy to child workflows.
 pub async fn process_close_execution_task_helper(&self,task:&dyn PersistenceTask,record_workflow_closed:bool,reply_to_parent_workflow_if_applicable:bool,apply_parent_close_policy:bool)->Result<()> {
  let ctx=self.get_context(task.get_domain_id(),task.get_workflow_id(),task.get_run_id()).await?;
  let result=self.process_close_execution_locked(ctx.as_ref(),task,record_workflow_closed,reply_to_parent_workflow_if_applicable,apply_parent_close_policy).await;
  ctx.release(result.clone().err());result
 }
 async fn process_close_execution_locked(&self,ctx:&dyn ActiveTransferContext,task:&dyn PersistenceTask,record_workflow_closed:bool,reply_to_parent_workflow_if_applicable:bool,apply_parent_close_policy:bool)->Result<()> {
  let Some(ms)=ctx.load_mutable_state(task,&self.queue_scope(),&self.base.logger,0).await? else{return Ok(());};
  if ms.is_workflow_execution_running(){return Ok(());}
  let last_write_version=ms.last_write_version()?;
  if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),last_write_version,task.get_version(),task)?{return Ok(());}
  let domain_entry=self.base.shard.get_domain_cache().get_domain_by_id(task.get_domain_id())?;
  let execution_info=ms.execution_info();
  let completion_event=ms.completion_event().await?;
  let wf_close_time=completion_event.timestamp;
  let workflow_history_length=ms.next_event_id()-1;
  let num_clusters=domain_entry.get_replication_config().clusters.len() as i16;
  let update_timestamp=self.now();
  let start_event=ms.start_event().await?;
  let workflow_execution_timestamp=get_workflow_execution_timestamp(Some(&start_event));
  let visibility_memo=get_workflow_memo(execution_info.memo.as_ref());
  let headers=get_workflow_headers(Some(&start_event));
  let domain_name=ms.domain_name();
  let children=ms.pending_child_execution_infos();
  let execution_status=get_workflow_execution_status(ms.as_ref(),&completion_event);
  // Calculate ScheduledExecutionTime
  let mut scheduled_execution_timestamp=start_event.timestamp;
  if start_event.first_decision_task_backoff_seconds>0 {scheduled_execution_timestamp=start_event.timestamp+i64::from(start_event.first_decision_task_backoff_seconds)*1_000_000_000;}
  let has_parent=ms.has_parent_execution();
  // we've gathered all necessary information from mutable state.
  // release the context lock since we no longer need mutable state builder and
  // the rest of logic is making RPC call, which takes time.
  ctx.release(None);
  // publish workflow closed visibility records
  if record_workflow_closed {
   let mut record=self.blank_record(task);
   record.workflow_type_name=execution_info.workflow_type_name.clone();record.start_timestamp=start_event.timestamp;
   record.execution_timestamp=workflow_execution_timestamp.unix_nano();record.close_timestamp=wf_close_time;record.close_status=execution_info.close_status;
   record.history_length=workflow_history_length;record.memo=visibility_memo;record.task_list=execution_info.task_list.clone();
   record.is_cron = !execution_info.cron_schedule.is_empty();record.cron_schedule=execution_info.cron_schedule.clone();record.num_clusters=num_clusters;
   record.update_timestamp=update_timestamp.unix_nano();record.search_attributes=execution_info.search_attributes.clone();record.headers=headers.unwrap_or_default();
   record.cluster_attribute=execution_info.active_cluster_selection_policy.as_ref().and_then(|p|p.cluster_attribute.clone());
   record.execution_status=execution_status;record.scheduled_execution_timestamp=scheduled_execution_timestamp;
   self.base.record_workflow_closed(record).await?;
  }
  // Communicate the result to parent execution if this is Child Workflow execution
  // and parent domain is in the same cluster
  let reply_to_parent_workflow=reply_to_parent_workflow_if_applicable&&has_parent&&!execution_info.is_continued_as_new;
  if reply_to_parent_workflow {
   let request=RecordChildExecutionCompletedRequest {
    domain_uuid:execution_info.parent_domain_id.clone(),workflow_execution:execution_ref(&execution_info.parent_workflow_id,&execution_info.parent_run_id),
    initiated_id:execution_info.initiated_id,completed_execution:execution_ref(task.get_workflow_id(),task.get_run_id()),completion_event:completion_event.clone(),
   };
   let result=self.rpc_with_timeout(self.dependencies.record_child_execution_completed(request)).await;
   // Check to see if the error is non-transient, in which case reset the error and continue with processing
   ignore_completed(result)?;
  }
  if apply_parent_close_policy {
   let parent_execution=execution_ref(task.get_workflow_id(),task.get_run_id());
   self.process_parent_close_policy(task.get_domain_id(),&domain_name,&parent_execution,&children).await?;
  }
  Ok(())
 }

 // -------------------------------------------------------------------------
 // cancel / signal
 // -------------------------------------------------------------------------
 fn cancel_signal_tags(&self,task:&dyn PersistenceTask,target_domain_id:&str,target_workflow_id:&str,target_run_id:&str)->Vec<Tag> {
  vec![tag::workflow_domain_id(task.get_domain_id()),tag::workflow_id(task.get_workflow_id()),tag::workflow_run_id(task.get_run_id()),
   tg("wf-target-domain-id",target_domain_id),tg("wf-target-id",target_workflow_id),tg("wf-target-run-id",target_run_id)]
 }
 pub async fn process_cancel_execution(&self,task:&CancelExecutionTask)->Result<()> {
  let ctx=self.get_context(task.domain_id(),task.workflow_id(),task.run_id()).await?;
  let result=self.process_cancel_execution_locked(ctx.as_ref(),task).await;
  ctx.release(result.clone().err());result
 }
 async fn process_cancel_execution_locked(&self,ctx:&dyn ActiveTransferContext,task:&CancelExecutionTask)->Result<()> {
  let Some(ms)=ctx.load_mutable_state(task,&self.queue_scope(),&self.base.logger,task.initiated_id).await? else{return Ok(());};
  if !ms.is_workflow_execution_running(){return Ok(());}
  let initiated_event_id=task.initiated_id;
  let Some(request_cancel_info)=ms.request_cancel_info(initiated_event_id) else{return Ok(());};
  if !verify_task_version(&self.base.shard,&self.base.logger,task.domain_id(),request_cancel_info.version,task.get_version(),task)?{return Ok(());}
  // TODO: handle the case where target domain does not exist
  let target_domain_entry=self.base.shard.get_domain_cache().get_domain_by_id(&task.target_domain_id)?;
  let target_domain_name=target_domain_entry.get_info().name.clone();
  // handle workflow cancel itself
  if task.domain_id()==task.target_domain_id&&task.workflow_id()==task.target_workflow_id {
   // it does not matter if the run ID is a mismatch
   return self.request_cancel_external_execution_failed(ctx,task,&target_domain_name,&task.target_workflow_id,&task.target_run_id,self.now()).await;
  }
  if let Err(err)=self.request_cancel_external_execution_with_retry(task,&target_domain_name,&request_cancel_info.cancel_request_id).await {
   let mut tags=self.cancel_signal_tags(task,&task.target_domain_id,&task.target_workflow_id,&task.target_run_id);tags.push(tag::error(&err));
   self.base.logger.error("Failed to cancel external workflow execution",&tags);
   // Check to see if the error is non-transient, in which case add RequestCancelFailed
   // event and complete transfer task by setting the err = nil
   if err.is_service_transient()||err.is_context_timeout(){
    // for retryable error just return
    return Err(err);
   }
   return self.request_cancel_external_execution_failed(ctx,task,&target_domain_name,&task.target_workflow_id,&task.target_run_id,self.now()).await;
  }
  self.base.logger.debug("RequestCancel successfully recorded to external workflow execution",&self.cancel_signal_tags(task,&task.target_domain_id,&task.target_workflow_id,&task.target_run_id));
  // Record ExternalWorkflowExecutionCancelRequested in source execution
  self.request_cancel_external_execution_completed(ctx,task,&target_domain_name,&task.target_workflow_id,&task.target_run_id,self.now()).await
 }
 pub async fn process_signal_execution(&self,task:&SignalExecutionTask)->Result<()> {
  let ctx=self.get_context(task.domain_id(),task.workflow_id(),task.run_id()).await?;
  let result=self.process_signal_execution_locked(ctx.as_ref(),task).await;
  // `release(retError)` of the deferred call; the explicit mid-way release(retError) is inside the locked part.
  ctx.release(result.clone().err());result
 }
 async fn process_signal_execution_locked(&self,ctx:&dyn ActiveTransferContext,task:&SignalExecutionTask)->Result<()> {
  let Some(ms)=ctx.load_mutable_state(task,&self.queue_scope(),&self.base.logger,task.initiated_id).await? else{return Ok(());};
  if !ms.is_workflow_execution_running(){return Ok(());}
  let initiated_event_id=task.initiated_id;
  // TODO: here we should also RemoveSignalMutableState from target workflow
  let Some(signal_info)=ms.signal_info(initiated_event_id) else{return Ok(());};
  if !verify_task_version(&self.base.shard,&self.base.logger,task.domain_id(),signal_info.version,task.get_version(),task)?{return Ok(());}
  // TODO: handle the case where target domain does not exist
  let target_domain_entry=self.base.shard.get_domain_cache().get_domain_by_id(&task.target_domain_id)?;
  let target_domain_name=target_domain_entry.get_info().name.clone();
  // handle workflow signal itself
  if task.domain_id()==task.target_domain_id&&task.workflow_id()==task.target_workflow_id {
   // it does not matter if the run ID is a mismatch
   return self.signal_external_execution_failed(ctx,task,&target_domain_name,&task.target_workflow_id,&task.target_run_id,&signal_info.control,self.now()).await;
  }
  if let Err(err)=self.signal_external_execution_with_retry(task,&target_domain_name,&signal_info).await {
   if !is_expected_error(&err) {
    let mut tags=self.cancel_signal_tags(task,&task.target_domain_id,&task.target_workflow_id,&task.target_run_id);tags.push(tag::error(&err));
    self.base.logger.error("Failed to signal external workflow execution",&tags);
   }
   // Check to see if the error is non-transient, in which case add SignalFailed
   // event and complete transfer task by setting the err = nil
   if err.is_service_transient()||err.is_context_timeout(){
    // for retryable error just return
    return Err(err);
   }
   return self.signal_external_execution_failed(ctx,task,&target_domain_name,&task.target_workflow_id,&task.target_run_id,&signal_info.control,self.now()).await;
  }
  self.base.logger.debug("Signal successfully recorded to external workflow execution",&self.cancel_signal_tags(task,&task.target_domain_id,&task.target_workflow_id,&task.target_run_id));
  self.signal_external_execution_completed(ctx,task,&target_domain_name,&task.target_workflow_id,&task.target_run_id,&signal_info.control,self.now()).await?;
  // release the context lock since we no longer need mutable state builder and
  // the rest of logic is making RPC call, which takes time.
  ctx.release(None);
  // remove signalRequestedID from target workflow, after Signal detail is removed from source workflow
  self.remove_signal_mutable_state_with_retry(task,&signal_info.signal_request_id).await
 }

 // -------------------------------------------------------------------------
 // start child execution
 // -------------------------------------------------------------------------
 pub async fn process_start_child_execution(&self,task:&StartChildExecutionTask)->Result<()> {
  let ctx=self.get_context(task.domain_id(),task.workflow_id(),task.run_id()).await?;
  let result=self.process_start_child_execution_locked(ctx.as_ref(),task).await;
  ctx.release(result.clone().err());result
 }
 async fn process_start_child_execution_locked(&self,ctx:&dyn ActiveTransferContext,task:&StartChildExecutionTask)->Result<()> {
  let Some(ms)=ctx.load_mutable_state(task,&self.queue_scope(),&self.base.logger,task.initiated_id).await? else{return Ok(());};
  let initiated_event_id=task.initiated_id;
  let Some(child_info)=ms.child_execution_info(initiated_event_id) else{return Ok(());};
  if !verify_task_version(&self.base.shard,&self.base.logger,task.domain_id(),child_info.version,task.get_version(),task)?{return Ok(());}
  let workflow_running=ms.is_workflow_execution_running();
  let child_started=child_info.started_id!=EMPTY_EVENT_ID;
  if !workflow_running&&(!child_started||child_info.parent_close_policy!=ParentClosePolicy::Abandon) {
   // three cases here:
   // case 1: workflow not running, child started, close policy is not abandon
   // case 2 & 3: workflow not running, child not started, close policy is or is not abandon
   return Ok(());
  }
  // Get target domain name
  let target_domain_name=match self.base.shard.get_domain_cache().get_domain_by_id(&task.target_domain_id) {
   Ok(entry)=>entry.get_info().name.clone(),
   // it is possible that the domain got deleted. Use domainID instead as this is only needed for the history event
   Err(TaskError::EntityNotExists(_))=>task.target_domain_id.clone(),
   Err(e)=>return Err(e),
  };
  // ChildExecution already started, just create DecisionTask and complete transfer task
  // if parent already closed, since child workflow started event already written to history,
  // still schedule the decision if the parent close policy is Abandon.
  if child_started {
   // NOTE: do not access anything related mutable state after this lock release
   ctx.release(None);
   return self.create_first_decision_task(&task.target_domain_id,&execution_ref(&child_info.started_workflow_id,&child_info.started_run_id)).await;
  }
  // remaining 2 cases: workflow running, child not started, close policy is or is not abandon
  let attributes=ms.child_execution_initiated_event(initiated_event_id).await?;
  let child_run_id=match self.start_workflow_with_retry(task,&target_domain_name,&child_info.create_request_id,&attributes,ms.execution_info().partition_config,ms.execution_info().active_cluster_selection_policy).await {
   Ok(run_id)=>run_id,
   Err(err)=>{
    // Check to see if the error is non-transient, in which case add StartChildWorkflowExecutionFailed
    // event and complete transfer task by setting the err = nil
    // TODO: we should also handle domain not exist error here
    return match err {
     TaskError::WorkflowExecutionAlreadyStarted(_)=>{
      self.base.logger.info("workflow has already started",&[tag::workflow_domain_id(task.domain_id()),tag::workflow_id(task.workflow_id()),tag::workflow_run_id(task.run_id()),
       tg("wf-target-domain-id",&task.target_domain_id),tg("wf-target-id",&attributes.workflow_id),tg("wf-request-id",&child_info.create_request_id)]);
      self.record_start_child_execution_failed(ctx,task,&attributes,self.now()).await
     },
     err=>{
      self.base.logger.error("Failed to start child workflow execution",&[tag::workflow_domain_id(task.domain_id()),tag::workflow_id(task.workflow_id()),tag::workflow_run_id(task.run_id()),
       tg("wf-target-domain-id",&task.target_domain_id),tg("wf-target-id",&attributes.workflow_id),tag::error(&err)]);
      Err(err)
     },
    };
   },
  };
  self.base.logger.debug("Child Execution started successfully",&[tag::workflow_domain_id(task.domain_id()),tag::workflow_id(task.workflow_id()),tag::workflow_run_id(task.run_id()),
   tg("wf-target-domain-id",&task.target_domain_id),tg("wf-target-id",&attributes.workflow_id),tg("wf-target-run-id",&child_run_id)]);
  // Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
  self.record_child_execution_started(ctx,task,&attributes,&child_run_id,self.now()).await?;
  // NOTE: do not access anything related mutable state after this lock release
  ctx.release(None);
  // Finally create first decision task for Child execution so it is really started
  // entity not exist error is checked and ignored in HandleErr() method in task.go
  self.create_first_decision_task(&task.target_domain_id,&execution_ref(&task.target_workflow_id,&child_run_id)).await
 }

 // -------------------------------------------------------------------------
 // record workflow started / upsert search attributes
 // -------------------------------------------------------------------------
 pub async fn process_record_workflow_started(&self,task:&RecordWorkflowStartedTask)->Result<()>{self.process_record_workflow_started_or_upsert_helper(task,true).await}
 pub async fn process_upsert_workflow_search_attributes(&self,task:&UpsertWorkflowSearchAttributesTask)->Result<()>{self.process_record_workflow_started_or_upsert_helper(task,false).await}
 pub async fn process_record_workflow_started_or_upsert_helper(&self,task:&dyn PersistenceTask,record_start:bool)->Result<()> {
  let workflow_started_scope=get_or_create_domain_tagged_scope(&self.base.shard,metrics::TRANSFER_ACTIVE_TASK_RECORD_WORKFLOW_STARTED_SCOPE,task.get_domain_id(),&self.base.logger);
  let ctx=self.get_context(task.get_domain_id(),task.get_workflow_id(),task.get_run_id()).await?;
  let result=self.process_record_workflow_started_or_upsert_locked(ctx.as_ref(),task,record_start,&workflow_started_scope).await;
  ctx.release(result.clone().err());result
 }
 async fn process_record_workflow_started_or_upsert_locked(&self,ctx:&dyn ActiveTransferContext,task:&dyn PersistenceTask,record_start:bool,workflow_started_scope:&Arc<dyn MetricsScope>)->Result<()> {
  let Some(ms)=ctx.load_mutable_state(task,&self.queue_scope(),&self.base.logger,0).await? else{return Ok(());};
  if !ms.is_workflow_execution_running(){return Ok(());}
  // verify task version for RecordWorkflowStarted.
  // upsert doesn't require verifyTask, because it is just a sync of mutableState.
  if record_start {
   let start_version=ms.start_version()?;
   if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),start_version,task.get_version(),task)?{return Ok(());}
  }
  let domain_entry=self.base.shard.get_domain_cache().get_domain_by_id(task.get_domain_id())?;
  let execution_info=ms.execution_info();
  let workflow_timeout=execution_info.workflow_timeout;
  let start_event=ms.start_event().await?;
  let execution_timestamp=get_workflow_execution_timestamp(Some(&start_event));
  let visibility_memo=get_workflow_memo(execution_info.memo.as_ref());
  let search_attr=copy_search_attributes(Some(&execution_info.search_attributes));
  let headers=get_workflow_headers(Some(&start_event));
  let is_cron=!execution_info.cron_schedule.is_empty();
  let cron_schedule=if is_cron{execution_info.cron_schedule.clone()}else{String::new()};
  let num_clusters=domain_entry.get_replication_config().clusters.len() as i16;
  let update_timestamp=self.now();
  let is_advanced_visibility_enabled=self.dependencies.advanced_visibility_writing_enabled();
  let (execution_status,scheduled_execution_timestamp)=determine_execution_status_for_visibility(&start_event,ms.as_ref(),is_advanced_visibility_enabled);
  // release the context lock since we no longer need mutable state builder and
  // the rest of logic is making RPC call, which takes time.
  ctx.release(None);
  let mut record=self.blank_record(task);
  record.workflow_type_name=execution_info.workflow_type_name.clone();record.start_timestamp=start_event.timestamp;record.execution_timestamp=execution_timestamp.unix_nano();
  record.workflow_timeout=i64::from(workflow_timeout);record.task_list=execution_info.task_list.clone();record.is_cron=is_cron;record.num_clusters=num_clusters;
  record.memo=visibility_memo;record.update_timestamp=update_timestamp.unix_nano();record.search_attributes=search_attr.unwrap_or_default();record.headers=headers.unwrap_or_default();
  record.cluster_attribute=execution_info.active_cluster_selection_policy.as_ref().and_then(|p|p.cluster_attribute.clone());
  record.cron_schedule=cron_schedule;record.execution_status=execution_status;record.scheduled_execution_timestamp=scheduled_execution_timestamp;
  if record_start {
   workflow_started_scope.inc_counter("WorkflowStartedCount");
   return self.base.record_workflow_started(record).await;
  }
  self.base.upsert_workflow_execution(record).await
 }

 // -------------------------------------------------------------------------
 // reset workflow
 // -------------------------------------------------------------------------
 pub async fn process_reset_workflow(&self,task:&ResetWorkflowTask)->Result<()> {
  let current_context=self.get_context(task.domain_id(),task.workflow_id(),task.run_id()).await?;
  let result=self.process_reset_workflow_locked(&current_context,task).await;
  current_context.release(result.clone().err());result
 }
 async fn process_reset_workflow_locked(&self,current_context:&Arc<dyn ActiveTransferContext>,task:&ResetWorkflowTask)->Result<()> {
  let base_slot:Mutex<Option<Arc<dyn ActiveTransferContext>>>=Mutex::new(None);
  let prepared=match tokio::time::timeout(TASK_DEFAULT_TIMEOUT,self.prepare_reset_workflow(current_context,task,&base_slot)).await {Ok(r)=>r,Err(_)=>Err(TaskError::ContextDeadlineExceeded)};
  let base_release=base_slot.lock().unwrap().take();
  let plan=match prepared {
   Ok(Some(plan))=>plan,
   Ok(None)=>{if let Some(b)=base_release{b.release(None);}return Ok(());},
   Err(e)=>{if let Some(b)=base_release{b.release(Some(e.clone()));}return Err(e);},
  };
  // reset workflow needs to go through the history so it may take a long time.
  // as a result it's not subject to the taskDefaultTimeout.
  let result=self.reset_workflow(task,&plan.domain_name,&plan.reason,&plan.reset_point,plan.base_context.clone(),plan.base_mutable_state.clone(),current_context.clone(),plan.current_mutable_state.clone(),&plan.logger).await;
  if let Some(b)=base_release{b.release(result.clone().err());}
  result
 }
 async fn prepare_reset_workflow(&self,current_context:&Arc<dyn ActiveTransferContext>,task:&ResetWorkflowTask,base_slot:&Mutex<Option<Arc<dyn ActiveTransferContext>>>)->Result<Option<ResetPlan>> {
  let Some(current_ms)=current_context.load_mutable_state(task,&self.queue_scope(),&self.base.logger,0).await? else{return Ok(None);};
  let mut logger=self.base.logger.with_tags(vec![tag::workflow_domain_id(task.domain_id()),tag::workflow_id(task.workflow_id()),tag::workflow_run_id(task.run_id())]);
  let domain_name=self.base.shard.get_domain_cache().get_domain_name(task.domain_id())?;
  if !current_ms.is_workflow_execution_running() {
   // it means this this might not be current anymore, we need to check
   let current_run_id=self.dependencies.get_current_execution(task.domain_id(),task.workflow_id(),&domain_name,self.base.shard.get_shard_id()).await?;
   if current_run_id!=task.run_id() {
    logger.warn("Auto-Reset is skipped, because current run is stale.",&[]);
    return Ok(None);
   }
  }
  // TODO: current reset doesn't allow childWFs, in the future we will release this restriction
  if !current_ms.pending_child_execution_infos().is_empty() {
   logger.warn("Auto-Reset is skipped, because current run has pending child executions.",&[]);
   return Ok(None);
  }
  let current_start_version=current_ms.start_version()?;
  if !verify_task_version(&self.base.shard,&self.base.logger,task.domain_id(),current_start_version,task.get_version(),task)?{return Ok(None);}
  let execution_info=current_ms.execution_info();
  let domain_entry=self.base.shard.get_domain_cache().get_domain_by_id(&execution_info.domain_id)?;
  logger=logger.with_tags(vec![tag::workflow_domain_name(&domain_entry.get_info().name)]);
  let Some((reason,reset_point))=current_ms.find_auto_reset_point(self.base.shard.get_time_source().as_ref(),&domain_entry) else{
   logger.warn("Auto-Reset is skipped, because reset point is not found.",&[]);
   return Ok(None);
  };
  logger=logger.with_tags(vec![tg("wf-reset-base-run-id",&reset_point.run_id),tg("binary-checksum",&reset_point.binary_checksum),tag::workflow_event_id(reset_point.first_decision_completed_id)]);
  let (base_context,base_mutable_state)=if reset_point.run_id==execution_info.run_id {
   (current_context.clone(),current_ms.clone())
  } else {
   let base_context=self.get_context_raw(task.domain_id(),task.workflow_id(),&reset_point.run_id).await?;
   *base_slot.lock().unwrap()=Some(base_context.clone());
   let Some(base_ms)=base_context.load_mutable_state(task,&self.queue_scope(),&self.base.logger,0).await? else{return Ok(None);};
   (base_context,base_ms)
  };
  Ok(Some(ResetPlan {domain_name:domain_entry.get_info().name.clone(),reason,reset_point,base_context,base_mutable_state,current_mutable_state:current_ms,logger}))
 }
 #[allow(clippy::too_many_arguments)]
 async fn reset_workflow(&self,task:&ResetWorkflowTask,_domain:&str,reason:&str,reset_point:&ResetPointInfo,_base_context:Arc<dyn ActiveTransferContext>,base_mutable_state:Arc<dyn ActiveMutableState>,current_context:Arc<dyn ActiveTransferContext>,current_mutable_state:Arc<dyn ActiveMutableState>,logger:&Logger)->Result<()> {
  let reset_run_id=uuid::Uuid::new_v4().to_string();
  let base_run_id=base_mutable_state.execution_info().run_id;
  let base_rebuild_last_event_id=reset_point.first_decision_completed_id-1;
  let Some(base_current_version_history)=base_mutable_state.current_version_history()? else{return Err(TaskError::MissingVersionHistories);};
  let base_rebuild_last_event_version=base_current_version_history.get_event_version(base_rebuild_last_event_id)?;
  let base_current_branch_token=base_current_version_history.get_branch_token();
  let base_next_event_id=base_mutable_state.next_event_id();
  let request=WorkflowResetRequest {
   domain_id:task.domain_id().into(),workflow_id:task.workflow_id().into(),base_run_id,base_branch_token:base_current_branch_token,
   base_rebuild_last_event_id,base_rebuild_last_event_version,base_next_event_id,reset_run_id,reset_request_id:uuid::Uuid::new_v4().to_string(),
   current_context,current_mutable_state,current_logger:logger.clone(),reset_reason:reason.into(),skip_signal_reapply:false,
  };
  let result=tokio::time::timeout(RESET_WORKFLOW_TIMEOUT,self.dependencies.reset_workflow(request)).await.unwrap_or(Err(TaskError::ContextDeadlineExceeded));
  match result {
   Ok(())=>Ok(()),
   Err(err@TaskError::BadRequest(_))=>{
    // This means the reset point is corrupted and not retry able.
    // There must be a bug in our system that we must fix.(for example, history is not the same in active/passive)
    self.base.shard.get_metrics_client().inc_counter("TransferQueueProcessorScope","AutoResetPointCorruptionCounter");
    logger.error("Auto-Reset workflow failed and not retryable. The reset point is corrupted.",&[tag::error(&err)]);
    Ok(())
   },
   Err(err)=>{
    // log this error and retry
    logger.error("Auto-Reset workflow failed",&[tag::error(&err)]);
    Err(err)
   },
  }
 }

 // -------------------------------------------------------------------------
 // mutable state updates
 // -------------------------------------------------------------------------
 pub async fn record_child_execution_started(&self,ctx:&dyn ActiveTransferContext,task:&StartChildExecutionTask,initiated_attributes:&StartChildAttributes,run_id:&str,now:Time)->Result<()> {
  self.update_workflow_execution(ctx,true,|ms|{
   if !ms.is_workflow_execution_running(){return Err(TaskError::EntityNotExists("Workflow execution already completed.".into()));}
   let domain=&initiated_attributes.domain;
   let initiated_event_id=task.initiated_id;
   match ms.child_execution_info(initiated_event_id) {
    Some(ci) if ci.started_id==EMPTY_EVENT_ID=>{},
    _=>return Err(TaskError::EntityNotExists("Pending child execution not found.".into())),
   }
   ms.add_child_workflow_execution_started_event(domain,&execution_ref(&task.target_workflow_id,run_id),initiated_attributes,initiated_event_id)
  },now).await
 }
 pub async fn record_start_child_execution_failed(&self,ctx:&dyn ActiveTransferContext,task:&StartChildExecutionTask,initiated_attributes:&StartChildAttributes,now:Time)->Result<()> {
  self.update_workflow_execution(ctx,true,|ms|{
   if !ms.is_workflow_execution_running(){return Err(TaskError::EntityNotExists("Workflow execution already completed.".into()));}
   let initiated_event_id=task.initiated_id;
   match ms.child_execution_info(initiated_event_id) {
    Some(ci) if ci.started_id==EMPTY_EVENT_ID=>{},
    _=>return Err(TaskError::EntityNotExists("Pending child execution not found.".into())),
   }
   ms.add_start_child_workflow_execution_failed_event(initiated_event_id,initiated_attributes)
  },now).await
 }
 /// `createFirstDecisionTask` is used by StartChildExecution transfer task to create the first decision task for child execution.
 pub async fn create_first_decision_task(&self,domain_id:&str,execution:&WorkflowExecutionRef)->Result<()> {
  let request=ScheduleDecisionTaskRequest {domain_uuid:domain_id.into(),workflow_execution:execution.clone(),is_first_decision:true};
  match self.rpc_with_timeout(self.dependencies.schedule_decision_task(request)).await {
   // Maybe child workflow execution already timedout or terminated
   // Safe to discard the error and complete this transfer task
   Err(TaskError::WorkflowExecutionAlreadyCompleted(_))=>Ok(()),
   other=>other,
  }
 }
 pub async fn request_cancel_external_execution_completed(&self,ctx:&dyn ActiveTransferContext,task:&CancelExecutionTask,target_domain:&str,target_workflow_id:&str,target_run_id:&str,now:Time)->Result<()> {
  let result=self.update_workflow_execution(ctx,true,|ms|{
   if !ms.is_workflow_execution_running(){return Err(TaskError::WorkflowExecutionAlreadyCompleted("Workflow execution already completed.".into()));}
   let initiated_event_id=task.initiated_id;
   if ms.request_cancel_info(initiated_event_id).is_none(){return Err(err_missing_request_cancel_info());}
   ms.add_external_workflow_execution_cancel_requested(initiated_event_id,target_domain,target_workflow_id,target_run_id)
  },now).await;
  // this could happen if this is a duplicate processing of the task, or the execution has already completed.
  ignore_completed(result)
 }
 #[allow(clippy::too_many_arguments)]
 pub async fn signal_external_execution_completed(&self,ctx:&dyn ActiveTransferContext,task:&SignalExecutionTask,target_domain:&str,target_workflow_id:&str,target_run_id:&str,control:&[u8],now:Time)->Result<()> {
  let result=self.update_workflow_execution(ctx,true,|ms|{
   if !ms.is_workflow_execution_running(){return Err(TaskError::WorkflowExecutionAlreadyCompleted("Workflow execution already completed.".into()));}
   let initiated_event_id=task.initiated_id;
   if ms.signal_info(initiated_event_id).is_none(){return Err(err_missing_signal_info());}
   ms.add_external_workflow_execution_signaled(initiated_event_id,target_domain,target_workflow_id,target_run_id,control)
  },now).await;
  ignore_completed(result)
 }
 pub async fn request_cancel_external_execution_failed(&self,ctx:&dyn ActiveTransferContext,task:&CancelExecutionTask,target_domain:&str,target_workflow_id:&str,target_run_id:&str,now:Time)->Result<()> {
  let result=self.update_workflow_execution(ctx,true,|ms|{
   if !ms.is_workflow_execution_running(){return Err(TaskError::WorkflowExecutionAlreadyCompleted("Workflow execution already completed.".into()));}
   let initiated_event_id=task.initiated_id;
   if ms.request_cancel_info(initiated_event_id).is_none(){return Err(err_missing_request_cancel_info());}
   ms.add_request_cancel_external_workflow_execution_failed_event(initiated_event_id,target_domain,target_workflow_id,target_run_id)
  },now).await;
  ignore_completed(result)
 }
 #[allow(clippy::too_many_arguments)]
 pub async fn signal_external_execution_failed(&self,ctx:&dyn ActiveTransferContext,task:&SignalExecutionTask,target_domain:&str,target_workflow_id:&str,target_run_id:&str,control:&[u8],now:Time)->Result<()> {
  let result=self.update_workflow_execution(ctx,true,|ms|{
   if !ms.is_workflow_execution_running(){return Err(TaskError::WorkflowExecutionAlreadyCompleted("Workflow execution already completed.".into()));}
   let initiated_event_id=task.initiated_id;
   if ms.signal_info(initiated_event_id).is_none(){return Err(err_missing_signal_info());}
   ms.add_signal_external_workflow_execution_failed_event(initiated_event_id,target_domain,target_workflow_id,target_run_id,control)
  },now).await;
  ignore_completed(result)
 }
 /// `updateWorkflowExecution`
 pub async fn update_workflow_execution<F>(&self,ctx:&dyn ActiveTransferContext,create_decision_task:bool,action:F,now:Time)->Result<()> where F:FnOnce(&dyn ActiveMutableState)->Result<()>+Send {
  let ms=ctx.load_workflow_execution().await?;
  action(ms.as_ref())?;
  if create_decision_task {
   // Create a transfer task to schedule a decision task
   ms.schedule_decision()?;
  }
  self.base.logger.debug(&format!("transferActiveTaskExecutor.updateWorkflowExecution calling UpdateWorkflowExecutionAsActive for wfID {}",ms.execution_info().workflow_id),&[]);
  ctx.update_workflow_execution_as_active(now).await
 }

 // -------------------------------------------------------------------------
 // history client calls with retry
 // -------------------------------------------------------------------------
 pub async fn request_cancel_external_execution_with_retry(&self,task:&CancelExecutionTask,target_domain:&str,cancel_request_id:&str)->Result<()> {
  let request=HistoryRequestCancelRequest {
   domain_uuid:task.target_domain_id.clone(),domain:target_domain.into(),workflow_execution:execution_ref(&task.target_workflow_id,&task.target_run_id),
   identity:IDENTITY_HISTORY_SERVICE.into(),
   // Use the same request ID to dedupe RequestCancelWorkflowExecution calls
   request_id:cancel_request_id.into(),first_execution_run_id:String::new(),external_initiated_event_id:Some(task.initiated_id),
   external_workflow_execution:Some(execution_ref(task.workflow_id(),task.run_id())),child_workflow_only:task.target_child_workflow_only,
  };
  let deps=&self.dependencies;
  let result=self.rpc_with_retry(||{let request=request.clone();async move{deps.request_cancel_workflow_execution(request).await}}).await;
  match result {
   // this could happen if target workflow cancellation is already requested, mark as success
   Err(TaskError::CancellationAlreadyRequested(_))=>Ok(()),
   other=>other,
  }
 }
 pub async fn signal_external_execution_with_retry(&self,task:&SignalExecutionTask,target_domain:&str,signal_info:&SignalInfo)->Result<()> {
  let request=HistorySignalRequest {
   domain_uuid:task.target_domain_id.clone(),domain:target_domain.into(),workflow_execution:execution_ref(&task.target_workflow_id,&task.target_run_id),
   identity:IDENTITY_HISTORY_SERVICE.into(),signal_name:signal_info.signal_name.clone(),input:signal_info.input.clone(),
   // Use same request ID to deduplicate SignalWorkflowExecution calls
   request_id:signal_info.signal_request_id.clone(),control:signal_info.control.clone(),
   external_workflow_execution:Some(execution_ref(task.workflow_id(),task.run_id())),child_workflow_only:task.target_child_workflow_only,
  };
  let deps=&self.dependencies;
  self.rpc_with_retry(||{let request=request.clone();async move{deps.signal_workflow_execution(request).await}}).await
 }
 pub async fn remove_signal_mutable_state_with_retry(&self,task:&SignalExecutionTask,signal_request_id:&str)->Result<()> {
  let request=RemoveSignalMutableStateRequest {domain_uuid:task.target_domain_id.clone(),workflow_execution:execution_ref(&task.target_workflow_id,&task.target_run_id),request_id:signal_request_id.into()};
  let deps=&self.dependencies;
  let result=self.rpc_with_retry(||{let request=request.clone();async move{deps.remove_signal_mutable_state(request).await}}).await;
  match result {
   // it's safe to discard entity not exists error here as there's nothing to remove.
   Err(TaskError::EntityNotExists(_))=>Ok(()),
   other=>other,
  }
 }
 #[allow(clippy::too_many_arguments)]
 pub async fn start_workflow_with_retry(&self,task:&StartChildExecutionTask,target_domain:&str,request_id:&str,attributes:&StartChildAttributes,partition_config:BytesMap,active_cluster_selection_policy:Option<ActiveClusterSelectionPolicy>)->Result<String> {
  // Get parent domain name
  let domain_name=match self.base.shard.get_domain_cache().get_domain_name(task.domain_id()) {
   Ok(name)=>name,
   // it is possible that the domain got deleted. Use domainID instead as this is only needed for the history event
   Err(TaskError::EntityNotExists(_))=>task.domain_id().to_string(),
   Err(e)=>return Err(e),
  };
  let mut frontend_start_req=FrontendStartRequest {
   domain:target_domain.into(),attributes:attributes.clone(),
   // Use the same request ID to dedupe StartWorkflowExecution calls
   request_id:request_id.into(),active_cluster_selection_policy,
  };
  frontend_start_req.attributes.domain=target_domain.into();
  let mut history_start_req=self.dependencies.create_history_start_workflow_request(&task.target_domain_id,frontend_start_req,self.now(),partition_config)?;
  history_start_req.parent_execution_info=Some(ParentExecutionInfo {domain_uuid:task.domain_id().into(),domain:domain_name,execution:execution_ref(task.workflow_id(),task.run_id()),initiated_id:task.initiated_id});
  let response:Arc<Mutex<Option<String>>>=Arc::new(Mutex::new(None));
  let deps=&self.dependencies;
  self.rpc_with_retry(||{
   let request=history_start_req.clone();let response=response.clone();
   async move{let run_id=deps.start_workflow_execution(request).await?;*response.lock().unwrap()=Some(run_id);Ok(())}
  }).await?;
  let run_id=response.lock().unwrap().clone().unwrap_or_default();
  Ok(run_id)
 }

 // -------------------------------------------------------------------------
 // parent close policy
 // -------------------------------------------------------------------------
 pub async fn process_parent_close_policy(&self,domain_id:&str,domain_name:&str,parent_execution:&WorkflowExecutionRef,child_infos:&BTreeMap<i64,ChildExecutionInfo>)->Result<()> {
  if child_infos.is_empty(){return Ok(());}
  let scope=self.base.shard.get_metrics_client().scope(metrics::TRANSFER_ACTIVE_TASK_CLOSE_EXECUTION_SCOPE,vec![]);
  if self.dependencies.enable_parent_close_policy_worker()&&child_infos.len()>=self.dependencies.parent_close_policy_threshold(domain_name) {
   let batch_size=self.dependencies.parent_close_policy_batch_size(domain_name);
   let mut executions:Vec<ParentClosePolicyRequestDetail>=Vec::with_capacity(child_infos.len().min(batch_size));
   let mut count=0usize;
   for child_info in child_infos.values() {
    count+=1;
    if child_info.parent_close_policy==ParentClosePolicy::Abandon{continue;}
    executions.push(ParentClosePolicyRequestDetail {domain_id:domain_id.into(),domain_name:domain_name.into(),workflow_id:child_info.started_workflow_id.clone(),run_id:child_info.started_run_id.clone(),policy:child_info.parent_close_policy});
    if executions.len()==batch_size {
     let batch=std::mem::replace(&mut executions,Vec::with_capacity(child_infos.len().saturating_sub(count).min(batch_size)));
     self.dependencies.send_parent_close_policy_request(ParentClosePolicyRequest {domain_name:domain_name.into(),executions:batch}).await?;
    }
   }
   if executions.is_empty(){return Ok(());}
   return self.dependencies.send_parent_close_policy_request(ParentClosePolicyRequest {domain_name:domain_name.into(),executions}).await;
  }
  for child_info in child_infos.values() {
   if let Err(err)=self.apply_parent_close_policy(domain_id,domain_name,parent_execution,child_info).await {
    match err {
     // expected error, no-op
     TaskError::EntityNotExists(_)|TaskError::WorkflowExecutionAlreadyCompleted(_)|TaskError::CancellationAlreadyRequested(_)=>{},
     err=>{scope.inc_counter("ParentClosePolicyProcessorFailures");return Err(err);},
    }
   }
   scope.inc_counter("ParentClosePolicyProcessorSuccess");
  }
  Ok(())
 }
 pub async fn apply_parent_close_policy(&self,domain_id:&str,domain_name:&str,parent_workflow_execution:&WorkflowExecutionRef,child_info:&ChildExecutionInfo)->Result<()> {
  match child_info.parent_close_policy {
   // noop
   ParentClosePolicy::Abandon=>Ok(()),
   ParentClosePolicy::Terminate=>self.dependencies.terminate_workflow_execution(HistoryTerminateRequest {
    domain_uuid:domain_id.into(),domain:domain_name.into(),workflow_execution:execution_ref(&child_info.started_workflow_id,""),reason:"by parent close policy".into(),
    identity:IDENTITY_HISTORY_SERVICE.into(),
    // Include StartedRunID as FirstExecutionRunID on the request to allow child to be terminated across runs.
    first_execution_run_id:child_info.started_run_id.clone(),external_workflow_execution:Some(parent_workflow_execution.clone()),child_workflow_only:true,
   }).await,
   ParentClosePolicy::RequestCancel=>self.dependencies.request_cancel_workflow_execution(HistoryRequestCancelRequest {
    domain_uuid:domain_id.into(),domain:domain_name.into(),workflow_execution:execution_ref(&child_info.started_workflow_id,""),identity:IDENTITY_HISTORY_SERVICE.into(),
    request_id:String::new(),
    // Include StartedRunID as FirstExecutionRunID on the request to allow child to be canceled across runs.
    first_execution_run_id:child_info.started_run_id.clone(),external_initiated_event_id:None,external_workflow_execution:Some(parent_workflow_execution.clone()),child_workflow_only:true,
   }).await,
   ParentClosePolicy::Unknown(v)=>Err(TaskError::InternalService(format!("unknown parent close policy: {v}"))),
  }
 }
}

/// `filterPendingChildExecutions`
pub fn filter_pending_child_executions(target_domain_ids:&HashSet<String>,children:BTreeMap<i64,ChildExecutionInfo>,dependencies:&dyn TransferActiveDependencies,parent_domain_entry:&DomainCacheEntry)->Result<BTreeMap<i64,ChildExecutionInfo>> {
 if target_domain_ids.is_empty(){return Ok(children);}
 let mut filtered_children=BTreeMap::new();
 for (initiated_id,child) in children {
  let domain_id=match dependencies.get_child_execution_domain_id(&child,parent_domain_entry) {
   Ok(id)=>id,
   // target domain deleted, ignore the child
   Err(TaskError::EntityNotExists(_))=>continue,
   Err(e)=>return Err(e),
  };
  if target_domain_ids.contains(&domain_id){filtered_children.insert(initiated_id,child);}
 }
 Ok(filtered_children)
}

/// `getWorkflowExecutionStatus`
pub fn get_workflow_execution_status(_mutable_state:&dyn ActiveMutableState,close_event:&CloseEvent)->WorkflowExecutionStatus {
 // Extract close status from the close event
 let close_status=match close_event.event_type {
  CloseEventType::Completed=>WorkflowExecutionStatus::Completed,
  CloseEventType::Failed=>WorkflowExecutionStatus::Failed,
  CloseEventType::Canceled=>WorkflowExecutionStatus::Canceled,
  CloseEventType::Terminated=>WorkflowExecutionStatus::Terminated,
  CloseEventType::TimedOut=>WorkflowExecutionStatus::TimedOut,
  CloseEventType::ContinuedAsNew=>{
   // For cron workflows that continue as new:
   // - If FailureReason is present and non-empty, the workflow failed
   // - If FailureReason is nil/empty, the workflow completed successfully
   if close_event.continued_as_new_attributes_present {
    let failure_reason=&close_event.continued_as_new_failure_reason;
    if !failure_reason.is_empty() {
     // Workflow failed - check if it's a timeout or generic failure
     let lower=failure_reason.to_lowercase();
     if lower.contains("timeout")||lower.contains("timed out"){WorkflowExecutionStatus::TimedOut}else{WorkflowExecutionStatus::Failed}
    } else {
     // No failure reason means successful completion
     WorkflowExecutionStatus::Completed
    }
   } else {WorkflowExecutionStatus::ContinuedAsNew}
  },
  // Unknown close event, default to completed
  CloseEventType::Other=>WorkflowExecutionStatus::Completed,
 };
 // Map close status to execution status
 close_status
}

#[async_trait]
impl Executor for TransferActiveTaskExecutor {
 async fn execute(&self,task:Arc<dyn Task>)->(ExecuteResponse,Result<()>) {
  self.dependencies.log_execute_history_task(self.base.shard.get_shard_id(),task.as_ref());
  let scope=get_or_create_domain_tagged_scope(&self.base.shard,get_transfer_task_metrics_scope(task.get_task_type(),true),task.get_domain_id(),&self.base.logger);
  let response=ExecuteResponse {scope,is_active_task:true};
  let info=task.get_info();
  // reset workflow is not subject to the taskDefaultTimeout (see processResetWorkflow)
  if let Some(t)=info.as_any().downcast_ref::<ResetWorkflowTask>(){return (response,self.process_reset_workflow(t).await);}
  let operation=async {
   if let Some(t)=info.as_any().downcast_ref::<ActivityTask>(){self.process_activity_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<DecisionTask>(){self.process_decision_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<CloseExecutionTask>(){self.process_close_execution(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<RecordWorkflowClosedTask>(){self.process_record_workflow_closed(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<RecordChildExecutionCompletedTask>(){self.process_record_child_execution_completed(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<CancelExecutionTask>(){self.process_cancel_execution(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<SignalExecutionTask>(){self.process_signal_execution(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<StartChildExecutionTask>(){self.process_start_child_execution(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<RecordWorkflowStartedTask>(){self.process_record_workflow_started(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<UpsertWorkflowSearchAttributesTask>(){self.process_upsert_workflow_search_attributes(t).await}
   else{Err(err_unknown_transfer_task())}
  };
  let result=tokio::time::timeout(TASK_DEFAULT_TIMEOUT,operation).await.unwrap_or(Err(TaskError::ContextDeadlineExceeded));
  (response,result)
 }
 // Empty func for now
 fn stop(&self){}
}
