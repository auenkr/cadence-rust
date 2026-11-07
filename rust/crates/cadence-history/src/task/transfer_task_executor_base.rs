//! Port of go/service/history/task/transfer_task_executor_base.go.
use std::{collections::{BTreeMap,HashSet},sync::Arc,time::Duration};
use async_trait::async_trait;
use super::{deps::*,task_util::should_push_to_matching};
pub const TASK_DEFAULT_TIMEOUT:Duration=Duration::from_secs(3);
pub const TASK_GET_EXECUTION_CONTEXT_TIMEOUT:Duration=Duration::from_millis(500);
pub const TASK_RPC_CALL_TIMEOUT:Duration=Duration::from_secs(2);
pub const SECONDS_IN_DAY:i32=86400;
pub const DEFAULT_DOMAIN_NAME:&str="defaultDomainName";
#[derive(Clone,Debug,Default)]
pub struct StartEvent {pub timestamp:i64,pub first_decision_task_backoff_seconds:i32,pub headers:Option<BytesMap>}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum WorkflowExecutionStatus {Pending,Started,Completed,Failed,Canceled,Terminated,ContinuedAsNew,TimedOut}
/// TODO(link): replace by execution::MutableState decision accessors.
pub trait DecisionState:Send+Sync {
 fn has_pending_decision(&self)->bool;
 fn has_in_flight_decision(&self)->bool;
 fn has_processed_or_pending_decision(&self)->bool;
}
pub fn get_workflow_execution_timestamp(start:Option<&StartEvent>)->Time {
 start.filter(|e|e.first_decision_task_backoff_seconds!=0).map(|e|Time(e.timestamp+i64::from(e.first_decision_task_backoff_seconds)*1_000_000_000)).unwrap_or(Time::ZERO)
}
pub fn get_workflow_memo(memo:Option<&BytesMap>)->Option<BytesMap>{memo.cloned()}
pub fn get_workflow_headers(start:Option<&StartEvent>)->Option<BytesMap>{start.and_then(|e|e.headers.clone())}
pub fn copy_search_attributes(input:Option<&BytesMap>)->Option<BytesMap>{input.cloned()}
pub fn is_workflow_not_exist_error(error:&TaskError)->bool{matches!(error,TaskError::EntityNotExists(_))}
pub fn should_redact_context_header(key:&str,hidden:Option<&BTreeMap<String,serde_json::Value>>)->bool {
 match hidden.and_then(|h|h.get(key)) {Some(serde_json::Value::Bool(v))=>*v,Some(serde_json::Value::String(v))=>v.to_lowercase()=="true",Some(serde_json::Value::Number(v))=>v.as_i64().is_some_and(|v|v>0),_=>false}
}
pub fn append_context_header_to_search_attributes(mut attr:BytesMap,context:&BytesMap,allowed:&HashSet<String>,hidden:Option<&BTreeMap<String,serde_json::Value>>)->Result<BytesMap> {
 for (k,v) in context {
  let mut sanitized=String::new(); let mut separator=false;
  for ch in k.chars(){if ch.is_ascii_alphanumeric(){if separator&&!sanitized.is_empty(){sanitized.push('_');}sanitized.push(ch);separator=false;}else{separator=true;}}
  let sanitized=sanitized.trim_start_matches(|c:char|c.is_ascii_digit());
  // Go drops sanitize failures (multierr.Append result is intentionally ignored).
  if !sanitized.starts_with(|c:char|c.is_ascii_alphabetic()){continue;}
  let key=format!("Header_{sanitized}");
  if attr.contains_key(&key)||!allowed.contains(&key){continue;}
  let value=if should_redact_context_header(&key,hidden){"***redacted***".into()}else{String::from_utf8_lossy(v).into_owned()};
  attr.insert(key,serde_json::to_vec(&value).expect("string JSON"));
 }
 Ok(attr)
}
pub fn determine_execution_status(start:&StartEvent,state:&dyn DecisionState)->(WorkflowExecutionStatus,i64) {
 let status=if start.first_decision_task_backoff_seconds>0&&!state.has_pending_decision()&&!state.has_in_flight_decision()&&!state.has_processed_or_pending_decision(){WorkflowExecutionStatus::Pending}else{WorkflowExecutionStatus::Started};
 (status,start.timestamp+i64::from(start.first_decision_task_backoff_seconds)*1_000_000_000)
}
pub fn determine_execution_status_for_visibility(start:&StartEvent,state:&dyn DecisionState,advanced:bool)->(WorkflowExecutionStatus,i64) {
 let (mut status,time)=determine_execution_status(start,state);if !advanced&&status==WorkflowExecutionStatus::Pending{status=WorkflowExecutionStatus::Started;}(status,time)
}
#[derive(Clone,Debug)]
pub struct PushMatchingInfo {pub task_list:TaskList,pub schedule_to_start_timeout:i32,pub partition_config:BytesMap}
#[derive(Clone,Debug)]
pub struct AddMatchingTaskRequest {pub domain_uuid:String,pub source_domain_uuid:Option<String>,pub execution:WorkflowIdentifier,pub task_list:TaskList,pub schedule_id:i64,pub schedule_to_start_timeout_seconds:i32,pub partition_config:BytesMap}
#[derive(Clone,Debug)]
pub struct VisibilityRecord {
 pub domain_id:String,pub domain:String,pub workflow_id:String,pub run_id:String,pub workflow_type_name:String,
 pub start_timestamp:i64,pub execution_timestamp:i64,pub close_timestamp:i64,pub workflow_timeout:i64,pub task_id:i64,
 pub memo:Option<BytesMap>,pub task_list:String,pub is_cron:bool,pub cron_schedule:String,pub num_clusters:i16,
 pub cluster_attribute:Option<ClusterAttribute>,pub update_timestamp:i64,pub search_attributes:BytesMap,pub headers:BytesMap,
 pub shard_id:i64,pub execution_status:WorkflowExecutionStatus,pub scheduled_execution_timestamp:i64,
 pub close_status:i32,pub history_length:i64,pub retention_seconds:i64,
}
#[derive(Clone,Debug,Default)]
pub struct VisibilityDomain {pub name:String,pub retention_days:i32,pub sampled_for_longer_retention_enabled:bool,pub sampled_for_longer_retention:bool,pub archive_visibility:bool,pub visibility_uri:String,pub history_uri:String}
/// TODO(link): replace by matching::Client, persistence::VisibilityManager, archiver::Client and config::Config.
#[async_trait]
pub trait TransferDependencies:Send+Sync {
 fn standby_task_missing_events_discard_delay(&self)->Duration;
 /// Go: dlqWriter (TaskDLQWriter).
 fn dlq_writer(&self)->Arc<dyn super::standby_task_util::TaskDLQWriter>;
 /// Go: config.HistoryTaskDLQMode.
 fn history_task_dlq_mode(&self)->StringPropertyFnWithDomainFilter;
 async fn add_activity_task(&self,request:AddMatchingTaskRequest)->Result<()>;
 async fn add_decision_task(&self,request:AddMatchingTaskRequest)->Result<()>;
 fn visibility_domain(&self,domain_id:&str,workflow_id:&str)->Result<VisibilityDomain>;
 fn enable_context_header_in_visibility(&self,domain:&str)->bool;
 fn valid_search_attributes(&self)->HashSet<String>;
 fn hidden_value_keys(&self)->BTreeMap<String,serde_json::Value>;
 fn enable_record_workflow_execution_uninitialized(&self,domain:&str)->bool;
 async fn record_workflow_execution_uninitialized(&self,record:&VisibilityRecord)->Result<()>;
 async fn record_workflow_execution_started(&self,record:VisibilityRecord)->Result<()>;
 async fn upsert_workflow_execution(&self,record:VisibilityRecord)->Result<()>;
 async fn record_workflow_execution_closed(&self,record:VisibilityRecord)->Result<()>;
 fn visibility_archival_time_limit(&self)->Duration;
 async fn archive_visibility(&self,record:VisibilityRecord,domain:VisibilityDomain)->Result<()>;
}
pub struct TransferTaskExecutorBase {pub shard:Arc<dyn ShardContext>,pub dependencies:Arc<dyn TransferDependencies>,pub logger:Logger,pub throttle_retry:ThrottleRetry}
pub fn new_transfer_task_executor_base(shard:Arc<dyn ShardContext>,dependencies:Arc<dyn TransferDependencies>,logger:Logger)->TransferTaskExecutorBase {
 TransferTaskExecutorBase {shard,dependencies,logger,throttle_retry:ThrottleRetry::new(create_task_processing_retry_policy(),TaskError::is_service_transient)}
}
impl TransferTaskExecutorBase {
 async fn push(&self,task:&dyn PersistenceTask,info:&PushMatchingInfo,activity:bool,target:&str,schedule_id:i64)->Result<()> {
  if activity&&task.get_task_type()!=ptask_type::TRANSFER_TASK_TYPE_ACTIVITY_TASK{self.logger.fatal("Cannot process non activity task",&[tag::task_type(task.get_task_type())]);}
  if !activity&&task.get_task_type()!=ptask_type::TRANSFER_TASK_TYPE_DECISION_TASK{self.logger.fatal("Cannot process non decision task",&[tag::task_type(task.get_task_type())]);}
  if !should_push_to_matching(&self.shard,task).await? {
   // Go: standbyTaskPostActionNoOp / standbyTaskPostActionWriteToDLQ with the push info.
   let post:super::standby_task_util::PostActionInfo=Some(Arc::new(info.clone()));
   if self.shard.get_time_source().now().before(task.get_visibility_timestamp().add(self.dependencies.standby_task_missing_events_discard_delay())) {
    let f=super::standby_task_util::standby_task_post_action_no_op();return f(task,post,&self.logger).await;
   }
   let f=super::standby_task_util::standby_task_post_action_write_to_dlq(self.dependencies.dlq_writer(),self.shard.clone(),self.dependencies.history_task_dlq_mode());
   return f(task,post,&self.logger).await;
  }
  let request=AddMatchingTaskRequest {domain_uuid:if activity{target}else{task.get_domain_id()}.into(),source_domain_uuid:activity.then(||task.get_domain_id().into()),execution:WorkflowIdentifier {domain_id:task.get_domain_id().into(),workflow_id:task.get_workflow_id().into(),run_id:task.get_run_id().into()},task_list:info.task_list.clone(),schedule_id,schedule_to_start_timeout_seconds:info.schedule_to_start_timeout.min(MAX_TASK_TIMEOUT),partition_config:info.partition_config.clone()};
  if activity {self.dependencies.add_activity_task(request).await}else{self.dependencies.add_decision_task(request).await}
 }
 pub async fn push_activity(&self,task:&ActivityTask,info:&PushMatchingInfo)->Result<()> {
  tokio::time::timeout(TASK_RPC_CALL_TIMEOUT,self.push(task,info,true,&task.target_domain_id,task.schedule_id)).await.map_err(|_|TaskError::ContextDeadlineExceeded)?
 }
 pub async fn push_decision(&self,task:&DecisionTask,info:&PushMatchingInfo)->Result<()> {
  tokio::time::timeout(TASK_RPC_CALL_TIMEOUT,self.push(task,info,false,task.get_domain_id(),task.schedule_id)).await.map_err(|_|TaskError::ContextDeadlineExceeded)?
 }
 fn prepare_visibility(&self,record:&mut VisibilityRecord)->Result<VisibilityDomain> {
  let domain=match self.dependencies.visibility_domain(&record.domain_id,&record.workflow_id){Ok(d)=>d,Err(TaskError::EntityNotExists(_))=>VisibilityDomain {name:DEFAULT_DOMAIN_NAME.into(),..Default::default()},Err(e)=>return Err(e)};
  record.domain=domain.name.clone();record.shard_id=i64::from(self.shard.get_shard_id());
  if self.dependencies.enable_context_header_in_visibility(&record.domain) {
   match append_context_header_to_search_attributes(record.search_attributes.clone(),&record.headers,&self.dependencies.valid_search_attributes(),Some(&self.dependencies.hidden_value_keys())) {
    Ok(attrs)=>record.search_attributes=attrs,Err(e)=>self.logger.error("failed to add headers to search attributes",&[tag::error(&e)])
   }
  }
  Ok(domain)
 }
 pub async fn record_workflow_started(&self,mut record:VisibilityRecord)->Result<()> {
  let domain=self.prepare_visibility(&mut record)?;
  if domain.sampled_for_longer_retention_enabled&&!domain.sampled_for_longer_retention{return Ok(());}
  if self.dependencies.enable_record_workflow_execution_uninitialized(&record.domain) {if let Err(e)=self.dependencies.record_workflow_execution_uninitialized(&record).await{self.logger.error("Failed to record uninitialized workflow execution",&[tag::error(&e)]);}}
  self.dependencies.record_workflow_execution_started(record).await
 }
 pub async fn upsert_workflow_execution(&self,mut record:VisibilityRecord)->Result<()> {self.prepare_visibility(&mut record)?;self.dependencies.upsert_workflow_execution(record).await}
 pub async fn record_workflow_closed(&self,mut record:VisibilityRecord)->Result<()> {
  let domain=self.prepare_visibility(&mut record)?;record.retention_seconds=i64::from(domain.retention_days)*i64::from(SECONDS_IN_DAY);
  if !domain.sampled_for_longer_retention_enabled||domain.sampled_for_longer_retention{self.dependencies.record_workflow_execution_closed(record.clone()).await?;}
  if domain.archive_visibility{tokio::time::timeout(self.dependencies.visibility_archival_time_limit(),self.dependencies.archive_visibility(record,domain)).await.map_err(|_|TaskError::ContextDeadlineExceeded)??;}
  Ok(())
 }
}
