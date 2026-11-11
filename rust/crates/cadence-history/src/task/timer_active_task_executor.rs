//! Port of go/service/history/task/timer_active_task_executor.go.
use std::{collections::HashSet,sync::Arc,time::Duration};
use async_trait::async_trait;
use super::{deps::*,interface::*,timer_task_executor_base::TimerTaskExecutorBase,transfer_task_executor_base::{TASK_DEFAULT_TIMEOUT,AddMatchingTaskRequest},task_util::*,history_task::get_or_create_domain_tagged_scope};
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum TimerType {ScheduleToStart,ScheduleToClose,StartToClose,Heartbeat}
#[derive(Clone,Debug)]
pub struct TimerSequenceId {pub event_id:i64,pub attempt:i64,pub timer_type:TimerType,pub expiry:Time}
#[derive(Clone,Debug)]
pub struct UserTimerInfo {pub timer_id:String,pub started_id:i64}
#[derive(Clone,Debug)]
pub struct TimerActivityInfo {pub schedule_id:i64,pub started_id:i64,pub attempt:i64,pub version:i64,pub domain_id:String,pub task_list:String,pub schedule_to_start_timeout:i32,pub last_heartbeat_timeout_visibility_in_seconds:i64,pub timer_task_status:i32,pub details:Vec<u8>}
#[derive(Clone,Debug)]
pub struct TimerDecisionInfo {pub schedule_id:i64,pub started_id:i64,pub attempt:i64,pub version:i64}
/// TODO(link): replace by execution::{MutableState,Context,TimerSequence}; all methods are required dependency operations.
#[async_trait]
pub trait TimerWorkflow:Send+Sync {
 fn release(&self,error:Option<TaskError>);
 fn running(&self)->bool;
 fn domain_name(&self)->String;
 fn workflow_type(&self)->Option<String>;
 fn user_timers(&self)->Vec<TimerSequenceId>;
 fn activity_timers(&self)->Vec<TimerSequenceId>;
 fn user_timer_info(&self,event_id:i64)->Option<UserTimerInfo>;
 fn activity_info(&self,event_id:i64)->Option<TimerActivityInfo>;
 fn decision_info(&self,event_id:i64)->Option<TimerDecisionInfo>;
 async fn resurrected_timers(&self)->Result<HashSet<String>>;
 async fn resurrected_activities(&self)->Result<HashSet<i64>>;
 fn delete_user_timer(&self,id:&str)->Result<()>;
 fn delete_activity(&self,id:i64)->Result<()>;
 fn add_timer_fired_event(&self,id:&str)->Result<()>;
 fn update_activity(&self,info:TimerActivityInfo)->Result<()>;
 fn retry_activity(&self,info:&TimerActivityInfo,timer_type:TimerType)->Result<bool>;
 fn add_activity_task_timed_out_event(&self,info:&TimerActivityInfo,timer_type:TimerType)->Result<()>;
 fn sticky(&self)->bool;
 fn add_decision_task_timed_out_event(&self,info:&TimerDecisionInfo)->Result<()>;
 fn add_decision_task_schedule_to_start_timeout_event(&self,schedule_id:i64)->Result<()>;
 fn decision_attempt(&self)->i64;
 fn terminate_decision_attempts_exceeded(&self,message:&str)->Result<()>;
 fn has_processed_or_pending_decision(&self)->bool;
 fn decision_schedule_id(&self)->i64;
 fn add_upsert_workflow_search_attributes_task(&self)->Result<()>;
 fn schedule_decision(&self)->Result<()>;
 async fn update_workflow_execution_as_active(&self,now:Time)->Result<()>;
 fn start_version(&self)->Result<i64>;
 fn next_event_id(&self)->i64;
 fn retry_backoff_duration(&self,reason:&str)->Option<Duration>;
 async fn cron_backoff_duration(&self)->Result<Option<Duration>>;
 fn cancel_requested(&self)->bool;
 fn timeout_workflow(&self,first_event_id:i64)->Result<()>;
 async fn retry_workflow(&self,first_event_id:i64,backoff:Duration,cron:bool,reason:&str)->Result<()>;
 async fn update_workflow_execution_with_new_as_active(&self,now:Time)->Result<()>;
 async fn activity_scheduled_domain(&self,event_id:i64)->Result<String>;
 fn partition_config(&self)->BytesMap;
}
/// TODO(link): replace by execution::Cache and config::Config.
#[async_trait]
pub trait TimerActiveDependencies:Send+Sync {
 async fn load_workflow(&self,task:&dyn PersistenceTask,event_id:i64)->Result<Option<Arc<dyn TimerWorkflow>>>;
 fn resurrection_check_min_delay(&self,domain:&str)->Duration;
 fn activity_max_schedule_to_start_timeout_for_retry(&self,domain:&str)->Duration;
 fn decision_retry_max_attempts(&self,domain:&str)->i32;
 fn enforce_decision_task_attempts(&self,domain:&str)->bool;
 fn advanced_visibility_writing_enabled(&self)->bool;
 fn delete_history_event_context_timeout(&self)->Duration;
 async fn add_activity_task(&self,request:AddMatchingTaskRequest)->Result<()>;
}
pub struct TimerActiveTaskExecutor {pub base:TimerTaskExecutorBase,pub dependencies:Arc<dyn TimerActiveDependencies>}
pub fn new_timer_active_task_executor(base:TimerTaskExecutorBase,dependencies:Arc<dyn TimerActiveDependencies>)->Arc<dyn Executor>{Arc::new(TimerActiveTaskExecutor {base,dependencies})}
impl TimerActiveTaskExecutor {
 async fn load(&self,task:&dyn PersistenceTask,event_id:i64)->Result<Option<Arc<dyn TimerWorkflow>>>{self.dependencies.load_workflow(task,event_id).await.map_err(|e|if e==TaskError::ContextDeadlineExceeded{TaskError::WorkflowBusy}else{e})}
 pub async fn execute_user_timer_timeout_task(&self,task:&UserTimerTask)->Result<()> {
  let Some(w)=self.load(task,task.event_id).await? else{return Ok(());};
  let result=async {
   if !w.running(){return Ok(());}
   let now=self.base.shard.get_time_source().now();let mut changed=false;let mut resurrected=None;
   let threshold=self.dependencies.resurrection_check_min_delay(&w.domain_name());
   for sequence in w.user_timers(){let info=w.user_timer_info(sequence.event_id).ok_or_else(||TaskError::Other(format!("failed to find in user timer event ID: {}",sequence.event_id)))?;
    if now.before(sequence.expiry){break;}
    if now.sub(sequence.expiry)>=threshold||resurrected.is_some(){
     if resurrected.is_none(){resurrected=Some(w.resurrected_timers().await?);}
     if resurrected.as_ref().unwrap().contains(&info.timer_id){self.base.metrics_client.inc_counter("TimerQueueProcessorScope","TimerResurrectionCounter");self.base.logger.warn("Encounter resurrected timer, skip",&[]);w.delete_user_timer(&info.timer_id)?;changed=true;continue;}
    }
    w.add_timer_fired_event(&info.timer_id)?;changed=true;
   }
   if changed{self.update_workflow_execution(w.as_ref(),true).await?;}Ok(())
  }.await;w.release(result.clone().err());result
 }
 pub async fn execute_activity_timeout_task(&self,task:&ActivityTimeoutTask)->Result<()> {
  let Some(w)=self.load(task,task.event_id).await? else{return Ok(());};
  let result=async {
   let domain=self.base.shard.get_domain_cache().get_domain_name(task.get_domain_id())?;
   if !w.running(){return Ok(());}
   let workflow_type=w.workflow_type().ok_or_else(||TaskError::Other(format!("unable to find workflow type, task {task:?}")))?;
   let now=self.base.shard.get_time_source().now();let threshold=self.dependencies.resurrection_check_min_delay(&w.domain_name());let mut changed=false;let mut schedule=false;let mut resurrected=None;
   if task.timeout_type==3 {if let Some(mut info)=w.activity_info(task.event_id){if info.last_heartbeat_timeout_visibility_in_seconds<=task.get_visibility_timestamp().unix(){info.timer_task_status&=!8;w.update_activity(info)?;changed=true;}}}
   for sequence in w.activity_timers(){let Some(info)=w.activity_info(sequence.event_id) else{continue;};
   if sequence.attempt<info.attempt{continue;}
   
   if now.before(sequence.expiry){break;}
    if now.sub(sequence.expiry)>=threshold||resurrected.is_some(){if resurrected.is_none(){resurrected=Some(w.resurrected_activities().await?);}
    if resurrected.as_ref().unwrap().contains(&info.schedule_id){self.base.metrics_client.inc_counter("TimerQueueProcessorScope","ActivityResurrectionCounter");self.base.logger.warn("Encounter resurrected activity, skip",&[]);w.delete_activity(info.schedule_id)?;changed=true;continue;}}
    if sequence.timer_type==TimerType::ScheduleToStart&&info.schedule_to_start_timeout>=self.dependencies.activity_max_schedule_to_start_timeout_for_retry(&domain).as_secs() as i32 {self.base.metrics_client.inc_counter("TimerActiveTaskActivityTimeoutScope","ActivityLostCounter");self.base.logger.warn("Potentially activity task lost",&[]);}
    if w.retry_activity(&info,sequence.timer_type)?{changed=true;continue;}
    self.emit_timeout_metric_scope_with_domain_tag(task.get_domain_id(),"TimerActiveTaskActivityTimeoutScope",sequence.timer_type,vec![workflow_type_tag(&workflow_type)]);
    self.base.logger.info("Activity timed out",&[]);w.add_activity_task_timed_out_event(&info,sequence.timer_type)?;changed=true;schedule=true;
   }
   if changed{self.update_workflow_execution(w.as_ref(),schedule).await?;}Ok(())
  }.await;w.release(result.clone().err());result
 }
 pub async fn execute_decision_timeout_task(&self,task:&DecisionTimeoutTask)->Result<()> {
  let Some(w)=self.load(task,task.event_id).await? else{tracing::warn!(event_id=task.event_id,"restartsig-diag: decision timeout: load none");return Ok(());};let result=async {
   let domain=self.base.shard.get_domain_cache().get_domain_name(task.get_domain_id())?;if !w.running(){tracing::warn!("restartsig-diag: decision timeout: not running");return Ok(());}
   let workflow_type=w.workflow_type().ok_or_else(||TaskError::Other("unable to find workflow type".into()))?;
   let Some(decision)=w.decision_info(task.event_id) else{tracing::warn!(event_id=task.event_id,"restartsig-diag: decision timeout: no decision info");return Ok(());};
   if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),decision.version,task.get_version(),task)?||decision.attempt!=task.schedule_attempt{tracing::warn!("restartsig-diag: decision timeout: version/attempt mismatch");return Ok(());}
   let mut schedule=false;
   let timer_type=match task.timeout_type {0=>Some(TimerType::StartToClose),1=>Some(TimerType::ScheduleToStart),_=>None};
   if let Some(kind)=timer_type {
    if kind==TimerType::ScheduleToStart&&decision.started_id!=EMPTY_EVENT_ID{return Ok(());}
    self.emit_timeout_metric_scope_with_domain_tag(task.get_domain_id(),"TimerActiveTaskDecisionTimeoutScope",kind,vec![workflow_type_tag(&workflow_type),decision_type_tag(if w.sticky(){"sticky"}else{"normal"})]);
    if kind==TimerType::StartToClose {
     w.add_decision_task_timed_out_event(&decision)?;let max=self.dependencies.decision_retry_max_attempts(&domain);
     if self.dependencies.enforce_decision_task_attempts(&domain)&&max>0&&w.decision_attempt()>i64::from(max){let message="Decision attempt exceeds limit. Last decision attempt failed due to start-to-close timeout.";self.base.logger.error(message,&[]);self.base.metrics_client.inc_counter("TimerActiveTaskDecisionTimeoutScope","DecisionRetriesExceededCounter");w.terminate_decision_attempts_exceeded(message)?;}else{schedule=true;}
    }else{if !w.sticky(){self.base.logger.warn("Potential lost normal decision task",&[]);}w.add_decision_task_schedule_to_start_timeout_event(task.event_id)?;schedule=true;}
   }
   self.update_workflow_execution(w.as_ref(),schedule).await
  }.await;w.release(result.clone().err());result
 }
 pub async fn execute_workflow_backoff_timer_task(&self,task:&WorkflowBackoffTimerTask)->Result<()> {
  let Some(w)=self.load(task,0).await? else{return Ok(());};let result=async {
   if !w.running(){return Ok(());}
   self.base.metrics_client.inc_counter("TimerActiveTaskWorkflowBackoffTimerScope",if task.timeout_type==0{"WorkflowRetryBackoffTimerCount"}else{"WorkflowCronBackoffTimerCount"});
   if w.has_processed_or_pending_decision(){return Ok(());}
   if task.timeout_type==1&&w.decision_schedule_id()==EMPTY_EVENT_ID&&self.dependencies.advanced_visibility_writing_enabled(){w.add_upsert_workflow_search_attributes_task()?;}
   self.update_workflow_execution(w.as_ref(),true).await
  }.await;w.release(result.clone().err());result
 }
 pub async fn execute_activity_retry_timer_task(&self,task:&ActivityRetryTimerTask)->Result<()> {
  let Some(w)=self.load(task,task.event_id).await? else{return Ok(());};let result=async {
   if !w.running(){return Ok(());}
   let Some(info)=w.activity_info(task.event_id) else{return Ok(());};if task.attempt<info.attempt||info.started_id!=EMPTY_EVENT_ID{return Ok(());}
   if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),info.version,task.get_version(),task)?{return Ok(());}
   let mut target=task.get_domain_id().to_owned();
   if !info.domain_id.is_empty(){target=info.domain_id.clone();}else{let name=w.activity_scheduled_domain(task.event_id).await?;if !name.is_empty(){target=self.base.shard.get_domain_cache().get_domain(&name).map_err(|_|TaskError::Other("unable to re-schedule activity across domain.".into()))?.info.id.clone();}}
   let request=AddMatchingTaskRequest {domain_uuid:target,source_domain_uuid:Some(task.get_domain_id().into()),execution:get_workflow_execution(task),task_list:TaskList {name:info.task_list,kind:TaskListKind::Normal},schedule_id:task.event_id,schedule_to_start_timeout_seconds:info.schedule_to_start_timeout,partition_config:w.partition_config()};
   w.release(None);if !should_push_to_matching(&self.base.shard,task).await?{return Ok(());}self.dependencies.add_activity_task(request).await
  }.await;w.release(result.clone().err());result
 }
 pub async fn execute_workflow_timeout_task(&self,task:&WorkflowTimeoutTask)->Result<()> {
  let Some(w)=self.load(task,0).await? else{return Ok(());};let result=async {
   if !w.running(){return Ok(());}
   if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),w.start_version()?,task.get_version(),task)?{return Ok(());}
   let first=w.next_event_id();let reason="START_TO_CLOSE";let mut backoff=w.retry_backoff_duration(reason);let mut cron=false;
   if backoff.is_none(){backoff=w.cron_backoff_duration().await?;cron=true;}
   if w.cancel_requested()||backoff.is_none(){w.timeout_workflow(first)?;return self.update_workflow_execution(w.as_ref(),false).await;}
   w.retry_workflow(first,backoff.unwrap(),cron,reason).await?;w.update_workflow_execution_with_new_as_active(self.base.shard.get_time_source().now()).await
  }.await;w.release(result.clone().err());result
 }
 pub async fn update_workflow_execution(&self,w:&dyn TimerWorkflow,schedule:bool)->Result<()> {if schedule{w.schedule_decision()?;}w.update_workflow_execution_as_active(self.base.shard.get_time_source().now()).await}
 pub fn emit_timeout_metric_scope_with_domain_tag(&self,domain_id:&str,scope:ScopeIdx,kind:TimerType,mut tags:Vec<Tag>) {
  let Ok(name)=self.base.shard.get_domain_cache().get_domain_name(domain_id) else{return;};tags.push(domain_tag(&name));
  self.base.metrics_client.scope(scope,tags).inc_counter(match kind{TimerType::ScheduleToStart=>"ScheduleToStartTimeoutCounter",TimerType::ScheduleToClose=>"ScheduleToCloseTimeoutCounter",TimerType::StartToClose=>"StartToCloseTimeoutCounter",TimerType::Heartbeat=>"HeartbeatTimeoutCounter"});
 }
}
#[async_trait]
impl Executor for TimerActiveTaskExecutor {
 async fn execute(&self,task:Arc<dyn Task>)->(ExecuteResponse,Result<()>) {
  let response=ExecuteResponse {scope:get_or_create_domain_tagged_scope(&self.base.shard,get_timer_task_metric_scope(task.get_task_type(),true),task.get_domain_id(),&self.base.logger),is_active_task:true};
  let info=task.get_info();let timeout=if info.as_any().is::<DeleteHistoryEventTask>(){self.dependencies.delete_history_event_context_timeout()}else{TASK_DEFAULT_TIMEOUT};
  let operation=async {
   if let Some(t)=info.as_any().downcast_ref::<UserTimerTask>(){self.execute_user_timer_timeout_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<ActivityTimeoutTask>(){self.execute_activity_timeout_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<DecisionTimeoutTask>(){self.execute_decision_timeout_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<WorkflowTimeoutTask>(){self.execute_workflow_timeout_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<ActivityRetryTimerTask>(){self.execute_activity_retry_timer_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<WorkflowBackoffTimerTask>(){self.execute_workflow_backoff_timer_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<DeleteHistoryEventTask>(){self.base.execute_delete_history_event_task(t).await}
   else{Err(TaskError::Other("unknown timer task".into()))}
  };
  let result=tokio::select!{_ = self.base.cancel.cancelled()=>Err(TaskError::ContextCanceled),r=tokio::time::timeout(timeout,operation)=>r.unwrap_or(Err(TaskError::ContextDeadlineExceeded))};(response,result)
 }
 fn stop(&self){self.base.stop();}
}
