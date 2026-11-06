//! Port of go/service/history/task/timer_standby_task_executor.go.
use std::{sync::Arc,time::Duration};
use async_trait::async_trait;
use super::{deps::*,interface::*,task_util::*,history_task::get_or_create_domain_tagged_scope,timer_task_executor_base::TimerTaskExecutorBase,timer_active_task_executor::TimerWorkflow,standby_task_util::HistoryResendInfo,transfer_task_executor_base::TASK_DEFAULT_TIMEOUT};
pub const ERR_UNEXPECTED_TASK:&str="unexpected task";
pub const ERR_UNKNOWN_TIMER_TASK:&str="unknown timer task";
/// TODO(link): replace by execution::MutableState standby operations.
#[async_trait]
pub trait StandbyTimerWorkflow:TimerWorkflow {
 fn last_write_version(&self)->Result<i64>;
 fn history_resend_info(&self)->Result<HistoryResendInfo>;
 fn create_next_activity_timer(&self)->Result<bool>;
 fn update_current_version(&self,version:i64,force:bool)->Result<()>;
 async fn update_workflow_execution_as_passive(&self,now:Time)->Result<()>;
}
/// TODO(link): replace by execution::Cache, ndc::HistoryResender, shard::Context and config::Config.
#[async_trait]
pub trait TimerStandbyDependencies:Send+Sync {
 async fn load_workflow(&self,task:&dyn PersistenceTask,event_id:i64)->Result<Option<Arc<dyn StandbyTimerWorkflow>>>;
 fn standby_cluster_delay(&self)->Duration;
 fn current_time(&self,cluster:&str)->Time;
 fn enable_timer_queue_v2(&self,shard_id:i32)->bool;
 fn resend_delay(&self)->Duration;
 fn discard_delay(&self)->Duration;
 fn delete_history_event_context_timeout(&self)->Duration;
 /// Go: dlqWriter (TaskDLQWriter).
 fn dlq_writer(&self)->Arc<dyn super::standby_task_util::TaskDLQWriter>;
 /// Go: config.HistoryTaskDLQMode.
 fn history_task_dlq_mode(&self)->StringPropertyFnWithDomainFilter;
 async fn send_single_workflow_history(&self,cluster:&str,task:&dyn PersistenceTask,last_event_id:i64,last_event_version:i64)->Result<()>;
}
pub struct TimerStandbyTaskExecutor {pub base:TimerTaskExecutorBase,pub cluster_name:String,pub dependencies:Arc<dyn TimerStandbyDependencies>}
pub fn new_timer_standby_task_executor(base:TimerTaskExecutorBase,cluster_name:String,dependencies:Arc<dyn TimerStandbyDependencies>)->Arc<dyn Executor>{Arc::new(TimerStandbyTaskExecutor {base,cluster_name,dependencies})}
#[derive(Clone,Copy)]
enum Action {User,Activity,Decision,Backoff,Workflow}
impl TimerStandbyTaskExecutor {
 async fn remote_cluster_name(&self,task:&dyn PersistenceTask)->Result<String> {
  if self.dependencies.enable_timer_queue_v2(self.base.shard.get_shard_id()) {super::standby_task_util::get_remote_cluster_name(&self.base.shard.get_cluster_metadata().get_current_cluster_name(),self.base.shard.get_active_cluster_manager().as_ref(),task).await}
  else{Ok(self.cluster_name.clone())}
 }
 pub fn get_standby_cluster_time(&self)->Time {self.dependencies.current_time(&self.cluster_name).add(self.dependencies.standby_cluster_delay())}
 pub async fn get_current_time(&self,task:&dyn PersistenceTask)->Result<Time>{let cluster=tokio::time::timeout(Duration::from_secs(1),self.remote_cluster_name(task)).await.map_err(|_|TaskError::ContextDeadlineExceeded)??;Ok(self.dependencies.current_time(&cluster))}
 async fn process_timer(&self,task:&dyn PersistenceTask,event_id:i64,action:Action)->Result<()> {
  let workflow=self.dependencies.load_workflow(task,event_id).await.map_err(|e|if e==TaskError::ContextDeadlineExceeded{TaskError::WorkflowBusy}else{e})?;
  let Some(w)=workflow else{return Ok(());};
  let result=async {
   if !w.running(){return Ok(None);}
   self.action(task,w.as_ref(),action).await
  }.await;
  let info=match result {Ok(info)=>info,Err(e)=>{w.release(if matches!(e,TaskError::Redispatch(_)){None}else{Some(e.clone())});return Err(e);}};
  w.release(None);
  let Some(info)=info else{return Ok(());};
  let now=match self.get_current_time(task).await {Ok(now)=>now,Err(e)=>{self.base.logger.error("getStandbyPostActionFn error getting current time, fallback to standbyTaskPostActionNoOp",&[tag::error(&e)]);return Err(TaskError::Redispatch("post action is historyResendInfo".into()));}};
  if now.before(task.get_visibility_timestamp().add(self.dependencies.resend_delay())){return Err(TaskError::Redispatch("post action is historyResendInfo".into()));}
  if now.before(task.get_visibility_timestamp().add(self.dependencies.discard_delay())){self.fetch_history_from_remote(task,Some(info)).await}else{
   let discard=super::standby_task_util::standby_task_post_action_write_to_dlq(self.dependencies.dlq_writer(),self.base.shard.clone(),self.dependencies.history_task_dlq_mode());
   let post:super::standby_task_util::PostActionInfo=Some(Arc::new(info));
   discard(task,post,&self.base.logger).await
  }
 }
 async fn action(&self,task:&dyn PersistenceTask,w:&dyn StandbyTimerWorkflow,action:Action)->Result<Option<HistoryResendInfo>> {
  match action {
   Action::User=>{
    if let Some(sequence)=w.user_timers().first(){if w.user_timer_info(sequence.event_id).is_none(){return Err(TaskError::Other(format!("failed to find in user timer event ID: {}",sequence.event_id)));}
  if !task.get_visibility_timestamp().before(sequence.expiry){return w.history_resend_info().map(Some);}}
    Ok(None)
   },
   Action::Activity=>{
    if let Some(sequence)=w.activity_timers().first(){if w.activity_info(sequence.event_id).is_none(){return Err(TaskError::Other(format!("failed to find in memory activity timer: {}",sequence.event_id)));}
  if !task.get_visibility_timestamp().before(sequence.expiry){return w.history_resend_info().map(Some);}}
    let timer=task.as_any().downcast_ref::<ActivityTimeoutTask>().ok_or_else(||TaskError::Other(ERR_UNEXPECTED_TASK.into()))?;
    let version=w.last_write_version()?;let mut changed=false;
    if timer.timeout_type==3 {if let Some(mut info)=w.activity_info(timer.event_id){if info.last_heartbeat_timeout_visibility_in_seconds<=timer.get_visibility_timestamp().unix(){info.timer_task_status&=!8;w.update_activity(info)?;changed=true;}}}
    let modified=w.create_next_activity_timer()?;changed|=modified;
    if changed{w.update_current_version(version,true)?;w.update_workflow_execution_as_passive(self.get_standby_cluster_time()).await?;}Ok(None)
   },
   Action::Decision=>{
    let timer=task.as_any().downcast_ref::<DecisionTimeoutTask>().ok_or_else(||TaskError::Other(ERR_UNEXPECTED_TASK.into()))?;
    let Some(decision)=w.decision_info(timer.event_id) else{return Ok(None);};
    if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),decision.version,task.get_version(),task)?{return Ok(None);}
    w.history_resend_info().map(Some)
   },
   Action::Backoff=>{if w.has_processed_or_pending_decision(){Ok(None)}else{w.history_resend_info().map(Some)}},
   Action::Workflow=>{if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),w.start_version()?,task.get_version(),task)?{Ok(None)}else{w.history_resend_info().map(Some)}}
  }
 }
 pub async fn execute_user_timer_timeout_task(&self,task:&UserTimerTask)->Result<()>{self.process_timer(task,task.event_id,Action::User).await}
 pub async fn execute_activity_timeout_task(&self,task:&ActivityTimeoutTask)->Result<()>{self.process_timer(task,task.event_id,Action::Activity).await}
 pub async fn execute_decision_timeout_task(&self,task:&DecisionTimeoutTask)->Result<()>{if task.timeout_type==1{return Ok(());}self.process_timer(task,task.event_id,Action::Decision).await}
 pub async fn execute_workflow_backoff_timer_task(&self,task:&WorkflowBackoffTimerTask)->Result<()>{self.process_timer(task,0,Action::Backoff).await}
 pub async fn execute_workflow_timeout_task(&self,task:&WorkflowTimeoutTask)->Result<()>{self.process_timer(task,0,Action::Workflow).await}
 pub async fn fetch_history_from_remote(&self,task:&dyn PersistenceTask,info:Option<HistoryResendInfo>)->Result<()> {
  let Some(info)=info else{return Ok(());};
  let scope=self.base.metrics_client.scope("HistoryRereplicationByTimerTaskScope",vec![]);scope.inc_counter("CadenceClientRequests");let start=std::time::Instant::now();
  let cluster=self.remote_cluster_name(task).await?;
  let result=if let (Some(id),Some(version))=(info.last_event_id,info.last_event_version){self.dependencies.send_single_workflow_history(&cluster,task,id,version).await}else{Err(TaskError::Other(format!("incomplete historyResendInfo: {info:?}")))};
  scope.record_timer("CadenceClientLatency",start.elapsed());scope.exponential_histogram("CadenceClientLatencyHistogram",start.elapsed());
  if let Err(e)=result{self.base.logger.error("Error re-replicating history from remote.",&[tag::error(&e)]);}else{self.base.logger.debug_tags("Successfully re-replicated history from remote.",&[]);}
  Err(TaskError::Redispatch("fetchHistoryFromRemote".into()))
 }
}
#[async_trait]
impl Executor for TimerStandbyTaskExecutor {
 async fn execute(&self,task:Arc<dyn Task>)->(ExecuteResponse,Result<()>) {
  let response=ExecuteResponse {scope:get_or_create_domain_tagged_scope(&self.base.shard,get_timer_task_metric_scope(task.get_task_type(),false),task.get_domain_id(),&self.base.logger),is_active_task:false};
  let info=task.get_info();let timeout=if info.as_any().is::<DeleteHistoryEventTask>(){self.dependencies.delete_history_event_context_timeout()}else{TASK_DEFAULT_TIMEOUT};
  let operation=async {
   if let Some(t)=info.as_any().downcast_ref::<UserTimerTask>(){self.execute_user_timer_timeout_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<ActivityTimeoutTask>(){self.execute_activity_timeout_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<DecisionTimeoutTask>(){self.execute_decision_timeout_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<WorkflowTimeoutTask>(){self.execute_workflow_timeout_task(t).await}
   else if info.as_any().is::<ActivityRetryTimerTask>(){self.base.logger.warn("Activity retry timer task on standby cluster will be dropped",&[]);response.scope.inc_counter("TaskDiscarded");Ok(())}
   else if let Some(t)=info.as_any().downcast_ref::<WorkflowBackoffTimerTask>(){self.execute_workflow_backoff_timer_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<DeleteHistoryEventTask>(){self.base.execute_delete_history_event_task(t).await}
   else{Err(TaskError::Other(ERR_UNKNOWN_TIMER_TASK.into()))}
  };
  let result=tokio::select!{_=self.base.cancel.cancelled()=>Err(TaskError::ContextCanceled),r=tokio::time::timeout(timeout,operation)=>r.unwrap_or(Err(TaskError::ContextDeadlineExceeded))};(response,result)
 }
 fn stop(&self){self.base.stop();}
}
