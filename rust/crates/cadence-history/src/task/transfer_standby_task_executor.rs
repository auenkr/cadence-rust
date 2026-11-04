//! Port of go/service/history/task/transfer_standby_task_executor.go.
use std::{sync::Arc,time::Duration};
use async_trait::async_trait;
use super::{deps::*,interface::*,task_util::*,history_task::get_or_create_domain_tagged_scope,standby_task_util::HistoryResendInfo,transfer_task_executor_base::*};
#[derive(Clone,Debug)]
pub struct TransferActivityInfo {pub version:i64,pub started_id:i64,pub task_list:TaskList,pub schedule_to_start_timeout:i32}
#[derive(Clone,Debug)]
pub struct TransferDecisionInfo {pub version:i64,pub started_id:i64}
#[derive(Clone,Debug)]
pub struct TransferPendingInfo {pub version:i64,pub started_id:i64}
/// TODO(link): replace by execution::MutableState and execution::Context.
#[async_trait]
pub trait TransferWorkflow:DecisionState {
 fn release(&self,error:Option<TaskError>);
 fn running(&self)->bool;
 fn activity_info(&self,id:i64)->Option<TransferActivityInfo>;
 fn decision_info(&self,id:i64)->Option<TransferDecisionInfo>;
 fn request_cancel_info(&self,id:i64)->Option<TransferPendingInfo>;
 fn signal_info(&self,id:i64)->Option<TransferPendingInfo>;
 fn child_execution_info(&self,id:i64)->Option<TransferPendingInfo>;
 fn task_list(&self)->TaskList;
 fn workflow_timeout(&self)->i32;
 fn sticky_schedule_to_start_timeout(&self)->i32;
 fn partition_config(&self)->BytesMap;
 fn history_resend_info(&self)->Result<HistoryResendInfo>;
 fn last_write_version(&self)->Result<i64>;
 fn start_version(&self)->Result<i64>;
 async fn start_event(&self)->Result<StartEvent>;
 async fn visibility_record(&self,task:&dyn PersistenceTask,now:Time,closed:bool)->Result<VisibilityRecord>;
}
/// TODO(link): replace by execution::Cache, shard::Context, ndc::HistoryResender and config::Config.
#[async_trait]
pub trait TransferStandbyDependencies:Send+Sync {
 async fn load_workflow(&self,task:&dyn PersistenceTask,event_id:i64)->Result<Option<Arc<dyn TransferWorkflow>>>;
 fn enable_transfer_queue_v2(&self,shard_id:i32)->bool;
 fn current_time(&self,cluster:&str)->Time;
 fn resend_delay(&self)->Duration;
 fn discard_delay(&self)->Duration;
 fn advanced_visibility_writing_enabled(&self)->bool;
 async fn send_single_workflow_history(&self,cluster:&str,task:&dyn PersistenceTask,id:i64,version:i64)->Result<()>;
 async fn write_to_dlq(&self,task:&dyn PersistenceTask,info:HistoryResendInfo)->Result<()>;
}
pub struct TransferStandbyTaskExecutor {pub base:TransferTaskExecutorBase,pub cluster_name:String,pub dependencies:Arc<dyn TransferStandbyDependencies>}
pub fn new_transfer_standby_task_executor(base:TransferTaskExecutorBase,cluster_name:String,dependencies:Arc<dyn TransferStandbyDependencies>)->Arc<dyn Executor>{Arc::new(TransferStandbyTaskExecutor {base,cluster_name,dependencies})}
enum Action {Activity,Decision,Close,Cancel,Signal,Child,Start,Upsert}
enum PostAction {Activity(PushMatchingInfo),Decision(PushMatchingInfo),Resend(HistoryResendInfo)}
impl TransferStandbyTaskExecutor {
 async fn remote_cluster_name(&self,task:&dyn PersistenceTask)->Result<String>{if self.dependencies.enable_transfer_queue_v2(self.base.shard.get_shard_id()){super::standby_task_util::get_remote_cluster_name(&self.base.shard.get_cluster_metadata().get_current_cluster_name(),self.base.shard.get_active_cluster_manager().as_ref(),task).await}else{Ok(self.cluster_name.clone())}}
 pub async fn get_current_time(&self,task:&dyn PersistenceTask)->Result<Time>{let name=tokio::time::timeout(Duration::from_secs(1),self.remote_cluster_name(task)).await.map_err(|_|TaskError::ContextDeadlineExceeded)??;Ok(self.dependencies.current_time(&name))}
 async fn process_transfer(&self,task:&dyn PersistenceTask,event_id:i64,closed:bool,action:Action)->Result<()> {
  let Some(w)=self.dependencies.load_workflow(task,event_id).await.map_err(|e|if e==TaskError::ContextDeadlineExceeded{TaskError::WorkflowBusy}else{e})? else{return Ok(());};
  let result=if !w.running()&&!closed {Ok(None)}else{self.action(task,w.as_ref(),action).await};
  let info=match result{Ok(info)=>info,Err(e)=>{w.release(Some(e.clone()));return Err(e);}};w.release(None);
  let Some(info)=info else{return Ok(());};
  let now=match self.get_current_time(task).await{Ok(now)=>now,Err(_)=>return Err(TaskError::Redispatch("post action is pending transfer".into()))};
  if now.before(task.get_visibility_timestamp().add(self.dependencies.resend_delay())){return Err(TaskError::Redispatch("post action is pending transfer".into()));}
  match info {
   PostAction::Activity(info)=>self.push_activity(task,Some(info)).await,
   PostAction::Decision(info)=>self.push_decision(task,Some(info)).await,
   PostAction::Resend(info)=>if now.before(task.get_visibility_timestamp().add(self.dependencies.discard_delay())){self.fetch_history_from_remote(task,Some(info)).await}else{self.dependencies.write_to_dlq(task,info).await},
  }
 }
 async fn action(&self,task:&dyn PersistenceTask,w:&dyn TransferWorkflow,action:Action)->Result<Option<PostAction>> {
  match action {
   Action::Activity=>{let t=task.as_any().downcast_ref::<ActivityTask>().unwrap();let Some(info)=w.activity_info(t.schedule_id) else{return Ok(None);};if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),info.version,task.get_version(),task)?||info.started_id!=EMPTY_EVENT_ID{return Ok(None);}let mut list=info.task_list;if list.name.is_empty(){list.name=t.task_list.clone();}Ok(Some(PostAction::Activity(PushMatchingInfo {task_list:list,schedule_to_start_timeout:info.schedule_to_start_timeout,partition_config:w.partition_config()})))},
   Action::Decision=>{let t=task.as_any().downcast_ref::<DecisionTask>().unwrap();let Some(info)=w.decision_info(t.schedule_id) else{return Ok(None);};let list=w.task_list();let timeout=if list.name!=t.task_list{w.sticky_schedule_to_start_timeout()}else{w.workflow_timeout().min(MAX_TASK_TIMEOUT)};if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),info.version,task.get_version(),task)?||info.started_id!=EMPTY_EVENT_ID{return Ok(None);}Ok(Some(PostAction::Decision(PushMatchingInfo {task_list:list,schedule_to_start_timeout:timeout,partition_config:w.partition_config()})))},
   Action::Close=>{if w.running(){return Ok(None);}let record=w.visibility_record(task,self.base.shard.get_time_source().now(),true).await?;if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),w.last_write_version()?,task.get_version(),task)?{return Ok(None);}self.base.record_workflow_closed(record).await?;Ok(None)},
   Action::Cancel|Action::Signal|Action::Child=>{
    let (info,child)=match action{Action::Cancel=>{let t=task.as_any().downcast_ref::<CancelExecutionTask>().unwrap();(w.request_cancel_info(t.initiated_id),false)},Action::Signal=>{let t=task.as_any().downcast_ref::<SignalExecutionTask>().unwrap();(w.signal_info(t.initiated_id),false)},_=>{let t=task.as_any().downcast_ref::<StartChildExecutionTask>().unwrap();(w.child_execution_info(t.initiated_id),true)}};
    let Some(info)=info else{return Ok(None);};if !verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),info.version,task.get_version(),task)?||child&&info.started_id!=EMPTY_EVENT_ID{return Ok(None);}w.history_resend_info().map(|info|Some(PostAction::Resend(info)))
   },
   Action::Start=>{self.process_record_workflow_started_or_upsert_helper(task,w,true).await?;Ok(None)},
   Action::Upsert=>{self.process_record_workflow_started_or_upsert_helper(task,w,false).await?;Ok(None)},
  }
 }
 pub async fn process_activity_task(&self,t:&ActivityTask)->Result<()>{self.process_transfer(t,t.schedule_id,false,Action::Activity).await}
 pub async fn process_decision_task(&self,t:&DecisionTask)->Result<()>{self.process_transfer(t,t.schedule_id,false,Action::Decision).await}
 pub async fn process_close_execution(&self,t:&dyn PersistenceTask)->Result<()>{self.process_transfer(t,0,true,Action::Close).await}
 pub async fn process_cancel_execution(&self,t:&CancelExecutionTask)->Result<()>{self.process_transfer(t,t.initiated_id,false,Action::Cancel).await}
 pub async fn process_signal_execution(&self,t:&SignalExecutionTask)->Result<()>{self.process_transfer(t,t.initiated_id,false,Action::Signal).await}
 pub async fn process_start_child_execution(&self,t:&StartChildExecutionTask)->Result<()>{self.process_transfer(t,t.initiated_id,false,Action::Child).await}
 pub async fn process_record_workflow_started(&self,t:&RecordWorkflowStartedTask)->Result<()>{self.process_transfer(t,0,false,Action::Start).await}
 pub async fn process_upsert_workflow_search_attributes(&self,t:&UpsertWorkflowSearchAttributesTask)->Result<()>{self.process_transfer(t,0,false,Action::Upsert).await}
 pub async fn process_record_workflow_started_or_upsert_helper(&self,task:&dyn PersistenceTask,w:&dyn TransferWorkflow,start:bool)->Result<()> {
  if start&&!verify_task_version(&self.base.shard,&self.base.logger,task.get_domain_id(),w.start_version()?,task.get_version(),task)?{return Ok(());}
  let mut record=w.visibility_record(task,self.base.shard.get_time_source().now(),false).await?;let event=w.start_event().await?;
  let (status,scheduled)=determine_execution_status_for_visibility(&event,w,self.dependencies.advanced_visibility_writing_enabled());record.execution_status=status;record.scheduled_execution_timestamp=scheduled;
  if start{get_or_create_domain_tagged_scope(&self.base.shard,"TransferStandbyTaskRecordWorkflowStartedScope",task.get_domain_id(),&self.base.logger).inc_counter("WorkflowStartedCount");self.base.record_workflow_started(record).await}else{self.base.upsert_workflow_execution(record).await}
 }
 pub async fn push_activity(&self,task:&dyn PersistenceTask,info:Option<PushMatchingInfo>)->Result<()>{if let Some(info)=info{self.base.push_activity(task.as_any().downcast_ref::<ActivityTask>().unwrap(),&info).await}else{Ok(())}}
 pub async fn push_decision(&self,task:&dyn PersistenceTask,info:Option<PushMatchingInfo>)->Result<()>{if let Some(info)=info{self.base.push_decision(task.as_any().downcast_ref::<DecisionTask>().unwrap(),&info).await}else{Ok(())}}
 pub async fn fetch_history_from_remote(&self,task:&dyn PersistenceTask,info:Option<HistoryResendInfo>)->Result<()> {
  let Some(info)=info else{return Ok(());};let scope=self.base.shard.get_metrics_client().scope("HistoryRereplicationByTransferTaskScope",vec![]);scope.inc_counter("CadenceClientRequests");let start=std::time::Instant::now();
  let name=self.remote_cluster_name(task).await?;let result=if let (Some(id),Some(version))=(info.last_event_id,info.last_event_version){self.dependencies.send_single_workflow_history(&name,task,id,version).await}else{Err(TaskError::Other(format!("incomplete historyResendInfo: {info:?}")))};
  scope.record_timer("CadenceClientLatency",start.elapsed());scope.exponential_histogram("CadenceClientLatencyHistogram",start.elapsed());if let Err(e)=result{self.base.logger.error("Error re-replicating history from remote.",&[tag::error(&e)]);}Err(TaskError::Redispatch("fetchHistoryFromRemote".into()))
 }
}
#[async_trait]
impl Executor for TransferStandbyTaskExecutor {
 async fn execute(&self,task:Arc<dyn Task>)->(ExecuteResponse,Result<()>) {
  let response=ExecuteResponse {scope:get_or_create_domain_tagged_scope(&self.base.shard,get_transfer_task_metrics_scope(task.get_task_type(),false),task.get_domain_id(),&self.base.logger),is_active_task:false};let info=task.get_info();
  let operation=async {
   if let Some(t)=info.as_any().downcast_ref::<ActivityTask>(){self.process_activity_task(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<DecisionTask>(){self.process_decision_task(t).await}
   else if info.as_any().is::<CloseExecutionTask>()||info.as_any().is::<RecordWorkflowClosedTask>(){self.process_close_execution(info.as_ref()).await}
   else if let Some(t)=info.as_any().downcast_ref::<CancelExecutionTask>(){self.process_cancel_execution(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<SignalExecutionTask>(){self.process_signal_execution(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<StartChildExecutionTask>(){self.process_start_child_execution(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<RecordWorkflowStartedTask>(){self.process_record_workflow_started(t).await}
   else if let Some(t)=info.as_any().downcast_ref::<UpsertWorkflowSearchAttributesTask>(){self.process_upsert_workflow_search_attributes(t).await}
   else if info.as_any().is::<RecordChildExecutionCompletedTask>()||info.as_any().is::<ResetWorkflowTask>(){Ok(())}
   else{Err(TaskError::Other("unknown transfer task".into()))}
  };let result=tokio::time::timeout(TASK_DEFAULT_TIMEOUT,operation).await.unwrap_or(Err(TaskError::ContextDeadlineExceeded));(response,result)
 }
 fn stop(&self){}
}
