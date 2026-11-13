//! Port of go/service/history/task/timer_task_executor_base.go.
use std::sync::Arc;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use super::{deps::*, task_util::verify_task_version};

/// TODO(link): replace by execution::MutableState and execution::Context.
#[async_trait]
pub trait CleanupWorkflow: Send + Sync {
 fn is_workflow_execution_running(&self)->bool;
 fn get_last_write_version(&self)->Result<i64>;
 fn get_current_branch_token(&self)->Result<Vec<u8>>;
 fn get_next_event_id(&self)->i64;
 async fn load_execution_stats(&self)->Result<i64>;
 fn clear(&self);
 fn release(&self,error:Option<TaskError>);
}
#[derive(Clone,Debug)]
pub struct ArchiveRequest {
 pub domain_id:String,pub workflow_id:String,pub run_id:String,pub domain_name:String,pub shard_id:i32,
 pub uri:String,pub next_event_id:i64,pub branch_token:Vec<u8>,pub close_failover_version:i64,
 pub caller_service:&'static str,pub targets:Vec<&'static str>,pub attempt_archive_inline:bool,
}
/// TODO(link): replace by execution::Cache, archiver::Client and persistence managers.
#[async_trait]
pub trait CleanupDependencies: Send + Sync {
 async fn get_workflow(&self,task:&dyn PersistenceTask)->Result<Option<Arc<dyn CleanupWorkflow>>>;
 fn history_archival_enabled(&self,domain_id:&str)->Result<bool>;
 fn history_archival_uri(&self,domain_id:&str)->Result<String>;
 fn archival_size_limit(&self)->i64;
 fn archival_time_limit(&self)->std::time::Duration;
 fn enable_workflow_timer_task_cleanup(&self)->bool;
 async fn archive(&self,request:ArchiveRequest)->Result<bool>;
 async fn delete_workflow_execution(&self,task:&dyn PersistenceTask,domain_name:&str,shard_id:i32)->Result<()>;
 async fn delete_current_workflow_execution(&self,task:&dyn PersistenceTask,domain_name:&str,shard_id:i32)->Result<()>;
 async fn delete_active_cluster_selection_policy(&self,task:&dyn PersistenceTask,shard_id:i32)->Result<()>;
 async fn delete_history_branch(&self,branch_token:Vec<u8>,domain_name:&str,shard_id:i32)->Result<()>;
 async fn delete_workflow_visibility(&self,task:&dyn PersistenceTask,domain_name:&str)->Result<()>;
 async fn fetch_workflow_timer_tasks_for_cleanup(&self,task:&dyn PersistenceTask,shard_id:i32)->Result<Vec<HistoryTaskKey>>;
 async fn complete_history_task(&self,keys:Vec<HistoryTaskKey>,category:HistoryTaskCategory,shard_id:i32)->Result<()>;
}
pub struct TimerTaskExecutorBase {
 pub shard:Arc<dyn ShardContext>,pub dependencies:Arc<dyn CleanupDependencies>,pub logger:Logger,
 pub metrics_client:Arc<dyn MetricsClient>,pub cancel:CancellationToken,pub throttle_retry:ThrottleRetry,
}
pub fn new_timer_task_executor_base(shard:Arc<dyn ShardContext>,dependencies:Arc<dyn CleanupDependencies>,logger:Logger,metrics_client:Arc<dyn MetricsClient>)->TimerTaskExecutorBase {
 TimerTaskExecutorBase {shard,dependencies,logger,metrics_client,cancel:CancellationToken::new(),throttle_retry:ThrottleRetry::new(create_task_processing_retry_policy(),TaskError::is_persistence_transient)}
}
impl TimerTaskExecutorBase {
 fn counter(&self,name:MetricIdx){self.metrics_client.inc_counter("HistoryProcessDeleteHistoryEventScope",name);}
 pub async fn execute_delete_history_event_task(&self,task:&dyn PersistenceTask)->Result<()> {
  let Some(workflow)=self.dependencies.get_workflow(task).await? else {self.logger.debug_tags("could not load mutable state while attempting to clean up workflow",&[]);self.counter("TimerProcessingDeletionTimerNoopDueToMutableStateNotLoading");return Ok(());};
  let result=self.execute_cleanup(task,workflow.as_ref()).await;workflow.release(result.clone().err());result
 }
 async fn execute_cleanup(&self,task:&dyn PersistenceTask,workflow:&dyn CleanupWorkflow)->Result<()> {
  if workflow.is_workflow_execution_running(){self.logger.warn("could not clean up workflow, it was running",&[]);self.counter("TimerProcessingDeletionTimerNoopDueToMutableStateNotLoading");return Ok(());}
  if !verify_task_version(&self.shard,&self.logger,task.get_domain_id(),workflow.get_last_write_version()?,task.get_version(),task)? {return Ok(());}
  if self.dependencies.history_archival_enabled(task.get_domain_id())? {self.counter("WorkflowCleanupArchiveCount");self.archive_workflow(task,workflow).await}
  else {self.counter("WorkflowCleanupDeleteCount");self.delete_workflow(task,workflow).await}
 }
 pub async fn delete_workflow(&self,task:&dyn PersistenceTask,workflow:&dyn CleanupWorkflow)->Result<()> {
  tracing::warn!(domain_id=task.get_domain_id(),workflow_id=task.get_workflow_id(),run_id=task.get_run_id(),task_version=task.get_version(),"retention cleanup: deleting workflow execution");
  self.delete_workflow_history(task,workflow).await?;self.delete_workflow_visibility(task).await?;
  self.delete_current_workflow_execution(task).await?;self.delete_active_cluster_selection_policy(task).await?;
  self.cleanup_workflow_timer_tasks(task).await;self.delete_workflow_execution(task).await?;workflow.clear();Ok(())
 }
 pub async fn archive_workflow(&self,task:&dyn PersistenceTask,workflow:&dyn CleanupWorkflow)->Result<()> {
  let request=ArchiveRequest {domain_id:task.get_domain_id().into(),workflow_id:task.get_workflow_id().into(),run_id:task.get_run_id().into(),
   domain_name:self.shard.get_domain_cache().get_domain_name(task.get_domain_id())?,shard_id:self.shard.get_shard_id(),
   uri:self.dependencies.history_archival_uri(task.get_domain_id())?,next_event_id:workflow.get_next_event_id(),branch_token:workflow.get_current_branch_token()?,close_failover_version:workflow.get_last_write_version()?,
   caller_service:"history",targets:vec!["history"],attempt_archive_inline:workflow.load_execution_stats().await.is_ok_and(|size|size<self.dependencies.archival_size_limit())};
  let inline=tokio::time::timeout(self.dependencies.archival_time_limit(),self.dependencies.archive(request)).await.map_err(|_|TaskError::ContextDeadlineExceeded)??;
  if inline {self.counter("WorkflowCleanupDeleteHistoryInlineCount");self.delete_workflow_history(task,workflow).await?;}
  self.delete_workflow_visibility(task).await?;self.delete_active_cluster_selection_policy(task).await?;
  self.delete_current_workflow_execution(task).await?;self.cleanup_workflow_timer_tasks(task).await;
  self.delete_workflow_execution(task).await?;workflow.clear();Ok(())
 }
 pub async fn delete_workflow_execution(&self,task:&dyn PersistenceTask)->Result<()> {
  let name=self.shard.get_domain_cache().get_domain_name(task.get_domain_id())?;
  self.throttle_retry.do_op(||self.dependencies.delete_workflow_execution(task,&name,self.shard.get_shard_id())).await
 }
 pub async fn delete_current_workflow_execution(&self,task:&dyn PersistenceTask)->Result<()> {
  let name=self.shard.get_domain_cache().get_domain_name(task.get_domain_id())?;
  self.throttle_retry.do_op(||self.dependencies.delete_current_workflow_execution(task,&name,self.shard.get_shard_id())).await
 }
 pub async fn delete_active_cluster_selection_policy(&self,task:&dyn PersistenceTask)->Result<()> {
  self.throttle_retry.do_op(||self.dependencies.delete_active_cluster_selection_policy(task,self.shard.get_shard_id())).await
 }
 pub async fn delete_workflow_history(&self,task:&dyn PersistenceTask,workflow:&dyn CleanupWorkflow)->Result<()> {
  self.throttle_retry.do_op(||async {let token=workflow.get_current_branch_token()?;let name=self.shard.get_domain_cache().get_domain_name(task.get_domain_id())?;self.dependencies.delete_history_branch(token,&name,self.shard.get_shard_id()).await}).await
 }
 pub async fn delete_workflow_visibility(&self,task:&dyn PersistenceTask)->Result<()> {
  let name=self.shard.get_domain_cache().get_domain_name(task.get_domain_id())?;
  self.throttle_retry.do_op(||self.dependencies.delete_workflow_visibility(task,&name)).await
 }
 pub async fn cleanup_workflow_timer_tasks(&self,task:&dyn PersistenceTask) {
  if !self.dependencies.enable_workflow_timer_task_cleanup(){return;}
  let keys=match self.dependencies.fetch_workflow_timer_tasks_for_cleanup(task,self.shard.get_shard_id()).await {Ok(keys)=>keys,Err(e)=>{self.logger.warn("failed to fetch workflow timer tasks for cleanup",&[tag::error(&e)]);return;}};
  if keys.is_empty(){return;}
  self.metrics_client.add_counter("HistoryProcessDeleteHistoryEventScope","WorkflowCleanupTimerTasksSentForDeletionCount",keys.len() as i64);
  if let Err(e)=self.dependencies.complete_history_task(keys,HistoryTaskCategory::Timer,self.shard.get_shard_id()).await {self.logger.error("failed to complete workflow timer tasks for cleanup",&[tag::error(&e)]);}
 }
 pub fn stop(&self){self.logger.info("Stopping timerTaskExecutorBase",&[]);self.cancel.cancel();}
}
