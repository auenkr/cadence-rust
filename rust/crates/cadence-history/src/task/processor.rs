//! Port of go/service/history/task/processor.go.
use std::{collections::HashMap, sync::{Arc, atomic::{AtomicI32, Ordering}}};
use async_trait::async_trait;
use super::{deps::*, interface::*, priority_assigner::HIGH_TASK_PRIORITY};
pub use super::rescheduler::DomainPriorityKey;
pub const EPHEMERAL_TASK_LIST_GROUP_KEY: &str = "__ephemeral__";
pub const NICE_VALUE_TO_WEIGHT: [i32; 20] = [349,278,223,179,143,114,92,73,58,47,38,30,24,19,16,12,10,8,7,5];
pub const ERR_TASK_PROCESSOR_NOT_RUNNING: &str = "queue task processor is not running";
// TODO(link): replace by config::Config task-scheduler properties.
pub trait SchedulerConfig: Send + Sync {
 fn enable_task_list_aware_task_scheduler_by_domain(&self,domain:&str)->bool;
 fn task_list_nice_value(&self,domain:&str,task_list:&str)->i32;
 fn task_scheduler_domain_round_robin_weights(&self,domain:&str)->Result<HashMap<i32,i32>>;
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum SchedulerKey { DomainPriority(DomainPriorityKey), TaskList(String) }
#[derive(Clone,Debug)]
pub struct WeightedKey {pub key:SchedulerKey,pub weight:i32}
pub fn get_domain_priority_weight(logger:&Logger,config:&dyn SchedulerConfig,cache:&dyn DomainCache,key:&DomainPriorityKey)->i32 {
 let defaults=||HashMap::from([(1,500),(9,20),(17,5)]);
 let weights=match cache.get_domain_name(&key.domain_id) {
  Err(e)=>{logger.error("failed to get domain name from cache, use default round robin weights",&[tag::error(&e)]);defaults()},
  Ok(name)=>match config.task_scheduler_domain_round_robin_weights(&name) {
   Ok(w)=>w,Err(e)=>{logger.error("failed to convert dynamic config map to int map, use default round robin weights",&[tag::error(&e)]);defaults()}
  }
 };
 *weights.get(&key.priority).unwrap_or_else(||{logger.error("weights not found for task priority, default to 1",&[]);&1})
}
pub fn task_to_weighted_keys(task:&dyn Task,config:&dyn SchedulerConfig,cache:&dyn DomainCache,logger:&Logger)->Vec<WeightedKey> {
 let key=DomainPriorityKey {domain_id:task.get_domain_id().to_owned(),priority:task.priority()};
 let name=cache.get_domain_name(&key.domain_id).unwrap_or_else(|e|{logger.error("failed to get domain name from cache",&[tag::error(&e)]);String::new()});
 let mut keys=vec![WeightedKey {weight:get_domain_priority_weight(logger,config,cache,&key),key:SchedulerKey::DomainPriority(key)}];
 if config.enable_task_list_aware_task_scheduler_by_domain(&name)&&task.priority()==HIGH_TASK_PRIORITY {
  let list=if task.get_original_task_list_kind()==TaskListKind::Ephemeral {EPHEMERAL_TASK_LIST_GROUP_KEY.to_owned()}else{task.get_original_task_list()};
  keys.push(WeightedKey {key:SchedulerKey::TaskList(list.to_owned()),weight:NICE_VALUE_TO_WEIGHT[(config.task_list_nice_value(&name,&list).clamp(-10,9)+10) as usize]});
 }
 keys
}
pub struct ProcessorImpl {priority_assigner:Arc<dyn PriorityAssigner>,task_processor:Arc<dyn Daemon>,scheduler:Arc<dyn Processor>,status:AtomicI32,logger:Logger}
/// Common task scheduler construction belongs to common/task; the selected scheduler and parallel task processor are injected.
/// TODO(link): replace injected dependencies with common::task::{NewParallelTaskProcessor,NewWeightedRoundRobinTaskScheduler,NewHierarchicalWeightedRoundRobinTaskScheduler}.
pub fn new_processor(priority_assigner:Arc<dyn PriorityAssigner>,task_processor:Arc<dyn Daemon>,scheduler:Arc<dyn Processor>,logger:Logger)->Arc<dyn Processor> {
 Arc::new(ProcessorImpl {priority_assigner,task_processor,scheduler,status:AtomicI32::new(DAEMON_STATUS_INITIALIZED),logger})
}
#[async_trait]
impl Daemon for ProcessorImpl {
 async fn start(&self) {if self.status.compare_exchange(DAEMON_STATUS_INITIALIZED,DAEMON_STATUS_STARTED,Ordering::SeqCst,Ordering::SeqCst).is_err(){return;} self.task_processor.start().await;self.scheduler.start().await;self.logger.info("Queue task processor started.",&[]);}
 async fn stop(&self) {if self.status.compare_exchange(DAEMON_STATUS_STARTED,DAEMON_STATUS_STOPPED,Ordering::SeqCst,Ordering::SeqCst).is_err(){return;}self.scheduler.stop().await;self.task_processor.stop().await;self.logger.info("Queue task processor stopped.",&[]);}
}
#[async_trait]
impl Processor for ProcessorImpl {
 async fn submit(&self,task:Arc<dyn Task>)->Result<()> {self.priority_assigner.assign(task.as_ref())?;self.scheduler.submit(task).await}
 async fn try_submit(&self,task:Arc<dyn Task>)->Result<bool> {self.priority_assigner.assign(task.as_ref())?;self.scheduler.try_submit(task).await}
}
