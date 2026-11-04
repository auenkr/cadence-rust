//! Port of go/service/history/task/rescheduler.go.
use std::{collections::{HashMap, HashSet}, sync::{Arc, Mutex, Weak, atomic::{AtomicI32, Ordering}}, time::Duration};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use super::{deps::*, interface::*};

pub const TASK_CHAN_FULL_BACKOFF: Duration = Duration::from_secs(2);
pub const TASK_CHAN_FULL_BACKOFF_JITTER_COEFFICIENT: f64 = 0.5;
pub const RESCHEDULER_PQ_CLEANUP_DURATION: Duration = Duration::from_secs(180);
pub const RESCHEDULER_PQ_CLEANUP_JITTER_COEFFICIENT: f64 = 0.15;
#[derive(Clone)]
struct RescheduledTask { task: Arc<dyn Task>, reschedule_time: Time }
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DomainPriorityKey { pub domain_id: String, pub priority: i32 }
#[derive(Default)]
struct State { queues: HashMap<DomainPriorityKey, Vec<RescheduledTask>>, count: usize }
pub struct ReschedulerImpl {
 scheduler: Arc<dyn Processor>, time_source: Arc<dyn TimeSource>, logger: Logger,
 metrics_scope: Arc<dyn MetricsScope>, status: AtomicI32, cancel: CancellationToken,
 timer_gate: Arc<TimerGate>, state: Mutex<State>, handle: Mutex<Option<tokio::task::JoinHandle<()>>>, weak: Weak<Self>,
}
pub fn new_rescheduler(scheduler: Arc<dyn Processor>, time_source: Arc<dyn TimeSource>, logger: Logger, metrics_scope: Arc<dyn MetricsScope>) -> Arc<dyn Rescheduler> {
 Arc::new_cyclic(|weak| ReschedulerImpl { scheduler, timer_gate: Arc::new(TimerGate::new(time_source.clone())), time_source, logger, metrics_scope, status: AtomicI32::new(DAEMON_STATUS_INITIALIZED), cancel: CancellationToken::new(), state: Mutex::new(State::default()), handle: Mutex::new(None), weak: weak.clone() })
}
impl ReschedulerImpl {
 fn is_stopped(&self) -> bool { self.status.load(Ordering::SeqCst) == DAEMON_STATUS_STOPPED }
 fn drain(&self) { let mut s=self.state.lock().unwrap(); s.queues.clear(); s.count=0; }
 fn cleanup_pq(&self) { self.state.lock().unwrap().queues.retain(|_, q| !q.is_empty()); }
 async fn reschedule(&self) {
  // Move queues out while awaiting the scheduler: synchronous producers never block a runtime thread on an async lock.
  let mut queues = { let mut s=self.state.lock().unwrap(); std::mem::take(&mut s.queues) };
  let now=self.time_source.now();
  self.metrics_scope.update_gauge("ReschedulerTaskCountGauge", self.size() as f64);
  let mut removed=0;
  for q in queues.values_mut() {
   q.sort_by_key(|t| t.reschedule_time);
   while let Some(item)=q.first().cloned() {
    if now.before(item.reschedule_time) { self.timer_gate.update(item.reschedule_time); break; }
    if item.task.state()!=TaskState::Pending { q.remove(0); removed+=1; continue; }
    if item.task.get_attempt()==0 { item.task.set_initial_submit_time(now); }
    let submitted=match self.scheduler.try_submit(item.task).await { Ok(v)=>v, Err(e)=> { if self.is_stopped() { break; } self.logger.error("Failed to reschedule task", &[tag::error(&e)]); false } };
    if !submitted { self.timer_gate.update(now.add(jit_duration(TASK_CHAN_FULL_BACKOFF,TASK_CHAN_FULL_BACKOFF_JITTER_COEFFICIENT))); break; }
    q.remove(0); removed+=1;
   }
  }
  let mut s=self.state.lock().unwrap();
  for (key, mut q) in queues { s.queues.entry(key).or_default().append(&mut q); }
  s.count-=removed;
 }
 async fn reschedule_loop(self: Arc<Self>) {
  loop { tokio::select! {
   _=self.cancel.cancelled()=> { self.drain(); return; },
   _=self.timer_gate.fired()=> self.reschedule().await,
   _=tokio::time::sleep(jit_duration(RESCHEDULER_PQ_CLEANUP_DURATION,RESCHEDULER_PQ_CLEANUP_JITTER_COEFFICIENT))=>self.cleanup_pq(),
  } }
 }
}
#[async_trait]
impl Daemon for ReschedulerImpl {
 async fn start(&self) {
  if self.status.compare_exchange(DAEMON_STATUS_INITIALIZED,DAEMON_STATUS_STARTED,Ordering::SeqCst,Ordering::SeqCst).is_err() { return; }
  let me=self.weak.upgrade().unwrap(); *self.handle.lock().unwrap()=Some(tokio::spawn(me.reschedule_loop()));
  self.logger.info("Task rescheduler started.", &[tag::life_cycle_started()]);
 }
 async fn stop(&self) {
  if self.status.compare_exchange(DAEMON_STATUS_STARTED,DAEMON_STATUS_STOPPED,Ordering::SeqCst,Ordering::SeqCst).is_err() { return; }
  self.cancel.cancel(); self.timer_gate.stop(); let handle=self.handle.lock().unwrap().take();
  if let Some(h)=handle { if tokio::time::timeout(Duration::from_secs(60),h).await.is_err() { self.logger.warn("Task rescheduler timedout on shutdown.", &[tag::life_cycle_stop_timedout()]); } }
  self.logger.info("Task rescheduler stopped.", &[tag::life_cycle_stopped()]);
 }
}
impl Rescheduler for ReschedulerImpl {
 fn reschedule_task(&self,task:Arc<dyn Task>,t:Time) {
  let key=DomainPriorityKey { domain_id:task.get_domain_id().to_owned(), priority:task.priority() };
  { let mut s=self.state.lock().unwrap(); s.queues.entry(key).or_default().push(RescheduledTask {task,reschedule_time:t}); s.count+=1; }
  self.timer_gate.update(t); if self.is_stopped() { self.drain(); }
 }
 fn reschedule_domains(&self,ids:&HashSet<String>) {
  let mut s=self.state.lock().unwrap(); let now=self.time_source.now(); let mut updated=false;
  for (key,q) in &mut s.queues { if !ids.contains(&key.domain_id) {continue;} updated=true;
   for item in q { item.reschedule_time=now.max(item.task.get_task_key().get_scheduled_time()); }
  }
  if updated {self.timer_gate.update(now);}
 }
 fn size(&self)->usize {self.state.lock().unwrap().count}
}
