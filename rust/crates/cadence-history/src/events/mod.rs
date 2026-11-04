//! History event cache, blob index, and notification pub/sub ported from Go.
use cadence_api::{service_types::HistoryEvent, types::{DataBlob, VersionHistories}};
use std::{collections::{HashMap,VecDeque}, sync::{Arc, Mutex, atomic::{AtomicI32, Ordering}}, time::{Duration,Instant}};
use uuid::Uuid;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersistedBlob { pub data_blob: DataBlob, pub branch_token: Vec<u8>, pub first_event_id:i64 }
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersistedBlobs(pub Vec<PersistedBlob>);
impl PersistedBlobs { /// Linear lookup is intentional: transactions hold one or two persisted blobs.
 pub fn find(&self,branch_token:&[u8],first_event_id:i64)->Option<&DataBlob>{self.0.iter().find(|b|b.branch_token==branch_token&&b.first_event_id==first_event_id).map(|b|&b.data_blob)} }
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct EventKey {pub domain_id:String,pub workflow_id:String,pub run_id:String,pub event_id:i64}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCacheError(pub String);
impl std::fmt::Display for EventCacheError{fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.write_str(&self.0)}} impl std::error::Error for EventCacheError{}
pub const ERR_EVENT_NOT_FOUND_IN_BATCH:&str="History event not found within expected batch";
pub trait HistoryEventReader:Send+Sync {fn read_event(&self,shard_id:i32,domain_id:&str,first_event_id:i64,event_id:i64,branch_token:&[u8])->Result<HistoryEvent,EventCacheError>;}
struct CachedEvent{event:Arc<HistoryEvent>,inserted:Instant}
struct EventCacheState{events:HashMap<EventKey,CachedEvent>,lru:VecDeque<EventKey>}
pub struct EventCache<R>{reader:Arc<R>,state:Mutex<EventCacheState>,max_count:usize,ttl:Duration,disabled:bool}
impl<R:HistoryEventReader> EventCache<R>{pub fn new(reader:Arc<R>,max_count:usize)->Self{Self::new_with_ttl(reader,max_count,Duration::from_secs(300))}pub fn new_with_ttl(reader:Arc<R>,max_count:usize,ttl:Duration)->Self{Self{reader,state:Mutex::new(EventCacheState{events:HashMap::new(),lru:VecDeque::new()}),max_count,ttl,disabled:false}}
 pub fn disabled(mut self,disabled:bool)->Self{self.disabled=disabled;self}
 #[allow(clippy::too_many_arguments)]
 pub fn get_event(&self,shard_id:i32,domain_id:&str,workflow_id:&str,run_id:&str,first_event_id:i64,event_id:i64,branch_token:&[u8])->Result<Arc<HistoryEvent>,EventCacheError>{let key=EventKey{domain_id:domain_id.into(),workflow_id:workflow_id.into(),run_id:run_id.into(),event_id};if !self.disabled {if let Some(e)=self.cached(&key){return Ok(e)}}let event=Arc::new(self.reader.read_event(shard_id,domain_id,first_event_id,event_id,branch_token)?);self.put(key,event.clone());Ok(event)}
 pub fn put_event(&self,domain_id:&str,workflow_id:&str,run_id:&str,event_id:i64,event:HistoryEvent){self.put(EventKey{domain_id:domain_id.into(),workflow_id:workflow_id.into(),run_id:run_id.into(),event_id},Arc::new(event))}
 fn cached(&self,key:&EventKey)->Option<Arc<HistoryEvent>>{let mut c=self.state.lock().unwrap();if let Some(entry)=c.events.get(key){if self.ttl.is_zero() || entry.inserted.elapsed()<=self.ttl{let event=entry.event.clone();c.lru.retain(|k|k!=key);c.lru.push_back(key.clone());return Some(event)}}c.events.remove(key);c.lru.retain(|k|k!=key);None}
 fn put(&self,key:EventKey,event:Arc<HistoryEvent>){let mut c=self.state.lock().unwrap();if c.events.contains_key(&key){c.lru.retain(|k|k!=&key);}while self.max_count>0&&c.events.len()>=self.max_count{if let Some(oldest)=c.lru.pop_front(){c.events.remove(&oldest);}else{break}}c.events.insert(key.clone(),CachedEvent{event,inserted:Instant::now()});c.lru.push_back(key);}
}
#[derive(Debug, Clone, Hash, PartialEq, Eq)]pub struct WorkflowIdentifier{pub domain_id:String,pub workflow_id:String,pub run_id:String}
#[derive(Debug, Clone, Default)]pub struct Notification{pub id:WorkflowIdentifier,pub last_first_event_id:i64,pub next_event_id:i64,pub previous_started_event_id:i64,pub timestamp_unix_millis:i64,pub workflow_state:i32,pub workflow_close_state:i32,pub version_histories:Option<VersionHistories>}
impl Default for WorkflowIdentifier {fn default()->Self{Self{domain_id:String::new(),workflow_id:String::new(),run_id:String::new()}}}
#[allow(clippy::too_many_arguments)]
pub fn new_notification(domain_id:impl Into<String>,workflow_id:impl Into<String>,run_id:impl Into<String>,last_first_event_id:i64,next_event_id:i64,previous_started_event_id:i64,workflow_state:i32,workflow_close_state:i32,version_histories:Option<VersionHistories>)->Notification{Notification{id:WorkflowIdentifier{domain_id:domain_id.into(),workflow_id:workflow_id.into(),run_id:run_id.into()},last_first_event_id,next_event_id,previous_started_event_id,timestamp_unix_millis:0,workflow_state,workflow_close_state,version_histories}}
pub trait NotificationObserver:Send+Sync {fn now_unix_millis(&self)->i64 {std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis().min(i64::MAX as u128) as i64} fn fail_delivery(&self){} fn in_flight(&self,_count:usize){} fn queueing_latency(&self,_elapsed:Duration){} fn fanout_latency(&self,_elapsed:Duration){} }
struct NoopNotificationObserver;impl NotificationObserver for NoopNotificationObserver{}
pub struct Notifier{observer:Arc<dyn NotificationObserver>,status:AtomicI32,stop_signal:tokio::sync::Notify,subscribers:Mutex<HashMap<WorkflowIdentifier,HashMap<String,mpsc::Sender<Arc<Notification>>>>>,events_tx:mpsc::Sender<Arc<Notification>>,events_rx:tokio::sync::Mutex<mpsc::Receiver<Arc<Notification>>>}
impl Notifier{pub const EVENTS_CHAN_SIZE:usize=1000;pub fn new()->Arc<Self>{Self::new_with_observer(Arc::new(NoopNotificationObserver))} pub fn new_with_observer(observer:Arc<dyn NotificationObserver>)->Arc<Self>{let(tx,rx)=mpsc::channel(Self::EVENTS_CHAN_SIZE);Arc::new(Self{observer,status:AtomicI32::new(0),stop_signal:tokio::sync::Notify::new(),subscribers:Mutex::new(HashMap::new()),events_tx:tx,events_rx:tokio::sync::Mutex::new(rx)})}
 pub fn watch_history_event(&self,id:WorkflowIdentifier)->Result<(String,mpsc::Receiver<Arc<Notification>>),EventCacheError>{let subscriber_id=Uuid::new_v4().to_string();let(tx,rx)=mpsc::channel(1);let mut all=self.subscribers.lock().unwrap();let subscribers=all.entry(id).or_default();if subscribers.contains_key(&subscriber_id){return Err(EventCacheError("Unable to watch on workflow execution.".into()))}subscribers.insert(subscriber_id.clone(),tx);Ok((subscriber_id,rx))}
 pub fn unwatch_history_event(&self,id:&WorkflowIdentifier,subscriber_id:&str)->Result<(),EventCacheError>{let mut all=self.subscribers.lock().unwrap();let Some(subs)=all.get_mut(id) else{return Ok(())};if subs.remove(subscriber_id).is_none(){return Err(EventCacheError("Unable to unwatch on workflow execution.".into()))}if subs.is_empty(){all.remove(id);}Ok(())}
 pub fn dispatch_history_event_notification(&self,event:Arc<Notification>){let started=Instant::now();if let Some(subs)=self.subscribers.lock().unwrap().get(&event.id){for tx in subs.values(){let _=tx.try_send(event.clone());}}self.observer.fanout_latency(started.elapsed());}
 pub fn enqueue_history_event_notification(&self,mut event:Notification){event.timestamp_unix_millis=self.observer.now_unix_millis();if self.events_tx.try_send(Arc::new(event)).is_err(){self.observer.fail_delivery();}}
 pub fn notify_new_history_event(&self,event:Notification){self.enqueue_history_event_notification(event);}
 fn process(&self,event:Arc<Notification>){let elapsed=self.observer.now_unix_millis().saturating_sub(event.timestamp_unix_millis).max(0) as u64;self.observer.queueing_latency(Duration::from_millis(elapsed));self.dispatch_history_event_notification(event);}
 pub async fn dispatch_one(&self)->bool{let mut receiver=self.events_rx.lock().await;self.observer.in_flight(receiver.len());let event=receiver.recv().await;drop(receiver);let Some(event)=event else{return false};self.process(event);true}
 pub fn start(self:&Arc<Self>){if self.status.compare_exchange(0,1,Ordering::SeqCst,Ordering::SeqCst).is_err(){return}let this=self.clone();tokio::spawn(async move{this.run().await;});}
 async fn run(self:Arc<Self>){loop{let mut receiver=self.events_rx.lock().await;self.observer.in_flight(receiver.len());let next=tokio::select!{event=receiver.recv()=>event,_=self.stop_signal.notified()=>None};drop(receiver);let Some(event)=next else{break};self.process(event);}self.status.store(2,Ordering::SeqCst);}
 pub fn stop(&self){if self.status.compare_exchange(1,2,Ordering::SeqCst,Ordering::SeqCst).is_ok(){self.stop_signal.notify_one();}}pub fn is_running(&self)->bool{self.status.load(Ordering::SeqCst)==1} }

/// TODO(link): HistoryManager adapter; request limits and batch selection remain here.
pub trait HistoryBranchStore: Send + Sync {
    fn domain_name(&self, domain_id: &str) -> Result<String, EventCacheError>;
    fn read_history_branch(&self, shard_id: i32, domain_name: &str, branch_token: &[u8], min_event_id: i64, max_event_id: i64, page_size: usize) -> Result<Vec<HistoryEvent>, EventCacheError>;
}
pub struct StoreEventReader<S>(pub S);
impl<S: HistoryBranchStore> HistoryEventReader for StoreEventReader<S> {
    fn read_event(&self, shard_id: i32, domain_id: &str, first_event_id: i64, event_id: i64, branch_token: &[u8]) -> Result<HistoryEvent, EventCacheError> {
        let domain_name = self.0.domain_name(domain_id)?;
        self.0.read_history_branch(shard_id, &domain_name, branch_token, first_event_id, event_id + 1, 1)?.into_iter().find(|event| event.event_id == event_id).ok_or_else(|| EventCacheError(ERR_EVENT_NOT_FOUND_IN_BATCH.into()))
    }
}
