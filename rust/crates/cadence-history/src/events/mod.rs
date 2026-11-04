//! History event cache, blob index, and notification pub/sub ported from Go.
use cadence_api::{service_types::HistoryEvent, types::{DataBlob, VersionHistories}};
use std::{collections::HashMap, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}};
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
pub struct EventCache<R>{reader:Arc<R>,events:Mutex<HashMap<EventKey,Arc<HistoryEvent>>>,max_count:usize,disabled:bool}
impl<R:HistoryEventReader> EventCache<R>{pub fn new(reader:Arc<R>,max_count:usize)->Self{Self{reader,events:Mutex::new(HashMap::new()),max_count,disabled:false}}
 pub fn disabled(mut self,disabled:bool)->Self{self.disabled=disabled;self}
 pub fn get_event(&self,shard_id:i32,domain_id:&str,workflow_id:&str,run_id:&str,first_event_id:i64,event_id:i64,branch_token:&[u8])->Result<Arc<HistoryEvent>,EventCacheError>{let key=EventKey{domain_id:domain_id.into(),workflow_id:workflow_id.into(),run_id:run_id.into(),event_id};if !self.disabled {if let Some(e)=self.events.lock().unwrap().get(&key).cloned(){return Ok(e)}}let event=Arc::new(self.reader.read_event(shard_id,domain_id,first_event_id,event_id,branch_token)?);if !self.disabled{self.put(key,event.clone())}Ok(event)}
 pub fn put_event(&self,domain_id:&str,workflow_id:&str,run_id:&str,event_id:i64,event:HistoryEvent){self.put(EventKey{domain_id:domain_id.into(),workflow_id:workflow_id.into(),run_id:run_id.into(),event_id},Arc::new(event))}
 fn put(&self,key:EventKey,event:Arc<HistoryEvent>){let mut c=self.events.lock().unwrap();if !c.contains_key(&key)&&self.max_count>0&&c.len()>=self.max_count{if let Some(k)=c.keys().next().cloned(){c.remove(&k);}}c.insert(key,event);}
}
#[derive(Debug, Clone, Hash, PartialEq, Eq)]pub struct WorkflowIdentifier{pub domain_id:String,pub workflow_id:String,pub run_id:String}
#[derive(Debug, Clone, Default)]pub struct Notification{pub id:WorkflowIdentifier,pub last_first_event_id:i64,pub next_event_id:i64,pub previous_started_event_id:i64,pub timestamp_unix_millis:i64,pub workflow_state:i32,pub workflow_close_state:i32,pub version_histories:Option<VersionHistories>}
impl Default for WorkflowIdentifier {fn default()->Self{Self{domain_id:String::new(),workflow_id:String::new(),run_id:String::new()}}}
pub fn new_notification(domain_id:impl Into<String>,workflow_id:impl Into<String>,run_id:impl Into<String>,last_first_event_id:i64,next_event_id:i64,previous_started_event_id:i64,workflow_state:i32,workflow_close_state:i32,version_histories:Option<VersionHistories>)->Notification{Notification{id:WorkflowIdentifier{domain_id:domain_id.into(),workflow_id:workflow_id.into(),run_id:run_id.into()},last_first_event_id,next_event_id,previous_started_event_id,timestamp_unix_millis:0,workflow_state,workflow_close_state,version_histories}}
pub struct Notifier{running:AtomicBool,subscribers:Mutex<HashMap<WorkflowIdentifier,HashMap<String,mpsc::Sender<Arc<Notification>>>>>,events_tx:mpsc::Sender<Arc<Notification>>,events_rx:tokio::sync::Mutex<mpsc::Receiver<Arc<Notification>>>}
impl Notifier{pub const EVENTS_CHAN_SIZE:usize=1000;pub fn new()->Arc<Self>{let(tx,rx)=mpsc::channel(Self::EVENTS_CHAN_SIZE);Arc::new(Self{running:AtomicBool::new(false),subscribers:Mutex::new(HashMap::new()),events_tx:tx,events_rx:tokio::sync::Mutex::new(rx)})}
 pub fn watch_history_event(&self,id:WorkflowIdentifier,subscriber_id:String)->(String,mpsc::Receiver<Arc<Notification>>){let(tx,rx)=mpsc::channel(1);self.subscribers.lock().unwrap().entry(id).or_default().insert(subscriber_id.clone(),tx);(subscriber_id,rx)}
 pub fn unwatch_history_event(&self,id:&WorkflowIdentifier,subscriber_id:&str)->Result<(),EventCacheError>{let mut all=self.subscribers.lock().unwrap();let Some(subs)=all.get_mut(id) else{return Err(EventCacheError("Unable to unwatch on workflow execution.".into()))};if subs.remove(subscriber_id).is_none(){return Err(EventCacheError("Unable to unwatch on workflow execution.".into()))}if subs.is_empty(){all.remove(id);}Ok(())}
 pub fn notify_new_history_event(&self,event:Notification){let _=self.events_tx.try_send(Arc::new(event));}
 pub async fn dispatch_one(&self)->bool{let event={self.events_rx.lock().await.recv().await};let Some(event)=event else{return false};if let Some(subs)=self.subscribers.lock().unwrap().get(&event.id){for tx in subs.values(){let _=tx.try_send(event.clone());}}true}
 pub fn start(&self){self.running.store(true,Ordering::SeqCst)}pub fn stop(&self){self.running.store(false,Ordering::SeqCst)}pub fn is_running(&self)->bool{self.running.load(Ordering::SeqCst)} }
