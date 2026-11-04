//! In-memory workflow query registry, ported from Go `service/history/query`.
use cadence_api::{enums::QueryResultType, tasks::{WorkflowQuery, WorkflowQueryResult}};
use std::{collections::HashMap, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}};
use tokio::sync::Notify;

pub const TERMINATION_TYPE_COMPLETED: i32 = 0;
pub const TERMINATION_TYPE_UNBLOCKED: i32 = 1;
pub const TERMINATION_TYPE_FAILED: i32 = 2;
pub const ERR_TERMINATION_STATE_INVALID: &str = "query termination state invalid";
pub const ERR_ALREADY_IN_TERMINAL_STATE: &str = "query already in terminal state";
pub const ERR_QUERY_NOT_IN_TERMINAL_STATE: &str = "query not in terminal state";
pub const ERR_QUERY_NOT_EXISTS: &str = "query does not exist";
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError(pub &'static str);
impl std::fmt::Display for QueryError { fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result { f.write_str(self.0) } }
impl std::error::Error for QueryError {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationType { Completed, Unblocked, Failed }
#[derive(Debug, Clone)]
pub struct TerminationState { pub termination_type: TerminationType, pub query_result: Option<WorkflowQueryResult>, pub failure: Option<String> }
struct Query { id:String, input:WorkflowQuery, term:Arc<Notify>, state:Mutex<Option<TerminationState>> }
impl Query {
 fn new(input:WorkflowQuery)->Self { let n=NEXT_ID.fetch_add(1,Ordering::Relaxed); Self{id:format!("00000000-0000-4000-8000-{n:012x}"),input,term:Arc::new(Notify::new()),state:Mutex::new(None)} }
 fn termination(&self)->Result<TerminationState,QueryError>{self.state.lock().unwrap().clone().ok_or(QueryError(ERR_QUERY_NOT_IN_TERMINAL_STATE))}
 fn set_termination(&self,s:TerminationState)->Result<(),QueryError>{validate_termination_state(Some(&s))?;let mut state=self.state.lock().unwrap();if state.is_some(){return Err(QueryError(ERR_ALREADY_IN_TERMINAL_STATE))}*state=Some(s);self.term.notify_waiters();Ok(())}
}
pub fn validate_termination_state(s:Option<&TerminationState>)->Result<(),QueryError>{
 let s=s.ok_or(QueryError(ERR_TERMINATION_STATE_INVALID))?;
 match s.termination_type {
  TerminationType::Completed=>{let r=s.query_result.as_ref().ok_or(QueryError(ERR_TERMINATION_STATE_INVALID))?;if s.failure.is_some(){return Err(QueryError(ERR_TERMINATION_STATE_INVALID))}let answered=r.result_type==Some(QueryResultType::Answered)&&!r.answer.is_empty()&&r.error_message.is_empty();let failed=r.result_type==Some(QueryResultType::Failed)&&r.answer.is_empty()&&!r.error_message.is_empty();if answered||failed {Ok(())}else{Err(QueryError(ERR_TERMINATION_STATE_INVALID))}},
  TerminationType::Unblocked if s.query_result.is_none()&&s.failure.is_none()=>Ok(()),
  TerminationType::Failed if s.query_result.is_none()&&s.failure.is_some()=>Ok(()),
  _=>Err(QueryError(ERR_TERMINATION_STATE_INVALID))
 }
}
#[derive(Default)]
pub struct Registry { buffered:HashMap<String,Arc<Query>>, completed:HashMap<String,Arc<Query>>, unblocked:HashMap<String,Arc<Query>>, failed:HashMap<String,Arc<Query>> }
impl Registry {
 pub fn new()->Self{Self::default()}
 pub fn has_buffered_query(&self)->bool{!self.buffered.is_empty()} pub fn get_buffered_ids(&self)->Vec<String>{ids(&self.buffered)}
 pub fn has_completed_query(&self)->bool{!self.completed.is_empty()} pub fn get_completed_ids(&self)->Vec<String>{ids(&self.completed)}
 pub fn has_unblocked_query(&self)->bool{!self.unblocked.is_empty()} pub fn get_unblocked_ids(&self)->Vec<String>{ids(&self.unblocked)}
 pub fn has_failed_query(&self)->bool{!self.failed.is_empty()} pub fn get_failed_ids(&self)->Vec<String>{ids(&self.failed)}
 pub fn buffer_query(&mut self,input:WorkflowQuery)->(String,Arc<Notify>){let q=Arc::new(Query::new(input));let id=q.id.clone();let ch=q.term.clone();self.buffered.insert(id.clone(),q);(id,ch)}
 fn get(&self,id:&str)->Result<&Arc<Query>,QueryError>{self.buffered.get(id).or_else(||self.completed.get(id)).or_else(||self.unblocked.get(id)).or_else(||self.failed.get(id)).ok_or(QueryError(ERR_QUERY_NOT_EXISTS))}
 pub fn get_query_term_ch(&self,id:&str)->Result<Arc<Notify>,QueryError>{Ok(self.get(id)?.term.clone())}
 pub fn get_query_input(&self,id:&str)->Result<WorkflowQuery,QueryError>{Ok(self.get(id)?.input.clone())}
 pub fn get_termination_state(&self,id:&str)->Result<TerminationState,QueryError>{self.get(id)?.termination()}
 pub fn set_termination_state(&mut self,id:&str,s:TerminationState)->Result<(),QueryError>{let q=self.buffered.get(id).ok_or(QueryError(ERR_QUERY_NOT_EXISTS))?.clone();q.set_termination(s.clone())?;self.buffered.remove(id);match s.termination_type{TerminationType::Completed=>{self.completed.insert(id.to_owned(),q);},TerminationType::Unblocked=>{self.unblocked.insert(id.to_owned(),q);},TerminationType::Failed=>{self.failed.insert(id.to_owned(),q);}}Ok(())}
 pub fn remove_query(&mut self,id:&str){self.buffered.remove(id);self.completed.remove(id);self.unblocked.remove(id);self.failed.remove(id);}
}
fn ids(m:&HashMap<String,Arc<Query>>)->Vec<String>{m.keys().cloned().collect()}
