//! Workflow reset operation orchestration, ported from Go resetter.go.
//! TODO(link): replace execution reset traits with execution::Workflow and execution::StateRebuilder.
use cadence_api::service_types::HistoryEvent;
use std::sync::Arc;
#[derive(Debug,Clone,PartialEq,Eq)]pub struct ResetError(pub String);
impl std::fmt::Display for ResetError{fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.write_str(&self.0)}}impl std::error::Error for ResetError{}
pub trait WorkflowResetBackend:Send+Sync{
 type Workflow:Send;
 fn active_failover_version(&self,domain_id:&str,selection_policy:&[u8])->Result<i64,ResetError>;
 fn current_workflow_info(&self,workflow:&Self::Workflow)->Result<CurrentWorkflowInfo,ResetError>;
 fn terminate_current(&self,workflow:&Self::Workflow,reason:&str)->Result<i64,ResetError>;
 fn replay_reset_workflow(&self,domain_id:&str,workflow_id:&str,base_run_id:&str,branch_token:&[u8],last_event_id:i64,last_event_version:i64,reset_run_id:&str,request_id:&str)->Result<Self::Workflow,ResetError>;
 fn prepare_reset(&self,workflow:Self::Workflow,reset_version:i64,base_run_id:&str,reset_run_id:&str,reason:&str,request_id:&str,additional_events:&[HistoryEvent],skip_signal_reapply:bool,current:&CurrentWorkflowInfo)->Result<Self::Workflow,ResetError>;
 fn persist(&self,current:&Self::Workflow,reset:Self::Workflow,current_terminated:bool)->Result<(),ResetError>;
}
#[derive(Debug,Clone,Default)]pub struct CurrentWorkflowInfo{pub run_id:String,pub next_event_id:i64,pub branch_token:Vec<u8>,pub selection_policy:Vec<u8>}
pub struct WorkflowResetter<B:WorkflowResetBackend>{backend:Arc<B>}
impl<B:WorkflowResetBackend> WorkflowResetter<B>{pub fn new(backend:Arc<B>)->Self{Self{backend}}
 #[allow(clippy::too_many_arguments)]pub fn reset_workflow(&self,domain_id:&str,workflow_id:&str,base_run_id:&str,base_branch_token:&[u8],base_rebuild_last_event_id:i64,base_rebuild_last_event_version:i64,base_next_event_id:i64,reset_run_id:&str,reset_request_id:&str,current:&B::Workflow,reset_reason:&str,additional_reapply_events:&[HistoryEvent],skip_signal_reapply:bool)->Result<(),ResetError>{let current_info=self.backend.current_workflow_info(current)?;let mut reset_version=self.backend.active_failover_version(domain_id,&current_info.selection_policy)?;let current_terminated=if reset_version<0{false}else{let v=self.backend.terminate_current(current,reset_reason)?;reset_version=v;true};let rebuilt=self.backend.replay_reset_workflow(domain_id,workflow_id,base_run_id,base_branch_token,base_rebuild_last_event_id,base_rebuild_last_event_version,reset_run_id,reset_request_id)?;let prepared=self.backend.prepare_reset(rebuilt,reset_version,base_run_id,reset_run_id,reset_reason,reset_request_id,additional_reapply_events,skip_signal_reapply,&current_info)?;let _=(base_next_event_id,current_info.next_event_id,current_info.run_id.as_str(),current_info.branch_token.as_slice());self.backend.persist(current,prepared,current_terminated)} }
pub const ERR_VERSION_MISMATCH:&str="workflowResetter encounter version mismatch.";
