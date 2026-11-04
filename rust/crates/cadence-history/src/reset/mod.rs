#![allow(clippy::too_many_arguments)]
//! Workflow reset operation orchestration, ported from Go resetter.go.
//! TODO(link): replace execution reset traits with execution::Workflow and execution::StateRebuilder.
pub mod history;
use cadence_api::service_types::HistoryEvent;
use std::sync::Arc;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetError(pub String);
impl std::fmt::Display for ResetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ResetError {}
#[derive(Debug, Clone, Default)]
pub struct CurrentWorkflowInfo {
    pub run_id: String,
    pub next_event_id: i64,
    pub branch_token: Vec<u8>,
    pub selection_policy: Vec<u8>,
    pub is_running: bool,
    pub current_version: i64,
}
pub struct WorkflowResetter<B> { backend: Arc<B> }
impl<B> WorkflowResetter<B> {
    pub fn new(backend: Arc<B>) -> Self { Self { backend } }
}
impl<B: history::ResetEngineBackend> WorkflowResetter<B>
where B::MutableState: history::ResetPreparation + history::ResetTermination {
    #[allow(clippy::too_many_arguments)]
    pub fn reset_workflow(&self, domain_id: &str, workflow_id: &str, base_run_id: &str, base_branch_token: &[u8], base_rebuild_last_event_id: i64, base_rebuild_last_event_version: i64, base_next_event_id: i64, reset_run_id: &str, reset_request_id: &str, current: &mut <B as history::ResetRebuilder>::Workflow, reset_reason: &str, additional_reapply_events: &[HistoryEvent], skip_signal_reapply: bool) -> Result<(), ResetError> {
        history::reset_workflow(self.backend.as_ref(), domain_id, workflow_id, base_run_id, base_branch_token, base_rebuild_last_event_id, base_rebuild_last_event_version, base_next_event_id, reset_run_id, reset_request_id, current, reset_reason, additional_reapply_events, skip_signal_reapply)
    }
}
pub const ERR_VERSION_MISMATCH: &str = "workflowResetter encounter version mismatch.";

pub trait SignalEventReapplier{fn add_workflow_execution_signaled(&mut self,signal_name:&str,input:&[u8],identity:&str,request_id:&str)->Result<(),ResetError>;}
pub fn reapply_events(state:&mut impl SignalEventReapplier,events:&[HistoryEvent])->Result<(),ResetError>{for event in events{if event.event_type==Some(cadence_api::enums::EventType::WorkflowExecutionSignaled){let attrs=event.workflow_execution_signaled_event_attributes.as_ref();state.add_workflow_execution_signaled(attrs.map_or("",|a|a.signal_name.as_str()),attrs.map_or(&[],|a|a.input.as_slice()),attrs.map_or("",|a|a.identity.as_str()),"")?}}Ok(())}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]pub struct ResetDecisionInfo{pub schedule_id:i64,pub started_id:i64}
pub trait ResetMutableState{fn pending_child_execution_count(&self)->usize;fn in_flight_decision(&self)->Option<ResetDecisionInfo>;fn pending_decision(&self)->Option<ResetDecisionInfo>;fn add_decision_task_failed_event(&mut self,decision:ResetDecisionInfo,base_run_id:&str,reset_run_id:&str,base_last_event_version:i64,reset_reason:&str,reset_request_id:&str)->Result<(),ResetError>;fn add_decision_task_reset_timeout_event(&mut self,decision:ResetDecisionInfo,base_run_id:&str,reset_run_id:&str,base_last_event_version:i64,reset_reason:&str,reset_request_id:&str)->Result<(),ResetError>;}
pub fn close_pending_decision_task(state:&mut impl ResetMutableState,base_run_id:&str,reset_run_id:&str,base_last_event_version:i64,reset_reason:&str,reset_request_id:&str)->Result<(),ResetError>{if state.pending_child_execution_count()>0{return Err(ResetError("Can not reset workflow with pending child workflows".into()))}
if let Some(decision)=state.in_flight_decision(){state.add_decision_task_failed_event(decision,base_run_id,reset_run_id,base_last_event_version,reset_reason,reset_request_id)}else if let Some(decision)=state.pending_decision(){state.add_decision_task_reset_timeout_event(decision,base_run_id,reset_run_id,base_last_event_version,reset_reason,reset_request_id)}else{Ok(())}}

pub const EMPTY_EVENT_ID:i64=-23;pub const TRANSIENT_EVENT_ID:i64=-124;
pub const ERR_TRANSIENT_ACTIVITY:&str="workflowResetter encounter transient activity";
#[derive(Debug,Clone,Default)]pub struct PendingActivityInfo{pub schedule_id:i64,pub started_id:i64,pub scheduled_time_unix_nanos:i64,pub timer_task_status:i32,pub details:Vec<u8>,pub started_identity:String}
pub trait PendingActivityMutableState{fn pending_activity_infos(&self)->Vec<PendingActivityInfo>;fn update_pending_activity(&mut self,activity:PendingActivityInfo)->Result<(),ResetError>;fn add_activity_task_failed_event(&mut self,schedule_id:i64,started_id:i64,reason:&str,details:&[u8],identity:&str)->Result<(),ResetError>;}
pub fn fail_inflight_activity(state:&mut impl PendingActivityMutableState,now_unix_nanos:i64,terminate_reason:&str)->Result<(),ResetError>{for mut activity in state.pending_activity_infos(){match activity.started_id{EMPTY_EVENT_ID=>{activity.scheduled_time_unix_nanos=now_unix_nanos;activity.timer_task_status=0;state.update_pending_activity(activity)?},TRANSIENT_EVENT_ID=>return Err(ResetError(ERR_TRANSIENT_ACTIVITY.into())),started_id=>state.add_activity_task_failed_event(activity.schedule_id,started_id,terminate_reason,&activity.details,&activity.started_identity)?}}Ok(())}
