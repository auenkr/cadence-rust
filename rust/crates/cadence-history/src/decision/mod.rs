//! Decision task validation and dispatch logic ported from Go `service/history/decision`.
//! TODO(link): execution-facing callbacks become `execution::MutableState` methods once that port lands.
use cadence_api::{enums::DecisionType,tasks::Decision};
use std::time::Duration;

pub const ACTIVITY_CANCELLATION_MSG_ACTIVITY_ID_UNKNOWN:&str="ACTIVITY_ID_UNKNOWN";
pub const ACTIVITY_CANCELLATION_MSG_ACTIVITY_NOT_STARTED:&str="ACTIVITY_ID_NOT_STARTED";
pub const FAILURE_REASON_HISTORY_SIZE_EXCEEDS_LIMIT:&str="Workflow history size / count exceeds limit.";
#[derive(Debug,Clone,PartialEq,Eq)]pub struct DecisionError{pub message:String}
impl DecisionError{pub fn new(m:impl Into<String>)->Self{Self{message:m.into()}}}
impl std::fmt::Display for DecisionError{fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.write_str(&self.message)}}impl std::error::Error for DecisionError{}

/// Validates that a decision carries attributes for exactly its declared action.
/// Detailed per-attribute policy checks are exposed through `AttributeValidator`.
pub fn validate_decision_attr(d:&Decision)->Result<DecisionType,DecisionError>{
 let ty=d.decision_type.ok_or_else(||DecisionError::new("DecisionType is not set"))?;
 let set=[d.schedule_activity_task_decision_attributes.is_some(),d.request_cancel_activity_task_decision_attributes.is_some(),d.start_timer_decision_attributes.is_some(),d.complete_workflow_execution_decision_attributes.is_some(),d.fail_workflow_execution_decision_attributes.is_some(),d.cancel_timer_decision_attributes.is_some(),d.cancel_workflow_execution_decision_attributes.is_some(),d.request_cancel_external_workflow_execution_decision_attributes.is_some(),d.record_marker_decision_attributes.is_some(),d.continue_as_new_workflow_execution_decision_attributes.is_some(),d.start_child_workflow_execution_decision_attributes.is_some(),d.signal_external_workflow_execution_decision_attributes.is_some(),d.upsert_workflow_search_attributes_decision_attributes.is_some()];
 let expected=match ty {DecisionType::ScheduleActivityTask=>0,DecisionType::RequestCancelActivityTask=>1,DecisionType::StartTimer=>2,DecisionType::CompleteWorkflowExecution=>3,DecisionType::FailWorkflowExecution=>4,DecisionType::CancelTimer=>5,DecisionType::CancelWorkflowExecution=>6,DecisionType::RequestCancelExternalWorkflowExecution=>7,DecisionType::RecordMarker=>8,DecisionType::ContinueAsNewWorkflowExecution=>9,DecisionType::StartChildWorkflowExecution=>10,DecisionType::SignalExternalWorkflowExecution=>11,DecisionType::UpsertWorkflowSearchAttributes=>12,DecisionType::AcquireSemaphore|DecisionType::ReleaseSemaphore=>return Err(DecisionError::new("decision type attributes are not supported by this API type")),_=>return Err(DecisionError::new("unknown decision type"))};
 if !set[expected]{return Err(DecisionError::new(format!("DecisionAttributes for {:?} is not set",ty)))}
 if set.iter().enumerate().any(|(i,present)|*present&&i!=expected){return Err(DecisionError::new(format!("DecisionAttributes do not match decision type {:?}",ty)))}Ok(ty)
}

/// Port of checker.go's shared decision attribute validation rules.
pub struct AttributeValidator {pub max_id_bytes:usize,pub max_input_bytes:usize,pub max_reason_bytes:usize,pub max_details_bytes:usize,pub max_marker_bytes:usize,pub max_search_attribute_count:usize,pub max_search_attribute_bytes:usize}
impl Default for AttributeValidator{fn default()->Self{Self{max_id_bytes:256,max_input_bytes:2*1024*1024,max_reason_bytes:256,max_details_bytes:2*1024*1024,max_marker_bytes:256,max_search_attribute_count:0,max_search_attribute_bytes:0}}}
impl AttributeValidator {
 pub fn validate(&self,d:&Decision)->Result<(),DecisionError>{let ty=validate_decision_attr(d)?;match ty {
  DecisionType::ScheduleActivityTask=>{let a=d.schedule_activity_task_decision_attributes.as_ref().unwrap();self.required("ActivityId",&a.activity_id)?;self.limit("ActivityId",a.activity_id.len(),self.max_id_bytes)?;self.limit("Input",a.input.len(),self.max_input_bytes)?;if let Some(t)=&a.task_list{self.required("TaskList",&t.name)?;}},
  DecisionType::StartTimer=>{let a=d.start_timer_decision_attributes.as_ref().unwrap();self.required("TimerId",&a.timer_id)?;if a.start_to_fire_timeout_seconds.unwrap_or(0)<0{return Err(DecisionError::new("StartToFireTimeoutSeconds cannot be negative"))}},
  DecisionType::FailWorkflowExecution=>{let a=d.fail_workflow_execution_decision_attributes.as_ref().unwrap();if let Some(r)=&a.reason{self.limit("Reason",r.len(),self.max_reason_bytes)?;}self.limit("Details",a.details.len(),self.max_details_bytes)?},
  DecisionType::RecordMarker=>{let a=d.record_marker_decision_attributes.as_ref().unwrap();self.required("MarkerName",&a.marker_name)?;self.limit("MarkerName",a.marker_name.len(),self.max_marker_bytes)?;},
  DecisionType::RequestCancelActivityTask=>self.required("ActivityId",&d.request_cancel_activity_task_decision_attributes.as_ref().unwrap().activity_id)?,
  DecisionType::CancelTimer=>self.required("TimerId",&d.cancel_timer_decision_attributes.as_ref().unwrap().timer_id)?,
  _=>{}
 }Ok(())}
 fn required(&self,field:&str,value:&str)->Result<(),DecisionError>{if value.is_empty(){Err(DecisionError::new(format!("{} is not set",field)))}else{Ok(())}}
 fn limit(&self,field:&str,n:usize,max:usize)->Result<(),DecisionError>{if max>0&&n>max{Err(DecisionError::new(format!("{} exceeds limit",field)))}else{Ok(())}}
 pub fn validate_activity_schedule_attributes(&self,d:&Decision)->Result<(),DecisionError>{if d.decision_type!=Some(DecisionType::ScheduleActivityTask){return Err(DecisionError::new("decision is not ScheduleActivityTask"))}self.validate(d)}
 pub fn validate_timer_schedule_attributes(&self,d:&Decision)->Result<(),DecisionError>{if d.decision_type!=Some(DecisionType::StartTimer){return Err(DecisionError::new("decision is not StartTimer"))}self.validate(d)}
 pub fn validate_activity_cancel_attributes(&self,d:&Decision)->Result<(),DecisionError>{if d.decision_type!=Some(DecisionType::RequestCancelActivityTask){return Err(DecisionError::new("decision is not RequestCancelActivityTask"))}self.validate(d)}
 pub fn validate_timer_cancel_attributes(&self,d:&Decision)->Result<(),DecisionError>{if d.decision_type!=Some(DecisionType::CancelTimer){return Err(DecisionError::new("decision is not CancelTimer"))}self.validate(d)}
 pub fn validate_record_marker_attributes(&self,d:&Decision)->Result<(),DecisionError>{if d.decision_type!=Some(DecisionType::RecordMarker){return Err(DecisionError::new("decision is not RecordMarker"))}self.validate(d)}
 pub fn validate_complete_workflow_execution_attributes(&self,d:&Decision)->Result<(),DecisionError>{if d.decision_type!=Some(DecisionType::CompleteWorkflowExecution){return Err(DecisionError::new("decision is not CompleteWorkflowExecution"))}self.validate(d)}
 pub fn validate_fail_workflow_execution_attributes(&self,d:&Decision)->Result<(),DecisionError>{if d.decision_type!=Some(DecisionType::FailWorkflowExecution){return Err(DecisionError::new("decision is not FailWorkflowExecution"))}self.validate(d)}
 pub fn validate_cancel_workflow_execution_attributes(&self,d:&Decision)->Result<(),DecisionError>{if d.decision_type!=Some(DecisionType::CancelWorkflowExecution){return Err(DecisionError::new("decision is not CancelWorkflowExecution"))}self.validate(d)}
}
#[derive(Debug,Clone,Copy,Default)]pub struct WorkflowSizeLimits{pub history_size_warn:usize,pub history_size_error:usize,pub history_count_warn:usize,pub history_count_error:usize,pub blob_size_warn:usize,pub blob_size_error:usize}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]pub enum SizeAction{Continue,Warn,Fail}
pub fn check_workflow_size(history_size:usize,history_count:usize,limits:WorkflowSizeLimits)->SizeAction{if history_size>limits.history_size_error||history_count>limits.history_count_error{SizeAction::Fail}else if history_size>limits.history_size_warn||history_count>limits.history_count_warn{SizeAction::Warn}else{SizeAction::Continue}}
pub fn check_blob_size(blob_size:usize,limits:WorkflowSizeLimits)->SizeAction{if limits.blob_size_error>0&&blob_size>limits.blob_size_error{SizeAction::Fail}else if limits.blob_size_warn>0&&blob_size>limits.blob_size_warn{SizeAction::Warn}else{SizeAction::Continue}}
pub fn acquire_wait_timeout_seconds(timeout:Duration)->i64{timeout.as_secs().min(i64::MAX as u64) as i64}
pub fn convert_search_attributes_to_byte_array(fields:&std::collections::BTreeMap<String,Vec<u8>>)->Vec<u8>{serde_json::to_vec(fields).unwrap_or_default()}

/// Workflow mutation seam while the mutable-state port is in progress.
pub trait DecisionWorkflowBackend {fn handle_decision(&mut self,decision:&Decision)->Result<(),DecisionError>;fn fail_decision(&mut self,cause:&str,message:Option<&str>)->Result<(),DecisionError>;fn complete_decision(&mut self,execution_context:&[u8])->Result<(),DecisionError>;}
pub struct DecisionTaskHandler<B>{pub backend:B,pub validator:AttributeValidator,pub stop_processing:bool,pub fail_decision:bool,pub has_unhandled_events_before_decisions:bool}
impl<B:DecisionWorkflowBackend> DecisionTaskHandler<B>{pub fn new(backend:B,validator:AttributeValidator,has_unhandled_events_before_decisions:bool)->Self{Self{backend,validator,stop_processing:false,fail_decision:false,has_unhandled_events_before_decisions}}
 pub fn handle_decisions(&mut self,execution_context:&[u8],decisions:&[Decision])->Result<(),DecisionError>{for d in decisions{if self.stop_processing{break}if let Err(e)=self.validator.validate(d){self.backend.fail_decision("BAD_DECISION_ATTRIBUTES",Some(&e.message))?;self.fail_decision=true;self.stop_processing=true;break}self.backend.handle_decision(d)?;}if !self.fail_decision{self.backend.complete_decision(execution_context)?}Ok(())}}
