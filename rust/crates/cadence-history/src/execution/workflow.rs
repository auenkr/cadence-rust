//! NDC workflow conflict resolution from Go `execution/workflow.go`.
use cadence_api::types::ActiveClusterSelectionPolicy;

pub const IDENTITY_HISTORY_SERVICE: &str = "history-service";
pub const WORKFLOW_TERMINATION_IDENTITY: &str = "worker-service";
pub const WORKFLOW_TERMINATION_REASON: &str = "Terminate Workflow Due To Version Conflict.";

#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowVectorClock {
    pub active_cluster_selection_policy: Option<ActiveClusterSelectionPolicy>,
    pub last_write_version: i64,
    pub last_event_task_id: i64,
    pub start_timestamp_unix_nanos: i64,
    pub run_id: String,
}
pub fn workflow_happens_after(this: &WorkflowVectorClock, that: &WorkflowVectorClock) -> bool {
    if this.active_cluster_selection_policy != that.active_cluster_selection_policy {
        if this.start_timestamp_unix_nanos != that.start_timestamp_unix_nanos { return this.start_timestamp_unix_nanos > that.start_timestamp_unix_nanos; }
        return this.run_id > that.run_id;
    }
    if this.last_write_version != that.last_write_version { return this.last_write_version > that.last_write_version; }
    this.last_event_task_id > that.last_event_task_id
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionPolicy { Active, Passive }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowState { Created, Running, Completed, Zombie, Corrupted }

/// Operations supplied by the execution context. Each mutating operation stays
/// in the current transaction: conflict resolution must not persist early.
pub trait WorkflowStateAccess {
    type Error;
    fn vector_clock(&self) -> Result<WorkflowVectorClock, Self::Error>;
    fn state(&self) -> WorkflowState;
    fn is_running(&self) -> bool;
    fn has_processed_or_pending_decision(&self) -> bool;
    fn has_buffered_events(&self) -> bool;
    fn has_in_flight_decision(&self) -> bool;
    fn next_event_id(&self) -> i64;
    fn cluster_for_version(&self, version: i64) -> Result<String, Self::Error>;
    fn current_cluster(&self) -> &str;
    fn internal_error(&self, message: &'static str) -> Self::Error;
    fn update_state(&mut self, state: WorkflowState) -> Result<(), Self::Error>;
    fn update_current_version(&mut self, version: i64, force: bool) -> Result<(), Self::Error>;
    fn fail_in_flight_decision_for_failover(&mut self) -> Result<(), Self::Error>;
    fn schedule_decision(&mut self) -> Result<(), Self::Error>;
    fn terminate(&mut self, batch_first_event_id: i64, reason: &str, details: Vec<u8>, identity: &str) -> Result<(), Self::Error>;
}
pub struct Workflow<S> { pub state: S }
impl<S: WorkflowStateAccess> Workflow<S> {
    pub fn get_vector_clock(&self) -> Result<WorkflowVectorClock, S::Error> { self.state.vector_clock() }
    pub fn happens_after<T: WorkflowStateAccess<Error = S::Error>>(&self, that: &Workflow<T>) -> Result<bool, S::Error> {
        Ok(workflow_happens_after(&self.get_vector_clock()?, &that.get_vector_clock()?))
    }
    pub fn revive(&mut self) -> Result<(), S::Error> {
        if self.state.state() != WorkflowState::Zombie { return Ok(()); }
        let state = if self.state.has_processed_or_pending_decision() { WorkflowState::Running } else { WorkflowState::Created };
        self.state.update_state(state)
    }
    pub fn suppress_by(&mut self, incoming: &WorkflowVectorClock) -> Result<TransactionPolicy, S::Error> {
        let current = self.get_vector_clock()?;
        if workflow_happens_after(&current, incoming) { return Err(self.state.internal_error("nDCWorkflow cannot suppress workflow by older workflow")); }
        if !self.state.is_running() { return Ok(TransactionPolicy::Passive); }
        let last_cluster = self.state.cluster_for_version(current.last_write_version)?;
        if self.state.current_cluster() == last_cluster {
            self.terminate_workflow(current.last_write_version, incoming.last_write_version, WORKFLOW_TERMINATION_REASON)?;
            Ok(TransactionPolicy::Active)
        } else {
            self.state.update_state(WorkflowState::Zombie)?;
            Ok(TransactionPolicy::Passive)
        }
    }
    pub fn flush_buffered_events(&mut self) -> Result<(), S::Error> {
        if !self.state.is_running() || !self.state.has_buffered_events() { return Ok(()); }
        let current = self.get_vector_clock()?;
        let last_cluster = self.state.cluster_for_version(current.last_write_version)?;
        if last_cluster != self.state.current_cluster() { return Err(self.state.internal_error("nDCWorkflow encounter workflow with buffered events but last write not from current cluster")); }
        self.fail_decision(current.last_write_version, true)
    }
    fn fail_decision(&mut self, version: i64, schedule_new: bool) -> Result<(), S::Error> {
        self.state.update_current_version(version, true)?;
        if !self.state.has_in_flight_decision() { return Ok(()); }
        self.state.fail_in_flight_decision_for_failover()?;
        if schedule_new { self.state.schedule_decision()?; }
        Ok(())
    }
    fn terminate_workflow(&mut self, version: i64, incoming_version: i64, reason: &str) -> Result<(), S::Error> {
        let batch_first_event_id = self.state.next_event_id();
        self.fail_decision(version, false)?;
        self.state.update_current_version(version, true)?;
        self.state.terminate(batch_first_event_id, reason, format!("terminated by version: {incoming_version}").into_bytes(), WORKFLOW_TERMINATION_IDENTITY)
    }
}
