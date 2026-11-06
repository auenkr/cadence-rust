//! Semaphore mutable-state transitions from Go's execution semaphore methods.
use std::collections::{BTreeMap, BTreeSet};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemaphoreInfo {
    pub version: i64, pub initiated_id: i64, pub semaphore_name: String,
    pub owner_id: String, pub acquire_deadline_unix_nanos: i64, pub token_id: i32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemaphoreEventAttributes {
    AcquireInitiated { decision_completed_event_id: i64, semaphore_name: String, wait_timeout_seconds: i32 },
    Acquired { initiated_event_id: i64, token_id: i32 },
    Released { initiated_event_id: i64 },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemaphoreEvent {
    pub id: i64, pub version: i64, pub timestamp_unix_nanos: i64,
    pub attributes: SemaphoreEventAttributes,
}
pub trait SemaphoreExecution {
    type Error;
    fn check_mutability(&self, operation: &'static str) -> Result<(), Self::Error>;
    fn workflow_id(&self) -> &str;
    fn run_id(&self) -> &str;
    fn add_semaphore_event(&mut self, attributes: SemaphoreEventAttributes) -> SemaphoreEvent;
    fn caller_error(&self, operation: &'static str) -> Self::Error;
    fn internal_error(&self, operation: &'static str) -> Self::Error;
    fn missing_semaphore_info(&self) -> Self::Error;
    fn log_missing_hold(&mut self, initiated_id: i64);
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SemaphoreState {
    pub pending: BTreeMap<i64, SemaphoreInfo>,
    pub updates: BTreeMap<i64, SemaphoreInfo>,
    pub deletes: BTreeSet<i64>,
}
impl SemaphoreState {
    pub fn get_semaphore_info(&self, id: i64) -> Option<&SemaphoreInfo> { self.pending.get(&id) }
    pub fn get_semaphore_info_by_name(&self, name: &str) -> Option<&SemaphoreInfo> { self.pending.values().find(|info| info.semaphore_name == name) }
    pub fn get_pending_semaphore_infos(&self) -> &BTreeMap<i64, SemaphoreInfo> { &self.pending }
    pub fn upsert_semaphore_info(&mut self, info: SemaphoreInfo) {
        self.pending.insert(info.initiated_id, info.clone()); self.updates.insert(info.initiated_id, info);
    }
    pub fn delete_semaphore_info<S: SemaphoreExecution>(&mut self, execution: &mut S, id: i64) -> Result<(), S::Error> {
        if self.pending.remove(&id).is_none() { execution.log_missing_hold(id); }
        self.updates.remove(&id); self.deletes.insert(id); Ok(())
    }
    pub fn add_semaphore_acquire_initiated_event<S: SemaphoreExecution>(&mut self, execution: &mut S, decision_completed_event_id: i64, name: String, wait_timeout_seconds: i32) -> Result<(SemaphoreEvent, SemaphoreInfo), S::Error> {
        const OP: &str = "SemaphoreAcquireInitiated";
        execution.check_mutability(OP)?;
        if self.get_semaphore_info_by_name(&name).is_some() { return Err(execution.caller_error(OP)); }
        let event = execution.add_semaphore_event(SemaphoreEventAttributes::AcquireInitiated { decision_completed_event_id, semaphore_name: name, wait_timeout_seconds });
        let info = self.replicate_semaphore_acquire_initiated_event(execution, &event)?;
        Ok((event, info))
    }
    pub fn replicate_semaphore_acquire_initiated_event<S: SemaphoreExecution>(&mut self, execution: &S, event: &SemaphoreEvent) -> Result<SemaphoreInfo, S::Error> {
        let SemaphoreEventAttributes::AcquireInitiated { semaphore_name, wait_timeout_seconds, .. } = &event.attributes else { return Err(execution.internal_error("SemaphoreAcquireInitiated")); };
        let info = SemaphoreInfo { version: event.version, initiated_id: event.id, semaphore_name: semaphore_name.clone(),
            owner_id: format!("{}:{}:{}:{}", execution.workflow_id().len(), execution.workflow_id(), execution.run_id(), event.id),
            acquire_deadline_unix_nanos: event.timestamp_unix_nanos + i64::from(*wait_timeout_seconds) * 1_000_000_000, token_id: 0 };
        self.upsert_semaphore_info(info.clone()); Ok(info)
    }
    pub fn add_semaphore_acquired_event<S: SemaphoreExecution>(&mut self, execution: &mut S, initiated_event_id: i64, token_id: i32) -> Result<SemaphoreEvent, S::Error> {
        const OP: &str = "SemaphoreAcquired";
        execution.check_mutability(OP)?;
        if self.get_semaphore_info(initiated_event_id).is_none_or(|info| info.token_id == token_id) { return Err(execution.internal_error(OP)); }
        let event = execution.add_semaphore_event(SemaphoreEventAttributes::Acquired { initiated_event_id, token_id });
        self.replicate_semaphore_acquired_event(execution, &event)?; Ok(event)
    }
    pub fn replicate_semaphore_acquired_event<S: SemaphoreExecution>(&mut self, execution: &S, event: &SemaphoreEvent) -> Result<(), S::Error> {
        let SemaphoreEventAttributes::Acquired { initiated_event_id, token_id } = event.attributes else { return Err(execution.internal_error("SemaphoreAcquired")); };
        let mut info = self.get_semaphore_info(initiated_event_id).cloned().ok_or_else(|| execution.missing_semaphore_info())?;
        info.version = event.version; info.token_id = token_id; self.upsert_semaphore_info(info); Ok(())
    }
    pub fn replicate_semaphore_released_event<S: SemaphoreExecution>(&mut self, execution: &mut S, event: &SemaphoreEvent) -> Result<(), S::Error> {
        let SemaphoreEventAttributes::Released { initiated_event_id } = event.attributes else { return Err(execution.internal_error("SemaphoreReleased")); };
        self.delete_semaphore_info(execution, initiated_event_id)
    }
}
