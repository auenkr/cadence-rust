//! group: assembly/engine_semaphore
//!
//! Engine-level semaphore hold transitions (Go `mutable_state_builder_methods_semaphore.go`):
//! `AddSemaphoreAcquireInitiatedEvent` + `ReplicateSemaphoreAcquireInitiatedEvent`, running the
//! library [`SemaphoreState`] over the engine `Inner` so the hold is flushed through
//! `upsert_semaphore_infos` / `delete_semaphore_infos`.

use cadence_api::enums::EventType;
use cadence_api::tasks::SemaphoreAcquireInitiatedEventAttributes;
use cadence_persistence::history_contract as pc;

use crate::engine::deps::WORKFLOW_STATE_COMPLETED;
use crate::execution::semaphore::{
    SemaphoreEvent, SemaphoreEventAttributes, SemaphoreExecution, SemaphoreInfo,
};
use crate::wiring::assembly::engine_state::{EngineMutableState, Inner};

/// Failure of a semaphore transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemaphoreOpError {
    /// Go `createCallerError` (`BadRequestError`): the run already holds / waits for the semaphore.
    Caller(String),
    /// Go `createInternalServerError` / `ErrMissingSemaphoreInfo`.
    Internal(String),
}

struct Exec<'a> {
    g: &'a mut Inner,
    now_ms: i64,
}

impl SemaphoreExecution for Exec<'_> {
    type Error = SemaphoreOpError;

    fn check_mutability(&self, operation: &'static str) -> Result<(), SemaphoreOpError> {
        if self.g.exec.state == WORKFLOW_STATE_COMPLETED {
            return Err(SemaphoreOpError::Internal(format!("{operation}: workflow execution is not mutable")));
        }
        Ok(())
    }
    fn workflow_id(&self) -> &str {
        &self.g.exec.workflow_id
    }
    fn run_id(&self) -> &str {
        &self.g.exec.run_id
    }
    fn add_semaphore_event(&mut self, attributes: SemaphoreEventAttributes) -> SemaphoreEvent {
        let SemaphoreEventAttributes::AcquireInitiated { decision_completed_event_id, semaphore_name, wait_timeout_seconds } =
            attributes.clone()
        else {
            unreachable!("engine only adds SemaphoreAcquireInitiated events");
        };
        let ev = self.g.push_event(EventType::SemaphoreAcquireInitiated, self.now_ms * 1_000_000, |e| {
            e.semaphore_acquire_initiated_event_attributes = Some(SemaphoreAcquireInitiatedEventAttributes {
                semaphore_name,
                wait_timeout_seconds: Some(wait_timeout_seconds),
                decision_task_completed_event_id: decision_completed_event_id,
            });
        });
        SemaphoreEvent {
            id: ev.event_id,
            version: ev.version,
            timestamp_unix_nanos: self.now_ms * 1_000_000,
            attributes,
        }
    }
    fn caller_error(&self, operation: &'static str) -> SemaphoreOpError {
        SemaphoreOpError::Caller(format!("{operation}: invalid history action"))
    }
    fn internal_error(&self, operation: &'static str) -> SemaphoreOpError {
        SemaphoreOpError::Internal(format!("{operation}: internal error"))
    }
    fn missing_semaphore_info(&self) -> SemaphoreOpError {
        SemaphoreOpError::Internal("unable to get semaphore info".into())
    }
    fn log_missing_hold(&mut self, initiated_id: i64) {
        tracing::error!(initiated_id, "unable to find semaphore hold event ID in mutable state");
        self.g.log_data_inconsistency();
    }
}

impl EngineMutableState {
    /// Go `AddSemaphoreAcquireInitiatedEvent`.
    pub fn add_semaphore_acquire_initiated(
        &self,
        decision_completed_event_id: i64,
        name: &str,
        wait_timeout_seconds: i32,
    ) -> Result<SemaphoreInfo, SemaphoreOpError> {
        let now_ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let mut sems = std::mem::take(&mut g.semaphores);
        let res = {
            let mut exec = Exec { g: &mut g, now_ms };
            sems.add_semaphore_acquire_initiated_event(
                &mut exec,
                decision_completed_event_id,
                name.to_string(),
                wait_timeout_seconds,
            )
        };
        g.semaphores = sems;
        res.map(|(_, info)| info)
    }

    /// Whether the run holds or waits for `name` (Go `GetSemaphoreInfoByName`).
    pub fn has_semaphore_named(&self, name: &str) -> bool {
        self.inner.lock().unwrap().semaphores.get_semaphore_info_by_name(name).is_some()
    }
}

/// `SemaphoreInfoRecord` <-> library `SemaphoreInfo`.
pub(super) fn to_record(i: &SemaphoreInfo) -> pc::SemaphoreInfoRecord {
    pc::SemaphoreInfoRecord {
        version: i.version,
        initiated_id: i.initiated_id,
        semaphore_name: i.semaphore_name.clone(),
        owner_id: i.owner_id.clone(),
        token_id: i.token_id,
        acquire_deadline_unix_nanos: i.acquire_deadline_unix_nanos,
    }
}

pub(super) fn from_record(r: &pc::SemaphoreInfoRecord) -> SemaphoreInfo {
    SemaphoreInfo {
        version: r.version,
        initiated_id: r.initiated_id,
        semaphore_name: r.semaphore_name.clone(),
        owner_id: r.owner_id.clone(),
        acquire_deadline_unix_nanos: r.acquire_deadline_unix_nanos,
        token_id: r.token_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trip() {
        let i = SemaphoreInfo {
            version: 3,
            initiated_id: 7,
            semaphore_name: "s".into(),
            owner_id: "o".into(),
            acquire_deadline_unix_nanos: 9,
            token_id: 2,
        };
        assert_eq!(from_record(&to_record(&i)), i);
    }
}

#[cfg(test)]
mod decision_tests {
    use super::super::engine_decision_ops::{resolve_semaphore_lookups, SemaphoreLookup};
    use async_trait::async_trait;
    use cadence_api::enums::DecisionType;
    use cadence_api::tasks::{AcquireSemaphoreDecisionAttributes, Decision};
    use cadence_persistence::semaphores::{AcquireError, SemaphoreError, SemaphoreMetadata};

    #[derive(Debug)]
    struct Store;
    #[async_trait]
    impl cadence_persistence::SemaphorePersistence for Store {
        async fn create_semaphore(&self, _: &str, _: &str, _: i32, _: i32) -> Result<SemaphoreMetadata, SemaphoreError> {
            unimplemented!()
        }
        async fn get_semaphore_metadata(&self, _: &str, name: &str) -> Result<SemaphoreMetadata, AcquireError> {
            match name {
                "ok" => Ok(SemaphoreMetadata { size: 1, bucket_size: 1 }),
                "missing" => Err(AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new("nope"))),
                _ => Err(AcquireError::Internal("boom".into())),
            }
        }
        async fn scan_semaphore_bucket(
            &self, _: &str, _: &str, _: i32, _: usize, _: Option<&[u8]>,
        ) -> Result<cadence_persistence::semaphores::SemaphoreBucketPage, AcquireError> {
            unimplemented!()
        }
        async fn get_semaphore_ownership_by_token(&self, _: &str, _: &str, _: i32, _: i32) -> Result<String, AcquireError> {
            unimplemented!()
        }
        async fn grant_semaphore_token(
            &self, _: &str, _: &str, _: i32, _: i32, _: &str,
        ) -> Result<cadence_persistence::semaphores::GrantOutcome, AcquireError> {
            unimplemented!()
        }
    }

    fn acquire(name: &str) -> Decision {
        Decision {
            decision_type: Some(DecisionType::AcquireSemaphore),
            acquire_semaphore_decision_attributes: Some(AcquireSemaphoreDecisionAttributes {
                semaphore_name: name.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn lookups_map_found_missing_and_failed() {
        let ds = vec![acquire("ok"), acquire("missing"), acquire("other"), acquire("ok")];
        let m = resolve_semaphore_lookups(&Store, "d", &ds).await;
        assert_eq!(m.len(), 3);
        assert_eq!(m["ok"], SemaphoreLookup::Found);
        assert!(matches!(m["missing"], SemaphoreLookup::NotFound(_)));
        assert!(matches!(m["other"], SemaphoreLookup::Failed(_)));
    }
}
