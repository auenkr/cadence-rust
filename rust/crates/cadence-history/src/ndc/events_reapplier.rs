//! Port of Go `ndc/events_reapplier.go`.
use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;

use super::{NdcError, NdcMutableState, NdcResult};

/// Go `EventsReapplier`.
pub trait EventsReapplier {
    fn reapply_events(
        &self,
        ms_builder: &mut dyn NdcMutableState,
        history_events: &[HistoryEvent],
        run_id: &str,
    ) -> NdcResult<Vec<HistoryEvent>>;
}

/// Go `eventsReapplierImpl` (metrics/logging omitted: no behavior).
#[derive(Debug, Default, Clone)]
pub struct EventsReapplierImpl;

impl EventsReapplierImpl {
    pub fn new() -> Self {
        Self
    }
}

impl EventsReapplier for EventsReapplierImpl {
    fn reapply_events(
        &self,
        ms_builder: &mut dyn NdcMutableState,
        history_events: &[HistoryEvent],
        run_id: &str,
    ) -> NdcResult<Vec<HistoryEvent>> {
        let mut reapplied: Vec<HistoryEvent> = Vec::new();
        for event in history_events {
            if event.event_type == Some(EventType::WorkflowExecutionSignaled) {
                if ms_builder.is_event_reapplied(run_id, event.event_id, event.version)? {
                    continue;
                }
                reapplied.push(event.clone());
            }
        }
        if reapplied.is_empty() {
            return Ok(Vec::new());
        }
        if !ms_builder.is_workflow_execution_running() {
            return Err(NdcError::internal(
                "unable to reapply events to closed workflow.",
            ));
        }
        for event in &reapplied {
            let signal = event.workflow_execution_signaled_event_attributes.clone().unwrap_or_default();
            // Do not set requestID for requests reapplied, because they have already been applied previously
            ms_builder.add_workflow_execution_signaled(
                &signal.signal_name,
                &signal.input,
                &signal.identity,
                "",
            )?;
            ms_builder.update_event_reapplied(run_id, event.event_id, event.version)?;
        }
        Ok(reapplied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ndc::test_support::MockMs;
    use cadence_api::service_types::WorkflowExecutionSignaledEventAttributes;

    const RUN: &str = "run-1";

    fn signal(id: i64) -> HistoryEvent {
        HistoryEvent {
            event_id: id,
            event_type: Some(EventType::WorkflowExecutionSignaled),
            workflow_execution_signaled_event_attributes: Some(WorkflowExecutionSignaledEventAttributes {
                identity: "test".into(),
                signal_name: "signal".into(),
                input: Vec::new(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn started() -> HistoryEvent {
        HistoryEvent { event_type: Some(EventType::WorkflowExecutionStarted), ..Default::default() }
    }

    /// Go `TestReapplyEvents_AppliedEvent`.
    #[test]
    fn reapply_events_applied_event() {
        let mut ms = MockMs { running: true, ..Default::default() };
        let events = vec![started(), signal(1)];
        let applied = EventsReapplierImpl::new().reapply_events(&mut ms, &events, RUN).unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(ms.signaled, vec![("signal".into(), Vec::new(), "test".into(), String::new())]);
        assert_eq!(ms.updated_dedup, vec![(RUN.to_string(), 1, 0)]);
    }

    /// Go `TestReapplyEvents_Noop`: duplicated event, running state never consulted.
    #[test]
    fn reapply_events_noop() {
        let mut ms = MockMs::default();
        ms.duplicated.insert((RUN.into(), 1, 0), true);
        let events = vec![started(), signal(1)];
        let applied = EventsReapplierImpl::new().reapply_events(&mut ms, &events, RUN).unwrap();
        assert!(applied.is_empty());
        assert!(ms.signaled.is_empty());
        assert_eq!(ms.is_running_calls.get(), 0);
    }

    /// Go `TestReapplyEvents_PartialAppliedEvent`.
    #[test]
    fn reapply_events_partial_applied_event() {
        let mut ms = MockMs { running: true, ..Default::default() };
        ms.duplicated.insert((RUN.into(), 2, 0), true);
        let events = vec![started(), signal(1), signal(2)];
        let applied = EventsReapplierImpl::new().reapply_events(&mut ms, &events, RUN).unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].event_id, 1);
        assert_eq!(ms.signaled.len(), 1);
        assert_eq!(ms.updated_dedup, vec![(RUN.to_string(), 1, 0)]);
    }

    /// Go `TestReapplyEvents_Error`: signal add fails, nothing is recorded as reapplied.
    #[test]
    fn reapply_events_error() {
        let mut ms = MockMs { running: true, fail_add_signaled: true, ..Default::default() };
        let events = vec![started(), signal(1)];
        let res = EventsReapplierImpl::new().reapply_events(&mut ms, &events, RUN);
        assert!(res.is_err());
        assert!(ms.updated_dedup.is_empty());
    }

    /// Go (impl): closed workflow with reapplicable events is an internal error.
    #[test]
    fn reapply_events_closed_workflow() {
        let mut ms = MockMs { running: false, ..Default::default() };
        let res = EventsReapplierImpl::new().reapply_events(&mut ms, &[signal(1)], RUN);
        assert!(matches!(res, Err(NdcError::Internal(_))));
    }
}
