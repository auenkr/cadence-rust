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
