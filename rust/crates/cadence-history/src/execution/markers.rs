//! Marker and search-attribute event application.

use cadence_api::{service_types::HistoryEvent, tasks::{MarkerRecordedEventAttributes, UpsertWorkflowSearchAttributesEventAttributes}};
use cadence_api::types::EventType;
use crate::mutable_state::MutableStateBuilder;

impl MutableStateBuilder {
    pub fn add_marker_recorded_event(&mut self, attributes: MarkerRecordedEventAttributes, timestamp_unix_nanos: i64, version: i64) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(EventType::MarkerRecorded), timestamp: Some(timestamp_unix_nanos), version, marker_recorded_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn replicate_marker_recorded_event(&mut self, event: &HistoryEvent) -> bool {
        event.marker_recorded_event_attributes.is_some()
    }
    pub fn add_upsert_workflow_search_attributes_event(&mut self, attributes: UpsertWorkflowSearchAttributesEventAttributes, timestamp_unix_nanos: i64, version: i64) -> HistoryEvent {
        let event = self.add_event(HistoryEvent { event_type: Some(EventType::UpsertWorkflowSearchAttributes), timestamp: Some(timestamp_unix_nanos), version, upsert_workflow_search_attributes_event_attributes: Some(attributes.clone()), ..Default::default() });
        self.apply_upsert_search_attributes(&attributes);
        event
    }
    pub fn replicate_upsert_workflow_search_attributes_event(&mut self, event: &HistoryEvent) -> bool {
        let Some(attrs) = event.upsert_workflow_search_attributes_event_attributes.as_ref() else { return false; };
        self.apply_upsert_search_attributes(attrs);
        true
    }
    fn apply_upsert_search_attributes(&mut self, attrs: &UpsertWorkflowSearchAttributesEventAttributes) {
        let values = serde_json::to_value(&attrs.search_attributes).unwrap_or(serde_json::Value::Null);
        self.execution_info["search_attributes"] = values;
    }
}
