//! In-memory history event builder from `history_builder.go`.

use cadence_api::service_types::HistoryEvent;

#[derive(Debug, Clone, PartialEq)]
pub struct HistoryBuilder {
    next_event_id: i64,
    version: i64,
    timestamp_unix_nanos: i64,
    events: Vec<HistoryEvent>,
}

impl HistoryBuilder {
    pub fn new(next_event_id: i64, version: i64, timestamp_unix_nanos: i64) -> Self {
        Self { next_event_id, version, timestamp_unix_nanos, events: Vec::new() }
    }
    pub fn next_event_id(&self) -> i64 { self.next_event_id }
    pub fn set_time_and_version(&mut self, timestamp_unix_nanos: i64, version: i64) { self.timestamp_unix_nanos = timestamp_unix_nanos; self.version = version; }
    pub fn add_event(&mut self, mut event: HistoryEvent) -> HistoryEvent {
        event.event_id = self.next_event_id;
        event.timestamp.get_or_insert(self.timestamp_unix_nanos);
        event.version = self.version;
        self.next_event_id += 1;
        self.events.push(event.clone());
        event
    }
    pub fn events(&self) -> &[HistoryEvent] { &self.events }
    pub fn take_events(&mut self) -> Vec<HistoryEvent> { std::mem::take(&mut self.events) }
}
