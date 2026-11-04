//! Port of `go/service/history/queue/processing_queue_state.go`.
use super::deps::TaskKey;
use super::domain_filter::DomainFilter;

/// Go `ProcessingQueueState` interface collapsed to its only implementation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessingQueueState {
    pub level: i32,
    pub ack_level: TaskKey,
    pub read_level: TaskKey,
    pub max_level: TaskKey,
    pub domain_filter: DomainFilter,
}

impl ProcessingQueueState {
    /// Go `NewProcessingQueueState` (read level starts at ack level).
    pub fn new(level: i32, ack_level: TaskKey, max_level: TaskKey, domain_filter: DomainFilter) -> Self {
        Self::new_full(level, ack_level, ack_level, max_level, domain_filter)
    }

    /// Go `newProcessingQueueState`.
    pub fn new_full(
        level: i32,
        ack_level: TaskKey,
        read_level: TaskKey,
        max_level: TaskKey,
        domain_filter: DomainFilter,
    ) -> Self {
        ProcessingQueueState { level, ack_level, read_level, max_level, domain_filter }
    }

    pub fn level(&self) -> i32 {
        self.level
    }
    pub fn ack_level(&self) -> TaskKey {
        self.ack_level
    }
    pub fn read_level(&self) -> TaskKey {
        self.read_level
    }
    pub fn max_level(&self) -> TaskKey {
        self.max_level
    }
    pub fn domain_filter(&self) -> &DomainFilter {
        &self.domain_filter
    }
}
