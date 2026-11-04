//! Port of Go `ndc/replication_task.go`.
use cadence_api::enums::{DecisionTaskFailedCause, EventType};
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::{DataBlob, VersionHistory, WorkflowExecution};
use cadence_persistence::version_history::EMPTY_VERSION;

use super::{ReplicateEventsV2Request, NdcClusterMetadata, NdcError, NdcResult};

pub fn err_invalid_domain_id() -> NdcError { NdcError::bad_request("invalid domain ID") }
pub fn err_invalid_execution() -> NdcError { NdcError::bad_request("invalid execution") }
pub fn err_invalid_run_id() -> NdcError { NdcError::bad_request("invalid run ID") }
pub fn err_event_id_mismatch() -> NdcError { NdcError::bad_request("event ID mismatch") }
pub fn err_event_version_mismatch() -> NdcError { NdcError::bad_request("event version mismatch") }
pub fn err_no_new_run_history() -> NdcError { NdcError::bad_request("no new run history events") }
pub fn err_last_event_is_not_continue_as_new() -> NdcError {
    NdcError::bad_request("last event is not continue as new")
}
pub fn err_empty_history_raw_event_batch() -> NdcError {
    NdcError::bad_request("encounter empty history batch")
}

/// Go `persistence.PayloadSerializer.DeserializeBatchEvents` (ThriftRW encoding).
pub trait HistoryBatchDeserializer {
    fn deserialize_batch_events(&self, data: &[u8]) -> NdcResult<Vec<HistoryEvent>>;
}

/// Go `replicationTask` (interface + impl collapsed into one struct).
#[derive(Debug, Clone)]
pub struct ReplicationTask {
    pub source_cluster: String,
    pub domain_id: String,
    pub execution: WorkflowExecution,
    pub version: i64,
    pub first_event: HistoryEvent,
    pub last_event: HistoryEvent,
    pub event_time_nanos: i64,
    pub events: Vec<HistoryEvent>,
    pub new_events: Vec<HistoryEvent>,
    pub version_history: VersionHistory,
    pub start_time_nanos: i64,
}

impl ReplicationTask {
    /// Go `newReplicationTask`.
    pub fn new(
        cluster_metadata: &dyn NdcClusterMetadata,
        serializer: &dyn HistoryBatchDeserializer,
        task_start_time_nanos: i64,
        request: &ReplicateEventsV2Request,
    ) -> NdcResult<Self> {
        let (events, new_events) = validate_replicate_events_request(serializer, request)?;
        let domain_id = request.domain_uuid.clone();
        let execution = request.workflow_execution.clone().ok_or_else(err_invalid_execution)?;
        let version_history = VersionHistory {
            branch_token: Vec::new(),
            items: request.version_history_items.clone(),
        };
        let first_event = events[0].clone();
        let last_event = events[events.len() - 1].clone();
        let version = first_event.version;
        let source_cluster = cluster_metadata.cluster_name_for_failover_version(version)?;
        let mut event_time = 0i64;
        for e in events.iter().chain(new_events.iter()) {
            let ts = e.timestamp.unwrap_or(0);
            if ts > event_time {
                event_time = ts;
            }
        }
        Ok(Self {
            source_cluster,
            domain_id,
            execution,
            version,
            first_event,
            last_event,
            event_time_nanos: event_time,
            events,
            new_events,
            version_history,
            start_time_nanos: task_start_time_nanos,
        })
    }

    pub fn get_domain_id(&self) -> &str { &self.domain_id }
    pub fn get_execution(&self) -> &WorkflowExecution { &self.execution }
    pub fn get_workflow_id(&self) -> &str { &self.execution.workflow_id }
    pub fn get_run_id(&self) -> &str { &self.execution.run_id }
    pub fn get_event_time(&self) -> i64 { self.event_time_nanos }
    pub fn get_first_event(&self) -> &HistoryEvent { &self.first_event }
    pub fn get_last_event(&self) -> &HistoryEvent { &self.last_event }
    pub fn get_version(&self) -> i64 { self.version }
    pub fn get_source_cluster(&self) -> &str { &self.source_cluster }
    pub fn get_events(&self) -> &[HistoryEvent] { &self.events }
    pub fn get_new_events(&self) -> &[HistoryEvent] { &self.new_events }
    pub fn get_version_history(&self) -> &VersionHistory { &self.version_history }

    pub fn is_workflow_reset(&self) -> bool {
        let (base_run_id, new_run_id, base_event_version, is_reset) = self.get_workflow_reset_metadata();
        !base_run_id.is_empty() && base_event_version != EMPTY_VERSION && !new_run_id.is_empty() && is_reset
    }

    /// Returns `(baseRunID, newRunID, baseEventVersion, isReset)`.
    pub fn get_workflow_reset_metadata(&self) -> (String, String, i64, bool) {
        let mut base_run_id = String::new();
        let mut new_run_id = String::new();
        let mut base_event_version = EMPTY_VERSION;
        let mut is_reset = false;
        match self.first_event.event_type {
            Some(EventType::DecisionTaskFailed) => {
                if let Some(attr) = &self.first_event.decision_task_failed_event_attributes {
                    base_run_id = attr.base_run_id.clone();
                    base_event_version = attr.fork_event_version;
                    new_run_id = attr.new_run_id.clone();
                    if attr.cause == Some(DecisionTaskFailedCause::ResetWorkflow) {
                        is_reset = true;
                    }
                }
            }
            Some(EventType::DecisionTaskTimedOut) => {
                if let Some(attr) = &self.first_event.decision_task_timed_out_event_attributes {
                    base_run_id = attr.base_run_id.clone();
                    base_event_version = attr.fork_event_version;
                    new_run_id = attr.new_run_id.clone();
                    if attr.cause == Some(cadence_api::enums::DecisionTaskTimedOutCause::Reset) {
                        is_reset = true;
                    }
                }
            }
            _ => {}
        }
        (base_run_id, new_run_id, base_event_version, is_reset)
    }

    /// Go `splitTask`: returns `(self-with-new-events-cleared, newRunTask)`.
    pub fn split_task(&mut self, task_start_time_nanos: i64) -> NdcResult<ReplicationTask> {
        if self.new_events.is_empty() {
            return Err(err_no_new_run_history());
        }
        let new_history_events = self.new_events.clone();
        let attrs = match (&self.last_event.event_type, &self.last_event.workflow_execution_continued_as_new_event_attributes) {
            (Some(EventType::WorkflowExecutionContinuedAsNew), Some(a)) => a.clone(),
            _ => return Err(err_last_event_is_not_continue_as_new()),
        };
        let new_run_id = attrs.new_execution_run_id.clone();
        let new_first_event = new_history_events[0].clone();
        let new_last_event = new_history_events[new_history_events.len() - 1].clone();
        let mut new_event_time = 0i64;
        for e in &new_history_events {
            let ts = e.timestamp.unwrap_or(0);
            if ts > new_event_time {
                new_event_time = ts;
            }
        }
        let new_version_history = VersionHistory {
            branch_token: Vec::new(),
            items: vec![cadence_api::types::VersionHistoryItem {
                event_id: new_last_event.event_id,
                version: new_last_event.version,
            }],
        };
        let new_run_task = ReplicationTask {
            source_cluster: self.source_cluster.clone(),
            domain_id: self.domain_id.clone(),
            execution: WorkflowExecution {
                workflow_id: self.execution.workflow_id.clone(),
                run_id: new_run_id,
            },
            version: self.version,
            first_event: new_first_event,
            last_event: new_last_event,
            event_time_nanos: new_event_time,
            events: new_history_events,
            new_events: Vec::new(),
            version_history: new_version_history,
            start_time_nanos: task_start_time_nanos,
        };
        self.new_events = Vec::new();
        Ok(new_run_task)
    }
}

/// Go `validateReplicateEventsRequest`.
pub fn validate_replicate_events_request(
    serializer: &dyn HistoryBatchDeserializer,
    request: &ReplicateEventsV2Request,
) -> NdcResult<(Vec<HistoryEvent>, Vec<HistoryEvent>)> {
    if !validate_uuid(&request.domain_uuid) {
        return Err(err_invalid_domain_id());
    }
    let execution = request.workflow_execution.as_ref().ok_or_else(err_invalid_execution)?;
    if !validate_uuid(&execution.run_id) {
        return Err(err_invalid_run_id());
    }
    let events = deserialize_blob(serializer, request.events.as_ref())?;
    if events.is_empty() {
        return Err(err_empty_history_raw_event_batch());
    }
    let version = validate_events(&events)?;
    let new_blob = match request.new_run_events.as_ref() {
        None => return Ok((events, Vec::new())),
        Some(b) => b,
    };
    let new_run_events = deserialize_blob(serializer, Some(new_blob))?;
    let new_run_version = validate_events(&new_run_events)?;
    if version != new_run_version {
        return Err(err_event_version_mismatch());
    }
    Ok((events, new_run_events))
}

pub fn validate_uuid(input: &str) -> bool {
    uuid::Uuid::parse_str(input).is_ok()
}

/// Go `validateEvents`.
pub fn validate_events(events: &[HistoryEvent]) -> NdcResult<i64> {
    let first = &events[0];
    let first_event_id = first.event_id;
    let version = first.version;
    for (index, event) in events.iter().enumerate() {
        if event.event_id != first_event_id + index as i64 {
            return Err(err_event_id_mismatch());
        }
        if event.version != version {
            return Err(err_event_version_mismatch());
        }
    }
    Ok(version)
}

/// Go `deserializeBlob`.
pub fn deserialize_blob(
    serializer: &dyn HistoryBatchDeserializer,
    blob: Option<&DataBlob>,
) -> NdcResult<Vec<HistoryEvent>> {
    match blob {
        None => Ok(Vec::new()),
        Some(b) => serializer.deserialize_batch_events(&b.data),
    }
}
