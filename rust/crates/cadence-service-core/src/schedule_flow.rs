//! Schedule APIs implemented on top of the scheduler workflow, mirroring
//! `go/service/frontend/api/handler_schedule.go`: Frontend starts, signals,
//! queries and lists the `cadence-scheduler` workflow that the Go Worker runs.

#![allow(clippy::result_large_err)]

use std::time::Duration;

use cadence_api::{
    enums::{QueryRejectCondition, WorkflowExecutionCloseStatus, WorkflowIDReusePolicy},
    errors::{
        BadRequestError, CadenceError, EntityNotExistsError, InternalServiceError, QueryFailedError,
    },
    schedule::{CreateScheduleRequest, DescribeScheduleResponse, ScheduleListEntry, ScheduleState},
    service_types::WorkflowExecutionInfo,
    types::{
        SearchAttributes, SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest, TaskList,
        TerminateWorkflowExecutionRequest, WorkflowExecution, WorkflowType,
    },
};
use cadence_persistence::{InMemoryExecutionStore, QueryError, StartError, WorkflowMutationError};

use crate::scheduler_wire as wire;

pub const WORKFLOW_ID_PREFIX: &str = "cadence-scheduler:";
pub const DEFAULT_LIST_PAGE_SIZE: usize = 10;
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

const SA_SCHEDULE_ID: &str = "CadenceScheduleID";
const SA_SCHEDULE_TIME: &str = "CadenceScheduleTime";
const SA_IS_BACKFILL: &str = "CadenceScheduleIsBackfill";
const SA_BACKFILL_ID: &str = "CadenceScheduleBackfillID";
const SA_STATE: &str = "CadenceScheduleState";
const SA_CRON: &str = "CadenceScheduleCron";
const SA_WORKFLOW_TYPE: &str = "CadenceScheduleWorkflowType";
const INTERNAL_SA_KEYS: [&str; 7] = [
    SA_SCHEDULE_ID,
    SA_SCHEDULE_TIME,
    SA_IS_BACKFILL,
    SA_BACKFILL_ID,
    SA_STATE,
    SA_CRON,
    SA_WORKFLOW_TYPE,
];

#[must_use]
pub fn workflow_id(schedule_id: &str) -> String {
    format!("{WORKFLOW_ID_PREFIX}{schedule_id}")
}

#[must_use]
pub fn resolve_backfill_id(client_id: &str) -> String {
    let trimmed = client_id.trim();
    if trimmed.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        trimmed.to_string()
    }
}

fn mutation_error(error: WorkflowMutationError) -> CadenceError {
    match error {
        WorkflowMutationError::NotFound(e) => CadenceError::EntityNotExists(e),
        WorkflowMutationError::AlreadyCompleted(e) => CadenceError::WorkflowAlreadyCompleted(e),
    }
}

/// Go `CreateSchedule` tail: start the scheduler workflow.
pub fn start_scheduler(
    store: &InMemoryExecutionStore,
    request: &CreateScheduleRequest,
) -> Result<String, CadenceError> {
    let start = StartWorkflowExecutionRequest {
        domain: request.domain.clone(),
        workflow_id: workflow_id(&request.schedule_id),
        workflow_type: Some(WorkflowType {
            name: wire::WORKFLOW_TYPE_NAME.to_string(),
        }),
        task_list: Some(TaskList {
            name: wire::TASK_LIST_NAME.to_string(),
            ..Default::default()
        }),
        input: wire::scheduler_input(request),
        execution_start_to_close_timeout_seconds: Some(wire::EXECUTION_TIMEOUT_SECONDS),
        task_start_to_close_timeout_seconds: Some(wire::DECISION_TIMEOUT_SECONDS),
        request_id: uuid::Uuid::new_v4().to_string(),
        workflow_id_reuse_policy: Some(WorkflowIDReusePolicy::AllowDuplicate),
        memo: request.memo.clone(),
        search_attributes: request.search_attributes.clone(),
        ..Default::default()
    };
    match store.start(&start) {
        Ok(_) => Ok(request.schedule_id.clone()),
        Err(StartError::AlreadyStarted(_)) => {
            Err(CadenceError::BadRequest(BadRequestError::new(format!(
                "schedule {:?} already exists in domain {:?}",
                request.schedule_id, request.domain
            ))))
        }
        Err(StartError::InvalidArgument(message)) => {
            Err(CadenceError::BadRequest(BadRequestError::new(message)))
        }
    }
}

/// Go `signalScheduleWorkflow`: signal and normalize entity-not-exists.
pub fn signal_scheduler(
    store: &InMemoryExecutionStore,
    domain: &str,
    schedule_id: &str,
    signal_name: &str,
    input: Vec<u8>,
) -> Result<(), CadenceError> {
    let request = SignalWorkflowExecutionRequest {
        domain: domain.to_string(),
        workflow_execution: Some(WorkflowExecution::new(
            workflow_id(schedule_id),
            String::new(),
        )),
        signal_name: signal_name.to_string(),
        input,
        request_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    };
    store
        .signal(&request)
        .map_err(|e| wire::normalize_schedule_error(mutation_error(e), schedule_id, domain))
}

/// Go `DeleteSchedule`: signal delete, falling back to terminate.
pub fn delete_scheduler(
    store: &InMemoryExecutionStore,
    domain: &str,
    schedule_id: &str,
) -> Result<(), CadenceError> {
    let signal_error =
        match signal_scheduler(store, domain, schedule_id, wire::SIGNAL_DELETE, Vec::new()) {
            Ok(()) => return Ok(()),
            Err(e) if wire::is_schedule_already_gone(&e) => return Ok(()),
            Err(e) => e,
        };
    if matches!(
        signal_error,
        CadenceError::ServiceBusy(_) | CadenceError::Internal(_)
    ) {
        // Transient failures: the client retries instead of terminating.
        return Err(signal_error);
    }
    let terminate = TerminateWorkflowExecutionRequest {
        domain: domain.to_string(),
        workflow_execution: Some(WorkflowExecution::new(
            workflow_id(schedule_id),
            String::new(),
        )),
        reason: "terminated by DeleteSchedule".to_string(),
        ..Default::default()
    };
    match store.terminate(&terminate).map_err(mutation_error) {
        Ok(()) => Ok(()),
        Err(e) if wire::is_schedule_already_gone(&e) => Ok(()),
        Err(_) => Err(signal_error),
    }
}

#[allow(clippy::large_enum_variant)]
enum DescribeOnce {
    Done(DescribeScheduleResponse),
    Retry(CadenceError),
    Fail(CadenceError),
}

fn closed_outcome(
    status: Option<WorkflowExecutionCloseStatus>,
    schedule_id: &str,
    domain: &str,
) -> DescribeOnce {
    let name = status.map_or_else(|| "UNKNOWN".to_string(), |s| s.to_string());
    let error = wire::not_operational_error(schedule_id, domain, &name);
    if status == Some(WorkflowExecutionCloseStatus::ContinuedAsNew) {
        DescribeOnce::Retry(error)
    } else {
        DescribeOnce::Fail(error)
    }
}

async fn describe_once(
    store: &InMemoryExecutionStore,
    domain: &str,
    schedule_id: &str,
) -> DescribeOnce {
    let execution = WorkflowExecution::new(workflow_id(schedule_id), String::new());
    match store.describe(domain, &execution) {
        Ok(resp) => {
            if let Some(info) = resp.workflow_execution_info.as_ref() {
                if info.close_status.is_some() {
                    return closed_outcome(info.close_status, schedule_id, domain);
                }
            }
        }
        Err(e) => {
            return DescribeOnce::Fail(wire::normalize_schedule_error(
                CadenceError::EntityNotExists(e),
                schedule_id,
                domain,
            ))
        }
    }
    let query_id = match store.buffer_query(
        domain,
        &execution,
        wire::QUERY_DESCRIBE,
        Vec::new(),
        Some(QueryRejectCondition::NotCompletedCleanly),
    ) {
        Ok(id) => id,
        Err(QueryError::NotFound(e)) => {
            return DescribeOnce::Fail(wire::normalize_schedule_error(
                CadenceError::EntityNotExists(e),
                schedule_id,
                domain,
            ))
        }
        Err(QueryError::Invalid(e)) => return DescribeOnce::Fail(CadenceError::BadRequest(e)),
        Err(QueryError::Rejected(status)) => return closed_outcome(status, schedule_id, domain),
    };
    let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
    loop {
        if let Some(result) = store.take_query_result(&query_id) {
            return match result.result_type {
                Some(cadence_api::enums::QueryResultType::Answered) => {
                    match wire::describe_response(&result.answer) {
                        Ok(resp) => DescribeOnce::Done(resp),
                        Err(e) => DescribeOnce::Fail(e),
                    }
                }
                _ => {
                    let message = if result.error_message.is_empty() {
                        "Query failed.".to_string()
                    } else {
                        result.error_message
                    };
                    let retryable = message.contains("decision task");
                    let error = CadenceError::QueryFailed(QueryFailedError::new(message));
                    if retryable {
                        DescribeOnce::Retry(error)
                    } else {
                        DescribeOnce::Fail(error)
                    }
                }
            };
        }
        if tokio::time::Instant::now() >= deadline {
            return DescribeOnce::Fail(CadenceError::QueryFailed(QueryFailedError::new(
                "Query timed out.",
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Go `DescribeSchedule`: probe + query with bounded retries.
pub async fn describe_scheduler(
    store: &InMemoryExecutionStore,
    domain: &str,
    schedule_id: &str,
) -> Result<DescribeScheduleResponse, CadenceError> {
    let mut last = None;
    for attempt in 0..wire::DESCRIBE_RETRY_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(wire::DESCRIBE_RETRY_INTERVAL_SECS)).await;
        }
        match describe_once(store, domain, schedule_id).await {
            DescribeOnce::Done(resp) => return Ok(resp),
            DescribeOnce::Fail(e) => return Err(e),
            DescribeOnce::Retry(e) => last = Some(e),
        }
    }
    if let Some(CadenceError::Internal(_)) = last {
        return Err(CadenceError::Internal(InternalServiceError::new(format!(
            "schedule {schedule_id:?} in domain {domain:?} is not queryable: scheduler workflow still mid-ContinueAsNew after {} attempts",
            wire::DESCRIBE_RETRY_ATTEMPTS
        ))));
    }
    Err(last.unwrap_or_else(|| {
        CadenceError::EntityNotExists(EntityNotExistsError::new(format!(
            "schedule {schedule_id:?} not found in domain {domain:?}"
        )))
    }))
}

fn sa_string(sa: &SearchAttributes, key: &str) -> Option<String> {
    let bytes = sa.indexed_fields.as_ref()?.get(key)?;
    serde_json::from_slice::<String>(bytes).ok()
}

fn filter_internal(sa: Option<&SearchAttributes>) -> Option<SearchAttributes> {
    let fields = sa?.indexed_fields.as_ref()?;
    let filtered: std::collections::HashMap<String, Vec<u8>> = fields
        .iter()
        .filter(|(k, _)| !INTERNAL_SA_KEYS.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if filtered.is_empty() {
        None
    } else {
        Some(SearchAttributes {
            indexed_fields: Some(filtered),
        })
    }
}

fn list_entry(info: &WorkflowExecutionInfo) -> Option<ScheduleListEntry> {
    let wf_id = info.execution.as_ref()?.workflow_id.as_str();
    let schedule_id = wf_id.strip_prefix(WORKFLOW_ID_PREFIX)?.to_string();
    let mut paused = false;
    let mut cron = String::new();
    let mut type_name = String::new();
    if let Some(sa) = info.search_attributes.as_ref() {
        if let Some(state) = sa_string(sa, SA_STATE) {
            paused = state == "paused";
        }
        cron = sa_string(sa, SA_CRON).unwrap_or_default();
        type_name = sa_string(sa, SA_WORKFLOW_TYPE).unwrap_or_default();
    }
    Some(ScheduleListEntry {
        schedule_id,
        workflow_type: (!type_name.is_empty()).then_some(WorkflowType { name: type_name }),
        state: Some(ScheduleState {
            paused,
            pause_info: None,
        }),
        cron_expression: cron,
        memo: info.memo.clone(),
        search_attributes: filter_internal(info.search_attributes.as_ref()),
    })
}

/// Go `ListSchedules`: open scheduler workflows, newest first, offset token.
#[must_use]
pub fn list_schedulers(
    store: &InMemoryExecutionStore,
    domain: &str,
    page_size: usize,
    page_token: &[u8],
) -> (Vec<ScheduleListEntry>, Vec<u8>) {
    let mut infos = store.list_filtered(domain, true, None, Some(wire::WORKFLOW_TYPE_NAME), None);
    infos.reverse();
    let offset: usize = std::str::from_utf8(page_token)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let total = infos.len();
    let page: Vec<&WorkflowExecutionInfo> =
        infos.iter().skip(offset).take(page_size.max(1)).collect();
    let next = offset + page.len();
    let entries = page.into_iter().filter_map(list_entry).collect();
    let token = if next < total {
        next.to_string().into_bytes()
    } else {
        Vec::new()
    };
    (entries, token)
}
