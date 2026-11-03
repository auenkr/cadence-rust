//! Matching-, History-read- and task-list-backed operations of the production
//! `WorkflowBackend` (Go `WorkflowHandler` poll/respond/history paths).
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cadence_api::errors::{CadenceError, EntityNotExistsError};
use cadence_api::service_types as st;
use cadence_api::tasks as tk;
use cadence_api::types as ty;
use cadence_persistence::history_reader::{
    read_history_branch, HistoryNodeStore, HistoryReadError, ReadHistoryBranchRequest,
};
use cadence_thrift::value::{FromThrift, Struct, ToThrift, Value};

use crate::handler_workflow_history::{
    deserialize_history_token, serialize_history_token, validate_history_run_id,
    verify_history_is_complete, HistoryContinuationToken,
};
use crate::workflow_backend::{
    bad_request, domain_uuid, internal, to_json, typed, ProductionBackend,
};

const MATCHING_SERVICE: &str = "cadence-matching";
/// Go's matching client long-poll timeout headroom.
const POLL_TIMEOUT: Duration = Duration::from_secs(75);
/// Go `constants.GetHistoryMaxPageSize`.
const HISTORY_MAX_PAGE_SIZE: i32 = 1000;
const FIRST_EVENT_ID: i64 = 1;
const END_EVENT_ID: i64 = i64::MAX;

/// Persistence-backed history reader (Go `HistoryManager` on the Frontend).
#[derive(Clone, Debug)]
pub struct HistoryAccess {
    store: Arc<dyn HistoryNodeStore>,
    num_shards: i32,
}

/// Go `common.WorkflowIDToHistoryShard`.
pub fn workflow_id_to_history_shard(workflow_id: &str, num_shards: i32) -> i32 {
    let n = num_shards.max(1) as u32;
    (cadence_common::membership::fingerprint32(workflow_id.as_bytes()) % n) as i32
}

fn read_error(error: HistoryReadError) -> CadenceError {
    match error {
        HistoryReadError::EntityNotExists(m) => {
            CadenceError::EntityNotExists(EntityNotExistsError::new(m))
        }
        HistoryReadError::InvalidRequest(m) => bad_request(m),
        HistoryReadError::DataInconsistency(m) | HistoryReadError::Internal(m) => internal(m),
    }
}

#[derive(Clone, Debug)]
struct TransientDecision {
    scheduled: st::HistoryEvent,
    started: st::HistoryEvent,
}

impl TransientDecision {
    fn from_thrift(s: &Struct) -> Option<Self> {
        Some(Self {
            scheduled: s.opt_struct::<st::HistoryEvent>(10).ok().flatten()?,
            started: s.opt_struct::<st::HistoryEvent>(20).ok().flatten()?,
        })
    }

    fn to_value(&self) -> serde_json::Value {
        serde_json::json!({
            "ScheduledEvent": serde_json::to_value(&self.scheduled).unwrap_or_default(),
            "StartedEvent": serde_json::to_value(&self.started).unwrap_or_default(),
        })
    }

    fn from_value(value: &serde_json::Value) -> Option<Self> {
        Some(Self {
            scheduled: serde_json::from_value(value.get("ScheduledEvent")?.clone()).ok()?,
            started: serde_json::from_value(value.get("StartedEvent")?.clone()).ok()?,
        })
    }
}

impl HistoryAccess {
    pub fn new(store: Arc<dyn HistoryNodeStore>, num_shards: i32) -> Self {
        Self { store, num_shards }
    }

    /// Go `persistenceutils.ReadFullPageV2Events`.
    async fn read_full_page(
        &self,
        request: ReadHistoryBranchRequest,
    ) -> Result<(Vec<st::HistoryEvent>, Vec<u8>), CadenceError> {
        let mut request = request;
        let mut events = Vec::new();
        loop {
            let store = self.store.clone();
            let req = request.clone();
            let response =
                tokio::task::spawn_blocking(move || read_history_branch(store.as_ref(), &req))
                    .await
                    .map_err(|e| internal(format!("history read task failed: {e}")))?
                    .map_err(read_error)?;
            events.extend(response.events());
            if events.len() as i64 >= request.page_size || response.next_page_token.is_empty() {
                return Ok((events, response.next_page_token));
            }
            request.next_page_token = response.next_page_token;
        }
    }

    /// Go `WorkflowHandler.getHistory`.
    #[allow(clippy::too_many_arguments)]
    async fn get_history(
        &self,
        execution: &ty::WorkflowExecution,
        first_event_id: i64,
        next_event_id: i64,
        page_size: i32,
        next_page_token: Vec<u8>,
        transient: Option<&TransientDecision>,
        branch_token: &[u8],
    ) -> Result<(st::History, Vec<u8>), CadenceError> {
        let is_first_page = next_page_token.is_empty();
        let shard_id = workflow_id_to_history_shard(&execution.workflow_id, self.num_shards);
        eprintln!(
            "GH-TRACE shard={} num_shards={} first={} next={} branch={}",
            shard_id,
            self.num_shards,
            first_event_id,
            next_event_id,
            String::from_utf8_lossy(branch_token).escape_default()
        );
        let (mut events, next_token) = self
            .read_full_page(ReadHistoryBranchRequest {
                branch_token: branch_token.to_vec(),
                min_event_id: first_event_id,
                max_event_id: next_event_id,
                page_size: i64::from(page_size),
                next_page_token,
                shard_id,
            })
            .await?;
        let is_last_page = next_token.is_empty();
        verify_history_is_complete(
            events.first().map(|e| e.event_id),
            events.last().map(|e| e.event_id),
            events.len(),
            first_event_id,
            next_event_id - 1,
            is_first_page,
            is_last_page,
            page_size.max(0) as usize,
        )
        .map_err(CadenceError::Internal)?;
        if is_last_page {
            if let Some(transient) = transient {
                // Go only logs an inconsistent transient decision.
                events.push(transient.scheduled.clone());
                events.push(transient.started.clone());
            }
        }
        Ok((st::History { events }, next_token))
    }
}

/// Matching `PollForDecisionTaskResponse` / History `RecordDecisionTaskStartedResponse`
/// reduced to what Go's `createPollForDecisionTaskResponse` consumes.
#[derive(Debug, Default)]
struct DecisionTask {
    task_token: Vec<u8>,
    execution: ty::WorkflowExecution,
    workflow_type: Option<ty::WorkflowType>,
    previous_started_event_id: Option<i64>,
    started_event_id: i64,
    attempt: i64,
    next_event_id: i64,
    backlog_count_hint: i64,
    sticky: bool,
    query: Option<tk::WorkflowQuery>,
    transient: Option<TransientDecision>,
    task_list: Option<ty::TaskList>,
    branch_token: Vec<u8>,
    scheduled_timestamp: Option<i64>,
    started_timestamp: Option<i64>,
    queries: Option<HashMap<String, tk::WorkflowQuery>>,
}

fn thrift_err(what: &str, e: impl std::fmt::Display) -> CadenceError {
    internal(format!("invalid {what}: {e}"))
}

fn transient_of(s: &Struct, id: i16) -> Option<TransientDecision> {
    match s.get(id) {
        Some(Value::Struct(inner)) => TransientDecision::from_thrift(inner),
        _ => None,
    }
}

impl DecisionTask {
    /// Matching.thrift `PollForDecisionTaskResponse`; `None` when no task.
    fn from_matching(s: &Struct) -> Result<Option<Self>, CadenceError> {
        let e = |x| thrift_err("Matching PollForDecisionTask response", x);
        let Some(execution) = s.opt_struct::<ty::WorkflowExecution>(20).map_err(e)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            task_token: s.bytes_or_empty(10).map_err(e)?,
            execution,
            workflow_type: s.opt_struct(30).map_err(e)?,
            previous_started_event_id: s.opt_i64(40).map_err(e)?,
            started_event_id: s.opt_i64(50).map_err(e)?.unwrap_or(0),
            attempt: s.opt_i64(51).map_err(e)?.unwrap_or(0),
            next_event_id: s.opt_i64(60).map_err(e)?.unwrap_or(0),
            backlog_count_hint: s.opt_i64(65).map_err(e)?.unwrap_or(0),
            sticky: s.opt_bool(70).map_err(e)?.unwrap_or(false),
            query: s.opt_struct(80).map_err(e)?,
            transient: transient_of(s, 90),
            task_list: s.opt_struct(100).map_err(e)?,
            branch_token: s.bytes_or_empty(120).map_err(e)?,
            scheduled_timestamp: s.opt_i64(130).map_err(e)?.map(|n| n / 1_000_000),
            started_timestamp: s.opt_i64(140).map_err(e)?.map(|n| n / 1_000_000),
            queries: s.opt_map_str(150).map_err(e)?,
        }))
    }

    /// Go `common.CreateMatchingPollForDecisionTaskResponse` over History's
    /// `RecordDecisionTaskStartedResponse`.
    fn from_started(
        s: &Struct,
        execution: ty::WorkflowExecution,
        task_token: Vec<u8>,
    ) -> Result<Self, CadenceError> {
        let e = |x| thrift_err("History RecordDecisionTaskStarted response", x);
        Ok(Self {
            task_token,
            execution,
            workflow_type: s.opt_struct(10).map_err(e)?,
            previous_started_event_id: s.opt_i64(20).map_err(e)?,
            started_event_id: s.opt_i64(40).map_err(e)?.unwrap_or(0),
            attempt: s.opt_i64(60).map_err(e)?.unwrap_or(0),
            next_event_id: s.opt_i64(50).map_err(e)?.unwrap_or(0),
            backlog_count_hint: 0,
            sticky: s.opt_bool(70).map_err(e)?.unwrap_or(false),
            query: None,
            transient: transient_of(s, 80),
            task_list: s.opt_struct(90).map_err(e)?,
            branch_token: s.bytes_or_empty(110).map_err(e)?,
            scheduled_timestamp: s.opt_i64(120).map_err(e)?.map(|n| n / 1_000_000),
            started_timestamp: s.opt_i64(130).map_err(e)?.map(|n| n / 1_000_000),
            queries: s.opt_map_str(140).map_err(e)?,
        })
    }
}

fn str_field(request: &serde_json::Value, key: &str) -> String {
    request
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

struct MutableState {
    branch_token: Vec<u8>,
    run_id: String,
    last_first_event_id: i64,
    next_event_id: i64,
    running: bool,
    last_item: ty::VersionHistoryItem,
}

impl ProductionBackend {
    fn access(&self) -> Result<&HistoryAccess, CadenceError> {
        self.history_access.as_ref().ok_or_else(|| {
            internal(
                "history persistence is not configured for this Frontend (MySQL/PostgreSQL only)",
            )
        })
    }

    fn domain_id(&self, domain: &str) -> Result<String, CadenceError> {
        match &self.domains {
            Some(lookup) => lookup.domain_id(domain),
            None => Err(internal("domain lookup is not configured")),
        }
    }

    async fn matching(
        &self,
        method: &str,
        envelope: Struct,
        timeout: Duration,
    ) -> Result<Struct, CadenceError> {
        self.matching_for("", method, envelope, timeout).await
    }

    async fn matching_for(
        &self,
        task_list: &str,
        method: &str,
        envelope: Struct,
        timeout: Duration,
    ) -> Result<Struct, CadenceError> {
        let addr = self.matching_addr_for(task_list);
        self.call_with_timeout(
            &addr,
            MATCHING_SERVICE,
            "MatchingService",
            method,
            envelope,
            timeout,
        )
        .await
    }

    /// Go `createPollForDecisionTaskResponse`.
    async fn decision_response(
        &self,
        domain_id: &str,
        task: DecisionTask,
    ) -> Result<tk::PollForDecisionTaskResponse, CadenceError> {
        let _ = domain_id;
        let (history, continuation) = if task.sticky && task.query.is_some() {
            // Sticky query: the worker only needs the current state.
            (st::History { events: vec![] }, Vec::new())
        } else {
            let first_event_id = if task.sticky {
                task.previous_started_event_id.unwrap_or(0) + 1
            } else {
                FIRST_EVENT_ID
            };
            let (history, token) = self
                .access()?
                .get_history(
                    &task.execution,
                    first_event_id,
                    task.next_event_id,
                    HISTORY_MAX_PAGE_SIZE,
                    Vec::new(),
                    task.transient.as_ref(),
                    &task.branch_token,
                )
                .await?;
            let continuation = if token.is_empty() {
                Vec::new()
            } else {
                serialize_history_token(Some(&HistoryContinuationToken {
                    run_id: task.execution.run_id.clone(),
                    first_event_id,
                    next_event_id: task.next_event_id,
                    is_workflow_running: false,
                    persistence_token: Some(token),
                    transient_decision: task.transient.as_ref().map(TransientDecision::to_value),
                    branch_token: Some(task.branch_token.clone()),
                    version_history_item: None,
                }))
                .map_err(|e| internal(e.to_string()))?
                .unwrap_or_default()
            };
            (history, continuation)
        };
        Ok(tk::PollForDecisionTaskResponse {
            task_token: task.task_token,
            workflow_execution: Some(task.execution),
            workflow_type: task.workflow_type,
            previous_started_event_id: task.previous_started_event_id,
            started_event_id: task.started_event_id,
            attempt: task.attempt,
            backlog_count_hint: task.backlog_count_hint,
            next_event_id: task.next_event_id,
            scheduled_timestamp: task.scheduled_timestamp,
            started_timestamp: task.started_timestamp,
            history: Some(history),
            next_page_token: continuation,
            workflow_execution_task_list: task.task_list,
            query: task.query,
            query_id: None,
            queries: task.queries,
        })
    }

    fn poll_envelope(
        &self,
        request: &serde_json::Value,
        poll_key: &str,
        poll: Struct,
    ) -> Result<Struct, CadenceError> {
        let mut fields = vec![
            (10, domain_uuid(request, "domainUUID")?),
            (
                15,
                Value::Bytes(str_field(request, "pollerID").into_bytes()),
            ),
            (20, Value::Struct(poll)),
        ];
        let _ = poll_key;
        let isolation = str_field(request, "isolationGroup");
        if !isolation.is_empty() {
            fields.push((40, Value::Bytes(isolation.into_bytes())));
        }
        Ok(Struct { fields })
    }

    pub(crate) async fn poll_decision(
        &self,
        request: &serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        let poll: tk::PollForDecisionTaskRequest = typed(request, "pollRequest")?;
        let envelope = self.poll_envelope(request, "pollRequest", poll.to_thrift())?;
        let reply = self
            .matching_for(
                poll.task_list.as_ref().map_or("", |t| t.name.as_str()),
                "PollForDecisionTask",
                envelope,
                POLL_TIMEOUT,
            )
            .await?;
        eprintln!(
            "PD-TRACE reply fields={:?}",
            reply.fields.iter().map(|(i, _)| *i).collect::<Vec<_>>()
        );
        let Some(task) = DecisionTask::from_matching(&reply)? else {
            eprintln!("PD-TRACE no task");
            return Ok(serde_json::json!({}));
        };
        eprintln!(
            "PD-TRACE task wf={:?} next_event_id={} started={}",
            task.execution.workflow_id, task.next_event_id, task.started_event_id
        );
        let response = self
            .decision_response(&str_field(request, "domainUUID"), task)
            .await;
        eprintln!(
            "PD-TRACE decision_response err={:?} events={:?}",
            response.as_ref().err().map(|e| e.to_string()),
            response
                .as_ref()
                .ok()
                .and_then(|r| r.history.as_ref().map(|h| h.events.len()))
        );
        let response = response?;
        to_json(&response)
    }

    pub(crate) async fn poll_activity(
        &self,
        request: &serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        let poll: tk::PollForActivityTaskRequest = typed(request, "pollRequest")?;
        let envelope = self.poll_envelope(request, "pollRequest", poll.to_thrift())?;
        let reply = self
            .matching_for(
                poll.task_list.as_ref().map_or("", |t| t.name.as_str()),
                "PollForActivityTask",
                envelope,
                POLL_TIMEOUT,
            )
            .await?;
        let response = tk::PollForActivityTaskResponse::from_thrift(&reply)
            .map_err(|e| thrift_err("Matching PollForActivityTask response", e))?;
        to_json(&response)
    }

    pub(crate) async fn respond_decision_completed(
        &self,
        request: &serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        let complete: tk::RespondDecisionTaskCompletedRequest = typed(request, "completeRequest")?;
        let envelope = Struct {
            fields: vec![
                (10, domain_uuid(request, "domainUUID")?),
                (20, Value::Struct(complete.to_thrift())),
            ],
        };
        let reply = self
            .history("RespondDecisionTaskCompleted", envelope)
            .await?;
        let mut response = tk::RespondDecisionTaskCompletedResponse::default();
        if complete.return_new_decision_task {
            if let Some(Value::Struct(started)) = reply.get(10) {
                let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize(
                    &complete.task_token,
                )
                .map_err(|e| bad_request(e.to_string()))?;
                let scheduled_id = started
                    .opt_i64(30)
                    .map_err(|e| thrift_err("History started response", e))?
                    .unwrap_or(0);
                let attempt = started
                    .opt_i64(60)
                    .map_err(|e| thrift_err("History started response", e))?
                    .unwrap_or(0);
                let new_token = cadence_api::tasktoken::TaskToken {
                    domain_id: token.domain_id.clone(),
                    workflow_id: token.workflow_id.clone(),
                    run_id: token.run_id.clone(),
                    schedule_id: scheduled_id,
                    schedule_attempt: attempt,
                    ..Default::default()
                };
                let bytes = cadence_api::tasktoken::JsonTaskTokenSerializer::serialize(&new_token)
                    .map_err(|e| internal(e.to_string()))?;
                let execution = ty::WorkflowExecution {
                    workflow_id: token.workflow_id,
                    run_id: token.run_id,
                };
                let task = DecisionTask::from_started(started, execution, bytes)?;
                response.decision_task =
                    Some(self.decision_response(&token.domain_id, task).await?);
            }
        }
        to_json(&response)
    }

    pub(crate) async fn respond_query_completed(
        &self,
        request: &serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        let task_list: ty::TaskList = typed(request, "taskList")?;
        let completed: tk::RespondQueryTaskCompletedRequest = typed(request, "completedRequest")?;
        let envelope = Struct {
            fields: vec![
                (10, domain_uuid(request, "domainUUID")?),
                (20, Value::Struct(task_list.to_thrift())),
                (30, Value::Bytes(str_field(request, "taskID").into_bytes())),
                (40, Value::Struct(completed.to_thrift())),
            ],
        };
        self.matching_for(
            &task_list.name,
            "RespondQueryTaskCompleted",
            envelope,
            Duration::from_secs(30),
        )
        .await?;
        Ok(serde_json::json!({}))
    }

    pub(crate) async fn describe_task_list(
        &self,
        request: &serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        let desc: tk::DescribeTaskListRequest = typed(request, "descRequest")?;
        let envelope = Struct {
            fields: vec![
                (10, domain_uuid(request, "domainUUID")?),
                (20, Value::Struct(desc.to_thrift())),
            ],
        };
        let reply = self
            .matching_for(
                desc.task_list.as_ref().map_or("", |t| t.name.as_str()),
                "DescribeTaskList",
                envelope,
                Duration::from_secs(30),
            )
            .await?;
        to_json(
            &tk::DescribeTaskListResponse::from_thrift(&reply)
                .map_err(|e| thrift_err("DescribeTaskList response", e))?,
        )
    }

    pub(crate) async fn list_task_list_partitions(
        &self,
        request: &serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        let list = tk::ListTaskListPartitionsRequest {
            domain: str_field(request, "domain"),
            task_list: typed(request, "taskList")?,
        };
        let reply = self
            .matching(
                "ListTaskListPartitions",
                list.to_thrift(),
                Duration::from_secs(30),
            )
            .await?;
        to_json(
            &tk::ListTaskListPartitionsResponse::from_thrift(&reply)
                .map_err(|e| thrift_err("ListTaskListPartitions response", e))?,
        )
    }

    pub(crate) async fn get_task_lists_by_domain(
        &self,
        request: &serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        let req = tk::GetTaskListsByDomainRequest {
            domain: str_field(request, "domain"),
        };
        let reply = self
            .matching(
                "GetTaskListsByDomain",
                req.to_thrift(),
                Duration::from_secs(30),
            )
            .await?;
        to_json(
            &tk::GetTaskListsByDomainResponse::from_thrift(&reply)
                .map_err(|e| thrift_err("GetTaskListsByDomain response", e))?,
        )
    }

    /// History `PollMutableState` (Go `queryHistory` closure).
    async fn poll_mutable_state(
        &self,
        domain_id: &str,
        execution: &ty::WorkflowExecution,
        expected_next_event_id: i64,
        current_branch_token: &[u8],
        long_poll: bool,
    ) -> Result<MutableState, CadenceError> {
        let mut fields = vec![
            (10, Value::Bytes(domain_id.as_bytes().to_vec())),
            (20, Value::Struct(execution.to_thrift())),
            (30, Value::I64(expected_next_event_id)),
        ];
        if !current_branch_token.is_empty() {
            fields.push((40, Value::Bytes(current_branch_token.to_vec())));
        }
        let timeout = if long_poll {
            POLL_TIMEOUT
        } else {
            Duration::from_secs(30)
        };
        let reply = self
            .call_with_timeout(
                &self.history_addr_for_workflow(&execution.workflow_id)?,
                "cadence-history",
                "HistoryService",
                "PollMutableState",
                Struct { fields },
                timeout,
            )
            .await?;
        let e = |x| thrift_err("PollMutableState response", x);
        let run_id = reply
            .opt_struct::<ty::WorkflowExecution>(10)
            .map_err(e)?
            .map(|x| x.run_id)
            .unwrap_or_default();
        let histories = match reply.get(130) {
            Some(Value::Struct(v)) => ty::VersionHistories {
                current_version_history_index: v.opt_i32(10).map_err(e)?.unwrap_or(0),
                histories: v
                    .opt_list::<ty::VersionHistory>(20)
                    .map_err(e)?
                    .unwrap_or_default(),
            },
            _ => ty::VersionHistories::default(),
        };
        let close_state = reply.opt_i32(150).map_err(e)?.unwrap_or(0);
        let last_item = histories
            .histories
            .get(histories.current_version_history_index.max(0) as usize)
            .and_then(|h| h.items.last().cloned())
            .ok_or_else(|| {
                internal("failed to get the current version from the response from history: version history is empty")
            })?;
        Ok(MutableState {
            branch_token: reply.bytes_or_empty(110).map_err(e)?,
            run_id,
            last_first_event_id: reply.opt_i64(40).map_err(e)?.unwrap_or(0),
            next_event_id: reply.opt_i64(30).map_err(e)?.unwrap_or(0),
            running: close_state == 0,
            last_item,
        })
    }

    /// Go `WorkflowHandler.GetWorkflowExecutionHistory` after validation and
    /// archival handling (raw-history responses are not produced).
    pub(crate) async fn get_workflow_execution_history(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, CadenceError> {
        let req: st::GetWorkflowExecutionHistoryRequest = serde_json::from_value(request)
            .map_err(|e| bad_request(format!("invalid request: {e}")))?;
        let domain_id = self.domain_id(&req.domain)?;
        let access = self.access()?;
        let mut page_size = req.maximum_page_size;
        if page_size <= 0 {
            page_size = HISTORY_MAX_PAGE_SIZE;
        }
        page_size = page_size.min(HISTORY_MAX_PAGE_SIZE);
        let long_poll = req.wait_for_new_event;
        let close_event_only =
            req.history_event_filter_type == Some(cadence_api::HistoryEventFilterType::CloseEvent);
        let mut execution = req.execution.clone().unwrap_or_default();

        let mut token = HistoryContinuationToken::default();
        let mut last_first_event_id = FIRST_EVENT_ID;
        let mut next_event_id = 0_i64;
        let mut running = false;

        if !req.next_page_token.is_empty() {
            token = deserialize_history_token(&req.next_page_token)
                .map_err(|_| bad_request("Invalid NextPageToken."))?;
            validate_history_run_id(&execution.run_id, &token).map_err(CadenceError::BadRequest)?;
            execution.run_id = token.run_id.clone();
            if token
                .persistence_token
                .as_deref()
                .is_none_or(<[u8]>::is_empty)
                && long_poll
                && token.is_workflow_running
            {
                let query_next = if close_event_only {
                    END_EVENT_ID
                } else {
                    token.next_event_id
                };
                let state = self
                    .poll_mutable_state(
                        &domain_id,
                        &execution,
                        query_next,
                        token.branch_token.as_deref().unwrap_or_default(),
                        true,
                    )
                    .await?;
                token.branch_token = Some(state.branch_token);
                last_first_event_id = state.last_first_event_id;
                next_event_id = state.next_event_id;
                running = state.running;
                token.version_history_item = serde_json::to_value(&state.last_item).ok();
                token.first_event_id = token.next_event_id;
                token.next_event_id = next_event_id;
                token.is_workflow_running = running;
            }
        } else {
            let query_next = if close_event_only {
                END_EVENT_ID
            } else {
                FIRST_EVENT_ID
            };
            let state = self
                .poll_mutable_state(&domain_id, &execution, query_next, &[], false)
                .await?;
            token.branch_token = Some(state.branch_token);
            last_first_event_id = state.last_first_event_id;
            next_event_id = state.next_event_id;
            running = state.running;
            token.version_history_item = serde_json::to_value(&state.last_item).ok();
            execution.run_id = state.run_id.clone();
            token.run_id = state.run_id;
            token.first_event_id = FIRST_EVENT_ID;
            token.next_event_id = next_event_id;
            token.is_workflow_running = running;
            token.persistence_token = None;
        }

        let transient = token
            .transient_decision
            .as_ref()
            .and_then(TransientDecision::from_value);
        let branch_token = token.branch_token.clone().unwrap_or_default();
        let mut history = st::History { events: vec![] };
        let mut token_out = Some(token);

        if close_event_only {
            if !running {
                let (h, _) = access
                    .get_history(
                        &execution,
                        last_first_event_id,
                        next_event_id,
                        page_size,
                        Vec::new(),
                        transient.as_ref(),
                        &branch_token,
                    )
                    .await?;
                history = h;
                if history.events.len() > 1 {
                    history.events = history.events.split_off(history.events.len() - 1);
                }
                token_out = None;
            } else if long_poll {
                if let Some(t) = token_out.as_mut() {
                    t.persistence_token = None;
                }
            } else {
                token_out = None;
            }
        } else {
            let t = token_out.as_mut().expect("token present");
            if t.first_event_id >= t.next_event_id {
                if !running {
                    token_out = None;
                }
            } else {
                let (h, persistence_token) = access
                    .get_history(
                        &execution,
                        t.first_event_id,
                        t.next_event_id,
                        page_size,
                        t.persistence_token.clone().unwrap_or_default(),
                        transient.as_ref(),
                        &branch_token,
                    )
                    .await?;
                history = h;
                let exhausted = persistence_token.is_empty();
                t.persistence_token = if exhausted {
                    None
                } else {
                    Some(persistence_token)
                };
                if exhausted && (!t.is_workflow_running || !long_poll) {
                    token_out = None;
                }
            }
        }

        let next_page_token = serialize_history_token(token_out.as_ref())
            .map_err(|e| internal(e.to_string()))?
            .unwrap_or_default();
        to_json(&st::GetWorkflowExecutionHistoryResponse {
            history: Some(history),
            raw_history: Vec::new(),
            next_page_token,
            archived: false,
        })
    }
}

impl crate::archive_readers::ExecutionProbe for ProductionBackend {
    fn exists<'a>(
        &'a self,
        domain_id: &'a str,
        workflow_id: &'a str,
        run_id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, CadenceError>> + Send + 'a>>
    {
        Box::pin(async move {
            let execution = ty::WorkflowExecution {
                workflow_id: workflow_id.to_owned(),
                run_id: run_id.to_owned(),
            };
            match self
                .poll_mutable_state(domain_id, &execution, FIRST_EVENT_ID, &[], false)
                .await
            {
                Ok(_) => Ok(true),
                Err(CadenceError::EntityNotExists(_)) => Ok(false),
                Err(e) => Err(e),
            }
        })
    }
}
