//! Durable matching query-task delivery, from Go `matcher.OfferQuery` /
//! `queryTaskC` and the handler's `lockableQueryTaskMap`. A query is an
//! ephemeral task offered to a decision poller: it never touches task storage
//! and is not visible to the backlog.
use cadence_proto::{api as p, history as h, matching as m};
use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
};
use tokio::sync::{oneshot, Notify};
use tonic::Status;

/// Go `EmptyEventID`: `previous_started_event_id` is omitted while equal.
const EMPTY_EVENT_ID: i64 = -23;
/// Go `GoWorkerStickyQueryVersion` / `StickyQueryUnknownImplConstraints` are
/// both 1.0.0.
const STICKY_QUERY_MIN_MAJOR: u64 = 1;

/// A query offered by `QueryWorkflow` and awaiting a decision poller.
pub struct PendingQuery {
    pub task_id: String,
    pub domain_id: String,
    pub task_list: String,
    pub request: p::QueryWorkflowRequest,
}

/// What a waiting `QueryWorkflow` call receives.
pub enum QueryOutcome {
    /// The worker's `RespondQueryTaskCompleted` result.
    Worker(p::WorkflowQueryResult),
    /// Go `queryResult{internalError}`: polling failed before delivery.
    Internal(Status),
}

/// Per-owner query rendezvous: Go `matcher.queryTaskC` (the pending offers)
/// plus the matching engine's `lockableQueryTaskMap` (the result waiters).
#[derive(Default)]
pub struct QueryChannel {
    pending: Mutex<VecDeque<PendingQuery>>,
    waiting: Mutex<HashMap<String, oneshot::Sender<QueryOutcome>>>,
    notify: Notify,
}

impl QueryChannel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `QueryWorkflow`: install the result waiter, then offer the task. The
    /// waiter is registered first so a poller cannot pick up the offer before
    /// the result channel exists.
    pub fn register(&self, query: PendingQuery) -> oneshot::Receiver<QueryOutcome> {
        let (sender, receiver) = oneshot::channel();
        self.waiting
            .lock()
            .expect("matching query lock")
            .insert(query.task_id.clone(), sender);
        self.pending
            .lock()
            .expect("matching query lock")
            .push_back(query);
        self.notify.notify_waiters();
        receiver
    }

    /// Go `matcher.Poll` on `queryTaskC`: wait for a query offer.
    pub async fn next(&self) -> PendingQuery {
        loop {
            if let Some(query) = self.take() {
                return query;
            }
            self.notify.notified().await;
        }
    }

    fn take(&self) -> Option<PendingQuery> {
        self.pending.lock().expect("matching query lock").pop_front()
    }

    /// Go `deliverQueryResult`. A missing waiter means the caller already gave
    /// up or the task expired.
    pub fn complete(&self, task_id: &str, outcome: QueryOutcome) -> Result<(), Status> {
        let sender = self
            .waiting
            .lock()
            .expect("matching query lock")
            .remove(task_id);
        match sender {
            Some(sender) => {
                // The waiter may have timed out; dropping the send is Go's
                // buffered-channel behaviour after the deferred delete.
                let _ = sender.send(outcome);
                Ok(())
            }
            None => Err(Status::not_found("query task not found, or already expired")),
        }
    }

    /// Withdraw an offer whose caller gave up (Go's deferred
    /// `lockableQueryTaskMap.delete` plus matcher cleanup).
    pub fn withdraw(&self, task_id: &str) {
        self.waiting
            .lock()
            .expect("matching query lock")
            .remove(task_id);
        self.pending
            .lock()
            .expect("matching query lock")
            .retain(|query| query.task_id != task_id);
    }
}

/// Go `versionChecker.SupportsStickyQuery` for the query poll response: only
/// the known SDK implementations, or an unknown-impl client, at feature
/// version >= 1.0.0.
fn supports_sticky_query(client_impl: &str, feature_version: &str) -> bool {
    if !matches!(client_impl, "uber-go" | "uber-java" | "") {
        return false;
    }
    feature_version
        .split('.')
        .next()
        .and_then(|major| major.parse::<u64>().ok())
        .is_some_and(|major| major >= STICKY_QUERY_MIN_MAJOR)
}

/// Go `createPollForDecisionTaskResponse` for `task.IsQuery()`: the token is a
/// query token, the response carries the query, and History's mutable state
/// supplies the metadata the frontend needs to load history.
pub fn query_response(
    query: PendingQuery,
    state: &h::GetMutableStateResponse,
) -> m::PollForDecisionTaskResponse {
    let execution = query.request.workflow_execution.clone().unwrap_or_default();
    let token = cadence_api::tasktoken::QueryTaskToken {
        domain_id: query.domain_id.clone(),
        workflow_id: execution.workflow_id.clone(),
        run_id: execution.run_id.clone(),
        task_list: query.task_list.clone(),
        task_id: query.task_id.clone(),
    };
    let task_token =
        cadence_api::tasktoken::JsonTaskTokenSerializer::serialize_query_token(&token)
            .unwrap_or_default();
    let sticky_task_list_configured = state
        .sticky_task_list
        .as_ref()
        .is_some_and(|list| !list.name.is_empty());
    let sticky_execution_enabled = sticky_task_list_configured
        && supports_sticky_query(&state.client_impl, &state.client_feature_version);
    m::PollForDecisionTaskResponse {
        task_token,
        workflow_execution: Some(execution),
        workflow_type: state.workflow_type.clone(),
        previous_started_event_id: state
            .previous_started_event_id
            .filter(|event| *event != EMPTY_EVENT_ID),
        next_event_id: state.next_event_id,
        sticky_execution_enabled,
        query: query.request.query.clone(),
        workflow_execution_task_list: state.task_list.clone(),
        branch_token: state.current_branch_token.clone(),
        total_history_bytes: state.history_size,
        ..Default::default()
    }
}

/// Go `waitForQueryResult`: an answered query returns the payload; anything
/// else is reported as a failed query with the worker's message.
pub fn query_outcome(outcome: QueryOutcome) -> Result<m::QueryWorkflowResponse, Status> {
    let result = match outcome {
        QueryOutcome::Internal(status) => return Err(status),
        QueryOutcome::Worker(result) => result,
    };
    if result.result_type == p::QueryResultType::Answered as i32 {
        return Ok(m::QueryWorkflowResponse {
            query_result: result.answer,
            query_rejected: None,
            partition_config: None,
        });
    }
    Err(Status::invalid_argument(if result.error_message.is_empty() {
        "Query failed.".to_string()
    } else {
        result.error_message
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(task_id: &str) -> PendingQuery {
        PendingQuery {
            task_id: task_id.into(),
            domain_id: "domain-uuid".into(),
            task_list: "queue".into(),
            request: p::QueryWorkflowRequest {
                domain: "domain".into(),
                workflow_execution: Some(p::WorkflowExecution {
                    workflow_id: "workflow".into(),
                    run_id: "run".into(),
                }),
                query: Some(p::WorkflowQuery {
                    query_type: "ping".into(),
                    query_args: None,
                }),
                ..Default::default()
            },
        }
    }

    fn mutable_state() -> h::GetMutableStateResponse {
        h::GetMutableStateResponse {
            workflow_execution: Some(p::WorkflowExecution {
                workflow_id: "workflow".into(),
                run_id: "run".into(),
            }),
            workflow_type: Some(p::WorkflowType {
                name: "service-compat".into(),
            }),
            next_event_id: 12,
            previous_started_event_id: Some(5),
            task_list: Some(p::TaskList {
                name: "queue".into(),
                ..Default::default()
            }),
            client_impl: "uber-go".into(),
            client_feature_version: "1.0.0".into(),
            current_branch_token: vec![7, 8],
            history_size: 1234,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn offer_is_delivered_once_and_completed_by_task_id() {
        let channel = QueryChannel::new();
        let receiver = channel.register(query("task-1"));
        let offered = channel.next().await;
        assert_eq!(offered.task_id, "task-1");
        // The query response carries a decodable query token, the query, and
        // the mutable state the frontend uses to load history.
        let response = query_response(offered, &mutable_state());
        let token =
            cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
                &response.task_token,
            )
            .unwrap();
        assert_eq!(token.task_id, "task-1");
        assert_eq!(token.workflow_id, "workflow");
        assert_eq!(token.task_list, "queue");
        assert_eq!(response.next_event_id, 12);
        assert_eq!(response.previous_started_event_id, Some(5));
        assert_eq!(response.total_history_bytes, 1234);
        assert_eq!(
            response.query.as_ref().map(|q| q.query_type.clone()),
            Some("ping".to_string())
        );
        channel
            .complete(
                "task-1",
                QueryOutcome::Worker(p::WorkflowQueryResult {
                    result_type: p::QueryResultType::Answered as i32,
                    answer: Some(p::Payload {
                        data: b"answer".to_vec(),
                    }),
                    error_message: String::new(),
                }),
            )
            .unwrap();
        let response = query_outcome(receiver.await.unwrap()).unwrap();
        assert_eq!(
            response.query_result.map(|payload| payload.data),
            Some(b"answer".to_vec())
        );
    }

    #[test]
    fn sticky_query_requires_a_configured_list_and_a_supported_client() {
        assert!(supports_sticky_query("uber-go", "1.0.0"));
        assert!(supports_sticky_query("", "1.0.0"));
        assert!(!supports_sticky_query("", ""));
        assert!(!supports_sticky_query("other-sdk", "9.9.9"));
    }

    #[tokio::test]
    async fn internal_error_fails_the_waiting_query() {
        let channel = QueryChannel::new();
        let receiver = channel.register(query("task-3"));
        let _ = channel.next().await;
        channel
            .complete("task-3", QueryOutcome::Internal(Status::internal("history down")))
            .unwrap();
        let error = query_outcome(receiver.await.unwrap()).unwrap_err();
        assert!(error.message().contains("history down"));
    }

    #[tokio::test]
    async fn completing_an_unknown_task_reports_not_found() {
        let channel = QueryChannel::new();
        assert!(channel
            .complete("missing", QueryOutcome::Worker(p::WorkflowQueryResult::default()))
            .is_err());
    }

    #[tokio::test]
    async fn withdraw_removes_an_abandoned_offer() {
        let channel = QueryChannel::new();
        let _receiver = channel.register(query("task-2"));
        channel.withdraw("task-2");
        // No offer remains, and a late completion is rejected.
        assert!(channel
            .complete("task-2", QueryOutcome::Worker(p::WorkflowQueryResult::default()))
            .is_err());
    }

    #[test]
    fn failed_query_reports_the_worker_message() {
        let error = query_outcome(QueryOutcome::Worker(p::WorkflowQueryResult {
            result_type: p::QueryResultType::Failed as i32,
            answer: None,
            error_message: "boom".into(),
        }))
        .unwrap_err();
        assert!(error.message().contains("boom"));
    }
}
