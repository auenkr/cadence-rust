//! Frontend history reads and decision-task response assembly, ported from
//! `go/service/frontend/api/handler.go` (`getHistory`, `verifyHistoryIsComplete`,
//! `createPollForDecisionTaskResponse`, `getHistoryContinuationToken`).
use crate::history_forward::ForwardError;
use crate::matching_client::decision_response_in;
use cadence_api::{
    enums::EventType,
    service_types::{History, HistoryEvent},
    tasks as tk,
};
use cadence_persistence::history_node_reader::{
    HistoryReadError, HistoryReader, ReadHistoryBranchRequest,
};
use cadence_proto::{convert as cv, matching as m};
use serde::{Deserialize, Serialize};

const FIRST_EVENT_ID: i64 = 1;

/// Go `getHistoryContinuationToken` (JSON, Go field names).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryContinuationToken {
    #[serde(rename = "RunID")]
    pub run_id: String,
    #[serde(rename = "FirstEventID")]
    pub first_event_id: i64,
    #[serde(rename = "NextEventID")]
    pub next_event_id: i64,
    #[serde(rename = "IsWorkflowRunning")]
    pub is_workflow_running: bool,
    #[serde(
        rename = "PersistenceToken",
        serialize_with = "cadence_api::b64::serialize_opt_bytes",
        deserialize_with = "cadence_api::b64::deserialize_opt_bytes"
    )]
    pub persistence_token: Option<Vec<u8>>,
    /// Transient decision events (scheduled, started) of the last page.
    #[serde(rename = "TransientDecision")]
    pub transient_decision: Option<TransientDecision>,
    #[serde(
        rename = "BranchToken",
        serialize_with = "cadence_api::b64::serialize_opt_bytes",
        deserialize_with = "cadence_api::b64::deserialize_opt_bytes"
    )]
    pub branch_token: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TransientDecision {
    #[serde(rename = "ScheduledEvent")]
    pub scheduled_event: Option<HistoryEvent>,
    #[serde(rename = "StartedEvent")]
    pub started_event: Option<HistoryEvent>,
}

pub fn serialize_history_token(token: &HistoryContinuationToken) -> Result<Vec<u8>, ForwardError> {
    serde_json::to_vec(token).map_err(|e| ForwardError::Internal(e.to_string()))
}

pub fn deserialize_history_token(bytes: &[u8]) -> Result<HistoryContinuationToken, ForwardError> {
    serde_json::from_slice(bytes).map_err(|_| ForwardError::BadRequest("Invalid token.".into()))
}

pub fn read_error(error: HistoryReadError) -> ForwardError {
    match error {
        HistoryReadError::NotFound(m) => ForwardError::NotFound(m),
        HistoryReadError::Invalid(m) => ForwardError::BadRequest(m),
        HistoryReadError::Inconsistent(m) | HistoryReadError::Internal(m) => {
            ForwardError::Internal(m)
        }
    }
}

/// Go `persistenceutils.ReadFullPageV2Events`.
fn read_full_page(
    reader: &HistoryReader,
    mut request: ReadHistoryBranchRequest,
) -> Result<(Vec<HistoryEvent>, usize, Vec<u8>), HistoryReadError> {
    let page_size = request.page_size as usize;
    let mut events = Vec::new();
    let mut size = 0;
    loop {
        let response = reader.read_history_branch(&request)?;
        events.extend(response.events());
        size += response.size;
        let token = response.next_page_token.unwrap_or_default();
        if events.len() >= page_size || token.is_empty() {
            return Ok((events, size, token));
        }
        request.next_page_token = token;
    }
}

/// Go `verifyHistoryIsComplete`.
pub fn verify_history_is_complete(
    events: &[HistoryEvent],
    expected_first_event_id: i64,
    expected_last_event_id: i64,
    is_first_page: bool,
    is_last_page: bool,
    page_size: i64,
) -> Result<(), ForwardError> {
    let n = events.len() as i64;
    if n == 0 {
        if is_last_page {
            return Ok(());
        }
        return Err(ForwardError::Internal(
            "invalid history: contains zero events".into(),
        ));
    }
    let first = events[0].event_id;
    let last = events[events.len() - 1].event_id;
    let mut expected_first = expected_first_event_id;
    let mut expected_last = expected_last_event_id;
    if !is_first_page {
        if first <= expected_first {
            return Err(ForwardError::Internal(format!(
                "invalid history: expected first eventID to be > {expected_first} but got {first}"
            )));
        }
        expected_first = first;
    }
    if !is_last_page {
        expected_last = expected_first + page_size - 1;
    }
    let expected_count = expected_last - expected_first + 1;
    if first == expected_first
        && ((is_last_page && last == expected_last && n == expected_count)
            || (!is_last_page && last >= expected_last && n >= expected_count))
    {
        return Ok(());
    }
    Err(ForwardError::Internal(format!(
        "incomplete history: expected events [{expected_first}-{expected_last}] but got events [{first}-{last}] of length {n}:isFirstPage={is_first_page},isLastPage={is_last_page},pageSize={page_size}"
    )))
}

/// Parameters of Go `getHistory`.
pub struct GetHistoryArgs {
    pub shard_id: i32,
    pub first_event_id: i64,
    pub next_event_id: i64,
    pub page_size: i32,
    pub next_page_token: Vec<u8>,
    pub transient: Option<TransientDecision>,
    pub branch_token: Vec<u8>,
}

/// Go `getHistory`: one page of events and the persistence page token.
pub async fn get_history(
    reader: &HistoryReader,
    args: GetHistoryArgs,
) -> Result<(History, Vec<u8>), ForwardError> {
    let is_first_page = args.next_page_token.is_empty();
    let request = ReadHistoryBranchRequest {
        branch_token: args.branch_token.clone(),
        min_event_id: args.first_event_id,
        max_event_id: args.next_event_id,
        page_size: i64::from(args.page_size),
        next_page_token: args.next_page_token.clone(),
        shard_id: args.shard_id,
    };
    let reader = reader.clone();
    let (mut events, _size, token) =
        tokio::task::spawn_blocking(move || read_full_page(&reader, request))
            .await
            .map_err(|e| ForwardError::Internal(e.to_string()))?
            .map_err(read_error)?;
    let is_last_page = token.is_empty();
    verify_history_is_complete(
        &events,
        args.first_event_id,
        args.next_event_id - 1,
        is_first_page,
        is_last_page,
        i64::from(args.page_size),
    )?;
    if token.is_empty() {
        if let Some(transient) = args.transient {
            // Go validates the IDs but only logs a mismatch.
            events.extend(transient.scheduled_event);
            events.extend(transient.started_event);
        }
    }
    Ok((History { events }, token))
}

fn event_ms(time: Option<prost_types::Timestamp>) -> Option<i64> {
    cv::nanos(time).map(|n| n / 1_000_000)
}

/// Proto transient decision events -> Thrift. Only decision scheduled and
/// started events can be transient.
fn transient_event_in(event: cadence_proto::api::HistoryEvent) -> Option<HistoryEvent> {
    use cadence_proto::api::history_event::Attributes as A;
    let mut out = HistoryEvent {
        event_id: event.event_id,
        timestamp: event_ms(event.event_time),
        version: event.version,
        task_id: event.task_id,
        ..Default::default()
    };
    match event.attributes? {
        A::DecisionTaskScheduledEventAttributes(a) => {
            out.event_type = Some(EventType::DecisionTaskScheduled);
            out.decision_task_scheduled_event_attributes =
                Some(tk::DecisionTaskScheduledEventAttributes {
                    task_list: a.task_list.and_then(|t| cv::task_list(t).ok()),
                    start_to_close_timeout_seconds: cv::duration_seconds(a.start_to_close_timeout),
                    attempt: i64::from(a.attempt),
                });
        }
        A::DecisionTaskStartedEventAttributes(a) => {
            out.event_type = Some(EventType::DecisionTaskStarted);
            out.decision_task_started_event_attributes =
                Some(tk::DecisionTaskStartedEventAttributes {
                    scheduled_event_id: a.scheduled_event_id,
                    identity: a.identity,
                    request_id: a.request_id,
                });
        }
        _ => return None,
    }
    Some(out)
}

fn transient_in(info: Option<cadence_proto::shared::TransientDecisionInfo>) -> Option<TransientDecision> {
    let info = info?;
    Some(TransientDecision {
        scheduled_event: info.scheduled_event.and_then(transient_event_in),
        started_event: info.started_event.and_then(transient_event_in),
    })
}

/// Go `createPollForDecisionTaskResponse`.
pub async fn create_poll_for_decision_task_response(
    reader: &HistoryReader,
    num_history_shards: u32,
    history_max_page_size: i32,
    matching: m::PollForDecisionTaskResponse,
) -> Result<tk::PollForDecisionTaskResponse, ForwardError> {
    let Some(execution) = matching.workflow_execution.clone() else {
        return Ok(tk::PollForDecisionTaskResponse::default());
    };
    let mut response = decision_response_in(&matching);
    if matching.sticky_execution_enabled && matching.query.is_some() {
        response.history = Some(History { events: Vec::new() });
        return Ok(response);
    }
    let mut first_event_id = FIRST_EVENT_ID;
    let next_event_id = matching.next_event_id;
    if matching.sticky_execution_enabled {
        first_event_id = matching.previous_started_event_id.unwrap_or(0) + 1;
    }
    let transient = transient_in(matching.decision_info.clone());
    let shard_id = (cadence_common::membership::fingerprint32(execution.workflow_id.as_bytes())
        % num_history_shards.max(1)) as i32;
    let (history, token) = get_history(
        reader,
        GetHistoryArgs {
            shard_id,
            first_event_id,
            next_event_id,
            page_size: history_max_page_size,
            next_page_token: Vec::new(),
            transient: transient.clone(),
            branch_token: matching.branch_token.clone(),
        },
    )
    .await?;
    response.history = Some(history);
    if !token.is_empty() {
        response.next_page_token = serialize_history_token(&HistoryContinuationToken {
            run_id: execution.run_id.clone(),
            first_event_id,
            next_event_id,
            persistence_token: Some(token),
            transient_decision: transient,
            branch_token: Some(matching.branch_token.clone()),
            ..Default::default()
        })?;
    }
    Ok(response)
}

/// Go `common.CreateMatchingPollForDecisionTaskResponse`.
pub fn matching_response_from_started(
    started: &cadence_proto::history::RecordDecisionTaskStartedResponse,
    execution: cadence_proto::api::WorkflowExecution,
    task_token: Vec<u8>,
) -> m::PollForDecisionTaskResponse {
    m::PollForDecisionTaskResponse {
        task_token,
        workflow_execution: Some(execution),
        workflow_type: started.workflow_type.clone(),
        previous_started_event_id: started.previous_started_event_id,
        started_event_id: started.started_event_id,
        attempt: started.attempt,
        next_event_id: started.next_event_id,
        sticky_execution_enabled: started.sticky_execution_enabled,
        decision_info: started.decision_info.clone(),
        workflow_execution_task_list: started.workflow_execution_task_list.clone(),
        event_store_version: started.event_store_version,
        branch_token: started.branch_token.clone(),
        scheduled_time: started.scheduled_time.clone(),
        started_time: started.started_time.clone(),
        queries: started.queries.clone(),
        total_history_bytes: started.history_size,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: i64) -> HistoryEvent {
        HistoryEvent {
            event_id: id,
            ..Default::default()
        }
    }

    #[test]
    fn complete_history_passes_and_gaps_fail() {
        let events: Vec<_> = (1..=3).map(event).collect();
        assert!(verify_history_is_complete(&events, 1, 3, true, true, 10).is_ok());
        let gap = vec![event(1), event(3)];
        assert!(verify_history_is_complete(&gap, 1, 3, true, true, 10).is_err());
        assert!(verify_history_is_complete(&[], 1, 3, true, true, 10).is_ok());
        assert!(verify_history_is_complete(&[], 1, 3, true, false, 10).is_err());
    }

    #[test]
    fn continuation_token_roundtrip() {
        let token = HistoryContinuationToken {
            run_id: "r".into(),
            first_event_id: 1,
            next_event_id: 9,
            persistence_token: Some(vec![1, 2]),
            branch_token: Some(vec![3]),
            ..Default::default()
        };
        let bytes = serialize_history_token(&token).unwrap();
        assert_eq!(deserialize_history_token(&bytes).unwrap(), token);
        assert!(deserialize_history_token(b"nope").is_err());
    }
}
