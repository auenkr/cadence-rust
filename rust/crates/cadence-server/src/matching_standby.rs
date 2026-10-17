//! Go taskCompleter: standby backlog is acknowledged only after History proves
//! that its scheduled task started, the execution closed, or a missing execution
//! has passed the 24 hour safety window.
use cadence_common::{ExponentialRetryPolicy, Retrier};
use cadence_proto::{api as p, history as h};
use std::{sync::Arc, time::Duration};
use tonic::Status;

pub type Enabled = Arc<dyn Fn(&str, &str, i32) -> bool + Send + Sync>;

pub fn task_started(
    task_type: i32,
    schedule_id: i64,
    state: &h::DescribeWorkflowExecutionResponse,
) -> Result<bool, Status> {
    let info = state
        .workflow_execution_info
        .as_ref()
        .ok_or_else(|| Status::internal("workflow execution info is nil"))?;
    if info.close_status != 0 {
        return Ok(true);
    }
    match task_type {
        0 => Ok(state.pending_decision.as_ref().is_none_or(|pending| {
            schedule_id < pending.schedule_id
                || (schedule_id == pending.schedule_id
                    && pending.state == p::PendingDecisionState::Started as i32)
        })),
        1 => Ok(state
            .pending_activities
            .iter()
            .find(|pending| pending.schedule_id == schedule_id)
            .is_none_or(|pending| pending.state == p::PendingActivityState::Started as i32)),
        _ => Err(Status::internal("task type not supported")),
    }
}

pub fn checker(
    domains: cadence_persistence::InMemoryDomainStore,
    cluster: String,
    history: crate::matching_history::MatchingHistoryClient,
    key: cadence_persistence::matching_sqlite::TaskListKey,
    enabled: Option<Enabled>,
) -> crate::matching_reader::DispatchCheck {
    Arc::new(move |task| {
        let domains = domains.clone();
        let cluster = cluster.clone();
        let history = history.clone();
        let key = key.clone();
        let enabled = enabled.clone();
        Box::pin(async move {
            let domain = key.domain_id.to_string();
            let policy = ExponentialRetryPolicy::new(Duration::from_secs(1))
                .with_maximum_interval(Duration::from_secs(10))
                .with_expiration_interval(Duration::from_secs(300));
            let mut retrier = Retrier::new(policy);
            loop {
                let result: Result<Option<bool>, Status> = async {
                    if domains
                        .is_active_in(&domain, &cluster)
                        .map_err(|error| Status::not_found(error.message))?
                    {
                        return Ok(Some(false));
                    }
                    let description = domains
                        .describe(&cadence_api::service_types::DescribeDomainRequest {
                            uuid: Some(domain.clone()),
                            ..Default::default()
                        })
                        .map_err(|error| Status::not_found(error.message))?;
                    let active_active = description
                        .replication_configuration
                        .as_ref()
                        .and_then(|replication| replication.active_clusters.as_ref())
                        .and_then(|active| active.active_clusters_by_cluster_attribute.as_ref())
                        .is_some_and(|scopes| {
                            scopes.values().any(|scope| {
                                scope
                                    .cluster_attributes
                                    .as_ref()
                                    .is_some_and(|attributes| !attributes.is_empty())
                            })
                        });
                    if active_active
                        || enabled
                            .as_ref()
                            .is_some_and(|enabled| !enabled(&domain, &key.name, key.task_type))
                    {
                        return Ok(Some(false));
                    }
                    let started = match history
                        .describe_execution(
                            &domain,
                            description
                                .domain_info
                                .as_ref()
                                .map(|info| info.name.as_str())
                                .unwrap_or_default(),
                            &task,
                        )
                        .await
                    {
                        Ok(state) => task_started(key.task_type, task.data.schedule_id, &state),
                        Err(error) if crate::matching_history::missing_execution(&error) => {
                            Ok(super::grpc_matching::matching_now()
                                .saturating_sub(task.data.created_nanos)
                                > 86_400_000_000_000)
                        }
                        Err(error) => Err(error),
                    }?;
                    Ok(started.then_some(true))
                }
                .await;
                if let Ok(Some(completed)) = result {
                    return Ok(completed);
                }
                let Some(delay) = retrier.next_backoff(&mut rand::thread_rng()) else {
                    return result.and_then(|_| Err(Status::unavailable("task not started")));
                };
                tokio::time::sleep(delay).await;
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state() -> h::DescribeWorkflowExecutionResponse {
        h::DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(p::WorkflowExecutionInfo::default()),
            ..Default::default()
        }
    }
    #[test]
    fn matching_standby_original_go_completion_decisions() {
        #[derive(serde::Deserialize)]
        struct Case {
            task_type: i32,
            schedule_id: i64,
            pending_id: i64,
            state: i32,
            closed: bool,
            started: bool,
            error: bool,
        }
        let cases: Vec<Case> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-standby.json"
        ))
        .unwrap();
        for case in cases {
            let mut state = state();
            if case.closed {
                state.workflow_execution_info.as_mut().unwrap().close_status =
                    p::WorkflowExecutionCloseStatus::Completed as i32;
            }
            if case.pending_id > 0 {
                if case.task_type == 0 {
                    state.pending_decision = Some(p::PendingDecisionInfo {
                        schedule_id: case.pending_id,
                        state: case.state + 1,
                        ..Default::default()
                    });
                } else {
                    state.pending_activities.push(p::PendingActivityInfo {
                        schedule_id: case.pending_id,
                        state: case.state + 1,
                        ..Default::default()
                    });
                }
            }
            let actual = task_started(case.task_type, case.schedule_id, &state);
            assert_eq!(actual.is_err(), case.error);
            if let Ok(started) = actual {
                assert_eq!(
                    started, case.started,
                    "type={} pending={} state={} closed={}",
                    case.task_type, case.pending_id, case.state, case.closed
                );
            }
        }
    }

    #[test]
    fn standby_decision_uses_schedule_order_and_started_state() {
        let mut state = state();
        assert!(task_started(0, 10, &state).unwrap());
        state.pending_decision = Some(p::PendingDecisionInfo {
            schedule_id: 10,
            state: p::PendingDecisionState::Scheduled as i32,
            ..Default::default()
        });
        assert!(!task_started(0, 10, &state).unwrap());
        assert!(task_started(0, 9, &state).unwrap());
        assert!(!task_started(0, 11, &state).unwrap());
        state.pending_decision.as_mut().unwrap().state = p::PendingDecisionState::Started as i32;
        assert!(task_started(0, 10, &state).unwrap());
    }
    #[test]
    fn standby_activity_requires_started_or_absent_activity() {
        let mut state = state();
        assert!(task_started(1, 10, &state).unwrap());
        state.pending_activities.push(p::PendingActivityInfo {
            schedule_id: 10,
            state: p::PendingActivityState::Scheduled as i32,
            ..Default::default()
        });
        assert!(!task_started(1, 10, &state).unwrap());
        state.pending_activities[0].state = p::PendingActivityState::Started as i32;
        assert!(task_started(1, 10, &state).unwrap());
        state.pending_activities[0].state = p::PendingActivityState::CancelRequested as i32;
        assert!(!task_started(1, 10, &state).unwrap());
        state.workflow_execution_info.as_mut().unwrap().close_status =
            p::WorkflowExecutionCloseStatus::Completed as i32;
        assert!(task_started(1, 10, &state).unwrap());
        assert!(task_started(0, 10, &h::DescribeWorkflowExecutionResponse::default()).is_err());
    }
}
