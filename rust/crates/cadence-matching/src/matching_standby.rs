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

// Keep the young-missing-execution sentinel distinct from task-not-started:
// Go counts the former, but excludes the latter, after retry exhaustion.
fn missing_execution_result(age_nanos: i64) -> Result<Option<bool>, Status> {
    if age_nanos > 86_400_000_000_000 {
        Ok(Some(true))
    } else {
        Err(Status::unavailable(
            "wait time not reached for workflow EntityNotExistsError",
        ))
    }
}

fn exhausted(
    result: Result<Option<bool>, Status>,
    metrics: &crate::matching_metrics::MatchingMetrics,
    context: &crate::matching_metrics::PollMetricContext,
) -> Result<bool, Status> {
    if result.is_err() {
        metrics.task_list_counter_event(
            crate::matching_metrics::STANDBY_TASKS_COMPLETION_FAILURE,
            context,
        );
    }
    result.and_then(|_| Err(Status::unavailable("task not started")))
}

// Dropping a running checker is Rust's context cancellation (reader shutdown).
// Go ThrottleRetry returns context.Canceled and counts that as a failure.
struct CompletionCancellation<'a> {
    metrics: &'a crate::matching_metrics::MatchingMetrics,
    context: &'a crate::matching_metrics::PollMetricContext,
    armed: bool,
}
impl Drop for CompletionCancellation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.metrics.task_list_counter_event(
                crate::matching_metrics::STANDBY_TASKS_COMPLETION_FAILURE,
                self.context,
            );
        }
    }
}

pub fn checker(
    domains: cadence_persistence::InMemoryDomainStore,
    cluster: String,
    history: crate::matching_history::MatchingHistoryClient,
    key: cadence_persistence::matching_sqlite::TaskListKey,
    enabled: Option<Enabled>,
    metrics: crate::matching_metrics::MatchingMetrics,
    metric_context: crate::matching_metrics::PollMetricContext,
) -> crate::matching_reader::DispatchCheck {
    Arc::new(move |task| {
        let domains = domains.clone();
        let cluster = cluster.clone();
        let history = history.clone();
        let key = key.clone();
        let enabled = enabled.clone();
        let metrics = metrics.clone();
        let metric_context = metric_context.clone();
        Box::pin(async move {
            let mut cancellation = CompletionCancellation {
                metrics: &metrics,
                context: &metric_context,
                armed: true,
            };
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
                    let completed = match history
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
                        Ok(state) => {
                            let started =
                                task_started(key.task_type, task.data.schedule_id, &state)?;
                            let metric = if started {
                                crate::matching_metrics::STANDBY_TASKS_COMPLETED
                            } else {
                                crate::matching_metrics::STANDBY_TASKS_NOT_STARTED
                            };
                            metrics.task_list_counter_event(metric, &metric_context);
                            Ok(started.then_some(true))
                        }
                        Err(error) if crate::matching_history::missing_execution(&error) => {
                            let result = missing_execution_result(
                                super::grpc_matching::matching_now()
                                    .saturating_sub(task.data.created_nanos),
                            );
                            if matches!(result, Ok(Some(true))) {
                                metrics.task_list_counter_event(
                                    crate::matching_metrics::STANDBY_TASKS_COMPLETED,
                                    &metric_context,
                                );
                            }
                            result
                        }
                        Err(error) => Err(error),
                    }?;
                    Ok(completed)
                }
                .await;
                if let Ok(Some(completed)) = result {
                    cancellation.armed = false;
                    return Ok(completed);
                }
                let Some(delay) = retrier.next_backoff(&mut rand::thread_rng()) else {
                    cancellation.armed = false;
                    return exhausted(result, &metrics, &metric_context);
                };
                tokio::time::sleep(delay).await;
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn standby_cancelled_checker_counts_one_completion_failure() {
        use crate::matching_metrics::*;
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let check = checker(
            cadence_persistence::InMemoryDomainStore::default(),
            "local".into(),
            crate::matching_history::MatchingHistoryClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
                Duration::from_secs(1),
                32 * 1024 * 1024,
            ),
            cadence_persistence::matching_sqlite::TaskListKey {
                domain_id: uuid::Uuid::from_u128(1),
                name: "list".into(),
                task_type: 0,
            },
            None,
            metrics,
            PollMetricContext {
                domain: "domain".into(),
                task_list: "list".into(),
                task_list_kind: 0,
                task_type: "decision_task",
                poller_isolation_group: String::new(),
            },
        );
        let mut future = check(cadence_persistence::matching_sqlite::AllocatedTask {
            task_id: 1,
            data: cadence_persistence::matching_blob::TaskData::default(),
        });
        // Missing domain is retryable; cancel the actual checker during backoff.
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut future)
            .await
            .is_err());
        assert!(sink.events().is_empty());
        drop(future);
        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, STANDBY_TASKS_COMPLETION_FAILURE);
        assert_eq!(events[0].tags[DOMAIN_TAG], "domain");
    }

    #[test]
    fn standby_completion_failure_excludes_not_started_but_counts_young_missing() {
        use crate::matching_metrics::*;
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let context = PollMetricContext {
            domain: "domain".into(),
            task_list: "list".into(),
            task_list_kind: 0,
            task_type: "decision_task",
            poller_isolation_group: String::new(),
        };
        assert!(exhausted(Ok(None), &metrics, &context).is_err());
        assert!(sink.events().is_empty());
        // Shutdown cancels an in-flight completer; normal completion disarms it.
        drop(CompletionCancellation {
            metrics: &metrics,
            context: &context,
            armed: false,
        });
        assert!(sink.events().is_empty());
        drop(CompletionCancellation {
            metrics: &metrics,
            context: &context,
            armed: true,
        });
        for error in [
            Status::internal("history failed"),
            Status::cancelled("cancelled"),
            Status::deadline_exceeded("deadline"),
        ] {
            assert!(exhausted(Err(error), &metrics, &context).is_err());
        }
        assert!(exhausted(
            missing_execution_result(86_400_000_000_000),
            &metrics,
            &context
        )
        .is_err());
        assert_eq!(
            missing_execution_result(86_400_000_000_001).unwrap(),
            Some(true)
        );
        assert_eq!(
            sink.events()
                .iter()
                .filter(|e| e.name == STANDBY_TASKS_COMPLETION_FAILURE)
                .count(),
            5
        );
    }

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
