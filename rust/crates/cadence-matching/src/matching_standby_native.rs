//! Byte-preserving Go taskCompleter for standby backlog (native protobuf).
//!
//! Same decisions, metrics and retry semantics as `matching_standby`, but the
//! History control call is `MatchingHistoryClient::describe_execution_native`
//! and no identifier (domain id/name, workflow id, run id, task list) is ever
//! converted to `String`. The domain cache is UTF-8 only, so the caller supplies
//! an explicit [`DomainResolverNative`] that returns the raw domain name.
//!
//! The retry helpers below intentionally mirror the private ones in
//! `matching_standby.rs` (that file is not edited by this port).
use cadence_common::{go_string::GoString, ExponentialRetryPolicy, Retrier};
use cadence_persistence::matching_native_task_store::{AllocatedTaskBytes, TaskListKeyBytes};
use cadence_proto::{api as p, native as n};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tonic::Status;

/// What the standby completer needs from the domain cache for one domain id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandbyDomainNative {
    /// Go `IsActiveIn(cluster)`.
    pub active_in_cluster: bool,
    /// Any active-active cluster attribute scope with attributes.
    pub active_active: bool,
    /// Raw domain name bytes (GoString), passed to History unchanged.
    pub name: GoString,
}

/// Domain lookup by raw domain id. Errors are retried until the retry policy
/// expires and are then returned unchanged (Go: domain cache error).
pub type DomainResolverNative =
    Arc<dyn Fn(&GoString) -> Result<StandbyDomainNative, Status> + Send + Sync>;
/// (domain id, task list name, persistence task type) -> standby completion enabled.
pub type EnabledNative = Arc<dyn Fn(&GoString, &GoString, i32) -> bool + Send + Sync>;
pub type DispatchCheckNative = Arc<
    dyn Fn(AllocatedTaskBytes) -> Pin<Box<dyn Future<Output = Result<bool, Status>> + Send>>
        + Send
        + Sync,
>;
type Describe = Arc<
    dyn Fn(
            GoString,
            GoString,
            AllocatedTaskBytes,
        ) -> Pin<
            Box<dyn Future<Output = Result<n::history::DescribeWorkflowExecutionResponse, Status>> + Send>,
        > + Send
        + Sync,
>;

pub fn task_started_native(
    task_type: i32,
    schedule_id: i64,
    state: &n::history::DescribeWorkflowExecutionResponse,
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

/// Go `common.CreateTaskCompleterRetryPolicy`: 1s initial, 10s maximum, 300s expiration.
fn completer_retry_policy() -> ExponentialRetryPolicy {
    ExponentialRetryPolicy::new(Duration::from_secs(1))
        .with_maximum_interval(Duration::from_secs(10))
        .with_expiration_interval(Duration::from_secs(300))
}

async fn retry_completion<F, Fut, S, SFut, E>(
    policy: ExponentialRetryPolicy,
    mut attempt: F,
    mut sleep: S,
    mut elapsed: E,
) -> Result<Option<bool>, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<bool>, Status>>,
    S: FnMut(Duration) -> SFut,
    SFut: Future<Output = ()>,
    E: FnMut() -> Duration,
{
    let mut retrier = Retrier::new(policy);
    loop {
        let result = attempt().await;
        if matches!(result, Ok(Some(_))) {
            return result;
        }
        let Some(delay) = retrier.next_backoff_at(&mut rand::thread_rng(), elapsed()) else {
            return result;
        };
        sleep(delay).await;
    }
}

struct Deps {
    domains: DomainResolverNative,
    cluster_enabled: Option<EnabledNative>,
    describe: Describe,
    key: TaskListKeyBytes,
    metrics: crate::matching_metrics::MatchingMetrics,
    context: crate::matching_metrics::PollMetricContext,
}

/// One completer attempt: `Ok(Some(b))` is final, `Ok(None)` and `Err` retry.
async fn attempt(deps: &Deps, domain_id: &GoString, task: &AllocatedTaskBytes) -> Result<Option<bool>, Status> {
    let domain = (deps.domains)(domain_id)?;
    if domain.active_in_cluster {
        return Ok(Some(false));
    }
    if domain.active_active
        || deps
            .cluster_enabled
            .as_ref()
            .is_some_and(|enabled| !enabled(domain_id, &deps.key.name, deps.key.task_type))
    {
        return Ok(Some(false));
    }
    match (deps.describe)(domain_id.clone(), domain.name, task.clone()).await {
        Ok(state) => {
            let started = task_started_native(deps.key.task_type, task.data.schedule_id, &state)?;
            let metric = if started {
                crate::matching_metrics::STANDBY_TASKS_COMPLETED
            } else {
                crate::matching_metrics::STANDBY_TASKS_NOT_STARTED
            };
            deps.metrics.task_list_counter_event(metric, &deps.context);
            Ok(started.then_some(true))
        }
        Err(error) if crate::matching_history::missing_execution(&error) => {
            let result = missing_execution_result(
                super::grpc_matching::matching_now().saturating_sub(task.data.created_nanos),
            );
            if matches!(result, Ok(Some(true))) {
                deps.metrics
                    .task_list_counter_event(crate::matching_metrics::STANDBY_TASKS_COMPLETED, &deps.context);
            }
            result
        }
        Err(error) => Err(error),
    }
}

async fn run<S, SFut, E>(
    deps: Arc<Deps>,
    task: AllocatedTaskBytes,
    policy: ExponentialRetryPolicy,
    sleep: S,
    elapsed: E,
) -> Result<bool, Status>
where
    S: FnMut(Duration) -> SFut,
    SFut: Future<Output = ()>,
    E: FnMut() -> Duration,
{
    let mut cancellation = CompletionCancellation {
        metrics: &deps.metrics,
        context: &deps.context,
        armed: true,
    };
    let domain_id = GoString::from(deps.key.domain_id.to_string());
    let result = retry_completion(
        policy,
        || {
            let deps = deps.clone();
            let domain_id = domain_id.clone();
            let task = task.clone();
            async move { attempt(&deps, &domain_id, &task).await }
        },
        sleep,
        elapsed,
    )
    .await;
    cancellation.armed = false;
    match result {
        Ok(Some(completed)) => Ok(completed),
        result => exhausted(result, &deps.metrics, &deps.context),
    }
}

fn dispatch(deps: Deps) -> DispatchCheckNative {
    let deps = Arc::new(deps);
    Arc::new(move |task| {
        let deps = deps.clone();
        Box::pin(async move {
            let started = std::time::Instant::now();
            run(deps, task, completer_retry_policy(), tokio::time::sleep, move || {
                started.elapsed()
            })
            .await
        })
    })
}

pub fn checker_native(
    domains: DomainResolverNative,
    history: crate::matching_history::MatchingHistoryClient,
    key: TaskListKeyBytes,
    enabled: Option<EnabledNative>,
    metrics: crate::matching_metrics::MatchingMetrics,
    metric_context: crate::matching_metrics::PollMetricContext,
) -> DispatchCheckNative {
    let describe: Describe = Arc::new(move |domain_id, domain_name, task| {
        let history = history.clone();
        Box::pin(async move {
            history
                .describe_execution_native(&domain_id, &domain_name, &task)
                .await
        })
    });
    dispatch(Deps {
        domains,
        cluster_enabled: enabled,
        describe,
        key,
        metrics,
        context: metric_context,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_metrics::*;
    use cadence_persistence::matching_blob::TaskDataBytes;
    use std::sync::Mutex;

    fn key(task_type: i32) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: uuid::Uuid::from_u128(3),
            name: GoString::from(&b"list-\xff"[..]),
            task_type,
        }
    }
    fn context() -> PollMetricContext {
        PollMetricContext {
            domain: "domain".into(),
            task_list: "list".into(),
            task_list_kind: 0,
            task_type: "decision_task",
            poller_isolation_group: String::new(),
        }
    }
    fn task(schedule_id: i64, created_nanos: i64) -> AllocatedTaskBytes {
        AllocatedTaskBytes {
            task_id: 1,
            data: TaskDataBytes {
                workflow_id: GoString::from(&b"wf-\xff\xfe"[..]),
                run_id: None,
                raw_run_id: Some(GoString::from(&b"run-\xfe"[..])),
                schedule_id,
                created_nanos,
                ..Default::default()
            },
        }
    }
    fn state() -> n::history::DescribeWorkflowExecutionResponse {
        n::history::DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(n::api::WorkflowExecutionInfo::default()),
            ..Default::default()
        }
    }
    fn with_decision(schedule_id: i64, s: i32) -> n::history::DescribeWorkflowExecutionResponse {
        let mut r = state();
        r.pending_decision = Some(n::api::PendingDecisionInfo {
            schedule_id,
            state: s,
            ..Default::default()
        });
        r
    }
    fn with_activity(schedule_id: i64, s: i32) -> n::history::DescribeWorkflowExecutionResponse {
        let mut r = state();
        r.pending_activities.push(n::api::PendingActivityInfo {
            schedule_id,
            state: s,
            ..Default::default()
        });
        r
    }
    fn resolver(active: bool, active_active: bool) -> DomainResolverNative {
        Arc::new(move |_| {
            Ok(StandbyDomainNative {
                active_in_cluster: active,
                active_active,
                name: GoString::from(&b"dom-\xff"[..]),
            })
        })
    }
    type Seen = Arc<Mutex<Vec<(GoString, GoString, GoString)>>>;
    fn deps(
        domains: DomainResolverNative,
        task_type: i32,
        reply: impl Fn() -> Result<n::history::DescribeWorkflowExecutionResponse, Status> + Send + Sync + 'static,
    ) -> (Arc<Deps>, Seen, Arc<RecordingMetricSink>) {
        let sink = Arc::new(RecordingMetricSink::default());
        let seen: Seen = Arc::default();
        let recorded = seen.clone();
        let describe: Describe = Arc::new(move |id, name, task| {
            recorded.lock().unwrap().push((id, name, task.data.workflow_id.clone()));
            let r = reply();
            Box::pin(async move { r })
        });
        let d = Arc::new(Deps {
            domains,
            cluster_enabled: None,
            describe,
            key: key(task_type),
            metrics: MatchingMetrics::new(sink.clone()),
            context: context(),
        });
        (d, seen, sink)
    }
    fn count(sink: &RecordingMetricSink, name: &str) -> usize {
        sink.events().iter().filter(|e| e.name == name).count()
    }
    async fn once(deps: Arc<Deps>, t: AllocatedTaskBytes) -> Result<bool, Status> {
        // One attempt only: no retries, so an unfinished attempt surfaces as exhaustion.
        run(
            deps,
            t,
            ExponentialRetryPolicy::new(Duration::from_millis(1))
                .with_expiration_interval(Duration::from_nanos(1)),
            |_| std::future::ready(()),
            || Duration::from_nanos(2),
        )
        .await
    }

    #[test]
    fn started_decisions_and_activities_match_go() {
        let started = p::PendingDecisionState::Started as i32;
        let scheduled = p::PendingDecisionState::Scheduled as i32;
        assert!(task_started_native(0, 5, &state()).unwrap());
        assert!(task_started_native(0, 4, &with_decision(5, scheduled)).unwrap());
        assert!(task_started_native(0, 5, &with_decision(5, started)).unwrap());
        assert!(!task_started_native(0, 5, &with_decision(5, scheduled)).unwrap());
        assert!(!task_started_native(0, 6, &with_decision(5, started)).unwrap());
        let a_started = p::PendingActivityState::Started as i32;
        let a_sched = p::PendingActivityState::Scheduled as i32;
        assert!(task_started_native(1, 5, &state()).unwrap());
        assert!(task_started_native(1, 5, &with_activity(5, a_started)).unwrap());
        assert!(task_started_native(1, 5, &with_activity(6, a_sched)).unwrap());
        assert!(!task_started_native(1, 5, &with_activity(5, a_sched)).unwrap());
        let mut closed = with_decision(5, scheduled);
        closed.workflow_execution_info.as_mut().unwrap().close_status = 1;
        assert!(task_started_native(0, 5, &closed).unwrap());
        assert_eq!(
            task_started_native(0, 1, &Default::default()).unwrap_err().message(),
            "workflow execution info is nil"
        );
        assert_eq!(
            task_started_native(2, 1, &state()).unwrap_err().message(),
            "task type not supported"
        );
    }

    #[tokio::test]
    async fn raw_identifiers_reach_history_unchanged_and_complete() {
        let (d, seen, sink) = deps(resolver(false, false), 0, || Ok(state()));
        assert!(once(d, task(1, 0)).await.unwrap());
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0.as_bytes(), uuid::Uuid::from_u128(3).to_string().as_bytes());
        assert_eq!(seen[0].1.as_bytes(), b"dom-\xff");
        assert_eq!(seen[0].2.as_bytes(), b"wf-\xff\xfe");
        assert_eq!(count(&sink, STANDBY_TASKS_COMPLETED), 1);
        assert_eq!(count(&sink, STANDBY_TASKS_NOT_STARTED), 0);
    }

    #[tokio::test]
    async fn active_domain_and_active_active_are_not_completed_without_history_call() {
        for (active, aa) in [(true, false), (false, true)] {
            let (d, seen, sink) = deps(resolver(active, aa), 0, || Ok(state()));
            assert!(!once(d, task(1, 0)).await.unwrap());
            assert!(seen.lock().unwrap().is_empty());
            assert!(sink.events().is_empty());
        }
    }

    #[tokio::test]
    async fn disabled_by_dynamic_config_receives_raw_task_list_and_is_not_completed() {
        let (d, seen, _) = deps(resolver(false, false), 1, || Ok(state()));
        type SeenConfigKeys = Arc<Mutex<Vec<(Vec<u8>, i32)>>>;
        let got: SeenConfigKeys = Arc::default();
        let g = got.clone();
        let mut d = Arc::into_inner(d).unwrap();
        d.cluster_enabled = Some(Arc::new(move |_, name, ty| {
            g.lock().unwrap().push((name.as_bytes().to_vec(), ty));
            false
        }));
        assert!(!once(Arc::new(d), task(1, 0)).await.unwrap());
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(*got.lock().unwrap(), vec![(b"list-\xff".to_vec(), 1)]);
    }

    #[tokio::test]
    async fn not_started_counts_not_started_and_not_failure_after_exhaustion() {
        let scheduled = p::PendingDecisionState::Scheduled as i32;
        let (d, seen, sink) = deps(resolver(false, false), 0, move || Ok(with_decision(5, scheduled)));
        let err = run(
            d,
            task(5, 0),
            ExponentialRetryPolicy::new(Duration::from_millis(1)).with_maximum_attempts(2),
            |_| std::future::ready(()),
            || Duration::ZERO,
        )
        .await
        .unwrap_err();
        assert_eq!(err.message(), "task not started");
        assert_eq!(seen.lock().unwrap().len(), 3);
        assert_eq!(count(&sink, STANDBY_TASKS_NOT_STARTED), 3);
        assert_eq!(count(&sink, STANDBY_TASKS_COMPLETION_FAILURE), 0);
    }

    #[tokio::test]
    async fn domain_lookup_and_history_errors_retry_then_return_original_error_once() {
        for which in ["error-getting-domain-by-id", "error-describing-workflow-execution"] {
            let domains: DomainResolverNative = if which.starts_with("error-getting") {
                Arc::new(|_| Err(Status::unavailable("error-getting-domain-by-id")))
            } else {
                resolver(false, false)
            };
            let (d, seen, sink) =
                deps(domains, 0, || Err(Status::unavailable("error-describing-workflow-execution")));
            let err = run(
                d,
                task(1, 0),
                ExponentialRetryPolicy::new(Duration::from_millis(1)).with_maximum_attempts(3),
                |_| std::future::ready(()),
                || Duration::ZERO,
            )
            .await
            .unwrap_err();
            assert_eq!(err.message(), which);
            assert_eq!(
                seen.lock().unwrap().len(),
                if which.starts_with("error-getting") { 0 } else { 4 }
            );
            assert_eq!(count(&sink, STANDBY_TASKS_COMPLETION_FAILURE), 1, "{which}");
        }
    }

    #[tokio::test]
    async fn young_missing_execution_waits_and_counts_failure_only_after_exhaustion() {
        // EntityNotExists detail is only recognised by `missing_execution`; an
        // unrecognised NotFound must be returned as a plain History error.
        let (d, _, sink) = deps(resolver(false, false), 0, || Err(Status::not_found("plain")));
        let err = once(d, task(1, 0)).await.unwrap_err();
        assert_eq!(err.message(), "plain");
        assert_eq!(count(&sink, STANDBY_TASKS_COMPLETION_FAILURE), 1);
        assert_eq!(count(&sink, STANDBY_TASKS_COMPLETED), 0);
        assert_eq!(missing_execution_result(86_400_000_000_000).unwrap_err().code(), tonic::Code::Unavailable);
        assert_eq!(missing_execution_result(86_400_000_000_001).unwrap(), Some(true));
    }

    #[tokio::test]
    async fn typed_missing_execution_uses_age_and_counts_completion_or_exhaustion() {
        for old in [false, true] {
            let (d, seen, sink) = deps(resolver(false, false), 0, || {
                Err(crate::matching_native_status::raw_detailed_status(
                    tonic::Code::NotFound,
                    &GoString::from("entity not exists"),
                    "EntityNotExistsError",
                    Vec::new(),
                ))
            });
            let created = crate::grpc_matching::matching_now()
                - if old { 86_401_000_000_000 } else { 0 };
            let result = once(d, task(1, created)).await;
            assert_eq!(seen.lock().unwrap().len(), 1);
            if old {
                assert!(result.unwrap());
                assert_eq!(count(&sink, STANDBY_TASKS_COMPLETED), 1);
                assert_eq!(count(&sink, STANDBY_TASKS_COMPLETION_FAILURE), 0);
            } else {
                assert_eq!(result.unwrap_err().message(),
                    "wait time not reached for workflow EntityNotExistsError");
                assert_eq!(count(&sink, STANDBY_TASKS_COMPLETED), 0);
                assert_eq!(count(&sink, STANDBY_TASKS_COMPLETION_FAILURE), 1);
            }
            assert_eq!(count(&sink, STANDBY_TASKS_NOT_STARTED), 0);
        }
    }

    #[tokio::test]
    async fn cancelled_checker_counts_one_completion_failure() {
        let (d, _, sink) = deps(Arc::new(|_| Err(Status::unavailable("x"))), 0, || Ok(state()));
        let check = dispatch(Arc::into_inner(d).unwrap());
        let mut future = check(task(1, 0));
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut future).await.is_err());
        assert!(sink.events().is_empty());
        drop(future);
        assert_eq!(count(&sink, STANDBY_TASKS_COMPLETION_FAILURE), 1);
    }
}
