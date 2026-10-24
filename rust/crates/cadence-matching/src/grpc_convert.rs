//! Conversions mirror go/common/types/mapper/proto. Thrift enums start at
//! zero; protobuf enums reserve zero for INVALID. Time in the current Rust
//! engine is milliseconds; protobuf Timestamp is seconds plus nanoseconds.
use cadence_api::{enums as e, schedule as tk_schedule, service_types as st, tasks as tk, types as t};
use cadence_proto::api as p;
use tonic::Status;

pub fn timestamp(ms: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ms.div_euclid(1000),
        nanos: (ms.rem_euclid(1000) * 1_000_000) as i32,
    }
}
pub fn timestamp_nanos(ns: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ns.div_euclid(1_000_000_000),
        nanos: ns.rem_euclid(1_000_000_000) as i32,
    }
}
pub fn duration(seconds: i64) -> prost_types::Duration {
    prost_types::Duration { seconds, nanos: 0 }
}
pub fn seconds(v: Option<prost_types::Duration>) -> Result<Option<i32>, Status> {
    v.map(|d| {
        if d.nanos.unsigned_abs() >= 1_000_000_000
            || (d.seconds > 0 && d.nanos < 0)
            || (d.seconds < 0 && d.nanos > 0)
        {
            return Err(Status::invalid_argument("Invalid protobuf duration."));
        }
        i32::try_from(d.seconds)
            .map_err(|_| Status::invalid_argument("Duration exceeds the Cadence seconds range."))
    })
    .transpose()
}
pub fn execution(v: p::WorkflowExecution) -> t::WorkflowExecution {
    t::WorkflowExecution {
        workflow_id: v.workflow_id,
        run_id: v.run_id,
    }
}
pub fn execution_out(v: t::WorkflowExecution) -> p::WorkflowExecution {
    p::WorkflowExecution {
        workflow_id: v.workflow_id,
        run_id: v.run_id,
    }
}
pub fn task_list(v: p::TaskList) -> Result<t::TaskList, Status> {
    Ok(t::TaskList {
        name: v.name,
        base_name: v.base_name,
        kind: enum_in(v.kind, e::TaskListKind::from_i32)?,
    })
}
pub fn task_list_out(v: t::TaskList) -> p::TaskList {
    p::TaskList {
        name: v.name,
        base_name: v.base_name,
        kind: v.kind.map_or(0, |k| k as i32 + 1),
    }
}
pub fn retry(v: p::RetryPolicy) -> Result<t::RetryPolicy, Status> {
    Ok(t::RetryPolicy {
        initial_interval_in_seconds: seconds(v.initial_interval)?.unwrap_or_default(),
        maximum_interval_in_seconds: seconds(v.maximum_interval)?.unwrap_or_default(),
        expiration_interval_in_seconds: seconds(v.expiration_interval)?.unwrap_or_default(),
        maximum_attempts: v.maximum_attempts,
        backoff_coefficient: v.backoff_coefficient,
        non_retriable_error_reasons: v.non_retryable_error_reasons,
        jitter_coefficient: v.jitter_coefficient,
    })
}
pub fn retry_out(v: t::RetryPolicy) -> p::RetryPolicy {
    p::RetryPolicy {
        initial_interval: Some(duration(v.initial_interval_in_seconds.into())),
        maximum_interval: Some(duration(v.maximum_interval_in_seconds.into())),
        expiration_interval: Some(duration(v.expiration_interval_in_seconds.into())),
        maximum_attempts: v.maximum_attempts,
        backoff_coefficient: v.backoff_coefficient,
        non_retryable_error_reasons: v.non_retriable_error_reasons,
        jitter_coefficient: v.jitter_coefficient,
    }
}
pub fn payload(v: Vec<u8>) -> Option<p::Payload> {
    if v.is_empty() {
        None
    } else {
        Some(p::Payload { data: v })
    }
}
pub fn data(v: Option<p::Payload>) -> Vec<u8> {
    v.map(|p| p.data).unwrap_or_default()
}
pub fn enum_in<T>(v: i32, convert: fn(i32) -> Option<T>) -> Result<Option<T>, Status> {
    if v == 0 {
        return Ok(None);
    }
    v.checked_sub(1)
        .and_then(convert)
        .map(Some)
        .ok_or_else(|| Status::invalid_argument("Unknown protobuf enum value."))
}
type FailureParts = (Option<String>, Vec<u8>, Option<t::FailureOptions>);
pub fn failure_in(v: Option<p::Failure>) -> Result<FailureParts, Status> {
    match v {
        None => Ok((None, Vec::new(), None)),
        Some(f) => Ok((
            Some(f.reason),
            f.details,
            f.options
                .map(|o| {
                    Ok::<_, Status>(t::FailureOptions {
                        failure_category: enum_in(
                            o.failure_category,
                            e::FailureCategory::from_i32,
                        )?,
                        next_retry_interval_seconds: seconds(o.next_retry_interval)?,
                    })
                })
                .transpose()?,
        )),
    }
}
#[allow(deprecated)]
pub fn active_clusters_in(v: p::ActiveClusters) -> t::ActiveClusters {
    fn info(v: p::ActiveClusterInfo) -> t::ActiveClusterInfo {
        t::ActiveClusterInfo {
            active_cluster_name: v.active_cluster_name,
            failover_version: v.failover_version,
        }
    }
    let scopes = v.active_clusters_by_cluster_attribute;
    t::ActiveClusters {
        active_clusters_by_region: None,
        active_clusters_by_cluster_attribute: (!scopes.is_empty()).then(|| scopes.into_iter().map(|(name, scope)| {
            let attributes = scope.cluster_attributes;
            (name, t::ClusterAttributeScope {
                cluster_attributes: (!attributes.is_empty()).then(|| attributes.into_iter().map(|(name, value)| (name, info(value))).collect()),
            })
        }).collect()),
    }
}
#[allow(deprecated)]
pub fn active_clusters_out(v: t::ActiveClusters) -> p::ActiveClusters {
    fn info(v: t::ActiveClusterInfo) -> p::ActiveClusterInfo {
        p::ActiveClusterInfo {
            active_cluster_name: v.active_cluster_name,
            failover_version: v.failover_version,
        }
    }
    p::ActiveClusters {
        region_to_cluster: Default::default(),
        active_clusters_by_cluster_attribute: v
            .active_clusters_by_cluster_attribute
            .unwrap_or_default()
            .into_iter()
            .map(|(k, s)| {
                (
                    k,
                    p::ClusterAttributeScope {
                        cluster_attributes: s
                            .cluster_attributes
                            .unwrap_or_default()
                            .into_iter()
                            .map(|(k, v)| (k, info(v)))
                            .collect(),
                    },
                )
            })
            .collect(),
    }
}
pub fn memo(v: t::Memo) -> p::Memo {
    p::Memo {
        fields: v
            .fields
            .unwrap_or_default()
            .into_iter()
            .map(|(k, data)| (k, p::Payload { data }))
            .collect(),
    }
}
pub fn search_attributes(v: t::SearchAttributes) -> p::SearchAttributes {
    p::SearchAttributes {
        indexed_fields: v
            .indexed_fields
            .unwrap_or_default()
            .into_iter()
            .map(|(k, data)| (k, p::Payload { data }))
            .collect(),
    }
}
pub fn header(v: t::Header) -> p::Header {
    p::Header {
        fields: v
            .fields
            .unwrap_or_default()
            .into_iter()
            .map(|(k, data)| (k, p::Payload { data }))
            .collect(),
    }
}
pub fn failure(
    reason: Option<String>,
    details: Vec<u8>,
    options: Option<t::FailureOptions>,
) -> Option<p::Failure> {
    reason.map(|reason| p::Failure {
        reason,
        details,
        options: options.map(|o| p::FailureOptions {
            failure_category: o.failure_category.map_or(0, |c| c as i32 + 1),
            next_retry_interval: o.next_retry_interval_seconds.map(|s| duration(s.into())),
        }),
    })
}
pub fn parent_info(
    domain: Option<String>,
    execution: Option<t::WorkflowExecution>,
    initiated: Option<i64>,
) -> Option<p::ParentExecutionInfo> {
    if domain.is_none() && execution.is_none() && initiated.is_none() {
        return None;
    }
    Some(p::ParentExecutionInfo {
        domain_name: domain.unwrap_or_default(),
        workflow_execution: execution.map(execution_out),
        initiated_id: initiated.unwrap_or_default(),
        domain_id: String::new(),
    })
}
pub fn external_info(
    execution: Option<t::WorkflowExecution>,
    initiated: Option<i64>,
) -> Option<p::ExternalExecutionInfo> {
    if execution.is_none() && initiated.is_none() {
        return None;
    }
    Some(p::ExternalExecutionInfo {
        workflow_execution: execution.map(execution_out),
        initiated_id: initiated.unwrap_or_default(),
    })
}
pub fn history(v: st::History) -> Result<p::History, Status> {
    Ok(p::History {
        events: v
            .events
            .into_iter()
            .map(crate::grpc_history::event)
            .collect::<Result<_, _>>()?,
    })
}
pub fn decision_task(
    v: cadence_api::tasks::PollForDecisionTaskResponse,
) -> Result<p::PollForDecisionTaskResponse, Status> {
    let mut queries = v.queries.unwrap_or_default();
    if let (Some(id), Some(q)) = (v.query_id, v.query.clone()) {
        queries.insert(id, q);
    }
    Ok(p::PollForDecisionTaskResponse {
        task_token: v.task_token,
        workflow_execution: v.workflow_execution.map(execution_out),
        workflow_type: v.workflow_type.map(|t| p::WorkflowType { name: t.name }),
        previous_started_event_id: v.previous_started_event_id,
        started_event_id: v.started_event_id,
        attempt: v.attempt,
        backlog_count_hint: v.backlog_count_hint,
        next_event_id: v.next_event_id,
        scheduled_time: v.scheduled_timestamp.map(timestamp),
        started_time: v.started_timestamp.map(timestamp),
        history: v.history.map(history).transpose()?,
        next_page_token: v.next_page_token,
        workflow_execution_task_list: v.workflow_execution_task_list.map(task_list_out),
        query: v.query.map(|q| p::WorkflowQuery {
            query_type: q.query_type,
            query_args: payload(q.query_args),
        }),
        queries: queries
            .into_iter()
            .map(|(k, q)| {
                (
                    k,
                    p::WorkflowQuery {
                        query_type: q.query_type,
                        query_args: payload(q.query_args),
                    },
                )
            })
            .collect(),
        ..Default::default()
    })
}
pub fn activity_task(
    v: cadence_api::tasks::PollForActivityTaskResponse,
) -> p::PollForActivityTaskResponse {
    p::PollForActivityTaskResponse {
        task_token: v.task_token,
        workflow_execution: v.workflow_execution.map(execution_out),
        activity_id: v.activity_id,
        activity_type: v.activity_type.map(|t| p::ActivityType { name: t.name }),
        input: payload(v.input),
        scheduled_time: v.scheduled_timestamp.map(timestamp),
        started_time: v.started_timestamp.map(timestamp),
        schedule_to_close_timeout: v
            .schedule_to_close_timeout_seconds
            .map(|s| duration(s.into())),
        start_to_close_timeout: v.start_to_close_timeout_seconds.map(|s| duration(s.into())),
        heartbeat_timeout: v.heartbeat_timeout_seconds.map(|s| duration(s.into())),
        attempt: v.attempt,
        scheduled_time_of_this_attempt: v.scheduled_timestamp_of_this_attempt.map(timestamp),
        heartbeat_details: payload(v.heartbeat_details),
        workflow_type: v.workflow_type.map(|t| p::WorkflowType { name: t.name }),
        workflow_domain: v.workflow_domain,
        header: v.header.map(header),
        ..Default::default()
    }
}
pub fn start(
    v: p::StartWorkflowExecutionRequest,
) -> Result<t::StartWorkflowExecutionRequest, Status> {
    if v.active_cluster_selection_policy.is_some() {
        return Err(Status::unimplemented(
            "Active cluster selection policy has not been ported.",
        ));
    }
    Ok(t::StartWorkflowExecutionRequest {
        domain: v.domain,
        workflow_id: v.workflow_id,
        workflow_type: v.workflow_type.map(|x| t::WorkflowType { name: x.name }),
        task_list: v.task_list.map(task_list).transpose()?,
        input: data(v.input),
        execution_start_to_close_timeout_seconds: seconds(v.execution_start_to_close_timeout)?,
        task_start_to_close_timeout_seconds: seconds(v.task_start_to_close_timeout)?,
        identity: v.identity,
        request_id: v.request_id,
        workflow_id_reuse_policy: enum_in(
            v.workflow_id_reuse_policy,
            e::WorkflowIDReusePolicy::from_i32,
        )?,
        retry_policy: v.retry_policy.map(retry).transpose()?,
        cron_schedule: v.cron_schedule,
        memo: v.memo.map(|m| t::Memo {
            fields: Some(m.fields.into_iter().map(|(k, v)| (k, v.data)).collect()),
        }),
        search_attributes: v.search_attributes.map(|m| t::SearchAttributes {
            indexed_fields: Some(
                m.indexed_fields
                    .into_iter()
                    .map(|(k, v)| (k, v.data))
                    .collect(),
            ),
        }),
        header: v.header.map(|m| t::Header {
            fields: Some(m.fields.into_iter().map(|(k, v)| (k, v.data)).collect()),
        }),
        delay_start_seconds: seconds(v.delay_start)?,
        jitter_start_seconds: seconds(v.jitter_start)?,
        first_run_at_timestamp: v
            .first_run_at
            .map(|t| t.seconds * 1000 + i64::from(t.nanos) / 1_000_000),
        cron_overlap_policy: enum_in(v.cron_overlap_policy, e::CronOverlapPolicy::from_i32)?,
    })
}
pub fn info(v: st::WorkflowExecutionInfo) -> p::WorkflowExecutionInfo {
    p::WorkflowExecutionInfo {
        workflow_execution: v.execution.map(execution_out),
        r#type: v.workflow_type.map(|t| p::WorkflowType { name: t.name }),
        start_time: v.start_time.map(timestamp),
        close_time: v.close_time.map(timestamp),
        close_status: v.close_status.map_or(0, |s| s as i32 + 1),
        history_length: v.history_length,
        memo: v.memo.map(memo),
        search_attributes: v.search_attributes.map(search_attributes),
        parent_execution_info: parent_info(
            v.parent_domain,
            v.parent_execution,
            v.parent_initiated_id,
        ),
        execution_time: v.execution_time.map(timestamp),
        task_list: v
            .task_list
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default(),
        task_list_info: v.task_list.map(task_list_out),
        is_cron: v.is_cron,
        cron_schedule: v.cron_schedule.unwrap_or_default(),
        cron_overlap_policy: v.cron_overlap_policy.map_or(0, |s| s as i32 + 1),
        update_time: v.update_time.map(timestamp),
        partition_config: v.partition_config.unwrap_or_default(),
        execution_status: v.execution_status.map_or(0, |s| s as i32 + 1),
        scheduled_execution_time: v.scheduled_execution_time.map(timestamp),
        ..Default::default()
    }
}
pub fn describe(v: st::DescribeWorkflowExecutionResponse) -> p::DescribeWorkflowExecutionResponse {
    p::DescribeWorkflowExecutionResponse {
        execution_configuration: v.execution_configuration.map(|c| {
            p::WorkflowExecutionConfiguration {
                task_list: c.task_list.map(task_list_out),
                execution_start_to_close_timeout: c
                    .execution_start_to_close_timeout_seconds
                    .map(|s| duration(s.into())),
                task_start_to_close_timeout: c
                    .task_start_to_close_timeout_seconds
                    .map(|s| duration(s.into())),
            }
        }),
        workflow_execution_info: v.workflow_execution_info.map(info),
        pending_activities: v
            .pending_activities
            .into_iter()
            .map(|a| p::PendingActivityInfo {
                activity_id: a.activity_id,
                activity_type: a.activity_type.map(|t| p::ActivityType { name: t.name }),
                state: a.state.map_or(0, |s| s as i32 + 1),
                attempt: a.attempt,
                maximum_attempts: a.maximum_attempts,
                scheduled_time: a.scheduled_timestamp.map(timestamp),
                last_started_time: a.last_started_timestamp.map(timestamp),
                heartbeat_details: payload(a.heartbeat_details),
                schedule_id: a.schedule_id,
                last_failure: a.last_failure_reason.map(|reason| p::Failure {
                    reason,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect(),
        pending_children: v
            .pending_children
            .into_iter()
            .map(|c| p::PendingChildExecutionInfo {
                workflow_execution: Some(p::WorkflowExecution {
                    workflow_id: c.workflow_id,
                    run_id: c.run_id,
                }),
                workflow_type_name: c.workflow_type_name,
                initiated_id: c.initiated_id,
                parent_close_policy: c.parent_close_policy.map_or(0, |s| s as i32 + 1),
                domain: c.domain,
            })
            .collect(),
        pending_decision: v.pending_decision.map(|d| p::PendingDecisionInfo {
            state: d.state.map_or(0, |s| s as i32 + 1),
            scheduled_time: d.scheduled_timestamp.map(timestamp),
            started_time: d.started_timestamp.map(timestamp),
            attempt: d.attempt as i32,
            original_scheduled_time: d.original_scheduled_timestamp.map(timestamp),
            schedule_id: d.schedule_id,
        }),
    }
}

/// Convert a proto `RespondQueryTaskCompletedRequest` to the internal Thrift
/// shape, mirroring Go `ToRespondQueryTaskCompletedRequest`: the proto
/// `WorkflowQueryResult.result_type` (ANSWERED/FAILED) becomes the Thrift
/// `completedType`, and the payload answer becomes `queryResult`.
pub fn query_completed(
    v: p::RespondQueryTaskCompletedRequest,
) -> Result<tk::RespondQueryTaskCompletedRequest, Status> {
    let result = v.result.unwrap_or_default();
    let completed_type = match enum_in(result.result_type, e::QueryResultType::from_i32)? {
        Some(e::QueryResultType::Answered) => Some(e::QueryTaskCompletedType::Completed),
        Some(e::QueryResultType::Failed) => Some(e::QueryTaskCompletedType::Failed),
        Some(_) | None => None,
    };
    Ok(tk::RespondQueryTaskCompletedRequest {
        task_token: v.task_token,
        completed_type,
        query_result: data(result.answer),
        error_message: result.error_message,
        worker_version_info: v.worker_version_info.map(|w| t::WorkerVersionInfo {
            impl_: w.r#impl,
            feature_version: w.feature_version,
        }),
    })
}

/// Convert an internal query-completion back to proto, mirroring Go
/// `FromRespondQueryTaskCompletedRequest`.
pub fn query_completed_out(
    v: tk::RespondQueryTaskCompletedRequest,
) -> p::RespondQueryTaskCompletedRequest {
    let result_type = match v.completed_type {
        Some(e::QueryTaskCompletedType::Completed) => 1,
        Some(e::QueryTaskCompletedType::Failed) => 2,
        Some(_) | None => 0,
    };
    p::RespondQueryTaskCompletedRequest {
        task_token: v.task_token,
        result: Some(p::WorkflowQueryResult {
            result_type,
            answer: payload(v.query_result),
            error_message: v.error_message,
        }),
        worker_version_info: v.worker_version_info.map(|w| p::WorkerVersionInfo {
            r#impl: w.impl_,
            feature_version: w.feature_version,
        }),
    }
}

/// Convert an internal describe-task-list response to proto, mirroring
/// Go's `FromDescribeTaskListResponse` (partition config stays empty on a
/// single host; the echoed task list is attached by the caller).
pub fn describe_task_list_out(v: tk::DescribeTaskListResponse) -> p::DescribeTaskListResponse {
    p::DescribeTaskListResponse {
        pollers: v
            .pollers
            .into_iter()
            .map(|x| p::PollerInfo {
                last_access_time: x.last_access_time.map(timestamp),
                identity: x.identity,
                rate_per_second: x.rate_per_second,
            })
            .collect(),
        task_list_status: v.task_list_status.map(|x| p::TaskListStatus {
            backlog_count_hint: x.backlog_count_hint,
            read_level: x.read_level,
            ack_level: x.ack_level,
            rate_per_second: x.rate_per_second,
            task_id_block: x.task_id_block.map(|b| p::TaskIdBlock {
                start_id: b.start_id,
                end_id: b.end_id,
            }),
            isolation_group_metrics: x
                .isolation_group_metrics
                .into_iter()
                .map(|(group, m)| {
                    (
                        group,
                        p::IsolationGroupMetrics {
                            new_tasks_per_second: m.new_tasks_per_second,
                            poller_count: m.poller_count,
                        },
                    )
                })
                .collect(),
            ..Default::default()
        }),
        task_list: None,
        partition_config: None,
    }
}

/// Convert a proto failover request, mirroring Go's proto→types mapping
/// (`failover_timeout` duration → seconds; at most one of the active-cluster
/// selectors is required by validation, not conversion).
pub fn failover_in(v: p::FailoverDomainRequest) -> t::FailoverDomainRequest {
    t::FailoverDomainRequest {
        domain_name: v.domain_name,
        domain_active_cluster_name: if v.domain_active_cluster_name.is_empty() {
            None
        } else {
            Some(v.domain_active_cluster_name)
        },
        active_clusters: v.active_clusters.map(active_clusters_in),
        reason: if v.reason.is_empty() {
            None
        } else {
            Some(v.reason)
        },
        failover_timeout_in_seconds: v.failover_timeout.map(|d| d.seconds as i32),
        skip_destination_cluster_check: v.skip_destination_cluster_check,
    }
}

/// Convert an internal failover event to proto, mirroring Go's
/// `ToFailoverEvents` wire shape (nanosecond timestamps).
pub fn failover_event_out(v: t::FailoverEvent) -> p::FailoverEvent {
    p::FailoverEvent {
        id: v.id.unwrap_or_default(),
        created_time: v.created_time.map(|ns| prost_types::Timestamp {
            seconds: ns.div_euclid(1_000_000_000),
            nanos: ns.rem_euclid(1_000_000_000) as i32,
        }),
        failover_type: v.failover_type.map_or(0, |t| t as i32 + 1),
        cluster_failovers: v
            .cluster_failovers
            .into_iter()
            .map(|c| p::ClusterFailover {
                from_cluster: c.from_cluster.map(|a| p::ActiveClusterInfo {
                    active_cluster_name: a.active_cluster_name,
                    failover_version: a.failover_version,
                }),
                to_cluster: c.to_cluster.map(|a| p::ActiveClusterInfo {
                    active_cluster_name: a.active_cluster_name,
                    failover_version: a.failover_version,
                }),
                cluster_attribute: c.cluster_attribute.map(|a| p::ClusterAttribute {
                    scope: a.scope,
                    name: a.name,
                }),
            })
            .collect(),
    }
}

/// Nanos since the epoch from a proto timestamp.
pub fn nanos(v: Option<prost_types::Timestamp>) -> Option<i64> {
    v.map(|t| t.seconds.saturating_mul(1_000_000_000).saturating_add(i64::from(t.nanos)))
}

/// Proto timestamp from nanos since the epoch.
pub fn timestamp_from_nanos(ns: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ns.div_euclid(1_000_000_000),
        nanos: ns.rem_euclid(1_000_000_000) as i32,
    }
}

/// Whole seconds from a proto duration (Thrift schedule convention).
pub fn duration_seconds(v: Option<prost_types::Duration>) -> Option<i32> {
    v.and_then(|d| i32::try_from(d.seconds).ok())
}

pub fn schedule_spec_in(v: p::ScheduleSpec) -> tk_schedule::ScheduleSpec {
    tk_schedule::ScheduleSpec {
        cron_expression: v.cron_expression,
        start_time_nano: nanos(v.start_time),
        end_time_nano: nanos(v.end_time),
        jitter_in_seconds: duration_seconds(v.jitter),
    }
}

pub fn schedule_spec_out(v: tk_schedule::ScheduleSpec) -> p::ScheduleSpec {
    p::ScheduleSpec {
        cron_expression: v.cron_expression,
        start_time: v.start_time_nano.map(timestamp_from_nanos),
        end_time: v.end_time_nano.map(timestamp_from_nanos),
        jitter: v.jitter_in_seconds.map(|s| prost_types::Duration {
            seconds: i64::from(s),
            nanos: 0,
        }),
    }
}

pub fn schedule_action_in(v: p::ScheduleAction) -> Result<tk_schedule::ScheduleAction, Status> {
    Ok(tk_schedule::ScheduleAction {
        start_workflow: v
            .start_workflow
            .map(|a| {
                Ok::<_, Status>(tk_schedule::ScheduleStartWorkflowAction {
                    workflow_type: a.workflow_type.map(|t| t::WorkflowType { name: t.name }),
                    task_list: a.task_list.map(task_list).transpose()?,
                    input: data(a.input),
                    workflow_id_prefix: a.workflow_id_prefix,
                    execution_start_to_close_timeout_seconds: seconds(
                        a.execution_start_to_close_timeout,
                    )?,
                    task_start_to_close_timeout_seconds: seconds(a.task_start_to_close_timeout)?,
                    retry_policy: a.retry_policy.map(retry).transpose()?,
                    memo: a.memo.map(|m| t::Memo {
                        fields: Some(m.fields.into_iter().map(|(k, v)| (k, v.data)).collect()),
                    }),
                    search_attributes: a.search_attributes.map(|m| t::SearchAttributes {
                        indexed_fields: Some(
                            m.indexed_fields
                                .into_iter()
                                .map(|(k, v)| (k, v.data))
                                .collect(),
                        ),
                    }),
                })
            })
            .transpose()?,
    })
}

pub fn schedule_action_out(v: tk_schedule::ScheduleAction) -> p::ScheduleAction {
    p::ScheduleAction {
        start_workflow: v.start_workflow.map(|a| p::schedule_action::StartWorkflowAction {
            workflow_type: a.workflow_type.map(|t| p::WorkflowType { name: t.name }),
            task_list: a.task_list.map(task_list_out),
            input: payload(a.input),
            workflow_id_prefix: a.workflow_id_prefix,
            execution_start_to_close_timeout: a
                .execution_start_to_close_timeout_seconds
                .map(|s| prost_types::Duration {
                    seconds: i64::from(s),
                    nanos: 0,
                }),
            task_start_to_close_timeout: a
                .task_start_to_close_timeout_seconds
                .map(|s| prost_types::Duration {
                    seconds: i64::from(s),
                    nanos: 0,
                }),
            retry_policy: a.retry_policy.map(retry_out),
            memo: a.memo.map(memo),
            search_attributes: a.search_attributes.map(search_attributes),
        }),
    }
}

pub fn schedule_policies_in(
    v: p::SchedulePolicies,
) -> Result<tk_schedule::SchedulePolicies, Status> {
    Ok(tk_schedule::SchedulePolicies {
        overlap_policy: enum_in(v.overlap_policy, e::ScheduleOverlapPolicy::from_i32)?,
        catch_up_policy: enum_in(v.catch_up_policy, e::ScheduleCatchUpPolicy::from_i32)?,
        catch_up_window_in_seconds: duration_seconds(v.catch_up_window),
        pause_on_failure: v.pause_on_failure,
        buffer_limit: v.buffer_limit,
        concurrency_limit: v.concurrency_limit,
    })
}

pub fn schedule_policies_out(v: tk_schedule::SchedulePolicies) -> p::SchedulePolicies {
    p::SchedulePolicies {
        overlap_policy: v.overlap_policy.map_or(0, |p| p as i32 + 1),
        catch_up_policy: v.catch_up_policy.map_or(0, |p| p as i32 + 1),
        catch_up_window: v.catch_up_window_in_seconds.map(|s| prost_types::Duration {
            seconds: i64::from(s),
            nanos: 0,
        }),
        pause_on_failure: v.pause_on_failure,
        buffer_limit: v.buffer_limit,
        concurrency_limit: v.concurrency_limit,
    }
}

pub fn schedule_state_out(v: tk_schedule::ScheduleState) -> p::ScheduleState {
    p::ScheduleState {
        paused: v.paused,
        pause_info: v.pause_info.map(|i| p::SchedulePauseInfo {
            reason: i.reason,
            paused_at: i.paused_time_nano.map(timestamp_from_nanos),
            paused_by: i.paused_by,
        }),
    }
}

pub fn schedule_info_out(v: tk_schedule::ScheduleInfo) -> p::ScheduleInfo {
    p::ScheduleInfo {
        last_run_time: v.last_run_time_nano.map(timestamp_from_nanos),
        next_run_time: v.next_run_time_nano.map(timestamp_from_nanos),
        total_runs: v.total_runs,
        create_time: v.create_time_nano.map(timestamp_from_nanos),
        last_update_time: v.last_update_time_nano.map(timestamp_from_nanos),
        ongoing_backfills: v
            .ongoing_backfills
            .into_iter()
            .map(|b| p::BackfillInfo {
                backfill_id: b.backfill_id,
                start_time: b.start_time_nano.map(timestamp_from_nanos),
                end_time: b.end_time_nano.map(timestamp_from_nanos),
                runs_completed: b.runs_completed,
                runs_total: b.runs_total,
            })
            .collect(),
        missed_runs: v.missed_runs,
        skipped_runs: v.skipped_runs,
        buffered_fire_count: v.buffered_fire_count,
        running_workflow_count: v.running_workflow_count,
    }
}

pub fn schedule_entry_out(v: tk_schedule::ScheduleListEntry) -> p::ScheduleListEntry {
    p::ScheduleListEntry {
        schedule_id: v.schedule_id,
        workflow_type: v.workflow_type.map(|t| p::WorkflowType { name: t.name }),
        state: v.state.map(schedule_state_out),
        cron_expression: v.cron_expression,
        memo: v.memo.map(memo),
        search_attributes: v.search_attributes.map(search_attributes),
    }
}


pub fn version_history_out(value: t::VersionHistory) -> cadence_proto::admin::VersionHistory {
    cadence_proto::admin::VersionHistory {
        branch_token: value.branch_token,
        items: value.items.into_iter().map(|item| cadence_proto::admin::VersionHistoryItem {
            event_id: item.event_id, version: item.version,
        }).collect(),
    }
}

pub fn version_histories_out(value: t::VersionHistories) -> cadence_proto::shared::VersionHistories {
    cadence_proto::shared::VersionHistories {
        current_version_history_index: value.current_version_history_index,
        histories: value.histories.into_iter().map(version_history_out).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_completed_maps_proto_result_types() {
        let completed = query_completed(p::RespondQueryTaskCompletedRequest {
            task_token: b"t".to_vec(),
            result: Some(p::WorkflowQueryResult {
                result_type: 1,
                answer: Some(p::Payload { data: b"a".to_vec() }),
                error_message: String::new(),
            }),
            worker_version_info: None,
        })
        .unwrap();
        assert_eq!(
            completed.completed_type,
            Some(e::QueryTaskCompletedType::Completed)
        );
        assert_eq!(completed.query_result, b"a");
        let failed = query_completed(p::RespondQueryTaskCompletedRequest {
            task_token: b"t".to_vec(),
            result: Some(p::WorkflowQueryResult {
                result_type: 2,
                answer: None,
                error_message: "boom".to_string(),
            }),
            worker_version_info: Some(p::WorkerVersionInfo {
                r#impl: "go".to_string(),
                feature_version: "1.0".to_string(),
            }),
        })
        .unwrap();
        assert_eq!(
            failed.completed_type,
            Some(e::QueryTaskCompletedType::Failed)
        );
        assert_eq!(failed.error_message, "boom");
        // Round trip back to proto preserves the wire encoding.
        let back = query_completed_out(failed);
        assert_eq!(
            back.result.as_ref().unwrap().result_type, 2,
            "failed must encode as QUERY_RESULT_TYPE_FAILED"
        );
        let back = query_completed_out(completed);
        assert_eq!(
            back.result.as_ref().unwrap().result_type, 1,
            "completed must encode as QUERY_RESULT_TYPE_ANSWERED"
        );
    }

    #[test]
    fn reserved_and_unknown_enums_do_not_panic() {
        assert_eq!(enum_in(0, e::TaskListKind::from_i32).unwrap(), None);
        assert_eq!(
            enum_in(1, e::TaskListKind::from_i32).unwrap(),
            Some(e::TaskListKind::Normal)
        );
        assert_eq!(
            enum_in(i32::MIN, e::TaskListKind::from_i32)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            enum_in(i32::MAX, e::TaskListKind::from_i32)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    #[test]
    fn timestamp_conversion_normalizes_negative_time_and_preserves_nanos() {
        assert_eq!(
            timestamp(-1),
            prost_types::Timestamp {
                seconds: -1,
                nanos: 999_000_000
            }
        );
        assert_eq!(
            timestamp_nanos(-1),
            prost_types::Timestamp {
                seconds: -1,
                nanos: 999_999_999
            }
        );
        assert_eq!(
            timestamp_nanos(1_000_000_123),
            prost_types::Timestamp {
                seconds: 1,
                nanos: 123
            }
        );
    }
    #[test]
    fn duration_validation_rejects_overflow_and_bad_signs() {
        assert_eq!(
            seconds(Some(prost_types::Duration {
                seconds: 5,
                nanos: 999_999_999
            }))
            .unwrap(),
            Some(5)
        );
        for d in [
            prost_types::Duration {
                seconds: 1,
                nanos: -1,
            },
            prost_types::Duration {
                seconds: 0,
                nanos: i32::MIN,
            },
            prost_types::Duration {
                seconds: i64::MAX,
                nanos: 0,
            },
        ] {
            assert_eq!(
                seconds(Some(d)).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }
}
