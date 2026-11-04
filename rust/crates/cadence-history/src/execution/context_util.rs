//! Execution metrics emission from Go `execution/context_util.go`.
use cadence_api::{enums::EventType, service_types::HistoryEvent};

pub trait ExecutionMetrics {
    fn record_timer(&mut self, scope: &str, domain: &str, metric: &str, value: i64);
    fn histogram(&mut self, scope: &str, domain: &str, metric: &str, value: i64);
    fn counter(&mut self, scope: &str, domain: &str, metric: &str);
    fn completion_counter(&mut self, domain: &str, workflow_type: &str, task_list: &str, metric: &str);
    fn unknown_completion_warning(&mut self, domain: &str, workflow_id: &str, run_id: &str, event_type: EventType);
    fn large_workflow_warning(&mut self, metric: &str, sample: bool);
    fn shard_counter(&mut self, scope: &str, domain: &str, shard_id: i32, metric: &str);
}
#[derive(Debug, Clone, Default)]
pub struct MutableStateStats {
    pub activity_info_count: i64,
    pub activity_info_size: i64,
    pub buffered_events_count: i64,
    pub buffered_events_size: i64,
    pub child_info_count: i64,
    pub child_info_size: i64,
    pub delete_activity_info_count: i64,
    pub delete_child_info_count: i64,
    pub delete_request_cancel_info_count: i64,
    pub delete_signal_info_count: i64,
    pub delete_timer_info_count: i64,
    pub execution_info_size: i64,
    pub mutable_state_size: i64,
    pub request_cancel_info_count: i64,
    pub signal_info_count: i64,
    pub signal_info_size: i64,
    pub timer_info_count: i64,
    pub timer_info_size: i64,
    pub transfer_tasks_count: i64,
    pub timer_tasks_count: i64,
    pub replication_tasks_count: i64,
}
pub fn emit_workflow_history_stats(metrics: &mut impl ExecutionMetrics, domain: &str, history_size: i64, history_count: i64) {
    metrics.record_timer("ExecutionSizeStatsScope", domain, "HistorySize", history_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "HistorySizeHistogram", history_size);
    metrics.record_timer("ExecutionCountStatsScope", domain, "HistoryCount", history_count);
    metrics.histogram("ExecutionCountStatsScope", domain, "HistoryCountHistogram", history_count);
}
pub fn emit_workflow_execution_stats(metrics: &mut impl ExecutionMetrics, domain: &str, stats: Option<&MutableStateStats>, execution_info_history_size: i64) {
    let Some(stats) = stats else { return; };
    metrics.record_timer("ExecutionSizeStatsScope", domain, "HistorySize", execution_info_history_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "HistorySizeHistogram", execution_info_history_size);
    metrics.record_timer("ExecutionSizeStatsScope", domain, "MutableStateSize", stats.mutable_state_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "MutableStateSizeHistogram", stats.mutable_state_size);
    metrics.record_timer("ExecutionSizeStatsScope", domain, "ExecutionInfoSize", stats.mutable_state_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "ExecutionInfoSizeHistogram", stats.mutable_state_size);
    metrics.record_timer("ExecutionSizeStatsScope", domain, "ActivityInfoSize", stats.activity_info_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "ActivityInfoSizeHistogram", stats.activity_info_size);
    metrics.record_timer("ExecutionSizeStatsScope", domain, "TimerInfoSize", stats.timer_info_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "TimerInfoSizeHistogram", stats.timer_info_size);
    metrics.record_timer("ExecutionSizeStatsScope", domain, "ChildInfoSize", stats.child_info_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "ChildInfoSizeHistogram", stats.child_info_size);
    metrics.record_timer("ExecutionSizeStatsScope", domain, "SignalInfoSize", stats.signal_info_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "SignalInfoSizeHistogram", stats.signal_info_size);
    metrics.record_timer("ExecutionSizeStatsScope", domain, "BufferedEventsSize", stats.buffered_events_size);
    metrics.histogram("ExecutionSizeStatsScope", domain, "BufferedEventsSizeHistogram", stats.buffered_events_size);
    metrics.record_timer("ExecutionCountStatsScope", domain, "ActivityInfoCount", stats.activity_info_count);
    metrics.histogram("ExecutionCountStatsScope", domain, "ActivityInfoCountHistogram", stats.activity_info_count);
    metrics.record_timer("ExecutionCountStatsScope", domain, "TimerInfoCount", stats.timer_info_count);
    metrics.histogram("ExecutionCountStatsScope", domain, "TimerInfoCountHistogram", stats.timer_info_count);
    metrics.record_timer("ExecutionCountStatsScope", domain, "ChildInfoCount", stats.child_info_count);
    metrics.histogram("ExecutionCountStatsScope", domain, "ChildInfoCountHistogram", stats.child_info_count);
    metrics.record_timer("ExecutionCountStatsScope", domain, "SignalInfoCount", stats.signal_info_count);
    metrics.histogram("ExecutionCountStatsScope", domain, "SignalInfoCountHistogram", stats.signal_info_count);
    metrics.record_timer("ExecutionCountStatsScope", domain, "RequestCancelInfoCount", stats.request_cancel_info_count);
    metrics.histogram("ExecutionCountStatsScope", domain, "RequestCancelInfoCountHistogram", stats.request_cancel_info_count);
    metrics.record_timer("ExecutionCountStatsScope", domain, "BufferedEventsCount", stats.buffered_events_count);
    metrics.histogram("ExecutionCountStatsScope", domain, "BufferedEventsCountHistogram", stats.buffered_events_count);
}
pub fn emit_session_update_stats(metrics: &mut impl ExecutionMetrics, domain: &str, stats: Option<&MutableStateStats>) {
    let Some(stats) = stats else { return; };
    metrics.record_timer("SessionSizeStatsScope", domain, "MutableStateSize", stats.mutable_state_size);
    metrics.histogram("SessionSizeStatsScope", domain, "MutableStateSizeHistogram", stats.mutable_state_size);
    metrics.record_timer("SessionSizeStatsScope", domain, "ExecutionInfoSize", stats.execution_info_size);
    metrics.histogram("SessionSizeStatsScope", domain, "ExecutionInfoSizeHistogram", stats.execution_info_size);
    metrics.record_timer("SessionSizeStatsScope", domain, "ActivityInfoSize", stats.activity_info_size);
    metrics.histogram("SessionSizeStatsScope", domain, "ActivityInfoSizeHistogram", stats.activity_info_size);
    metrics.record_timer("SessionSizeStatsScope", domain, "TimerInfoSize", stats.timer_info_size);
    metrics.histogram("SessionSizeStatsScope", domain, "TimerInfoSizeHistogram", stats.timer_info_size);
    metrics.record_timer("SessionSizeStatsScope", domain, "ChildInfoSize", stats.child_info_size);
    metrics.histogram("SessionSizeStatsScope", domain, "ChildInfoSizeHistogram", stats.child_info_size);
    metrics.record_timer("SessionSizeStatsScope", domain, "SignalInfoSize", stats.signal_info_size);
    metrics.histogram("SessionSizeStatsScope", domain, "SignalInfoSizeHistogram", stats.signal_info_size);
    metrics.record_timer("SessionSizeStatsScope", domain, "BufferedEventsSize", stats.buffered_events_size);
    metrics.histogram("SessionSizeStatsScope", domain, "BufferedEventsSizeHistogram", stats.buffered_events_size);
    metrics.record_timer("SessionCountStatsScope", domain, "ActivityInfoCount", stats.activity_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "ActivityInfoCountHistogram", stats.activity_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "TimerInfoCount", stats.timer_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "TimerInfoCountHistogram", stats.timer_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "ChildInfoCount", stats.child_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "ChildInfoCountHistogram", stats.child_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "SignalInfoCount", stats.signal_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "SignalInfoCountHistogram", stats.signal_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "RequestCancelInfoCount", stats.request_cancel_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "RequestCancelInfoCountHistogram", stats.request_cancel_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "DeleteActivityInfoCount", stats.delete_activity_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "DeleteActivityInfoCountHistogram", stats.delete_activity_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "DeleteTimerInfoCount", stats.delete_timer_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "DeleteTimerInfoCountHistogram", stats.delete_timer_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "DeleteChildInfoCount", stats.delete_child_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "DeleteChildInfoCountHistogram", stats.delete_child_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "DeleteSignalInfoCount", stats.delete_signal_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "DeleteSignalInfoCountHistogram", stats.delete_signal_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "DeleteRequestCancelInfoCount", stats.delete_request_cancel_info_count);
    metrics.histogram("SessionCountStatsScope", domain, "DeleteRequestCancelInfoCountHistogram", stats.delete_request_cancel_info_count);
    metrics.record_timer("SessionCountStatsScope", domain, "TransferTasksCount", stats.transfer_tasks_count);
    metrics.histogram("SessionCountStatsScope", domain, "TransferTasksCountHistogram", stats.transfer_tasks_count);
    metrics.record_timer("SessionCountStatsScope", domain, "TimerTasksCount", stats.timer_tasks_count);
    metrics.histogram("SessionCountStatsScope", domain, "TimerTasksCountHistogram", stats.timer_tasks_count);
    metrics.record_timer("SessionCountStatsScope", domain, "ReplicationTasksCount", stats.replication_tasks_count);
    metrics.histogram("SessionCountStatsScope", domain, "ReplicationTasksCountHistogram", stats.replication_tasks_count);
    metrics.counter("SessionCountStatsScope", domain, "UpdateWorkflowExecutionCount");
}
pub fn emit_workflow_completion_stats(metrics: &mut impl ExecutionMetrics, domain: &str, workflow_type: &str, workflow_id: &str, run_id: &str, task_list: &str, event: &HistoryEvent) {
    let Some(kind) = event.event_type else { return; };
    let metric = match kind {
        EventType::WorkflowExecutionCompleted => "WorkflowSuccessCount",
        EventType::WorkflowExecutionCanceled => "WorkflowCancelCount",
        EventType::WorkflowExecutionFailed => "WorkflowFailedCount",
        EventType::WorkflowExecutionTimedOut => "WorkflowTimeoutCount",
        EventType::WorkflowExecutionTerminated => "WorkflowTerminateCount",
        EventType::WorkflowExecutionContinuedAsNew => "WorkflowContinuedAsNew",
        _ => { metrics.completion_counter(domain, workflow_type, task_list, "WorkflowCompletedUnknownType"); metrics.unknown_completion_warning(domain, workflow_id, run_id, kind); return; }
    };
    metrics.completion_counter(domain, workflow_type, task_list, metric);
}
#[derive(Debug, Clone, Copy)]
pub struct LargeWorkflowThresholds {
    pub enabled: bool, pub shard_id: i32,
    pub shard_blob: i64, pub blob_warn: i64,
    pub shard_history_count: i64, pub history_count_warn: i64,
    pub shard_history_size: i64, pub history_size_warn: i64,
}
#[allow(clippy::too_many_arguments)]
pub fn emit_large_workflow_shard_id_stats(metrics: &mut impl ExecutionMetrics, domain: &str, limits: LargeWorkflowThresholds, blob_size: i64, old_count: i64, old_size: i64, new_count: i64, new_size: i64) {
    if !limits.enabled { return; }
    if blob_size > limits.shard_blob.min(limits.blob_warn) {
        metrics.large_workflow_warning("Workflow writing a large blob", true);
        metrics.shard_counter("LargeExecutionBlobShardScope", domain, limits.shard_id, "LargeHistoryBlobCount");
    }
    let count_warn = limits.shard_history_count.min(limits.history_count_warn);
    if old_count < count_warn && new_count >= count_warn {
        metrics.large_workflow_warning("Workflow history event count is reaching dangerous levels", false);
        metrics.shard_counter("LargeExecutionCountShardScope", domain, limits.shard_id, "LargeHistoryEventCount");
    }
    let size_warn = limits.shard_history_size.min(limits.history_size_warn);
    if old_size < size_warn && new_size >= size_warn {
        metrics.large_workflow_warning("Workflow history event size is reaching dangerous levels", false);
        metrics.shard_counter("LargeExecutionSizeShardScope", domain, limits.shard_id, "LargeHistorySizeCount");
    }
}
