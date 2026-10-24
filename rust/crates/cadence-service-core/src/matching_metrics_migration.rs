//! Matching-only Go-compatible metric migration policies. Allow-lists mirror go/common/metrics/config.go.
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use serde::de::Error as DeError;
use crate::matching_metrics::{MetricEvent, MetricKind, MetricSink};

pub const HISTOGRAM_MIGRATION_NAMES: &[&str] = &[
    "task_attempt",
    "task_attempt_counts",
    "task_attempt_per_domain",
    "task_attempt_per_domain_counts",
    "task_latency",
    "task_latency_ns",
    "task_latency_per_domain",
    "task_latency_per_domain_ns",
    "task_latency_processing",
    "task_latency_processing_ns",
    "task_latency_queue",
    "task_latency_queue_ns",
    "task_latency_processing_per_domain",
    "task_latency_processing_per_domain_ns",
    "task_latency_queue_per_domain",
    "task_latency_queue_per_domain_ns",
    "replication_tasks_lag",
    "replication_tasks_lag_counts",
    "replication_tasks_applied_latency",
    "replication_tasks_applied_latency_ns",
    "cache_latency",
    "cache_latency_ns",
    "cache_size",
    "cache_size_counts",
    "replication_task_latency",
    "replication_task_latency_ns",
    "replicator_latency",
    "replicator_latency_ns",
    "archiver_handle_history_request_latency",
    "archiver_handle_history_request_latency_ns",
    "archiver_handle_visibility_request_latency",
    "archiver_handle_visibility_request_latency_ns",
    "archiver_upload_with_retries_latency",
    "archiver_upload_with_retries_latency_ns",
    "archiver_delete_with_retries_latency",
    "archiver_delete_with_retries_latency_ns",
    "replication_tasks_fetched",
    "replication_tasks_fetched_counts",
    "replication_tasks_lag_raw",
    "replication_tasks_lag_raw_counts",
    "activity_end_to_end_latency",
    "activity_end_to_end_latency_ns",
    "schedule_to_start_history_queue_latency_per_tl",
    "schedule_to_start_history_queue_latency_per_tl_ns",
    "processing_queue_num",
    "processing_queue_num_counts",
    "processing_queue_max_level",
    "processing_queue_max_level_counts",
    "persistence_latency_per_domain",
    "persistence_latency_per_domain_ns",
    "persistence_latency",
    "persistence_latency_ns",
    "persistence_latency_rollup",
    "persistence_latency_ns_rollup",
    "persistence_latency_histogram",
    "persistence_latency_per_shard",
    "persistence_latency_per_shard_ns",
    "history_size",
    "history_size_counts",
    "history_count",
    "history_count_counts",
    "mutable_state_size",
    "mutable_state_size_counts",
    "execution_info_size",
    "execution_info_size_counts",
    "activity_info_size",
    "activity_info_size_counts",
    "timer_info_size",
    "timer_info_size_counts",
    "child_info_size",
    "child_info_size_counts",
    "signal_info_size",
    "signal_info_size_counts",
    "buffered_events_size",
    "buffered_events_size_counts",
    "activity_info_count",
    "activity_info_count_counts",
    "timer_info_count",
    "timer_info_count_counts",
    "child_info_count",
    "child_info_count_counts",
    "signal_info_count",
    "signal_info_count_counts",
    "request_cancel_info_count",
    "request_cancel_info_count_counts",
    "buffered_events_count",
    "buffered_events_count_counts",
    "elasticsearch_latency_per_domain",
    "elasticsearch_latency_per_domain_ns",
    "elasticsearch_latency",
    "elasticsearch_latency_ns",
    "event_blob_size",
    "event_blob_size_counts",
    "paralleltask_submit_latency",
    "paralleltask_submit_latency_ns",
    "paralleltask_task_processing_latency",
    "paralleltask_task_processing_latency_ns",
    "decision_attempt",
    "decision_attempt_counts",
    "acquire_shards_latency",
    "acquire_shards_latency_ns",
    "transfer_tasks_count",
    "transfer_tasks_count_counts",
    "timer_tasks_count",
    "timer_tasks_count_counts",
    "replication_tasks_count",
    "replication_tasks_count_counts",
    "delete_activity_info",
    "delete_activity_info_counts",
    "delete_timer_info",
    "delete_timer_info_counts",
    "delete_child_info",
    "delete_child_info_counts",
    "delete_signal_info",
    "delete_signal_info_counts",
    "delete_request_cancel_info",
    "delete_request_cancel_info_counts",
    "workflow_id_external_requests_max_requests_per_seconds",
    "workflow_id_external_requests_max_requests_per_seconds_counts",
    "workflow_id_internal_requests_max_requests_per_seconds",
    "workflow_id_internal_requests_max_requests_per_seconds_counts",
    "decision_result_count",
    "decision_result_count_counts",
    "sharditem_acquisition_latency",
    "sharditem_acquisition_latency_ns",
    "get_engine_for_shard_latency",
    "get_engine_for_shard_latency_ns",
    "remove_engine_for_shard_latency",
    "remove_engine_for_shard_latency_ns",
    "domain_cache_prepare_callbacks_latency",
    "domain_cache_prepare_callbacks_latency_ns",
    "domain_cache_callbacks_latency",
    "domain_cache_callbacks_latency_ns",
    "history_archiver_total_upload_size",
    "history_archiver_total_upload_size_counts",
    "history_archiver_history_size",
    "history_archiver_history_size_counts",
    "history_archiver_blob_size",
    "history_archiver_blob_size_counts",
    "shardinfo_replication_pending_task",
    "shardinfo_replication_pending_task_counts",
    "shardinfo_transfer_active_pending_task",
    "shardinfo_transfer_active_pending_task_counts",
    "shardinfo_transfer_standby_pending_task",
    "shardinfo_transfer_standby_pending_task_counts",
    "shardinfo_timer_active_pending_task",
    "shardinfo_timer_active_pending_task_counts",
    "shardinfo_timer_standby_pending_task",
    "shardinfo_timer_standby_pending_task_counts",
    "shardinfo_cross_cluster_pending_task",
    "shardinfo_cross_cluster_pending_task_counts",
    "shardinfo_replication_lag",
    "shardinfo_replication_lag_counts",
    "shardinfo_transfer_lag",
    "shardinfo_transfer_lag_counts",
    "shardinfo_timer_lag",
    "shardinfo_timer_lag_ns",
    "shardinfo_transfer_diff",
    "shardinfo_transfer_diff_counts",
    "shardinfo_timer_diff",
    "shardinfo_timer_diff_ns",
    "shardinfo_transfer_failover_in_progress",
    "shardinfo_transfer_failover_in_progress_counts",
    "shardinfo_timer_failover_in_progress",
    "shardinfo_timer_failover_in_progress_counts",
    "shardinfo_transfer_failover_latency",
    "shardinfo_transfer_failover_latency_ns",
    "shardinfo_timer_failover_latency",
    "shardinfo_timer_failover_latency_ns",
    "history_event_notification_queueing_latency",
    "history_event_notification_queueing_latency_ns",
    "history_event_notification_fanout_latency",
    "history_event_notification_fanout_latency_ns",
    "direct_query_dispatch_latency",
    "direct_query_dispatch_latency_ns",
    "direct_query_dispatch_sticky_latency",
    "direct_query_dispatch_sticky_latency_ns",
    "direct_query_dispatch_non_sticky_latency",
    "direct_query_dispatch_non_sticky_latency_ns",
    "direct_query_dispatch_clear_stickiness_latency",
    "direct_query_dispatch_clear_stickiness_latency_ns",
    "cadence_authorization_latency",
    "cadence_authorization_latency_ns",
    "pinot_latency",
    "pinot_latency_ns",
    "pinot_latency_per_domain",
    "pinot_latency_per_domain_ns",
    "sequentialtask_submit_latency",
    "sequentialtask_submit_latency_ns",
    "sequentialtask_queue_size",
    "sequentialtask_queue_size_counts",
    "sequentialtask_queue_processing_latency",
    "sequentialtask_queue_processing_latency_ns",
    "sequentialtask_task_processing_latency",
    "sequentialtask_task_processing_latency_ns",
    "prioritytask_submit_latency",
    "prioritytask_submit_latency_ns",
    "graceful_failover_latency",
    "graceful_failover_latency_ns",
    "async_request_payload_size_per_domain",
    "async_request_payload_size_per_domain_counts",
    "async_request_payload_size",
    "async_request_payload_size_counts",
    "task_redispatch_queue_pending_tasks",
    "task_redispatch_queue_pending_tasks_counts",
    "workflow_context_lock_latency",
    "workflow_context_lock_latency_ns",
    "get_replication_messages_for_shard",
    "get_replication_messages_for_shard_ns",
    "get_dlq_replication_messages",
    "get_dlq_replication_messages_ns",
    "decision_task_query_latency",
    "decision_task_query_latency_ns",
    "syncmatch_latency_per_tl",
    "syncmatch_latency_per_tl_ns",
    "syncmatch_latency",
    "syncmatch_latency_ns",
    "syncmatch_local_poll_latency_per_tl",
    "syncmatch_local_poll_latency_per_tl_ns",
    "syncmatch_local_poll_latency",
    "syncmatch_local_poll_latency_ns",
    "syncmatch_forward_poll_latency_per_tl",
    "syncmatch_forward_poll_latency_per_tl_ns",
    "syncmatch_forward_poll_latency",
    "syncmatch_forward_poll_latency_ns",
    "asyncmatch_latency_per_tl",
    "asyncmatch_latency_per_tl_ns",
    "asyncmatch_latency",
    "asyncmatch_latency_ns",
    "asyncmatch_local_poll_latency_per_tl",
    "asyncmatch_local_poll_latency_per_tl_ns",
    "asyncmatch_local_poll_latency",
    "asyncmatch_local_poll_latency_ns",
    "asyncmatch_local_poll_attempt_per_tl",
    "asyncmatch_local_poll_attempt_per_tl_counts",
    "asyncmatch_local_poll_attempt",
    "asyncmatch_local_poll_attempt_counts",
    "asyncmatch_forward_poll_attempt_per_tl",
    "asyncmatch_forward_poll_attempt_per_tl_counts",
    "asyncmatch_forward_poll_attempt",
    "asyncmatch_forward_poll_attempt_counts",
    "asyncmatch_forward_poll_latency_per_tl",
    "asyncmatch_forward_poll_latency_per_tl_ns",
    "asyncmatch_forward_poll_latency",
    "asyncmatch_forward_poll_latency_ns",
    "asyncmatch_local_poll_after_forward_failed_attempt_per_tl",
    "asyncmatch_local_poll_after_forward_failed_attempt_per_tl_counts",
    "asyncmatch_local_poll_after_forward_failed_attempt",
    "asyncmatch_local_poll_after_forward_failed_attempt_counts",
    "asyncmatch_local_poll_after_forward_failed_latency_per_tl",
    "asyncmatch_local_poll_after_forward_failed_latency_per_tl_ns",
    "asyncmatch_local_poll_after_forward_failed_latency",
    "asyncmatch_local_poll_after_forward_failed_latency_ns",
    "poll_local_match_latency_per_tl",
    "poll_local_match_latency_per_tl_ns",
    "poll_local_match_latency",
    "poll_local_match_latency_ns",
    "poll_forward_match_latency_per_tl",
    "poll_forward_match_latency_per_tl_ns",
    "poll_forward_match_latency",
    "poll_forward_match_latency_ns",
    "poll_local_match_after_forward_failed_latency_per_tl",
    "poll_local_match_after_forward_failed_latency_per_tl_ns",
    "poll_local_match_after_forward_failed_latency",
    "poll_local_match_after_forward_failed_latency_ns",
    "forward_task_latency_per_tl",
    "forward_task_latency_per_tl_ns",
    "forward_task_latency",
    "forward_task_latency_ns",
    "forward_query_latency_per_tl",
    "forward_query_latency_per_tl_ns",
    "forward_query_latency",
    "forward_query_latency_ns",
    "forward_poll_latency_per_tl",
    "forward_poll_latency_per_tl_ns",
    "forward_poll_latency",
    "forward_poll_latency_ns",
    "es_processor_process_msg_latency",
    "es_processor_process_msg_latency_ns",
    "index_processor_process_msg_latency",
    "index_processor_process_msg_latency_ns",
    "async_workflow_process_msg_latency",
    "async_workflow_process_msg_latency_ns",
    "diagnostics_workflow_execution_latency",
    "diagnostics_workflow_execution_latency_ns",
    "global_ratelimiter_update_latency",
    "global_ratelimiter_update_latency_ns",
    "cadence_latency",
    "cadence_latency_ns",
    "cadence_latency_rollup",
    "cadence_latency_ns_rollup",
    "cadence_client_latency",
    "cadence_client_latency_ns",
    "cadence_client_latency_redirection",
    "cadence_client_latency_redirection_ns",
    "cadence_latency_per_tl",
    "cadence_latency_per_tl_ns",
];
pub const GAUGE_MIGRATION_NAMES: &[&str] = &[
    "cache_size_gauge",
    "replication_tasks_lag_gauge",
    "replication_tasks_lag_raw_gauge",
    "replication_tasks_returned",
    "replication_tasks_returned_gauge",
    "replication_tasks_returned_diff",
    "replication_tasks_returned_diff_gauge",
];
pub const COUNTER_MIGRATION_NAMES: &[&str] = &[
    "replication_tasks_fetched_counter",
    "replication_tasks_returned_counter",
    "replication_tasks_returned_diff_counter",
];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramMigration { #[serde(default, deserialize_with="de_histogram_mode")] pub default: String, #[serde(default)] pub names: BTreeMap<String,bool> }
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GaugeMigration { #[serde(default, deserialize_with="de_gauge_mode")] pub default: String, #[serde(default)] pub names: BTreeMap<String,bool> }
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterMigration { #[serde(default, deserialize_with="de_counter_mode")] pub default: String, #[serde(default)] pub names: BTreeMap<String,bool> }
fn de_histogram_mode<'de,D:serde::Deserializer<'de>>(d:D)->Result<String,D::Error>{ let v=String::deserialize(d)?; if ["timer","histogram","both"].contains(&v.as_str()){Ok(v)}else{Err(D::Error::custom(format!("unsupported histogram migration mode {v:?}, must be \"timer\", \"histogram\", or \"both\"")))} }
fn de_gauge_mode<'de,D:serde::Deserializer<'de>>(d:D)->Result<String,D::Error>{ let v=String::deserialize(d)?; if ["timer","gauge","both"].contains(&v.as_str()){Ok(v)}else{Err(D::Error::custom(format!("unsupported gauge migration mode {v:?}, must be \"timer\", \"gauge\", or \"both\"")))} }
fn de_counter_mode<'de,D:serde::Deserializer<'de>>(d:D)->Result<String,D::Error>{ let v=String::deserialize(d)?; if ["timer","counter","both"].contains(&v.as_str()){Ok(v)}else{Err(D::Error::custom(format!("unsupported counter migration mode {v:?}, must be \"timer\", \"counter\", or \"both\"")))} }
fn valid(mode: &str, choices: &[&str], kind: &str) -> anyhow::Result<()> { if mode.is_empty() || choices.contains(&mode) { Ok(()) } else { anyhow::bail!("unsupported {kind} migration mode {mode:?}") } }
fn allow(name: &str, names: &[&str]) -> bool { names.contains(&name) }
fn pick(name: &str, names: &[&str], overrides: &BTreeMap<String,bool>, default: bool) -> bool { !allow(name,names) || overrides.get(name).copied().unwrap_or(default) }
impl HistogramMigration { pub fn validate(&self)->anyhow::Result<()> { valid(&self.default,&["timer","histogram","both"],"histogram")?; for n in self.names.keys(){if !allow(n,HISTOGRAM_MIGRATION_NAMES){anyhow::bail!("unknown histogram-migration metric name {n:?}")}} Ok(()) } fn emit(&self,n:&str,hist:bool)->bool { pick(n,HISTOGRAM_MIGRATION_NAMES,&self.names,if hist {self.default=="histogram"||self.default=="both"}else{self.default.is_empty()||self.default=="timer"||self.default=="both"}) } }
impl GaugeMigration { pub fn validate(&self)->anyhow::Result<()> { valid(&self.default,&["timer","gauge","both"],"gauge")?; for n in self.names.keys(){if !allow(n,GAUGE_MIGRATION_NAMES){anyhow::bail!("unknown gauge-migration metric name {n:?}")}} Ok(()) } fn emit(&self,n:&str,gauge:bool)->bool { pick(n,GAUGE_MIGRATION_NAMES,&self.names,if gauge {self.default=="gauge"||self.default=="both"}else{self.default.is_empty()||self.default=="timer"||self.default=="both"}) } }
impl CounterMigration { pub fn validate(&self)->anyhow::Result<()> { valid(&self.default,&["timer","counter","both"],"counter")?; for n in self.names.keys(){if !allow(n,COUNTER_MIGRATION_NAMES){anyhow::bail!("unknown counter-migration metric name {n:?}")}} Ok(()) } fn emit(&self,n:&str,counter:bool)->bool { let emit = matches!((self.default.as_str(), counter), ("timer", false) | ("both", false) | ("counter", true) | ("both", true) | ("", false)); pick(n,COUNTER_MIGRATION_NAMES,&self.names,emit) } }
#[derive(Clone, Default)] pub struct MigrationPolicy { pub histograms: HistogramMigration, pub gauges: GaugeMigration, pub counters: CounterMigration }
impl MigrationPolicy { pub fn validate(&self)->anyhow::Result<()> { self.histograms.validate()?;self.gauges.validate()?;self.counters.validate() } fn accepts(&self,e:&MetricEvent)->bool { match e.kind { MetricKind::Timer => self.histograms.emit(e.name,false)&&self.gauges.emit(e.name,false)&&self.counters.emit(e.name,false), MetricKind::Histogram=>self.histograms.emit(e.name,true), MetricKind::Gauge=>self.gauges.emit(e.name,true), MetricKind::Counter=>self.counters.emit(e.name,true) } } }
pub struct MigrationMetricSink { inner: Arc<dyn MetricSink>, policy: MigrationPolicy }
impl MigrationMetricSink { pub fn new(inner:Arc<dyn MetricSink>,policy:MigrationPolicy)->Self { Self{inner,policy} } }
impl MetricSink for MigrationMetricSink { fn record(&self,event:MetricEvent){ if self.policy.accepts(&event){self.inner.record(event)} } fn flush(&self)->std::io::Result<()> {self.inner.flush()} }
#[cfg(test)] mod tests { use super::*; #[test] fn defaults_and_modes_match_go(){ let p=MigrationPolicy::default(); assert!(p.accepts(&event("task_latency",MetricKind::Timer))); assert!(!p.accepts(&event("task_latency",MetricKind::Histogram))); let mut p=p; p.histograms.default="both".into();assert!(p.accepts(&event("task_latency",MetricKind::Histogram))); p.histograms.names.insert("task_latency".into(),false);assert!(!p.accepts(&event("task_latency",MetricKind::Histogram)));} fn event(name:&'static str,kind:MetricKind)->MetricEvent{MetricEvent{name,kind,value:crate::matching_metrics::MetricValue::Count(1),tags:BTreeMap::new(),histogram:None}} }

#[cfg(test)]
mod sink_tests {
    use super::*;
    use crate::matching_metrics::{MetricValue, RecordingMetricSink};
    use std::time::Duration;

    fn event(name: &'static str, kind: MetricKind) -> MetricEvent {
        MetricEvent { name, kind, value: match kind { MetricKind::Timer => MetricValue::Duration(Duration::from_millis(2)), MetricKind::Histogram => MetricValue::Number(2.0), _ => MetricValue::Count(1) }, tags: BTreeMap::from([("domain".into(), "example".into())]), histogram: None }
    }

    #[test]
    fn records_real_events_with_go_defaults_both_modes_and_name_overrides() {
        let recording = Arc::new(RecordingMetricSink::default());
        let policy = MigrationPolicy::default();
        let sink = MigrationMetricSink::new(recording.clone(), policy);
        sink.record(event("task_latency", MetricKind::Timer));
        sink.record(event("task_latency", MetricKind::Histogram));
        sink.record(event("unmigrated_metric", MetricKind::Gauge));
        assert_eq!(recording.events().len(), 2);
        assert_eq!(recording.events()[0].tags.get("domain").unwrap(), "example");

        let mut rollup = event("persistence_latency_ns_rollup", MetricKind::Histogram);
        rollup.tags = BTreeMap::from([
            ("histogram_start".into(), "1ms".into()),
            ("histogram_end".into(), "17m28.576s".into()),
            ("histogram_scale".into(), "2".into()),
        ]);
        let recording = Arc::new(RecordingMetricSink::default());
        MigrationMetricSink::new(recording.clone(), MigrationPolicy::default()).record(rollup.clone());
        assert!(recording.events().is_empty());
        let mut rollup_policy = MigrationPolicy::default();
        rollup_policy.histograms.default = "both".into();
        MigrationMetricSink::new(recording.clone(), rollup_policy).record(rollup.clone());
        assert_eq!(recording.events(), vec![rollup]);
        assert_eq!(recording.events()[0].tags["histogram_scale"], "2");

        let mut policy = MigrationPolicy::default();
        policy.histograms.default = "both".into();
        policy.gauges.default = "both".into();
        policy.counters.default = "both".into();
        policy.histograms.names.insert("task_latency".into(), false);
        policy.gauges.names.insert("cache_size_gauge".into(), true);
        let recording = Arc::new(RecordingMetricSink::default());
        let sink = MigrationMetricSink::new(recording.clone(), policy);
        sink.record(event("task_latency", MetricKind::Histogram));
        sink.record(event("cache_size_gauge", MetricKind::Gauge));
        sink.record(event("replication_tasks_fetched_counter", MetricKind::Counter));
        assert_eq!(recording.events().iter().map(|e| (e.name,e.kind)).collect::<Vec<_>>(), vec![("cache_size_gauge",MetricKind::Gauge),("replication_tasks_fetched_counter",MetricKind::Counter)]);
    }

    #[test]
    fn rejects_unknown_names_and_invalid_modes() {
        let mut p = MigrationPolicy::default();
        p.histograms.default = "nope".into();
        assert!(p.validate().is_err());
        p.histograms.default.clear();
        p.histograms.names.insert("not_a_go_metric".into(), true);
        assert!(p.validate().is_err());
        let yaml = "histograms: {default: nope}";
        assert!(crate::config::Config::from_yaml(yaml).is_err());
        assert!(crate::config::Config::from_yaml("histograms: {default: ''}").is_err());
        assert!(crate::config::Config::from_yaml("gauge-migration: {default: ''}").is_err());
        assert!(crate::config::Config::from_yaml("counter-migration: {default: ''}").is_err());
        let yaml = "gauge-migration: {names: {bogus_gauge: false}}";
        assert!(crate::config::Config::from_yaml(yaml).is_err());
    }
}

#[cfg(test)] mod counter_mode_tests { use super::*; #[test] fn counter_modes_match_go_for_every_kind(){ for (mode,timer,counter) in [("",true,false),("timer",true,false),("counter",false,true),("both",true,true)] { let p=CounterMigration { default: mode.into(), ..CounterMigration::default() };p.validate().unwrap(); assert_eq!(p.emit("replication_tasks_fetched_counter",false),timer,"timer mode {mode}");assert_eq!(p.emit("replication_tasks_fetched_counter",true),counter,"counter mode {mode}"); } let mut p=CounterMigration { default: "both".into(), ..CounterMigration::default() };p.names.insert("replication_tasks_fetched_counter".into(),false);assert!(!p.emit("replication_tasks_fetched_counter",false));assert!(!p.emit("replication_tasks_fetched_counter",true)); } }

#[cfg(test)]
mod mode_matrix_tests {
    use super::*;
    #[test]
    fn all_modes_gate_each_migrated_metric_kind() {
        let names = ["task_latency", "cache_size_gauge", "replication_tasks_fetched_counter"];
        for mode in ["timer", "histogram", "gauge", "counter", "both"] {
            let h = HistogramMigration { default: mode.into(), ..HistogramMigration::default() };
            let g = GaugeMigration { default: mode.into(), ..GaugeMigration::default() };
            let c = CounterMigration { default: mode.into(), ..CounterMigration::default() };
            // The same name must be in the relevant Go allow-list for each predicate.
            assert_eq!(h.emit(names[0], false), mode == "timer" || mode == "both");
            assert_eq!(h.emit(names[0], true), mode == "histogram" || mode == "both");
            assert_eq!(g.emit(names[1], false), mode == "timer" || mode == "both");
            assert_eq!(g.emit(names[1], true), mode == "gauge" || mode == "both");
            assert_eq!(c.emit(names[2], false), mode == "timer" || mode == "both");
            assert_eq!(c.emit(names[2], true), mode == "counter" || mode == "both");
        }
    }
}
