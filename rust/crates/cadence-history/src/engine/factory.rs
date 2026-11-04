//! Constructor orchestration from engineimpl/history_engine.go.
//! TODO(link): concrete constructors live in concurrently ported packages.
use std::sync::Arc;
use super::{deps::HistoryTaskCategory, runtime::HistoryEngine};

/// Implementations retain the constructed components until finish assembles the
/// engine. The engine constructor owns policy/order; adapters only build dependencies.
pub trait EngineAssembly {
    fn create_execution_cache(&mut self);
    fn create_failover_marker_notifier(&mut self);
    fn create_deferred_replication_hydrator(&mut self);
    fn create_replication_task_store(&mut self);
    fn create_dynamic_replication_batch_sizer(&mut self);
    fn create_replication_task_reader(&mut self);
    fn create_core_engine(&mut self);
    fn create_decision_handler(&mut self);
    fn create_persistence_retryer_with_shard_id(&mut self);
    fn create_concrete_execution_exists_check(&mut self);
    fn queue_factory_categories(&self) -> Vec<HistoryTaskCategory>;
    fn create_queue(&mut self, index: usize, category: HistoryTaskCategory);
    fn create_events_reapplier(&mut self);
    fn create_history_replicator(&mut self);
    fn create_activity_replicator(&mut self);
    /// Raw history client with replication service-busy retry policy and
    /// common.IsServiceBusyError predicate (not the normal client retry policy).
    fn create_retryable_replication_history_client(&mut self);
    fn replication_source_clusters(&self) -> Vec<String>;
    fn create_history_resender(&mut self, source: &str);
    fn create_replication_task_executor(&mut self, source: &str);
    fn create_replication_task_processor(&mut self, source: &str);
    fn create_replication_dlq_handler(&mut self);
    /// taskdlq.NewProcessorFromShard: batch=100, configured interval,
    /// failover jitter, mode, enabled predicate and shared host limiter.
    fn create_task_dlq_processor(&mut self, batch_size: usize);
    fn finish(self) -> Arc<HistoryEngine>;
}
pub trait EngineShardRegistration { fn set_engine(&self, engine: Arc<HistoryEngine>); }
pub fn new_engine_with_shard_context(mut assembly: impl EngineAssembly, shard: &dyn EngineShardRegistration) -> Arc<HistoryEngine> {
    assembly.create_execution_cache(); assembly.create_failover_marker_notifier();
    assembly.create_deferred_replication_hydrator(); assembly.create_replication_task_store();
    assembly.create_dynamic_replication_batch_sizer(); assembly.create_replication_task_reader();
    assembly.create_core_engine(); assembly.create_decision_handler();
    assembly.create_persistence_retryer_with_shard_id(); assembly.create_concrete_execution_exists_check();
    for (index, category) in assembly.queue_factory_categories().into_iter().enumerate() { assembly.create_queue(index, category); }
    assembly.create_events_reapplier(); assembly.create_history_replicator(); assembly.create_activity_replicator();
    assembly.create_retryable_replication_history_client();
    for source in assembly.replication_source_clusters() {
        assembly.create_history_resender(&source); assembly.create_replication_task_executor(&source);
        assembly.create_replication_task_processor(&source);
    }
    assembly.create_replication_dlq_handler(); assembly.create_task_dlq_processor(100);
    let engine = assembly.finish(); shard.set_engine(engine.clone()); engine
}
