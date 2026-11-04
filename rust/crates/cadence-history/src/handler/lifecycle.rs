//! Handler host component creation/start/stop order from handler/handler.go.
//! TODO(link): resource adapters construct the concrete package implementations.
use crate::engine::deps::HResult;

pub trait HandlerComponentFactory: Send + Sync {
    fn create_replication_task_fetchers(&self) -> HResult<()>;
    fn start_replication_task_fetchers(&self);
    fn create_task_priority_assigner(&self);
    /// BudgetManager(name=replication-budget-manager, admission=optimistic,
    /// max bytes/count and soft cap from configuration, history host metrics).
    fn create_replication_budget_manager(&self);
    fn create_shard_controller(&self);
    fn create_priority_task_processor(&self) -> HResult<()>;
    fn create_task_rate_limiter(&self);
    fn create_rate_limited_task_processor(&self);
    fn start_queue_task_processor(&self);
    fn create_transfer_queue_factory(&self);
    fn create_timer_queue_factory(&self);
    fn create_history_event_notifier(&self);
    fn start_history_event_notifier(&self);
    fn create_failover_coordinator(&self);
    fn enable_graceful_failover(&self) -> bool;
    fn start_failover_coordinator(&self);
    fn start_shard_controller(&self);
    fn stop_replication_budget_manager_if_present(&self);
    fn stop_replication_task_fetchers(&self);
    fn stop_shard_controller(&self);
    fn stop_queue_task_processor(&self);
    fn stop_history_event_notifier(&self);
    fn stop_failover_coordinator(&self);
}
pub fn start_components(factory: &dyn HandlerComponentFactory) {
    if let Err(error) = factory.create_replication_task_fetchers() {
        tracing::error!(error = %error, "Creating replication task fetchers failed");
        panic!("Creating replication task fetchers failed: {error}");
    }
    factory.start_replication_task_fetchers(); factory.create_task_priority_assigner();
    factory.create_replication_budget_manager(); factory.create_shard_controller();
    if let Err(error) = factory.create_priority_task_processor() {
        tracing::error!(error = %error, "Creating priority task processor failed");
        panic!("Creating priority task processor failed: {error}");
    }
    factory.create_task_rate_limiter(); factory.create_rate_limited_task_processor();
    factory.start_queue_task_processor(); factory.create_transfer_queue_factory(); factory.create_timer_queue_factory();
    factory.create_history_event_notifier(); factory.start_history_event_notifier();
    factory.create_failover_coordinator();
    if factory.enable_graceful_failover() { factory.start_failover_coordinator(); }
    factory.start_shard_controller();
}
pub fn stop_components(factory: &dyn HandlerComponentFactory) {
    factory.stop_replication_budget_manager_if_present(); factory.stop_replication_task_fetchers();
    factory.stop_shard_controller(); factory.stop_queue_task_processor(); factory.stop_history_event_notifier();
    factory.stop_failover_coordinator();
}
