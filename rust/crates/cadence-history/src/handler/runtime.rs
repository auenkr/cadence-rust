//! TODO(link): resource/controller contracts used by the handler port.
use super::Handler;
use crate::engine::deps::{HResult, HistoryError};
use async_trait::async_trait;
use cadence_api::{tasktoken::TaskToken, CadenceError};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
#[async_trait]
pub trait RoutedEngine: Handler {
    async fn reapply_history_events(
        &self,
        domain: &str,
        workflow: &str,
        run: &str,
        events: Vec<cadence_api::service_types::HistoryEvent>,
    ) -> HResult<()>;
}

pub trait RequestProfile: Send {}
pub trait HandlerMetrics: Send + Sync {
    fn start_request_profile(&self, operation: &str) -> Box<dyn RequestProfile>;
    fn error_counter(&self, operation: &str, name: &str);
    fn empty_completion_decisions(&self);
    fn non_deterministic(&self, domain_id: &str);
}
#[async_trait]
pub trait HandlerResources: super::lifecycle::HandlerComponentFactory + Send + Sync {
    fn prepare_controller_to_stop(&self);
    fn allow(&self) -> bool;
    fn enable_debug_mode(&self) -> bool;
    fn enable_task_info_log_by_domain_id(&self, domain_id: &str) -> bool;
    fn engine_for_workflow(&self, workflow_id: &str) -> HResult<Arc<dyn RoutedEngine>>;
    fn engine_for_shard(&self, shard_id: i32) -> HResult<Arc<dyn RoutedEngine>>;
    fn deserialize_token(&self, data: &[u8]) -> HResult<TaskToken>;
    fn deserialize_thriftrw_history(
        &self,
        data: &[u8],
    ) -> HResult<Vec<cadence_api::service_types::HistoryEvent>>;
    fn shard_ownership_lost(&self, shard_id: i32, persistence_message: &str) -> HistoryError;
}
pub struct HistoryHandler {
    pub resources: Arc<dyn HandlerResources>,
    pub admin: super::admin::AdminResourcesRef,
    pub metrics: Arc<dyn HandlerMetrics>,
    pub shutting_down: AtomicBool,
    pub started: tokio::sync::Semaphore,
}
impl HistoryHandler {
    pub fn new(
        resources: Arc<dyn HandlerResources>,
        metrics: Arc<dyn HandlerMetrics>,
        admin: super::admin::AdminResourcesRef,
    ) -> Self {
        Self {
            resources,
            metrics,
            admin,
            shutting_down: AtomicBool::new(false),
            started: tokio::sync::Semaphore::new(0),
        }
    }
    pub fn start(&self) {
        super::lifecycle::start_components(&*self.resources);
        self.started.add_permits(1);
    }
    pub fn stop(&self) {
        self.prepare_to_shut_down();
        super::lifecycle::stop_components(&*self.resources);
    }
    pub async fn prepare_to_stop(&self, remaining: std::time::Duration) -> std::time::Duration {
        tracing::info!("ShutdownHandler: Initiating shardController shutdown");
        self.resources.prepare_controller_to_stop();
        tracing::info!("ShutdownHandler: Waiting for traffic to drain");
        let delay = std::time::Duration::from_secs(5).min(remaining);
        tokio::time::sleep(delay).await;
        tracing::info!("ShutdownHandler: No longer taking rpc requests");
        self.shutting_down.store(true, Ordering::SeqCst);
        remaining.saturating_sub(delay)
    }
    pub async fn wait_started(&self) {
        drop(
            self.started
                .acquire()
                .await
                .expect("startup semaphore remains open"),
        );
    }
    pub fn emit_info_or_debug_log(
        &self,
        domain: &str,
        message: &str,
        workflow: &str,
        run: &str,
        schedule: i64,
    ) {
        if self.resources.enable_debug_mode()
            && self.resources.enable_task_info_log_by_domain_id(domain)
        {
            tracing::info!(
                domain_id = domain,
                workflow_id = workflow,
                run_id = run,
                schedule_id = schedule,
                "{message}"
            );
        } else {
            tracing::debug!(
                domain_id = domain,
                workflow_id = workflow,
                run_id = run,
                schedule_id = schedule,
                "{message}"
            );
        }
    }
    pub fn prepare_to_shut_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }
    pub fn create_engine(
        &self,
        assembly: impl crate::engine::factory::EngineAssembly,
        shard: &dyn crate::engine::factory::EngineShardRegistration,
    ) -> Arc<crate::engine::runtime::HistoryEngine> {
        crate::engine::factory::new_engine_with_shard_context(assembly, shard)
    }
    pub fn check_shutdown(&self) -> HResult<()> {
        if self.is_shutting_down() {
            Err(HistoryError::internal("Shutting down"))
        } else {
            Ok(())
        }
    }
    pub fn check_rate(&self) -> HResult<()> {
        if self.resources.allow() {
            Ok(())
        } else {
            Err(cadence_api::errors::ServiceBusyError {
                message: "History host rps exceeded".into(),
                ..Default::default()
            }
            .into())
        }
    }
    pub fn convert_error(&self, error: HistoryError) -> HistoryError {
        match error {
            HistoryError::PersistenceShardOwnershipLost { shard_id, message } => {
                self.resources.shard_ownership_lost(shard_id, &message)
            }
            HistoryError::PersistenceAlreadyStarted(error) => HistoryError::internal(error.msg),
            HistoryError::CurrentWorkflowConditionFailed(message)
            | HistoryError::PersistenceTimeout(message) => HistoryError::internal(message),
            HistoryError::TransactionSizeLimit(message) => HistoryError::bad_request(message),
            error => error,
        }
    }
    pub fn error(
        &self,
        operation: &str,
        domain: &str,
        workflow: &str,
        run: &str,
        error: HistoryError,
    ) -> HistoryError {
        let error = self.convert_error(error);
        let counter = match &error {
            HistoryError::DeadlineExceeded | HistoryError::Canceled => {
                "CadenceErrContextTimeoutCounter"
            }
            HistoryError::TransportStatus { code, .. } => {
                // YARPC uses the canonical gRPC code numbers (DeadlineExceeded = 4).
                if *code == 4 {
                    self.metrics
                        .error_counter(operation, "CadenceErrContextTimeoutCounter");
                }
                "CadenceFailures"
            }
            HistoryError::EventAlreadyStarted(_) => "CadenceErrEventAlreadyStartedCounter",
            HistoryError::Cadence(error) => match &**error {
                CadenceError::ShardOwnershipLost(_) => "CadenceErrShardOwnershipLostCounter",
                CadenceError::BadRequest(_) => "CadenceErrBadRequestCounter",
                CadenceError::DomainNotActive(_) => "CadenceErrDomainNotActiveCounter",
                CadenceError::WorkflowAlreadyStarted(_) => {
                    "CadenceErrExecutionAlreadyStartedCounter"
                }
                CadenceError::EntityNotExists(_) => "CadenceErrEntityNotExistsCounter",
                CadenceError::WorkflowAlreadyCompleted(_) => {
                    "CadenceErrWorkflowExecutionAlreadyCompletedCounter"
                }
                CadenceError::CancellationAlreadyRequested(_) => {
                    "CadenceErrCancellationAlreadyRequestedCounter"
                }
                CadenceError::LimitExceeded(_) => "CadenceErrLimitExceededCounter",
                CadenceError::RetryTaskV2(_) => "CadenceErrRetryTaskCounter",
                CadenceError::ServiceBusy(_) => "CadenceErrServiceBusyCounter",
                CadenceError::QueryFailed(_) => "CadenceErrQueryFailedCounter",
                CadenceError::Internal(_) => {
                    tracing::error!(error = %error, workflow_id = workflow, run_id = run, domain_id = domain, "Internal service error");
                    "CadenceFailures"
                }
                _ => {
                    tracing::error!(error = %error, workflow_id = workflow, run_id = run, domain_id = domain, "Uncategorized error");
                    "CadenceFailures"
                }
            },
            _ => {
                tracing::error!(error = %error, workflow_id = workflow, run_id = run, domain_id = domain, "Uncategorized error");
                "CadenceFailures"
            }
        };
        self.metrics.error_counter(operation, counter);
        error
    }
    pub fn deserialize_token(&self, data: &[u8]) -> HResult<TaskToken> {
        self.resources.deserialize_token(data).map_err(|error| {
            HistoryError::bad_request(format!("Error deserializing task token. Error: {error}"))
        })
    }
    pub fn token(&self, data: &[u8]) -> HResult<TaskToken> {
        let token = self.deserialize_token(data)?;
        validate_task_token(&token)?;
        Ok(token)
    }
}
pub fn validate_task_token(token: &TaskToken) -> HResult<()> {
    if token.workflow_id.is_empty() {
        return Err(HistoryError::bad_request(
            "WorkflowId is not set on request.",
        ));
    }
    if !token.run_id.is_empty() && uuid::Uuid::parse_str(&token.run_id).is_err() {
        return Err(HistoryError::bad_request("RunID is not valid UUID."));
    }
    Ok(())
}
