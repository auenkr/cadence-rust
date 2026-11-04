//! TODO(link): resource/controller contracts used by the handler port.
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use async_trait::async_trait;
use cadence_api::{CadenceError, tasktoken::TaskToken};
use crate::engine::deps::{HResult, HistoryError};
use super::Handler;

pub trait RequestProfile: Send {}
pub trait HandlerMetrics: Send + Sync {
    fn start_request_profile(&self, operation: &str) -> Box<dyn RequestProfile>;
    fn error_counter(&self, operation: &str, name: &str);
    fn empty_completion_decisions(&self);
    fn non_deterministic(&self, domain_id: &str);
}
#[async_trait]
pub trait HandlerResources: Send + Sync {
    fn start(&self);
    fn stop(&self);
    fn prepare_controller_to_stop(&self);
    fn allow(&self) -> bool;
    fn engine_for_workflow(&self, workflow_id: &str) -> HResult<Arc<dyn Handler>>;
    fn engine_for_shard(&self, shard_id: i32) -> HResult<Arc<dyn Handler>>;
    fn deserialize_token(&self, data: &[u8]) -> HResult<TaskToken>;
    fn shard_ownership_lost(&self, persistence_message: &str) -> HistoryError;
}
pub struct HistoryHandler {
    pub resources: Arc<dyn HandlerResources>,
    pub admin: super::admin::AdminResourcesRef,
    pub metrics: Arc<dyn HandlerMetrics>,
    pub shutting_down: AtomicBool,
    pub started: tokio::sync::Semaphore,
}
impl HistoryHandler {
    pub fn new(resources: Arc<dyn HandlerResources>, metrics: Arc<dyn HandlerMetrics>, admin: super::admin::AdminResourcesRef) -> Self {
        Self { resources, metrics, admin, shutting_down: AtomicBool::new(false), started: tokio::sync::Semaphore::new(0) }
    }
    pub fn start(&self) { self.resources.start(); self.started.add_permits(1); }
    pub fn stop(&self) { self.shutting_down.store(true, Ordering::SeqCst); self.resources.stop(); }
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
    pub async fn wait_started(&self) { drop(self.started.acquire().await.expect("startup semaphore remains open")); }
    pub fn check_shutdown(&self) -> HResult<()> {
        if self.shutting_down.load(Ordering::SeqCst) { Err(HistoryError::internal("Shutting down")) } else { Ok(()) }
    }
    pub fn check_rate(&self) -> HResult<()> {
        if self.resources.allow() { Ok(()) } else {
            Err(cadence_api::errors::ServiceBusyError { message: "History host rps exceeded".into(), ..Default::default() }.into())
        }
    }
    pub fn convert_error(&self, error: HistoryError) -> HistoryError {
        match error {
            HistoryError::PersistenceShardOwnershipLost(message) => self.resources.shard_ownership_lost(&message),
            HistoryError::PersistenceAlreadyStarted(error) => HistoryError::internal(error.msg),
            HistoryError::CurrentWorkflowConditionFailed(message) | HistoryError::PersistenceTimeout(message) => HistoryError::internal(message),
            HistoryError::TransactionSizeLimit(message) => HistoryError::bad_request(message),
            error => error,
        }
    }
    pub fn error(&self, operation: &str, domain: &str, workflow: &str, run: &str, error: HistoryError) -> HistoryError {
        let error = self.convert_error(error);
        let counter = match &error {
            HistoryError::DeadlineExceeded | HistoryError::Canceled => "CadenceErrContextTimeoutCounter",
            HistoryError::EventAlreadyStarted(_) => "CadenceErrEventAlreadyStartedCounter",
            HistoryError::Cadence(error) => match &**error {
                CadenceError::ShardOwnershipLost(_) => "CadenceErrShardOwnershipLostCounter",
                CadenceError::BadRequest(_) => "CadenceErrBadRequestCounter",
                CadenceError::DomainNotActive(_) => "CadenceErrDomainNotActiveCounter",
                CadenceError::WorkflowAlreadyStarted(_) => "CadenceErrExecutionAlreadyStartedCounter",
                CadenceError::EntityNotExists(_) => "CadenceErrEntityNotExistsCounter",
                CadenceError::WorkflowAlreadyCompleted(_) => "CadenceErrWorkflowExecutionAlreadyCompletedCounter",
                CadenceError::CancellationAlreadyRequested(_) => "CadenceErrCancellationAlreadyRequestedCounter",
                CadenceError::LimitExceeded(_) => "CadenceErrLimitExceededCounter",
                CadenceError::RetryTaskV2(_) => "CadenceErrRetryTaskCounter",
                CadenceError::ServiceBusy(_) => "CadenceErrServiceBusyCounter",
                CadenceError::QueryFailed(_) => "CadenceErrQueryFailedCounter",
                CadenceError::Internal(_) => {
                    tracing::error!(error = %error, workflow_id = workflow, run_id = run, domain_id = domain, "Internal service error");
                    "CadenceFailures"
                }
                _ => { tracing::error!(error = %error, workflow_id = workflow, run_id = run, domain_id = domain, "Uncategorized error"); "CadenceFailures" }
            },
            _ => { tracing::error!(error = %error, workflow_id = workflow, run_id = run, domain_id = domain, "Uncategorized error"); "CadenceFailures" }
        };
        self.metrics.error_counter(operation, counter);
        error
    }
    pub fn token(&self, data: &[u8]) -> HResult<TaskToken> {
        let token = self.resources.deserialize_token(data).map_err(|error| HistoryError::bad_request(format!("Error deserializing task token. Error: {error}")))?;
        validate_task_token(&token)?; Ok(token)
    }
}
pub fn validate_task_token(token: &TaskToken) -> HResult<()> {
    if token.workflow_id.is_empty() { return Err(HistoryError::bad_request("WorkflowId is not set on request.")); }
    if !token.run_id.is_empty() && uuid::Uuid::parse_str(&token.run_id).is_err() { return Err(HistoryError::bad_request("RunID is not valid UUID.")); }
    Ok(())
}
