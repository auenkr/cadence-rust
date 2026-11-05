//! Port of `go/common/ndc/history_resender.go` (`HistoryResenderImpl`).
//!
//! Pages the raw history of one workflow from the remote cluster's admin API
//! (`GetWorkflowExecutionRawHistoryV2`) and feeds every batch to the local
//! `ReplicateEventsV2` path, retrying service-busy errors like the Go
//! `RetryableClient` wrapper used by `NewHistoryResender`'s replication fn.
//!
//! Deviation: Go's `currentExecutionCheck` invariant (fixCurrentExecution) is
//! not ported; an `EntityNotExists` from the local replicate call is returned
//! as-is (Go returns it too when the invariant is nil or reports a problem).
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::admin::GetWorkflowExecutionRawHistoryV2Request;
use cadence_api::types::WorkflowExecution;
use cadence_common::backoff::ExponentialRetryPolicy;

use crate::replication::deps::{
    throttle_retry, ClientBean, DomainCache, HistoryClient, HistoryResender, ReplicateEventsV2Request,
    ReplicationError, Result as ReplResult, RetryPolicy,
};

/// `resendContextTimeout`.
const RESEND_CONTEXT_TIMEOUT: Duration = Duration::from_secs(30);
/// `defaultPageSize`.
const DEFAULT_PAGE_SIZE: i32 = 100;

pub struct HistoryResenderImpl {
    domains: Arc<dyn DomainCache>,
    client_bean: Arc<dyn ClientBean>,
    /// Local history client used to deliver `ReplicateEventsV2` (Go: `historyReplicationFn`).
    history: Arc<dyn HistoryClient>,
}

impl HistoryResenderImpl {
    pub fn new(
        domains: Arc<dyn DomainCache>,
        client_bean: Arc<dyn ClientBean>,
        history: Arc<dyn HistoryClient>,
    ) -> Self {
        Self { domains, client_bean, history }
    }

    /// `common.CreateReplicationServiceBusyRetryPolicy`.
    fn busy_policy() -> ExponentialRetryPolicy {
        ExponentialRetryPolicy::new(Duration::from_secs(2))
            .with_maximum_interval(Duration::from_secs(10))
            .with_expiration_interval(Duration::from_secs(5 * 60))
    }

    async fn send(&self, req: ReplicateEventsV2Request) -> ReplResult<()> {
        let policy = Self::busy_policy();
        throttle_retry(&policy as &dyn RetryPolicy, |e| e.is_service_busy(), || {
            let req = req.clone();
            async move { self.history.replicate_events_v2(req, RESEND_CONTEXT_TIMEOUT).await }
        })
        .await
    }
}

#[async_trait]
impl HistoryResender for HistoryResenderImpl {
    async fn send_single_workflow_history(
        &self,
        remote_cluster: &str,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
        start_event_id: Option<i64>,
        start_event_version: Option<i64>,
        end_event_id: Option<i64>,
        end_event_version: Option<i64>,
    ) -> ReplResult<()> {
        let domain_name = self
            .domains
            .get_domain_name(domain_id)
            .map_err(|e| ReplicationError::Other(format!("getting domain: {e}")))?;
        let mut token: Vec<u8> = Vec::new();
        loop {
            let admin = self.client_bean.remote_admin_client(remote_cluster)?;
            let page = admin
                .get_workflow_execution_raw_history_v2(
                    GetWorkflowExecutionRawHistoryV2Request {
                        domain: domain_name.clone(),
                        execution: Some(WorkflowExecution {
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                        }),
                        start_event_id,
                        start_event_version,
                        end_event_id,
                        end_event_version,
                        maximum_page_size: DEFAULT_PAGE_SIZE,
                        next_page_token: token.clone(),
                    },
                    RESEND_CONTEXT_TIMEOUT,
                )
                .await
                .map_err(|e| ReplicationError::Other(format!("history iterator: getting history: {e}")))?;
            let items = page.version_history.map(|v| v.items);
            for batch in page.history_batches {
                let req = ReplicateEventsV2Request {
                    domain_uuid: Some(domain_id.to_string()),
                    workflow_execution: WorkflowExecution {
                        workflow_id: workflow_id.to_string(),
                        run_id: run_id.to_string(),
                    },
                    version_history_items: items.clone(),
                    events: Some(batch),
                    new_run_events: None,
                };
                match self.send(req).await {
                    Ok(()) => {}
                    Err(e @ ReplicationError::EntityNotExists(_)) => return Err(e),
                    Err(e) => {
                        return Err(ReplicationError::Other(format!("sending replication request: {e}")))
                    }
                }
            }
            if page.next_page_token.is_empty() {
                return Ok(());
            }
            token = page.next_page_token;
        }
    }
}
