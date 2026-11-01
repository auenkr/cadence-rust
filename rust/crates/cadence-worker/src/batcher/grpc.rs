//! gRPC implementation of the batch activity's `Backend` (Go
//! `clientBean.GetFrontendClient()` and `GetRemoteAdminClient`).

use std::sync::Arc;

use cadence_proto::admin::admin_api_client::AdminApiClient;
use cadence_proto::api as pb;
use cadence_proto::api::visibility_api_client::VisibilityApiClient;
use cadence_proto::api::workflow_api_client::WorkflowApiClient;
use tonic::transport::Channel;

use super::activity::{Backend, ScanPage};
use super::{BatchOperationError, PendingChild, WorkflowExecution};
use crate::client::CadenceClient;

/// Resolves the admin channel of a remote cluster by name.
pub type AdminResolver = Arc<dyn Fn(&str) -> Option<Channel> + Send + Sync>;

pub struct GrpcBackend {
    workflow: WorkflowApiClient<Channel>,
    visibility: VisibilityApiClient<Channel>,
    local: CadenceClient,
    current_cluster: String,
    admin: AdminResolver,
}

impl GrpcBackend {
    pub fn new(
        frontend: Channel,
        local: CadenceClient,
        current_cluster: String,
        admin: AdminResolver,
    ) -> Self {
        Self {
            workflow: WorkflowApiClient::new(frontend.clone()),
            visibility: VisibilityApiClient::new(frontend),
            local,
            current_cluster,
            admin,
        }
    }
}

fn execution(e: &WorkflowExecution) -> Option<pb::WorkflowExecution> {
    Some(pb::WorkflowExecution {
        workflow_id: e.workflow_id.clone(),
        run_id: e.run_id.clone(),
    })
}

fn op_error(status: tonic::Status) -> BatchOperationError<String> {
    let entity_missing = status.code() == tonic::Code::NotFound
        && crate::parentclosepolicy_processor::detail_name(&status)
            == "uber.cadence.api.v1.EntityNotExistsError";
    if entity_missing {
        BatchOperationError::EntityNotExists
    } else {
        BatchOperationError::Other(status.to_string())
    }
}

impl Backend for GrpcBackend {
    fn current_cluster(&self) -> String {
        self.current_cluster.clone()
    }

    fn check_remote_admin(&self, cluster: &str) -> Result<(), String> {
        (self.admin)(cluster)
            .map(|_| ())
            .ok_or_else(|| format!("remote admin client for cluster {cluster} not found"))
    }

    async fn describe_domain_id(&self, name: &str) -> Result<String, String> {
        let mut api = self.local.domain_api();
        let response = api
            .describe_domain(tonic::Request::new(pb::DescribeDomainRequest {
                describe_by: Some(pb::describe_domain_request::DescribeBy::Name(name.to_owned())),
            }))
            .await
            .map_err(|s| s.to_string())?;
        response
            .into_inner()
            .domain
            .map(|d| d.id)
            .ok_or_else(|| "DescribeDomain returned no domain".to_owned())
    }

    async fn count(&self, domain: &str, query: &str) -> Result<i64, String> {
        let mut c = self.visibility.clone();
        c.count_workflow_executions(tonic::Request::new(pb::CountWorkflowExecutionsRequest {
            domain: domain.to_owned(),
            query: query.to_owned(),
        }))
        .await
        .map(|r| r.into_inner().count)
        .map_err(|s| s.to_string())
    }

    async fn scan(
        &self,
        domain: &str,
        page_size: i32,
        token: &[u8],
        query: &str,
    ) -> Result<ScanPage, String> {
        let mut c = self.visibility.clone();
        let r = c
            .scan_workflow_executions(tonic::Request::new(pb::ScanWorkflowExecutionsRequest {
                domain: domain.to_owned(),
                page_size,
                next_page_token: token.to_vec(),
                query: query.to_owned(),
            }))
            .await
            .map_err(|s| s.to_string())?
            .into_inner();
        Ok(ScanPage {
            executions: r
                .executions
                .into_iter()
                .filter_map(|e| e.workflow_execution)
                .map(|e| WorkflowExecution {
                    workflow_id: e.workflow_id,
                    run_id: e.run_id,
                })
                .collect(),
            next_page_token: r.next_page_token,
        })
    }

    async fn terminate(
        &self,
        domain: &str,
        wf: &WorkflowExecution,
        reason: &str,
        identity: &str,
    ) -> Result<(), BatchOperationError<String>> {
        let mut c = self.workflow.clone();
        c.terminate_workflow_execution(tonic::Request::new(
            pb::TerminateWorkflowExecutionRequest {
                domain: domain.to_owned(),
                workflow_execution: execution(wf),
                reason: reason.to_owned(),
                identity: identity.to_owned(),
                ..Default::default()
            },
        ))
        .await
        .map(|_| ())
        .map_err(op_error)
    }

    async fn request_cancel(
        &self,
        domain: &str,
        wf: &WorkflowExecution,
        identity: &str,
        request_id: &str,
    ) -> Result<(), BatchOperationError<String>> {
        let mut c = self.workflow.clone();
        c.request_cancel_workflow_execution(tonic::Request::new(
            pb::RequestCancelWorkflowExecutionRequest {
                domain: domain.to_owned(),
                workflow_execution: execution(wf),
                identity: identity.to_owned(),
                request_id: request_id.to_owned(),
                ..Default::default()
            },
        ))
        .await
        .map(|_| ())
        .map_err(op_error)
    }

    async fn signal(
        &self,
        domain: &str,
        wf: &WorkflowExecution,
        identity: &str,
        request_id: &str,
        name: &str,
        input: &[u8],
    ) -> Result<(), BatchOperationError<String>> {
        let mut c = self.workflow.clone();
        c.signal_workflow_execution(tonic::Request::new(pb::SignalWorkflowExecutionRequest {
            domain: domain.to_owned(),
            workflow_execution: execution(wf),
            identity: identity.to_owned(),
            request_id: request_id.to_owned(),
            signal_name: name.to_owned(),
            signal_input: Some(pb::Payload {
                data: input.to_vec(),
            }),
            ..Default::default()
        }))
        .await
        .map(|_| ())
        .map_err(op_error)
    }

    async fn resend_replication(
        &self,
        target_cluster: &str,
        domain_id: &str,
        wf: &WorkflowExecution,
        source_cluster: &str,
    ) -> Result<(), BatchOperationError<String>> {
        let channel = (self.admin)(target_cluster).ok_or_else(|| {
            BatchOperationError::Other(format!(
                "remote admin client for cluster {target_cluster} not found"
            ))
        })?;
        let mut c = AdminApiClient::new(channel);
        c.resend_replication_tasks(tonic::Request::new(
            cadence_proto::admin::ResendReplicationTasksRequest {
                domain_id: domain_id.to_owned(),
                workflow_execution: execution(wf),
                remote_cluster: source_cluster.to_owned(),
                ..Default::default()
            },
        ))
        .await
        .map(|_| ())
        .map_err(op_error)
    }

    async fn refresh(
        &self,
        domain: &str,
        wf: &WorkflowExecution,
    ) -> Result<(), BatchOperationError<String>> {
        let mut c = self.workflow.clone();
        c.refresh_workflow_tasks(tonic::Request::new(pb::RefreshWorkflowTasksRequest {
            domain: domain.to_owned(),
            workflow_execution: execution(wf),
        }))
        .await
        .map(|_| ())
        .map_err(op_error)
    }

    async fn describe_pending_children(
        &self,
        domain: &str,
        wf: &WorkflowExecution,
    ) -> Result<Vec<PendingChild>, BatchOperationError<String>> {
        let mut c = self.workflow.clone();
        let r = c
            .describe_workflow_execution(tonic::Request::new(
                pb::DescribeWorkflowExecutionRequest {
                    domain: domain.to_owned(),
                    workflow_execution: execution(wf),
                    ..Default::default()
                },
            ))
            .await
            .map_err(op_error)?
            .into_inner();
        Ok(r.pending_children
            .into_iter()
            .filter_map(|p| p.workflow_execution)
            .map(|e| PendingChild {
                workflow_id: e.workflow_id,
                run_id: e.run_id,
            })
            .collect())
    }
}
