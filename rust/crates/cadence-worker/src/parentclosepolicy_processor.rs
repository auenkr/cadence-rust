//! Port of Go `parentclosepolicy.ProcessorActivity`: applies each child's
//! parent close policy through the History service and forwards executions of
//! passive domains to their active cluster's Frontend.

use std::collections::BTreeMap;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use cadence_proto::api as pb;
use prost::Message;
use tonic::transport::Channel;

use crate::client::CadenceClient;
use crate::parentclosepolicy::*;
use crate::sdk::activity::{ActivityFailure, ActivityRegistry};
use crate::sdk::converter;

/// Go `signalTimeout`.
pub const SIGNAL_REMOTE_TIMEOUT: Duration = Duration::from_millis(SIGNAL_TIMEOUT_MILLIS);

/// External dependencies of the activity (history client, domain cache and
/// remote frontend clients in Go).
pub trait Backend: Send + Sync + 'static {
    fn resolve_domain_id(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<String, DomainLookupError>> + Send;
    fn apply_history(
        &self,
        operation: &Operation,
    ) -> impl Future<Output = Result<(), HistoryError>> + Send;
    fn active_cluster(&self, domain_id: &str)
        -> impl Future<Output = Result<String, String>> + Send;
    fn signal_remote(
        &self,
        cluster: &str,
        request: pb::SignalWithStartWorkflowExecutionRequest,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

fn random_u64() -> u64 {
    std::collections::hash_map::RandomState::new().build_hasher().finish()
}

pub(crate) fn random_request_id() -> String {
    let (a, b) = (random_u64(), random_u64());
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (a >> 32) as u32,
        (a >> 16) as u16,
        (a & 0xfff) as u16,
        ((b >> 48) as u16 & 0x3fff) | 0x8000,
        b & 0xffff_ffff_ffff
    )
}

/// The body of `ProcessorActivity`.
pub async fn process_request<B: Backend>(
    backend: &B,
    request: &Request,
    num_workflows: usize,
) -> Result<(), String> {
    let mut remote: BTreeMap<String, Vec<RequestDetail>> = BTreeMap::new();
    let child_workflow_only = !request.parent_execution.workflow_id.is_empty()
        && !request.parent_execution.run_id.is_empty();
    for execution in &request.executions {
        let domain_name = if execution.domain_name.is_empty() {
            request.domain_name.as_str()
        } else {
            execution.domain_name.as_str()
        };
        let domain_id = if execution.domain_id.is_empty() {
            match backend.resolve_domain_id(domain_name).await {
                Ok(id) => id,
                Err(DomainLookupError::EntityNotExists) => continue,
                Err(DomainLookupError::Other(e)) => return Err(e),
            }
        } else {
            execution.domain_id.clone()
        };
        let action = match execution.policy {
            POLICY_ABANDON => continue,
            POLICY_REQUEST_CANCEL => Action::RequestCancel,
            POLICY_TERMINATE => Action::Terminate,
            p => return Err(format!("unknown parent close policy: {p}")),
        };
        let operation = Operation {
            action,
            domain_id,
            domain_name: domain_name.to_owned(),
            workflow_id: execution.workflow_id.clone(),
            first_execution_run_id: execution.run_id.clone(),
            identity: WORKFLOW_TYPE,
            reason: (action == Action::Terminate).then_some(TERMINATE_REASON),
            child_workflow_only,
            external_workflow_execution: child_workflow_only
                .then(|| request.parent_execution.clone()),
        };
        match backend.apply_history(&operation).await {
            Ok(())
            | Err(HistoryError::EntityNotExists)
            | Err(HistoryError::WorkflowAlreadyCompleted)
            | Err(HistoryError::CancellationAlreadyRequested) => {}
            Err(HistoryError::DomainNotActive) => {
                let cluster = backend.active_cluster(&operation.domain_id).await?;
                remote.entry(cluster).or_default().push(execution.clone());
            }
            Err(HistoryError::Other(e)) => return Err(e),
        }
    }
    for (cluster, executions) in remote {
        let signal = Request {
            parent_execution: request.parent_execution.clone(),
            executions,
            domain_name: String::new(),
        };
        let shard = (random_u64() % num_workflows.max(1) as u64) as usize;
        let rpc = remote_signal_request(shard, random_request_id(), &signal)
            .map_err(|e| e.to_string())?;
        backend.signal_remote(&cluster, rpc).await?;
    }
    Ok(())
}

/// Registers the activity in the SDK activity registry.
pub fn register_activity<B: Backend>(
    registry: &mut ActivityRegistry,
    backend: Arc<B>,
    num_workflows: usize,
) {
    registry.register(ACTIVITY_TYPE, move |_ctx, input| {
        let backend = backend.clone();
        async move {
            let request: Request = converter::decode(&input)
                .map_err(|e| ActivityFailure::generic(e.to_string()))?;
            process_request(backend.as_ref(), &request, num_workflows)
                .await
                .map(|()| Some(converter::encode(&()).unwrap_or_default()))
                .map_err(ActivityFailure::generic)
        }
    });
}

#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

pub(crate) fn detail_name(status: &tonic::Status) -> String {
    RpcStatus::decode(status.details())
        .ok()
        .and_then(|s| s.details.into_iter().next())
        .map(|d| d.type_url.rsplit('/').next().unwrap_or_default().to_owned())
        .unwrap_or_default()
}

/// Maps a gRPC error from History like Go's proto error mapper.
pub fn history_error(status: &tonic::Status) -> HistoryError {
    use tonic::Code;
    match (status.code(), detail_name(status).as_str()) {
        (Code::NotFound, "uber.cadence.api.v1.EntityNotExistsError") => {
            HistoryError::EntityNotExists
        }
        (Code::NotFound, "uber.cadence.api.v1.WorkflowExecutionAlreadyCompletedError") => {
            HistoryError::WorkflowAlreadyCompleted
        }
        (Code::AlreadyExists, "uber.cadence.api.v1.CancellationAlreadyRequestedError") => {
            HistoryError::CancellationAlreadyRequested
        }
        (Code::FailedPrecondition, "uber.cadence.api.v1.DomainNotActiveError") => {
            HistoryError::DomainNotActive
        }
        _ => HistoryError::Other(status.to_string()),
    }
}

/// Resolves the Frontend client of a remote cluster by name.
pub type RemoteResolver = Arc<dyn Fn(&str) -> Option<CadenceClient> + Send + Sync>;

/// Production backend over gRPC.
pub struct GrpcBackend {
    pub history: HistoryClient,
    pub local: CadenceClient,
    /// Resolves the Frontend client of a remote cluster by name.
    pub remote: RemoteResolver,
}

impl GrpcBackend {
    pub fn new(
        channel: Channel,
        local: CadenceClient,
        remote: RemoteResolver,
    ) -> Self {
        Self {
            history: HistoryClient::new(channel),
            local,
            remote,
        }
    }

    #[allow(clippy::result_large_err)]
    async fn describe(
        &self,
        by: pb::describe_domain_request::DescribeBy,
    ) -> Result<pb::Domain, tonic::Status> {
        let mut c = self.local.domain_api();
        let r = c
            .describe_domain(tonic::Request::new(pb::DescribeDomainRequest {
                describe_by: Some(by),
            }))
            .await?;
        r.into_inner()
            .domain
            .ok_or_else(|| tonic::Status::internal("DescribeDomain returned no domain"))
    }
}

impl Backend for GrpcBackend {
    async fn resolve_domain_id(&self, name: &str) -> Result<String, DomainLookupError> {
        use pb::describe_domain_request::DescribeBy;
        match self.describe(DescribeBy::Name(name.to_owned())).await {
            Ok(d) => Ok(d.id),
            Err(s) if s.code() == tonic::Code::NotFound => Err(DomainLookupError::EntityNotExists),
            Err(s) => Err(DomainLookupError::Other(s.to_string())),
        }
    }

    async fn apply_history(&self, operation: &Operation) -> Result<(), HistoryError> {
        let mut h = self.history.clone();
        h.apply(operation).await.map_err(|s| history_error(&s))
    }

    async fn active_cluster(&self, domain_id: &str) -> Result<String, String> {
        use pb::describe_domain_request::DescribeBy;
        self.describe(DescribeBy::Id(domain_id.to_owned()))
            .await
            .map(|d| d.active_cluster_name)
            .map_err(|s| s.to_string())
    }

    async fn signal_remote(
        &self,
        cluster: &str,
        request: pb::SignalWithStartWorkflowExecutionRequest,
    ) -> Result<(), String> {
        let mut client = (self.remote)(cluster)
            .ok_or_else(|| format!("remote frontend client for cluster {cluster} not found"))?;
        match tokio::time::timeout(SIGNAL_REMOTE_TIMEOUT, client.signal_with_start(request)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(s)) => Err(s.to_string()),
            Err(_) => Err("context deadline exceeded".to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct Fake {
        applied: Mutex<Vec<Operation>>,
        signals: Mutex<Vec<(String, pb::SignalWithStartWorkflowExecutionRequest)>>,
        inactive: bool,
        missing_domain: bool,
    }

    impl Backend for Fake {
        async fn resolve_domain_id(&self, name: &str) -> Result<String, DomainLookupError> {
            if self.missing_domain {
                Err(DomainLookupError::EntityNotExists)
            } else {
                Ok(format!("id-{name}"))
            }
        }
        async fn apply_history(&self, op: &Operation) -> Result<(), HistoryError> {
            self.applied.lock().unwrap().push(op.clone());
            if self.inactive {
                Err(HistoryError::DomainNotActive)
            } else {
                Ok(())
            }
        }
        async fn active_cluster(&self, _: &str) -> Result<String, String> {
            Ok("remote".into())
        }
        async fn signal_remote(
            &self,
            cluster: &str,
            r: pb::SignalWithStartWorkflowExecutionRequest,
        ) -> Result<(), String> {
            self.signals.lock().unwrap().push((cluster.to_owned(), r));
            Ok(())
        }
    }

    fn detail(wf: &str, policy: i32, domain_id: &str) -> RequestDetail {
        RequestDetail {
            domain_id: domain_id.into(),
            domain_name: "d".into(),
            workflow_id: wf.into(),
            run_id: "r".into(),
            policy,
        }
    }

    fn request() -> Request {
        Request {
            parent_execution: WorkflowExecution {
                workflow_id: "p".into(),
                run_id: "pr".into(),
            },
            executions: vec![
                detail("a", POLICY_TERMINATE, "did"),
                detail("b", POLICY_ABANDON, "did"),
                detail("c", POLICY_REQUEST_CANCEL, ""),
            ],
            domain_name: String::new(),
        }
    }

    #[tokio::test]
    async fn applies_terminate_and_cancel_skips_abandon() {
        let f = Fake::default();
        process_request(&f, &request(), 10).await.unwrap();
        let ops = f.applied.lock().unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].action, Action::Terminate);
        assert_eq!(ops[0].reason, Some(TERMINATE_REASON));
        assert!(ops[0].child_workflow_only);
        assert_eq!(ops[1].action, Action::RequestCancel);
        assert_eq!(ops[1].domain_id, "id-d");
        assert!(f.signals.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn inactive_domain_forwards_to_active_cluster() {
        let f = Fake {
            inactive: true,
            ..Default::default()
        };
        process_request(&f, &request(), 10).await.unwrap();
        let s = f.signals.lock().unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].0, "remote");
        assert_eq!(s[0].1.signal_name, SIGNAL_NAME);
        let sent: Request =
            converter::decode(&s[0].1.signal_input.as_ref().unwrap().data).unwrap();
        assert_eq!(sent.executions.len(), 2);
    }

    #[tokio::test]
    async fn missing_domain_is_skipped_and_unknown_policy_fails() {
        let f = Fake {
            missing_domain: true,
            ..Default::default()
        };
        process_request(&f, &request(), 10).await.unwrap();
        assert_eq!(f.applied.lock().unwrap().len(), 1);
        let bad = Request {
            executions: vec![detail("x", 9, "d")],
            ..Default::default()
        };
        let e = process_request(&Fake::default(), &bad, 10).await.unwrap_err();
        assert!(e.contains("unknown parent close policy"));
    }

    #[test]
    fn maps_history_errors() {
        use prost_types::Any;
        let status = |code, name: &str| {
            let d = RpcStatus {
                code: 0,
                message: String::new(),
                details: vec![Any {
                    type_url: format!("type.googleapis.com/{name}"),
                    value: Vec::new(),
                }],
            };
            tonic::Status::with_details(code, "x", d.encode_to_vec().into())
        };
        assert_eq!(
            history_error(&status(
                tonic::Code::FailedPrecondition,
                "uber.cadence.api.v1.DomainNotActiveError"
            )),
            HistoryError::DomainNotActive
        );
        assert_eq!(
            history_error(&status(
                tonic::Code::NotFound,
                "uber.cadence.api.v1.EntityNotExistsError"
            )),
            HistoryError::EntityNotExists
        );
        assert!(matches!(
            history_error(&tonic::Status::unavailable("x")),
            HistoryError::Other(_)
        ));
    }
}
