//! Port of Go `service/worker/parentclosepolicy/client.go` (`clientImpl.SendParentClosePolicyRequest`):
//! SignalWithStart of the system worker's parent-close-policy workflow in the
//! `cadence-system` domain through the Frontend (Go `PublicClient`).

use std::time::Duration;

use cadence_api::{enums::WorkflowIDReusePolicy, service_types as st, types as t};
use rand::Rng;

use super::env::HistoryEnv;
use crate::task::deps::TaskError;
use crate::task::transfer_active_task_executor::{ParentClosePolicy, ParentClosePolicyRequest};

type TaskResult<T> = Result<T, TaskError>;

const SIGNAL_TIMEOUT: Duration = Duration::from_millis(400);
const WORKFLOW_ID_PREFIX: &str = "parent-close-policy-workflow";
const SYSTEM_LOCAL_DOMAIN_NAME: &str = "cadence-system";
const PROCESSOR_TASK_LIST_NAME: &str = "cadence-sys-processor-parent-close-policy";
const PROCESSOR_WF_TYPE_NAME: &str = "cadence-sys-parent-close-policy-workflow";
const PROCESSOR_CHANNEL_NAME: &str = "ParentClosePolicyProcessorChannelName";
/// `infiniteDuration = 20 * 365 * 24 * time.Hour` in seconds.
const INFINITE_SECONDS: i32 = 20 * 365 * 24 * 3600;

fn policy_text(p: ParentClosePolicy) -> String {
    // types.ParentClosePolicy.MarshalText
    match p {
        ParentClosePolicy::Abandon => "ABANDON".into(),
        ParentClosePolicy::RequestCancel => "REQUEST_CANCEL".into(),
        ParentClosePolicy::Terminate => "TERMINATE".into(),
        ParentClosePolicy::Unknown(v) => format!("ParentClosePolicy({v})"),
    }
}

/// SDK default data converter: JSON of the Go `parentclosepolicy.Request` (no json tags,
/// `types.WorkflowExecution` has `workflowId`/`runId` omitempty tags).
fn encode(request: &ParentClosePolicyRequest) -> Vec<u8> {
    let executions: Vec<serde_json::Value> = request
        .executions
        .iter()
        .map(|e| {
            serde_json::json!({
                "DomainID": e.domain_id,
                "DomainName": e.domain_name,
                "WorkflowID": e.workflow_id,
                "RunID": e.run_id,
                "Policy": policy_text(e.policy),
            })
        })
        .collect();
    let v = serde_json::json!({
        "ParentExecution": {},
        "Executions": executions,
        "DomainName": request.domain_name,
    });
    let mut out = serde_json::to_vec(&v).unwrap_or_default();
    out.push(b'\n'); // json.Encoder newline
    out
}

pub async fn send(env: &HistoryEnv, request: ParentClosePolicyRequest) -> TaskResult<()> {
    let num = (env.config.num_parent_close_policy_system_workflows)().max(1);
    let random_id = rand::thread_rng().gen_range(0..num);
    let workflow_id = format!("{WORKFLOW_ID_PREFIX}-{random_id}");
    let meta = env.cluster.metadata();
    let info = meta
        .clusters
        .get(&meta.current_cluster_name)
        .ok_or_else(|| TaskError::InternalService("current cluster info not found".into()))?;
    let channel = cadence_service_core::grpc_tls::endpoint_channel(&info.rpc_address, None)
        .map_err(|e| TaskError::InternalService(e.to_string()))?;
    let req = st::SignalWithStartWorkflowExecutionRequest {
        domain: SYSTEM_LOCAL_DOMAIN_NAME.into(),
        workflow_id: workflow_id.clone(),
        workflow_type: Some(t::WorkflowType { name: PROCESSOR_WF_TYPE_NAME.into() }),
        task_list: Some(t::TaskList { name: PROCESSOR_TASK_LIST_NAME.into(), ..Default::default() }),
        execution_start_to_close_timeout_seconds: Some(INFINITE_SECONDS),
        task_start_to_close_timeout_seconds: Some(60),
        identity: "cadence-history-parent-close-policy".into(),
        request_id: uuid::Uuid::new_v4().to_string(),
        workflow_id_reuse_policy: Some(WorkflowIDReusePolicy::AllowDuplicate),
        signal_name: PROCESSOR_CHANNEL_NAME.into(),
        signal_input: encode(&request),
        ..Default::default()
    };
    let proto = cadence_proto::convert::client::signal_with_start_request_out(req);
    let mut r = tonic::Request::new(proto);
    r.set_timeout(SIGNAL_TIMEOUT);
    for (k, v) in [("rpc-service", info.rpc_name.as_str()), ("rpc-caller", "cadence-history"), ("rpc-encoding", "proto")] {
        if let Ok(v) = tonic::metadata::MetadataValue::try_from(v) {
            r.metadata_mut().insert(k, v);
        }
    }
    let mut c = cadence_proto::api::workflow_api_client::WorkflowApiClient::new(channel);
    c.signal_with_start_workflow_execution(r)
        .await
        .map_err(|s| TaskError::Other(format!("parent close policy signal-with-start: {}", s.message())))?;
    Ok(())
}
