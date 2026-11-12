//! History-side wiring of the archiver client (Go `service/history/resource/resource.go`
//! `archiver.NewClient(...)`): the history batch source (Go `archiver.HistoryIterator` over the
//! history manager), the archival signal client (Go `cadenceClient.SignalWithStartWorkflow` through
//! the Frontend `PublicClient`) and the per-member inline rate limiters.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::{enums::WorkflowIDReusePolicy, service_types as st, types as t};
use cadence_common::archiver::{ArchiveHistoryRequest, ArchiverError, HistoryBatchSource};
use cadence_persistence::history_reader::{read_history_branch, HistoryNodeStore, HistoryReadError, ReadHistoryBranchRequest};
use serde_json::{json, Value};

use super::archiver_client::{AllowLimiter, ArchivalSignaler, ArchiveRequest, ArchiverClient, ArchivalTarget, FileInlineArchivers};
use crate::config::Config;
use crate::wiring::resources::cluster::ClusterMetadataImpl;
use crate::wiring::resources::membership::MembershipResolver;

/// Go `historyPageSize`.
const HISTORY_PAGE_SIZE: i64 = 250;
/// Go `constants.EndEventID`.
const END_EVENT_ID: i64 = i64::MAX;
/// Go `archiver.ErrHistoryMutated`.
pub const ERR_HISTORY_MUTATED: &str = "history was mutated";

/// Go `decisionTaskList`, `signalName`, `archivalWorkflowFnName`, timeouts.
const DECISION_TASK_LIST: &str = "cadence-archival-tl";
const SIGNAL_NAME: &str = "cadence-archival-signal";
const ARCHIVAL_WORKFLOW_FN_NAME: &str = "archivalWorkflow";
const WORKFLOW_START_TO_CLOSE_SECONDS: i32 = 30 * 24 * 3600;
const WORKFLOW_TASK_START_TO_CLOSE_SECONDS: i32 = 60;
const SYSTEM_LOCAL_DOMAIN_NAME: &str = "cadence-system";

/// Go `archiver.NewHistoryIterator` drained into one list, with `IsHistoryMutated` applied to the
/// final blob (the only one that has `isLast`). `Ok(None)` = history gone on the first read.
pub struct HistoryNodeBatchSource {
    pub store: Arc<dyn HistoryNodeStore>,
    pub shard_id: i32,
    /// `Config.AllowArchivingIncompleteHistory`.
    pub allow_incomplete: bool,
}

impl HistoryBatchSource for HistoryNodeBatchSource {
    /// Go `readHistory` = `ReadFullPageV2EventsByBatch`: reads `ReadHistoryBranch` pages from
    /// `first_event_id` until `historyPageSize` events were read or the token is exhausted.
    fn read_page(&self, request: &ArchiveHistoryRequest, first_event_id: i64) -> Result<Vec<Value>, ArchiverError> {
        let mut batches: Vec<Value> = Vec::new();
        let mut events = 0_i64;
        let mut token = Vec::new();
        loop {
            let req = ReadHistoryBranchRequest {
                branch_token: request.branch_token.clone(),
                min_event_id: first_event_id,
                max_event_id: END_EVENT_ID,
                page_size: HISTORY_PAGE_SIZE,
                next_page_token: token,
                shard_id: self.shard_id,
            };
            let resp = read_history_branch(self.store.as_ref(), &req).map_err(|e| match e {
                HistoryReadError::EntityNotExists(m) => ArchiverError::EntityNotExists(m),
                HistoryReadError::InvalidRequest(m) => ArchiverError::BadRequest(m),
                e => ArchiverError::Internal(e.to_string()),
            })?;
            for b in resp.batches.into_iter().filter(|b| !b.is_empty()) {
                events += b.len() as i64;
                batches.push(serde_json::to_value(st::History { events: b }).map_err(|e| ArchiverError::Other(e.to_string()))?);
            }
            if events >= HISTORY_PAGE_SIZE || resp.next_page_token.is_empty() {
                return Ok(batches);
            }
            token = resp.next_page_token;
        }
    }

    fn archive_incomplete_history(&self) -> bool {
        self.allow_incomplete
    }

    fn read_all(&self, request: &ArchiveHistoryRequest) -> Result<Option<Vec<Value>>, ArchiverError> {
        let mut batches: Vec<Vec<st::HistoryEvent>> = Vec::new();
        let mut token = Vec::new();
        loop {
            let req = ReadHistoryBranchRequest {
                branch_token: request.branch_token.clone(),
                min_event_id: 1,
                max_event_id: END_EVENT_ID,
                page_size: HISTORY_PAGE_SIZE,
                next_page_token: token.clone(),
                shard_id: self.shard_id,
            };
            match read_history_branch(self.store.as_ref(), &req) {
                Ok(resp) => {
                    batches.extend(resp.batches.into_iter().filter(|b| !b.is_empty()));
                    if resp.next_page_token.is_empty() {
                        break;
                    }
                    token = resp.next_page_token;
                }
                // Go: EntityNotExists on the first read fails (skipped by the caller), later it ends the iteration.
                Err(HistoryReadError::EntityNotExists(_)) if batches.is_empty() => return Ok(None),
                Err(HistoryReadError::EntityNotExists(_)) => break,
                Err(HistoryReadError::InvalidRequest(m)) => return Err(ArchiverError::BadRequest(m)),
                Err(e) => return Err(ArchiverError::Internal(e.to_string())),
            }
        }
        if batches.is_empty() {
            return Ok(None);
        }
        if !self.allow_incomplete && is_history_mutated(request, &batches) {
            return Err(ArchiverError::Other(ERR_HISTORY_MUTATED.into()));
        }
        batches
            .into_iter()
            .map(|events| serde_json::to_value(st::History { events }).map_err(|e| ArchiverError::Other(e.to_string())))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }
}

/// Archive options of the inline archivers (Go `archiveHistoryInline`): only the metrics client of
/// the archiver container; no `ProgressManager` and no `NonRetriableError` option is passed.
pub fn inline_archive_options(metrics: Arc<dyn cadence_common::cloud_archiver::ArchiverMetrics>) -> cadence_common::cloud_archiver::ArchiveOptions {
    cadence_common::cloud_archiver::ArchiveOptions { metrics, non_retriable_error: None, progress_manager: None }
}

/// Go `archiver.IsHistoryMutated` with `isLast = true` (the whole history is read).
pub fn is_history_mutated(request: &ArchiveHistoryRequest, batches: &[Vec<st::HistoryEvent>]) -> bool {
    let Some(last) = batches.last().and_then(|b| b.last()) else {
        return false;
    };
    if last.version > request.close_failover_version {
        return true;
    }
    last.version != request.close_failover_version || last.event_id + 1 != request.next_event_id
}

/// Go `types.WorkflowExecutionCloseStatus.String()`.
fn close_status_text(v: i32) -> String {
    match v {
        0 => "COMPLETED".into(),
        1 => "FAILED".into(),
        2 => "CANCELED".into(),
        3 => "TERMINATED".into(),
        4 => "CONTINUED_AS_NEW".into(),
        5 => "TIMED_OUT".into(),
        n => format!("WorkflowExecutionCloseStatus({n})"),
    }
}

fn bytes_map(m: &std::collections::BTreeMap<String, Vec<u8>>) -> Value {
    Value::Object(m.iter().map(|(k, v)| (k.clone(), Value::String(cadence_api::b64::encode_bytes(v)))).collect())
}

/// SDK default data converter encoding of Go `archiver.ArchiveRequest` (no json tags, so Go field
/// names; `[]byte` is base64, `*types.Memo` is `{"fields":{..}}`, close status is text).
pub fn encode_archive_request(r: &ArchiveRequest) -> Vec<u8> {
    let memo = match &r.memo {
        None => Value::Null,
        Some(fields) if fields.is_empty() => json!({}),
        Some(fields) => json!({ "fields": bytes_map(fields) }),
    };
    let search_attributes = if r.search_attributes.is_empty() { Value::Null } else { bytes_map(&r.search_attributes) };
    let targets: Vec<i32> = r
        .targets
        .iter()
        .map(|t| match t {
            ArchivalTarget::History => 0,
            ArchivalTarget::Visibility => 1,
        })
        .collect();
    let v = json!({
        "DomainID": r.domain_id,
        "DomainName": r.domain_name,
        "WorkflowID": r.workflow_id,
        "RunID": r.run_id,
        "ShardID": r.shard_id,
        "BranchToken": if r.branch_token.is_empty() { Value::Null } else { Value::String(cadence_api::b64::encode_bytes(&r.branch_token)) },
        "NextEventID": r.next_event_id,
        "CloseFailoverVersion": r.close_failover_version,
        "URI": r.uri,
        "WorkflowTypeName": r.workflow_type_name,
        "StartTimestamp": r.start_timestamp,
        "ExecutionTimestamp": r.execution_timestamp,
        "CloseTimestamp": r.close_timestamp,
        "CloseStatus": close_status_text(r.close_status),
        "HistoryLength": r.history_length,
        "Memo": memo,
        "SearchAttributes": search_attributes,
        "VisibilityURI": r.visibility_uri,
        "Targets": if targets.is_empty() { Value::Null } else { json!(targets) },
    });
    let mut out = serde_json::to_vec(&v).unwrap_or_default();
    out.push(b'\n'); // json.Encoder newline
    out
}

/// Go `cadenceClient.SignalWithStartWorkflow(ctx, workflowID, signalName, *request, workflowOptions,
/// archivalWorkflowFnName, nil)` through the current cluster's Frontend (Go `PublicClient`).
pub struct FrontendArchivalSignaler {
    pub cluster: Arc<ClusterMetadataImpl>,
}

fn build_request(workflow_id: &str, request: &ArchiveRequest) -> st::SignalWithStartWorkflowExecutionRequest {
    st::SignalWithStartWorkflowExecutionRequest {
        domain: SYSTEM_LOCAL_DOMAIN_NAME.into(),
        workflow_id: workflow_id.into(),
        workflow_type: Some(t::WorkflowType { name: ARCHIVAL_WORKFLOW_FN_NAME.into() }),
        task_list: Some(t::TaskList { name: DECISION_TASK_LIST.into(), ..Default::default() }),
        input: b"null\n".to_vec(), // workflowArgs = nil
        execution_start_to_close_timeout_seconds: Some(WORKFLOW_START_TO_CLOSE_SECONDS),
        task_start_to_close_timeout_seconds: Some(WORKFLOW_TASK_START_TO_CLOSE_SECONDS),
        identity: "cadence-history-archiver".into(),
        request_id: uuid::Uuid::new_v4().to_string(),
        workflow_id_reuse_policy: Some(WorkflowIDReusePolicy::AllowDuplicate),
        signal_name: SIGNAL_NAME.into(),
        signal_input: encode_archive_request(request),
        ..Default::default()
    }
}

#[async_trait]
impl ArchivalSignaler for FrontendArchivalSignaler {
    async fn signal_with_start(&self, workflow_id: &str, request: &ArchiveRequest, timeout: Duration) -> Result<(), String> {
        let meta = self.cluster.metadata();
        let info = meta.clusters.get(&meta.current_cluster_name).ok_or_else(|| "current cluster info not found".to_string())?;
        let channel = cadence_service_core::grpc_tls::endpoint_channel(&info.rpc_address, None).map_err(|e| e.to_string())?;
        let proto = cadence_proto::convert::client::signal_with_start_request_out(build_request(workflow_id, request));
        let mut r = tonic::Request::new(proto);
        r.set_timeout(timeout);
        for (k, v) in [("rpc-service", info.rpc_name.as_str()), ("rpc-caller", "cadence-history"), ("rpc-encoding", "proto")] {
            if let Ok(v) = tonic::metadata::MetadataValue::try_from(v) {
                r.metadata_mut().insert(k, v);
            }
        }
        let mut c = cadence_proto::api::workflow_api_client::WorkflowApiClient::new(channel);
        c.signal_with_start_workflow_execution(r).await.map(|_| ()).map_err(|s| s.message().to_string())
    }
}

/// Go `permember.PerMember(service.History, global, instance, resolver)`.
fn per_member(global: i64, instance: i64, membership: &MembershipResolver) -> i64 {
    if global <= 0 {
        return instance;
    }
    let count = membership.members().len();
    if count < 1 {
        return instance;
    }
    let avg = (global as f64 / count as f64).max(1.0);
    avg.min(instance as f64) as i64
}

/// Go `archiver.NewClient(...)` as built in `service/history/resource/resource.go`.
pub fn build_client(
    config: &Arc<Config>,
    membership: Arc<MembershipResolver>,
    cluster: Arc<ClusterMetadataImpl>,
    history_nodes: Arc<dyn HistoryNodeStore>,
    provider: Arc<cadence_common::archiver::ArchiverProvider>,
) -> ArchiverClient {
    let (c1, c2, c3, c4) = (config.clone(), config.clone(), config.clone(), config.clone());
    let (m1, m2) = (membership.clone(), membership);
    let (g1, g2) = (config.clone(), config.clone());
    ArchiverClient::new(
        Box::new(move || (c1.num_archive_system_workflows)()),
        AllowLimiter::new(move || (c2.archive_request_rps)()),
        AllowLimiter::new(move || per_member((c3.archive_inline_history_global_rps)(), (g1.archive_inline_history_rps)(), &m1)),
        AllowLimiter::new(move || per_member((c4.archive_inline_visibility_global_rps)(), (g2.archive_inline_visibility_rps)(), &m2)),
        Arc::new(FileInlineArchivers {
            history_nodes: Some(history_nodes),
            allow_incomplete: Some(config.allow_archiving_incomplete_history.clone()),
            provider,
        }),
        Arc::new(FrontendArchivalSignaler { cluster }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::enums::EventType;

    fn ev(id: i64, version: i64) -> st::HistoryEvent {
        st::HistoryEvent { event_id: id, version, event_type: Some(EventType::WorkflowExecutionStarted), ..Default::default() }
    }

    fn req(next: i64, version: i64) -> ArchiveHistoryRequest {
        ArchiveHistoryRequest {
            domain_id: "d".into(),
            domain_name: "dn".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            branch_token: vec![1],
            next_event_id: next,
            close_failover_version: version,
        }
    }

    // Go TestIsHistoryMutated (common/archiver/util_test.go).
    #[test]
    fn history_mutated_cases() {
        let b = vec![vec![ev(1, 1), ev(2, 1), ev(3, 1)]];
        assert!(!is_history_mutated(&req(4, 1), &b));
        assert!(is_history_mutated(&req(5, 1), &b), "next event id mismatch");
        assert!(is_history_mutated(&req(4, 2), &b), "last version below close version");
        assert!(is_history_mutated(&req(4, 0), &b), "last version above close version");
        assert!(!is_history_mutated(&req(4, 1), &[]));
    }

    #[test]
    fn inline_options_have_no_progress_manager_or_non_retriable_error() {
        let o = inline_archive_options(Arc::new(cadence_common::cloud_archiver::NoopArchiverMetrics));
        assert!(o.progress_manager.is_none());
        assert!(o.non_retriable_error.is_none());
    }

    #[test]
    fn close_status_names_match_go() {
        assert_eq!(close_status_text(0), "COMPLETED");
        assert_eq!(close_status_text(4), "CONTINUED_AS_NEW");
        assert_eq!(close_status_text(5), "TIMED_OUT");
        assert_eq!(close_status_text(9), "WorkflowExecutionCloseStatus(9)");
    }

    #[test]
    fn archive_request_json_uses_go_field_names() {
        let r = ArchiveRequest {
            domain_id: "id".into(),
            domain_name: "dom".into(),
            workflow_id: "wf".into(),
            run_id: "run".into(),
            shard_id: 7,
            branch_token: vec![1, 2, 3],
            next_event_id: 9,
            close_failover_version: 4,
            uri: "file:///tmp/a".into(),
            close_status: 3,
            memo: Some([("k".to_string(), b"v".to_vec())].into()),
            targets: vec![ArchivalTarget::History, ArchivalTarget::Visibility],
            ..Default::default()
        };
        let bytes = encode_archive_request(&r);
        assert_eq!(*bytes.last().unwrap(), b'\n');
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["DomainID"], "id");
        assert_eq!(v["ShardID"], 7);
        assert_eq!(v["BranchToken"], "AQID");
        assert_eq!(v["URI"], "file:///tmp/a");
        assert_eq!(v["CloseStatus"], "TERMINATED");
        assert_eq!(v["Memo"], json!({"fields": {"k": "dg=="}}));
        assert_eq!(v["SearchAttributes"], Value::Null);
        assert_eq!(v["Targets"], json!([0, 1]));
    }

    #[test]
    fn signal_request_matches_go_options() {
        let q = build_request("cadence-archival-5", &ArchiveRequest::default());
        assert_eq!(q.domain, "cadence-system");
        assert_eq!(q.workflow_id, "cadence-archival-5");
        assert_eq!(q.workflow_type.unwrap().name, "archivalWorkflow");
        assert_eq!(q.task_list.unwrap().name, "cadence-archival-tl");
        assert_eq!(q.signal_name, "cadence-archival-signal");
        assert_eq!(q.execution_start_to_close_timeout_seconds, Some(2_592_000));
        assert_eq!(q.task_start_to_close_timeout_seconds, Some(60));
        assert_eq!(q.workflow_id_reuse_policy, Some(WorkflowIDReusePolicy::AllowDuplicate));
        assert_eq!(q.input, b"null\n");
    }

    #[test]
    fn per_member_matches_go() {
        let m = MembershipResolver::new("a", vec![]);
        assert_eq!(per_member(0, 1000, &m), 1000, "no global limit");
        assert_eq!(per_member(10000, 1000, &m), 1000, "no members -> instance");
    }
}
