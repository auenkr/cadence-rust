//! Real implementations of the task executor dependency traits over the shard's
//! execution cache, the SQL stores, the visibility writer and the Matching gRPC
//! client (Go `task.NewTransferTaskExecutorBase` / `NewTimerTaskExecutorBase`
//! dependencies).
//!
//! Done here: `TransferDependencies` (`TransferDepsImpl`) and
//! `CleanupDependencies` (`CleanupDepsImpl`). `archive` / `archive_visibility` go through
//! `archiver_client` (Go archiver.Client), whose inline write archivers and archival-workflow
//! signal seams are not ported yet and fail explicitly (TODO(link)); the domain archival flag is honoured so history is never deleted
//! silently when archival is enabled.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cadence_api::errors::CadenceError;
use cadence_persistence::history_contract::{self as pc, PersistenceError};
use cadence_thrift::{ToThrift, Writer};

use super::archiver_client::{ArchivalTarget, ArchiveRequest as ArchiverRequest, ClientRequest};
use super::engine_cache::{EngineExecutionCache, EngineExecutionContext};
use super::engine_state::EngineMutableState;
use super::env::HistoryEnv;
use crate::engine::deps::{HistoryError, ReleaseFunc, WorkflowExecution};
use crate::engine::deps_ms::MutableState as _;
use crate::task::deps::{
    ArchivalStatus, DomainCache, HistoryTaskCategory, HistoryTaskKey, PersistenceTask, Result as TaskResult,
    StringPropertyFnWithDomainFilter, TaskError, TaskListKind,
};
use crate::task::standby_task_util::TaskDLQWriter;
use crate::task::timer_task_executor_base::{ArchiveRequest, CleanupDependencies, CleanupWorkflow};
use crate::task::transfer_task_executor_base::{
    AddMatchingTaskRequest, TransferDependencies, VisibilityDomain, VisibilityRecord, WorkflowExecutionStatus,
};
use crate::wiring::persistence::domain_dlq::SqlTaskDlqWriter;
use crate::wiring::persistence::engine_ndc::branch_request;
use crate::wiring::persistence::visibility::VisibilityWriteRow;

fn pe(e: PersistenceError) -> TaskError {
    match e {
        PersistenceError::EntityNotExists(m) => TaskError::EntityNotExists(m),
        PersistenceError::ServiceBusy(m) => TaskError::ServiceBusy(m),
        PersistenceError::Timeout(_) => TaskError::ContextDeadlineExceeded,
        PersistenceError::ShardOwnershipLost { .. } => TaskError::ShardOwnershipLost(e.to_string()),
        other => TaskError::InternalService(other.to_string()),
    }
}

fn he(e: HistoryError) -> TaskError {
    match e {
        HistoryError::Cadence(c) => match *c {
            CadenceError::EntityNotExists(x) => TaskError::EntityNotExists(x.message),
            CadenceError::ServiceBusy(x) => TaskError::ServiceBusy(x.message),
            other => TaskError::InternalService(other.to_string()),
        },
        HistoryError::DeadlineExceeded => TaskError::ContextDeadlineExceeded,
        HistoryError::Canceled => TaskError::ContextCanceled,
        HistoryError::PersistenceShardOwnershipLost { message: m, .. } => TaskError::ShardOwnershipLost(m),
        other => {
            let text = other.to_string();
            // Persistence "Workflow execution not found" for a deleted execution is an expected EntityNotExists (Go task handling).
            if text.starts_with("Workflow execution not found") {
                TaskError::EntityNotExists(text)
            } else {
                TaskError::InternalService(text)
            }
        }
    }
}

fn status_err(s: tonic::Status) -> TaskError {
    match s.code() {
        tonic::Code::NotFound => TaskError::EntityNotExists(s.message().to_string()),
        tonic::Code::ResourceExhausted => TaskError::ServiceBusy(s.message().to_string()),
        tonic::Code::DeadlineExceeded => TaskError::ContextDeadlineExceeded,
        _ => TaskError::InternalService(format!("matching add task: {}", s.message())),
    }
}

fn thrift_close_status(persistence_close_status: i32) -> i32 {
    // persistence: None=0, Completed=1 ... TimedOut=6; thrift/visibility: Completed=0 ... TimedOut=5.
    (persistence_close_status - 1).max(0)
}

fn visibility_execution_status(status: WorkflowExecutionStatus) -> i32 {
    match status {
        WorkflowExecutionStatus::Pending => 0,
        WorkflowExecutionStatus::Started => 1,
        WorkflowExecutionStatus::Completed => 2,
        WorkflowExecutionStatus::Failed => 3,
        WorkflowExecutionStatus::Canceled => 4,
        WorkflowExecutionStatus::Terminated => 5,
        WorkflowExecutionStatus::ContinuedAsNew => 6,
        WorkflowExecutionStatus::TimedOut => 7,
    }
}

/// Go `serializeMemo` (ThriftRW `Memo` blob; empty for a missing memo).
fn encode_memo(memo: &Option<BTreeMap<String, Vec<u8>>>) -> Vec<u8> {
    let Some(fields) = memo else { return Vec::new() };
    let memo = cadence_api::types::Memo { fields: Some(fields.clone().into_iter().collect()) };
    let mut writer = Writer::new();
    writer.write_u8(0x59);
    cadence_thrift::value::write_struct(&mut writer, &memo.to_thrift());
    writer.into_bytes()
}

fn to_row(record: &VisibilityRecord, closed: bool) -> VisibilityWriteRow {
    let execution_status = if closed {
        // The close status decides the visibility status of a closed run (Go sql visibility store).
        match record.close_status {
            1 => 2,
            2 => 3,
            3 => 4,
            4 => 5,
            5 => 6,
            6 => 7,
            _ => visibility_execution_status(record.execution_status),
        }
    } else {
        visibility_execution_status(record.execution_status)
    };
    VisibilityWriteRow {
        domain_id: record.domain_id.clone(),
        workflow_id: record.workflow_id.clone(),
        run_id: record.run_id.clone(),
        start_time: record.start_timestamp,
        execution_time: record.execution_timestamp,
        workflow_type_name: record.workflow_type_name.clone(),
        memo: encode_memo(&record.memo),
        encoding: "thriftrw".to_string(),
        is_cron: record.is_cron,
        cron_schedule: record.cron_schedule.clone(),
        num_clusters: record.num_clusters,
        update_time: record.update_timestamp,
        shard_id: record.shard_id,
        execution_status,
        scheduled_execution_time: record.scheduled_execution_timestamp,
        closed: closed.then(|| (record.close_timestamp, thrift_close_status(record.close_status), record.history_length)),
        task_list: record.task_list.clone(),
        workflow_timeout_seconds: record.workflow_timeout,
        retention_seconds: record.retention_seconds,
        task_id: record.task_id,
        request_execution_status: visibility_execution_status(record.execution_status),
        cluster_attribute_scope: record.cluster_attribute.as_ref().map(|a| a.scope.clone()).unwrap_or_default(),
        cluster_attribute_name: record.cluster_attribute.as_ref().map(|a| a.name.clone()).unwrap_or_default(),
        search_attributes: record.search_attributes.clone(),
    }
}

fn proto_task_list(name: &str, kind: TaskListKind) -> cadence_proto::api::TaskList {
    cadence_proto::api::TaskList {
        name: name.to_string(),
        base_name: String::new(),
        kind: match kind {
            TaskListKind::Normal => 1,
            TaskListKind::Sticky => 2,
            TaskListKind::Ephemeral => 3,
        },
    }
}

#[allow(deprecated)]
fn partition_config_from_proto(
    c: cadence_proto::task_list_partition::matching::TaskListPartitionConfig,
) -> cadence_common::matching_lb::PartitionConfig {
    let groups = |m: std::collections::HashMap<i32, cadence_proto::matching::TaskListPartition>| {
        m.into_iter().map(|(k, v)| (k, v.isolation_groups)).collect()
    };
    cadence_common::matching_lb::PartitionConfig::from_parts(
        c.version,
        c.num_read_partitions,
        groups(c.read_partitions),
        c.num_write_partitions,
        groups(c.write_partitions),
    )
}

fn matching_metadata<T>(request: &mut tonic::Request<T>) {
    let metadata = request.metadata_mut();
    metadata.insert("rpc-service", tonic::metadata::MetadataValue::from_static("cadence-matching"));
    metadata.insert("rpc-caller", tonic::metadata::MetadataValue::from_static("cadence-history"));
    metadata.insert("rpc-encoding", tonic::metadata::MetadataValue::from_static("proto"));
    cadence_service_core::header_forwarding::apply_caller_type(metadata);
    // Go passes a context with a deadline (taskRPCCallTimeout); yarpc rejects a request
    // without one ("missing TTL"). set_timeout emits the grpc-timeout header.
    request.set_timeout(crate::task::transfer_task_executor_base::TASK_RPC_CALL_TIMEOUT);
}

/// `TransferDependencies` over the real matching client, visibility writer and domain cache.
pub struct TransferDepsImpl {
    env: Arc<HistoryEnv>,
    domains: Arc<dyn DomainCache>,
}

impl TransferDepsImpl {
    pub fn new(env: Arc<HistoryEnv>) -> Arc<Self> {
        let domains = env.domain_cache.clone() as Arc<dyn DomainCache>;
        Arc::new(Self { env, domains })
    }

    async fn matching_client(
        &self,
        task_list: &str,
    ) -> TaskResult<cadence_proto::matching::matching_api_client::MatchingApiClient<tonic::transport::Channel>> {
        let channel = (self.env.matching_resolver)(task_list).map_err(TaskError::InternalService)?;
        Ok(cadence_proto::matching::matching_api_client::MatchingApiClient::new(channel))
    }

    fn lb_request<'a>(request: &'a AddMatchingTaskRequest, partition_config: &'a std::collections::HashMap<String, String>) -> cadence_common::matching_lb::LbRequest<'a> {
        cadence_common::matching_lb::LbRequest {
            domain_id: &request.domain_uuid,
            task_list: &request.task_list.name,
            kind_normal: matches!(request.task_list.kind, TaskListKind::Normal),
            forwarded_from: "",
            partition_config: Some(partition_config),
        }
    }

    /// Go matching client `loadBalancer.PickWritePartition`.
    fn pick_write_partition(&self, request: &AddMatchingTaskRequest, tt: i32) -> String {
        let config = Self::partition_config(request);
        self.env.matching_lb.pick_write_partition(tt, &Self::lb_request(request, &config))
    }

    /// Go matching client `provider.UpdatePartitionConfig` on the original task list.
    fn update_partition_config(
        &self,
        request: &AddMatchingTaskRequest,
        tt: i32,
        config: Option<cadence_proto::task_list_partition::matching::TaskListPartitionConfig>,
    ) {
        let config = config.map(partition_config_from_proto);
        self.env.matching_lb.provider().update_partition_config(
            &request.domain_uuid,
            &request.task_list.name,
            matches!(request.task_list.kind, TaskListKind::Normal),
            tt,
            config,
        );
    }

    /// Go: on `ReadOnlyPartitionError` the partition is being drained; invalidate
    /// the cached partition config so the next request routes to the root.
    fn matching_add_err(&self, request: &AddMatchingTaskRequest, tt: i32, status: tonic::Status) -> TaskError {
        if crate::wiring::server::errors::is_read_only_partition_error(&status) {
            self.env.matching_lb.provider().invalidate(
                &request.domain_uuid,
                &request.task_list.name,
                matches!(request.task_list.kind, TaskListKind::Normal),
                tt,
            );
        }
        status_err(status)
    }

    fn partition_config(request: &AddMatchingTaskRequest) -> std::collections::HashMap<String, String> {
        request
            .partition_config
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned()))
            .collect()
    }
}

#[async_trait]
impl TransferDependencies for TransferDepsImpl {
    fn standby_task_missing_events_discard_delay(&self) -> Duration {
        (self.env.config.standby_task_missing_events_discard_delay)()
    }

    fn dlq_writer(&self) -> Arc<dyn TaskDLQWriter> {
        Arc::new(SqlTaskDlqWriter)
    }

    fn history_task_dlq_mode(&self) -> StringPropertyFnWithDomainFilter {
        let f = self.env.config.history_task_dlq_mode.clone();
        Arc::new(move |domain: &str| f(domain))
    }

    async fn add_activity_task(&self, request: AddMatchingTaskRequest) -> TaskResult<()> {
        let tt = cadence_common::matching_lb::TASK_LIST_TYPE_ACTIVITY;
        let partition = self.pick_write_partition(&request, tt);
        let mut client = self.matching_client(&partition).await?;
        let mut rpc = tonic::Request::new(cadence_proto::matching::AddActivityTaskRequest {
            domain_id: request.domain_uuid.clone(),
            source_domain_id: request.source_domain_uuid.clone().unwrap_or_default(),
            workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                workflow_id: request.execution.workflow_id.clone(),
                run_id: request.execution.run_id.clone(),
            }),
            task_list: Some(proto_task_list(&partition, request.task_list.kind)),
            schedule_id: request.schedule_id,
            schedule_to_start_timeout: Some(cadence_proto::convert::duration(i64::from(
                request.schedule_to_start_timeout_seconds,
            ))),
            // TASK_SOURCE_HISTORY
            source: 1,
            forwarded_from: String::new(),
            partition_config: Self::partition_config(&request),
            activity_task_dispatch_info: None,
        });
        matching_metadata(&mut rpc);
        let response = client.add_activity_task(rpc).await.map_err(|e| self.matching_add_err(&request, tt, e))?.into_inner();
        self.update_partition_config(&request, tt, response.partition_config);
        Ok(())
    }

    async fn add_decision_task(&self, request: AddMatchingTaskRequest) -> TaskResult<()> {
        let tt = cadence_common::matching_lb::TASK_LIST_TYPE_DECISION;
        let partition = self.pick_write_partition(&request, tt);
        crate::decision_trace!(
            "d0_matching_resolve_started",
            request.execution.workflow_id,
            request.execution.run_id,
            "schedule_id={}",
            request.schedule_id
        );
        let mut client = self.matching_client(&partition).await?;
        let mut rpc = tonic::Request::new(cadence_proto::matching::AddDecisionTaskRequest {
            domain_id: request.domain_uuid.clone(),
            workflow_execution: Some(cadence_proto::api::WorkflowExecution {
                workflow_id: request.execution.workflow_id.clone(),
                run_id: request.execution.run_id.clone(),
            }),
            task_list: Some(proto_task_list(&partition, request.task_list.kind)),
            schedule_id: request.schedule_id,
            schedule_to_start_timeout: Some(cadence_proto::convert::duration(i64::from(
                request.schedule_to_start_timeout_seconds,
            ))),
            // TASK_SOURCE_HISTORY
            source: 1,
            forwarded_from: String::new(),
            partition_config: Self::partition_config(&request),
        });
        matching_metadata(&mut rpc);
        crate::decision_trace!(
            "d_add_decision_sent",
            request.execution.workflow_id,
            request.execution.run_id,
            "schedule_id={}",
            request.schedule_id
        );
        let response = client.add_decision_task(rpc).await.map_err(|e| self.matching_add_err(&request, tt, e))?.into_inner();
        crate::decision_trace!(
            "d2_add_decision_returned",
            request.execution.workflow_id,
            request.execution.run_id,
            "schedule_id={}",
            request.schedule_id
        );
        self.update_partition_config(&request, tt, response.partition_config);
        Ok(())
    }

    fn visibility_domain(&self, domain_id: &str, workflow_id: &str) -> TaskResult<VisibilityDomain> {
        let entry = self.domains.get_domain_by_id(domain_id)?;
        let config = entry.get_config();
        Ok(VisibilityDomain {
            name: entry.info.name.clone(),
            retention_days: entry.get_retention_days(workflow_id),
            sampled_for_longer_retention_enabled: entry.sampled_for_longer_retention_enabled,
            sampled_for_longer_retention: entry.sampled_for_longer_retention,
            archive_visibility: config.visibility_archival_status == ArchivalStatus::Enabled,
            visibility_uri: config.visibility_archival_uri.clone(),
            history_uri: config.history_archival_uri.clone(),
        })
    }

    fn enable_context_header_in_visibility(&self, domain: &str) -> bool {
        (self.env.config.enable_context_header_in_visibility)(domain)
    }

    fn valid_search_attributes(&self) -> HashSet<String> {
        (self.env.config.valid_search_attributes)().into_keys().collect()
    }

    fn hidden_value_keys(&self) -> BTreeMap<String, serde_json::Value> {
        (self.env.config.search_attributes_hidden_value_keys)().into_iter().collect()
    }

    fn enable_record_workflow_execution_uninitialized(&self, domain: &str) -> bool {
        (self.env.config.enable_record_workflow_execution_uninitialized)(domain)
    }

    async fn record_workflow_execution_uninitialized(&self, record: &VisibilityRecord) -> TaskResult<()> {
        // Go sql visibility store: no-op; the ES store publishes a Create message.
        self.env.visibility.record_uninitialized_row(to_row(record, false)).await.map_err(pe)
    }

    async fn record_workflow_execution_started(&self, record: VisibilityRecord) -> TaskResult<()> {
        self.env.visibility.record_started_async(to_row(&record, false)).await.map_err(pe)
    }

    async fn upsert_workflow_execution(&self, record: VisibilityRecord) -> TaskResult<()> {
        // Go IsNopUpsertWorkflowRequest: a CadenceChangeVersion-only upsert is a no-op.
        if record.search_attributes.contains_key("CadenceChangeVersion") {
            return Ok(());
        }
        // Go sql visibility store: UpsertWorkflowExecution returns the
        // *types.BadRequestError ErrVisibilityOperationNotSupported.
        self.env.visibility.upsert_row(to_row(&record, false)).await.map_err(|e| match e {
            PersistenceError::Internal(m) if m == crate::wiring::persistence::visibility::VISIBILITY_OPERATION_NOT_SUPPORTED => {
                TaskError::BadRequest(m)
            }
            other => pe(other),
        })
    }

    async fn record_workflow_execution_closed(&self, record: VisibilityRecord) -> TaskResult<()> {
        self.env.visibility.record_closed_async(to_row(&record, true)).await.map_err(pe)
    }

    fn visibility_archival_time_limit(&self) -> Duration {
        (self.env.config.transfer_processor_visibility_archival_time_limit)()
    }

    async fn archive_visibility(&self, record: VisibilityRecord, domain: VisibilityDomain) -> TaskResult<()> {
        // Go transferTaskExecutorBase.archiveVisibility: inline attempt, then archival-workflow signal.
        let request = ClientRequest {
            archive_request: ArchiverRequest {
                domain_id: record.domain_id.clone(),
                domain_name: record.domain.clone(),
                workflow_id: record.workflow_id.clone(),
                run_id: record.run_id.clone(),
                workflow_type_name: record.workflow_type_name.clone(),
                start_timestamp: record.start_timestamp,
                execution_timestamp: record.execution_timestamp,
                close_timestamp: record.close_timestamp,
                close_status: thrift_close_status(record.close_status),
                history_length: record.history_length,
                memo: record.memo.clone(),
                search_attributes: record.search_attributes.clone(),
                visibility_uri: domain.visibility_uri.clone(),
                uri: domain.history_uri.clone(),
                targets: vec![ArchivalTarget::Visibility],
                ..Default::default()
            },
            caller_service: "history".into(),
            attempt_archive_inline: true,
        };
        let limit = (self.env.config.transfer_processor_visibility_archival_time_limit)();
        match tokio::time::timeout(limit, self.env.archiver_client.archive(request)).await {
            Ok(r) => r.map(|_| ()).map_err(TaskError::Other),
            Err(_) => Err(TaskError::Other("context deadline exceeded".into())),
        }
    }
}

/// `CleanupWorkflow` over a locked execution context and its loaded mutable state.
struct EngineCleanupWorkflow {
    state: Arc<EngineMutableState>,
    context: Arc<EngineExecutionContext>,
    release: ReleaseFunc,
}

#[async_trait]
impl CleanupWorkflow for EngineCleanupWorkflow {
    fn is_workflow_execution_running(&self) -> bool {
        self.state.is_workflow_execution_running()
    }
    fn get_last_write_version(&self) -> TaskResult<i64> {
        self.state.get_last_write_version().map_err(he)
    }
    fn get_current_branch_token(&self) -> TaskResult<Vec<u8>> {
        self.state.get_current_branch_token().map_err(he)
    }
    fn get_next_event_id(&self) -> i64 {
        self.state.get_next_event_id()
    }
    async fn load_execution_stats(&self) -> TaskResult<i64> {
        Ok(self.state.get_history_size())
    }
    fn clear(&self) {
        self.context.clear();
    }
    fn release(&self, error: Option<TaskError>) {
        match error {
            Some(e) => (self.release)(Some(&HistoryError::internal(e.to_string()))),
            None => (self.release)(None),
        }
    }
}

/// `CleanupDependencies` over the shard's execution cache and the SQL stores.
pub struct CleanupDepsImpl {
    env: Arc<HistoryEnv>,
    domains: Arc<dyn DomainCache>,
    cache: Arc<EngineExecutionCache>,
}

impl CleanupDepsImpl {
    pub fn new(env: Arc<HistoryEnv>, cache: Arc<EngineExecutionCache>) -> Arc<Self> {
        let domains = env.domain_cache.clone() as Arc<dyn DomainCache>;
        Arc::new(Self { env, domains, cache })
    }
}

fn persistence_category(category: HistoryTaskCategory) -> pc::HistoryTaskCategory {
    match category {
        HistoryTaskCategory::Transfer => pc::HistoryTaskCategory { category_type: 1, category_id: 1 },
        _ => pc::HistoryTaskCategory { category_type: 2, category_id: 2 },
    }
}

#[async_trait]
impl CleanupDependencies for CleanupDepsImpl {
    async fn get_workflow(&self, task: &dyn PersistenceTask) -> TaskResult<Option<Arc<dyn CleanupWorkflow>>> {
        let execution = WorkflowExecution {
            workflow_id: task.get_workflow_id().to_string(),
            run_id: task.get_run_id().to_string(),
        };
        let (context, release) = self.cache.get_or_create(task.get_domain_id(), &execution).await.map_err(he)?;
        match context.load_state().await {
            Ok(state) => Ok(Some(Arc::new(EngineCleanupWorkflow { state, context, release }))),
            Err(e) => {
                release(Some(&e));
                match he(e) {
                    TaskError::EntityNotExists(_) => Ok(None),
                    other => Err(other),
                }
            }
        }
    }

    fn history_archival_enabled(&self, domain_id: &str) -> TaskResult<bool> {
        let entry = self.domains.get_domain_by_id(domain_id)?;
        Ok(entry.get_config().history_archival_status == ArchivalStatus::Enabled)
    }

    fn history_archival_uri(&self, domain_id: &str) -> TaskResult<String> {
        Ok(self.domains.get_domain_by_id(domain_id)?.get_config().history_archival_uri.clone())
    }

    fn archival_size_limit(&self) -> i64 {
        (self.env.config.timer_processor_history_archival_size_limit)()
    }

    fn archival_time_limit(&self) -> Duration {
        (self.env.config.timer_processor_archival_time_limit)()
    }

    fn enable_workflow_timer_task_cleanup(&self) -> bool {
        (self.env.config.enable_workflow_timer_task_cleanup)()
    }

    async fn archive(&self, request: ArchiveRequest) -> TaskResult<bool> {
        // Go timerTaskExecutorBase.archiveWorkflow -> archiver.Client.Archive (history target).
        let targets = request
            .targets
            .iter()
            .map(|t| if *t == "visibility" { ArchivalTarget::Visibility } else { ArchivalTarget::History })
            .collect();
        let client_request = ClientRequest {
            archive_request: ArchiverRequest {
                domain_id: request.domain_id,
                domain_name: request.domain_name,
                workflow_id: request.workflow_id,
                run_id: request.run_id,
                shard_id: request.shard_id,
                branch_token: request.branch_token,
                next_event_id: request.next_event_id,
                close_failover_version: request.close_failover_version,
                uri: request.uri,
                targets,
                ..Default::default()
            },
            caller_service: request.caller_service.to_string(),
            attempt_archive_inline: request.attempt_archive_inline,
        };
        let limit = (self.env.config.timer_processor_archival_time_limit)();
        match tokio::time::timeout(limit, self.env.archiver_client.archive(client_request)).await {
            Ok(r) => r.map(|resp| resp.history_archived_inline).map_err(TaskError::Other),
            Err(_) => Err(TaskError::Other("context deadline exceeded".into())),
        }
    }

    async fn delete_workflow_execution(&self, task: &dyn PersistenceTask, _domain_name: &str, shard_id: i32) -> TaskResult<()> {
        self.env
            .persistence
            .executions
            .delete_workflow_execution(
                shard_id,
                task.get_domain_id().to_string(),
                task.get_workflow_id().to_string(),
                task.get_run_id().to_string(),
            )
            .await
            .map_err(pe)
    }

    async fn delete_current_workflow_execution(&self, task: &dyn PersistenceTask, _domain_name: &str, shard_id: i32) -> TaskResult<()> {
        self.env
            .persistence
            .executions
            .delete_current_workflow_execution(
                shard_id,
                task.get_domain_id().to_string(),
                task.get_workflow_id().to_string(),
                task.get_run_id().to_string(),
            )
            .await
            .map_err(pe)
    }

    async fn delete_active_cluster_selection_policy(&self, task: &dyn PersistenceTask, shard_id: i32) -> TaskResult<()> {
        self.env
            .persistence
            .executions
            .delete_active_cluster_selection_policy(
                shard_id,
                task.get_domain_id().to_string(),
                task.get_workflow_id().to_string(),
                task.get_run_id().to_string(),
            )
            .await
            .map_err(pe)
    }

    async fn delete_history_branch(&self, branch_token: Vec<u8>, _domain_name: &str, shard_id: i32) -> TaskResult<()> {
        let request = branch_request(shard_id, &branch_token).map_err(pe)?;
        self.env.persistence.history_v2.delete_history_branch(request).await.map_err(pe)
    }

    async fn delete_workflow_visibility(&self, task: &dyn PersistenceTask, _domain_name: &str) -> TaskResult<()> {
        self.env
            .visibility
            .delete_row(
                task.get_domain_id().to_string(),
                task.get_workflow_id().to_string(),
                task.get_run_id().to_string(),
                task.get_task_id(),
            )
            .await
            .map_err(pe)
    }

    async fn fetch_workflow_timer_tasks_for_cleanup(&self, _task: &dyn PersistenceTask, _shard_id: i32) -> TaskResult<Vec<HistoryTaskKey>> {
        // TODO(link): read WorkflowTimerTasks from the execution snapshot; only used when
        // `system.enableWorkflowTimerTaskCleanup` is on (default off).
        Ok(Vec::new())
    }

    async fn complete_history_task(&self, keys: Vec<HistoryTaskKey>, category: HistoryTaskCategory, shard_id: i32) -> TaskResult<()> {
        for key in keys {
            self.env
                .persistence
                .executions
                .complete_history_task(shard_id, persistence_category(category), key.task_id)
                .await
                .map_err(pe)?;
        }
        Ok(())
    }
}


#[cfg(test)]
mod matching_metadata_tests {
    use super::*;

    #[test]
    fn matching_metadata_sets_grpc_timeout_deadline() {
        let mut r = tonic::Request::new(());
        matching_metadata(&mut r);
        assert!(r.metadata().get("grpc-timeout").is_some(), "yarpc requires a TTL");
    }
}
