//! DescribeMutableState's Go persistence JSON view. No persistence or workflow mutation.
use super::engine_state::{decode, EngineMutableState, Inner};
use crate::engine::deps::{HResult, HistoryError};
use base64::Engine as _;
use cadence_persistence::history_contract as pc;
use cadence_thrift::FromThrift;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Default)]
struct GoBytes(Option<Vec<u8>>);
impl Serialize for GoBytes {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            None => s.serialize_none(),
            Some(bytes) => {
                s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
            }
        }
    }
}
fn bytes(value: &[u8]) -> GoBytes {
    GoBytes((!value.is_empty()).then(|| value.to_vec()))
}
fn time(nanos: i64) -> String {
    if nanos == 0 {
        return "0001-01-01T00:00:00Z".into();
    }
    let value = chrono::DateTime::from_timestamp(
        nanos.div_euclid(1_000_000_000),
        nanos.rem_euclid(1_000_000_000) as u32,
    )
    .expect("i64 nanoseconds are within chrono range")
    .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let value = value
        .trim_end_matches('Z')
        .trim_end_matches('0')
        .trim_end_matches('.');
    format!("{value}Z")
}
fn strings(value: &[String]) -> Option<Vec<String>> {
    (!value.is_empty()).then(|| value.to_vec())
}
fn byte_map(value: &BTreeMap<String, Vec<u8>>) -> Option<BTreeMap<String, GoBytes>> {
    (!value.is_empty()).then(|| {
        value
            .iter()
            .map(|(k, v)| (k.clone(), GoBytes(Some(v.clone()))))
            .collect()
    })
}
fn event(data: &[u8], encoding: &str) -> HResult<Option<ApiJson>> {
    if data.is_empty() {
        return Ok(None);
    }
    let st = decode(&pc::DataBlob {
        data: data.to_vec(),
        encoding: encoding.into(),
    })?;
    let event = cadence_api::service_types::HistoryEvent::from_thrift(&st)
        .map_err(|e| HistoryError::internal(e.to_string()))?;
    api(&event, "HistoryEvent").map(Some)
}
fn blob_api<T: FromThrift + Serialize>(
    blob: Option<&pc::DataBlob>,
    name: &'static str,
) -> HResult<Option<ApiJson>> {
    blob.filter(|b| !b.data.is_empty())
        .map(|b| {
            let value =
                T::from_thrift(&decode(b)?).map_err(|e| HistoryError::internal(e.to_string()))?;
            api(&value, name)
        })
        .transpose()
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct WorkflowMutableState {
    #[serde(serialize_with = "int_map")]
    ActivityInfos: Option<BTreeMap<i64, Option<ActivityInfo>>>,
    TimerInfos: Option<BTreeMap<String, Option<TimerInfo>>>,
    #[serde(serialize_with = "int_map")]
    ChildExecutionInfos: Option<BTreeMap<i64, Option<ChildExecutionInfo>>>,
    #[serde(serialize_with = "int_map")]
    RequestCancelInfos: Option<BTreeMap<i64, Option<RequestCancelInfo>>>,
    #[serde(serialize_with = "int_map")]
    SignalInfos: Option<BTreeMap<i64, Option<SignalInfo>>>,
    #[serde(serialize_with = "int_map")]
    SemaphoreInfos: Option<BTreeMap<i64, Option<SemaphoreInfo>>>,
    SignalRequestedIDs: Option<BTreeMap<String, BTreeMap<String, String>>>,
    ExecutionInfo: Option<WorkflowExecutionInfo>,
    ExecutionStats: Option<ExecutionStats>,
    BufferedEvents: Option<Vec<Option<ApiJson>>>,
    VersionHistories: Option<VersionHistories>,
    ReplicationState: Option<ReplicationState>,
    Checksum: Checksum,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct WorkflowExecutionInfo {
    DomainID: String,
    WorkflowID: String,
    RunID: String,
    FirstExecutionRunID: String,
    ParentDomainID: String,
    ParentWorkflowID: String,
    ParentRunID: String,
    InitiatedID: i64,
    CompletionEventBatchID: i64,
    CompletionEvent: Option<ApiJson>,
    TaskList: String,
    TaskListKind: i64,
    WorkflowTypeName: String,
    WorkflowTimeout: i64,
    DecisionStartToCloseTimeout: i64,
    ExecutionContext: GoBytes,
    State: i64,
    CloseStatus: i64,
    LastFirstEventID: i64,
    LastEventTaskID: i64,
    NextEventID: i64,
    LastProcessedEvent: i64,
    StartTimestamp: String,
    LastUpdatedTimestamp: String,
    CreateRequestID: String,
    SignalCount: i64,
    DecisionVersion: i64,
    DecisionScheduleID: i64,
    DecisionStartedID: i64,
    DecisionRequestID: String,
    DecisionTimeout: i64,
    DecisionAttempt: i64,
    DecisionStartedTimestamp: i64,
    DecisionScheduledTimestamp: i64,
    DecisionOriginalScheduledTimestamp: i64,
    CancelRequested: bool,
    CancelRequestID: String,
    StickyTaskList: String,
    StickyScheduleToStartTimeout: i64,
    ClientLibraryVersion: String,
    ClientFeatureVersion: String,
    ClientImpl: String,
    AutoResetPoints: Option<ApiJson>,
    Memo: Option<BTreeMap<String, GoBytes>>,
    SearchAttributes: Option<BTreeMap<String, GoBytes>>,
    PartitionConfig: Option<BTreeMap<String, String>>,
    ExecutionStatus: i64,
    ScheduledExecutionTimestamp: i64,
    Attempt: i64,
    HasRetryPolicy: bool,
    InitialInterval: i64,
    #[serde(serialize_with = "go_float")]
    BackoffCoefficient: f64,
    #[serde(serialize_with = "go_float")]
    JitterCoefficient: f64,
    MaximumInterval: i64,
    ExpirationTime: String,
    MaximumAttempts: i64,
    NonRetriableErrors: Option<Vec<String>>,
    BranchToken: GoBytes,
    IsCron: bool,
    CronOverlapPolicy: i64,
    ExpirationSeconds: i64,
    CronSchedule: String,
    ActiveClusterSelectionPolicy: Option<ApiJson>,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct ExecutionStats {
    HistorySize: i64,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct ActivityInfo {
    Version: i64,
    ScheduleID: i64,
    ScheduledEventBatchID: i64,
    ScheduledEvent: Option<ApiJson>,
    ScheduledTime: String,
    StartedID: i64,
    StartedEvent: Option<ApiJson>,
    StartedTime: String,
    DomainID: String,
    ActivityID: String,
    RequestID: String,
    Details: GoBytes,
    ScheduleToStartTimeout: i64,
    ScheduleToCloseTimeout: i64,
    StartToCloseTimeout: i64,
    HeartbeatTimeout: i64,
    CancelRequested: bool,
    CancelRequestID: i64,
    LastHeartBeatUpdatedTime: String,
    TimerTaskStatus: i64,
    Attempt: i64,
    StartedIdentity: String,
    TaskList: String,
    TaskListKind: i64,
    HasRetryPolicy: bool,
    InitialInterval: i64,
    #[serde(serialize_with = "go_float")]
    BackoffCoefficient: f64,
    #[serde(serialize_with = "go_float")]
    JitterCoefficient: f64,
    MaximumInterval: i64,
    ExpirationTime: String,
    MaximumAttempts: i64,
    NonRetriableErrors: Option<Vec<String>>,
    LastFailureReason: String,
    LastWorkerIdentity: String,
    LastFailureDetails: GoBytes,
    LastFailureCategory: i64,
    LastRetryIntervalSeconds: i64,
    LastHeartbeatTimeoutVisibilityInSeconds: i64,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct TimerInfo {
    Version: i64,
    TimerID: String,
    StartedID: i64,
    ExpiryTime: String,
    TaskStatus: i64,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct ChildExecutionInfo {
    Version: i64,
    InitiatedID: i64,
    InitiatedEventBatchID: i64,
    InitiatedEvent: Option<ApiJson>,
    StartedID: i64,
    StartedWorkflowID: String,
    StartedRunID: String,
    StartedEvent: Option<ApiJson>,
    CreateRequestID: String,
    DomainID: String,
    DomainNameDEPRECATED: String,
    WorkflowTypeName: String,
    ParentClosePolicy: i64,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct RequestCancelInfo {
    Version: i64,
    InitiatedEventBatchID: i64,
    InitiatedID: i64,
    CancelRequestID: String,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct SignalInfo {
    Version: i64,
    InitiatedEventBatchID: i64,
    InitiatedID: i64,
    SignalRequestID: String,
    SignalName: String,
    Input: GoBytes,
    Control: GoBytes,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct SemaphoreInfo {
    Version: i64,
    InitiatedID: i64,
    SemaphoreName: String,
    OwnerID: String,
    TokenID: i64,
    AcquireDeadline: String,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct VersionHistories {
    CurrentVersionHistoryIndex: i64,
    Histories: Option<Vec<Option<VersionHistory>>>,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct VersionHistory {
    BranchToken: GoBytes,
    Items: Option<Vec<Option<VersionHistoryItem>>>,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct VersionHistoryItem {
    EventID: i64,
    Version: i64,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct ReplicationState {
    CurrentVersion: i64,
    StartVersion: i64,
    LastWriteVersion: i64,
    LastWriteEventID: i64,
    LastReplicationInfo: Option<BTreeMap<String, Option<ReplicationInfo>>>,
}

#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct ReplicationInfo {
    Version: i64,
    LastEventID: i64,
}

#[derive(Default, Serialize, serde::Deserialize)]
#[allow(non_snake_case)]
struct Checksum {
    Version: i64,
    Flavor: i64,
    Value: Option<String>,
}

fn workflow_execution_info(g: &Inner) -> HResult<WorkflowExecutionInfo> {
    let p = &g.pc_exec;
    Ok(WorkflowExecutionInfo {
        DomainID: p.domain_id.clone(),
        WorkflowID: p.workflow_id.clone(),
        RunID: p.run_id.clone(),
        FirstExecutionRunID: p.first_execution_run_id.clone(),
        ParentDomainID: p.parent_domain_id.clone(),
        ParentWorkflowID: p.parent_workflow_id.clone(),
        ParentRunID: p.parent_run_id.clone(),
        InitiatedID: p.initiated_id,
        CompletionEventBatchID: p.completion_event_batch_id,
        CompletionEvent: blob_api::<cadence_api::service_types::HistoryEvent>(
            p.completion_event.as_ref(),
            "HistoryEvent",
        )?,
        TaskList: p.task_list.clone(),
        TaskListKind: p.task_list_kind as i64,
        WorkflowTypeName: p.workflow_type_name.clone(),
        WorkflowTimeout: p.workflow_timeout_nanos / 1_000_000_000,
        DecisionStartToCloseTimeout: p.decision_start_to_close_timeout_nanos / 1_000_000_000,
        ExecutionContext: bytes(&p.execution_context),
        State: g.exec.state as i64,
        CloseStatus: g.exec.close_status as i64,
        LastFirstEventID: g.last_first_event_id,
        LastEventTaskID: p.last_event_task_id,
        NextEventID: g.next_event_id,
        LastProcessedEvent: g.last_processed_event,
        StartTimestamp: time(p.start_time_unix_nanos),
        LastUpdatedTimestamp: time(g.last_updated_nanos),
        CreateRequestID: p.create_request_id.clone(),
        SignalCount: g.exec.signal_count as i64,
        DecisionVersion: p.decision_version,
        DecisionScheduleID: p.decision_schedule_id,
        DecisionStartedID: p.decision_started_id,
        DecisionRequestID: p.decision_request_id.clone(),
        DecisionTimeout: p.decision_timeout_nanos / 1_000_000_000,
        DecisionAttempt: p.decision_attempt,
        DecisionStartedTimestamp: p.decision_started_time_unix_nanos,
        DecisionScheduledTimestamp: p.decision_scheduled_time_unix_nanos,
        DecisionOriginalScheduledTimestamp: p.decision_original_scheduled_time_unix_nanos,
        CancelRequested: p.cancel_requested,
        CancelRequestID: p.cancel_request_id.clone(),
        StickyTaskList: p.sticky_task_list.clone(),
        StickyScheduleToStartTimeout: p.sticky_schedule_to_start_timeout_nanos / 1_000_000_000,
        ClientLibraryVersion: p.client_library_version.clone(),
        ClientFeatureVersion: p.client_feature_version.clone(),
        ClientImpl: p.client_impl.clone(),
        AutoResetPoints: blob_api::<cadence_api::types::ResetPoints>(
            p.auto_reset_points.as_ref(),
            "ResetPoints",
        )?,
        Memo: byte_map(&p.memo),
        SearchAttributes: byte_map(&p.search_attributes),
        PartitionConfig: (!p.partition_config.is_empty()).then(|| p.partition_config.clone()),
        ExecutionStatus: 0,
        ScheduledExecutionTimestamp: 0,
        Attempt: p.attempt as i64,
        HasRetryPolicy: p.has_retry_policy,
        InitialInterval: p.initial_interval_nanos / 1_000_000_000,
        BackoffCoefficient: p.backoff_coefficient,
        JitterCoefficient: p.jitter_coefficient,
        MaximumInterval: p.maximum_interval_nanos / 1_000_000_000,
        ExpirationTime: time(p.expiration_time_unix_nanos),
        MaximumAttempts: p.maximum_attempts as i64,
        NonRetriableErrors: strings(&p.non_retriable_errors),
        BranchToken: bytes(&g.branch_token),
        IsCron: p.is_cron,
        CronOverlapPolicy: p.cron_overlap_policy as i64,
        ExpirationSeconds: p.expiration_interval_nanos / 1_000_000_000,
        CronSchedule: p.cron_schedule.clone(),
        ActiveClusterSelectionPolicy: blob_api::<cadence_api::types::ActiveClusterSelectionPolicy>(
            p.active_cluster_selection_policy.as_ref(),
            "ActiveClusterSelectionPolicy",
        )?,
    })
}

fn activity_info(id: i64, p: &pc::ActivityInfo, g: &Inner) -> HResult<ActivityInfo> {
    Ok(ActivityInfo {
        Version: p.version,
        ScheduleID: id,
        ScheduledEventBatchID: p.scheduled_event_batch_id,
        ScheduledEvent: event(&p.scheduled_event, &p.scheduled_event_encoding)?,
        ScheduledTime: time(p.scheduled_time_unix_nanos),
        StartedID: p.started_id,
        StartedEvent: event(&p.started_event, &p.started_event_encoding)?,
        StartedTime: time(p.started_time_unix_nanos),
        DomainID: g.exec.domain_id.clone(),
        ActivityID: p.activity_id.clone(),
        RequestID: p.request_id.clone(),
        Details: g
            .heartbeats
            .get(&id)
            .map(|h| bytes(&h.details))
            .unwrap_or_default(),
        ScheduleToStartTimeout: p.schedule_to_start_timeout_seconds as i64,
        ScheduleToCloseTimeout: p.schedule_to_close_timeout_seconds as i64,
        StartToCloseTimeout: p.start_to_close_timeout_seconds as i64,
        HeartbeatTimeout: p.heartbeat_timeout_seconds as i64,
        CancelRequested: p.cancel_requested,
        CancelRequestID: p.cancel_request_id,
        LastHeartBeatUpdatedTime: time(
            g.heartbeats
                .get(&id)
                .map_or(0, |h| h.updated_time_unix_nanos),
        ),
        TimerTaskStatus: p.timer_task_status as i64,
        Attempt: p.attempt as i64,
        StartedIdentity: p.started_identity.clone(),
        TaskList: p.task_list.clone(),
        TaskListKind: p.task_list_kind as i64,
        HasRetryPolicy: p.has_retry_policy,
        InitialInterval: p.retry_initial_interval_seconds as i64,
        BackoffCoefficient: p.retry_backoff_coefficient,
        JitterCoefficient: p.retry_jitter_coefficient,
        MaximumInterval: p.retry_maximum_interval_seconds as i64,
        ExpirationTime: time(p.retry_expiration_time_unix_nanos),
        MaximumAttempts: p.retry_maximum_attempts as i64,
        NonRetriableErrors: strings(&p.retry_non_retriable_errors),
        LastFailureReason: p.retry_last_failure_reason.clone(),
        LastWorkerIdentity: p.retry_last_worker_identity.clone(),
        LastFailureDetails: bytes(&p.retry_last_failure_details),
        LastFailureCategory: p.retry_last_failure_category as i64,
        LastRetryIntervalSeconds: p.retry_last_retry_interval_seconds as i64,
        LastHeartbeatTimeoutVisibilityInSeconds: p.last_heartbeat_timeout_visibility_in_seconds,
    })
}

fn child_execution_info(id: i64, p: &pc::ChildExecutionInfo) -> HResult<ChildExecutionInfo> {
    Ok(ChildExecutionInfo {
        Version: p.version,
        InitiatedID: id,
        InitiatedEventBatchID: p.initiated_event_batch_id,
        InitiatedEvent: event(&p.initiated_event, &p.initiated_event_encoding)?,
        StartedID: p.started_id,
        StartedWorkflowID: p.started_workflow_id.clone(),
        StartedRunID: uuid_string(&p.started_run_id),
        StartedEvent: event(&p.started_event, &p.started_event_encoding)?,
        CreateRequestID: p.create_request_id.clone(),
        DomainID: p.domain_id.clone(),
        DomainNameDEPRECATED: p.domain_name.clone(),
        WorkflowTypeName: p.workflow_type_name.clone(),
        ParentClosePolicy: p.parent_close_policy as i64,
    })
}

fn timer_info(id: &str, p: &pc::TimerInfo) -> HResult<TimerInfo> {
    Ok(TimerInfo {
        Version: p.version,
        TimerID: id.to_owned(),
        StartedID: p.started_id,
        ExpiryTime: time(p.expiry_time_unix_nanos),
        TaskStatus: p.task_id,
    })
}

fn request_cancel_info(id: i64, p: &pc::RequestCancelInfo) -> HResult<RequestCancelInfo> {
    Ok(RequestCancelInfo {
        Version: p.version,
        InitiatedEventBatchID: p.initiated_event_batch_id,
        InitiatedID: id,
        CancelRequestID: p.cancel_request_id.clone(),
    })
}

fn signal_info(id: i64, p: &pc::SignalInfo) -> HResult<SignalInfo> {
    Ok(SignalInfo {
        Version: p.version,
        InitiatedEventBatchID: p.initiated_event_batch_id,
        InitiatedID: id,
        SignalRequestID: p.request_id.clone(),
        SignalName: p.name.clone(),
        Input: bytes(&p.input),
        Control: bytes(&p.control),
    })
}

fn uuid_string(value: &[u8]) -> String {
    uuid::Uuid::from_slice(value)
        .map(|u| u.to_string())
        .unwrap_or_else(|_| String::from_utf8_lossy(value).into_owned())
}
fn version_histories(value: &cadence_api::types::VersionHistories) -> VersionHistories {
    VersionHistories {
        CurrentVersionHistoryIndex: value.current_version_history_index as i64,
        Histories: Some(
            value
                .histories
                .iter()
                .map(|h| {
                    Some(VersionHistory {
                        BranchToken: GoBytes(Some(h.branch_token.clone())),
                        Items: Some(
                            h.items
                                .iter()
                                .map(|i| {
                                    Some(VersionHistoryItem {
                                        EventID: i.event_id,
                                        Version: i.version,
                                    })
                                })
                                .collect(),
                        ),
                    })
                })
                .collect(),
        ),
    }
}
impl EngineMutableState {
    pub(super) fn persistence_json(&self) -> HResult<String> {
        let g = self.inner.lock().unwrap();
        let state = WorkflowMutableState {
            ActivityInfos: Some(
                g.pc_acts
                    .iter()
                    .map(|(id, p)| Ok((*id, Some(activity_info(*id, p, &g)?))))
                    .collect::<HResult<_>>()?,
            ),
            TimerInfos: Some(
                g.timers
                    .iter()
                    .map(|(id, p)| Ok((id.clone(), Some(timer_info(id, p)?))))
                    .collect::<HResult<_>>()?,
            ),
            ChildExecutionInfos: Some(
                g.pc_children
                    .iter()
                    .map(|(id, p)| Ok((*id, Some(child_execution_info(*id, p)?))))
                    .collect::<HResult<_>>()?,
            ),
            RequestCancelInfos: Some(
                g.pc_cancels
                    .iter()
                    .map(|(id, p)| Ok((*id, Some(request_cancel_info(*id, p)?))))
                    .collect::<HResult<_>>()?,
            ),
            SignalInfos: Some(
                g.pc_signals
                    .iter()
                    .map(|(id, p)| Ok((*id, Some(signal_info(*id, p)?))))
                    .collect::<HResult<_>>()?,
            ),
            SemaphoreInfos: Some(
                g.semaphores
                    .pending
                    .iter()
                    .map(|(id, p)| {
                        (
                            *id,
                            Some(SemaphoreInfo {
                                Version: p.version,
                                InitiatedID: p.initiated_id,
                                SemaphoreName: p.semaphore_name.clone(),
                                OwnerID: p.owner_id.clone(),
                                TokenID: p.token_id as i64,
                                AcquireDeadline: time(p.acquire_deadline_unix_nanos),
                            }),
                        )
                    })
                    .collect(),
            ),
            SignalRequestedIDs: Some(
                g.signal_requested
                    .iter()
                    .map(|id| (id.clone(), BTreeMap::new()))
                    .collect(),
            ),
            ExecutionInfo: Some(workflow_execution_info(&g)?),
            ExecutionStats: Some(ExecutionStats {
                HistorySize: g.history_size,
            }),
            BufferedEvents: Some(
                g.buffered
                    .iter()
                    .map(|e| api(e, "HistoryEvent").map(Some))
                    .collect::<HResult<_>>()?,
            ),
            VersionHistories: g.version_histories.as_ref().map(version_histories),
            ReplicationState: None,
            Checksum: self
                .snapshot
                .checksum
                .as_ref()
                .map(|b| serde_json::from_slice(&b.data))
                .transpose()
                .map_err(|e| HistoryError::internal(e.to_string()))?
                .unwrap_or_default(),
        };
        serde_json::to_string(&state)
            .map(|s| {
                s.replace('&', "\\u0026")
                    .replace('<', "\\u003c")
                    .replace('>', "\\u003e")
                    .replace('\u{2028}', "\\u2028")
                    .replace('\u{2029}', "\\u2029")
            })
            .map_err(|e| HistoryError::internal(e.to_string()))
    }
}

/// The API objects nested inside persistence structs have Go json tags, unlike
/// the persistence structs themselves. Keep their tag order, omitempty rules and
/// binary fields here rather than changing the service/wire serializers.
#[derive(Default)]
struct ApiJson {
    value: serde_json::Value,
    kind: &'static str,
}
fn api<T: Serialize>(value: &T, kind: &'static str) -> HResult<ApiJson> {
    Ok(ApiJson {
        value: serde_json::to_value(value).map_err(|e| HistoryError::internal(e.to_string()))?,
        kind,
    })
}
type ApiSchema = BTreeMap<String, Vec<(String, String, bool)>>;
fn api_schema() -> &'static ApiSchema {
    static SCHEMA: std::sync::OnceLock<ApiSchema> =
        std::sync::OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(include_str!("mutable_state_api_schema.json"))
            .expect("Go API JSON field schema")
    })
}
impl Serialize for ApiJson {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ApiField {
            value: &self.value,
            kind: self.kind,
        }
        .serialize(serializer)
    }
}
struct ApiField<'a> {
    value: &'a serde_json::Value,
    kind: &'a str,
}
impl Serialize for ApiField<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        let kind = self.kind.strip_prefix('*').unwrap_or(self.kind);
        if self.value.is_null() {
            return serializer.serialize_none();
        }
        if kind == "[]byte" {
            if let Some(array) = self.value.as_array() {
                let bytes = array
                    .iter()
                    .map(|v| v.as_u64().unwrap_or_default() as u8)
                    .collect::<Vec<_>>();
                return GoBytes(Some(bytes)).serialize(serializer);
            }
            return self.value.serialize(serializer);
        }
        if let Some(child) = kind.strip_prefix("[]") {
            let Some(array) = self.value.as_array() else {
                return self.value.serialize(serializer);
            };
            let mut seq = serializer.serialize_seq(Some(array.len()))?;
            for value in array {
                seq.serialize_element(&ApiField { value, kind: child })?;
            }
            return seq.end();
        }
        if let Some((_, child)) = kind.strip_prefix("map[").and_then(|v| v.split_once(']')) {
            let Some(obj) = self.value.as_object() else {
                return self.value.serialize(serializer);
            };
            let mut map = serializer.serialize_map(Some(obj.len()))?;
            for (key, value) in obj {
                map.serialize_entry(key, &ApiField { value, kind: child })?;
            }
            return map.end();
        }
        if let Some(fields) = api_schema().get(kind) {
            let mut map = serializer.serialize_map(None)?;
            for (key, child, omit) in fields {
                let value = self.value.get(key).unwrap_or(&serde_json::Value::Null);
                let empty = value.is_null()
                    || value == false
                    || value == 0
                    || value == ""
                    || value.as_array().is_some_and(Vec::is_empty)
                    || value.as_object().is_some_and(serde_json::Map::is_empty);
                // Pointers to zero values are present in Go encoding/json.
                if *omit && (value.is_null() || (!child.starts_with('*') && empty)) {
                    continue;
                }
                map.serialize_entry(key, &ApiField { value, kind: child })?;
            }
            return map.end();
        }
        if self.value.is_f64() {
            return go_float(&self.value.as_f64().expect("JSON float"), serializer);
        }
        self.value.serialize(serializer)
    }
}

fn go_float<S: serde::Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        serializer.serialize_i64(*value as i64)
    } else {
        serializer.serialize_f64(*value)
    }
}

// encoding/json orders integer map keys by their decimal string, not numerically.
fn int_map<S: serde::Serializer, T: Serialize>(
    value: &Option<BTreeMap<i64, T>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        None => serializer.serialize_none(),
        Some(map) => map
            .iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn full_mutable_state_matches_hand_built_go_json() {
        let snapshot = pc::WorkflowSnapshot {
            execution_info: pc::WorkflowExecutionInfo {
                domain_id: "domain".into(),
                workflow_id: "workflow".into(),
                run_id: "run".into(),
                workflow_timeout_nanos: 12_000_000_000,
                decision_timeout_nanos: 3_000_000_000,
                start_time_unix_nanos: 1_234_567_890,
                execution_context: vec![1, 2, 255],
                memo: BTreeMap::from([("empty".into(), vec![])]),
                ..Default::default()
            },
            ..Default::default()
        };
        let state =
            EngineMutableState::from_snapshot(snapshot, Default::default(), 0, Arc::new(|| 0))
                .unwrap();
        let expected = r#"{"ActivityInfos":{},"TimerInfos":{},"ChildExecutionInfos":{},"RequestCancelInfos":{},"SignalInfos":{},"SemaphoreInfos":{},"SignalRequestedIDs":{},"ExecutionInfo":{"DomainID":"domain","WorkflowID":"workflow","RunID":"run","FirstExecutionRunID":"","ParentDomainID":"","ParentWorkflowID":"","ParentRunID":"","InitiatedID":0,"CompletionEventBatchID":0,"CompletionEvent":null,"TaskList":"","TaskListKind":0,"WorkflowTypeName":"","WorkflowTimeout":12,"DecisionStartToCloseTimeout":0,"ExecutionContext":"AQL/","State":0,"CloseStatus":0,"LastFirstEventID":0,"LastEventTaskID":0,"NextEventID":0,"LastProcessedEvent":0,"StartTimestamp":"1970-01-01T00:00:01.23456789Z","LastUpdatedTimestamp":"0001-01-01T00:00:00Z","CreateRequestID":"","SignalCount":0,"DecisionVersion":0,"DecisionScheduleID":0,"DecisionStartedID":0,"DecisionRequestID":"","DecisionTimeout":3,"DecisionAttempt":0,"DecisionStartedTimestamp":0,"DecisionScheduledTimestamp":0,"DecisionOriginalScheduledTimestamp":0,"CancelRequested":false,"CancelRequestID":"","StickyTaskList":"","StickyScheduleToStartTimeout":0,"ClientLibraryVersion":"","ClientFeatureVersion":"","ClientImpl":"","AutoResetPoints":null,"Memo":{"empty":""},"SearchAttributes":null,"PartitionConfig":null,"ExecutionStatus":0,"ScheduledExecutionTimestamp":0,"Attempt":0,"HasRetryPolicy":false,"InitialInterval":0,"BackoffCoefficient":0,"JitterCoefficient":0,"MaximumInterval":0,"ExpirationTime":"0001-01-01T00:00:00Z","MaximumAttempts":0,"NonRetriableErrors":null,"BranchToken":null,"IsCron":false,"CronOverlapPolicy":0,"ExpirationSeconds":0,"CronSchedule":"","ActiveClusterSelectionPolicy":null},"ExecutionStats":{"HistorySize":0},"BufferedEvents":[],"VersionHistories":null,"ReplicationState":null,"Checksum":{"Version":0,"Flavor":0,"Value":null}}"#;
        assert_eq!(state.persistence_json().unwrap(), expected);
    }

    #[test]
    fn nil_and_empty_collections_and_binary_are_distinct() {
        let mut state = WorkflowMutableState::default();
        let nil = serde_json::to_value(&state).unwrap();
        assert!(nil["ActivityInfos"].is_null());
        assert!(nil["BufferedEvents"].is_null());
        state.ActivityInfos = Some(BTreeMap::new());
        state.BufferedEvents = Some(Vec::new());
        let empty = serde_json::to_value(&state).unwrap();
        assert_eq!(empty["ActivityInfos"], serde_json::json!({}));
        assert_eq!(empty["BufferedEvents"], serde_json::json!([]));
        assert_eq!(serde_json::to_string(&GoBytes(None)).unwrap(), "null");
        assert_eq!(
            serde_json::to_string(&GoBytes(Some(Vec::new()))).unwrap(),
            "\"\""
        );
        assert_eq!(time(1_120_000_000), "1970-01-01T00:00:01.12Z");
    }

    #[test]
    fn pending_records_and_version_histories_match_go_field_order() {
        let signal = signal_info(
            9,
            &pc::SignalInfo {
                version: 20,
                initiated_event_batch_id: 7,
                request_id: "request".into(),
                name: "signal".into(),
                input: vec![1, 2, 255],
                control: vec![],
            },
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&signal).unwrap(),
            r#"{"Version":20,"InitiatedEventBatchID":7,"InitiatedID":9,"SignalRequestID":"request","SignalName":"signal","Input":"AQL/","Control":null}"#
        );
        let histories = version_histories(&cadence_api::types::VersionHistories {
            current_version_history_index: 0,
            histories: vec![cadence_api::types::VersionHistory {
                branch_token: vec![1, 2, 255],
                items: vec![cadence_api::types::VersionHistoryItem {
                    event_id: 7,
                    version: 20,
                }],
            }],
        });
        assert_eq!(
            serde_json::to_string(&histories).unwrap(),
            r#"{"CurrentVersionHistoryIndex":0,"Histories":[{"BranchToken":"AQL/","Items":[{"EventID":7,"Version":20}]}]}"#
        );
    }

    #[test]
    fn nested_api_tags_bytes_and_zero_pointer_values_follow_go() {
        let input = ApiJson {
            kind: "HistoryEvent",
            value: serde_json::json!({
                "eventId": 7, "timestamp": 0, "version": 0, "taskId": 0,
                "workflowExecutionSignaledEventAttributes": { "signalName": "signal", "input": [1,2,255], "identity": "" }
            }),
        };
        assert_eq!(
            serde_json::to_string(&input).unwrap(),
            r#"{"eventId":7,"timestamp":0,"workflowExecutionSignaledEventAttributes":{"signalName":"signal","input":"AQL/"}}"#
        );
    }
}

