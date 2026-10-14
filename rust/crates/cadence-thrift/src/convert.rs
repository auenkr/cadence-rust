//! Thrift converters for Cadence API structs.
//!
//! Field IDs come from the IDL (`go/.gen/.../rawIDL`). Each converter maps
//! between the Rust struct and the generic value model; see `value.rs`.

use cadence_api::enums::*;
use cadence_api::errors::*;
use cadence_api::service_types::*;
use cadence_api::tasks::*;
use cadence_api::types::*;

use crate::value::{Fields, FromI32, FromThrift, FromValue, Struct, ToThrift, ToValue, Value};

// The execution store uses milliseconds for timer scheduling; Cadence's
// Thrift IDL represents timestamp fields as Unix nanoseconds.
fn millis_to_nanos(value: Option<i64>) -> Option<i64> {
    value.map(|millis| millis.saturating_mul(1_000_000))
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

macro_rules! wrap_enum {
    ($($t:ty),*) => {
        $(impl FromI32 for $t {
            fn from_i32(v: i32) -> Option<Self> {
                <$t>::from_i32(v)
            }
            fn value(self) -> i32 {
                <$t>::value(self)
            }
        }
        impl FromValue for $t {
            fn from_value(v: &Value) -> Result<Self, crate::protocol::Error> {
                match v {
                    Value::I32(x) => <Self as FromI32>::from_i32(*x).ok_or_else(|| {
                        crate::protocol::Error::Invalid(format!("unknown enum value {x}"))
                    }),
                    _ => Err(crate::protocol::Error::Invalid(format!(
                        "expected enum, got {v:?}"
                    ))),
                }
            }
        }
        impl ToValue for $t {
            fn to_value(&self) -> Value {
                Value::I32(FromI32::value(*self))
            }
        })*
    };
}

wrap_enum!(
    ArchivalStatus,
    CancelExternalWorkflowExecutionFailedCause,
    ChildWorkflowExecutionFailedCause,
    ContinueAsNewInitiator,
    CronOverlapPolicy,
    CrossClusterTaskFailedCause,
    CrossClusterTaskType,
    DecisionTaskFailedCause,
    DecisionTaskTimedOutCause,
    DecisionType,
    DomainStatus,
    EncodingType,
    EventType,
    FailoverType,
    FailureCategory,
    GetTaskFailedCause,
    HistoryEventFilterType,
    IndexedValueType,
    ParentClosePolicy,
    PendingActivityState,
    PendingDecisionState,
    QueryConsistencyLevel,
    QueryRejectCondition,
    QueryResultType,
    QueryTaskCompletedType,
    SignalExternalWorkflowExecutionFailedCause,
    TaskListKind,
    TaskListType,
    TimeoutType,
    WorkflowExecutionCloseStatus,
    WorkflowExecutionStatus,
    WorkflowIDReusePolicy
);

// ---------------------------------------------------------------------------
// Identity / payload structs
// ---------------------------------------------------------------------------

impl FromThrift for WorkflowExecution {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            workflow_id: s.req_string(10, "")?,
            run_id: s.req_string(20, "")?,
        })
    }
}

impl ToThrift for WorkflowExecution {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.workflow_id);
        f.sstr(20, &self.run_id);
        f.finish()
    }
}

impl FromThrift for WorkflowType {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            name: s.req_string(10, "")?,
        })
    }
}

impl ToThrift for WorkflowType {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.name);
        f.finish()
    }
}

impl FromThrift for ActivityType {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            name: s.req_string(10, "")?,
        })
    }
}

impl ToThrift for ActivityType {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.name);
        f.finish()
    }
}

impl FromThrift for TaskList {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            name: s.req_string(10, "")?,
            kind: s.opt_enum(20)?,
            base_name: s.req_string(30, "")?,
        })
    }
}

impl ToThrift for TaskList {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.name);
        f.oenum(20, self.kind);
        f.sstr(30, &self.base_name);
        f.finish()
    }
}

impl FromThrift for TaskListMetadata {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            max_tasks_per_second: s.opt_f64(10)?,
        })
    }
}

impl ToThrift for TaskListMetadata {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.of64(10, self.max_tasks_per_second);
        f.finish()
    }
}

impl FromThrift for DataBlob {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            encoding_type: s.opt_enum(10)?,
            data: s.bytes_or_empty(20)?,
        })
    }
}

impl ToThrift for DataBlob {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oenum(10, self.encoding_type);
        f.sbytes(20, &self.data);
        f.finish()
    }
}

impl FromThrift for Header {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            fields: s.opt_map_str(10)?,
        })
    }
}

impl ToThrift for Header {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.omap_str_bytes(10, &self.fields);
        f.finish()
    }
}

impl FromThrift for Memo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            fields: s.opt_map_str(10)?,
        })
    }
}

impl ToThrift for Memo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.omap_str_bytes(10, &self.fields);
        f.finish()
    }
}

impl FromThrift for SearchAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            indexed_fields: s.opt_map_str(10)?,
        })
    }
}

impl ToThrift for SearchAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.omap_str_bytes(10, &self.indexed_fields);
        f.finish()
    }
}

impl FromThrift for RetryPolicy {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            initial_interval_in_seconds: s.opt_i32(10)?.unwrap_or(0),
            backoff_coefficient: s.opt_f64(20)?.unwrap_or(0.0),
            maximum_interval_in_seconds: s.opt_i32(30)?.unwrap_or(0),
            maximum_attempts: s.opt_i32(40)?.unwrap_or(0),
            non_retriable_error_reasons: s
                .opt_list::<String>(50)?
                .unwrap_or_default(),
            expiration_interval_in_seconds: s.opt_i32(60)?.unwrap_or(0),
            jitter_coefficient: s.opt_f64(70)?.unwrap_or(0.0),
        })
    }
}

impl ToThrift for RetryPolicy {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.initial_interval_in_seconds != 0 {
            f.fields.push((10, Value::I32(self.initial_interval_in_seconds)));
        }
        if self.backoff_coefficient != 0.0 {
            f.fields.push((20, Value::F64(self.backoff_coefficient)));
        }
        if self.maximum_interval_in_seconds != 0 {
            f.fields.push((30, Value::I32(self.maximum_interval_in_seconds)));
        }
        if self.maximum_attempts != 0 {
            f.fields.push((40, Value::I32(self.maximum_attempts)));
        }
        if !self.non_retriable_error_reasons.is_empty() {
            f.fields.push((
                50,
                Value::List(
                    self.non_retriable_error_reasons
                        .iter()
                        .map(|s| Value::Bytes(s.as_bytes().to_vec()))
                        .collect(),
                ),
            ));
        }
        if self.expiration_interval_in_seconds != 0 {
            f.fields.push((60, Value::I32(self.expiration_interval_in_seconds)));
        }
        if self.jitter_coefficient != 0.0 {
            f.fields.push((70, Value::F64(self.jitter_coefficient)));
        }
        f.finish()
    }
}

impl FromThrift for FailureOptions {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            failure_category: s.opt_enum(10)?,
            next_retry_interval_seconds: s.opt_i32(20)?,
        })
    }
}

impl ToThrift for FailureOptions {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oenum(10, self.failure_category);
        f.oi32(20, self.next_retry_interval_seconds);
        f.finish()
    }
}

// ---------------------------------------------------------------------------
// Domain structs
// ---------------------------------------------------------------------------

impl FromThrift for BadBinaryInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            reason: s.req_string(10, "")?,
            operator: s.req_string(20, "")?,
            created_time_nano: s.opt_i64(30)?,
        })
    }
}

impl ToThrift for BadBinaryInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.reason);
        f.sstr(20, &self.operator);
        f.oi64(30, self.created_time_nano);
        f.finish()
    }
}

impl FromThrift for BadBinaries {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        let binaries: Option<std::collections::HashMap<String, BadBinaryInfo>> =
            s.opt_map_str(10)?;
        Ok(Self {
            binaries: binaries.map(|m| m.into_iter().map(|(k, v)| (k, Some(v))).collect()),
        })
    }
}

impl ToThrift for BadBinaries {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        match &self.binaries {
            None => {}
            Some(m) => {
                let mut items: Vec<(Value, Value)> = Vec::with_capacity(m.len());
                for (k, v) in m {
                    if let Some(info) = v {
                        items.push((
                            Value::Bytes(k.as_bytes().to_vec()),
                            Value::Struct(info.to_thrift()),
                        ));
                    }
                }
                items.sort_by(|a, b| match (a, b) {
                    ((Value::Bytes(x), _), (Value::Bytes(y), _)) => x.cmp(y),
                    _ => std::cmp::Ordering::Equal,
                });
                if !items.is_empty() {
                    f.fields.push((10, Value::Map(items)));
                }
            }
        }
        f.finish()
    }
}

impl FromThrift for DomainInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            name: s.req_string(10, "")?,
            status: s.opt_enum(20)?,
            description: s.req_string(30, "")?,
            owner_email: s.req_string(40, "")?,
            data: s.opt_map_str(50)?,
            uuid: s.req_string(60, "")?,
        })
    }
}

impl ToThrift for DomainInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.name);
        f.oenum(20, self.status);
        f.sstr(30, &self.description);
        f.sstr(40, &self.owner_email);
        f.omap_str_str(50, &self.data);
        f.sstr(60, &self.uuid);
        f.finish()
    }
}

impl FromThrift for DomainConfiguration {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            workflow_execution_retention_period_in_days: s.opt_i32(10)?.unwrap_or(0),
            emit_metric: s.opt_bool(20)?.unwrap_or(true),
            bad_binaries: s.opt_struct(70)?,
            history_archival_status: s.opt_enum(80)?,
            history_archival_uri: s.req_string(90, "")?,
            visibility_archival_status: s.opt_enum(100)?,
            visibility_archival_uri: s.req_string(110, "")?,
        })
    }
}

impl ToThrift for DomainConfiguration {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.workflow_execution_retention_period_in_days != 0 {
            f.fields.push((
                10,
                Value::I32(self.workflow_execution_retention_period_in_days),
            ));
        }
        // IDL default is `emitMetric = true`: Go readers apply the default
        // when absent, so always write to preserve an explicit false.
        f.fields.push((20, Value::Bool(self.emit_metric)));
        f.ostruct(70, self.bad_binaries.as_ref());
        f.oenum(80, self.history_archival_status);
        f.sstr(90, &self.history_archival_uri);
        f.oenum(100, self.visibility_archival_status);
        f.sstr(110, &self.visibility_archival_uri);
        f.finish()
    }
}

impl FromThrift for ClusterReplicationConfiguration {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            cluster_name: s.req_string(10, "")?,
        })
    }
}

impl ToThrift for ClusterReplicationConfiguration {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.cluster_name);
        f.finish()
    }
}

impl FromThrift for ActiveClusterInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            active_cluster_name: s.req_string(10, "")?,
            failover_version: s.opt_i64(20)?.unwrap_or(0),
        })
    }
}

impl ToThrift for ActiveClusterInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.active_cluster_name);
        if self.failover_version != 0 {
            f.fields.push((20, Value::I64(self.failover_version)));
        }
        f.finish()
    }
}

impl FromThrift for ClusterAttributeScope {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            cluster_attributes: s.opt_map_str(10)?,
        })
    }
}

impl ToThrift for ClusterAttributeScope {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.omap_str_struct(10, &self.cluster_attributes);
        f.finish()
    }
}

impl FromThrift for ActiveClusters {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            active_clusters_by_region: s.opt_map_str(10)?,
            active_clusters_by_cluster_attribute: s.opt_map_str(11)?,
        })
    }
}

impl ToThrift for ActiveClusters {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.omap_str_struct(10, &self.active_clusters_by_region);
        f.omap_str_struct(11, &self.active_clusters_by_cluster_attribute);
        f.finish()
    }
}

impl FromThrift for DomainReplicationConfiguration {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            active_cluster_name: s.req_string(10, "")?,
            clusters: s
                .opt_list::<ClusterReplicationConfiguration>(20)?
                .unwrap_or_default(),
            active_clusters: s.opt_struct(30)?,
        })
    }
}

impl ToThrift for DomainReplicationConfiguration {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.active_cluster_name);
        f.olist(20, &self.clusters);
        f.ostruct(30, self.active_clusters.as_ref());
        f.finish()
    }
}

// ---------------------------------------------------------------------------
// Domain requests / responses
// ---------------------------------------------------------------------------

impl FromThrift for RegisterDomainRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            name: s.req_string(10, "")?,
            description: s.req_string(20, "")?,
            owner_email: s.req_string(30, "")?,
            workflow_execution_retention_period_in_days: s.opt_i32(40)?.unwrap_or(0),
            emit_metric: s.opt_bool(50)?,
            clusters: s
                .opt_list::<ClusterReplicationConfiguration>(60)?
                .unwrap_or_default(),
            active_cluster_name: s.req_string(70, "")?,
            active_clusters: s.opt_struct(76)?,
            data: s.opt_map_str(80)?,
            security_token: s.req_string(90, "")?,
            is_global_domain: s.opt_bool(120)?.unwrap_or(false),
            history_archival_status: s.opt_enum(130)?,
            history_archival_uri: s.req_string(140, "")?,
            visibility_archival_status: s.opt_enum(150)?,
            visibility_archival_uri: s.req_string(160, "")?,
        })
    }
}

impl ToThrift for RegisterDomainRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.name);
        f.sstr(20, &self.description);
        f.sstr(30, &self.owner_email);
        if self.workflow_execution_retention_period_in_days != 0 {
            f.fields
                .push((40, Value::I32(self.workflow_execution_retention_period_in_days)));
        }
        // IDL default `emitMetric = true`: always write to preserve explicit false.
        f.fields.push((50, Value::Bool(self.emit_metric.unwrap_or(true))));
        f.olist(60, &self.clusters);
        f.sstr(70, &self.active_cluster_name);
        f.ostruct(76, self.active_clusters.as_ref());
        f.omap_str_str(80, &self.data);
        f.sstr(90, &self.security_token);
        f.obool(120, self.is_global_domain);
        f.oenum(130, self.history_archival_status);
        f.sstr(140, &self.history_archival_uri);
        f.oenum(150, self.visibility_archival_status);
        f.sstr(160, &self.visibility_archival_uri);
        f.finish()
    }
}

impl FromThrift for DescribeDomainRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            name: s.opt_string(10)?,
            uuid: s.opt_string(20)?,
        })
    }
}

impl ToThrift for DescribeDomainRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostr(10, &self.name);
        f.ostr(20, &self.uuid);
        f.finish()
    }
}

impl FromThrift for DescribeDomainResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain_info: s.opt_struct(10)?,
            configuration: s.opt_struct(20)?,
            replication_configuration: s.opt_struct(30)?,
            failover_version: s.opt_i64(40)?.unwrap_or(0),
            is_global_domain: s.opt_bool(50)?.unwrap_or(false),
        })
    }
}

impl ToThrift for DescribeDomainResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.domain_info.as_ref());
        f.ostruct(20, self.configuration.as_ref());
        f.ostruct(30, self.replication_configuration.as_ref());
        if self.failover_version != 0 {
            f.fields.push((40, Value::I64(self.failover_version)));
        }
        f.obool(50, self.is_global_domain);
        f.finish()
    }
}

impl FromThrift for ListDomainsRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            page_size: s.opt_i32(10)?.unwrap_or(0),
            next_page_token: s.bytes_or_empty(20)?,
        })
    }
}

impl ToThrift for ListDomainsRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.page_size != 0 {
            f.fields.push((10, Value::I32(self.page_size)));
        }
        f.sbytes(20, &self.next_page_token);
        f.finish()
    }
}

impl FromThrift for ListDomainsResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domains: s.opt_list::<DescribeDomainResponse>(10)?.unwrap_or_default(),
            next_page_token: s.bytes_or_empty(20)?,
        })
    }
}

impl ToThrift for ListDomainsResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.olist(10, &self.domains);
        f.sbytes(20, &self.next_page_token);
        f.finish()
    }
}

impl FromThrift for UpdateDomainInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            description: s.opt_string(10)?,
            owner_email: s.opt_string(20)?,
            data: s.opt_map_str(30)?,
        })
    }
}

impl ToThrift for UpdateDomainInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostr(10, &self.description);
        f.ostr(20, &self.owner_email);
        f.omap_str_str(30, &self.data);
        f.finish()
    }
}

impl FromThrift for UpdateDomainRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            name: s.req_string(10, "")?,
            updated_info: s.opt_struct(20)?,
            configuration: s.opt_struct(30)?,
            replication_configuration: s.opt_struct(40)?,
            security_token: s.req_string(50, "")?,
            delete_bad_binary: s.opt_string(60)?,
            failover_timeout_in_seconds: s.opt_i32(70)?,
        })
    }
}

impl ToThrift for UpdateDomainRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.name);
        f.ostruct(20, self.updated_info.as_ref());
        f.ostruct(30, self.configuration.as_ref());
        f.ostruct(40, self.replication_configuration.as_ref());
        f.sstr(50, &self.security_token);
        f.ostr(60, &self.delete_bad_binary);
        f.oi32(70, self.failover_timeout_in_seconds);
        f.finish()
    }
}

impl FromThrift for UpdateDomainResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain_info: s.opt_struct(10)?,
            configuration: s.opt_struct(20)?,
            replication_configuration: s.opt_struct(30)?,
            failover_version: s.opt_i64(40)?.unwrap_or(0),
            is_global_domain: s.opt_bool(50)?.unwrap_or(false),
        })
    }
}

impl ToThrift for UpdateDomainResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.domain_info.as_ref());
        f.ostruct(20, self.configuration.as_ref());
        f.ostruct(30, self.replication_configuration.as_ref());
        if self.failover_version != 0 {
            f.fields.push((40, Value::I64(self.failover_version)));
        }
        f.obool(50, self.is_global_domain);
        f.finish()
    }
}

macro_rules! name_token_request {
    ($t:ty) => {
        impl FromThrift for $t {
            fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
                Ok(Self {
                    name: s.req_string(10, "")?,
                    security_token: s.req_string(20, "")?,
                })
            }
        }

        impl ToThrift for $t {
            fn to_thrift(&self) -> Struct {
                let mut f = Fields::new();
                f.sstr(10, &self.name);
                f.sstr(20, &self.security_token);
                f.finish()
            }
        }
    };
}

name_token_request!(DeprecateDomainRequest);
name_token_request!(DeleteDomainRequest);

// ---------------------------------------------------------------------------
// Workflow requests / responses
// ---------------------------------------------------------------------------

impl FromThrift for StartWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_id: s.req_string(20, "")?,
            workflow_type: s.opt_struct(30)?,
            task_list: s.opt_struct(40)?,
            input: s.bytes_or_empty(50)?,
            execution_start_to_close_timeout_seconds: s.opt_i32(60)?,
            task_start_to_close_timeout_seconds: s.opt_i32(70)?,
            identity: s.req_string(80, "")?,
            request_id: s.req_string(90, "")?,
            workflow_id_reuse_policy: s.opt_enum(100)?,
            retry_policy: s.opt_struct(120)?,
            cron_schedule: s.req_string(130, "")?,
            memo: s.opt_struct(140)?,
            search_attributes: s.opt_struct(141)?,
            header: s.opt_struct(150)?,
            cron_overlap_policy: s.opt_enum(190)?,
            delay_start_seconds: s.opt_i32(160)?,
            jitter_start_seconds: s.opt_i32(170)?,
            first_run_at_timestamp: s.opt_i64(180)?,
        })
    }
}

impl ToThrift for StartWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.sstr(20, &self.workflow_id);
        f.ostruct(30, self.workflow_type.as_ref());
        f.ostruct(40, self.task_list.as_ref());
        f.sbytes(50, &self.input);
        f.oi32(60, self.execution_start_to_close_timeout_seconds);
        f.oi32(70, self.task_start_to_close_timeout_seconds);
        f.sstr(80, &self.identity);
        f.sstr(90, &self.request_id);
        f.oenum(100, self.workflow_id_reuse_policy);
        f.ostruct(120, self.retry_policy.as_ref());
        f.sstr(130, &self.cron_schedule);
        f.ostruct(140, self.memo.as_ref());
        f.ostruct(141, self.search_attributes.as_ref());
        f.ostruct(150, self.header.as_ref());
        f.oi32(160, self.delay_start_seconds);
        f.oi32(170, self.jitter_start_seconds);
        f.oi64(180, self.first_run_at_timestamp);
        f.oenum(190, self.cron_overlap_policy);
        f.finish()
    }
}

impl FromThrift for StartWorkflowExecutionResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            run_id: s.req_string(10, "")?,
        })
    }
}

impl ToThrift for StartWorkflowExecutionResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.run_id);
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionConfiguration {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_list: s.opt_struct(10)?,
            execution_start_to_close_timeout_seconds: s.opt_i32(20)?,
            task_start_to_close_timeout_seconds: s.opt_i32(30)?,
        })
    }
}

impl ToThrift for WorkflowExecutionConfiguration {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.task_list.as_ref());
        f.oi32(20, self.execution_start_to_close_timeout_seconds);
        f.oi32(30, self.task_start_to_close_timeout_seconds);
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            execution: s.opt_struct(10)?,
            workflow_type: s.opt_struct(20)?,
            start_time: s.opt_i64(30)?,
            close_time: s.opt_i64(40)?,
            close_status: s.opt_enum(50)?,
            history_length: s.opt_i64(60)?.unwrap_or(0),
            memo: s.opt_struct(100)?,
        })
    }
}

impl ToThrift for WorkflowExecutionInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.execution.as_ref());
        f.ostruct(20, self.workflow_type.as_ref());
        f.oi64(30, self.start_time);
        f.oi64(40, self.close_time);
        f.oenum(50, self.close_status);
        if self.history_length != 0 {
            f.fields.push((60, Value::I64(self.history_length)));
        }
        f.ostruct(100, self.memo.as_ref());
        f.finish()
    }
}

impl FromThrift for DescribeWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            execution: s.opt_struct(20)?,
            query_consistency_level: s.opt_enum(30)?,
        })
    }
}

impl ToThrift for DescribeWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.execution.as_ref());
        f.oenum(30, self.query_consistency_level);
        f.finish()
    }
}

impl FromThrift for DescribeWorkflowExecutionResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            execution_configuration: s.opt_struct(10)?,
            workflow_execution_info: s.opt_struct(20)?,
            pending_activities: s.opt_list::<PendingActivityInfo>(30)?.unwrap_or_default(),
            pending_children: s.opt_list::<PendingChildExecutionInfo>(40)?.unwrap_or_default(),
            pending_decision: s.opt_struct(50)?,
        })
    }
}

impl ToThrift for DescribeWorkflowExecutionResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.execution_configuration.as_ref());
        f.ostruct(20, self.workflow_execution_info.as_ref());
        f.olist(30, &self.pending_activities);
        f.olist(40, &self.pending_children);
        f.ostruct(50, self.pending_decision.as_ref());
        f.finish()
    }
}

impl FromThrift for History {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            events: s.opt_list::<HistoryEvent>(10)?.unwrap_or_default(),
        })
    }
}

impl ToThrift for History {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.olist(10, &self.events);
        f.finish()
    }
}

impl FromThrift for GetWorkflowExecutionHistoryRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            execution: s.opt_struct(20)?,
            maximum_page_size: s.opt_i32(30)?.unwrap_or(0),
            next_page_token: s.bytes_or_empty(40)?,
            wait_for_new_event: s.opt_bool(50)?.unwrap_or(false),
            history_event_filter_type: s.opt_enum(60)?,
            skip_archival: s.opt_bool(70)?.unwrap_or(false),
            query_consistency_level: s.opt_enum(80)?,
        })
    }
}

impl ToThrift for GetWorkflowExecutionHistoryRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.execution.as_ref());
        if self.maximum_page_size != 0 {
            f.fields.push((30, Value::I32(self.maximum_page_size)));
        }
        f.sbytes(40, &self.next_page_token);
        f.obool(50, self.wait_for_new_event);
        f.oenum(60, self.history_event_filter_type);
        f.obool(70, self.skip_archival);
        f.oenum(80, self.query_consistency_level);
        f.finish()
    }
}

impl FromThrift for GetWorkflowExecutionHistoryResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            history: s.opt_struct(10)?,
            next_page_token: s.bytes_or_empty(20)?,
            archived: s.opt_bool(30)?.unwrap_or(false),
        })
    }
}

impl ToThrift for GetWorkflowExecutionHistoryResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.history.as_ref());
        f.sbytes(20, &self.next_page_token);
        f.obool(30, self.archived);
        f.finish()
    }
}

impl FromThrift for SignalWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            signal_name: s.req_string(30, "")?,
            input: s.bytes_or_empty(40)?,
            identity: s.req_string(50, "")?,
            request_id: s.req_string(60, "")?,
            control: s.bytes_or_empty(70)?,
        })
    }
}

impl ToThrift for SignalWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.workflow_execution.as_ref());
        f.sstr(30, &self.signal_name);
        f.sbytes(40, &self.input);
        f.sstr(50, &self.identity);
        f.sstr(60, &self.request_id);
        f.sbytes(70, &self.control);
        f.finish()
    }
}

impl FromThrift for SignalWithStartWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_id: s.req_string(20, "")?,
            workflow_type: s.opt_struct(30)?,
            task_list: s.opt_struct(40)?,
            input: s.bytes_or_empty(50)?,
            execution_start_to_close_timeout_seconds: s.opt_i32(60)?,
            task_start_to_close_timeout_seconds: s.opt_i32(70)?,
            identity: s.req_string(80, "")?,
            request_id: s.req_string(90, "")?,
            workflow_id_reuse_policy: s.opt_enum(100)?,
            retry_policy: s.opt_struct(140)?,
            control: s.bytes_or_empty(130)?,
            signal_name: s.req_string(110, "")?,
            signal_input: s.bytes_or_empty(120)?,
            cron_schedule: s.req_string(150, "")?,
            memo: s.opt_struct(160)?,
            search_attributes: s.opt_struct(161)?,
            header: s.opt_struct(170)?,
            delay_start_seconds: s.opt_i32(180)?,
            jitter_start_seconds: s.opt_i32(190)?,
            first_run_at_timestamp: s.opt_i64(200)?,
            cron_overlap_policy: s.opt_enum(210)?,
        })
    }
}

impl ToThrift for SignalWithStartWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.sstr(20, &self.workflow_id);
        f.ostruct(30, self.workflow_type.as_ref());
        f.ostruct(40, self.task_list.as_ref());
        f.sbytes(50, &self.input);
        f.oi32(60, self.execution_start_to_close_timeout_seconds);
        f.oi32(70, self.task_start_to_close_timeout_seconds);
        f.sstr(80, &self.identity);
        f.sstr(90, &self.request_id);
        f.oenum(100, self.workflow_id_reuse_policy);
        f.sstr(110, &self.signal_name);
        f.sbytes(120, &self.signal_input);
        f.sbytes(130, &self.control);
        f.ostruct(140, self.retry_policy.as_ref());
        f.sstr(150, &self.cron_schedule);
        f.ostruct(160, self.memo.as_ref());
        f.ostruct(161, self.search_attributes.as_ref());
        f.ostruct(170, self.header.as_ref());
        f.oi32(180, self.delay_start_seconds);
        f.oi32(190, self.jitter_start_seconds);
        f.oi64(200, self.first_run_at_timestamp);
        f.oenum(210, self.cron_overlap_policy);
        f.finish()
    }
}

impl FromThrift for TerminateWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            reason: s.req_string(30, "")?,
            details: s.bytes_or_empty(40)?,
            identity: s.req_string(50, "")?,
            first_execution_run_id: s.req_string(60, "")?,
        })
    }
}

impl ToThrift for TerminateWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.workflow_execution.as_ref());
        f.sstr(30, &self.reason);
        f.sbytes(40, &self.details);
        f.sstr(50, &self.identity);
        f.sstr(60, &self.first_execution_run_id);
        f.finish()
    }
}

impl FromThrift for RequestCancelWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            identity: s.req_string(30, "")?,
            request_id: s.req_string(40, "")?,
            cause: s.req_string(50, "")?,
            first_execution_run_id: s.req_string(60, "")?,
        })
    }
}

impl ToThrift for RequestCancelWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.workflow_execution.as_ref());
        f.sstr(30, &self.identity);
        f.sstr(40, &self.request_id);
        f.sstr(50, &self.cause);
        f.sstr(60, &self.first_execution_run_id);
        f.finish()
    }
}

impl FromThrift for WorkflowQuery {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            query_type: s.req_string(10, "")?,
            query_args: s.bytes_or_empty(20)?,
        })
    }
}

impl ToThrift for WorkflowQuery {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.query_type);
        f.sbytes(20, &self.query_args);
        f.finish()
    }
}

impl FromThrift for WorkflowQueryResult {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            result_type: s.opt_enum(10)?,
            answer: s.bytes_or_empty(20)?,
            error_message: s.req_string(30, "")?,
        })
    }
}

impl ToThrift for WorkflowQueryResult {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oenum(10, self.result_type);
        f.sbytes(20, &self.answer);
        f.sstr(30, &self.error_message);
        f.finish()
    }
}

impl FromThrift for QueryWorkflowRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            execution: s.opt_struct(20)?,
            query: s.opt_struct(30)?,
            query_reject_condition: s.opt_enum(40)?,
            query_consistency_level: s.opt_enum(50)?,
        })
    }
}

impl ToThrift for QueryWorkflowRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.execution.as_ref());
        f.ostruct(30, self.query.as_ref());
        f.oenum(40, self.query_reject_condition);
        f.oenum(50, self.query_consistency_level);
        f.finish()
    }
}

impl FromThrift for QueryWorkflowResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            query_result: s.bytes_or_empty(10)?,
            query_rejected: s.opt_struct(20)?,
        })
    }
}

impl ToThrift for QueryWorkflowResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.query_result);
        f.ostruct(20, self.query_rejected.as_ref());
        f.finish()
    }
}

impl FromThrift for QueryRejected {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            close_status: s.opt_enum(10)?,
        })
    }
}

impl ToThrift for QueryRejected {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oenum(10, self.close_status);
        f.finish()
    }
}

// ---------------------------------------------------------------------------
// Pending info / misc
// ---------------------------------------------------------------------------

impl FromThrift for PendingActivityInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            activity_id: s.req_string(10, "")?,
            activity_type: s.opt_struct(20)?,
            state: s.opt_enum(30)?,
            heartbeat_details: s.bytes_or_empty(40)?,
            attempt: s.opt_i32(70)?.unwrap_or(0),
            maximum_attempts: s.opt_i32(80)?.unwrap_or(0),
            scheduled_timestamp: s.opt_i64(90)?,
            last_started_timestamp: None,
            schedule_id: s.opt_i64(150)?.unwrap_or(0),
            last_failure_reason: s.opt_string(110)?,
        })
    }
}

impl ToThrift for PendingActivityInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.activity_id);
        f.ostruct(20, self.activity_type.as_ref());
        f.oenum(30, self.state);
        f.sbytes(40, &self.heartbeat_details);
        if self.attempt != 0 {
            f.fields.push((70, Value::I32(self.attempt)));
        }
        f.oi32(80, Some(self.maximum_attempts).filter(|v| *v != 0));
        f.oi64(90, self.scheduled_timestamp);
        f.ostr(110, &self.last_failure_reason);
        if self.schedule_id != 0 {
            f.fields.push((150, Value::I64(self.schedule_id)));
        }
        f.finish()
    }
}

impl FromThrift for PendingDecisionInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            state: s.opt_enum(10)?,
            scheduled_timestamp: s.opt_i64(20)?,
            started_timestamp: s.opt_i64(30)?,
            attempt: s.opt_i64(40)?.unwrap_or(0),
            original_scheduled_timestamp: s.opt_i64(50)?,
            schedule_id: s.opt_i64(60)?.unwrap_or(0),
        })
    }
}

impl ToThrift for PendingDecisionInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oenum(10, self.state);
        f.oi64(20, self.scheduled_timestamp);
        f.oi64(30, self.started_timestamp);
        if self.attempt != 0 {
            f.fields.push((40, Value::I64(self.attempt)));
        }
        f.oi64(50, self.original_scheduled_timestamp);
        if self.schedule_id != 0 {
            f.fields.push((60, Value::I64(self.schedule_id)));
        }
        f.finish()
    }
}

impl FromThrift for PendingChildExecutionInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(1, "")?,
            workflow_id: s.req_string(10, "")?,
            run_id: s.req_string(20, "")?,
            workflow_type_name: s.req_string(30, "")?,
            initiated_id: s.opt_i64(40)?.unwrap_or(0),
            parent_close_policy: s.opt_enum(50)?,
        })
    }
}

impl ToThrift for PendingChildExecutionInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(1, &self.domain);
        f.sstr(10, &self.workflow_id);
        f.sstr(20, &self.run_id);
        f.sstr(30, &self.workflow_type_name);
        if self.initiated_id != 0 {
            f.fields.push((40, Value::I64(self.initiated_id)));
        }
        f.oenum(50, self.parent_close_policy);
        f.finish()
    }
}

impl FromThrift for StickyExecutionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            worker_task_list: s.opt_struct(10)?,
            schedule_to_start_timeout_seconds: s.opt_i32(20)?,
        })
    }
}

impl ToThrift for StickyExecutionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.worker_task_list.as_ref());
        f.oi32(20, self.schedule_to_start_timeout_seconds);
        f.finish()
    }
}

impl FromThrift for StartTimeFilter {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            earliest_time: s.opt_i64(10)?,
            latest_time: s.opt_i64(20)?,
        })
    }
}

impl ToThrift for StartTimeFilter {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oi64(10, self.earliest_time);
        f.oi64(20, self.latest_time);
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionFilter {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            workflow_id: s.req_string(10, "")?,
            run_id: s.req_string(20, "")?,
        })
    }
}

impl ToThrift for WorkflowExecutionFilter {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.workflow_id);
        f.sstr(20, &self.run_id);
        f.finish()
    }
}

impl FromThrift for WorkflowTypeFilter {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            name: s.req_string(10, "")?,
        })
    }
}

impl ToThrift for WorkflowTypeFilter {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.name);
        f.finish()
    }
}

impl FromThrift for TaskListStatus {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            backlog_count_hint: s.opt_i64(10)?.unwrap_or(0),
            read_level: s.opt_i64(20)?.unwrap_or(0),
            ack_level: s.opt_i64(30)?.unwrap_or(0),
            rate_per_second: s.opt_f64(35)?.unwrap_or(0.0),
            task_id_block: s.opt_struct(40)?,
            new_tasks_per_second: s.opt_f64(60)?.unwrap_or(0.0),
            empty: s.opt_bool(70)?.unwrap_or(false),
        })
    }
}

impl ToThrift for TaskListStatus {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.backlog_count_hint != 0 {
            f.fields.push((10, Value::I64(self.backlog_count_hint)));
        }
        if self.read_level != 0 {
            f.fields.push((20, Value::I64(self.read_level)));
        }
        if self.ack_level != 0 {
            f.fields.push((30, Value::I64(self.ack_level)));
        }
        if self.rate_per_second != 0.0 {
            f.fields.push((35, Value::F64(self.rate_per_second)));
        }
        f.ostruct(40, self.task_id_block.as_ref());
        if self.new_tasks_per_second != 0.0 {
            f.fields.push((60, Value::F64(self.new_tasks_per_second)));
        }
        f.obool(70, self.empty);
        f.finish()
    }
}

impl FromThrift for TaskIDBlock {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            start_id: s.opt_i64(10)?.unwrap_or(0),
            end_id: s.opt_i64(20)?.unwrap_or(0),
        })
    }
}

impl ToThrift for TaskIDBlock {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.start_id != 0 {
            f.fields.push((10, Value::I64(self.start_id)));
        }
        if self.end_id != 0 {
            f.fields.push((20, Value::I64(self.end_id)));
        }
        f.finish()
    }
}

impl FromThrift for PollerInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            identity: s.req_string(20, "")?,
            last_access_time: s.opt_i64(10)?,
            rate_per_second: s.opt_f64(30)?.unwrap_or(0.0),
        })
    }
}

impl ToThrift for PollerInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oi64(10, self.last_access_time);
        f.sstr(20, &self.identity);
        if self.rate_per_second != 0.0 {
            f.fields.push((30, Value::F64(self.rate_per_second)));
        }
        f.finish()
    }
}

impl FromThrift for WorkerVersionInfo {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            impl_: s.req_string(10, "")?,
            feature_version: s.req_string(20, "")?,
        })
    }
}

impl ToThrift for WorkerVersionInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.impl_);
        f.sstr(20, &self.feature_version);
        f.finish()
    }
}

// ---------------------------------------------------------------------------
// Task poll / respond
// ---------------------------------------------------------------------------

impl FromThrift for PollForDecisionTaskRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            task_list: s.opt_struct(20)?,
            identity: s.req_string(30, "")?,
            binary_checksum: s.req_string(40, "")?,
        })
    }
}

impl ToThrift for PollForDecisionTaskRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.task_list.as_ref());
        f.sstr(30, &self.identity);
        f.sstr(40, &self.binary_checksum);
        f.finish()
    }
}

impl FromThrift for PollForDecisionTaskResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_token: s.bytes_or_empty(10)?,
            workflow_execution: s.opt_struct(20)?,
            workflow_type: s.opt_struct(30)?,
            previous_started_event_id: s.opt_i64(40)?,
            started_event_id: s.opt_i64(50)?.unwrap_or(0),
            attempt: s.opt_i64(51)?.unwrap_or(0),
            backlog_count_hint: s.opt_i64(54)?.unwrap_or(0),
            history: s.opt_struct(60)?,
            next_page_token: s.bytes_or_empty(70)?,
            workflow_execution_task_list: s.opt_struct(90)?,
            query: s.opt_struct(80)?,
            query_id: None,
            queries: s.opt_map_str(120)?,
        })
    }
}

impl ToThrift for PollForDecisionTaskResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.task_token);
        f.ostruct(20, self.workflow_execution.as_ref());
        f.ostruct(30, self.workflow_type.as_ref());
        f.oi64(40, self.previous_started_event_id);
        if self.started_event_id != 0 {
            f.fields.push((50, Value::I64(self.started_event_id)));
        }
        if self.attempt != 0 {
            f.fields.push((51, Value::I64(self.attempt)));
        }
        if self.backlog_count_hint != 0 {
            f.fields.push((54, Value::I64(self.backlog_count_hint)));
        }
        f.ostruct(60, self.history.as_ref());
        f.sbytes(70, &self.next_page_token);
        f.ostruct(80, self.query.as_ref());
        f.ostruct(90, self.workflow_execution_task_list.as_ref());
        if let Some(q) = &self.queries {
            let mut items: Vec<(Value, Value)> = q
                .iter()
                .map(|(k, v)| {
                    (
                        Value::Bytes(k.as_bytes().to_vec()),
                        Value::Struct(v.to_thrift()),
                    )
                })
                .collect();
            items.sort_by(|a, b| match (a, b) {
                ((Value::Bytes(x), _), (Value::Bytes(y), _)) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            if !items.is_empty() {
                f.fields.push((120, Value::Map(items)));
            }
        }
        f.finish()
    }
}

impl FromThrift for RespondDecisionTaskCompletedRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_token: s.bytes_or_empty(10)?,
            decisions: s.opt_list::<Decision>(20)?.unwrap_or_default(),
            execution_context: s.bytes_or_empty(30)?,
            identity: s.req_string(40, "")?,
            binary_checksum: s.req_string(80, "")?,
            query_results: s.opt_map_str(90)?,
            sticky_attributes: s.opt_struct(50)?,
            return_new_decision_task: s.opt_bool(60)?.unwrap_or(false),
            force_create_new_decision_task: s.opt_bool(70)?.unwrap_or(false),
        })
    }
}

impl ToThrift for RespondDecisionTaskCompletedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.task_token);
        f.olist(20, &self.decisions);
        f.sbytes(30, &self.execution_context);
        f.sstr(40, &self.identity);
        f.ostruct(50, self.sticky_attributes.as_ref());
        f.obool(60, self.return_new_decision_task);
        f.obool(70, self.force_create_new_decision_task);
        f.sstr(80, &self.binary_checksum);
        if let Some(q) = &self.query_results {
            let mut items: Vec<(Value, Value)> = q
                .iter()
                .map(|(k, v)| {
                    (
                        Value::Bytes(k.as_bytes().to_vec()),
                        Value::Struct(v.to_thrift()),
                    )
                })
                .collect();
            items.sort_by(|a, b| match (a, b) {
                ((Value::Bytes(x), _), (Value::Bytes(y), _)) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            if !items.is_empty() {
                f.fields.push((90, Value::Map(items)));
            }
        }
        f.finish()
    }
}

impl FromThrift for RespondDecisionTaskCompletedResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            decision_task: s.opt_struct(10)?,
        })
    }
}

impl ToThrift for RespondDecisionTaskCompletedResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.decision_task.as_ref());
        f.finish()
    }
}

impl FromThrift for RespondDecisionTaskFailedRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_token: s.bytes_or_empty(10)?,
            cause: s.opt_enum(20)?,
            details: s.bytes_or_empty(30)?,
            identity: s.req_string(40, "")?,
            binary_checksum: s.req_string(50, "")?,
        })
    }
}

impl ToThrift for RespondDecisionTaskFailedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.task_token);
        f.oenum(20, self.cause);
        f.sbytes(30, &self.details);
        f.sstr(40, &self.identity);
        f.sstr(50, &self.binary_checksum);
        f.finish()
    }
}

impl FromThrift for PollForActivityTaskRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            task_list: s.opt_struct(20)?,
            identity: s.req_string(30, "")?,
            task_list_metadata: s.opt_struct(40)?,
        })
    }
}

impl ToThrift for PollForActivityTaskRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.task_list.as_ref());
        f.sstr(30, &self.identity);
        f.ostruct(40, self.task_list_metadata.as_ref());
        f.finish()
    }
}

impl FromThrift for PollForActivityTaskResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_token: s.bytes_or_empty(10)?,
            workflow_execution: s.opt_struct(20)?,
            activity_id: s.req_string(30, "")?,
            activity_type: s.opt_struct(40)?,
            input: s.bytes_or_empty(50)?,
            scheduled_timestamp: s.opt_i64(70)?,
            schedule_to_close_timeout_seconds: s.opt_i32(80)?,
            started_timestamp: s.opt_i64(90)?,
            start_to_close_timeout_seconds: s.opt_i32(100)?,
            heartbeat_timeout_seconds: s.opt_i32(110)?,
            attempt: s.opt_i32(120)?.unwrap_or(0),
            scheduled_timestamp_of_this_attempt: s.opt_i64(130)?,
            heartbeat_details: s.bytes_or_empty(140)?,
            workflow_type: s.opt_struct(150)?,
            workflow_domain: s.req_string(160, "")?,
            header: s.opt_struct(170)?,
        })
    }
}

impl ToThrift for PollForActivityTaskResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.task_token);
        f.ostruct(20, self.workflow_execution.as_ref());
        f.sstr(30, &self.activity_id);
        f.ostruct(40, self.activity_type.as_ref());
        f.sbytes(50, &self.input);
        f.oi64(70, millis_to_nanos(self.scheduled_timestamp));
        f.oi32(80, self.schedule_to_close_timeout_seconds);
        f.oi64(90, millis_to_nanos(self.started_timestamp));
        f.oi32(100, self.start_to_close_timeout_seconds);
        f.oi32(110, self.heartbeat_timeout_seconds);
        if self.attempt != 0 {
            f.fields.push((120, Value::I32(self.attempt)));
        }
        f.oi64(130, millis_to_nanos(self.scheduled_timestamp_of_this_attempt));
        f.sbytes(140, &self.heartbeat_details);
        f.ostruct(150, self.workflow_type.as_ref());
        f.sstr(160, &self.workflow_domain);
        f.ostruct(170, self.header.as_ref());
        f.finish()
    }
}

impl FromThrift for RespondActivityTaskCompletedRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_token: s.bytes_or_empty(10)?,
            result: s.bytes_or_empty(20)?,
            identity: s.req_string(30, "")?,
        })
    }
}

impl ToThrift for RespondActivityTaskCompletedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.task_token);
        f.sbytes(20, &self.result);
        f.sstr(30, &self.identity);
        f.finish()
    }
}

impl FromThrift for RespondActivityTaskFailedRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_token: s.bytes_or_empty(10)?,
            reason: s.opt_string(20)?,
            details: s.bytes_or_empty(30)?,
            identity: s.req_string(40, "")?,
            failure_options: s.opt_struct(45)?,
            heartbeat_details: s.bytes_or_empty(50)?,
        })
    }
}

impl ToThrift for RespondActivityTaskFailedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.task_token);
        f.ostr(20, &self.reason);
        f.sbytes(30, &self.details);
        f.sstr(40, &self.identity);
        f.ostruct(45, self.failure_options.as_ref());
        f.sbytes(50, &self.heartbeat_details);
        f.finish()
    }
}

impl FromThrift for RespondActivityTaskCanceledRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_token: s.bytes_or_empty(10)?,
            details: s.bytes_or_empty(20)?,
            identity: s.req_string(30, "")?,
        })
    }
}

impl ToThrift for RespondActivityTaskCanceledRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.task_token);
        f.sbytes(20, &self.details);
        f.sstr(30, &self.identity);
        f.finish()
    }
}

impl FromThrift for RecordActivityTaskHeartbeatRequest {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_token: s.bytes_or_empty(10)?,
            details: s.bytes_or_empty(20)?,
            identity: s.req_string(30, "")?,
        })
    }
}

impl ToThrift for RecordActivityTaskHeartbeatRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.task_token);
        f.sbytes(20, &self.details);
        f.sstr(30, &self.identity);
        f.finish()
    }
}

impl FromThrift for RecordActivityTaskHeartbeatResponse {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            cancel_requested: s.opt_bool(10)?.unwrap_or(false),
        })
    }
}

impl ToThrift for RecordActivityTaskHeartbeatResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.obool(10, self.cancel_requested);
        f.finish()
    }
}

// ---------------------------------------------------------------------------
// Decisions
// ---------------------------------------------------------------------------

impl FromThrift for ScheduleActivityTaskDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            activity_id: s.req_string(10, "")?,
            activity_type: s.opt_struct(20)?,
            domain: s.req_string(25, "")?,
            task_list: s.opt_struct(30)?,
            input: s.bytes_or_empty(40)?,
            schedule_to_close_timeout_seconds: s.opt_i32(45)?,
            schedule_to_start_timeout_seconds: s.opt_i32(50)?,
            start_to_close_timeout_seconds: s.opt_i32(55)?,
            heartbeat_timeout_seconds: s.opt_i32(60)?,
            retry_policy: s.opt_struct(70)?,
            header: s.opt_struct(80)?,
        })
    }
}

impl ToThrift for ScheduleActivityTaskDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.activity_id);
        f.ostruct(20, self.activity_type.as_ref());
        f.sstr(25, &self.domain);
        f.ostruct(30, self.task_list.as_ref());
        f.sbytes(40, &self.input);
        f.oi32(45, self.schedule_to_close_timeout_seconds);
        f.oi32(50, self.schedule_to_start_timeout_seconds);
        f.oi32(55, self.start_to_close_timeout_seconds);
        f.oi32(60, self.heartbeat_timeout_seconds);
        f.ostruct(70, self.retry_policy.as_ref());
        f.ostruct(80, self.header.as_ref());
        f.finish()
    }
}

impl FromThrift for CompleteWorkflowExecutionDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            result: s.bytes_or_empty(10)?,
        })
    }
}

impl ToThrift for CompleteWorkflowExecutionDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.result);
        f.finish()
    }
}

impl FromThrift for FailWorkflowExecutionDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            reason: s.opt_string(10)?,
            details: s.bytes_or_empty(20)?,
        })
    }
}

impl ToThrift for FailWorkflowExecutionDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostr(10, &self.reason);
        f.sbytes(20, &self.details);
        f.finish()
    }
}

impl FromThrift for RecordMarkerDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            marker_name: s.req_string(10, "")?,
            details: s.bytes_or_empty(20)?,
            header: s.opt_struct(30)?,
        })
    }
}

impl ToThrift for RecordMarkerDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.marker_name);
        f.sbytes(20, &self.details);
        f.ostruct(30, self.header.as_ref());
        f.finish()
    }
}

impl FromThrift for StartTimerDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            timer_id: s.req_string(10, "")?,
            start_to_fire_timeout_seconds: s.opt_i64(20)?,
        })
    }
}

impl ToThrift for StartTimerDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.timer_id);
        f.oi64(20, self.start_to_fire_timeout_seconds);
        f.finish()
    }
}

impl FromThrift for CancelTimerDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            timer_id: s.req_string(10, "")?,
        })
    }
}

impl ToThrift for CancelTimerDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.timer_id);
        f.finish()
    }
}

impl FromThrift for RequestCancelActivityTaskDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            activity_id: s.req_string(10, "")?,
        })
    }
}

impl ToThrift for RequestCancelActivityTaskDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.activity_id);
        f.finish()
    }
}

impl FromThrift for CancelWorkflowExecutionDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            details: s.bytes_or_empty(10)?,
        })
    }
}

impl ToThrift for CancelWorkflowExecutionDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.details);
        f.finish()
    }
}

impl FromThrift for ContinueAsNewWorkflowExecutionDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            workflow_type: s.opt_struct(10)?,
            task_list: s.opt_struct(20)?,
            input: s.bytes_or_empty(30)?,
            execution_start_to_close_timeout_seconds: s.opt_i32(40)?,
            task_start_to_close_timeout_seconds: s.opt_i32(50)?,
            backoff_start_interval_in_seconds: s.opt_i32(60)?,
            retry_policy: s.opt_struct(70)?,
            initiator: s.opt_enum(80)?,
            failure_reason: s.opt_string(90)?,
            failure_details: s.bytes_or_empty(100)?,
            last_completion_result: s.bytes_or_empty(110)?,
            cron_schedule: s.req_string(120, "")?,
            header: s.opt_struct(130)?,
            memo: s.opt_struct(140)?,
            search_attributes: s.opt_struct(150)?,
            jitter_start_seconds: s.opt_i32(160)?,
            cron_overlap_policy: s.opt_enum(170)?,
        })
    }
}

impl ToThrift for ContinueAsNewWorkflowExecutionDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.workflow_type.as_ref());
        f.ostruct(20, self.task_list.as_ref());
        f.sbytes(30, &self.input);
        f.oi32(40, self.execution_start_to_close_timeout_seconds);
        f.oi32(50, self.task_start_to_close_timeout_seconds);
        f.oi32(60, self.backoff_start_interval_in_seconds);
        f.ostruct(70, self.retry_policy.as_ref());
        f.oenum(80, self.initiator);
        f.ostr(90, &self.failure_reason);
        f.sbytes(100, &self.failure_details);
        f.sbytes(110, &self.last_completion_result);
        f.sstr(120, &self.cron_schedule);
        f.ostruct(130, self.header.as_ref());
        f.ostruct(140, self.memo.as_ref());
        f.ostruct(150, self.search_attributes.as_ref());
        f.oi32(160, self.jitter_start_seconds);
        f.oenum(170, self.cron_overlap_policy);
        f.finish()
    }
}

impl FromThrift for StartChildWorkflowExecutionDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_id: s.req_string(20, "")?,
            workflow_type: s.opt_struct(30)?,
            task_list: s.opt_struct(40)?,
            input: s.bytes_or_empty(50)?,
            execution_start_to_close_timeout_seconds: s.opt_i32(60)?,
            task_start_to_close_timeout_seconds: s.opt_i32(70)?,
            parent_close_policy: s.opt_enum(81)?,
            control: s.bytes_or_empty(90)?,
            workflow_id_reuse_policy: s.opt_enum(100)?,
            retry_policy: s.opt_struct(110)?,
            header: s.opt_struct(130)?,
            memo: s.opt_struct(140)?,
            search_attributes: s.opt_struct(150)?,
            cron_schedule: s.req_string(120, "")?,
            cron_overlap_policy: s.opt_enum(160)?,
        })
    }
}

impl ToThrift for StartChildWorkflowExecutionDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.sstr(20, &self.workflow_id);
        f.ostruct(30, self.workflow_type.as_ref());
        f.ostruct(40, self.task_list.as_ref());
        f.sbytes(50, &self.input);
        f.oi32(60, self.execution_start_to_close_timeout_seconds);
        f.oi32(70, self.task_start_to_close_timeout_seconds);
        f.oenum(81, self.parent_close_policy);
        f.sbytes(90, &self.control);
        f.oenum(100, self.workflow_id_reuse_policy);
        f.ostruct(110, self.retry_policy.as_ref());
        f.sstr(120, &self.cron_schedule);
        f.ostruct(130, self.header.as_ref());
        f.ostruct(140, self.memo.as_ref());
        f.ostruct(150, self.search_attributes.as_ref());
        f.oenum(160, self.cron_overlap_policy);
        f.finish()
    }
}

impl FromThrift for SignalExternalWorkflowExecutionDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            execution: s.opt_struct(20)?,
            signal_name: s.req_string(30, "")?,
            input: s.bytes_or_empty(40)?,
            control: s.bytes_or_empty(50)?,
            child_workflow_only: s.opt_bool(60)?.unwrap_or(false),
        })
    }
}

impl ToThrift for SignalExternalWorkflowExecutionDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.execution.as_ref());
        f.sstr(30, &self.signal_name);
        f.sbytes(40, &self.input);
        f.sbytes(50, &self.control);
        f.obool(60, self.child_workflow_only);
        f.finish()
    }
}

impl FromThrift for UpsertWorkflowSearchAttributesDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            search_attributes: s.opt_struct(10)?,
        })
    }
}

impl ToThrift for UpsertWorkflowSearchAttributesDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.search_attributes.as_ref());
        f.finish()
    }
}

impl FromThrift for RequestCancelExternalWorkflowExecutionDecisionAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_id: s.req_string(20, "")?,
            run_id: s.req_string(30, "")?,
            control: s.bytes_or_empty(40)?,
            child_workflow_only: s.opt_bool(50)?.unwrap_or(false),
        })
    }
}

impl ToThrift for RequestCancelExternalWorkflowExecutionDecisionAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.sstr(20, &self.workflow_id);
        f.sstr(30, &self.run_id);
        f.sbytes(40, &self.control);
        f.obool(50, self.child_workflow_only);
        f.finish()
    }
}

impl FromThrift for Decision {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            decision_type: s.opt_enum(10)?,
            schedule_activity_task_decision_attributes: s.opt_struct(20)?,
            start_timer_decision_attributes: s.opt_struct(25)?,
            complete_workflow_execution_decision_attributes: s.opt_struct(30)?,
            fail_workflow_execution_decision_attributes: s.opt_struct(35)?,
            request_cancel_activity_task_decision_attributes: s.opt_struct(40)?,
            cancel_timer_decision_attributes: s.opt_struct(50)?,
            cancel_workflow_execution_decision_attributes: s.opt_struct(60)?,
            request_cancel_external_workflow_execution_decision_attributes: s
                .opt_struct(70)?,
            record_marker_decision_attributes: s.opt_struct(80)?,
            continue_as_new_workflow_execution_decision_attributes: s.opt_struct(90)?,
            start_child_workflow_execution_decision_attributes: s.opt_struct(100)?,
            signal_external_workflow_execution_decision_attributes: s.opt_struct(110)?,
            upsert_workflow_search_attributes_decision_attributes: s.opt_struct(120)?,
        })
    }
}

impl ToThrift for Decision {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oenum(10, self.decision_type);
        f.ostruct(20, self.schedule_activity_task_decision_attributes.as_ref());
        f.ostruct(25, self.start_timer_decision_attributes.as_ref());
        f.ostruct(30, self.complete_workflow_execution_decision_attributes.as_ref());
        f.ostruct(35, self.fail_workflow_execution_decision_attributes.as_ref());
        f.ostruct(40, self.request_cancel_activity_task_decision_attributes.as_ref());
        f.ostruct(50, self.cancel_timer_decision_attributes.as_ref());
        f.ostruct(60, self.cancel_workflow_execution_decision_attributes.as_ref());
        f.ostruct(70, self.request_cancel_external_workflow_execution_decision_attributes.as_ref());
        f.ostruct(80, self.record_marker_decision_attributes.as_ref());
        f.ostruct(90, self.continue_as_new_workflow_execution_decision_attributes.as_ref());
        f.ostruct(100, self.start_child_workflow_execution_decision_attributes.as_ref());
        f.ostruct(110, self.signal_external_workflow_execution_decision_attributes.as_ref());
        f.ostruct(120, self.upsert_workflow_search_attributes_decision_attributes.as_ref());
        f.finish()
    }
}

// ---------------------------------------------------------------------------
// History events
// ---------------------------------------------------------------------------

impl FromThrift for DecisionTaskScheduledEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            task_list: s.opt_struct(10)?,
            start_to_close_timeout_seconds: s.opt_i32(20)?,
            attempt: s.opt_i64(30)?.unwrap_or(0),
        })
    }
}

impl ToThrift for DecisionTaskScheduledEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.task_list.as_ref());
        f.oi32(20, self.start_to_close_timeout_seconds);
        if self.attempt != 0 {
            f.fields.push((30, Value::I64(self.attempt)));
        }
        f.finish()
    }
}

impl FromThrift for DecisionTaskStartedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            scheduled_event_id: s.opt_i64(10)?.unwrap_or(0),
            identity: s.req_string(20, "")?,
            request_id: s.req_string(30, "")?,
        })
    }
}

impl ToThrift for DecisionTaskStartedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.scheduled_event_id != 0 {
            f.fields.push((10, Value::I64(self.scheduled_event_id)));
        }
        f.sstr(20, &self.identity);
        f.sstr(30, &self.request_id);
        f.finish()
    }
}

impl FromThrift for DecisionTaskCompletedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            execution_context: s.bytes_or_empty(10)?,
            scheduled_event_id: s.opt_i64(20)?.unwrap_or(0),
            started_event_id: s.opt_i64(30)?.unwrap_or(0),
            identity: s.req_string(40, "")?,
            binary_checksum: s.req_string(50, "")?,
        })
    }
}

impl ToThrift for DecisionTaskCompletedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.execution_context);
        if self.scheduled_event_id != 0 {
            f.fields.push((20, Value::I64(self.scheduled_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((30, Value::I64(self.started_event_id)));
        }
        f.sstr(40, &self.identity);
        f.sstr(50, &self.binary_checksum);
        f.finish()
    }
}

impl FromThrift for DecisionTaskFailedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            scheduled_event_id: s.opt_i64(10)?.unwrap_or(0),
            started_event_id: s.opt_i64(20)?.unwrap_or(0),
            cause: s.opt_enum(30)?,
            details: s.bytes_or_empty(35)?,
            identity: s.req_string(40, "")?,
            reason: s.opt_string(50)?,
            binary_checksum: s.req_string(90, "")?,
        })
    }
}

impl ToThrift for DecisionTaskFailedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.scheduled_event_id != 0 {
            f.fields.push((10, Value::I64(self.scheduled_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((20, Value::I64(self.started_event_id)));
        }
        f.oenum(30, self.cause);
        f.sbytes(35, &self.details);
        f.sstr(40, &self.identity);
        f.ostr(50, &self.reason);
        f.sstr(90, &self.binary_checksum);
        f.finish()
    }
}

impl FromThrift for ActivityTaskScheduledEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            activity_id: s.req_string(10, "")?,
            activity_type: s.opt_struct(20)?,
            domain: s.opt_string(25)?,
            task_list: s.opt_struct(30)?,
            input: s.bytes_or_empty(40)?,
            schedule_to_close_timeout_seconds: s.opt_i32(45)?,
            schedule_to_start_timeout_seconds: s.opt_i32(50)?,
            start_to_close_timeout_seconds: s.opt_i32(55)?,
            heartbeat_timeout_seconds: s.opt_i32(60)?,
            decision_task_completed_event_id: s.opt_i64(90)?.unwrap_or(0),
            retry_policy: s.opt_struct(110)?,
            header: s.opt_struct(120)?,
        })
    }
}

impl ToThrift for ActivityTaskScheduledEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.activity_id);
        f.ostruct(20, self.activity_type.as_ref());
        f.ostr(25, &self.domain);
        f.ostruct(30, self.task_list.as_ref());
        f.sbytes(40, &self.input);
        f.oi32(45, self.schedule_to_close_timeout_seconds);
        f.oi32(50, self.schedule_to_start_timeout_seconds);
        f.oi32(55, self.start_to_close_timeout_seconds);
        f.oi32(60, self.heartbeat_timeout_seconds);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((90, Value::I64(self.decision_task_completed_event_id)));
        }
        f.ostruct(110, self.retry_policy.as_ref());
        f.ostruct(120, self.header.as_ref());
        f.finish()
    }
}

impl FromThrift for ActivityTaskStartedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            scheduled_event_id: s.opt_i64(10)?.unwrap_or(0),
            identity: s.req_string(20, "")?,
            request_id: s.req_string(30, "")?,
            attempt: s.opt_i32(40)?.unwrap_or(0),
            last_failure_reason: s.opt_string(50)?,
            last_failure_details: s.bytes_or_empty(60)?,
            last_failure_options: s.opt_struct(70)?,
        })
    }
}

impl ToThrift for ActivityTaskStartedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.scheduled_event_id != 0 {
            f.fields.push((10, Value::I64(self.scheduled_event_id)));
        }
        f.sstr(20, &self.identity);
        f.sstr(30, &self.request_id);
        if self.attempt != 0 {
            f.fields.push((40, Value::I32(self.attempt)));
        }
        f.ostr(50, &self.last_failure_reason);
        f.sbytes(60, &self.last_failure_details);
        f.ostruct(70, self.last_failure_options.as_ref());
        f.finish()
    }
}

impl FromThrift for ActivityTaskCompletedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            result: s.bytes_or_empty(10)?,
            scheduled_event_id: s.opt_i64(20)?.unwrap_or(0),
            started_event_id: s.opt_i64(30)?.unwrap_or(0),
            identity: s.req_string(40, "")?,
        })
    }
}

impl ToThrift for ActivityTaskCompletedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.result);
        if self.scheduled_event_id != 0 {
            f.fields.push((20, Value::I64(self.scheduled_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((30, Value::I64(self.started_event_id)));
        }
        f.sstr(40, &self.identity);
        f.finish()
    }
}

impl FromThrift for ActivityTaskFailedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            reason: s.opt_string(10)?,
            details: s.bytes_or_empty(20)?,
            failure_options: s.opt_struct(25)?,
            scheduled_event_id: s.opt_i64(30)?.unwrap_or(0),
            started_event_id: s.opt_i64(40)?.unwrap_or(0),
            identity: s.req_string(50, "")?,
        })
    }
}

impl ToThrift for ActivityTaskFailedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostr(10, &self.reason);
        f.sbytes(20, &self.details);
        f.ostruct(25, self.failure_options.as_ref());
        if self.scheduled_event_id != 0 {
            f.fields.push((30, Value::I64(self.scheduled_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((40, Value::I64(self.started_event_id)));
        }
        f.sstr(50, &self.identity);
        f.finish()
    }
}

impl FromThrift for ActivityTaskCancelRequestedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            activity_id: s.req_string(10, "")?,
            decision_task_completed_event_id: s.opt_i64(20)?.unwrap_or(0),
        })
    }
}

impl ToThrift for ActivityTaskCancelRequestedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.activity_id);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((20, Value::I64(self.decision_task_completed_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for ActivityTaskCanceledEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            details: s.bytes_or_empty(10)?,
            latest_cancel_requested_event_id: s.opt_i64(20)?.unwrap_or(0),
            scheduled_event_id: s.opt_i64(30)?.unwrap_or(0),
            started_event_id: s.opt_i64(40)?.unwrap_or(0),
            identity: s.req_string(50, "")?,
        })
    }
}

impl ToThrift for ActivityTaskCanceledEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.details);
        if self.latest_cancel_requested_event_id != 0 {
            f.fields.push((20, Value::I64(self.latest_cancel_requested_event_id)));
        }
        if self.scheduled_event_id != 0 {
            f.fields.push((30, Value::I64(self.scheduled_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((40, Value::I64(self.started_event_id)));
        }
        f.sstr(50, &self.identity);
        f.finish()
    }
}

impl FromThrift for TimerStartedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            timer_id: s.req_string(10, "")?,
            start_to_fire_timeout_seconds: s.opt_i64(20)?,
            decision_task_completed_event_id: s.opt_i64(30)?.unwrap_or(0),
        })
    }
}

impl ToThrift for TimerStartedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.timer_id);
        f.oi64(20, self.start_to_fire_timeout_seconds);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((30, Value::I64(self.decision_task_completed_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for TimerFiredEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            timer_id: s.req_string(10, "")?,
            started_event_id: s.opt_i64(20)?.unwrap_or(0),
        })
    }
}

impl ToThrift for TimerFiredEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.timer_id);
        if self.started_event_id != 0 {
            f.fields.push((20, Value::I64(self.started_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for TimerCanceledEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            timer_id: s.req_string(10, "")?,
            started_event_id: s.opt_i64(20)?.unwrap_or(0),
            decision_task_completed_event_id: s.opt_i64(30)?.unwrap_or(0),
            identity: s.req_string(40, "")?,
        })
    }
}

impl ToThrift for TimerCanceledEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.timer_id);
        if self.started_event_id != 0 {
            f.fields.push((20, Value::I64(self.started_event_id)));
        }
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((30, Value::I64(self.decision_task_completed_event_id)));
        }
        f.sstr(40, &self.identity);
        f.finish()
    }
}

impl FromThrift for MarkerRecordedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            marker_name: s.req_string(10, "")?,
            details: s.bytes_or_empty(20)?,
            decision_task_completed_event_id: s.opt_i64(30)?.unwrap_or(0),
            header: s.opt_struct(40)?,
        })
    }
}

impl ToThrift for MarkerRecordedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.marker_name);
        f.sbytes(20, &self.details);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((30, Value::I64(self.decision_task_completed_event_id)));
        }
        f.ostruct(40, self.header.as_ref());
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionStartedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            workflow_type: s.opt_struct(10)?,
            parent_workflow_domain: s.opt_string(12)?,
            parent_workflow_execution: s.opt_struct(14)?,
            parent_initiated_event_id: s.opt_i64(16)?,
            task_list: s.opt_struct(20)?,
            input: s.bytes_or_empty(30)?,
            execution_start_to_close_timeout_seconds: s.opt_i32(40)?,
            task_start_to_close_timeout_seconds: s.opt_i32(50)?,
            continued_execution_run_id: s.req_string(54, "")?,
            initiator: s.opt_enum(55)?,
            continued_failure_reason: s.req_string(56, "")?,
            continued_failure_details: s.bytes_or_empty(57)?,
            last_completion_result: s.bytes_or_empty(58)?,
            original_execution_run_id: s.req_string(59, "")?,
            identity: s.req_string(60, "")?,
            first_execution_run_id: s.req_string(61, "")?,
            first_scheduled_time_nano: s.opt_i64(62)?,
            retry_policy: s.opt_struct(70)?,
            attempt: s.opt_i32(80)?.unwrap_or(0),
            expiration_timestamp: s.opt_i64(90)?,
            cron_schedule: s.req_string(100, "")?,
            first_decision_task_backoff_seconds: s.opt_i32(110)?,
            memo: s.opt_struct(120)?,
            search_attributes: s.opt_struct(121)?,
            header: s.opt_struct(140)?,
            partition_config: s.opt_map_str(150)?,
            request_id: s.req_string(160, "")?,
            cron_overlap_policy: s.opt_enum(170)?,
        })
    }
}

impl ToThrift for WorkflowExecutionStartedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.workflow_type.as_ref());
        f.ostr(12, &self.parent_workflow_domain);
        f.ostruct(14, self.parent_workflow_execution.as_ref());
        f.oi64(16, self.parent_initiated_event_id);
        f.ostruct(20, self.task_list.as_ref());
        f.sbytes(30, &self.input);
        f.oi32(40, self.execution_start_to_close_timeout_seconds);
        f.oi32(50, self.task_start_to_close_timeout_seconds);
        f.sstr(54, &self.continued_execution_run_id);
        f.oenum(55, self.initiator);
        f.sstr(56, &self.continued_failure_reason);
        f.sbytes(57, &self.continued_failure_details);
        f.sbytes(58, &self.last_completion_result);
        f.sstr(59, &self.original_execution_run_id);
        f.sstr(60, &self.identity);
        f.sstr(61, &self.first_execution_run_id);
        f.oi64(62, self.first_scheduled_time_nano);
        f.ostruct(70, self.retry_policy.as_ref());
        if self.attempt != 0 {
            f.fields.push((80, Value::I32(self.attempt)));
        }
        f.oi64(90, self.expiration_timestamp);
        f.sstr(100, &self.cron_schedule);
        f.oi32(110, self.first_decision_task_backoff_seconds);
        f.ostruct(120, self.memo.as_ref());
        f.ostruct(121, self.search_attributes.as_ref());
        f.ostruct(140, self.header.as_ref());
        f.omap_str_str(150, &self.partition_config);
        f.sstr(160, &self.request_id);
        f.oenum(170, self.cron_overlap_policy);
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionCompletedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            result: s.bytes_or_empty(10)?,
            decision_task_completed_event_id: s.opt_i64(20)?.unwrap_or(0),
        })
    }
}

impl ToThrift for WorkflowExecutionCompletedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.result);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((20, Value::I64(self.decision_task_completed_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionFailedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            reason: s.opt_string(10)?,
            details: s.bytes_or_empty(20)?,
            decision_task_completed_event_id: s.opt_i64(30)?.unwrap_or(0),
        })
    }
}

impl ToThrift for WorkflowExecutionFailedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostr(10, &self.reason);
        f.sbytes(20, &self.details);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((30, Value::I64(self.decision_task_completed_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionSignaledEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            signal_name: s.req_string(10, "")?,
            input: s.bytes_or_empty(20)?,
            identity: s.req_string(30, "")?,
            request_id: s.req_string(40, "")?,
        })
    }
}

impl ToThrift for WorkflowExecutionSignaledEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.signal_name);
        f.sbytes(20, &self.input);
        f.sstr(30, &self.identity);
        f.sstr(40, &self.request_id);
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionTerminatedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            reason: s.req_string(10, "")?,
            details: s.bytes_or_empty(20)?,
            identity: s.req_string(30, "")?,
        })
    }
}

impl ToThrift for WorkflowExecutionTerminatedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.reason);
        f.sbytes(20, &self.details);
        f.sstr(30, &self.identity);
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionCancelRequestedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            cause: s.req_string(10, "")?,
            external_initiated_event_id: s.opt_i64(20)?,
            external_workflow_execution: s.opt_struct(30)?,
            identity: s.req_string(40, "")?,
            request_id: s.req_string(50, "")?,
        })
    }
}

impl ToThrift for WorkflowExecutionCancelRequestedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.cause);
        f.oi64(20, self.external_initiated_event_id);
        f.ostruct(30, self.external_workflow_execution.as_ref());
        f.sstr(40, &self.identity);
        f.sstr(50, &self.request_id);
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionCanceledEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            decision_task_completed_event_id: s.opt_i64(10)?.unwrap_or(0),
            details: s.bytes_or_empty(20)?,
        })
    }
}

impl ToThrift for WorkflowExecutionCanceledEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((10, Value::I64(self.decision_task_completed_event_id)));
        }
        f.sbytes(20, &self.details);
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionContinuedAsNewEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            new_execution_run_id: s.req_string(10, "")?,
            workflow_type: s.opt_struct(20)?,
            task_list: s.opt_struct(30)?,
            input: s.bytes_or_empty(40)?,
            execution_start_to_close_timeout_seconds: s.opt_i32(50)?,
            task_start_to_close_timeout_seconds: s.opt_i32(60)?,
            decision_task_completed_event_id: s.opt_i64(70)?.unwrap_or(0),
            backoff_start_interval_in_seconds: s.opt_i32(80)?,
            initiator: s.opt_enum(90)?,
            failure_reason: s.opt_string(100)?,
            failure_details: s.bytes_or_empty(110)?,
            last_completion_result: s.bytes_or_empty(120)?,
            header: s.opt_struct(130)?,
            memo: s.opt_struct(140)?,
            search_attributes: s.opt_struct(150)?,
            cron_overlap_policy: s.opt_enum(160)?,
        })
    }
}

impl ToThrift for WorkflowExecutionContinuedAsNewEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.new_execution_run_id);
        f.ostruct(20, self.workflow_type.as_ref());
        f.ostruct(30, self.task_list.as_ref());
        f.sbytes(40, &self.input);
        f.oi32(50, self.execution_start_to_close_timeout_seconds);
        f.oi32(60, self.task_start_to_close_timeout_seconds);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((70, Value::I64(self.decision_task_completed_event_id)));
        }
        f.oi32(80, self.backoff_start_interval_in_seconds);
        f.oenum(90, self.initiator);
        f.ostr(100, &self.failure_reason);
        f.sbytes(110, &self.failure_details);
        f.sbytes(120, &self.last_completion_result);
        f.ostruct(130, self.header.as_ref());
        f.ostruct(140, self.memo.as_ref());
        f.ostruct(150, self.search_attributes.as_ref());
        f.oenum(160, self.cron_overlap_policy);
        f.finish()
    }
}

impl FromThrift for StartChildWorkflowExecutionInitiatedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_id: s.req_string(20, "")?,
            workflow_type: s.opt_struct(30)?,
            task_list: s.opt_struct(40)?,
            input: s.bytes_or_empty(50)?,
            execution_start_to_close_timeout_seconds: s.opt_i32(60)?,
            task_start_to_close_timeout_seconds: s.opt_i32(70)?,
            parent_close_policy: s.opt_enum(81)?,
            control: s.bytes_or_empty(90)?,
            decision_task_completed_event_id: s.opt_i64(100)?.unwrap_or(0),
            workflow_id_reuse_policy: s.opt_enum(110)?,
            retry_policy: s.opt_struct(120)?,
            cron_schedule: s.req_string(130, "")?,
            header: s.opt_struct(140)?,
            memo: s.opt_struct(150)?,
            search_attributes: s.opt_struct(160)?,
            delay_start_seconds: s.opt_i32(170)?,
            jitter_start_seconds: s.opt_i32(180)?,
            first_run_at_timestamp: s.opt_i64(190)?,
            cron_overlap_policy: s.opt_enum(200)?,
        })
    }
}

impl ToThrift for StartChildWorkflowExecutionInitiatedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.sstr(20, &self.workflow_id);
        f.ostruct(30, self.workflow_type.as_ref());
        f.ostruct(40, self.task_list.as_ref());
        f.sbytes(50, &self.input);
        f.oi32(60, self.execution_start_to_close_timeout_seconds);
        f.oi32(70, self.task_start_to_close_timeout_seconds);
        f.oenum(81, self.parent_close_policy);
        f.sbytes(90, &self.control);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((100, Value::I64(self.decision_task_completed_event_id)));
        }
        f.oenum(110, self.workflow_id_reuse_policy);
        f.ostruct(120, self.retry_policy.as_ref());
        f.sstr(130, &self.cron_schedule);
        f.ostruct(140, self.header.as_ref());
        f.ostruct(150, self.memo.as_ref());
        f.ostruct(160, self.search_attributes.as_ref());
        f.oi32(170, self.delay_start_seconds);
        f.oi32(180, self.jitter_start_seconds);
        f.oi64(190, self.first_run_at_timestamp);
        f.oenum(200, self.cron_overlap_policy);
        f.finish()
    }
}

impl FromThrift for StartChildWorkflowExecutionFailedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_id: s.req_string(20, "")?,
            workflow_type: s.opt_struct(30)?,
            cause: s.opt_enum(40)?,
            control: s.bytes_or_empty(50)?,
            initiated_event_id: s.opt_i64(60)?.unwrap_or(0),
            decision_task_completed_event_id: s.opt_i64(70)?.unwrap_or(0),
        })
    }
}

impl ToThrift for StartChildWorkflowExecutionFailedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.sstr(20, &self.workflow_id);
        f.ostruct(30, self.workflow_type.as_ref());
        f.oenum(40, self.cause);
        f.sbytes(50, &self.control);
        if self.initiated_event_id != 0 {
            f.fields.push((60, Value::I64(self.initiated_event_id)));
        }
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((70, Value::I64(self.decision_task_completed_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for ChildWorkflowExecutionStartedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            initiated_event_id: s.opt_i64(20)?.unwrap_or(0),
            workflow_execution: s.opt_struct(30)?,
            workflow_type: s.opt_struct(40)?,
            header: s.opt_struct(50)?,
        })
    }
}

impl ToThrift for ChildWorkflowExecutionStartedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        if self.initiated_event_id != 0 {
            f.fields.push((20, Value::I64(self.initiated_event_id)));
        }
        f.ostruct(30, self.workflow_execution.as_ref());
        f.ostruct(40, self.workflow_type.as_ref());
        f.ostruct(50, self.header.as_ref());
        f.finish()
    }
}

impl FromThrift for ChildWorkflowExecutionCompletedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            result: s.bytes_or_empty(10)?,
            domain: s.req_string(20, "")?,
            workflow_execution: s.opt_struct(30)?,
            workflow_type: s.opt_struct(40)?,
            initiated_event_id: s.opt_i64(50)?.unwrap_or(0),
            started_event_id: s.opt_i64(60)?.unwrap_or(0),
        })
    }
}

impl ToThrift for ChildWorkflowExecutionCompletedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.result);
        f.sstr(20, &self.domain);
        f.ostruct(30, self.workflow_execution.as_ref());
        f.ostruct(40, self.workflow_type.as_ref());
        if self.initiated_event_id != 0 {
            f.fields.push((50, Value::I64(self.initiated_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((60, Value::I64(self.started_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for ChildWorkflowExecutionFailedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            reason: s.opt_string(10)?,
            details: s.bytes_or_empty(20)?,
            domain: s.req_string(30, "")?,
            workflow_execution: s.opt_struct(40)?,
            workflow_type: s.opt_struct(50)?,
            initiated_event_id: s.opt_i64(60)?.unwrap_or(0),
            started_event_id: s.opt_i64(70)?.unwrap_or(0),
        })
    }
}

impl ToThrift for ChildWorkflowExecutionFailedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostr(10, &self.reason);
        f.sbytes(20, &self.details);
        f.sstr(30, &self.domain);
        f.ostruct(40, self.workflow_execution.as_ref());
        f.ostruct(50, self.workflow_type.as_ref());
        if self.initiated_event_id != 0 {
            f.fields.push((60, Value::I64(self.initiated_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((70, Value::I64(self.started_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for ChildWorkflowExecutionCanceledEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            details: s.bytes_or_empty(10)?,
            domain: s.req_string(20, "")?,
            workflow_execution: s.opt_struct(30)?,
            workflow_type: s.opt_struct(40)?,
            initiated_event_id: s.opt_i64(50)?.unwrap_or(0),
            started_event_id: s.opt_i64(60)?.unwrap_or(0),
        })
    }
}

impl ToThrift for ChildWorkflowExecutionCanceledEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sbytes(10, &self.details);
        f.sstr(20, &self.domain);
        f.ostruct(30, self.workflow_execution.as_ref());
        f.ostruct(40, self.workflow_type.as_ref());
        if self.initiated_event_id != 0 {
            f.fields.push((50, Value::I64(self.initiated_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((60, Value::I64(self.started_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for ChildWorkflowExecutionTerminatedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            domain: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            workflow_type: s.opt_struct(30)?,
            initiated_event_id: s.opt_i64(40)?.unwrap_or(0),
            started_event_id: s.opt_i64(50)?.unwrap_or(0),
        })
    }
}

impl ToThrift for ChildWorkflowExecutionTerminatedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain);
        f.ostruct(20, self.workflow_execution.as_ref());
        f.ostruct(30, self.workflow_type.as_ref());
        if self.initiated_event_id != 0 {
            f.fields.push((40, Value::I64(self.initiated_event_id)));
        }
        if self.started_event_id != 0 {
            f.fields.push((50, Value::I64(self.started_event_id)));
        }
        f.finish()
    }
}

impl FromThrift for SignalExternalWorkflowExecutionInitiatedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            decision_task_completed_event_id: s.opt_i64(10)?.unwrap_or(0),
            domain: s.req_string(20, "")?,
            workflow_execution: s.opt_struct(30)?,
            signal_name: s.req_string(40, "")?,
            input: s.bytes_or_empty(50)?,
            control: s.bytes_or_empty(60)?,
        })
    }
}

impl ToThrift for SignalExternalWorkflowExecutionInitiatedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((10, Value::I64(self.decision_task_completed_event_id)));
        }
        f.sstr(20, &self.domain);
        f.ostruct(30, self.workflow_execution.as_ref());
        f.sstr(40, &self.signal_name);
        f.sbytes(50, &self.input);
        f.sbytes(60, &self.control);
        f.finish()
    }
}

impl FromThrift for SignalExternalWorkflowExecutionFailedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            cause: s.opt_enum(10)?,
            decision_task_completed_event_id: s.opt_i64(20)?.unwrap_or(0),
            domain: s.req_string(30, "")?,
            workflow_execution: s.opt_struct(40)?,
            initiated_event_id: s.opt_i64(50)?.unwrap_or(0),
            control: s.bytes_or_empty(60)?,
        })
    }
}

impl ToThrift for SignalExternalWorkflowExecutionFailedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oenum(10, self.cause);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((20, Value::I64(self.decision_task_completed_event_id)));
        }
        f.sstr(30, &self.domain);
        f.ostruct(40, self.workflow_execution.as_ref());
        if self.initiated_event_id != 0 {
            f.fields.push((50, Value::I64(self.initiated_event_id)));
        }
        f.sbytes(60, &self.control);
        f.finish()
    }
}

impl FromThrift for RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            decision_task_completed_event_id: s.opt_i64(10)?.unwrap_or(0),
            domain: s.req_string(20, "")?,
            workflow_execution: s.opt_struct(30)?,
            control: s.bytes_or_empty(40)?,
            child_workflow_only: s.opt_bool(50)?.unwrap_or(false),
        })
    }
}

impl ToThrift for RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((10, Value::I64(self.decision_task_completed_event_id)));
        }
        f.sstr(20, &self.domain);
        f.ostruct(30, self.workflow_execution.as_ref());
        f.sbytes(40, &self.control);
        f.obool(50, self.child_workflow_only);
        f.finish()
    }
}

impl FromThrift for RequestCancelExternalWorkflowExecutionFailedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            cause: s.opt_enum(10)?,
            decision_task_completed_event_id: s.opt_i64(20)?.unwrap_or(0),
            domain: s.req_string(30, "")?,
            workflow_execution: s.opt_struct(40)?,
            initiated_event_id: s.opt_i64(50)?.unwrap_or(0),
            control: s.bytes_or_empty(60)?,
        })
    }
}

impl ToThrift for RequestCancelExternalWorkflowExecutionFailedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oenum(10, self.cause);
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((20, Value::I64(self.decision_task_completed_event_id)));
        }
        f.sstr(30, &self.domain);
        f.ostruct(40, self.workflow_execution.as_ref());
        if self.initiated_event_id != 0 {
            f.fields.push((50, Value::I64(self.initiated_event_id)));
        }
        f.sbytes(60, &self.control);
        f.finish()
    }
}

impl FromThrift for UpsertWorkflowSearchAttributesEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            decision_task_completed_event_id: s.opt_i64(10)?.unwrap_or(0),
            search_attributes: s.opt_struct(20)?,
        })
    }
}

impl ToThrift for UpsertWorkflowSearchAttributesEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.decision_task_completed_event_id != 0 {
            f.fields.push((10, Value::I64(self.decision_task_completed_event_id)));
        }
        f.ostruct(20, self.search_attributes.as_ref());
        f.finish()
    }
}

impl FromThrift for ExternalWorkflowExecutionSignaledEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            initiated_event_id: s.opt_i64(10)?.unwrap_or(0),
            domain: s.req_string(20, "")?,
            workflow_execution: s.opt_struct(30)?,
            control: s.bytes_or_empty(40)?,
        })
    }
}

impl ToThrift for ExternalWorkflowExecutionSignaledEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.initiated_event_id != 0 {
            f.fields.push((10, Value::I64(self.initiated_event_id)));
        }
        f.sstr(20, &self.domain);
        f.ostruct(30, self.workflow_execution.as_ref());
        f.sbytes(40, &self.control);
        f.finish()
    }
}

impl FromThrift for ExternalWorkflowExecutionCancelRequestedEventAttributes {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            initiated_event_id: s.opt_i64(10)?.unwrap_or(0),
            domain: s.req_string(20, "")?,
            workflow_execution: s.opt_struct(30)?,
        })
    }
}

impl ToThrift for ExternalWorkflowExecutionCancelRequestedEventAttributes {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.initiated_event_id != 0 {
            f.fields.push((10, Value::I64(self.initiated_event_id)));
        }
        f.sstr(20, &self.domain);
        f.ostruct(30, self.workflow_execution.as_ref());
        f.finish()
    }
}

impl FromThrift for HistoryEvent {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            event_id: s.opt_i64(10)?.unwrap_or(0),
            timestamp: s.opt_i64(20)?,
            event_type: s.opt_enum(30)?,
            version: s.opt_i64(35)?.unwrap_or(0),
            task_id: s.opt_i64(36)?.unwrap_or(0),
            workflow_execution_started_event_attributes: s.opt_struct(40)?,
            workflow_execution_completed_event_attributes: s.opt_struct(50)?,
            workflow_execution_failed_event_attributes: s.opt_struct(60)?,
            workflow_execution_signaled_event_attributes: s.opt_struct(260)?,
            workflow_execution_terminated_event_attributes: s.opt_struct(270)?,
            workflow_execution_cancel_requested_event_attributes: s.opt_struct(280)?,
            workflow_execution_canceled_event_attributes: s.opt_struct(290)?,
            decision_task_scheduled_event_attributes: s.opt_struct(80)?,
            decision_task_started_event_attributes: s.opt_struct(90)?,
            decision_task_completed_event_attributes: s.opt_struct(100)?,
            decision_task_failed_event_attributes: s.opt_struct(120)?,
            activity_task_scheduled_event_attributes: s.opt_struct(130)?,
            activity_task_started_event_attributes: s.opt_struct(140)?,
            activity_task_completed_event_attributes: s.opt_struct(150)?,
            activity_task_failed_event_attributes: s.opt_struct(160)?,
            activity_task_cancel_requested_event_attributes: s.opt_struct(200)?,
            activity_task_canceled_event_attributes: s.opt_struct(220)?,
            timer_started_event_attributes: s.opt_struct(180)?,
            timer_canceled_event_attributes: s.opt_struct(230)?,
            marker_recorded_event_attributes: s.opt_struct(250)?,
            workflow_execution_continued_as_new_event_attributes: s.opt_struct(330)?,
            start_child_workflow_execution_initiated_event_attributes: s.opt_struct(340)?,
            start_child_workflow_execution_failed_event_attributes: s.opt_struct(350)?,
            child_workflow_execution_started_event_attributes: s.opt_struct(360)?,
            child_workflow_execution_completed_event_attributes: s.opt_struct(370)?,
            child_workflow_execution_failed_event_attributes: s.opt_struct(380)?,
            child_workflow_execution_canceled_event_attributes: s.opt_struct(390)?,
            child_workflow_execution_terminated_event_attributes: s.opt_struct(410)?,
            signal_external_workflow_execution_initiated_event_attributes: s.opt_struct(420)?,
            signal_external_workflow_execution_failed_event_attributes: s.opt_struct(430)?,
            request_cancel_external_workflow_execution_initiated_event_attributes: s.opt_struct(300)?,
            request_cancel_external_workflow_execution_failed_event_attributes: s.opt_struct(310)?,
            upsert_workflow_search_attributes_event_attributes: s.opt_struct(450)?,
            timer_fired_event_attributes: s.opt_struct(190)?,
            external_workflow_execution_signaled_event_attributes: s.opt_struct(440)?,
            external_workflow_execution_cancel_requested_event_attributes: s.opt_struct(320)?,
        })
    }
}

impl ToThrift for HistoryEvent {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        if self.event_id != 0 {
            f.fields.push((10, Value::I64(self.event_id)));
        }
        f.oi64(20, millis_to_nanos(self.timestamp));
        f.oenum(30, self.event_type);
        if self.version != 0 {
            f.fields.push((35, Value::I64(self.version)));
        }
        if self.task_id != 0 {
            f.fields.push((36, Value::I64(self.task_id)));
        }
        f.ostruct(40, self.workflow_execution_started_event_attributes.as_ref());
        f.ostruct(50, self.workflow_execution_completed_event_attributes.as_ref());
        f.ostruct(60, self.workflow_execution_failed_event_attributes.as_ref());
        f.ostruct(80, self.decision_task_scheduled_event_attributes.as_ref());
        f.ostruct(90, self.decision_task_started_event_attributes.as_ref());
        f.ostruct(100, self.decision_task_completed_event_attributes.as_ref());
        f.ostruct(120, self.decision_task_failed_event_attributes.as_ref());
        f.ostruct(130, self.activity_task_scheduled_event_attributes.as_ref());
        f.ostruct(140, self.activity_task_started_event_attributes.as_ref());
        f.ostruct(150, self.activity_task_completed_event_attributes.as_ref());
        f.ostruct(160, self.activity_task_failed_event_attributes.as_ref());
        f.ostruct(180, self.timer_started_event_attributes.as_ref());
        f.ostruct(190, self.timer_fired_event_attributes.as_ref());
        f.ostruct(200, self.activity_task_cancel_requested_event_attributes.as_ref());
        f.ostruct(220, self.activity_task_canceled_event_attributes.as_ref());
        f.ostruct(230, self.timer_canceled_event_attributes.as_ref());
        f.ostruct(250, self.marker_recorded_event_attributes.as_ref());
        f.ostruct(260, self.workflow_execution_signaled_event_attributes.as_ref());
        f.ostruct(270, self.workflow_execution_terminated_event_attributes.as_ref());
        f.ostruct(280, self.workflow_execution_cancel_requested_event_attributes.as_ref());
        f.ostruct(290, self.workflow_execution_canceled_event_attributes.as_ref());
        f.ostruct(300, self.request_cancel_external_workflow_execution_initiated_event_attributes.as_ref());
        f.ostruct(310, self.request_cancel_external_workflow_execution_failed_event_attributes.as_ref());
        f.ostruct(320, self.external_workflow_execution_cancel_requested_event_attributes.as_ref());
        f.ostruct(330, self.workflow_execution_continued_as_new_event_attributes.as_ref());
        f.ostruct(340, self.start_child_workflow_execution_initiated_event_attributes.as_ref());
        f.ostruct(350, self.start_child_workflow_execution_failed_event_attributes.as_ref());
        f.ostruct(360, self.child_workflow_execution_started_event_attributes.as_ref());
        f.ostruct(370, self.child_workflow_execution_completed_event_attributes.as_ref());
        f.ostruct(380, self.child_workflow_execution_failed_event_attributes.as_ref());
        f.ostruct(390, self.child_workflow_execution_canceled_event_attributes.as_ref());
        f.ostruct(410, self.child_workflow_execution_terminated_event_attributes.as_ref());
        f.ostruct(420, self.signal_external_workflow_execution_initiated_event_attributes.as_ref());
        f.ostruct(430, self.signal_external_workflow_execution_failed_event_attributes.as_ref());
        f.ostruct(440, self.external_workflow_execution_signaled_event_attributes.as_ref());
        f.ostruct(450, self.upsert_workflow_search_attributes_event_attributes.as_ref());
        f.finish()
    }
}

// ---------------------------------------------------------------------------
// Errors (Thrift exceptions; `message` is required = always written)
// ---------------------------------------------------------------------------

macro_rules! message_exception {
    ($t:ty) => {
        impl FromThrift for $t {
            fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
                Ok(Self {
                    message: s.req_string(1, "")?,
                })
            }
        }

        impl ToThrift for $t {
            fn to_thrift(&self) -> Struct {
                let mut f = Fields::new();
                f.fields
                    .push((1, Value::Bytes(self.message.as_bytes().to_vec())));
                f.finish()
            }
        }
    };
}

message_exception!(AccessDeniedError);
message_exception!(BadRequestError);
message_exception!(CancellationAlreadyRequestedError);
message_exception!(CurrentBranchChangedError);
message_exception!(DomainAlreadyExistsError);
message_exception!(WorkflowExecutionAlreadyCompletedError);
message_exception!(InternalDataInconsistencyError);
message_exception!(InternalServiceError);
message_exception!(LimitExceededError);
message_exception!(QueryFailedError);
message_exception!(RemoteSyncMatchedError);
message_exception!(ShardOwnershipLostError);
message_exception!(EventAlreadyStartedError);
message_exception!(StickyWorkerUnavailableError);
message_exception!(ReadOnlyPartitionError);

impl FromThrift for EntityNotExistsError {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            message: s.req_string(1, "")?,
            current_cluster: s.req_string(2, "")?,
            active_cluster: s.req_string(3, "")?,
            active_clusters: s.opt_list::<String>(4)?.unwrap_or_default(),
        })
    }
}

impl ToThrift for EntityNotExistsError {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.fields
            .push((1, Value::Bytes(self.message.as_bytes().to_vec())));
        f.sstr(2, &self.current_cluster);
        f.sstr(3, &self.active_cluster);
        // Field 4 is required: always write, even when empty.
        f.fields.push((
            4,
            Value::List(
                self.active_clusters
                    .iter()
                    .map(|s| Value::Bytes(s.as_bytes().to_vec()))
                    .collect(),
            ),
        ));
        f.finish()
    }
}

impl FromThrift for DomainNotActiveError {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            message: s.req_string(1, "")?,
            domain_name: s.req_string(2, "")?,
            current_cluster: s.req_string(3, "")?,
            active_cluster: s.req_string(4, "")?,
            active_clusters: s.opt_list::<String>(5)?.unwrap_or_default(),
        })
    }
}

impl ToThrift for DomainNotActiveError {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        // All IDL fields are required: always write.
        f.fields
            .push((1, Value::Bytes(self.message.as_bytes().to_vec())));
        f.fields
            .push((2, Value::Bytes(self.domain_name.as_bytes().to_vec())));
        f.fields.push((
            3,
            Value::Bytes(self.current_cluster.as_bytes().to_vec()),
        ));
        f.fields
            .push((4, Value::Bytes(self.active_cluster.as_bytes().to_vec())));
        f.fields.push((
            5,
            Value::List(
                self.active_clusters
                    .iter()
                    .map(|s| Value::Bytes(s.as_bytes().to_vec()))
                    .collect(),
            ),
        ));
        f.finish()
    }
}

impl FromThrift for ServiceBusyError {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            message: s.req_string(1, "")?,
            reason: s.opt_string(2)?,
        })
    }
}

impl ToThrift for ServiceBusyError {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.fields
            .push((1, Value::Bytes(self.message.as_bytes().to_vec())));
        f.ostr(2, &self.reason);
        f.finish()
    }
}

impl FromThrift for ClientVersionNotSupportedError {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            feature_version: s.req_string(1, "")?,
            client_impl: s.req_string(2, "")?,
            supported_versions: s.req_string(3, "")?,
        })
    }
}

impl ToThrift for ClientVersionNotSupportedError {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.fields
            .push((1, Value::Bytes(self.feature_version.as_bytes().to_vec())));
        f.fields
            .push((2, Value::Bytes(self.client_impl.as_bytes().to_vec())));
        f.fields.push((
            3,
            Value::Bytes(self.supported_versions.as_bytes().to_vec()),
        ));
        f.finish()
    }
}

impl FromThrift for FeatureNotEnabledError {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            feature_flag: s.req_string(1, "")?,
        })
    }
}

impl ToThrift for FeatureNotEnabledError {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.fields
            .push((1, Value::Bytes(self.feature_flag.as_bytes().to_vec())));
        f.finish()
    }
}

impl FromThrift for WorkflowExecutionAlreadyStartedError {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            message: s.req_string(10, "")?,
            start_request_id: s.req_string(20, "")?,
            run_id: s.req_string(30, "")?,
        })
    }
}

impl ToThrift for WorkflowExecutionAlreadyStartedError {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.message);
        f.sstr(20, &self.start_request_id);
        f.sstr(30, &self.run_id);
        f.finish()
    }
}

impl FromThrift for RetryTaskV2Error {
    fn from_thrift(s: &Struct) -> Result<Self, crate::protocol::Error> {
        Ok(Self {
            message: s.req_string(1, "")?,
            domain_id: s.req_string(2, "")?,
            workflow_id: s.req_string(3, "")?,
            run_id: s.req_string(4, "")?,
            start_event_id: s.opt_i64(5)?,
            start_event_version: s.opt_i64(6)?,
            end_event_id: s.opt_i64(7)?,
            end_event_version: s.opt_i64(8)?,
        })
    }
}

impl ToThrift for RetryTaskV2Error {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.fields
            .push((1, Value::Bytes(self.message.as_bytes().to_vec())));
        f.sstr(2, &self.domain_id);
        f.sstr(3, &self.workflow_id);
        f.sstr(4, &self.run_id);
        f.oi64(5, self.start_event_id);
        f.oi64(6, self.start_event_version);
        f.oi64(7, self.end_event_id);
        f.oi64(8, self.end_event_version);
        f.finish()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T>(v: &T) -> T
    where
        T: ToThrift + FromThrift + PartialEq + std::fmt::Debug,
    {
        let s = v.to_thrift();
        let mut w = crate::protocol::Writer::new();
        crate::value::write_struct(&mut w, &s);
        let bytes = w.into_bytes();
        let mut r = crate::protocol::Reader::new(&bytes);
        let back = crate::value::read_struct(&mut r).unwrap();
        assert_eq!(r.remaining(), 0);
        let out = T::from_thrift(&back).unwrap();
        assert_eq!(&out, v);
        out
    }

    #[test]
    fn primitives_round_trip() {
        round_trip(&WorkflowExecution::new("wf-1", "run-1"));
        round_trip(&TaskList {
            name: "tl".to_string(),
            kind: Some(TaskListKind::Sticky),
            base_name: "base".to_string(),
        });
        round_trip(&DataBlob::new(EncodingType::JSON, b"{}".to_vec()));
        round_trip(&RetryPolicy {
            initial_interval_in_seconds: 1,
            backoff_coefficient: 2.0,
            maximum_attempts: 3,
            non_retriable_error_reasons: vec!["x".to_string()],
            ..Default::default()
        });
    }

    #[test]
    fn requests_round_trip() {
        round_trip(&StartWorkflowExecutionRequest {
            domain: "d".to_string(),
            workflow_id: "w".to_string(),
            workflow_type: Some(WorkflowType { name: "wt".to_string() }),
            request_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            workflow_id_reuse_policy: Some(WorkflowIDReusePolicy::RejectDuplicate),
            ..Default::default()
        });
        round_trip(&SignalWorkflowExecutionRequest {
            domain: "d".to_string(),
            workflow_execution: Some(WorkflowExecution::new("w", "r")),
            signal_name: "s".to_string(),
            request_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            ..Default::default()
        });
        let attrs = ScheduleActivityTaskDecisionAttributes {
            activity_id: "activity-1".to_string(),
            activity_type: Some(ActivityType { name: "hello".to_string() }),
            domain: "domain-1".to_string(),
            task_list: Some(TaskList { name: "task-list-1".to_string(), ..Default::default() }),
            input: b"input".to_vec(),
            schedule_to_close_timeout_seconds: Some(90),
            schedule_to_start_timeout_seconds: Some(30),
            start_to_close_timeout_seconds: Some(60),
            heartbeat_timeout_seconds: Some(10),
            ..Default::default()
        };
        let wire = attrs.to_thrift();
        assert!(matches!(wire.get(30), Some(Value::Struct(_))));
        assert!(matches!(wire.get(25), Some(Value::Bytes(_))));
        assert_eq!(ScheduleActivityTaskDecisionAttributes::from_thrift(&wire).unwrap(), attrs);
    }

    #[test]
    fn history_event_round_trip() {
        let e = HistoryEvent {
            event_id: 1,
            event_type: Some(EventType::WorkflowExecutionStarted),
            version: 5,
            workflow_execution_started_event_attributes: Some(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: Some(WorkflowType { name: "wt".to_string() }),
                    identity: "id".to_string(),
                    request_id: "r".to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        round_trip(&e);

        let timestamp = HistoryEvent {
            timestamp: Some(1_700_000_000_123),
            ..Default::default()
        }
        .to_thrift();
        assert_eq!(timestamp.opt_i64(20).unwrap(), Some(1_700_000_000_123_000_000));
    }

    #[test]
    fn errors_round_trip() {
        round_trip(&BadRequestError::new("bad"));
        round_trip(&WorkflowExecutionAlreadyStartedError {
            message: "m".to_string(),
            start_request_id: "s".to_string(),
            run_id: "r".to_string(),
        });
        round_trip(&DomainNotActiveError {
            message: "m".to_string(),
            domain_name: "d".to_string(),
            current_cluster: "c1".to_string(),
            active_cluster: "c2".to_string(),
            active_clusters: vec!["c2".to_string()],
        });
        round_trip(&FailureOptions {
            failure_category: Some(FailureCategory::Poll),
            next_retry_interval_seconds: Some(3),
        });
    }
}
