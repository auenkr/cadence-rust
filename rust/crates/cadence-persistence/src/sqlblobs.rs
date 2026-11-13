//! ThriftRW codec shared by History's SQL `sqlblobs` values.
//!
//! `sqlblobs.thrift` structs are encoded with the binary protocol without a
//! Thrift message envelope or version byte. The column's `data_encoding`
//! carries the codec choice; `thriftrw_snappy` is a raw Snappy block around
//! the exact same struct bytes. Struct-specific field mapping belongs at the
//! contract boundary, while this module owns the byte-compatible wire layer.

use crate::history_contract::{
    ActivityInfo, ChildExecutionInfo, CrossClusterTask, HistoryBranchRange, HistoryTreeInfo,
    ReplicationTask, RequestCancelInfo, SignalInfo, SqlWorkflowExecutionInfo, TimerInfo, TimerTask,
    TransferTask,
};
use cadence_thrift::Value;
use cadence_thrift::{
    value::{read_struct, write_value, Struct},
    Error, Reader, Result, TType, Writer,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    ThriftRW,
    ThriftRWSnappy,
}

impl Encoding {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "thriftrw" => Ok(Self::ThriftRW),
            "thriftrw_snappy" => Ok(Self::ThriftRWSnappy),
            _ => Err(Error::Invalid(format!("invalid encoding type: {value}"))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ThriftRW => "thriftrw",
            Self::ThriftRWSnappy => "thriftrw_snappy",
        }
    }

    pub fn encode(self, value: &Struct) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        let mut fields: Vec<_> = value.fields.iter().collect();
        fields.sort_by_key(|(id, _)| *id);
        for (id, field) in fields {
            let ty = value_type(field);
            writer.write_field_begin(ty, *id);
            write_value(&mut writer, field);
        }
        writer.write_field_stop();
        let bytes = writer.into_bytes();
        match self {
            Self::ThriftRW => Ok(bytes),
            Self::ThriftRWSnappy => snap::raw::Encoder::new()
                .compress_vec(&bytes)
                .map_err(|error| Error::Invalid(format!("snappy encode: {error}"))),
        }
    }

    pub fn decode(self, data: &[u8]) -> Result<Struct> {
        let uncompressed;
        let bytes = match self {
            Self::ThriftRW => data,
            Self::ThriftRWSnappy => {
                uncompressed = snap::raw::Decoder::new()
                    .decompress_vec(data)
                    .map_err(|error| Error::Invalid(format!("snappy decode: {error}")))?;
                &uncompressed
            }
        };
        let mut reader = Reader::new(bytes);
        let value = read_struct(&mut reader)?;
        if reader.remaining() != 0 {
            return Err(Error::Invalid(
                "trailing bytes after sqlblobs struct".into(),
            ));
        }
        Ok(value)
    }
}

fn value_type(value: &cadence_thrift::Value) -> TType {
    use cadence_thrift::Value;

    /// types.QueueState keeps queue ids, slice ranges and predicates through the thrift shard blob (Go context.go UpdateQueueState).
    #[test]
    fn shard_queue_state_roundtrip_is_lossless() {
        let mut rec = ShardInfoRecord::default();
        let mut q = ShardQueueStateRecord { exclusive_max_read_level: (9, 100), ..Default::default() };
        q.virtual_queue_states.insert(0, vec![
            ShardVirtualSliceRecord { inclusive_min: (1, 10), exclusive_max: (2, 20), predicate: Some(ShardPredicateRecord::Universal) },
            ShardVirtualSliceRecord { inclusive_min: (3, 30), exclusive_max: (4, 40), predicate: Some(ShardPredicateRecord::Empty) },
        ]);
        q.virtual_queue_states.insert(5, vec![ShardVirtualSliceRecord {
            inclusive_min: (5, 50), exclusive_max: (6, 60),
            predicate: Some(ShardPredicateRecord::DomainId { domain_ids: vec!["a".into(), "b".into()], is_exclusive: Some(true) }),
        }]);
        q.virtual_queue_states.insert(7, vec![]);
        rec.queue_states.insert(3, q);
        let back = ShardInfoRecord::decode(&rec.encode(Encoding::ThriftRW).unwrap(), Encoding::ThriftRW).unwrap();
        assert_eq!(back.queue_states, rec.queue_states);
    }
    match value {
        Value::Bool(_) => TType::Bool,
        Value::I8(_) => TType::Byte,
        Value::I16(_) => TType::I16,
        Value::I32(_) => TType::I32,
        Value::I64(_) => TType::I64,
        Value::F64(_) => TType::Double,
        Value::Bytes(_) => TType::String,
        Value::List(_) => TType::List,
        Value::Map(_) => TType::Map,
        Value::Set(_) => TType::Set,
        Value::Struct(_) => TType::Struct,
    }
}

/// Names of the persisted SQL structs handled by the History serializer.
pub const HISTORY_STRUCTS: &[&str] = &[
    "ShardInfo",
    "WorkflowExecutionInfo",
    "ActivityInfo",
    "ChildExecutionInfo",
    "SignalInfo",
    "RequestCancelInfo",
    "TimerInfo",
    "TransferTaskInfo",
    "TimerTaskInfo",
    "ReplicationTaskInfo",
    "CrossClusterTaskInfo",
    "HistoryTreeInfo",
    "HistoryBranchRange",
];

fn uuid_to_bytes(value: &str, field: &str) -> Result<Vec<u8>> {
    uuid::Uuid::parse_str(value)
        .map(|id| id.as_bytes().to_vec())
        .map_err(|error| Error::Invalid(format!("{field}: invalid UUID {value:?}: {error}")))
}

fn uuid_from_bytes(value: &[u8], field: &str) -> Result<String> {
    uuid::Uuid::from_slice(value)
        .map(|id| id.to_string())
        .map_err(|error| Error::Invalid(format!("{field}: invalid UUID bytes: {error}")))
}

fn string_field(value: &str) -> Value {
    Value::Bytes(value.as_bytes().to_vec())
}

fn i64_field(value: i64) -> Value {
    Value::I64(value)
}

/// Map SQL ActivityInfo's seconds and unix-nanos fields to its typed contract.
pub fn activity_info_to_struct(info: &ActivityInfo) -> Struct {
    use Value::*;
    let bytes = |v: &str| Bytes(v.as_bytes().to_vec());
    let blob = |v: &[u8]| Bytes(v.to_vec());
    cadence_thrift::Struct {
        fields: vec![
            (10, I64(info.version)),
            (12, I64(info.scheduled_event_batch_id)),
            (14, blob(&info.scheduled_event)),
            (16, bytes(&info.scheduled_event_encoding)),
            (18, I64(info.scheduled_time_unix_nanos)),
            (20, I64(info.started_id)),
            (22, blob(&info.started_event)),
            (24, bytes(&info.started_event_encoding)),
            (26, I64(info.started_time_unix_nanos)),
            (28, bytes(&info.activity_id)),
            (30, bytes(&info.request_id)),
            (32, I32(info.schedule_to_start_timeout_seconds)),
            (34, I32(info.schedule_to_close_timeout_seconds)),
            (36, I32(info.start_to_close_timeout_seconds)),
            (38, I32(info.heartbeat_timeout_seconds)),
            (40, Bool(info.cancel_requested)),
            (42, I64(info.cancel_request_id)),
            (44, I32(info.timer_task_status)),
            (46, I32(info.attempt)),
            (48, bytes(&info.task_list)),
            (49, I32(info.task_list_kind)),
            (50, bytes(&info.started_identity)),
            (52, Bool(info.has_retry_policy)),
            (54, I32(info.retry_initial_interval_seconds)),
            (56, I32(info.retry_maximum_interval_seconds)),
            (58, I32(info.retry_maximum_attempts)),
            (60, I64(info.retry_expiration_time_unix_nanos)),
            (62, F64(info.retry_backoff_coefficient)),
            (
                64,
                List(
                    info.retry_non_retriable_errors
                        .iter()
                        .map(|s| bytes(s))
                        .collect(),
                ),
            ),
            (66, bytes(&info.retry_last_failure_reason)),
            (68, bytes(&info.retry_last_worker_identity)),
            (70, blob(&info.retry_last_failure_details)),
            (
                72,
                Struct(cadence_thrift::Struct {
                    fields: vec![
                        (10, I32(info.retry_last_failure_category)),
                        (20, I32(info.retry_last_retry_interval_seconds)),
                    ],
                }),
            ),
            (74, F64(info.retry_jitter_coefficient)),
        ],
    }
}

pub fn activity_info_from_struct(value: &Struct) -> Result<ActivityInfo> {
    use Value::*;
    let i64v = |id| match value.get(id) {
        Some(I64(v)) => Ok(*v),
        Some(I32(v)) => Ok(i64::from(*v)),
        None => Ok(0),
        Some(v) => Err(Error::Invalid(format!(
            "field {id}: expected integer, got {v:?}"
        ))),
    };
    let i32v = |id| {
        i64v(id).and_then(|v| {
            i32::try_from(v)
                .map_err(|_| Error::Invalid(format!("field {id}: integer out of range")))
        })
    };
    let boolv = |id| match value.get(id) {
        Some(Bool(v)) => Ok(*v),
        None => Ok(false),
        Some(v) => Err(Error::Invalid(format!(
            "field {id}: expected bool, got {v:?}"
        ))),
    };
    let strv = |id| match value.get(id) {
        Some(Bytes(v)) => String::from_utf8(v.clone()).map_err(Error::from),
        None => Ok(String::new()),
        Some(v) => Err(Error::Invalid(format!(
            "field {id}: expected string, got {v:?}"
        ))),
    };
    let blob = |id| match value.get(id) {
        Some(Bytes(v)) => Ok(v.clone()),
        None => Ok(Vec::new()),
        Some(v) => Err(Error::Invalid(format!(
            "field {id}: expected binary, got {v:?}"
        ))),
    };
    let f64v = |id| match value.get(id) {
        Some(F64(v)) => Ok(*v),
        None => Ok(0.0),
        Some(v) => Err(Error::Invalid(format!(
            "field {id}: expected double, got {v:?}"
        ))),
    };
    let retry_non_retriable_errors = match value.get(64) {
        Some(List(xs)) => xs
            .iter()
            .map(|x| match x {
                Bytes(b) => String::from_utf8(b.clone()).map_err(Error::from),
                v => Err(Error::Invalid(format!("retryNonRetryableErrors: {v:?}"))),
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
        Some(v) => return Err(Error::Invalid(format!("retryNonRetryableErrors: {v:?}"))),
    };
    Ok(ActivityInfo {
        version: i64v(10)?,
        scheduled_event_batch_id: i64v(12)?,
        scheduled_event: blob(14)?,
        scheduled_event_encoding: strv(16)?,
        scheduled_time_unix_nanos: i64v(18)?,
        started_id: i64v(20)?,
        started_event: blob(22)?,
        started_event_encoding: strv(24)?,
        started_time_unix_nanos: i64v(26)?,
        activity_id: strv(28)?,
        request_id: strv(30)?,
        schedule_to_start_timeout_seconds: i32v(32)?,
        schedule_to_close_timeout_seconds: i32v(34)?,
        start_to_close_timeout_seconds: i32v(36)?,
        heartbeat_timeout_seconds: i32v(38)?,
        cancel_requested: boolv(40)?,
        cancel_request_id: i64v(42)?,
        timer_task_status: i32v(44)?,
        // Go keeps this deduplication hint only in memory, never in sqlblobs.
        last_heartbeat_timeout_visibility_in_seconds: 0,
        attempt: i32v(46)?,
        task_list: strv(48)?,
        task_list_kind: i32v(49)?,
        started_identity: strv(50)?,
        has_retry_policy: boolv(52)?,
        retry_initial_interval_seconds: i32v(54)?,
        retry_maximum_interval_seconds: i32v(56)?,
        retry_maximum_attempts: i32v(58)?,
        retry_expiration_time_unix_nanos: i64v(60)?,
        retry_backoff_coefficient: f64v(62)?,
        retry_non_retriable_errors,
        retry_last_failure_reason: strv(66)?,
        retry_last_worker_identity: strv(68)?,
        retry_last_failure_details: blob(70)?,
        retry_last_failure_category: match value.get(72) {
            Some(Struct(options)) => contract_i32(options, 10)?,
            None => 0,
            Some(v) => return Err(Error::Invalid(format!("retryLastFailureOptions: {v:?}"))),
        },
        retry_last_retry_interval_seconds: match value.get(72) {
            Some(Struct(options)) => contract_i32(options, 20)?,
            None => 0,
            Some(v) => return Err(Error::Invalid(format!("retryLastFailureOptions: {v:?}"))),
        },
        retry_jitter_coefficient: f64v(74)?,
    })
}

fn contract_i64(value: &Struct, id: i16) -> Result<i64> {
    match value.get(id) {
        Some(Value::I64(v)) => Ok(*v),
        Some(Value::I32(v)) => Ok(i64::from(*v)),
        None => Ok(0),
        Some(v) => Err(Error::Invalid(format!(
            "field {id}: expected integer, got {v:?}"
        ))),
    }
}
fn contract_i32(value: &Struct, id: i16) -> Result<i32> {
    i32::try_from(contract_i64(value, id)?)
        .map_err(|_| Error::Invalid(format!("field {id}: integer out of range")))
}
fn contract_bytes(value: &Struct, id: i16) -> Result<Vec<u8>> {
    match value.get(id) {
        Some(Value::Bytes(v)) => Ok(v.clone()),
        None => Ok(Vec::new()),
        Some(v) => Err(Error::Invalid(format!(
            "field {id}: expected bytes, got {v:?}"
        ))),
    }
}
fn contract_string(value: &Struct, id: i16) -> Result<String> {
    String::from_utf8(contract_bytes(value, id)?).map_err(Error::from)
}

pub fn child_execution_info_to_struct(x: &ChildExecutionInfo) -> Struct {
    use Value::*;
    cadence_thrift::Struct {
        fields: vec![
            (10, I64(x.version)),
            (12, I64(x.initiated_event_batch_id)),
            (14, I64(x.started_id)),
            (16, Bytes(x.initiated_event.clone())),
            (18, string_field(&x.initiated_event_encoding)),
            (20, string_field(&x.started_workflow_id)),
            (22, Bytes(x.started_run_id.clone())),
            (24, Bytes(x.started_event.clone())),
            (26, string_field(&x.started_event_encoding)),
            (28, string_field(&x.create_request_id)),
            (29, string_field(&x.domain_id)),
            (30, string_field(&x.domain_name)),
            (32, string_field(&x.workflow_type_name)),
            (35, I32(x.parent_close_policy)),
        ],
    }
}
pub fn child_execution_info_from_struct(v: &Struct) -> Result<ChildExecutionInfo> {
    Ok(ChildExecutionInfo {
        version: contract_i64(v, 10)?,
        initiated_event_batch_id: contract_i64(v, 12)?,
        started_id: contract_i64(v, 14)?,
        initiated_event: contract_bytes(v, 16)?,
        initiated_event_encoding: contract_string(v, 18)?,
        started_workflow_id: contract_string(v, 20)?,
        started_run_id: contract_bytes(v, 22)?,
        started_event: contract_bytes(v, 24)?,
        started_event_encoding: contract_string(v, 26)?,
        create_request_id: contract_string(v, 28)?,
        domain_id: contract_string(v, 29)?,
        domain_name: contract_string(v, 30)?,
        workflow_type_name: contract_string(v, 32)?,
        parent_close_policy: contract_i32(v, 35)?,
    })
}
pub fn signal_info_to_struct(x: &SignalInfo) -> Struct {
    use Value::*;
    cadence_thrift::Struct {
        fields: vec![
            (10, I64(x.version)),
            (11, I64(x.initiated_event_batch_id)),
            (12, string_field(&x.request_id)),
            (14, string_field(&x.name)),
            (16, Bytes(x.input.clone())),
            (18, Bytes(x.control.clone())),
        ],
    }
}
pub fn signal_info_from_struct(v: &Struct) -> Result<SignalInfo> {
    Ok(SignalInfo {
        version: contract_i64(v, 10)?,
        initiated_event_batch_id: contract_i64(v, 11)?,
        request_id: contract_string(v, 12)?,
        name: contract_string(v, 14)?,
        input: contract_bytes(v, 16)?,
        control: contract_bytes(v, 18)?,
    })
}
pub fn request_cancel_info_to_struct(x: &RequestCancelInfo) -> Struct {
    use Value::*;
    cadence_thrift::Struct {
        fields: vec![
            (10, I64(x.version)),
            (11, I64(x.initiated_event_batch_id)),
            (12, string_field(&x.cancel_request_id)),
        ],
    }
}
pub fn request_cancel_info_from_struct(v: &Struct) -> Result<RequestCancelInfo> {
    Ok(RequestCancelInfo {
        version: contract_i64(v, 10)?,
        initiated_event_batch_id: contract_i64(v, 11)?,
        cancel_request_id: contract_string(v, 12)?,
    })
}
pub fn timer_info_to_struct(x: &TimerInfo) -> Struct {
    use Value::*;
    cadence_thrift::Struct {
        fields: vec![
            (10, I64(x.version)),
            (12, I64(x.started_id)),
            (14, I64(x.expiry_time_unix_nanos)),
            (16, I64(x.task_id)),
        ],
    }
}
pub fn timer_info_from_struct(v: &Struct) -> Result<TimerInfo> {
    Ok(TimerInfo {
        version: contract_i64(v, 10)?,
        started_id: contract_i64(v, 12)?,
        expiry_time_unix_nanos: contract_i64(v, 14)?,
        task_id: contract_i64(v, 16)?,
    })
}

pub fn sql_workflow_execution_info_to_struct(x: &SqlWorkflowExecutionInfo) -> Struct {
    use Value::*;
    let text = |s: &str| Bytes(s.as_bytes().to_vec());
    let data = |b: &[u8]| Bytes(b.to_vec());
    let strings = |xs: &[String]| List(xs.iter().map(|s| text(s)).collect());
    let blobmap = |m: &std::collections::BTreeMap<String, Vec<u8>>| {
        Map(m.iter().map(|(k, v)| (text(k), data(v))).collect())
    };
    let stringmap = |m: &std::collections::BTreeMap<String, String>| {
        Map(m.iter().map(|(k, v)| (text(k), text(v))).collect())
    };
    cadence_thrift::Struct {
        fields: vec![
            (10, data(&x.parent_domain_id)),
            (12, text(&x.parent_workflow_id)),
            (14, data(&x.parent_run_id)),
            (16, I64(x.initiated_id)),
            (18, I64(x.completion_event_batch_id)),
            (20, data(&x.completion_event)),
            (22, text(&x.completion_event_encoding)),
            (24, text(&x.task_list)),
            (25, I32(x.task_list_kind)),
            (26, text(&x.workflow_type_name)),
            (28, I32(x.workflow_timeout_seconds)),
            (30, I32(x.decision_task_timeout_seconds)),
            (32, data(&x.execution_context)),
            (34, I32(x.state)),
            (36, I32(x.close_status)),
            (38, I64(x.start_version)),
            (44, I64(x.last_write_event_id)),
            (48, I64(x.last_event_task_id)),
            (50, I64(x.last_first_event_id)),
            (52, I64(x.last_processed_event)),
            (54, I64(x.start_time_unix_nanos)),
            (56, I64(x.last_updated_time_unix_nanos)),
            (58, I64(x.decision_version)),
            (60, I64(x.decision_schedule_id)),
            (62, I64(x.decision_started_id)),
            (64, I32(x.decision_timeout_seconds)),
            (66, I64(x.decision_attempt)),
            (68, I64(x.decision_started_timestamp_unix_nanos)),
            (69, I64(x.decision_scheduled_timestamp_unix_nanos)),
            (70, Bool(x.cancel_requested)),
            (71, I64(x.decision_original_scheduled_timestamp_unix_nanos)),
            (72, text(&x.create_request_id)),
            (74, text(&x.decision_request_id)),
            (76, text(&x.cancel_request_id)),
            (78, text(&x.sticky_task_list)),
            (80, I64(x.sticky_schedule_to_start_timeout_seconds)),
            (82, I64(x.retry_attempt)),
            (84, I32(x.retry_initial_interval_seconds)),
            (86, I32(x.retry_maximum_interval_seconds)),
            (88, I32(x.retry_maximum_attempts)),
            (90, I32(x.retry_expiration_seconds)),
            (92, F64(x.retry_backoff_coefficient)),
            (93, F64(x.retry_jitter_coefficient)),
            (94, I64(x.retry_expiration_time_unix_nanos)),
            (96, strings(&x.retry_non_retryable_errors)),
            (98, Bool(x.has_retry_policy)),
            (100, text(&x.cron_schedule)),
            (102, I32(x.event_store_version)),
            (104, data(&x.event_branch_token)),
            (106, I64(x.signal_count)),
            (108, I64(x.history_size)),
            (110, text(&x.client_library_version)),
            (112, text(&x.client_feature_version)),
            (114, text(&x.client_impl)),
            (115, data(&x.auto_reset_points)),
            (116, text(&x.auto_reset_points_encoding)),
            (118, blobmap(&x.search_attributes)),
            (120, blobmap(&x.memo)),
            (122, data(&x.version_histories)),
            (124, text(&x.version_histories_encoding)),
            (126, data(&x.first_execution_run_id)),
            (128, stringmap(&x.partition_config)),
            (130, data(&x.checksum)),
            (132, text(&x.checksum_encoding)),
            (134, I32(x.cron_overlap_policy)),
            (137, data(&x.active_cluster_selection_policy)),
            (138, text(&x.active_cluster_selection_policy_encoding)),
        ],
    }
}

// Decoding is intentionally strict about each Go-declared field's wire type.
pub fn sql_workflow_execution_info_from_struct(v: &Struct) -> Result<SqlWorkflowExecutionInfo> {
    use Value::*;
    let i64v = |id| contract_i64(v, id);
    let i32v = |id| contract_i32(v, id);
    let bytes = |id| contract_bytes(v, id);
    let string = |id| contract_string(v, id);
    let boolv = |id| match v.get(id) {
        Some(Bool(x)) => Ok(*x),
        None => Ok(false),
        Some(x) => Err(Error::Invalid(format!(
            "field {id}: expected bool, got {x:?}"
        ))),
    };
    let f64v = |id| match v.get(id) {
        Some(F64(x)) => Ok(*x),
        None => Ok(0.),
        Some(x) => Err(Error::Invalid(format!(
            "field {id}: expected double, got {x:?}"
        ))),
    };
    let list = match v.get(96) {
        Some(List(xs)) => xs
            .iter()
            .map(|x| match x {
                Bytes(b) => String::from_utf8(b.clone()).map_err(Error::from),
                x => Err(Error::Invalid(format!("retryNonRetryableErrors: {x:?}"))),
            })
            .collect::<Result<Vec<_>>>()?,
        None => vec![],
        Some(x) => return Err(Error::Invalid(format!("retryNonRetryableErrors: {x:?}"))),
    };
    let blobmap = |id| -> Result<std::collections::BTreeMap<String, Vec<u8>>> {
        match v.get(id) {
            Some(Map(m)) => m
                .iter()
                .map(|(k, val)| match (k, val) {
                    (Bytes(k), Bytes(b)) => Ok((String::from_utf8(k.clone())?, b.clone())),
                    _ => Err(Error::Invalid(format!(
                        "field {id}: expected map<string,binary>"
                    ))),
                })
                .collect(),
            None => Ok(Default::default()),
            Some(x) => Err(Error::Invalid(format!(
                "field {id}: expected map, got {x:?}"
            ))),
        }
    };
    let partition_config = match v.get(128) {
        Some(Map(m)) => m
            .iter()
            .map(|(k, val)| match (k, val) {
                (Bytes(k), Bytes(b)) => {
                    Ok((String::from_utf8(k.clone())?, String::from_utf8(b.clone())?))
                }
                _ => Err(Error::Invalid(
                    "partitionConfig: expected map<string,string>".into(),
                )),
            })
            .collect::<Result<_>>()?,
        None => Default::default(),
        Some(x) => return Err(Error::Invalid(format!("partitionConfig: {x:?}"))),
    };
    Ok(SqlWorkflowExecutionInfo {
        parent_domain_id: bytes(10)?,
        parent_workflow_id: string(12)?,
        parent_run_id: bytes(14)?,
        initiated_id: i64v(16)?,
        completion_event_batch_id: i64v(18)?,
        completion_event: bytes(20)?,
        completion_event_encoding: string(22)?,
        task_list: string(24)?,
        task_list_kind: i32v(25)?,
        workflow_type_name: string(26)?,
        workflow_timeout_seconds: i32v(28)?,
        decision_task_timeout_seconds: i32v(30)?,
        execution_context: bytes(32)?,
        state: i32v(34)?,
        close_status: i32v(36)?,
        start_version: i64v(38)?,
        last_write_event_id: i64v(44)?,
        last_event_task_id: i64v(48)?,
        last_first_event_id: i64v(50)?,
        last_processed_event: i64v(52)?,
        start_time_unix_nanos: i64v(54)?,
        last_updated_time_unix_nanos: i64v(56)?,
        decision_version: i64v(58)?,
        decision_schedule_id: i64v(60)?,
        decision_started_id: i64v(62)?,
        decision_timeout_seconds: i32v(64)?,
        decision_attempt: i64v(66)?,
        decision_started_timestamp_unix_nanos: i64v(68)?,
        decision_scheduled_timestamp_unix_nanos: i64v(69)?,
        cancel_requested: boolv(70)?,
        decision_original_scheduled_timestamp_unix_nanos: i64v(71)?,
        create_request_id: string(72)?,
        decision_request_id: string(74)?,
        cancel_request_id: string(76)?,
        sticky_task_list: string(78)?,
        sticky_schedule_to_start_timeout_seconds: i64v(80)?,
        retry_attempt: i64v(82)?,
        retry_initial_interval_seconds: i32v(84)?,
        retry_maximum_interval_seconds: i32v(86)?,
        retry_maximum_attempts: i32v(88)?,
        retry_expiration_seconds: i32v(90)?,
        retry_backoff_coefficient: f64v(92)?,
        retry_jitter_coefficient: f64v(93)?,
        retry_expiration_time_unix_nanos: i64v(94)?,
        retry_non_retryable_errors: list,
        has_retry_policy: boolv(98)?,
        cron_schedule: string(100)?,
        event_store_version: i32v(102)?,
        event_branch_token: bytes(104)?,
        signal_count: i64v(106)?,
        history_size: i64v(108)?,
        client_library_version: string(110)?,
        client_feature_version: string(112)?,
        client_impl: string(114)?,
        auto_reset_points: bytes(115)?,
        auto_reset_points_encoding: string(116)?,
        search_attributes: blobmap(118)?,
        memo: blobmap(120)?,
        version_histories: bytes(122)?,
        version_histories_encoding: string(124)?,
        first_execution_run_id: bytes(126)?,
        partition_config,
        checksum: bytes(130)?,
        checksum_encoding: string(132)?,
        cron_overlap_policy: i32v(134)?,
        active_cluster_selection_policy: bytes(137)?,
        active_cluster_selection_policy_encoding: string(138)?,
    })
}

/// Convert the History task contract to Go `sqlblobs.TransferTaskInfo`.
pub fn transfer_task_to_struct(task: &TransferTask) -> Result<Struct> {
    let value = Struct {
        fields: vec![
            (
                10,
                Value::Bytes(uuid_to_bytes(&task.domain_id, "domainID")?),
            ),
            (12, string_field(&task.workflow_id)),
            (14, Value::Bytes(uuid_to_bytes(&task.run_id, "runID")?)),
            (16, Value::I16(task.task_type as i16)),
            (
                18,
                Value::Bytes(uuid_to_bytes(&task.target_domain_id, "targetDomainID")?),
            ),
            (20, string_field(&task.target_workflow_id)),
            (
                22,
                Value::Bytes(uuid_to_bytes(&task.target_run_id, "targetRunID")?),
            ),
            (24, string_field(&task.task_list)),
            (26, Value::Bool(task.target_child_workflow_only)),
            (28, i64_field(task.schedule_id)),
            (30, i64_field(task.version)),
            (32, i64_field(task.visibility_time_unix_nanos)),
            (
                34,
                Value::Set(
                    task.target_domain_ids
                        .iter()
                        .map(|id| uuid_to_bytes(id, "targetDomainIDs").map(Value::Bytes))
                        .collect::<Result<Vec<_>>>()?,
                ),
            ),
            (36, string_field(&task.original_task_list)),
            (38, Value::I32(task.original_task_list_kind)),
        ],
    };
    Ok(value)
}

pub fn transfer_task_from_struct(value: &Struct) -> Result<TransferTask> {
    let get_uuid = |id, field| match value.get(id) {
        Some(Value::Bytes(bytes)) => uuid_from_bytes(bytes, field),
        None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!(
            "{field}: expected binary, got {other:?}"
        ))),
    };
    let get_string = |id, field| match value.get(id) {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone()).map_err(Error::from),
        None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!(
            "{field}: expected string, got {other:?}"
        ))),
    };
    let get_i64 = |id| match value.get(id) {
        Some(Value::I64(v)) => Ok(*v),
        Some(Value::I32(v)) => Ok(i64::from(*v)),
        None => Ok(0),
        Some(other) => Err(Error::Invalid(format!("expected i64, got {other:?}"))),
    };
    let target_domain_ids = match value.get(34) {
        Some(Value::Set(values)) => values
            .iter()
            .map(|v| match v {
                Value::Bytes(bytes) => uuid_from_bytes(bytes, "targetDomainIDs"),
                other => Err(Error::Invalid(format!(
                    "targetDomainIDs: expected binary, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
        Some(other) => {
            return Err(Error::Invalid(format!(
                "targetDomainIDs: expected set, got {other:?}"
            )))
        }
    };
    let task_type = match value.get(16) {
        Some(Value::I16(v)) => i32::from(*v),
        None => 0,
        Some(other) => {
            return Err(Error::Invalid(format!(
                "taskType: expected i16, got {other:?}"
            )))
        }
    };
    let original_task_list_kind = match value.get(38) {
        Some(Value::I32(v)) => *v,
        None => 0,
        Some(other) => {
            return Err(Error::Invalid(format!(
                "originalTaskListKind: expected enum, got {other:?}"
            )))
        }
    };
    let target_child_workflow_only = match value.get(26) {
        Some(Value::Bool(v)) => *v,
        None => false,
        Some(other) => {
            return Err(Error::Invalid(format!(
                "targetChildWorkflowOnly: expected bool, got {other:?}"
            )))
        }
    };
    Ok(TransferTask {
        domain_id: get_uuid(10, "domainID")?,
        workflow_id: get_string(12, "workflowID")?,
        run_id: get_uuid(14, "runID")?,
        visibility_time_unix_nanos: get_i64(32)?,
        task_id: 0,
        target_domain_id: get_uuid(18, "targetDomainID")?,
        target_domain_ids,
        target_workflow_id: get_string(20, "targetWorkflowID")?,
        target_run_id: get_uuid(22, "targetRunID")?,
        target_child_workflow_only,
        task_list: get_string(24, "taskList")?,
        task_type,
        schedule_id: get_i64(28)?,
        version: get_i64(30)?,
        record_visibility: false,
        original_task_list: get_string(36, "originalTaskList")?,
        original_task_list_kind,
    })
}

/// Cross-cluster task payload is the same sqlblobs struct as a transfer task;
/// `target_cluster` belongs to the SQL primary key, not its encoded payload.
pub fn cross_cluster_task_to_struct(task: &CrossClusterTask) -> Result<Struct> {
    transfer_task_to_struct(&task.transfer_task)
}

pub fn cross_cluster_task_from_struct(value: &Struct) -> Result<CrossClusterTask> {
    Ok(CrossClusterTask {
        transfer_task: transfer_task_from_struct(value)?,
        target_cluster: String::new(),
    })
}

pub fn history_branch_range_to_struct(range: &HistoryBranchRange) -> Struct {
    Struct {
        fields: vec![
            (10, string_field(&range.branch_id)),
            (20, i64_field(range.begin_node_id)),
            (30, i64_field(range.end_node_id)),
        ],
    }
}

pub fn history_branch_range_from_struct(value: &Struct) -> Result<HistoryBranchRange> {
    let branch_id = match value.get(10) {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone())?,
        None => String::new(),
        Some(other) => {
            return Err(Error::Invalid(format!(
                "branchID: expected string, got {other:?}"
            )))
        }
    };
    let number = |id| match value.get(id) {
        Some(Value::I64(v)) => Ok(*v),
        None => Ok(0),
        Some(other) => Err(Error::Invalid(format!(
            "field {id}: expected i64, got {other:?}"
        ))),
    };
    Ok(HistoryBranchRange {
        branch_id,
        begin_node_id: number(20)?,
        end_node_id: number(30)?,
    })
}

pub fn history_tree_to_struct(tree: &HistoryTreeInfo) -> Struct {
    let ancestors = tree
        .ancestors
        .iter()
        .map(|range| Value::Struct(history_branch_range_to_struct(range)))
        .collect();
    Struct {
        fields: vec![
            (10, i64_field(tree.created_time_unix_nanos)),
            (12, Value::List(ancestors)),
            (14, string_field(&tree.info)),
        ],
    }
}

pub fn history_tree_from_struct(value: &Struct) -> Result<HistoryTreeInfo> {
    let created_time_unix_nanos = match value.get(10) {
        Some(Value::I64(v)) => *v,
        None => 0,
        Some(other) => {
            return Err(Error::Invalid(format!(
                "createdTimeNanos: expected i64, got {other:?}"
            )))
        }
    };
    let ancestors = match value.get(12) {
        Some(Value::List(items)) => items
            .iter()
            .map(|item| match item {
                Value::Struct(s) => history_branch_range_from_struct(s),
                other => Err(Error::Invalid(format!(
                    "ancestors: expected struct, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
        Some(other) => {
            return Err(Error::Invalid(format!(
                "ancestors: expected list, got {other:?}"
            )))
        }
    };
    let info = match value.get(14) {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone())?,
        None => String::new(),
        Some(other) => {
            return Err(Error::Invalid(format!(
                "info: expected string, got {other:?}"
            )))
        }
    };
    Ok(HistoryTreeInfo {
        created_time_unix_nanos,
        ancestors,
        info,
    })
}

pub fn timer_task_to_struct(task: &TimerTask) -> Result<Struct> {
    Ok(Struct {
        fields: vec![
            (
                10,
                Value::Bytes(uuid_to_bytes(&task.domain_id, "domainID")?),
            ),
            (12, string_field(&task.workflow_id)),
            (14, Value::Bytes(uuid_to_bytes(&task.run_id, "runID")?)),
            (16, Value::I16(task.task_type as i16)),
            (18, Value::I16(task.timeout_type as i16)),
            (20, i64_field(task.version)),
            (22, i64_field(task.schedule_attempt)),
            (24, i64_field(task.event_id)),
            (26, string_field(&task.task_list)),
        ],
    })
}

pub fn timer_task_from_struct(value: &Struct) -> Result<TimerTask> {
    let uuid = |id, name| match value.get(id) {
        Some(Value::Bytes(bytes)) => uuid_from_bytes(bytes, name),
        None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!(
            "{name}: expected binary, got {other:?}"
        ))),
    };
    let string = |id, name| match value.get(id) {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone()).map_err(Error::from),
        None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!(
            "{name}: expected string, got {other:?}"
        ))),
    };
    let i64v = |id| match value.get(id) {
        Some(Value::I64(v)) => Ok(*v),
        None => Ok(0),
        Some(other) => Err(Error::Invalid(format!(
            "field {id}: expected i64, got {other:?}"
        ))),
    };
    let i16v = |id| match value.get(id) {
        Some(Value::I16(v)) => Ok(i32::from(*v)),
        None => Ok(0),
        Some(other) => Err(Error::Invalid(format!(
            "field {id}: expected i16, got {other:?}"
        ))),
    };
    Ok(TimerTask {
        domain_id: uuid(10, "domainID")?,
        workflow_id: string(12, "workflowID")?,
        run_id: uuid(14, "runID")?,
        visibility_timestamp_unix_nanos: 0,
        task_id: 0,
        task_type: i16v(16)?,
        timeout_type: i16v(18)?,
        event_id: i64v(24)?,
        schedule_attempt: i64v(22)?,
        version: i64v(20)?,
        task_list: string(26, "taskList")?,
    })
}

pub fn replication_task_to_struct(task: &ReplicationTask) -> Result<Struct> {
    Ok(Struct {
        fields: vec![
            (
                10,
                Value::Bytes(uuid_to_bytes(&task.domain_id, "domainID")?),
            ),
            (12, string_field(&task.workflow_id)),
            (14, Value::Bytes(uuid_to_bytes(&task.run_id, "runID")?)),
            (16, Value::I16(task.task_type as i16)),
            (18, i64_field(task.version)),
            (20, i64_field(task.first_event_id)),
            (22, i64_field(task.next_event_id)),
            (24, i64_field(task.scheduled_id)),
            (26, Value::I32(task.event_store_version)),
            (28, Value::I32(task.new_run_event_store_version)),
            (30, Value::Bytes(task.branch_token.clone())),
            (34, Value::Bytes(task.new_run_branch_token.clone())),
            (38, i64_field(task.creation_time_unix_nanos)),
        ],
    })
}

pub fn replication_task_from_struct(value: &Struct) -> Result<ReplicationTask> {
    let uuid = |id, name| match value.get(id) {
        Some(Value::Bytes(bytes)) => uuid_from_bytes(bytes, name),
        None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!(
            "{name}: expected binary, got {other:?}"
        ))),
    };
    let string = |id, name| match value.get(id) {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone()).map_err(Error::from),
        None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!(
            "{name}: expected string, got {other:?}"
        ))),
    };
    let i64v = |id| match value.get(id) {
        Some(Value::I64(v)) => Ok(*v),
        None => Ok(0),
        Some(other) => Err(Error::Invalid(format!(
            "field {id}: expected i64, got {other:?}"
        ))),
    };
    let i32v = |id| match value.get(id) {
        Some(Value::I32(v)) => Ok(*v),
        None => Ok(0),
        Some(other) => Err(Error::Invalid(format!(
            "field {id}: expected i32, got {other:?}"
        ))),
    };
    let bytes = |id| match value.get(id) {
        Some(Value::Bytes(v)) => Ok(v.clone()),
        None => Ok(Vec::new()),
        Some(other) => Err(Error::Invalid(format!(
            "field {id}: expected binary, got {other:?}"
        ))),
    };
    let task_type = match value.get(16) {
        Some(Value::I16(v)) => i32::from(*v),
        None => 0,
        Some(other) => {
            return Err(Error::Invalid(format!(
                "taskType: expected i16, got {other:?}"
            )))
        }
    };
    Ok(ReplicationTask {
        domain_id: uuid(10, "domainID")?,
        workflow_id: string(12, "workflowID")?,
        run_id: uuid(14, "runID")?,
        task_id: 0,
        task_type,
        first_event_id: i64v(20)?,
        next_event_id: i64v(22)?,
        version: i64v(18)?,
        scheduled_id: i64v(24)?,
        event_store_version: i32v(26)?,
        new_run_event_store_version: i32v(28)?,
        branch_token: bytes(30)?,
        new_run_branch_token: bytes(34)?,
        creation_time_unix_nanos: i64v(38)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history_contract::{HistoryTreeInfo, ReplicationTask};
    use cadence_thrift::Value;

    // Go sqlblobs.TaskInfo emits these exact field bytes for scheduleID=7.
    const TASK_INFO_SCHEDULE_ID_7: &[u8] = &[10, 0, 13, 0, 0, 0, 0, 0, 0, 0, 7, 0];

    #[test]
    fn raw_thriftrw_golden_round_trip() {
        let mut value = Struct::default();
        value.fields.push((13, Value::I64(7)));
        let encoded = Encoding::ThriftRW.encode(&value).unwrap();
        assert_eq!(encoded, TASK_INFO_SCHEDULE_ID_7);
        assert_eq!(Encoding::ThriftRW.decode(&encoded).unwrap(), value);
    }

    #[test]
    fn history_branch_range_matches_go_thriftrw_golden() {
        let range = HistoryBranchRange {
            branch_id: "branch".into(),
            begin_node_id: 4,
            end_node_id: 9,
        };
        let expected = [
            11, 0, 10, 0, 0, 0, 6, b'b', b'r', b'a', b'n', b'c', b'h', 10, 0, 20, 0, 0, 0, 0, 0, 0,
            0, 4, 10, 0, 30, 0, 0, 0, 0, 0, 0, 0, 9, 0,
        ];
        let encoded = Encoding::ThriftRW
            .encode(&history_branch_range_to_struct(&range))
            .unwrap();
        assert_eq!(encoded, expected);
        assert_eq!(
            history_branch_range_from_struct(&Encoding::ThriftRW.decode(&expected).unwrap())
                .unwrap(),
            range
        );
    }

    #[test]
    fn all_go_sqlblobs_golden_fixtures_decode_and_reencode_byte_for_byte() {
        fn decode_hex(hex: &str) -> Vec<u8> {
            assert_eq!(hex.len() % 2, 0, "fixture hex must have full bytes");
            let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
            assert!(remainder.is_empty(), "fixture hex must have full bytes");
            pairs
                .iter()
                .map(|pair| {
                    let digit = |byte: u8| match byte {
                        b'0'..=b'9' => byte - b'0',
                        b'a'..=b'f' => byte - b'a' + 10,
                        _ => panic!("invalid fixture hex byte {byte}"),
                    };
                    (digit(pair[0]) << 4) | digit(pair[1])
                })
                .collect()
        }

        let mut seen = std::collections::BTreeSet::new();
        for line in include_str!("../tests/fixtures/sqlblobs_go_golden.txt").lines() {
            let (name, hex) = line
                .split_once(' ')
                .expect("fixture line includes a struct name");
            let bytes = decode_hex(hex);
            let decoded = Encoding::ThriftRW
                .decode(&bytes)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            let encoded = Encoding::ThriftRW
                .encode(&decoded)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(encoded, bytes, "Go {name} ThriftRW bytes changed");
            seen.insert(name);
        }
        let expected: std::collections::BTreeSet<_> = HISTORY_STRUCTS.iter().copied().collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn snappy_wraps_the_same_struct_and_encoding_names_match_go() {
        let mut value = Struct::default();
        value.fields.push((13, Value::I64(7)));
        let packed = Encoding::ThriftRWSnappy.encode(&value).unwrap();
        assert_eq!(Encoding::ThriftRWSnappy.decode(&packed).unwrap(), value);
        assert_eq!(Encoding::parse("thriftrw").unwrap().as_str(), "thriftrw");
        assert_eq!(
            Encoding::parse("thriftrw_snappy").unwrap().as_str(),
            "thriftrw_snappy"
        );
        assert!(Encoding::parse("json").is_err());
    }

    #[test]
    fn activity_info_contract_round_trips_seconds_and_unix_nanos() {
        let activity = ActivityInfo {
            version: 7,
            scheduled_time_unix_nanos: 1_700_000_000_123_456_789,
            schedule_to_close_timeout_seconds: 3600,
            activity_id: "act".into(),
            timer_task_status: 5,
            last_heartbeat_timeout_visibility_in_seconds: 1_700_000_123,
            retry_non_retriable_errors: vec!["Fatal".into()],
            ..Default::default()
        };
        let encoded = activity_info_to_struct(&activity);
        assert!(!encoded.fields.iter().any(|(id, _)| *id == 76));
        let mut expected = activity;
        expected.last_heartbeat_timeout_visibility_in_seconds = 0;
        assert_eq!(activity_info_from_struct(&encoded).unwrap(), expected);
    }

    #[test]
    fn remaining_sqlblobs_contracts_round_trip_non_defaults() {
        let child = ChildExecutionInfo {
            version: 1,
            initiated_event_batch_id: 2,
            started_id: 3,
            initiated_event: vec![4],
            initiated_event_encoding: "e".into(),
            started_workflow_id: "wf".into(),
            started_run_id: vec![5],
            started_event: vec![6],
            started_event_encoding: "s".into(),
            create_request_id: "c".into(),
            domain_id: "d".into(),
            domain_name: "dn".into(),
            workflow_type_name: "wt".into(),
            parent_close_policy: 7,
        };
        assert_eq!(
            child_execution_info_from_struct(&child_execution_info_to_struct(&child)).unwrap(),
            child
        );
        let signal = SignalInfo {
            version: 1,
            initiated_event_batch_id: 2,
            request_id: "r".into(),
            name: "n".into(),
            input: vec![3],
            control: vec![4],
        };
        assert_eq!(
            signal_info_from_struct(&signal_info_to_struct(&signal)).unwrap(),
            signal
        );
        let cancel = RequestCancelInfo {
            version: 1,
            initiated_event_batch_id: 2,
            cancel_request_id: "cancel".into(),
        };
        assert_eq!(
            request_cancel_info_from_struct(&request_cancel_info_to_struct(&cancel)).unwrap(),
            cancel
        );
        let timer = TimerInfo {
            version: 1,
            started_id: 2,
            expiry_time_unix_nanos: 1_700_000_000_123_456_789,
            task_id: 4,
        };
        assert_eq!(
            timer_info_from_struct(&timer_info_to_struct(&timer)).unwrap(),
            timer
        );
        let workflow = SqlWorkflowExecutionInfo {
            parent_domain_id: vec![1],
            parent_workflow_id: "parent".into(),
            parent_run_id: vec![2],
            initiated_id: 3,
            completion_event_batch_id: 4,
            completion_event: vec![5],
            completion_event_encoding: "blob".into(),
            task_list: "tasks".into(),
            task_list_kind: 1,
            workflow_type_name: "type".into(),
            workflow_timeout_seconds: 10,
            decision_task_timeout_seconds: 11,
            execution_context: vec![6],
            state: 2,
            close_status: 3,
            start_version: 4,
            last_write_event_id: 5,
            last_event_task_id: 6,
            last_first_event_id: 7,
            last_processed_event: 8,
            start_time_unix_nanos: 1_700_000_000_123_456_789,
            last_updated_time_unix_nanos: 1_700_000_000_987_654_321,
            decision_version: 9,
            decision_schedule_id: 10,
            decision_started_id: 11,
            decision_timeout_seconds: 12,
            decision_attempt: 13,
            decision_started_timestamp_unix_nanos: 14,
            decision_scheduled_timestamp_unix_nanos: 15,
            cancel_requested: true,
            decision_original_scheduled_timestamp_unix_nanos: 16,
            create_request_id: "create".into(),
            decision_request_id: "decision".into(),
            cancel_request_id: "cancel".into(),
            sticky_task_list: "sticky".into(),
            sticky_schedule_to_start_timeout_seconds: 17,
            retry_attempt: 18,
            retry_initial_interval_seconds: 19,
            retry_maximum_interval_seconds: 20,
            retry_maximum_attempts: 21,
            retry_expiration_seconds: 22,
            retry_backoff_coefficient: 1.5,
            retry_jitter_coefficient: 0.25,
            retry_expiration_time_unix_nanos: 23,
            retry_non_retryable_errors: vec!["Fatal".into()],
            has_retry_policy: true,
            cron_schedule: "daily".into(),
            event_store_version: 1,
            event_branch_token: vec![7],
            signal_count: 24,
            history_size: 25,
            client_library_version: "lib".into(),
            client_feature_version: "feature".into(),
            client_impl: "impl".into(),
            auto_reset_points: vec![8],
            auto_reset_points_encoding: "a".into(),
            search_attributes: [("k".into(), vec![9])].into(),
            memo: [("m".into(), vec![10])].into(),
            version_histories: vec![11],
            version_histories_encoding: "v".into(),
            first_execution_run_id: vec![12],
            partition_config: [("p".into(), "q".into())].into(),
            checksum: vec![13],
            checksum_encoding: "c".into(),
            cron_overlap_policy: 2,
            active_cluster_selection_policy: vec![14],
            active_cluster_selection_policy_encoding: "policy".into(),
        };
        assert_eq!(
            sql_workflow_execution_info_from_struct(&sql_workflow_execution_info_to_struct(
                &workflow
            ))
            .unwrap(),
            workflow
        );
    }

    #[test]
    fn typed_history_contract_mappers_round_trip() {
        let range = HistoryBranchRange {
            branch_id: "branch".into(),
            begin_node_id: 4,
            end_node_id: 9,
        };
        assert_eq!(
            history_branch_range_from_struct(&history_branch_range_to_struct(&range)).unwrap(),
            range
        );
        let tree = HistoryTreeInfo {
            created_time_unix_nanos: 12,
            ancestors: vec![range],
            info: "workflow debug".into(),
        };
        assert_eq!(
            history_tree_from_struct(&history_tree_to_struct(&tree)).unwrap(),
            tree
        );

        let transfer = TransferTask {
            domain_id: "6ba7b810-9dad-11d1-80b4-00c04fd430c8".into(),
            workflow_id: "workflow".into(),
            run_id: "6ba7b811-9dad-11d1-80b4-00c04fd430c8".into(),
            visibility_time_unix_nanos: 50,
            task_id: 7,
            target_domain_id: "6ba7b812-9dad-11d1-80b4-00c04fd430c8".into(),
            target_domain_ids: vec!["6ba7b813-9dad-11d1-80b4-00c04fd430c8".into()],
            target_workflow_id: "target".into(),
            target_run_id: "6ba7b814-9dad-11d1-80b4-00c04fd430c8".into(),
            target_child_workflow_only: true,
            task_list: "queue".into(),
            task_type: 3,
            schedule_id: 4,
            version: 5,
            record_visibility: false,
            original_task_list: "old-queue".into(),
            original_task_list_kind: 1,
        };
        assert_eq!(
            transfer_task_from_struct(&transfer_task_to_struct(&transfer).unwrap()).unwrap(),
            TransferTask {
                task_id: 0,
                ..transfer
            },
        );

        let timer = TimerTask {
            domain_id: "6ba7b810-9dad-11d1-80b4-00c04fd430c8".into(),
            workflow_id: "workflow".into(),
            run_id: "6ba7b811-9dad-11d1-80b4-00c04fd430c8".into(),
            visibility_timestamp_unix_nanos: 77,
            task_id: 0,
            task_type: 1,
            timeout_type: 2,
            event_id: 3,
            schedule_attempt: 4,
            version: 5,
            task_list: "queue".into(),
        };
        let timer_struct = timer_task_to_struct(&timer).unwrap();
        assert_eq!(
            timer_task_from_struct(&timer_struct).unwrap(),
            TimerTask {
                visibility_timestamp_unix_nanos: 0,
                ..timer
            }
        );

        let replication = ReplicationTask {
            domain_id: "6ba7b810-9dad-11d1-80b4-00c04fd430c8".into(),
            workflow_id: "workflow".into(),
            run_id: "6ba7b811-9dad-11d1-80b4-00c04fd430c8".into(),
            task_id: 0,
            task_type: 1,
            first_event_id: 2,
            next_event_id: 3,
            version: 4,
            scheduled_id: 5,
            event_store_version: 6,
            new_run_event_store_version: 7,
            branch_token: b"branch".to_vec(),
            new_run_branch_token: b"new".to_vec(),
            creation_time_unix_nanos: 8,
        };
        assert_eq!(
            replication_task_from_struct(&replication_task_to_struct(&replication).unwrap())
                .unwrap(),
            replication
        );
    }
}

// ---- History shard row (`sqlblobs.ShardInfo`) plain-data codec -------------------------

/// One `types.ProcessingQueueState` as stored inside the shard blob.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ShardProcessingQueueRecord { pub level: i32, pub ack_level: i64, pub max_level: i64, pub domain_ids: Vec<String>, pub reverse_match: bool }

/// `types.QueueState`; keys are `(scheduled_time_nano, task_id)`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ShardQueueStateRecord { pub exclusive_max_read_level: (i64, i64), pub virtual_queue_states: std::collections::BTreeMap<i64, Vec<ShardVirtualSliceRecord>> }

/// `types.Predicate` (thrift `shared.Predicate`).
#[derive(Debug, Clone, PartialEq)]
pub enum ShardPredicateRecord { Universal, Empty, DomainId { domain_ids: Vec<String>, is_exclusive: Option<bool> } }

/// `types.VirtualSliceState`; keys are `(scheduled_time_nano, task_id)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShardVirtualSliceRecord { pub inclusive_min: (i64, i64), pub exclusive_max: (i64, i64), pub predicate: Option<ShardPredicateRecord> }

fn predicate_struct(p: &ShardPredicateRecord) -> Value {
    // mapper/thrift/predicate.go FromPredicate: type + the matching attributes struct.
    let fields = match p {
        ShardPredicateRecord::Universal => vec![(10, Value::I32(0)), (20, Value::Struct(Struct::default()))],
        ShardPredicateRecord::Empty => vec![(10, Value::I32(1)), (30, Value::Struct(Struct::default()))],
        ShardPredicateRecord::DomainId { domain_ids, is_exclusive } => {
            let mut a = vec![(10, Value::List(domain_ids.iter().map(|d| string_field(d)).collect()))];
            if let Some(x) = is_exclusive { a.push((20, Value::Bool(*x))); }
            vec![(10, Value::I32(2)), (40, Value::Struct(Struct { fields: a }))]
        }
    };
    Value::Struct(Struct { fields })
}
fn read_predicate(value: Option<&Value>) -> Result<Option<ShardPredicateRecord>> {
    let Some(Value::Struct(s)) = value else { return Ok(None) };
    Ok(Some(match s.get(10) {
        Some(Value::I32(1)) => ShardPredicateRecord::Empty,
        Some(Value::I32(2)) => {
            let mut domain_ids = Vec::new();
            let mut is_exclusive = None;
            if let Some(Value::Struct(a)) = s.get(40) {
                if let Some(Value::List(l)) = a.get(10) {
                    for d in l { if let Value::Bytes(b) = d { domain_ids.push(String::from_utf8(b.clone()).map_err(Error::from)?); } }
                }
                if let Some(Value::Bool(b)) = a.get(20) { is_exclusive = Some(*b); }
            }
            ShardPredicateRecord::DomainId { domain_ids, is_exclusive }
        }
        _ => ShardPredicateRecord::Universal,
    }))
}

/// Plain-data view of `sqlblobs.ShardInfo` (the `shards.data` blob).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ShardInfoRecord {
    pub stolen_since_renew: i32,
    pub updated_at_nanos: i64,
    pub replication_ack_level: i64,
    pub transfer_ack_level: i64,
    pub timer_ack_level_nanos: i64,
    pub domain_notification_version: i64,
    pub cluster_transfer_ack_level: std::collections::BTreeMap<String, i64>,
    pub cluster_timer_ack_level: std::collections::BTreeMap<String, i64>,
    pub owner: String,
    pub cluster_replication_level: std::collections::BTreeMap<String, i64>,
    /// `(domain_id, failover_version, creation_time)`
    pub pending_failover_markers: Vec<(String, i64, i64)>,
    pub transfer_processing_queue_states: std::collections::BTreeMap<String, Vec<ShardProcessingQueueRecord>>,
    pub timer_processing_queue_states: std::collections::BTreeMap<String, Vec<ShardProcessingQueueRecord>>,
    pub queue_states: std::collections::BTreeMap<i32, ShardQueueStateRecord>,
}

fn i64_map(map: &std::collections::BTreeMap<String, i64>) -> Value {
    Value::Map(map.iter().map(|(k, v)| (string_field(k), Value::I64(*v))).collect())
}
fn read_i64_map(value: &Struct, id: i16) -> Result<std::collections::BTreeMap<String, i64>> {
    let mut out = std::collections::BTreeMap::new();
    match value.get(id) {
        None => {}
        Some(Value::Map(entries)) => {
            for (k, v) in entries {
                let (Value::Bytes(k), Value::I64(v)) = (k, v) else { return Err(Error::Invalid(format!("field {id}: bad map<string,i64> entry"))) };
                out.insert(String::from_utf8(k.clone())?, *v);
            }
        }
        Some(v) => return Err(Error::Invalid(format!("field {id}: expected map, got {v:?}"))),
    }
    Ok(out)
}
fn task_key_struct(key: (i64, i64)) -> Value { Value::Struct(Struct { fields: vec![(10, Value::I64(key.0)), (20, Value::I64(key.1))] }) }
fn read_task_key(value: Option<&Value>) -> Result<(i64, i64)> {
    match value { Some(Value::Struct(s)) => Ok((contract_i64(s, 10)?, contract_i64(s, 20)?)), _ => Ok((0, 0)) }
}
fn processing_queue_states_blob(states: &std::collections::BTreeMap<String, Vec<ShardProcessingQueueRecord>>) -> Result<Vec<u8>> {
    let entries = states.iter().map(|(cluster, list)| {
        let list = list.iter().map(|s| {
            let mut fields = vec![(10, Value::I32(s.level)), (20, Value::I64(s.ack_level)), (30, Value::I64(s.max_level))];
            let mut filter = Vec::new();
            if !s.domain_ids.is_empty() { filter.push((10, Value::List(s.domain_ids.iter().map(|d| string_field(d)).collect()))); }
            filter.push((20, Value::Bool(s.reverse_match)));
            fields.push((40, Value::Struct(Struct { fields: filter })));
            Value::Struct(Struct { fields })
        }).collect();
        (string_field(cluster), Value::List(list))
    }).collect();
    Encoding::ThriftRW.encode(&Struct { fields: vec![(10, Value::Map(entries))] })
}
fn read_processing_queue_states(data: &[u8]) -> Result<std::collections::BTreeMap<String, Vec<ShardProcessingQueueRecord>>> {
    let mut out = std::collections::BTreeMap::new();
    if data.is_empty() { return Ok(out); }
    let root = Encoding::ThriftRW.decode(data)?;
    if let Some(Value::Map(entries)) = root.get(10) {
        for (k, v) in entries {
            let (Value::Bytes(k), Value::List(list)) = (k, v) else { return Err(Error::Invalid("bad ProcessingQueueStates entry".into())) };
            let mut states = Vec::new();
            for item in list {
                let Value::Struct(s) = item else { return Err(Error::Invalid("bad ProcessingQueueState".into())) };
                let mut rec = ShardProcessingQueueRecord { level: contract_i32(s, 10)?, ack_level: contract_i64(s, 20)?, max_level: contract_i64(s, 30)?, ..Default::default() };
                if let Some(Value::Struct(f)) = s.get(40) {
                    if let Some(Value::List(ids)) = f.get(10) {
                        for id in ids { if let Value::Bytes(b) = id { rec.domain_ids.push(String::from_utf8(b.clone())?); } }
                    }
                    rec.reverse_match = matches!(f.get(20), Some(Value::Bool(true)));
                }
                states.push(rec);
            }
            out.insert(String::from_utf8(k.clone())?, states);
        }
    }
    Ok(out)
}

impl ShardInfoRecord {
    pub fn to_struct(&self) -> Result<Struct> {
        let mut f: Vec<(i16, Value)> = vec![
            (10, Value::I32(self.stolen_since_renew)), (12, Value::I64(self.updated_at_nanos)), (14, Value::I64(self.replication_ack_level)),
            (16, Value::I64(self.transfer_ack_level)), (18, Value::I64(self.timer_ack_level_nanos)), (24, Value::I64(self.domain_notification_version)),
            (38, string_field(&self.owner)),
        ];
        if !self.cluster_transfer_ack_level.is_empty() { f.push((34, i64_map(&self.cluster_transfer_ack_level))); }
        if !self.cluster_timer_ack_level.is_empty() { f.push((36, i64_map(&self.cluster_timer_ack_level))); }
        if !self.cluster_replication_level.is_empty() { f.push((40, i64_map(&self.cluster_replication_level))); }
        if !self.pending_failover_markers.is_empty() {
            let list = self.pending_failover_markers.iter().map(|(d, v, c)| Value::Struct(Struct { fields: vec![(10, string_field(d)), (20, Value::I64(*v)), (30, Value::I64(*c))] })).collect();
            let blob = Encoding::ThriftRW.encode(&Struct { fields: vec![(10, Value::List(list))] })?;
            f.push((42, Value::Bytes(blob))); f.push((44, string_field("thriftrw")));
        }
        if !self.transfer_processing_queue_states.is_empty() {
            f.push((50, Value::Bytes(processing_queue_states_blob(&self.transfer_processing_queue_states)?))); f.push((51, string_field("thriftrw")));
        }
        if !self.timer_processing_queue_states.is_empty() {
            f.push((55, Value::Bytes(processing_queue_states_blob(&self.timer_processing_queue_states)?))); f.push((56, string_field("thriftrw")));
        }
        if !self.queue_states.is_empty() {
            let entries = self.queue_states.iter().map(|(category, state)| {
                let mut qf = Vec::new();
                if !state.virtual_queue_states.is_empty() {
                    let queues = state.virtual_queue_states.iter().map(|(id, slices)| {
                        let slices: Vec<Value> = slices.iter().map(|sl| {
                            let mut sf = vec![(10, Value::Struct(Struct { fields: vec![(10, task_key_struct(sl.inclusive_min)), (20, task_key_struct(sl.exclusive_max))] }))];
                            if let Some(p) = &sl.predicate { sf.push((20, predicate_struct(p))); }
                            Value::Struct(Struct { fields: sf })
                        }).collect();
                        let vf = if slices.is_empty() { vec![] } else { vec![(10, Value::List(slices))] };
                        (Value::I64(*id), Value::Struct(Struct { fields: vf }))
                    }).collect();
                    qf.push((10, Value::Map(queues)));
                }
                qf.push((20, task_key_struct(state.exclusive_max_read_level)));
                (Value::I32(*category), Value::Struct(Struct { fields: qf }))
            }).collect();
            f.push((64, Value::Map(entries)));
        }
        Ok(Struct { fields: f })
    }

    pub fn from_struct(v: &Struct) -> Result<Self> {
        let mut rec = Self {
            stolen_since_renew: contract_i32(v, 10)?, updated_at_nanos: contract_i64(v, 12)?, replication_ack_level: contract_i64(v, 14)?,
            transfer_ack_level: contract_i64(v, 16)?, timer_ack_level_nanos: contract_i64(v, 18)?, domain_notification_version: contract_i64(v, 24)?,
            cluster_transfer_ack_level: read_i64_map(v, 34)?, cluster_timer_ack_level: read_i64_map(v, 36)?, owner: contract_string(v, 38)?,
            cluster_replication_level: read_i64_map(v, 40)?, ..Default::default()
        };
        let markers = contract_bytes(v, 42)?;
        if !markers.is_empty() {
            let root = Encoding::ThriftRW.decode(&markers)?;
            if let Some(Value::List(list)) = root.get(10) {
                for item in list { if let Value::Struct(s) = item { rec.pending_failover_markers.push((contract_string(s, 10)?, contract_i64(s, 20)?, contract_i64(s, 30)?)); } }
            }
        }
        rec.transfer_processing_queue_states = read_processing_queue_states(&contract_bytes(v, 50)?)?;
        rec.timer_processing_queue_states = read_processing_queue_states(&contract_bytes(v, 55)?)?;
        if let Some(Value::Map(entries)) = v.get(64) {
            for (k, s) in entries {
                let (Value::I32(k), Value::Struct(s)) = (k, s) else { return Err(Error::Invalid("bad queueStates entry".into())) };
                let mut q = ShardQueueStateRecord { exclusive_max_read_level: read_task_key(s.get(20))?, ..Default::default() };
                if let Some(Value::Map(vqs)) = s.get(10) {
                    for (id, vq) in vqs {
                        let (Value::I64(id), Value::Struct(vq)) = (id, vq) else { continue };
                        let list = q.virtual_queue_states.entry(*id).or_default();
                        if let Some(Value::List(slices)) = vq.get(10) {
                            for slice in slices {
                                let Value::Struct(slice) = slice else { continue };
                                let (min, max) = match slice.get(10) { Some(Value::Struct(range)) => (read_task_key(range.get(10))?, read_task_key(range.get(20))?), _ => ((0, 0), (0, 0)) };
                                list.push(ShardVirtualSliceRecord { inclusive_min: min, exclusive_max: max, predicate: read_predicate(slice.get(20))? });
                            }
                        }
                    }
                }
                rec.queue_states.insert(*k, q);
            }
        }
        Ok(rec)
    }

    pub fn encode(&self, encoding: Encoding) -> Result<Vec<u8>> { encoding.encode(&self.to_struct()?) }
    pub fn decode(data: &[u8], encoding: Encoding) -> Result<Self> { Self::from_struct(&encoding.decode(data)?) }
}
