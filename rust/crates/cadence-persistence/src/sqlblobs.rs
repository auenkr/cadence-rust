//! ThriftRW codec shared by History's SQL `sqlblobs` values.
//!
//! `sqlblobs.thrift` structs are encoded with the binary protocol without a
//! Thrift message envelope or version byte. The column's `data_encoding`
//! carries the codec choice; `thriftrw_snappy` is a raw Snappy block around
//! the exact same struct bytes. Struct-specific field mapping belongs at the
//! contract boundary, while this module owns the byte-compatible wire layer.

use cadence_thrift::{value::{read_struct, write_value, Struct}, Error, Reader, Result, TType, Writer};
use crate::history_contract::{HistoryBranchRange, TimerTask, TransferTask};
use cadence_thrift::Value;

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
            return Err(Error::Invalid("trailing bytes after sqlblobs struct".into()));
        }
        Ok(value)
    }
}

fn value_type(value: &cadence_thrift::Value) -> TType {
    use cadence_thrift::Value;
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

fn string_field(value: &str) -> Value { Value::Bytes(value.as_bytes().to_vec()) }

fn i64_field(value: i64) -> Value { Value::I64(value) }

/// Convert the History task contract to Go `sqlblobs.TransferTaskInfo`.
pub fn transfer_task_to_struct(task: &TransferTask) -> Result<Struct> {
    let mut value = Struct::default();
    value.fields = vec![
        (10, Value::Bytes(uuid_to_bytes(&task.domain_id, "domainID")?)),
        (12, string_field(&task.workflow_id)),
        (14, Value::Bytes(uuid_to_bytes(&task.run_id, "runID")?)),
        (16, Value::I16(task.task_type as i16)),
        (18, Value::Bytes(uuid_to_bytes(&task.target_domain_id, "targetDomainID")?)),
        (20, string_field(&task.target_workflow_id)),
        (22, Value::Bytes(uuid_to_bytes(&task.target_run_id, "targetRunID")?)),
        (24, string_field(&task.task_list)),
        (26, Value::Bool(task.target_child_workflow_only)),
        (28, i64_field(task.schedule_id)),
        (30, i64_field(task.version)),
        (32, i64_field(task.visibility_time_unix_nanos)),
        (34, Value::Set(task.target_domain_ids.iter().map(|id| uuid_to_bytes(id, "targetDomainIDs").map(Value::Bytes)).collect::<Result<Vec<_>>>()?)),
        (36, string_field(&task.original_task_list)),
        (38, Value::I32(task.original_task_list_kind)),
    ];
    Ok(value)
}

pub fn transfer_task_from_struct(value: &Struct) -> Result<TransferTask> {
    let get_uuid = |id, field| match value.get(id) {
        Some(Value::Bytes(bytes)) => uuid_from_bytes(bytes, field),
        None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!("{field}: expected binary, got {other:?}"))),
    };
    let get_string = |id, field| match value.get(id) {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone()).map_err(Error::from),
        None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!("{field}: expected string, got {other:?}"))),
    };
    let get_i64 = |id| match value.get(id) {
        Some(Value::I64(v)) => Ok(*v),
        Some(Value::I32(v)) => Ok(i64::from(*v)),
        None => Ok(0),
        Some(other) => Err(Error::Invalid(format!("expected i64, got {other:?}"))),
    };
    let target_domain_ids = match value.get(34) {
        Some(Value::Set(values)) => values.iter().map(|v| match v {
            Value::Bytes(bytes) => uuid_from_bytes(bytes, "targetDomainIDs"),
            other => Err(Error::Invalid(format!("targetDomainIDs: expected binary, got {other:?}"))),
        }).collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
        Some(other) => return Err(Error::Invalid(format!("targetDomainIDs: expected set, got {other:?}"))),
    };
    let task_type = match value.get(16) { Some(Value::I16(v)) => i32::from(*v), None => 0, Some(other) => return Err(Error::Invalid(format!("taskType: expected i16, got {other:?}"))) };
    let original_task_list_kind = match value.get(38) { Some(Value::I32(v)) => *v, None => 0, Some(other) => return Err(Error::Invalid(format!("originalTaskListKind: expected enum, got {other:?}"))) };
    let target_child_workflow_only = match value.get(26) { Some(Value::Bool(v)) => *v, None => false, Some(other) => return Err(Error::Invalid(format!("targetChildWorkflowOnly: expected bool, got {other:?}"))) };
    Ok(TransferTask {
        domain_id: get_uuid(10, "domainID")?, workflow_id: get_string(12, "workflowID")?, run_id: get_uuid(14, "runID")?,
        visibility_time_unix_nanos: get_i64(32)?, task_id: 0, target_domain_id: get_uuid(18, "targetDomainID")?, target_domain_ids,
        target_workflow_id: get_string(20, "targetWorkflowID")?, target_run_id: get_uuid(22, "targetRunID")?, target_child_workflow_only,
        task_list: get_string(24, "taskList")?, task_type, schedule_id: get_i64(28)?, version: get_i64(30)?, record_visibility: false,
        original_task_list: get_string(36, "originalTaskList")?, original_task_list_kind,
    })
}

pub fn history_branch_range_to_struct(range: &HistoryBranchRange) -> Struct {
    Struct { fields: vec![(10, string_field(&range.branch_id)), (20, i64_field(range.begin_node_id)), (30, i64_field(range.end_node_id))] }
}

pub fn history_branch_range_from_struct(value: &Struct) -> Result<HistoryBranchRange> {
    let branch_id = match value.get(10) {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone())?,
        None => String::new(),
        Some(other) => return Err(Error::Invalid(format!("branchID: expected string, got {other:?}"))),
    };
    let number = |id| match value.get(id) {
        Some(Value::I64(v)) => Ok(*v), None => Ok(0),
        Some(other) => Err(Error::Invalid(format!("field {id}: expected i64, got {other:?}"))),
    };
    Ok(HistoryBranchRange { branch_id, begin_node_id: number(20)?, end_node_id: number(30)? })
}

pub fn timer_task_to_struct(task: &TimerTask) -> Result<Struct> {
    Ok(Struct { fields: vec![
        (10, Value::Bytes(uuid_to_bytes(&task.domain_id, "domainID")?)),
        (12, string_field(&task.workflow_id)),
        (14, Value::Bytes(uuid_to_bytes(&task.run_id, "runID")?)),
        (16, Value::I16(task.task_type as i16)),
        (18, Value::I16(task.timeout_type as i16)),
        (20, i64_field(task.version)),
        (22, i64_field(task.schedule_attempt)),
        (24, i64_field(task.event_id)),
        (26, string_field(&task.task_list)),
    ] })
}

pub fn timer_task_from_struct(value: &Struct) -> Result<TimerTask> {
    let uuid = |id, name| match value.get(id) {
        Some(Value::Bytes(bytes)) => uuid_from_bytes(bytes, name), None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!("{name}: expected binary, got {other:?}"))),
    };
    let string = |id, name| match value.get(id) {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes.clone()).map_err(Error::from), None => Ok(String::new()),
        Some(other) => Err(Error::Invalid(format!("{name}: expected string, got {other:?}"))),
    };
    let i64v = |id| match value.get(id) {
        Some(Value::I64(v)) => Ok(*v), None => Ok(0),
        Some(other) => Err(Error::Invalid(format!("field {id}: expected i64, got {other:?}"))),
    };
    let i16v = |id| match value.get(id) {
        Some(Value::I16(v)) => Ok(i32::from(*v)), None => Ok(0),
        Some(other) => Err(Error::Invalid(format!("field {id}: expected i16, got {other:?}"))),
    };
    Ok(TimerTask {
        domain_id: uuid(10, "domainID")?, workflow_id: string(12, "workflowID")?, run_id: uuid(14, "runID")?,
        visibility_timestamp_unix_nanos: 0, task_id: 0, task_type: i16v(16)?, timeout_type: i16v(18)?, event_id: i64v(24)?,
        schedule_attempt: i64v(22)?, version: i64v(20)?, task_list: string(26, "taskList")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_thrift::Value;

    // Go sqlblobs.TaskInfo emits these exact field bytes for scheduleID=7.
    const TASK_INFO_SCHEDULE_ID_7: &[u8] = &[8, 0, 13, 0, 0, 0, 0, 0, 0, 0, 7, 0];

    #[test]
    fn raw_thriftrw_golden_round_trip() {
        let mut value = Struct::default();
        value.fields.push((13, Value::I64(7)));
        let encoded = Encoding::ThriftRW.encode(&value).unwrap();
        assert_eq!(encoded, TASK_INFO_SCHEDULE_ID_7);
        assert_eq!(Encoding::ThriftRW.decode(&encoded).unwrap(), value);
    }

    #[test]
    fn snappy_wraps_the_same_struct_and_encoding_names_match_go() {
        let mut value = Struct::default();
        value.fields.push((13, Value::I64(7)));
        let packed = Encoding::ThriftRWSnappy.encode(&value).unwrap();
        assert_eq!(Encoding::ThriftRWSnappy.decode(&packed).unwrap(), value);
        assert_eq!(Encoding::parse("thriftrw").unwrap().as_str(), "thriftrw");
        assert_eq!(Encoding::parse("thriftrw_snappy").unwrap().as_str(), "thriftrw_snappy");
        assert!(Encoding::parse("json").is_err());
    }
}
