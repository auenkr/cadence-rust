//! Go SQL matching blobs (sqlblobs.thrift), with exact optional containers.
//! These are raw Thrift structs, unlike history branch tokens (no 0x59 prefix).
use cadence_thrift::{value::read_struct, Error, Reader, Result, Struct, TType, Value, Writer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskData {
    pub workflow_id: String,
    pub run_id: Option<Vec<u8>>,
    /// Runtime-only raw `WorkflowExecution.RunID` for a request whose run ID is not a
    /// valid UUID (Go passes the string through unparsed on sync match). Never
    /// serialized, never persisted; `run_id` stays `None` while this is set.
    #[serde(skip)]
    pub raw_run_id: Option<String>,
    pub schedule_id: i64,
    pub expiry_nanos: i64,
    pub created_nanos: i64,
    pub partition_config: Option<BTreeMap<String, String>>,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SqlRunIdError {
    #[error("invalid SQL Matching RunID {raw:?}: {message}")]
    Malformed { raw: String, message: String },
}
/// time.Time{}.UnixNano(), as written by the pinned Go SQL serializer.
pub const GO_ZERO_TIME_NANOS: i64 = -6_795_364_578_871_345_152;
impl TaskData {
    /// Install the request RunID exactly as Go carries it (`TaskInfo.RunID` string),
    /// for every value: valid, noncanonical, malformed or empty. `run_id` is a best-effort
    /// parse for in-process readers only; stores must call `sql_run_id`/`cassandra_run_id`.
    pub fn with_request_run_id(mut self, run_id: &str) -> Self {
        // Replace both fields so a reused consuming setter never keeps stale state.
        self.raw_run_id = Some(run_id.to_owned());
        self.run_id = None;
        self.run_id = uuid::Uuid::parse_str(run_id)
            .ok()
            .map(|id| id.as_bytes().to_vec());
        self
    }
    /// RunID string as Go's `TaskInfo.RunID`: raw request string, canonical UUID, or "".
    pub fn run_id_string(&self) -> String {
        if let Some(raw) = &self.raw_run_id {
            return raw.clone();
        }
        self.run_id
            .as_deref()
            .and_then(|id| uuid::Uuid::from_slice(id).ok())
            .map(|id| id.to_string())
            .unwrap_or_default()
    }
    /// Go SQL `serialization.MustParseUUID`: "" -> nil slice (field omitted from the blob,
    /// i.e. absent, not the nil UUID); google/uuid `Parse` forms -> 16 bytes; else Go panics
    /// (source-only inference: unrecovered in the task writer goroutine).
    pub fn sql_run_id(&self) -> std::result::Result<Option<Vec<u8>>, String> {
        match &self.raw_run_id {
            None => Ok(self.run_id.clone()),
            Some(raw) if raw.is_empty() => Ok(None),
            Some(raw) => uuid::Uuid::parse_str(raw)
                .map(|id| Some(id.as_bytes().to_vec()))
                .map_err(|_| format!("invalid UUID {raw:?}")),
        }
    }
    /// Go Cassandra passes the string to gocql `marshalUUID` -> `ParseUUID`: hyphens are
    /// skipped at even nibble offsets, exactly 32 hex digits, empty is rejected.
    pub fn cassandra_run_id(&self) -> std::result::Result<Option<Vec<u8>>, String> {
        let Some(raw) = &self.raw_run_id else {
            return Ok(self.run_id.clone());
        };
        let mut out = [0u8; 16];
        let mut j = 0usize;
        for c in raw.chars() {
            match c {
                '-' if j & 1 == 0 => continue,
                _ if j < 32 && c.is_ascii_hexdigit() => {
                    let v = c.to_digit(16).unwrap() as u8;
                    out[j / 2] |= v << (4 - (j & 1) * 4);
                }
                _ => return Err(format!("invalid UUID {raw:?}")),
            }
            j += 1;
        }
        if j != 32 {
            return Err(format!("invalid UUID {raw:?}"));
        }
        Ok(Some(out.to_vec()))
    }
    /// Copy for a SQL row: raw RunID resolved per `sql_run_id` and cleared.
    pub fn for_sql(&self) -> std::result::Result<Self, SqlRunIdError> {
        let mut data = self.clone();
        data.run_id = match &self.raw_run_id {
            None => self.run_id.clone(),
            Some(raw) if raw.is_empty() => None,
            Some(raw) => Some(uuid::Uuid::parse_str(raw).map_err(|error| SqlRunIdError::Malformed {
                raw: raw.clone(), message: error.to_string(),
            })?.as_bytes().to_vec()),
        };
        data.raw_run_id = None;
        Ok(data)
    }
    /// Go SQL CreateTasks computes expiry at persistence time, not enqueue time.
    /// Nonpositive schedule-to-start timeout means no expiry (Go zero time).
    pub fn at_creation(mut self, now_nanos: i64, schedule_to_start_seconds: i32) -> Self {
        self.created_nanos = now_nanos;
        self.expiry_nanos = if schedule_to_start_seconds > 0 {
            now_nanos.wrapping_add(i64::from(schedule_to_start_seconds) * 1_000_000_000)
        } else {
            GO_ZERO_TIME_NANOS
        };
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partition {
    pub isolation_groups: Option<Vec<String>>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionConfig {
    pub version: i64,
    pub num_read_partitions: i32,
    pub num_write_partitions: i32,
    pub read_partitions: Option<BTreeMap<i32, Partition>>,
    pub write_partitions: Option<BTreeMap<i32, Partition>>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskListData {
    pub kind: i16,
    pub ack_level: i64,
    pub expiry_nanos: i64,
    pub last_updated_nanos: i64,
    pub adaptive_partition_config: Option<PartitionConfig>,
}

fn i64_field(w: &mut Writer, id: i16, value: i64) {
    w.write_field_begin(TType::I64, id);
    w.write_i64(value);
}
fn i32_field(w: &mut Writer, id: i16, value: i32) {
    w.write_field_begin(TType::I32, id);
    w.write_i32(value);
}
pub fn encode_task(data: &TaskData) -> std::result::Result<Vec<u8>, String> {
    // All SQL call sites resolve through `TaskData::for_sql`; Cassandra writes CQL, not this blob.
    // Keep this serialization boundary fallible in release builds too. SQL stores resolve
    // raw identifiers before calling it; accidental direct callers must not panic or drop it.
    if data.raw_run_id.is_some() {
        return Err("resolve raw RunID with TaskData::for_sql first".into());
    }
    let mut w = Writer::new();
    w.write_field_begin(TType::String, 10);
    w.write_string(&data.workflow_id);
    if let Some(run) = &data.run_id {
        w.write_field_begin(TType::String, 12);
        w.write_bytes(run);
    }
    i64_field(&mut w, 13, data.schedule_id);
    i64_field(&mut w, 14, data.expiry_nanos);
    i64_field(&mut w, 15, data.created_nanos);
    if let Some(config) = &data.partition_config {
        w.write_field_begin(TType::Map, 17);
        w.write_map_begin(TType::String, TType::String, config.len());
        for (key, value) in config {
            w.write_string(key);
            w.write_string(value);
        }
    }
    w.write_field_stop();
    Ok(w.into_bytes())
}
fn write_config(w: &mut Writer, config: &PartitionConfig) {
    i64_field(w, 10, config.version);
    i32_field(w, 12, config.num_read_partitions);
    i32_field(w, 14, config.num_write_partitions);
    for (id, map) in [
        (16, &config.read_partitions),
        (18, &config.write_partitions),
    ] {
        if let Some(map) = map {
            w.write_field_begin(TType::Map, id);
            w.write_map_begin(TType::I32, TType::Struct, map.len());
            for (partition, metadata) in map {
                w.write_i32(*partition);
                if let Some(groups) = &metadata.isolation_groups {
                    w.write_field_begin(TType::List, 10);
                    w.write_list_begin(TType::String, groups.len());
                    for group in groups {
                        w.write_string(group);
                    }
                }
                w.write_field_stop();
            }
        }
    }
    w.write_field_stop();
}
pub fn encode_task_list(data: &TaskListData) -> Vec<u8> {
    let mut w = Writer::new();
    w.write_field_begin(TType::I16, 10);
    w.write_i16(data.kind);
    i64_field(&mut w, 12, data.ack_level);
    i64_field(&mut w, 14, data.expiry_nanos);
    i64_field(&mut w, 16, data.last_updated_nanos);
    if let Some(config) = &data.adaptive_partition_config {
        w.write_field_begin(TType::Struct, 18);
        write_config(&mut w, config);
    }
    w.write_field_stop();
    w.into_bytes()
}
fn i64_value(s: &Struct, id: i16) -> i64 {
    match s.get(id) {
        Some(Value::I64(value)) => *value,
        _ => 0,
    }
}
fn i32_value(s: &Struct, id: i16) -> i32 {
    match s.get(id) {
        Some(Value::I32(value)) => *value,
        _ => 0,
    }
}
fn string(value: &Value) -> Result<String> {
    match value {
        Value::Bytes(data) => Ok(String::from_utf8(data.clone())?),
        _ => Err(Error::Invalid("expected string in matching blob".into())),
    }
}
pub fn decode_task(bytes: &[u8]) -> Result<TaskData> {
    let s = read_struct(&mut Reader::new(bytes))?;
    let partition_config = match s.get(17) {
        Some(Value::Map(values)) => Some(
            values
                .iter()
                .map(|(k, v)| Ok((string(k)?, string(v)?)))
                .collect::<Result<_>>()?,
        ),
        _ => None,
    };
    Ok(TaskData {
        workflow_id: match s.get(10) {
            Some(v @ Value::Bytes(_)) => string(v)?,
            _ => String::new(),
        },
        run_id: match s.get(12) {
            Some(Value::Bytes(bytes)) => Some(bytes.clone()),
            _ => None,
        },
        raw_run_id: None,
        schedule_id: i64_value(&s, 13),
        expiry_nanos: i64_value(&s, 14),
        created_nanos: i64_value(&s, 15),
        partition_config,
    })
}
fn read_partitions(s: &Struct, id: i16) -> Result<Option<BTreeMap<i32, Partition>>> {
    let Some(Value::Map(values)) = s.get(id) else {
        return Ok(None);
    };
    values
        .iter()
        .map(|(key, value)| {
            let (Value::I32(key), Value::Struct(metadata)) = (key, value) else {
                return Err(Error::Invalid("invalid matching partition entry".into()));
            };
            let isolation_groups = match metadata.get(10) {
                Some(Value::List(values)) => {
                    Some(values.iter().map(string).collect::<Result<_>>()?)
                }
                _ => None,
            };
            Ok((*key, Partition { isolation_groups }))
        })
        .collect::<Result<_>>()
        .map(Some)
}
pub fn decode_task_list(bytes: &[u8]) -> Result<TaskListData> {
    let s = read_struct(&mut Reader::new(bytes))?;
    let adaptive_partition_config = match s.get(18) {
        Some(Value::Struct(config)) => Some(PartitionConfig {
            version: i64_value(config, 10),
            num_read_partitions: i32_value(config, 12),
            num_write_partitions: i32_value(config, 14),
            read_partitions: read_partitions(config, 16)?,
            write_partitions: read_partitions(config, 18)?,
        }),
        _ => None,
    };
    Ok(TaskListData {
        kind: match s.get(10) {
            Some(Value::I16(kind)) => *kind,
            _ => 0,
        },
        ack_level: i64_value(&s, 12),
        expiry_nanos: i64_value(&s, 14),
        last_updated_nanos: i64_value(&s, 16),
        adaptive_partition_config,
    })
}

/// SQL data_encoding values used by the pinned Go parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobEncoding {
    ThriftRW,
    ThriftRWSnappy,
}
impl BlobEncoding {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "thriftrw" => Ok(Self::ThriftRW),
            "thriftrw_snappy" => Ok(Self::ThriftRWSnappy),
            _ => Err(Error::Invalid(format!(
                "unsupported matching blob encoding {value}"
            ))),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ThriftRW => "thriftrw",
            Self::ThriftRWSnappy => "thriftrw_snappy",
        }
    }
    pub fn encode(self, bytes: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::ThriftRW => Ok(bytes.to_vec()),
            Self::ThriftRWSnappy => snap::raw::Encoder::new()
                .compress_vec(bytes)
                .map_err(|error| Error::Invalid(error.to_string())),
        }
    }
    pub fn decode(self, bytes: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::ThriftRW => Ok(bytes.to_vec()),
            Self::ThriftRWSnappy => snap::raw::Decoder::new()
                .decompress_vec(bytes)
                .map_err(|error| Error::Invalid(error.to_string())),
        }
    }
}

/// Encoding used for newly written SQL blobs. Go never reads the SQL
/// `encodingType` setting for task rows: `parser.TaskInfoToBlob` and
/// `TaskListInfoToBlob` (serialization/parser.go:224,241) encode with the
/// dynamic property `history.serializationEncoding` (default `thriftrw`),
/// sampled on every write.
#[derive(Clone)]
pub struct EncodingSource(std::sync::Arc<dyn Fn() -> String + Send + Sync>);
impl EncodingSource {
    pub fn fixed(encoding: BlobEncoding) -> Self {
        Self(std::sync::Arc::new(move || encoding.as_str().to_owned()))
    }
    pub fn dynamic(source: std::sync::Arc<dyn Fn() -> String + Send + Sync>) -> Self {
        Self(source)
    }
    pub fn current(&self) -> Result<BlobEncoding> {
        BlobEncoding::parse(&(self.0)())
    }
}
impl std::fmt::Debug for EncodingSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("EncodingSource").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_run_id_keeps_every_raw_string() {
        let valid = uuid::Uuid::from_u128(9);
        let upper = valid.to_string().to_uppercase();
        for raw in [valid.to_string(), upper.clone(), valid.simple().to_string(), "".into(), "not-a-uuid".into()] {
            let d = TaskData::default().with_request_run_id(&raw);
            assert_eq!(d.run_id_string(), raw);
        }
        assert_eq!(TaskData::default().with_request_run_id(&upper).run_id.as_deref(), Some(valid.as_bytes().as_slice()));
        assert!(TaskData::default().with_request_run_id("bad").run_id.is_none());
    }
    #[test]
    fn unresolved_raw_run_id_encoding_is_fallible() {
        let data = TaskData::default().with_request_run_id("not-a-uuid");
        assert!(encode_task(&data).unwrap_err().contains("resolve raw RunID"));
    }
    #[test]
    fn sql_boundary_empty_is_absent_not_nil_uuid_and_malformed_errors() {
        let empty = TaskData::default().with_request_run_id("").for_sql().unwrap();
        assert_eq!(empty.run_id, None);
        assert_eq!(empty.raw_run_id, None);
        assert_eq!(decode_task(&encode_task(&empty).unwrap()).unwrap().run_id, None);
        let error = TaskData::default().with_request_run_id("bad").for_sql().unwrap_err();
        assert!(matches!(error, SqlRunIdError::Malformed { ref raw, .. } if raw == "bad"));
        let store_error: crate::matching_task_store::MatchingTaskStoreError = error.into();
        assert!(matches!(store_error, crate::matching_task_store::MatchingTaskStoreError::MalformedSqlMatchingRunId(_)));
        let config_error = crate::matching_task_store::MatchingTaskStoreError::BackendConfiguration("bad config".into());
        assert!(!matches!(config_error, crate::matching_task_store::MatchingTaskStoreError::MalformedSqlMatchingRunId(_)));
        let cassandra_error = TaskData::default().with_request_run_id("bad").cassandra_run_id().unwrap_err();
        assert!(cassandra_error.contains("invalid UUID"));
        assert!(!cassandra_error.contains("SQL Matching"));
        let up = TaskData::default().with_request_run_id(&uuid::Uuid::from_u128(5).to_string().to_uppercase()).for_sql().unwrap();
        assert_eq!(up.run_id, Some(uuid::Uuid::from_u128(5).as_bytes().to_vec()));
        assert_eq!(decode_task(&encode_task(&up).unwrap()).unwrap().raw_run_id, None);
    }
    #[test]
    fn cassandra_boundary_follows_gocql_parse_uuid() {
        let ok = |s: &str| TaskData::default().with_request_run_id(s).cassandra_run_id();
        assert!(ok("").is_err());
        assert!(ok("bad").is_err());
        let id = uuid::Uuid::from_u128(0xabcdef).as_bytes().to_vec();
        assert_eq!(ok(&uuid::Uuid::from_u128(0xabcdef).to_string().to_uppercase()).unwrap(), Some(id.clone()));
        assert_eq!(ok(&uuid::Uuid::from_u128(0xabcdef).simple().to_string()).unwrap(), Some(id.clone()));
        // gocql skips '-' at any even nibble offset, unlike google/uuid
        assert_eq!(ok("00-00-00-00-00-00-00-00-00-00-00-00-00-ab-cd-ef").unwrap(), Some(id));
        assert!(ok("0-000000000000000000000000000abcdef").is_err());
        assert!(ok("00000000000000000000000000abcdef0").is_err());
    }
    #[test]
    fn dynamic_encoding_source_is_sampled_per_write_like_go_parser() {
        let value = std::sync::Arc::new(std::sync::Mutex::new("thriftrw".to_owned()));
        let reader = value.clone();
        let source =
            EncodingSource::dynamic(std::sync::Arc::new(move || reader.lock().unwrap().clone()));
        assert_eq!(source.current().unwrap(), BlobEncoding::ThriftRW);
        *value.lock().unwrap() = "thriftrw_snappy".into();
        assert_eq!(source.current().unwrap(), BlobEncoding::ThriftRWSnappy);
        // Go's getEncoder rejects anything else on write.
        *value.lock().unwrap() = "json".into();
        assert!(source.current().is_err());
        assert_eq!(
            EncodingSource::fixed(BlobEncoding::ThriftRWSnappy)
                .current()
                .unwrap(),
            BlobEncoding::ThriftRWSnappy
        );
    }
    fn unhex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|chunk| u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap())
            .collect()
    }
    #[test]
    fn matching_blob_create_expiry_matches_go_sql() {
        let now = 1_700_000_000_123_456_789;
        assert_eq!(
            TaskData::default().at_creation(now, 0).expiry_nanos,
            GO_ZERO_TIME_NANOS
        );
        assert_eq!(
            TaskData::default().at_creation(now, -1).expiry_nanos,
            GO_ZERO_TIME_NANOS
        );
        let data = TaskData::default().at_creation(now, 3);
        assert_eq!(data.created_nanos, now);
        assert_eq!(data.expiry_nanos, now + 3_000_000_000);
    }

    #[test]
    fn matching_blob_original_go_fixtures() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-blobs.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let plain = unhex(case["thrift_hex"].as_str().unwrap());
            assert_eq!(
                BlobEncoding::ThriftRWSnappy
                    .decode(&unhex(case["snappy_hex"].as_str().unwrap()))
                    .unwrap(),
                plain
            );
            assert_eq!(
                BlobEncoding::ThriftRWSnappy
                    .decode(&BlobEncoding::ThriftRWSnappy.encode(&plain).unwrap())
                    .unwrap(),
                plain
            );
            if case["kind"] == "task" {
                let data: TaskData = serde_json::from_value(case["data"].clone()).unwrap();
                assert_eq!(decode_task(&plain).unwrap(), data);
                assert_eq!(encode_task(&data).unwrap(), plain);
            } else {
                let data: TaskListData = serde_json::from_value(case["data"].clone()).unwrap();
                assert_eq!(decode_task_list(&plain).unwrap(), data);
                assert_eq!(encode_task_list(&data), plain);
            }
        }
    }
    #[test]
    fn matching_blob_unknown_fields_and_malformed_input() {
        let mut bytes = encode_task(&TaskData::default()).unwrap();
        bytes.pop();
        let mut extra = Writer::new();
        extra.write_field_begin(TType::String, 99);
        extra.write_string("future field");
        extra.write_field_stop();
        bytes.extend(extra.into_bytes());
        assert_eq!(decode_task(&bytes).unwrap(), TaskData::default());
        for end in 0..bytes.len() {
            assert!(decode_task(&bytes[..end]).is_err());
        }
        assert!(BlobEncoding::ThriftRWSnappy.decode(&[255]).is_err());
        assert!(BlobEncoding::parse("json").is_err());
    }
}
