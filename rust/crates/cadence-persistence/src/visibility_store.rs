//! Durable visibility manager for the Frontend list/get APIs.
//!
//! Ports Go's basic (non-Elasticsearch) visibility stores:
//! `common/persistence/sql/sql_visibility_store.go` (MySQL, PostgreSQL and
//! SQLite) over the `executions_visibility` table and
//! `common/persistence/nosql/nosql_visibility_store.go` over the Cassandra
//! `open_executions` / `closed_executions` / `closed_executions_v2` tables.
//!
//! Like Go's basic stores, query-based List/Scan/Count and non-trivial Upsert
//! return `BadRequest("Operation is not supported")`; only the by-filter
//! lists, GetClosed and Delete are served. Times are nanoseconds since the Unix
//! epoch at this boundary (Go `time.Time.UnixNano()`), stored as UTC.

use crate::{
    matching_cassandra_exec::{bound, BoundValue, BoundValues},
    matching_mysql::{ConnectionLease, MySqlTaskStoreConfig, MySqlTaskStore, SyncConnectionPool},
    matching_postgres::{PostgresConnectionPool, PostgresShardConfig},
};
use cadence_api::{
    enums::{WorkflowExecutionCloseStatus, WorkflowExecutionStatus},
    service_types::WorkflowExecutionInfo,
    types::{Memo, TaskList, WorkflowExecution, WorkflowType},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

/// `time.Time{}.UnixNano()` in Go: the value Go reports for an unset time.
pub const GO_ZERO_TIME_NANOS: i64 = -6_795_364_578_871_345_152;
/// Go `openExecutionTTLBuffer` and default close TTL (seconds).
const OPEN_EXECUTION_TTL_BUFFER: i64 = 86_400;
const DEFAULT_CLOSE_TTL_SECONDS: i64 = 86_400;
const MAX_CASSANDRA_TTL: i64 = 157_680_000;
const DOMAIN_PARTITION: i32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisibilityError {
    BadRequest(String),
    EntityNotExists(String),
    Internal(String),
}

impl VisibilityError {
    /// Go `ErrVisibilityOperationNotSupported`.
    pub fn not_supported() -> Self {
        Self::BadRequest("Operation is not supported".into())
    }
}

impl fmt::Display for VisibilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadRequest(m) | Self::EntityNotExists(m) | Self::Internal(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for VisibilityError {}

pub type VisibilityResult<T> = std::result::Result<T, VisibilityError>;

fn internal(error: impl fmt::Display) -> VisibilityError {
    VisibilityError::Internal(error.to_string())
}

/// One visibility row as written by History (Go
/// `InternalRecordWorkflowExecutionStarted/ClosedRequest`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VisibilityRecord {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type_name: String,
    /// `None` is Go's zero `time.Time`.
    pub start_time_ns: Option<i64>,
    pub execution_time_ns: Option<i64>,
    pub memo: Vec<u8>,
    pub encoding: String,
    pub task_list: String,
    pub is_cron: bool,
    pub cron_schedule: String,
    pub num_clusters: i32,
    pub update_time_ns: Option<i64>,
    pub shard_id: i32,
    /// Go `types.WorkflowExecutionStatus` integer.
    pub execution_status: i32,
    pub scheduled_execution_time_ns: Option<i64>,
    pub close_time_ns: Option<i64>,
    /// Go `WorkflowExecutionCloseStatus` thrift integer.
    pub close_status: i32,
    pub history_length: i64,
}

/// Which list API is requested; mirrors Go's per-filter store methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListFilter {
    None,
    WorkflowType(String),
    WorkflowId(String),
    CloseStatus(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityListRequest {
    pub domain_id: String,
    pub earliest_time_ns: i64,
    pub latest_time_ns: i64,
    pub page_size: i32,
    pub next_page_token: Vec<u8>,
    pub closed: bool,
    pub filter: ListFilter,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VisibilityListResponse {
    pub executions: Vec<WorkflowExecutionInfo>,
    pub next_page_token: Vec<u8>,
}

pub trait VisibilityManager: Send + Sync + fmt::Debug {
    fn name(&self) -> &'static str;
    fn record_started(
        &self,
        record: &VisibilityRecord,
        workflow_timeout_seconds: i64,
    ) -> VisibilityResult<()>;
    fn record_closed(
        &self,
        record: &VisibilityRecord,
        retention_seconds: i64,
    ) -> VisibilityResult<()>;
    /// Go: only implemented for Elasticsearch, a no-op here.
    fn record_uninitialized(&self, _record: &VisibilityRecord) -> VisibilityResult<()> {
        Ok(())
    }
    /// Go `IsNopUpsertWorkflowRequest` succeeds, anything else is unsupported.
    fn upsert(&self, nop_request: bool) -> VisibilityResult<()> {
        if nop_request {
            Ok(())
        } else {
            Err(VisibilityError::not_supported())
        }
    }
    fn list(&self, request: &VisibilityListRequest) -> VisibilityResult<VisibilityListResponse>;
    fn get_closed(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> VisibilityResult<WorkflowExecutionInfo>;
    /// `admin` is Go's `VisibilityAdminDeletionKey` context flag; Cassandra
    /// only deletes (from open_executions) when it is set.
    fn delete(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
        admin: bool,
    ) -> VisibilityResult<()>;
    fn list_by_query(&self, _domain_id: &str, _query: &str) -> VisibilityResult<VisibilityListResponse> {
        Err(VisibilityError::not_supported())
    }
    fn scan_by_query(&self, _domain_id: &str, _query: &str) -> VisibilityResult<VisibilityListResponse> {
        Err(VisibilityError::not_supported())
    }
    fn count_by_query(&self, _domain_id: &str, _query: &str) -> VisibilityResult<i64> {
        Err(VisibilityError::not_supported())
    }
}

// ---------------------------------------------------------------------------
// Record -> WorkflowExecutionInfo (Go `convertVisibilityWorkflowExecutionInfo`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct InternalInfo {
    workflow_id: String,
    run_id: String,
    type_name: String,
    start_ns: i64,
    execution_ns: i64,
    memo: Vec<u8>,
    encoding: String,
    task_list: String,
    is_cron: bool,
    cron_schedule: String,
    execution_status: i32,
    scheduled_ns: i64,
    close: Option<InternalClose>,
}

#[derive(Debug, Clone)]
struct InternalClose {
    close_ns: i64,
    status: i32,
    history_length: i64,
}

impl InternalInfo {
    fn into_info(mut self) -> WorkflowExecutionInfo {
        // Go: ExecutionTime of cron or retry runs falls back to StartTime.
        if self.execution_ns == 0 {
            self.execution_ns = self.start_ns;
        }
        let mut info = WorkflowExecutionInfo {
            execution: Some(WorkflowExecution::new(self.workflow_id, self.run_id)),
            workflow_type: Some(WorkflowType {
                name: self.type_name,
            }),
            start_time: Some(self.start_ns),
            execution_time: Some(self.execution_ns),
            memo: decode_memo(&self.memo, &self.encoding),
            task_list: Some(TaskList {
                name: self.task_list,
                ..Default::default()
            }),
            is_cron: self.is_cron,
            cron_schedule: Some(self.cron_schedule),
            execution_status: WorkflowExecutionStatus::from_i32(self.execution_status),
            scheduled_execution_time: Some(self.scheduled_ns),
            ..Default::default()
        };
        if let Some(close) = self.close {
            info.close_time = Some(close.close_ns);
            info.close_status = WorkflowExecutionCloseStatus::from_i32(close.status);
            info.history_length = close.history_length;
        }
        info
    }
}

/// Go `DeserializeVisibilityMemo`: empty data is an empty memo, an unreadable
/// or unknown-encoding blob is dropped (Go logs and returns nil).
pub fn decode_memo(data: &[u8], encoding: &str) -> Option<Memo> {
    if data.is_empty() {
        return Some(Memo::default());
    }
    match encoding.to_ascii_lowercase().as_str() {
        "json" => serde_json::from_slice::<Memo>(data).ok(),
        "thriftrw" => decode_thrift_memo(data).ok(),
        _ => None,
    }
}

/// ThriftRW binary encoding of `shared.Memo { 10: optional map<string,binary> fields }`.
pub fn encode_thrift_memo(memo: &Memo) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(fields) = &memo.fields {
        out.push(13); // map
        out.extend_from_slice(&10_i16.to_be_bytes());
        out.push(11); // string key
        out.push(11); // binary value
        out.extend_from_slice(&(fields.len() as i32).to_be_bytes());
        let mut keys: Vec<_> = fields.keys().collect();
        keys.sort();
        for key in keys {
            out.extend_from_slice(&(key.len() as i32).to_be_bytes());
            out.extend_from_slice(key.as_bytes());
            let value = &fields[key];
            out.extend_from_slice(&(value.len() as i32).to_be_bytes());
            out.extend_from_slice(value);
        }
    }
    out.push(0);
    out
}

fn decode_thrift_memo(data: &[u8]) -> Result<Memo, String> {
    struct Reader<'a>(&'a [u8]);
    impl<'a> Reader<'a> {
        fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
            if self.0.len() < n {
                return Err("truncated thrift memo".into());
            }
            let (head, tail) = self.0.split_at(n);
            self.0 = tail;
            Ok(head)
        }
        fn i32(&mut self) -> Result<i32, String> {
            Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
        }
        fn bytes(&mut self) -> Result<Vec<u8>, String> {
            let len = self.i32()?;
            if len < 0 {
                return Err("negative thrift length".into());
            }
            Ok(self.take(len as usize)?.to_vec())
        }
    }
    let mut reader = Reader(data);
    let mut memo = Memo::default();
    loop {
        let field_type = reader.take(1)?[0];
        if field_type == 0 {
            return Ok(memo);
        }
        let id = i16::from_be_bytes(reader.take(2)?.try_into().unwrap());
        if id != 10 || field_type != 13 {
            return Err(format!("unexpected thrift memo field {id} type {field_type}"));
        }
        let key_type = reader.take(1)?[0];
        let value_type = reader.take(1)?[0];
        let size = reader.i32()?;
        if size < 0 || (size > 0 && (key_type != 11 || value_type != 11)) {
            return Err("unexpected thrift memo map".into());
        }
        let mut fields = HashMap::new();
        for _ in 0..size {
            let key = String::from_utf8(reader.bytes()?).map_err(|e| e.to_string())?;
            fields.insert(key, reader.bytes()?);
        }
        memo.fields = Some(fields);
    }
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

const MYSQL_MIN_DATETIME: &str = "1000-01-01 00:00:00.000000";
const POSTGRES_MIN_DATETIME: &str = "0001-01-01 00:00:00.000000";

fn micros_text(ns: i64) -> String {
    let micros = ns.div_euclid(1_000);
    let secs = micros.div_euclid(1_000_000);
    let sub_micros = micros.rem_euclid(1_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, sub_micros * 1_000)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .unwrap_or_else(|| MYSQL_MIN_DATETIME.to_string())
}

fn parse_micros_text(text: &str) -> Option<i64> {
    let parsed = chrono::NaiveDateTime::parse_from_str(text.trim(), "%Y-%m-%d %H:%M:%S%.f").ok()?;
    Some(parsed.and_utc().timestamp_micros().saturating_mul(1_000))
}

/// Parse a stored datetime; the dialect's minimum literal is Go's zero time.
fn datetime_cell_ns(text: Option<&str>) -> Option<i64> {
    let text = text?;
    if text.starts_with("1000-01-01 00:00:00") || text.starts_with("0001-01-01 00:00:00") {
        return None;
    }
    parse_micros_text(text)
}

#[derive(Serialize, Deserialize)]
struct PageToken {
    #[serde(rename = "Time")]
    time: String,
    #[serde(rename = "RunID")]
    run_id: String,
}

/// Go `time.Time` JSON (RFC3339Nano, trailing zeros trimmed).
fn rfc3339_nano(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let sub = ns.rem_euclid(1_000_000_000) as u32;
    let t = chrono::DateTime::from_timestamp(secs, sub).unwrap_or_default();
    let base = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    let frac = format!("{sub:09}");
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        format!("{base}Z")
    } else {
        format!("{base}.{frac}Z")
    }
}

fn parse_page_token(token: &[u8]) -> VisibilityResult<(i64, String)> {
    let parsed: PageToken = serde_json::from_slice(token).map_err(internal)?;
    let time = chrono::DateTime::parse_from_rfc3339(&parsed.time).map_err(internal)?;
    let ns = time
        .timestamp()
        .checked_mul(1_000_000_000)
        .and_then(|n| n.checked_add(i64::from(time.timestamp_subsec_nanos())))
        .ok_or_else(|| internal("page token time out of range"))?;
    Ok((ns, parsed.run_id))
}

// ---------------------------------------------------------------------------
// SQL backend
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dialect {
    MySql,
    Postgres,
    Sqlite,
}

#[derive(Debug, Clone)]
enum SqlParam {
    Int(Option<i64>),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
enum SqlCell {
    Null,
    Int(i64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
}

impl SqlCell {
    fn text(&self) -> Option<String> {
        match self {
            SqlCell::Text(v) => Some(v.clone()),
            SqlCell::Bytes(v) => Some(String::from_utf8_lossy(v).into_owned()),
            SqlCell::Int(v) => Some(v.to_string()),
            _ => None,
        }
    }
    fn int(&self) -> Option<i64> {
        match self {
            SqlCell::Int(v) => Some(*v),
            SqlCell::Bool(v) => Some(i64::from(*v)),
            SqlCell::Text(v) => v.parse().ok(),
            SqlCell::Bytes(v) => String::from_utf8_lossy(v).parse().ok(),
            SqlCell::Null => None,
        }
    }
    fn boolean(&self) -> bool {
        self.int().is_some_and(|v| v != 0)
    }
    fn bytes(&self) -> Vec<u8> {
        match self {
            SqlCell::Bytes(v) => v.clone(),
            SqlCell::Text(v) => v.clone().into_bytes(),
            _ => Vec::new(),
        }
    }
}

trait SqlConn: Send + Sync {
    /// Returns affected rows.
    fn exec(&self, sql: &str, params: Vec<SqlParam>) -> Result<u64, String>;
    fn query(&self, sql: &str, params: Vec<SqlParam>) -> Result<Vec<Vec<SqlCell>>, String>;
}

/// Rewrite `?` (plain), `?T` (timestamp text) and `?I` (integer) markers for
/// the dialect. PostgreSQL gets numbered, cast placeholders so the driver's
/// strict parameter typing accepts text timestamps and 64-bit integers.
fn render(dialect: Dialect, template: &str) -> String {
    let mut out = String::with_capacity(template.len() + 16);
    let chars: Vec<char> = template.chars().collect();
    let mut n = 0;
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '?' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        let kind = chars.get(i + 1).copied().filter(|c| matches!(c, 'T' | 'I'));
        // `?T`/`?I` markers are only recognized when not followed by an identifier
        // character (e.g. `?Total`), which no template uses.
        let consumed_marker = kind.is_some()
            && !chars
                .get(i + 2)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_');
        n += 1;
        match dialect {
            Dialect::Postgres => {
                out.push_str(&format!("${n}"));
                if consumed_marker {
                    out.push_str(match kind {
                        Some('T') => "::text::timestamp",
                        _ => "::bigint",
                    });
                }
            }
            _ => out.push('?'),
        }
        i += if consumed_marker { 2 } else { 1 };
    }
    out
}

fn ts_select(dialect: Dialect, column: &str) -> String {
    match dialect {
        Dialect::MySql => format!("DATE_FORMAT({column}, '%Y-%m-%d %H:%i:%s.%f')"),
        Dialect::Postgres => format!("to_char({column}, 'YYYY-MM-DD HH24:MI:SS.US')"),
        Dialect::Sqlite => column.to_string(),
    }
}

fn min_datetime(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Postgres => POSTGRES_MIN_DATETIME,
        _ => MYSQL_MIN_DATETIME,
    }
}

const STARTED_COLUMNS: &str = "domain_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time";
const STARTED_VALUES: &str = "?, ?, ?, ?T, ?T, ?, ?, ?, ?, ?I, ?T, ?I, ?I, ?, ?T";
const CLOSED_COLUMNS: &str = "domain_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, close_time, close_status, history_length, memo, encoding, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time";
const CLOSED_VALUES: &str = "?, ?, ?, ?T, ?T, ?, ?T, ?I, ?I, ?, ?, ?, ?I, ?T, ?I, ?I, ?, ?T";
const POSTGRES_CLOSED_CONFLICT: &str = " ON CONFLICT (domain_id, run_id) DO UPDATE SET workflow_id = excluded.workflow_id, start_time = excluded.start_time, execution_time = excluded.execution_time, workflow_type_name = excluded.workflow_type_name, close_time = excluded.close_time, close_status = excluded.close_status, history_length = excluded.history_length, memo = excluded.memo, encoding = excluded.encoding, is_cron = excluded.is_cron, num_clusters = excluded.num_clusters, update_time = excluded.update_time, shard_id = excluded.shard_id, execution_status = excluded.execution_status, cron_schedule = excluded.cron_schedule, scheduled_execution_time = excluded.scheduled_execution_time";

const SQLITE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS executions_visibility (
    domain_id CHAR(64) NOT NULL,
    run_id CHAR(64) NOT NULL,
    start_time DATETIME NOT NULL,
    execution_time DATETIME NOT NULL,
    workflow_id VARCHAR(255) NOT NULL,
    workflow_type_name VARCHAR(255) NOT NULL,
    close_status INT,
    close_time DATETIME NULL,
    history_length BIGINT,
    memo BLOB,
    encoding VARCHAR(64) NOT NULL,
    task_list VARCHAR(255) DEFAULT '' NOT NULL,
    is_cron BOOLEAN DEFAULT false NOT NULL,
    num_clusters INT NULL,
    update_time DATETIME NULL,
    shard_id INT NULL,
    cron_schedule TEXT NULL,
    execution_status INT NULL,
    scheduled_execution_time TIMESTAMP NULL,
    PRIMARY KEY (domain_id, run_id)
);
CREATE INDEX IF NOT EXISTS by_type_start_time ON executions_visibility (domain_id, workflow_type_name, close_status, start_time DESC, run_id);
CREATE INDEX IF NOT EXISTS by_workflow_id_start_time ON executions_visibility (domain_id, workflow_id, close_status, start_time DESC, run_id);
CREATE INDEX IF NOT EXISTS by_status_by_close_time ON executions_visibility (domain_id, close_status, start_time DESC, run_id);
CREATE INDEX IF NOT EXISTS by_close_time_by_status ON executions_visibility (domain_id, close_time DESC, run_id, close_status);";

pub struct SqlVisibilityStore {
    dialect: Dialect,
    conn: Box<dyn SqlConn>,
}

impl fmt::Debug for SqlVisibilityStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlVisibilityStore")
            .field("dialect", &self.dialect)
            .finish_non_exhaustive()
    }
}

impl SqlVisibilityStore {
    /// MySQL visibility over the same pool settings as the Matching adapter.
    /// Go's multiple-database sharding is not used by the visibility plugin
    /// configuration path and is rejected.
    pub fn mysql(config: &MySqlTaskStoreConfig) -> VisibilityResult<Self> {
        if config.use_multiple_databases {
            return Err(internal(
                "visibility persistence does not support multipleDatabasesConfig",
            ));
        }
        let pool = MySqlTaskStore::create_mysql_pool(config).map_err(internal)?;
        Ok(Self {
            dialect: Dialect::MySql,
            conn: Box::new(MySqlConn { pool }),
        })
    }

    pub fn postgres(
        shard: &PostgresShardConfig,
        max_conns: i64,
        max_idle_conns: i64,
        max_conn_lifetime: Option<Duration>,
    ) -> VisibilityResult<Self> {
        let pool = crate::matching_postgres::create_postgres_pool(
            shard,
            max_conns,
            max_idle_conns,
            max_conn_lifetime,
        )
        .map_err(internal)?;
        Ok(Self {
            dialect: Dialect::Postgres,
            conn: Box::new(PostgresConn { pool }),
        })
    }

    /// SQLite for embedded runs and tests; creates the table when missing.
    pub fn sqlite(path: &str) -> VisibilityResult<Self> {
        let connection = if path.is_empty() || path == ":memory:" {
            rusqlite::Connection::open_in_memory()
        } else {
            rusqlite::Connection::open(path)
        }
        .map_err(internal)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(internal)?;
        connection.execute_batch(SQLITE_SCHEMA).map_err(internal)?;
        Ok(Self {
            dialect: Dialect::Sqlite,
            conn: Box::new(SqliteConn {
                connection: Mutex::new(connection),
            }),
        })
    }

    fn ts(&self, ns: Option<i64>) -> SqlParam {
        SqlParam::Text(match ns {
            Some(ns) => micros_text(ns),
            None => min_datetime(self.dialect).to_string(),
        })
    }

    fn common_params_tail(&self, r: &VisibilityRecord) -> Vec<SqlParam> {
        vec![
            SqlParam::Bytes(r.memo.clone()),
            SqlParam::Text(r.encoding.clone()),
            SqlParam::Bool(r.is_cron),
            SqlParam::Int(Some(i64::from(r.num_clusters))),
            self.ts(r.update_time_ns),
            SqlParam::Int(Some(i64::from(r.shard_id))),
            SqlParam::Int(Some(i64::from(r.execution_status))),
            SqlParam::Text(r.cron_schedule.clone()),
            self.ts(r.scheduled_execution_time_ns),
        ]
    }

    fn select_columns(&self, closed: bool) -> String {
        let d = self.dialect;
        let mut cols = format!(
            "workflow_id, run_id, {}, {}, workflow_type_name, memo, encoding, is_cron, {}, shard_id",
            ts_select(d, "start_time"),
            ts_select(d, "execution_time"),
            ts_select(d, "update_time")
        );
        if closed {
            cols.push_str(&format!(
                ", {}, close_status, history_length",
                ts_select(d, "close_time")
            ));
        }
        cols
    }

    fn trim(&self, value: String) -> String {
        if self.dialect == Dialect::Postgres {
            value.trim().to_string()
        } else {
            value
        }
    }

    /// Open/closed list row layout from `select_columns`.
    fn list_row(&self, cells: &[SqlCell], closed: bool) -> VisibilityResult<InternalInfo> {
        if cells.len() < if closed { 13 } else { 10 } {
            return Err(internal("short visibility row"));
        }
        let text = |i: usize| cells[i].text().unwrap_or_default();
        let ts = |i: usize| datetime_cell_ns(cells[i].text().as_deref());
        let close = if closed && cells[11].int().is_some() {
            Some(InternalClose {
                close_ns: ts(10).unwrap_or(GO_ZERO_TIME_NANOS),
                status: cells[11].int().unwrap_or_default() as i32,
                history_length: cells[12].int().unwrap_or_default(),
            })
        } else {
            None
        };
        Ok(InternalInfo {
            workflow_id: self.trim(text(0)),
            run_id: self.trim(text(1)),
            start_ns: ts(2).unwrap_or(GO_ZERO_TIME_NANOS),
            execution_ns: ts(3).unwrap_or(GO_ZERO_TIME_NANOS),
            type_name: text(4),
            memo: cells[5].bytes(),
            encoding: text(6),
            is_cron: cells[7].boolean(),
            // Not selected by Go's SQL list queries.
            task_list: String::new(),
            cron_schedule: String::new(),
            execution_status: 0,
            scheduled_ns: GO_ZERO_TIME_NANOS,
            close,
        })
    }

    fn sql_filter_and_params(
        &self,
        closed: bool,
        request: &VisibilityListRequest,
        max_ns: i64,
        run_id: &str,
    ) -> (String, Vec<SqlParam>) {
        let (clause, first): (&str, Option<SqlParam>) = match &request.filter {
            ListFilter::None => ("", None),
            ListFilter::WorkflowType(name) => (
                "AND workflow_type_name = ?",
                Some(SqlParam::Text(name.clone())),
            ),
            ListFilter::WorkflowId(id) => ("AND workflow_id = ?", Some(SqlParam::Text(id.clone()))),
            ListFilter::CloseStatus(status) => (
                "AND close_status = ?I",
                Some(SqlParam::Int(Some(i64::from(*status)))),
            ),
        };
        let state = if closed {
            "close_status IS NOT NULL"
        } else {
            "close_status IS NULL"
        };
        let template = format!(
            "SELECT {} FROM executions_visibility WHERE {state} {clause} AND domain_id = ? AND start_time >= ?T AND start_time <= ?T AND (run_id > ? OR start_time < ?T) ORDER BY start_time DESC, run_id LIMIT ?I",
            self.select_columns(closed)
        );
        let mut params: Vec<SqlParam> = first.into_iter().collect();
        let min_text = self.ts(Some(request.earliest_time_ns));
        // Go quirk: the by-WorkflowID query compares against MinStartTime, all
        // other queries against MaxStartTime.
        let compare = if matches!(request.filter, ListFilter::WorkflowId(_)) {
            min_text.clone()
        } else {
            self.ts(Some(max_ns))
        };
        params.extend([
            SqlParam::Text(request.domain_id.clone()),
            min_text,
            self.ts(Some(max_ns)),
            SqlParam::Text(run_id.to_string()),
            compare,
            SqlParam::Int(Some(i64::from(request.page_size))),
        ]);
        (render(self.dialect, &template), params)
    }
}

impl VisibilityManager for SqlVisibilityStore {
    fn name(&self) -> &'static str {
        match self.dialect {
            Dialect::MySql => "mysql",
            Dialect::Postgres => "postgres",
            Dialect::Sqlite => "sqlite",
        }
    }

    fn record_started(&self, r: &VisibilityRecord, _timeout: i64) -> VisibilityResult<()> {
        let sql = match self.dialect {
            Dialect::MySql => format!(
                "INSERT IGNORE INTO executions_visibility ({STARTED_COLUMNS}) VALUES ({STARTED_VALUES})"
            ),
            Dialect::Postgres => format!(
                "INSERT INTO executions_visibility ({STARTED_COLUMNS}) VALUES ({STARTED_VALUES}) ON CONFLICT (domain_id, run_id) DO NOTHING"
            ),
            Dialect::Sqlite => format!(
                "INSERT OR IGNORE INTO executions_visibility ({STARTED_COLUMNS}) VALUES ({STARTED_VALUES})"
            ),
        };
        let mut params = vec![
            SqlParam::Text(r.domain_id.clone()),
            SqlParam::Text(r.workflow_id.clone()),
            SqlParam::Text(r.run_id.clone()),
            self.ts(r.start_time_ns),
            self.ts(r.execution_time_ns),
            SqlParam::Text(r.workflow_type_name.clone()),
        ];
        params.extend(self.common_params_tail(r));
        self.conn
            .exec(&render(self.dialect, &sql), params)
            .map(|_| ())
            .map_err(internal)
    }

    fn record_closed(&self, r: &VisibilityRecord, _retention: i64) -> VisibilityResult<()> {
        let sql = match self.dialect {
            Dialect::MySql => format!(
                "REPLACE INTO executions_visibility ({CLOSED_COLUMNS}) VALUES ({CLOSED_VALUES})"
            ),
            Dialect::Postgres => format!(
                "INSERT INTO executions_visibility ({CLOSED_COLUMNS}) VALUES ({CLOSED_VALUES}){POSTGRES_CLOSED_CONFLICT}"
            ),
            Dialect::Sqlite => format!(
                "INSERT OR REPLACE INTO executions_visibility ({CLOSED_COLUMNS}) VALUES ({CLOSED_VALUES})"
            ),
        };
        let mut params = vec![
            SqlParam::Text(r.domain_id.clone()),
            SqlParam::Text(r.workflow_id.clone()),
            SqlParam::Text(r.run_id.clone()),
            self.ts(r.start_time_ns),
            self.ts(r.execution_time_ns),
            SqlParam::Text(r.workflow_type_name.clone()),
            self.ts(r.close_time_ns),
            SqlParam::Int(Some(i64::from(r.close_status))),
            SqlParam::Int(Some(r.history_length)),
        ];
        params.extend(self.common_params_tail(r));
        let affected = self
            .conn
            .exec(&render(self.dialect, &sql), params)
            .map_err(internal)?;
        if affected > 2 {
            // either adds a new row or deletes the old row and adds a new row
            return Err(VisibilityError::Internal(format!(
                "RecordWorkflowExecutionClosed unexpected numRows ({affected}) updated"
            )));
        }
        Ok(())
    }

    fn list(&self, request: &VisibilityListRequest) -> VisibilityResult<VisibilityListResponse> {
        let (max_ns, run_id) = if request.next_page_token.is_empty() {
            (request.latest_time_ns, String::new())
        } else {
            parse_page_token(&request.next_page_token)?
        };
        let closed = request.closed || matches!(request.filter, ListFilter::CloseStatus(_));
        let (sql, params) = self.sql_filter_and_params(closed, request, max_ns, &run_id);
        let rows = self.conn.query(&sql, params).map_err(internal)?;
        if rows.is_empty() {
            return Ok(VisibilityListResponse::default());
        }
        let mut infos = Vec::with_capacity(rows.len());
        for row in &rows {
            infos.push(self.list_row(row, closed)?);
        }
        let last = infos.last().expect("rows are not empty");
        // Go: a token is returned whenever the last start time is after the
        // earliest bound, regardless of whether the page is full.
        let next_page_token = if last.start_ns.saturating_sub(request.earliest_time_ns) > 0 {
            serde_json::to_vec(&PageToken {
                time: rfc3339_nano(last.start_ns),
                run_id: last.run_id.clone(),
            })
            .map_err(internal)?
        } else {
            Vec::new()
        };
        Ok(VisibilityListResponse {
            executions: infos.into_iter().map(InternalInfo::into_info).collect(),
            next_page_token,
        })
    }

    fn get_closed(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> VisibilityResult<WorkflowExecutionInfo> {
        let d = self.dialect;
        let template = format!(
            "SELECT workflow_id, run_id, {}, {}, memo, encoding, {}, workflow_type_name, close_status, history_length, is_cron, {}, shard_id FROM executions_visibility WHERE domain_id = ? AND close_status IS NOT NULL AND run_id = ?",
            ts_select(d, "start_time"),
            ts_select(d, "execution_time"),
            ts_select(d, "close_time"),
            ts_select(d, "update_time"),
        );
        let rows = self
            .conn
            .query(
                &render(d, &template),
                vec![
                    SqlParam::Text(domain_id.to_string()),
                    SqlParam::Text(run_id.to_string()),
                ],
            )
            .map_err(internal)?;
        let Some(cells) = rows.first() else {
            return Err(VisibilityError::EntityNotExists(format!(
                "Workflow execution not found.  WorkflowId: {workflow_id}, RunId: {run_id}"
            )));
        };
        if cells.len() < 13 {
            return Err(internal("short visibility row"));
        }
        let ts = |i: usize| datetime_cell_ns(cells[i].text().as_deref());
        let info = InternalInfo {
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            start_ns: ts(2).unwrap_or(GO_ZERO_TIME_NANOS),
            execution_ns: ts(3).unwrap_or(GO_ZERO_TIME_NANOS),
            memo: cells[4].bytes(),
            encoding: cells[5].text().unwrap_or_default(),
            type_name: cells[7].text().unwrap_or_default(),
            is_cron: cells[10].boolean(),
            task_list: String::new(),
            cron_schedule: String::new(),
            execution_status: 0,
            scheduled_ns: GO_ZERO_TIME_NANOS,
            close: Some(InternalClose {
                close_ns: ts(6).unwrap_or(GO_ZERO_TIME_NANOS),
                status: cells[8].int().unwrap_or_default() as i32,
                history_length: cells[9].int().unwrap_or_default(),
            }),
        };
        Ok(info.into_info())
    }

    fn delete(
        &self,
        domain_id: &str,
        _workflow_id: &str,
        run_id: &str,
        _admin: bool,
    ) -> VisibilityResult<()> {
        self.conn
            .exec(
                &render(
                    self.dialect,
                    "DELETE FROM executions_visibility WHERE domain_id=? AND run_id=?",
                ),
                vec![
                    SqlParam::Text(domain_id.to_string()),
                    SqlParam::Text(run_id.to_string()),
                ],
            )
            .map(|_| ())
            .map_err(internal)
    }
}

struct MySqlConn {
    pool: Arc<SyncConnectionPool>,
}

impl MySqlConn {
    fn lease(&self) -> Result<ConnectionLease, String> {
        self.pool.checkout().map_err(|e| e.to_string())
    }
}

fn mysql_params(params: Vec<SqlParam>) -> mysql::Params {
    mysql::Params::Positional(
        params
            .into_iter()
            .map(|p| match p {
                SqlParam::Int(Some(v)) => mysql::Value::Int(v),
                SqlParam::Int(None) => mysql::Value::NULL,
                SqlParam::Bool(v) => mysql::Value::Int(i64::from(v)),
                SqlParam::Text(v) => mysql::Value::Bytes(v.into_bytes()),
                SqlParam::Bytes(v) => mysql::Value::Bytes(v),
            })
            .collect(),
    )
}

impl SqlConn for MySqlConn {
    fn exec(&self, sql: &str, params: Vec<SqlParam>) -> Result<u64, String> {
        use mysql::prelude::Queryable;
        let mut lease = self.lease()?;
        match lease.exec_drop(sql, mysql_params(params)) {
            Ok(()) => Ok(lease.affected_rows()),
            Err(error) => {
                lease.discard_on_connectivity_error(&error);
                Err(error.to_string())
            }
        }
    }

    fn query(&self, sql: &str, params: Vec<SqlParam>) -> Result<Vec<Vec<SqlCell>>, String> {
        use mysql::prelude::Queryable;
        let mut lease = self.lease()?;
        let rows: Vec<mysql::Row> = match lease.exec(sql, mysql_params(params)) {
            Ok(rows) => rows,
            Err(error) => {
                lease.discard_on_connectivity_error(&error);
                return Err(error.to_string());
            }
        };
        Ok(rows
            .into_iter()
            .map(|row| {
                row.unwrap()
                    .into_iter()
                    .map(|value| match value {
                        mysql::Value::NULL => SqlCell::Null,
                        mysql::Value::Bytes(b) => SqlCell::Bytes(b),
                        mysql::Value::Int(v) => SqlCell::Int(v),
                        mysql::Value::UInt(v) => SqlCell::Int(v as i64),
                        other => SqlCell::Text(format!("{other:?}")),
                    })
                    .collect()
            })
            .collect())
    }
}

struct PostgresConn {
    pool: Arc<PostgresConnectionPool>,
}

fn postgres_params(params: &[SqlParam]) -> Vec<Box<dyn postgres::types::ToSql + Sync>> {
    params
        .iter()
        .map(|p| -> Box<dyn postgres::types::ToSql + Sync> {
            match p {
                SqlParam::Int(v) => Box::new(*v),
                SqlParam::Bool(v) => Box::new(*v),
                SqlParam::Text(v) => Box::new(v.clone()),
                SqlParam::Bytes(v) => Box::new(v.clone()),
            }
        })
        .collect()
}

fn postgres_cell(row: &postgres::Row, index: usize) -> Result<SqlCell, String> {
    use postgres::types::Type;
    let ty = row.columns()[index].type_().clone();
    let err = |e: postgres::Error| e.to_string();
    Ok(if ty == Type::TEXT || ty == Type::VARCHAR || ty == Type::BPCHAR || ty == Type::NAME {
        row.try_get::<_, Option<String>>(index)
            .map_err(err)?
            .map_or(SqlCell::Null, SqlCell::Text)
    } else if ty == Type::INT2 {
        row.try_get::<_, Option<i16>>(index)
            .map_err(err)?
            .map_or(SqlCell::Null, |v| SqlCell::Int(i64::from(v)))
    } else if ty == Type::INT4 {
        row.try_get::<_, Option<i32>>(index)
            .map_err(err)?
            .map_or(SqlCell::Null, |v| SqlCell::Int(i64::from(v)))
    } else if ty == Type::INT8 {
        row.try_get::<_, Option<i64>>(index)
            .map_err(err)?
            .map_or(SqlCell::Null, SqlCell::Int)
    } else if ty == Type::BOOL {
        row.try_get::<_, Option<bool>>(index)
            .map_err(err)?
            .map_or(SqlCell::Null, SqlCell::Bool)
    } else if ty == Type::BYTEA {
        row.try_get::<_, Option<Vec<u8>>>(index)
            .map_err(err)?
            .map_or(SqlCell::Null, SqlCell::Bytes)
    } else {
        return Err(format!("unsupported PostgreSQL column type {ty}"));
    })
}

impl SqlConn for PostgresConn {
    fn exec(&self, sql: &str, params: Vec<SqlParam>) -> Result<u64, String> {
        let mut client = self.pool.checkout().map_err(|e| e.to_string())?;
        let boxed = postgres_params(&params);
        let refs: Vec<&(dyn postgres::types::ToSql + Sync)> = boxed.iter().map(|b| &**b).collect();
        client.execute(sql, &refs).map_err(|error| {
            client.poison_if_closed(&error);
            error.to_string()
        })
    }

    fn query(&self, sql: &str, params: Vec<SqlParam>) -> Result<Vec<Vec<SqlCell>>, String> {
        let mut client = self.pool.checkout().map_err(|e| e.to_string())?;
        let boxed = postgres_params(&params);
        let refs: Vec<&(dyn postgres::types::ToSql + Sync)> = boxed.iter().map(|b| &**b).collect();
        let rows = client.query(sql, &refs).map_err(|error| {
            client.poison_if_closed(&error);
            error.to_string()
        })?;
        rows.iter()
            .map(|row| (0..row.len()).map(|i| postgres_cell(row, i)).collect())
            .collect()
    }
}

struct SqliteConn {
    connection: Mutex<rusqlite::Connection>,
}

fn sqlite_params(params: Vec<SqlParam>) -> Vec<rusqlite::types::Value> {
    use rusqlite::types::Value;
    params
        .into_iter()
        .map(|p| match p {
            SqlParam::Int(Some(v)) => Value::Integer(v),
            SqlParam::Int(None) => Value::Null,
            SqlParam::Bool(v) => Value::Integer(i64::from(v)),
            SqlParam::Text(v) => Value::Text(v),
            SqlParam::Bytes(v) => Value::Blob(v),
        })
        .collect()
}

impl SqlConn for SqliteConn {
    fn exec(&self, sql: &str, params: Vec<SqlParam>) -> Result<u64, String> {
        let connection = self.connection.lock().map_err(|e| e.to_string())?;
        connection
            .execute(sql, rusqlite::params_from_iter(sqlite_params(params)))
            .map(|n| n as u64)
            .map_err(|e| e.to_string())
    }

    fn query(&self, sql: &str, params: Vec<SqlParam>) -> Result<Vec<Vec<SqlCell>>, String> {
        use rusqlite::types::ValueRef;
        let connection = self.connection.lock().map_err(|e| e.to_string())?;
        let mut statement = connection.prepare(sql).map_err(|e| e.to_string())?;
        let columns = statement.column_count();
        let mut rows = statement
            .query(rusqlite::params_from_iter(sqlite_params(params)))
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut cells = Vec::with_capacity(columns);
            for i in 0..columns {
                cells.push(match row.get_ref(i).map_err(|e| e.to_string())? {
                    ValueRef::Null => SqlCell::Null,
                    ValueRef::Integer(v) => SqlCell::Int(v),
                    ValueRef::Real(v) => SqlCell::Text(v.to_string()),
                    ValueRef::Text(v) => SqlCell::Text(String::from_utf8_lossy(v).into_owned()),
                    ValueRef::Blob(v) => SqlCell::Bytes(v.to_vec()),
                });
            }
            out.push(cells);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Cassandra backend
// ---------------------------------------------------------------------------

const OPEN_COLUMNS: &str = " workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time ";
const CLOSED_CQL_COLUMNS: &str = " workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, encoding, task_list, is_cron, num_clusters, update_time, shard_id, execution_status, cron_schedule, scheduled_execution_time ";

pub struct CassandraVisibilityStore {
    driver: Arc<crate::cassandra::CassandraBlockingSession>,
    sort_by_close_time: bool,
}

impl fmt::Debug for CassandraVisibilityStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraVisibilityStore")
            .field("sort_by_close_time", &self.sort_by_close_time)
            .finish_non_exhaustive()
    }
}

fn cql_ts(ns: Option<i64>) -> scylla::value::CqlTimestamp {
    scylla::value::CqlTimestamp(ns.unwrap_or(GO_ZERO_TIME_NANOS).div_euclid(1_000_000))
}

fn cql_uuid(value: &str, what: &str) -> VisibilityResult<uuid::Uuid> {
    uuid::Uuid::parse_str(value)
        .map_err(|error| VisibilityError::BadRequest(format!("invalid {what} {value:?}: {error}")))
}

impl CassandraVisibilityStore {
    /// `sort_by_close_time` is Go's `listClosedOrderingByCloseTime`.
    pub fn new(
        driver: Arc<crate::cassandra::CassandraBlockingSession>,
        sort_by_close_time: bool,
    ) -> Self {
        Self {
            driver,
            sort_by_close_time,
        }
    }

    fn insert_values(&self, r: &VisibilityRecord, closed: bool) -> VisibilityResult<BoundValues> {
        let mut values: BoundValues = vec![
            bound(cql_uuid(&r.domain_id, "domain ID")?),
            bound(DOMAIN_PARTITION),
            bound(r.workflow_id.clone()),
            bound(cql_uuid(&r.run_id, "run ID")?),
            bound(cql_ts(r.start_time_ns)),
            bound(cql_ts(r.execution_time_ns)),
        ];
        if closed {
            values.push(bound(cql_ts(r.close_time_ns)));
        }
        values.push(bound(r.workflow_type_name.clone()));
        if closed {
            values.push(bound(r.close_status));
            values.push(bound(r.history_length));
        }
        values.extend([
            bound(r.memo.clone()),
            bound(r.encoding.clone()),
            bound(r.task_list.clone()),
            bound(r.is_cron),
            bound(r.num_clusters),
            bound(cql_ts(r.update_time_ns)),
            bound(r.shard_id),
            bound(r.execution_status),
            bound(r.cron_schedule.clone()),
            bound(cql_ts(r.scheduled_execution_time_ns)),
        ]);
        Ok(values)
    }

    fn run_cql(
        &self,
        statement: scylla::statement::Statement,
        values: &[Box<dyn BoundValue>],
    ) -> VisibilityResult<scylla::response::query_result::QueryResult> {
        let session = self.driver.session();
        self.driver
            .run(session.query_unpaged(statement, values))
            .map_err(internal)
    }

    fn insert_statement(table: &str, closed: bool, ttl: bool) -> String {
        let (columns, count) = if closed {
            (CLOSED_CQL_COLUMNS, 20)
        } else {
            (OPEN_COLUMNS, 17)
        };
        let placeholders = vec!["?"; count].join(", ");
        format!(
            "INSERT INTO {table} (domain_id, domain_partition, {columns})VALUES ({placeholders}){}",
            if ttl { " using TTL ?" } else { "" }
        )
    }

    fn cql_text(&self, closed: bool, filter: &ListFilter) -> String {
        let (table, columns, time_column) = if !closed {
            ("open_executions", OPEN_COLUMNS, "start_time")
        } else if self.sort_by_close_time {
            ("closed_executions_v2", CLOSED_CQL_COLUMNS, "close_time")
        } else {
            ("closed_executions", CLOSED_CQL_COLUMNS, "start_time")
        };
        let partition = if matches!(filter, ListFilter::None) {
            "domain_partition IN (?)"
        } else {
            "domain_partition = ?"
        };
        let extra = match filter {
            ListFilter::None => "",
            ListFilter::WorkflowType(_) => "AND workflow_type_name = ? ",
            ListFilter::WorkflowId(_) => "AND workflow_id = ? ",
            ListFilter::CloseStatus(_) => "AND status = ? ",
        };
        format!(
            "SELECT {columns}FROM {table} WHERE domain_id = ? AND {partition} AND {time_column} >= ? AND {time_column} <= ? {extra}"
        )
    }

    fn row_to_info(row: &scylla::value::Row, closed: bool) -> InternalInfo {
        use scylla::value::CqlValue;
        let cell = |i: usize| row.columns.get(i).and_then(|c| c.as_ref());
        let text = |i: usize| match cell(i) {
            Some(CqlValue::Text(v)) | Some(CqlValue::Ascii(v)) => v.clone(),
            _ => String::new(),
        };
        let ns = |i: usize| match cell(i) {
            Some(CqlValue::Timestamp(t)) => t.0.saturating_mul(1_000_000),
            _ => GO_ZERO_TIME_NANOS,
        };
        let int = |i: usize| -> i64 {
            match cell(i) {
                Some(CqlValue::Int(v)) => i64::from(*v),
                Some(CqlValue::SmallInt(v)) => i64::from(*v),
                Some(CqlValue::BigInt(v)) => *v,
                _ => 0,
            }
        };
        let uuid_text = |i: usize| match cell(i) {
            Some(CqlValue::Uuid(v)) => v.to_string(),
            Some(CqlValue::Timeuuid(v)) => uuid::Uuid::from(*v).to_string(),
            _ => String::new(),
        };
        let blob = |i: usize| match cell(i) {
            Some(CqlValue::Blob(v)) => v.clone(),
            _ => Vec::new(),
        };
        let boolean = |i: usize| matches!(cell(i), Some(CqlValue::Boolean(true)));
        if closed {
            InternalInfo {
                workflow_id: text(0),
                run_id: uuid_text(1),
                start_ns: ns(2),
                execution_ns: ns(3),
                type_name: text(5),
                memo: blob(8),
                encoding: text(9),
                task_list: text(10),
                is_cron: boolean(11),
                execution_status: int(15) as i32,
                cron_schedule: text(16),
                scheduled_ns: ns(17),
                close: Some(InternalClose {
                    close_ns: ns(4),
                    status: int(6) as i32,
                    history_length: int(7),
                }),
            }
        } else {
            InternalInfo {
                workflow_id: text(0),
                run_id: uuid_text(1),
                start_ns: ns(2),
                execution_ns: ns(3),
                type_name: text(4),
                memo: blob(5),
                encoding: text(6),
                task_list: text(7),
                is_cron: boolean(8),
                execution_status: int(12) as i32,
                cron_schedule: text(13),
                scheduled_ns: ns(14),
                close: None,
            }
        }
    }
}

impl VisibilityManager for CassandraVisibilityStore {
    fn name(&self) -> &'static str {
        "cassandra"
    }

    fn record_started(&self, r: &VisibilityRecord, timeout: i64) -> VisibilityResult<()> {
        use scylla::statement::Statement;
        let ttl = timeout + OPEN_EXECUTION_TTL_BUFFER;
        let use_ttl = ttl <= MAX_CASSANDRA_TTL;
        let mut values = self.insert_values(r, false)?;
        if use_ttl {
            values.push(bound(ttl as i32));
        }
        let mut statement = Statement::new(Self::insert_statement("open_executions", false, use_ttl));
        statement.set_timestamp(Some(cql_ts(r.start_time_ns).0));
        self.run_cql(statement, &values).map(|_| ())
    }

    fn record_closed(&self, r: &VisibilityRecord, retention: i64) -> VisibilityResult<()> {
        use scylla::statement::{
            batch::{Batch, BatchType},
            Statement,
        };
        let retention = if retention == 0 {
            DEFAULT_CLOSE_TTL_SECONDS
        } else {
            retention
        };
        let use_ttl = retention <= MAX_CASSANDRA_TTL;
        let mut batch = Batch::new(BatchType::Logged);
        let mut batch_values: Vec<BoundValues> = Vec::new();
        // First, remove the execution from the open table.
        batch.append_statement(Statement::new(
            "DELETE FROM open_executions WHERE domain_id = ? AND domain_partition = ? AND start_time = ? AND run_id = ?",
        ));
        batch_values.push(vec![
            bound(cql_uuid(&r.domain_id, "domain ID")?),
            bound(DOMAIN_PARTITION),
            bound(cql_ts(r.start_time_ns)),
            bound(cql_uuid(&r.run_id, "run ID")?),
        ]);
        // Then add rows to closed_executions and the close-time ordered v2.
        for table in ["closed_executions", "closed_executions_v2"] {
            batch.append_statement(Statement::new(Self::insert_statement(table, true, use_ttl)));
            let mut values = self.insert_values(r, true)?;
            if use_ttl {
                values.push(bound(retention as i32));
            }
            batch_values.push(values);
        }
        // CloseTimestamp can be before StartTimestamp; use start + 1s then so
        // the open-row deletion is not ignored (Go #1501).
        let start_ns = r.start_time_ns.unwrap_or(GO_ZERO_TIME_NANOS);
        let close_ns = r.close_time_ns.unwrap_or(GO_ZERO_TIME_NANOS);
        let query_ns = if close_ns < start_ns {
            start_ns.saturating_add(1_000_000_000)
        } else {
            close_ns
        };
        batch.set_timestamp(Some(query_ns.div_euclid(1_000_000)));
        let session = self.driver.session();
        self.driver
            .run(session.batch(&batch, batch_values))
            .map(|_| ())
            .map_err(internal)
    }

    fn list(&self, request: &VisibilityListRequest) -> VisibilityResult<VisibilityListResponse> {
        use scylla::{response::PagingState, statement::Statement};
        let closed = request.closed || matches!(request.filter, ListFilter::CloseStatus(_));
        let mut statement = Statement::new(self.cql_text(closed, &request.filter));
        statement.set_consistency(scylla::frame::types::Consistency::LocalOne);
        statement.set_page_size(request.page_size);
        let mut values: BoundValues = vec![
            bound(cql_uuid(&request.domain_id, "domain ID")?),
            bound(DOMAIN_PARTITION),
            bound(cql_ts(Some(request.earliest_time_ns))),
            bound(cql_ts(Some(request.latest_time_ns))),
        ];
        match &request.filter {
            ListFilter::None => {}
            ListFilter::WorkflowType(v) | ListFilter::WorkflowId(v) => values.push(bound(v.clone())),
            ListFilter::CloseStatus(v) => values.push(bound(*v)),
        }
        let paging_state = if request.next_page_token.is_empty() {
            PagingState::start()
        } else {
            PagingState::new_from_raw_bytes(request.next_page_token.clone())
        };
        let session = self.driver.session();
        let (result, paging) = self
            .driver
            .run(session.query_single_page(statement, &values[..], paging_state))
            .map_err(internal)?;
        let next_page_token = match paging.into_paging_control_flow() {
            std::ops::ControlFlow::Continue(state) => {
                state.as_bytes_slice().map(|b| b.to_vec()).unwrap_or_default()
            }
            std::ops::ControlFlow::Break(()) => Vec::new(),
        };
        let rows = result.into_rows_result().map_err(internal)?;
        let mut executions = Vec::new();
        for row in rows.rows::<scylla::value::Row>().map_err(internal)? {
            let row = row.map_err(internal)?;
            executions.push(Self::row_to_info(&row, closed).into_info());
        }
        Ok(VisibilityListResponse {
            executions,
            next_page_token,
        })
    }

    fn get_closed(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> VisibilityResult<WorkflowExecutionInfo> {
        use scylla::statement::Statement;
        let statement = Statement::new(format!(
            "SELECT {CLOSED_CQL_COLUMNS}FROM closed_executions WHERE domain_id = ? AND domain_partition = ? AND workflow_id = ? AND run_id = ? ALLOW FILTERING "
        ));
        let values: BoundValues = vec![
            bound(cql_uuid(domain_id, "domain ID")?),
            bound(DOMAIN_PARTITION),
            bound(workflow_id.to_string()),
            bound(cql_uuid(run_id, "run ID")?),
        ];
        let rows = self
            .run_cql(statement, &values)?
            .into_rows_result()
            .map_err(internal)?;
        let first = rows
            .maybe_first_row::<scylla::value::Row>()
            .map_err(internal)?;
        let Some(row) = first else {
            return Err(VisibilityError::EntityNotExists(format!(
                "Workflow execution not found.  WorkflowId: {workflow_id}, RunId: {run_id}"
            )));
        };
        Ok(Self::row_to_info(&row, true).into_info())
    }

    fn delete(
        &self,
        domain_id: &str,
        _workflow_id: &str,
        run_id: &str,
        admin: bool,
    ) -> VisibilityResult<()> {
        use scylla::statement::Statement;
        // Normally Cassandra visibility relies on TTL; only admin deletes act.
        if !admin {
            return Ok(());
        }
        let domain = cql_uuid(domain_id, "domain ID")?;
        let run = cql_uuid(run_id, "run ID")?;
        let select = Statement::new(format!(
            "SELECT {OPEN_COLUMNS}FROM open_executions WHERE domain_id = ? AND domain_partition = ? AND run_id = ? ALLOW FILTERING "
        ));
        let rows = self
            .run_cql(
                select,
                &[bound(domain), bound(DOMAIN_PARTITION), bound(run)],
            )?
            .into_rows_result()
            .map_err(internal)?;
        let Some(row) = rows
            .maybe_first_row::<scylla::value::Row>()
            .map_err(internal)?
        else {
            return Ok(());
        };
        let start_time = match row.columns.get(2).and_then(|c| c.as_ref()) {
            Some(scylla::value::CqlValue::Timestamp(t)) => *t,
            _ => return Ok(()),
        };
        let mut delete = Statement::new(
            "DELETE FROM open_executions WHERE domain_id = ? and domain_partition = ? and start_time = ? and run_id = ? ",
        );
        delete.set_consistency(scylla::frame::types::Consistency::All);
        self.run_cql(
            delete,
            &[
                bound(domain),
                bound(DOMAIN_PARTITION),
                bound(start_time),
                bound(run),
            ],
        )
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOMAIN: &str = "11111111-1111-1111-1111-111111111111";

    fn record(workflow_id: &str, run_id: &str, type_name: &str, start_secs: i64) -> VisibilityRecord {
        VisibilityRecord {
            domain_id: DOMAIN.into(),
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            workflow_type_name: type_name.into(),
            start_time_ns: Some(start_secs * 1_000_000_000),
            execution_time_ns: Some(start_secs * 1_000_000_000),
            encoding: "thriftrw".into(),
            task_list: "tl".into(),
            execution_status: 1,
            update_time_ns: Some(start_secs * 1_000_000_000),
            ..Default::default()
        }
    }

    fn request(closed: bool, filter: ListFilter, page_size: i32, token: Vec<u8>) -> VisibilityListRequest {
        VisibilityListRequest {
            domain_id: DOMAIN.into(),
            earliest_time_ns: 0,
            latest_time_ns: 10_000 * 1_000_000_000,
            page_size,
            next_page_token: token,
            closed,
            filter,
        }
    }

    fn ids(response: &VisibilityListResponse) -> Vec<String> {
        response
            .executions
            .iter()
            .map(|e| e.execution.as_ref().unwrap().run_id.clone())
            .collect()
    }

    #[test]
    fn render_rewrites_markers_per_dialect() {
        assert_eq!(render(Dialect::MySql, "a = ?T AND b = ?I AND c = ?"), "a = ? AND b = ? AND c = ?");
        assert_eq!(
            render(Dialect::Postgres, "a = ?T AND b = ?I AND c = ?"),
            "a = $1::text::timestamp AND b = $2::bigint AND c = $3"
        );
    }

    #[test]
    fn page_token_round_trips_go_time_json() {
        let ns = 1_700_000_000_123_456_000;
        let token = serde_json::to_vec(&PageToken {
            time: rfc3339_nano(ns),
            run_id: "r".into(),
        })
        .unwrap();
        assert_eq!(
            String::from_utf8(token.clone()).unwrap(),
            r#"{"Time":"2023-11-14T22:13:20.123456Z","RunID":"r"}"#
        );
        assert_eq!(parse_page_token(&token).unwrap(), (ns, "r".to_string()));
    }

    #[test]
    fn thrift_memo_round_trips() {
        let mut fields = HashMap::new();
        fields.insert("k".to_string(), b"v".to_vec());
        let memo = Memo {
            fields: Some(fields),
        };
        let bytes = encode_thrift_memo(&memo);
        assert_eq!(decode_memo(&bytes, "thriftrw"), Some(memo));
        assert_eq!(decode_memo(&[], "thriftrw"), Some(Memo::default()));
        assert_eq!(decode_memo(b"x", "bogus"), None);
    }

    #[test]
    fn sqlite_open_list_paginates_with_go_tokens() {
        let store = SqlVisibilityStore::sqlite(":memory:").unwrap();
        for (i, run) in ["a", "b", "c"].iter().enumerate() {
            store
                .record_started(&record(&format!("wf{i}"), run, "T", 100 + i as i64), 0)
                .unwrap();
        }
        let first = store.list(&request(false, ListFilter::None, 2, vec![])).unwrap();
        assert_eq!(ids(&first), vec!["c", "b"]);
        assert!(!first.next_page_token.is_empty());
        let second = store
            .list(&request(false, ListFilter::None, 2, first.next_page_token))
            .unwrap();
        assert_eq!(ids(&second), vec!["a"]);
        let info = &second.executions[0];
        assert_eq!(info.start_time, Some(100 * 1_000_000_000));
        assert_eq!(info.close_status, None);
        assert_eq!(info.execution_status, Some(WorkflowExecutionStatus::Pending));
    }

    #[test]
    fn sqlite_close_moves_open_to_closed_and_filters() {
        let store = SqlVisibilityStore::sqlite(":memory:").unwrap();
        store.record_started(&record("w1", "r1", "TypeA", 100), 0).unwrap();
        store.record_started(&record("w2", "r2", "TypeB", 101), 0).unwrap();
        let mut closed = record("w1", "r1", "TypeA", 100);
        closed.close_time_ns = Some(200 * 1_000_000_000);
        closed.close_status = 3;
        closed.history_length = 7;
        store.record_closed(&closed, 0).unwrap();

        let open = store.list(&request(false, ListFilter::None, 10, vec![])).unwrap();
        assert_eq!(ids(&open), vec!["r2"]);
        let all_closed = store.list(&request(true, ListFilter::None, 10, vec![])).unwrap();
        assert_eq!(ids(&all_closed), vec!["r1"]);
        let info = &all_closed.executions[0];
        assert_eq!(info.close_status, Some(WorkflowExecutionCloseStatus::Terminated));
        assert_eq!(info.history_length, 7);
        assert_eq!(info.close_time, Some(200 * 1_000_000_000));

        let by_status = store
            .list(&request(true, ListFilter::CloseStatus(3), 10, vec![]))
            .unwrap();
        assert_eq!(ids(&by_status), vec!["r1"]);
        let wrong_status = store
            .list(&request(true, ListFilter::CloseStatus(0), 10, vec![]))
            .unwrap();
        assert!(wrong_status.executions.is_empty());
        let by_type = store
            .list(&request(false, ListFilter::WorkflowType("TypeB".into()), 10, vec![]))
            .unwrap();
        assert_eq!(ids(&by_type), vec!["r2"]);
        let by_id = store
            .list(&request(false, ListFilter::WorkflowId("w2".into()), 10, vec![]))
            .unwrap();
        assert_eq!(ids(&by_id), vec!["r2"]);

        let got = store.get_closed(DOMAIN, "w1", "r1").unwrap();
        assert_eq!(got.history_length, 7);
        assert_eq!(got.execution.unwrap().workflow_id, "w1");
        assert!(matches!(
            store.get_closed(DOMAIN, "w2", "r2"),
            Err(VisibilityError::EntityNotExists(_))
        ));
    }

    #[test]
    fn sqlite_started_does_not_overwrite_and_delete_removes() {
        let store = SqlVisibilityStore::sqlite(":memory:").unwrap();
        store.record_started(&record("w", "r", "First", 100), 0).unwrap();
        store.record_started(&record("w", "r", "Second", 100), 0).unwrap();
        let open = store.list(&request(false, ListFilter::None, 10, vec![])).unwrap();
        assert_eq!(open.executions[0].workflow_type.as_ref().unwrap().name, "First");
        store.delete(DOMAIN, "w", "r", false).unwrap();
        assert!(store
            .list(&request(false, ListFilter::None, 10, vec![]))
            .unwrap()
            .executions
            .is_empty());
    }

    #[test]
    fn sqlite_time_range_and_unsupported_operations() {
        let store = SqlVisibilityStore::sqlite(":memory:").unwrap();
        store.record_started(&record("w", "r", "T", 100), 0).unwrap();
        let mut req = request(false, ListFilter::None, 10, vec![]);
        req.earliest_time_ns = 200 * 1_000_000_000;
        assert!(store.list(&req).unwrap().executions.is_empty());
        assert_eq!(
            store.list_by_query(DOMAIN, "x").unwrap_err(),
            VisibilityError::BadRequest("Operation is not supported".into())
        );
        assert!(store.count_by_query(DOMAIN, "x").is_err());
        assert!(store.upsert(true).is_ok());
        assert!(store.upsert(false).is_err());
    }
}
