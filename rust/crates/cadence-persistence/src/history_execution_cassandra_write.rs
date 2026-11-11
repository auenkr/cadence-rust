//! Write/read paths of the Cassandra `GoExecutionStore`
//! (Go `workflow.go`, `workflow_utils.go`, `workflow_parsing_utils.go`,
//! `nosql_execution_store_util.go`).
//!
//! UDT values are bound as inline literals exactly like Go does
//! (`{field: ?, ...}`); the per-field values come from a single ordered
//! `Fields` list so the literal text and the bound values cannot drift apart.
//! Reads map `CqlValue` generically.
#![allow(clippy::too_many_arguments)]

use crate::cassandra::CassandraBlockingSession;
use crate::history_contract::*;
use crate::history_execution_cassandra::{
    internal, parse_uuid, vis_ts, DEFAULT_VISIBILITY_MS, EXECUTION_TASK_ID, PERMANENT_RUN_ID,
    ROW_TYPE_EXECUTION,
};
use crate::matching_cassandra_exec::{bound, BoundValues};
use crate::sqlblobs::{self, Encoding};
use chrono::{DateTime, Utc};
use scylla::statement::batch::{Batch, BatchType};
use scylla::cluster::metadata::{CollectionType, ColumnType};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::{CellWriter, WrittenCellProof};
use scylla::serialize::SerializationError;
use scylla::value::{CqlTimestamp, CqlValue, Row};
use std::collections::HashMap;
use uuid::Uuid;

type E = PersistenceError;
pub(crate) type Fields = Vec<(&'static str, Option<CqlValue>)>;
pub(crate) type RowMap = HashMap<String, Option<CqlValue>>;

pub(crate) const ROW_TYPE_SHARD: i32 = 0;
pub(crate) const ROW_TYPE_TRANSFER: i32 = 2;
pub(crate) const ROW_TYPE_TIMER: i32 = 3;
pub(crate) const ROW_TYPE_REPLICATION: i32 = 4;
pub(crate) const ROW_TYPE_REQUEST_START: i32 = 7;
pub(crate) const ROW_TYPE_POLICY: i32 = 11;
pub(crate) const POLICY_TASK_ID: i64 = -1001;
const EMPTY_REQUEST_VERSION: i64 = -1000;
const REQUEST_TTL_SECONDS: i32 = 10_800;
const EVENT_STORE_VERSION: i32 = 2;
const ACTIVITY_SENTINEL_SCHEDULE_ID: i64 = -1;
pub(crate) const SHARD_DOMAIN: Uuid = Uuid::from_u128(0x10000000_1000_f000_f000_000000000000);
pub(crate) const SHARD_WORKFLOW: &str = "20000000-1000-f000-f000-000000000000";
pub(crate) const SHARD_RUN: Uuid = Uuid::from_u128(0x30000000_1000_f000_f000_000000000000);
pub(crate) const SHARD_TASK_ID: i64 = -11;
pub(crate) const TRANSFER_DOMAIN: Uuid = Uuid::from_u128(0x10000000_3000_f000_f000_000000000000);
pub(crate) const TRANSFER_WORKFLOW: &str = "20000000-3000-f000-f000-000000000000";
pub(crate) const TRANSFER_RUN: Uuid = Uuid::from_u128(0x30000000_3000_f000_f000_000000000000);
pub(crate) const TIMER_DOMAIN: Uuid = Uuid::from_u128(0x10000000_4000_f000_f000_000000000000);
pub(crate) const TIMER_WORKFLOW: &str = "20000000-4000-f000-f000-000000000000";
pub(crate) const TIMER_RUN: Uuid = Uuid::from_u128(0x30000000_4000_f000_f000_000000000000);
pub(crate) const REPLICATION_DOMAIN: Uuid = Uuid::from_u128(0x10000000_5000_f000_f000_000000000000);
pub(crate) const REPLICATION_WORKFLOW: &str = "20000000-5000-f000-f000-000000000000";
pub(crate) const REPLICATION_RUN: Uuid = Uuid::from_u128(0x30000000_5000_f000_f000_000000000000);

pub(crate) const KEY_WHERE: &str = "WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ?";

// ---------------------------------------------------------------- values

pub(crate) fn ms_ts(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default()
}
pub(crate) fn nanos_ts(nanos: i64) -> DateTime<Utc> {
    ms_ts(nanos.div_euclid(1_000_000))
}
fn i(v: i32) -> Option<CqlValue> {
    Some(CqlValue::Int(v))
}
fn l(v: i64) -> Option<CqlValue> {
    Some(CqlValue::BigInt(v))
}
fn s(v: &str) -> Option<CqlValue> {
    Some(CqlValue::Text(v.to_owned()))
}
fn b(v: &[u8]) -> Option<CqlValue> {
    Some(CqlValue::Blob(v.to_vec()))
}
fn bo(v: bool) -> Option<CqlValue> {
    Some(CqlValue::Boolean(v))
}
fn d(v: f64) -> Option<CqlValue> {
    Some(CqlValue::Double(v))
}
/// timestamp column from unix nanos (0 = unset time).
fn ts(nanos: i64) -> Option<CqlValue> {
    if nanos == 0 {
        None
    } else {
        Some(CqlValue::Timestamp(CqlTimestamp(nanos.div_euclid(1_000_000))))
    }
}
fn uu(v: &str) -> Result<Option<CqlValue>, E> {
    if v.is_empty() {
        Ok(None)
    } else {
        Ok(Some(CqlValue::Uuid(parse_uuid(v)?)))
    }
}
fn uu_bytes(v: &[u8]) -> Option<CqlValue> {
    Uuid::from_slice(v).ok().map(CqlValue::Uuid)
}
fn strs(v: &[String]) -> Option<CqlValue> {
    Some(CqlValue::List(v.iter().map(|x| CqlValue::Text(x.clone())).collect()))
}
fn secs(nanos: i64) -> i32 {
    (nanos / 1_000_000_000) as i32
}
fn blob_data(v: &Option<DataBlob>) -> Vec<u8> {
    v.as_ref().map(|x| x.data.clone()).unwrap_or_default()
}
fn blob_enc(v: &Option<DataBlob>) -> String {
    v.as_ref().map(|x| x.encoding.clone()).unwrap_or_default()
}

pub(crate) fn lit(f: &Fields) -> String {
    let parts: Vec<String> = f.iter().map(|(n, _)| format!("{n}: ?")).collect();
    format!("{{{}}}", parts.join(", "))
}
pub(crate) fn bind_fields(v: &mut BoundValues, f: Fields) {
    for (_, x) in f {
        v.push(bound(x));
    }
}
/// A `CqlValue` tree whose UDT keyspace/name are filled in from the destination column type at
/// serialization time (the driver rejects a `CqlValue` UDT whose name does not match).
#[derive(Debug, Clone)]
pub(crate) struct Filled(pub CqlValue);

fn fill(v: CqlValue, typ: &ColumnType) -> CqlValue {
    match (v, typ) {
        (CqlValue::UserDefinedType { fields, .. }, ColumnType::UserDefinedType { definition, .. }) => CqlValue::UserDefinedType {
            keyspace: definition.keyspace.to_string(),
            name: definition.name.to_string(),
            fields,
        },
        (CqlValue::List(xs), ColumnType::Collection { typ: CollectionType::List(e), .. }) => {
            CqlValue::List(xs.into_iter().map(|x| fill(x, e)).collect())
        }
        (CqlValue::Set(xs), ColumnType::Collection { typ: CollectionType::Set(e), .. }) => {
            CqlValue::Set(xs.into_iter().map(|x| fill(x, e)).collect())
        }
        (CqlValue::Map(xs), ColumnType::Collection { typ: CollectionType::Map(k, e), .. }) => {
            CqlValue::Map(xs.into_iter().map(|(a, b)| (fill(a, k), fill(b, e))).collect())
        }
        (other, _) => other,
    }
}

impl SerializeValue for Filled {
    fn serialize<'b>(&self, typ: &ColumnType, writer: CellWriter<'b>) -> Result<WrittenCellProof<'b>, SerializationError> {
        fill(self.0.clone(), typ).serialize(typ, writer)
    }
}

pub(crate) fn udt_value(f: Fields) -> CqlValue {
    CqlValue::UserDefinedType {
        keyspace: String::new(),
        name: String::new(),
        fields: f.into_iter().map(|(n, v)| (n.to_owned(), v)).collect(),
    }
}

// ---------------------------------------------------------------- batches

#[derive(Default)]
pub(crate) struct Stmts {
    pub items: Vec<(String, BoundValues)>,
}
impl Stmts {
    pub fn push(&mut self, text: impl Into<String>, values: BoundValues) {
        self.items.push((text.into(), values));
    }
}

pub(crate) fn key_values(shard: i32, row_type: i32, domain: Uuid, workflow: &str, run: Uuid, task_id: i64) -> BoundValues {
    vec![bound(shard), bound(row_type), bound(domain), bound(workflow.to_owned()), bound(run), bound(vis_ts()), bound(task_id)]
}
fn exec_key(shard: i32, domain: Uuid, workflow: &str, run: Uuid) -> BoundValues {
    key_values(shard, ROW_TYPE_EXECUTION, domain, workflow, run, EXECUTION_TASK_ID)
}

// ---------------------------------------------------------------- execution UDT

fn exec_fields(info: &WorkflowExecutionInfo) -> Result<Fields, E> {
    Ok(vec![
        ("domain_id", uu(&info.domain_id)?),
        ("workflow_id", s(&info.workflow_id)),
        ("run_id", uu(&info.run_id)?),
        ("first_run_id", uu(&info.first_execution_run_id)?),
        ("parent_domain_id", uu(&info.parent_domain_id)?),
        ("parent_workflow_id", s(&info.parent_workflow_id)),
        ("parent_run_id", uu(&info.parent_run_id)?),
        ("initiated_id", l(info.initiated_id)),
        ("completion_event_batch_id", l(info.completion_event_batch_id)),
        ("completion_event", b(&blob_data(&info.completion_event))),
        ("completion_event_data_encoding", s(&blob_enc(&info.completion_event))),
        ("task_list", s(&info.task_list)),
        ("task_list_kind", i(info.task_list_kind)),
        ("workflow_type_name", s(&info.workflow_type_name)),
        ("workflow_timeout", i(secs(info.workflow_timeout_nanos))),
        ("decision_task_timeout", i(secs(info.decision_start_to_close_timeout_nanos))),
        ("execution_context", b(&info.execution_context)),
        ("state", i(info.state)),
        ("close_status", i(info.close_status)),
        ("last_first_event_id", l(info.last_first_event_id)),
        ("last_event_task_id", l(info.last_event_task_id)),
        ("next_event_id", l(info.next_event_id)),
        ("last_processed_event", l(info.last_processed_event)),
        ("start_time", ts(info.start_time_unix_nanos)),
        ("last_updated_time", ts(info.last_updated_time_unix_nanos)),
        ("create_request_id", uu(&info.create_request_id)?),
        ("signal_count", i(info.signal_count)),
        ("history_size", l(info.history_size)),
        ("decision_version", l(info.decision_version)),
        ("decision_schedule_id", l(info.decision_schedule_id)),
        ("decision_started_id", l(info.decision_started_id)),
        ("decision_request_id", s(&info.decision_request_id)),
        ("decision_timeout", i(secs(info.decision_timeout_nanos))),
        ("decision_attempt", l(info.decision_attempt)),
        ("decision_timestamp", l(info.decision_started_time_unix_nanos)),
        ("decision_scheduled_timestamp", l(info.decision_scheduled_time_unix_nanos)),
        ("decision_original_scheduled_timestamp", l(info.decision_original_scheduled_time_unix_nanos)),
        ("cancel_requested", bo(info.cancel_requested)),
        ("cancel_request_id", s(&info.cancel_request_id)),
        ("sticky_task_list", s(&info.sticky_task_list)),
        ("sticky_schedule_to_start_timeout", i(secs(info.sticky_schedule_to_start_timeout_nanos))),
        ("client_library_version", s(&info.client_library_version)),
        ("client_feature_version", s(&info.client_feature_version)),
        ("client_impl", s(&info.client_impl)),
        ("auto_reset_points", b(&blob_data(&info.auto_reset_points))),
        ("auto_reset_points_encoding", s(&blob_enc(&info.auto_reset_points))),
        ("attempt", i(info.attempt)),
        ("has_retry_policy", bo(info.has_retry_policy)),
        ("init_interval", i(secs(info.initial_interval_nanos))),
        ("backoff_coefficient", d(info.backoff_coefficient)),
        ("jitter_coefficient", d(info.jitter_coefficient)),
        ("max_interval", i(secs(info.maximum_interval_nanos))),
        ("expiration_time", ts(info.expiration_time_unix_nanos)),
        ("max_attempts", i(info.maximum_attempts)),
        ("non_retriable_errors", strs(&info.non_retriable_errors)),
        ("event_store_version", i(EVENT_STORE_VERSION)),
        ("branch_token", b(&info.branch_token)),
        ("cron_schedule", s(&info.cron_schedule)),
        ("cron_overlap_policy", i(info.cron_overlap_policy)),
        ("expiration_seconds", i(secs(info.expiration_interval_nanos))),
        (
            "search_attributes",
            Some(CqlValue::Map(
                info.search_attributes.iter().map(|(k, v)| (CqlValue::Text(k.clone()), CqlValue::Blob(v.clone()))).collect(),
            )),
        ),
        (
            "memo",
            Some(CqlValue::Map(info.memo.iter().map(|(k, v)| (CqlValue::Text(k.clone()), CqlValue::Blob(v.clone()))).collect())),
        ),
        (
            "partition_config",
            Some(CqlValue::Map(
                info.partition_config.iter().map(|(k, v)| (CqlValue::Text(k.clone()), CqlValue::Text(v.clone()))).collect(),
            )),
        ),
        ("active_cluster_selection_policy", b(&blob_data(&info.active_cluster_selection_policy))),
        ("active_cluster_selection_policy_encoding", s(&blob_enc(&info.active_cluster_selection_policy))),
    ])
}

/// Contract checksum blob (thriftrw `{1: version, 2: flavor, 3: value}`) -> checksum UDT.
fn checksum_fields(blob: &Option<DataBlob>) -> Fields {
    let mut version = 0;
    let mut flavor = 0;
    let mut value: Vec<u8> = Vec::new();
    if let Some(blob) = blob {
        if let Ok(enc) = Encoding::parse(&blob.encoding) {
            if let Ok(st) = enc.decode(&blob.data) {
                use cadence_thrift::Value;
                if let Some(Value::I32(v)) = st.get(1) {
                    version = *v;
                }
                if let Some(Value::I32(v)) = st.get(2) {
                    flavor = *v;
                }
                if let Some(Value::Bytes(v)) = st.get(3) {
                    value.clone_from(v);
                }
            }
        }
    }
    vec![("version", i(version)), ("flavor", i(flavor)), ("value", b(&value))]
}

fn checksum_blob(version: i32, flavor: i32, value: Vec<u8>) -> Option<DataBlob> {
    if version == 0 && flavor == 0 && value.is_empty() {
        return None;
    }
    use cadence_thrift::{value::Struct, Value};
    let st = Struct { fields: vec![(1, Value::I32(version)), (2, Value::I32(flavor)), (3, Value::Bytes(value))] };
    Encoding::ThriftRW.encode(&st).ok().map(|data| DataBlob { encoding: Encoding::ThriftRW.as_str().to_owned(), data })
}

// ---------------------------------------------------------------- map UDTs

pub(crate) fn decode_struct(blob: &DataBlob) -> Result<cadence_thrift::value::Struct, E> {
    Encoding::parse(&blob.encoding).and_then(|e| e.decode(&blob.data)).map_err(internal)
}

fn activity_fields(key: i64, v: &KeyedBlob<i64>, hb: Option<&ActivityHeartbeat>) -> Result<Fields, E> {
    let a = sqlblobs::activity_info_from_struct(&decode_struct(&v.value)?).map_err(internal)?;
    let (details, hb_time) = hb.map(|h| (h.details.clone(), h.updated_time_unix_nanos)).unwrap_or_default();
    Ok(vec![
        ("version", l(a.version)),
        ("schedule_id", l(key)),
        ("scheduled_event_batch_id", l(a.scheduled_event_batch_id)),
        ("scheduled_event", b(&a.scheduled_event)),
        ("scheduled_time", ts(a.scheduled_time_unix_nanos)),
        ("started_id", l(a.started_id)),
        ("started_event", b(&a.started_event)),
        ("started_time", ts(a.started_time_unix_nanos)),
        ("activity_id", s(&a.activity_id)),
        ("request_id", s(&a.request_id)),
        ("details", b(&details)),
        ("schedule_to_start_timeout", i(a.schedule_to_start_timeout_seconds)),
        ("schedule_to_close_timeout", i(a.schedule_to_close_timeout_seconds)),
        ("start_to_close_timeout", i(a.start_to_close_timeout_seconds)),
        ("heart_beat_timeout", i(a.heartbeat_timeout_seconds)),
        ("cancel_requested", bo(a.cancel_requested)),
        ("cancel_request_id", l(a.cancel_request_id)),
        ("last_hb_updated_time", ts(hb_time)),
        ("timer_task_status", i(a.timer_task_status)),
        ("attempt", i(a.attempt)),
        ("task_list", s(&a.task_list)),
        ("task_list_kind", i(a.task_list_kind)),
        ("started_identity", s(&a.started_identity)),
        ("has_retry_policy", bo(a.has_retry_policy)),
        ("init_interval", i(a.retry_initial_interval_seconds)),
        ("backoff_coefficient", d(a.retry_backoff_coefficient)),
        ("jitter_coefficient", d(a.retry_jitter_coefficient)),
        ("max_interval", i(a.retry_maximum_interval_seconds)),
        ("expiration_time", ts(a.retry_expiration_time_unix_nanos)),
        ("max_attempts", i(a.retry_maximum_attempts)),
        ("non_retriable_errors", strs(&a.retry_non_retriable_errors)),
        ("last_failure_reason", s(&a.retry_last_failure_reason)),
        ("last_worker_identity", s(&a.retry_last_worker_identity)),
        ("last_failure_details", b(&a.retry_last_failure_details)),
        ("last_failure_category", i(a.retry_last_failure_category)),
        ("last_retry_interval_seconds", i(a.retry_last_retry_interval_seconds)),
        ("event_data_encoding", s(&a.scheduled_event_encoding)),
    ])
}

fn timer_fields(key: &str, v: &KeyedBlob<String>) -> Result<Fields, E> {
    let t = sqlblobs::timer_info_from_struct(&decode_struct(&v.value)?).map_err(internal)?;
    Ok(vec![
        ("version", l(t.version)),
        ("timer_id", s(key)),
        ("started_id", l(t.started_id)),
        ("expiry_time", ts(t.expiry_time_unix_nanos)),
        ("task_id", l(t.task_id)),
    ])
}

fn child_fields(key: i64, v: &KeyedBlob<i64>) -> Result<Fields, E> {
    let c = sqlblobs::child_execution_info_from_struct(&decode_struct(&v.value)?).map_err(internal)?;
    Ok(vec![
        ("version", l(c.version)),
        ("initiated_id", l(key)),
        ("initiated_event_batch_id", l(c.initiated_event_batch_id)),
        ("initiated_event", b(&c.initiated_event)),
        ("started_id", l(c.started_id)),
        ("started_workflow_id", s(&c.started_workflow_id)),
        ("started_run_id", uu_bytes(&c.started_run_id)),
        ("started_event", b(&c.started_event)),
        ("create_request_id", uu(&c.create_request_id)?),
        ("event_data_encoding", s(&c.initiated_event_encoding)),
        ("domain_id", uu(&c.domain_id)?),
        ("domain_name", s(&c.domain_name)),
        ("workflow_type_name", s(&c.workflow_type_name)),
        ("parent_close_policy", i(c.parent_close_policy)),
    ])
}

fn request_cancel_fields(key: i64, v: &KeyedBlob<i64>) -> Result<Fields, E> {
    let r = sqlblobs::request_cancel_info_from_struct(&decode_struct(&v.value)?).map_err(internal)?;
    Ok(vec![
        ("version", l(r.version)),
        ("initiated_id", l(key)),
        ("initiated_event_batch_id", l(r.initiated_event_batch_id)),
        ("cancel_request_id", s(&r.cancel_request_id)),
    ])
}

fn signal_fields(key: i64, v: &KeyedBlob<i64>) -> Result<Fields, E> {
    let g = sqlblobs::signal_info_from_struct(&decode_struct(&v.value)?).map_err(internal)?;
    Ok(vec![
        ("version", l(g.version)),
        ("initiated_id", l(key)),
        ("initiated_event_batch_id", l(g.initiated_event_batch_id)),
        ("signal_request_id", uu(&g.request_id)?),
        ("signal_name", s(&g.name)),
        ("input", b(&g.input)),
        ("control", b(&g.control)),
    ])
}

#[derive(Clone, Copy)]
enum MapKind {
    Activity,
    Timer,
    Child,
    RequestCancel,
    Signal,
}
impl MapKind {
    fn column(self) -> &'static str {
        match self {
            Self::Activity => "activity_map",
            Self::Timer => "timer_map",
            Self::Child => "child_executions_map",
            Self::RequestCancel => "request_cancel_map",
            Self::Signal => "signal_map",
        }
    }
}

/// Pushes `UPDATE executions SET <map>[ ? ] = {..}, last_updated_time = ? WHERE key`.
fn push_map_upsert(st: &mut Stmts, kind: MapKind, map_key: CqlValue, fields: Fields, key: BoundValues, now: DateTime<Utc>) {
    let text = format!("UPDATE executions SET {}[ ? ] = {}, last_updated_time = ? {}", kind.column(), lit(&fields), KEY_WHERE);
    let mut v: BoundValues = vec![bound(map_key)];
    bind_fields(&mut v, fields);
    v.push(bound(now));
    v.extend(key);
    st.push(text, v);
}
fn push_map_delete(st: &mut Stmts, kind: MapKind, map_key: CqlValue, key: BoundValues) {
    let text = format!("DELETE {}[ ? ] FROM executions {}", kind.column(), KEY_WHERE);
    let mut v: BoundValues = vec![bound(map_key)];
    v.extend(key);
    st.push(text, v);
}
/// Whole-map replacement (Go reset*).
fn push_map_reset(st: &mut Stmts, kind: MapKind, entries: Vec<(CqlValue, Fields)>, key: BoundValues, now: DateTime<Utc>) {
    let text = format!("UPDATE executions SET {} = ?, last_updated_time = ? {}", kind.column(), KEY_WHERE);
    let map = CqlValue::Map(entries.into_iter().map(|(k, f)| (k, udt_value(f))).collect());
    let mut v: BoundValues = vec![bound(Filled(map)), bound(now)];
    v.extend(key);
    st.push(text, v);
}

// ---------------------------------------------------------------- execution mutations

/// Per-execution write input shared by snapshot and mutation.
pub(crate) struct ExecWrite<'a> {
    pub info: &'a WorkflowExecutionInfo,
    pub version_histories: &'a Option<DataBlob>,
    pub checksum: &'a Option<DataBlob>,
    pub last_write_version: i64,
    pub heartbeats: &'a std::collections::BTreeMap<i64, ActivityHeartbeat>,
    pub workflow_timer_tasks: &'a [HistoryTaskKey],
}

fn push_workflow_timer_tasks(st: &mut Stmts, keys: &[HistoryTaskKey], key: &BoundValuesFn, now: DateTime<Utc>) {
    if keys.is_empty() {
        return;
    }
    let tuples: Vec<(DateTime<Utc>, i64)> = keys.iter().map(|k| (nanos_ts(k.version), k.task_id)).collect();
    let mut v: BoundValues = vec![bound(tuples), bound(now)];
    v.extend(key());
    st.push(format!("UPDATE executions SET workflow_timer_tasks = workflow_timer_tasks + ?, last_updated_time = ? {KEY_WHERE}"), v);
}
type BoundValuesFn = dyn Fn() -> BoundValues;

fn push_signal_requested(st: &mut Stmts, add: &[String], remove: &[String], key: &BoundValuesFn, now: DateTime<Utc>) -> Result<(), E> {
    for (ids, op) in [(add, "+"), (remove, "-")] {
        if ids.is_empty() {
            continue;
        }
        let set: Vec<Uuid> = ids.iter().map(|x| parse_uuid(x)).collect::<Result<_, _>>()?;
        let mut v: BoundValues = vec![bound(set), bound(now)];
        v.extend(key());
        st.push(format!("UPDATE executions SET signal_requested = signal_requested {op} ?, last_updated_time = ? {KEY_WHERE}"), v);
    }
    Ok(())
}

/// Go `createWorkflowExecutionWithMergeMaps`.
pub(crate) fn push_create_execution(
    st: &mut Stmts,
    shard: i32,
    domain: Uuid,
    workflow: &str,
    run: Uuid,
    w: &ExecWrite<'_>,
    snapshot: &WorkflowSnapshot,
    now: DateTime<Utc>,
) -> Result<(), E> {
    let fields = exec_fields(w.info)?;
    let cks = checksum_fields(w.checksum);
    let text = format!(
        "INSERT INTO executions (shard_id, domain_id, workflow_id, run_id, type, execution, next_event_id, visibility_ts, task_id, version_histories, version_histories_encoding, checksum, workflow_last_write_version, workflow_state, created_time) VALUES(?, ?, ?, ?, ?, {}, ?, ?, ?, ?, ?, {}, ?, ?, ?) IF NOT EXISTS",
        lit(&fields),
        lit(&cks)
    );
    let mut v: BoundValues = vec![bound(shard), bound(domain), bound(workflow.to_owned()), bound(run), bound(ROW_TYPE_EXECUTION)];
    bind_fields(&mut v, fields);
    v.push(bound(w.info.next_event_id));
    v.push(bound(vis_ts()));
    v.push(bound(EXECUTION_TASK_ID));
    v.push(bound(blob_data(w.version_histories)));
    v.push(bound(blob_enc(w.version_histories)));
    bind_fields(&mut v, cks);
    v.push(bound(w.last_write_version));
    v.push(bound(w.info.state));
    v.push(bound(now));
    st.push(text, v);

    let keyf: Box<BoundValuesFn> = Box::new({
        let wf = workflow.to_owned();
        move || exec_key(shard, domain, &wf, run)
    });
    for a in &snapshot.activity_infos {
        push_map_upsert(st, MapKind::Activity, CqlValue::BigInt(a.key), activity_fields(a.key, a, w.heartbeats.get(&a.key))?, keyf(), now);
    }
    for t in &snapshot.timer_infos {
        push_map_upsert(st, MapKind::Timer, CqlValue::Text(t.key.clone()), timer_fields(&t.key, t)?, keyf(), now);
    }
    for c in &snapshot.child_execution_infos {
        push_map_upsert(st, MapKind::Child, CqlValue::BigInt(c.key), child_fields(c.key, c)?, keyf(), now);
    }
    for r in &snapshot.request_cancel_infos {
        push_map_upsert(st, MapKind::RequestCancel, CqlValue::BigInt(r.key), request_cancel_fields(r.key, r)?, keyf(), now);
    }
    for g in &snapshot.signal_infos {
        push_map_upsert(st, MapKind::Signal, CqlValue::BigInt(g.key), signal_fields(g.key, g)?, keyf(), now);
    }
    push_signal_requested(st, &snapshot.signal_requested_ids, &[], &*keyf, now)?;
    push_workflow_timer_tasks(st, w.workflow_timer_tasks, &*keyf, now);
    Ok(())
}

/// Go `updateWorkflowExecution` (the UPDATE ... IF next_event_id = ? form).
fn push_update_execution(
    st: &mut Stmts,
    shard: i32,
    domain: Uuid,
    workflow: &str,
    run: Uuid,
    w: &ExecWrite<'_>,
    condition: i64,
    now: DateTime<Utc>,
) -> Result<(), E> {
    let fields = exec_fields(w.info)?;
    let cks = checksum_fields(w.checksum);
    let text = format!(
        "UPDATE executions SET execution = {}, next_event_id = ?, version_histories = ?, version_histories_encoding = ?, checksum = {}, workflow_last_write_version = ?, workflow_state = ?, last_updated_time = ? {KEY_WHERE} IF next_event_id = ?",
        lit(&fields),
        lit(&cks)
    );
    let mut v: BoundValues = Vec::new();
    bind_fields(&mut v, fields);
    v.push(bound(w.info.next_event_id));
    v.push(bound(blob_data(w.version_histories)));
    v.push(bound(blob_enc(w.version_histories)));
    bind_fields(&mut v, cks);
    v.push(bound(w.last_write_version));
    v.push(bound(w.info.state));
    v.push(bound(now));
    v.extend(exec_key(shard, domain, workflow, run));
    v.push(bound(condition));
    st.push(text, v);
    Ok(())
}

/// Go `updateWorkflowExecutionAndEventBufferWithMergeAndDeleteMaps`.
pub(crate) fn push_mutation(
    st: &mut Stmts,
    shard: i32,
    domain: Uuid,
    workflow: &str,
    run: Uuid,
    m: &WorkflowMutation,
    now: DateTime<Utc>,
) -> Result<(), E> {
    let w = ExecWrite {
        info: &m.execution_info,
        version_histories: &m.version_histories,
        checksum: &m.checksum,
        last_write_version: m.last_write_version,
        heartbeats: &m.activity_heartbeats,
        workflow_timer_tasks: &m.workflow_timer_tasks,
    };
    push_update_execution(st, shard, domain, workflow, run, &w, m.condition, now)?;
    let wf = workflow.to_owned();
    let keyf = move || exec_key(shard, domain, &wf, run);

    // Event buffer: Clear / Append / None.
    if m.clear_buffered_events {
        push_delete_buffered(st, keyf(), now);
    } else if let Some(events) = &m.new_buffered_events {
        let item = CqlValue::UserDefinedType {
            keyspace: String::new(),
            name: String::new(),
            fields: vec![
                ("encoding_type".to_owned(), s(&events.encoding)),
                ("version".to_owned(), i(0)),
                ("data".to_owned(), b(&events.data)),
            ],
        };
        let mut v: BoundValues = vec![bound(Filled(CqlValue::List(vec![item]))), bound(now)];
        v.extend(keyf());
        st.push(format!("UPDATE executions SET buffered_events_list = buffered_events_list + ?, last_updated_time = ? {KEY_WHERE}"), v);
    }

    // Activities / timers: rewrite replaces the whole map, otherwise upsert + delete (or sentinel).
    if !m.rewrite_activity_infos.is_empty() {
        let mut entries = Vec::new();
        for a in &m.rewrite_activity_infos {
            entries.push((CqlValue::BigInt(a.key), activity_fields(a.key, a, m.activity_heartbeats.get(&a.key))?));
        }
        push_map_reset(st, MapKind::Activity, entries, keyf(), now);
    } else {
        for a in &m.upsert_activity_infos {
            push_map_upsert(st, MapKind::Activity, CqlValue::BigInt(a.key), activity_fields(a.key, a, m.activity_heartbeats.get(&a.key))?, keyf(), now);
        }
        for key in &m.delete_activity_infos {
            if m.sentinel_write_enabled {
                push_map_upsert(st, MapKind::Activity, CqlValue::BigInt(*key), vec![("schedule_id", l(ACTIVITY_SENTINEL_SCHEDULE_ID))], keyf(), now);
            } else {
                push_map_delete(st, MapKind::Activity, CqlValue::BigInt(*key), keyf());
            }
        }
    }
    if !m.rewrite_timer_infos.is_empty() {
        let mut entries = Vec::new();
        for t in &m.rewrite_timer_infos {
            entries.push((CqlValue::Text(t.key.clone()), timer_fields(&t.key, t)?));
        }
        push_map_reset(st, MapKind::Timer, entries, keyf(), now);
    } else {
        for t in &m.upsert_timer_infos {
            push_map_upsert(st, MapKind::Timer, CqlValue::Text(t.key.clone()), timer_fields(&t.key, t)?, keyf(), now);
        }
        for key in &m.delete_timer_infos {
            if m.sentinel_write_enabled {
                push_map_upsert(st, MapKind::Timer, CqlValue::Text(key.clone()), vec![("timer_id", s(""))], keyf(), now);
            } else {
                push_map_delete(st, MapKind::Timer, CqlValue::Text(key.clone()), keyf());
            }
        }
    }
    push_workflow_timer_tasks(st, &m.workflow_timer_tasks, &keyf, now);
    for c in &m.upsert_child_execution_infos {
        push_map_upsert(st, MapKind::Child, CqlValue::BigInt(c.key), child_fields(c.key, c)?, keyf(), now);
    }
    for k in &m.delete_child_execution_infos {
        push_map_delete(st, MapKind::Child, CqlValue::BigInt(*k), keyf());
    }
    for r in &m.upsert_request_cancel_infos {
        push_map_upsert(st, MapKind::RequestCancel, CqlValue::BigInt(r.key), request_cancel_fields(r.key, r)?, keyf(), now);
    }
    for k in &m.delete_request_cancel_infos {
        push_map_delete(st, MapKind::RequestCancel, CqlValue::BigInt(*k), keyf());
    }
    for g in &m.upsert_signal_infos {
        push_map_upsert(st, MapKind::Signal, CqlValue::BigInt(g.key), signal_fields(g.key, g)?, keyf(), now);
    }
    for k in &m.delete_signal_infos {
        push_map_delete(st, MapKind::Signal, CqlValue::BigInt(*k), keyf());
    }
    push_signal_requested(st, &m.upsert_signal_requested_ids, &m.delete_signal_requested_ids, &keyf, now)?;
    Ok(())
}

fn push_delete_buffered(st: &mut Stmts, key: BoundValues, now: DateTime<Utc>) {
    let mut v: BoundValues = vec![bound(now)];
    v.extend(key);
    st.push(format!("UPDATE executions SET buffered_events_list = [], last_updated_time = ? {KEY_WHERE}"), v);
}

/// Go `resetWorkflowExecutionAndMapsAndEventBuffer`.
pub(crate) fn push_reset(
    st: &mut Stmts,
    shard: i32,
    domain: Uuid,
    workflow: &str,
    run: Uuid,
    snapshot: &WorkflowSnapshot,
    now: DateTime<Utc>,
) -> Result<(), E> {
    let w = ExecWrite {
        info: &snapshot.execution_info,
        version_histories: &snapshot.version_histories,
        checksum: &snapshot.checksum,
        last_write_version: snapshot.last_write_version,
        heartbeats: &snapshot.activity_heartbeats,
        workflow_timer_tasks: &snapshot.workflow_timer_tasks,
    };
    push_update_execution(st, shard, domain, workflow, run, &w, snapshot.condition, now)?;
    let wf = workflow.to_owned();
    let keyf = move || exec_key(shard, domain, &wf, run);
    push_delete_buffered(st, keyf(), now);

    let mut activities = Vec::new();
    for a in &snapshot.activity_infos {
        activities.push((CqlValue::BigInt(a.key), activity_fields(a.key, a, snapshot.activity_heartbeats.get(&a.key))?));
    }
    push_map_reset(st, MapKind::Activity, activities, keyf(), now);
    let mut timers = Vec::new();
    for t in &snapshot.timer_infos {
        timers.push((CqlValue::Text(t.key.clone()), timer_fields(&t.key, t)?));
    }
    push_map_reset(st, MapKind::Timer, timers, keyf(), now);
    push_workflow_timer_tasks(st, &snapshot.workflow_timer_tasks, &keyf, now);
    let mut children = Vec::new();
    for c in &snapshot.child_execution_infos {
        let mut f = child_fields(c.key, c)?;
        // Go resetChildExecutionInfoMap uses emptyRunID when started_run_id is empty.
        for (name, value) in f.iter_mut() {
            if *name == "started_run_id" && value.is_none() {
                *value = Some(CqlValue::Uuid(Uuid::from_u128(0x30000000_0000_f000_f000_000000000000)));
            }
        }
        children.push((CqlValue::BigInt(c.key), f));
    }
    push_map_reset(st, MapKind::Child, children, keyf(), now);
    let mut cancels = Vec::new();
    for r in &snapshot.request_cancel_infos {
        cancels.push((CqlValue::BigInt(r.key), request_cancel_fields(r.key, r)?));
    }
    push_map_reset(st, MapKind::RequestCancel, cancels, keyf(), now);
    let mut signals = Vec::new();
    for g in &snapshot.signal_infos {
        signals.push((CqlValue::BigInt(g.key), signal_fields(g.key, g)?));
    }
    push_map_reset(st, MapKind::Signal, signals, keyf(), now);
    let set: Vec<Uuid> = snapshot.signal_requested_ids.iter().map(|x| parse_uuid(x)).collect::<Result<_, _>>()?;
    let mut v: BoundValues = vec![bound(set), bound(now)];
    v.extend(keyf());
    st.push(format!("UPDATE executions SET signal_requested = ?, last_updated_time = ? {KEY_WHERE}"), v);
    Ok(())
}

// ---------------------------------------------------------------- rows: current / requests / policy / lease

#[derive(Clone)]
pub(crate) struct CurrentRow {
    pub run_id: String,
    pub create_request_id: String,
    pub state: i32,
    pub close_status: i32,
    pub last_write_version: i64,
}
pub(crate) enum CurrentWrite {
    Noop,
    Insert(CurrentRow),
    Update { row: CurrentRow, current_run_id: String, previous: Option<(i64, i32)> },
}

pub(crate) fn current_row(info: &WorkflowExecutionInfo, last_write_version: i64) -> CurrentRow {
    CurrentRow {
        run_id: info.run_id.clone(),
        create_request_id: info.create_request_id.clone(),
        state: info.state,
        close_status: info.close_status,
        last_write_version,
    }
}

pub(crate) fn push_current(st: &mut Stmts, shard: i32, domain: Uuid, workflow: &str, write: &CurrentWrite, now: DateTime<Utc>) -> Result<(), E> {
    match write {
        CurrentWrite::Noop => {}
        CurrentWrite::Insert(r) => {
            let mut v = key_values(shard, ROW_TYPE_EXECUTION, domain, workflow, PERMANENT_RUN_ID, EXECUTION_TASK_ID);
            v.push(bound(parse_uuid(&r.run_id)?));
            v.push(bound(parse_uuid(&r.run_id)?));
            v.push(bound(uu(&r.create_request_id)?));
            v.push(bound(r.state));
            v.push(bound(r.close_status));
            v.push(bound(r.last_write_version));
            v.push(bound(r.state));
            v.push(bound(now));
            st.push(
                "INSERT INTO executions (shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, execution, workflow_last_write_version, workflow_state, created_time) VALUES(?, ?, ?, ?, ?, ?, ?, ?, {run_id: ?, create_request_id: ?, state: ?, close_status: ?}, ?, ?, ?) IF NOT EXISTS USING TTL 0",
                v,
            );
        }
        CurrentWrite::Update { row, current_run_id, previous } => {
            if current_run_id.is_empty() {
                return Err(E::InvalidPersistenceRequest("CurrentWorkflowWriteModeUpdate require Condition.CurrentRunID".into()));
            }
            let mut v: BoundValues = vec![
                bound(parse_uuid(&row.run_id)?),
                bound(parse_uuid(&row.run_id)?),
                bound(uu(&row.create_request_id)?),
                bound(row.state),
                bound(row.close_status),
                bound(row.last_write_version),
                bound(row.state),
                bound(now),
            ];
            v.extend(key_values(shard, ROW_TYPE_EXECUTION, domain, workflow, PERMANENT_RUN_ID, EXECUTION_TASK_ID));
            v.push(bound(parse_uuid(current_run_id)?));
            let mut text = "UPDATE executions USING TTL 0 SET current_run_id = ?, execution = {run_id: ?, create_request_id: ?, state: ?, close_status: ?}, workflow_last_write_version = ?, workflow_state = ?, last_updated_time = ? WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ? IF current_run_id = ?".to_owned();
            if let Some((version, state)) = previous {
                text.push_str(" and workflow_last_write_version = ? and workflow_state = ?");
                v.push(bound(*version));
                v.push(bound(*state));
            }
            st.push(text, v);
        }
    }
    Ok(())
}

/// Go `insertOrUpsertWorkflowRequestRow` (type/version are not representable in the contract:
/// rows are written as WorkflowRequestStart with the snapshot's last write version).
pub(crate) fn push_requests(
    st: &mut Stmts,
    shard: i32,
    domain: Uuid,
    workflow: &str,
    run: &str,
    version: i64,
    requests: &[WorkflowRequest],
    mode: WorkflowRequestMode,
    now: DateTime<Utc>,
) -> Result<(), E> {
    let text = match mode {
        WorkflowRequestMode::New => "INSERT INTO executions (shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, created_time) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS USING TTL ?",
        WorkflowRequestMode::Replicated => "INSERT INTO executions (shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, last_updated_time) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
    };
    for r in requests {
        let request_id = parse_uuid(&r.request_id)?;
        for task_id in [-EMPTY_REQUEST_VERSION, -version] {
            let mut v = key_values(shard, ROW_TYPE_REQUEST_START, domain, workflow, request_id, task_id);
            v.push(bound(parse_uuid(run)?));
            v.push(bound(now));
            v.push(bound(REQUEST_TTL_SECONDS));
            st.push(text, v);
        }
    }
    Ok(())
}

pub(crate) fn push_policy(st: &mut Stmts, shard: i32, domain: Uuid, workflow: &str, run: Uuid, policy: &Option<DataBlob>, now: DateTime<Utc>) {
    let Some(p) = policy else { return };
    let mut v = key_values(shard, ROW_TYPE_POLICY, domain, workflow, run, POLICY_TASK_ID);
    v.push(bound(now));
    v.push(bound(p.data.clone()));
    v.push(bound(p.encoding.clone()));
    st.push(
        "INSERT INTO executions (shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, created_time, data, data_encoding) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS",
        v,
    );
}

pub(crate) fn push_lease(st: &mut Stmts, shard: i32, range_id: i64, now: DateTime<Utc>) {
    let mut v: BoundValues = vec![bound(range_id), bound(now)];
    v.extend(key_values(shard, ROW_TYPE_SHARD, SHARD_DOMAIN, SHARD_WORKFLOW, SHARD_RUN, SHARD_TASK_ID));
    v.push(bound(range_id));
    st.push(format!("UPDATE executions SET range_id = ?, last_updated_time = ? {KEY_WHERE} IF range_id = ?"), v);
}

// ---------------------------------------------------------------- task rows

pub(crate) fn transfer_fields(t: &TransferTask) -> Result<Fields, E> {
    let target_domains: Vec<CqlValue> = t.target_domain_ids.iter().map(|x| parse_uuid(x).map(CqlValue::Uuid)).collect::<Result<_, _>>()?;
    Ok(vec![
        ("domain_id", uu(&t.domain_id)?),
        ("workflow_id", s(&t.workflow_id)),
        ("run_id", uu(&t.run_id)?),
        ("visibility_ts", ts(t.visibility_time_unix_nanos)),
        ("task_id", l(t.task_id)),
        ("target_domain_id", uu(&t.target_domain_id)?),
        ("target_domain_ids", Some(CqlValue::Set(target_domains))),
        ("target_workflow_id", s(&t.target_workflow_id)),
        ("target_run_id", Some(CqlValue::Uuid(if t.target_run_id.is_empty() {
            Uuid::from_u128(0x30000000_2000_f000_f000_000000000000)
        } else {
            parse_uuid(&t.target_run_id)?
        }))),
        ("target_child_workflow_only", bo(t.target_child_workflow_only)),
        ("task_list", s(&t.task_list)),
        ("type", i(t.task_type)),
        ("schedule_id", l(t.schedule_id)),
        ("record_visibility", bo(t.record_visibility)),
        ("version", l(t.version)),
        ("original_task_list", s(&t.original_task_list)),
        ("original_task_list_kind", i(t.original_task_list_kind)),
    ])
}

pub(crate) fn timer_task_fields(t: &TimerTask) -> Result<Fields, E> {
    Ok(vec![
        ("domain_id", uu(&t.domain_id)?),
        ("workflow_id", s(&t.workflow_id)),
        ("run_id", uu(&t.run_id)?),
        ("visibility_ts", ts(t.visibility_timestamp_unix_nanos)),
        ("task_id", l(t.task_id)),
        ("type", i(t.task_type)),
        ("timeout_type", i(t.timeout_type)),
        ("event_id", l(t.event_id)),
        ("schedule_attempt", l(t.schedule_attempt)),
        ("version", l(t.version)),
        ("task_list", s(&t.task_list)),
    ])
}

pub(crate) fn replication_fields(t: &ReplicationTask) -> Result<Fields, E> {
    Ok(vec![
        ("domain_id", uu(&t.domain_id)?),
        ("workflow_id", s(&t.workflow_id)),
        ("run_id", uu(&t.run_id)?),
        ("task_id", l(t.task_id)),
        ("type", i(t.task_type)),
        ("first_event_id", l(t.first_event_id)),
        ("next_event_id", l(t.next_event_id)),
        ("version", l(t.version)),
        ("scheduled_id", l(t.scheduled_id)),
        ("event_store_version", i(EVENT_STORE_VERSION)),
        ("branch_token", b(&t.branch_token)),
        ("new_run_event_store_version", i(EVENT_STORE_VERSION)),
        ("new_run_branch_token", b(&t.new_run_branch_token)),
        ("created_time", l(t.creation_time_unix_nanos)),
    ])
}

/// Inserts one task row (`type` column UDT named `column`).
pub(crate) fn push_task_row(
    st: &mut Stmts,
    shard: i32,
    row_type: i32,
    column: &str,
    ids: (Uuid, &str, Uuid),
    fields: Fields,
    blob: &DataBlob,
    visibility_ms: i64,
    task_id: i64,
    now: DateTime<Utc>,
) {
    let text = format!(
        "INSERT INTO executions (shard_id, type, domain_id, workflow_id, run_id, {column}, data, data_encoding, visibility_ts, task_id, created_time) VALUES(?, ?, ?, ?, ?, {}, ?, ?, ?, ?, ?)",
        lit(&fields)
    );
    let mut v: BoundValues = vec![bound(shard), bound(row_type), bound(ids.0), bound(ids.1.to_owned()), bound(ids.2)];
    bind_fields(&mut v, fields);
    v.push(bound(if blob.data.is_empty() { None } else { Some(blob.data.clone()) }));
    v.push(bound(if blob.encoding.is_empty() { None } else { Some(blob.encoding.clone()) }));
    v.push(bound(ms_ts(visibility_ms)));
    v.push(bound(task_id));
    v.push(bound(now));
    st.push(text, v);
}

/// Go `createTasksByCategory` for the `(type, id)` categories Go handles.
pub(crate) fn push_tasks_by_category(
    st: &mut Stmts,
    shard: i32,
    tasks: &std::collections::BTreeMap<HistoryTaskCategory, Vec<Task>>,
    now: DateTime<Utc>,
) -> Result<(), E> {
    for (category, list) in tasks {
        for task in list {
            push_one_task(st, shard, category, task, now)?;
        }
    }
    Ok(())
}

pub(crate) fn push_one_task(st: &mut Stmts, shard: i32, category: &HistoryTaskCategory, task: &Task, now: DateTime<Utc>) -> Result<(), E> {
    let decoded = decode_struct(&task.data);
    match (category.category_type, category.category_id) {
        (1, 1) => {
            let mut t = sqlblobs::transfer_task_from_struct(&decoded?).map_err(internal)?;
            t.task_id = task.task_id;
            push_task_row(
                st,
                shard,
                ROW_TYPE_TRANSFER,
                "transfer",
                (TRANSFER_DOMAIN, TRANSFER_WORKFLOW, TRANSFER_RUN),
                transfer_fields(&t)?,
                &task.data,
                DEFAULT_VISIBILITY_MS,
                t.task_id,
                now,
            );
        }
        (2, 2) => {
            let mut t = sqlblobs::timer_task_from_struct(&decoded?).map_err(internal)?;
            t.task_id = task.task_id;
            // The thrift blob carries no visibility time: it comes from the Task (Go `GetVisibilityTimestamp`).
            t.visibility_timestamp_unix_nanos = task.visibility_timestamp_unix_nanos;
            push_task_row(
                st,
                shard,
                ROW_TYPE_TIMER,
                "timer",
                (TIMER_DOMAIN, TIMER_WORKFLOW, TIMER_RUN),
                timer_task_fields(&t)?,
                &task.data,
                t.visibility_timestamp_unix_nanos.div_euclid(1_000_000),
                t.task_id,
                now,
            );
        }
        (1, 3) => {
            let mut t = sqlblobs::replication_task_from_struct(&decoded?).map_err(internal)?;
            t.task_id = task.task_id;
            push_task_row(
                st,
                shard,
                ROW_TYPE_REPLICATION,
                "replication",
                (REPLICATION_DOMAIN, REPLICATION_WORKFLOW, REPLICATION_RUN),
                replication_fields(&t)?,
                &task.data,
                DEFAULT_VISIBILITY_MS,
                t.task_id,
                now,
            );
        }
        _ => {}
    }
    Ok(())
}

// ---------------------------------------------------------------- execute + CAS analysis

pub(crate) fn run_batch(driver: &CassandraBlockingSession, st: Stmts) -> Result<Vec<RowMap>, E> {
    let mut batch = Batch::new(BatchType::Logged);
    let mut values = Vec::with_capacity(st.items.len());
    for (text, v) in st.items {
        batch.append_statement(scylla::statement::Statement::new(text));
        values.push(v);
    }
    let session = driver.session();
    let result = driver.run(session.batch(&batch, values)).map_err(internal)?;
    let rows = result.into_rows_result().map_err(internal)?;
    let names: Vec<String> = rows.column_specs().iter().map(|c| c.name().to_owned()).collect();
    let mut out = Vec::new();
    for row in rows.rows::<Row>().map_err(internal)? {
        let row = row.map_err(internal)?;
        out.push(names.iter().cloned().zip(row.columns).collect::<RowMap>());
    }
    Ok(out)
}

pub(crate) fn applied(rows: &[RowMap]) -> bool {
    rows.first().and_then(|r| r.get("[applied]")).and_then(|v| v.as_ref()).is_some_and(|v| matches!(v, CqlValue::Boolean(true)))
}

pub(crate) fn cell<'a>(row: &'a RowMap, name: &str) -> Option<&'a CqlValue> {
    row.get(name).and_then(|v| v.as_ref())
}
pub(crate) fn get_i64(v: Option<&CqlValue>) -> Option<i64> {
    match v? {
        CqlValue::BigInt(x) => Some(*x),
        CqlValue::Int(x) => Some(i64::from(*x)),
        CqlValue::SmallInt(x) => Some(i64::from(*x)),
        CqlValue::TinyInt(x) => Some(i64::from(*x)),
        CqlValue::Counter(c) => Some(c.0),
        _ => None,
    }
}
pub(crate) fn get_i32(v: Option<&CqlValue>) -> i32 {
    get_i64(v).unwrap_or_default() as i32
}
pub(crate) fn get_str(v: Option<&CqlValue>) -> String {
    match v {
        Some(CqlValue::Text(x) | CqlValue::Ascii(x)) => x.clone(),
        Some(CqlValue::Uuid(u)) => u.to_string(),
        Some(CqlValue::Timeuuid(u)) => Uuid::from(*u).to_string(),
        _ => String::new(),
    }
}
pub(crate) fn get_blob(v: Option<&CqlValue>) -> Vec<u8> {
    match v {
        Some(CqlValue::Blob(x)) => x.clone(),
        _ => Vec::new(),
    }
}
pub(crate) fn get_bool(v: Option<&CqlValue>) -> bool {
    matches!(v, Some(CqlValue::Boolean(true)))
}
fn get_f64(v: Option<&CqlValue>) -> f64 {
    match v {
        Some(CqlValue::Double(x)) => *x,
        Some(CqlValue::Float(x)) => f64::from(*x),
        _ => 0.0,
    }
}
pub(crate) fn get_ts_nanos(v: Option<&CqlValue>) -> i64 {
    match v {
        Some(CqlValue::Timestamp(t)) => t.0 * 1_000_000,
        _ => 0,
    }
}
pub(crate) fn get_strs(v: Option<&CqlValue>) -> Vec<String> {
    match v {
        Some(CqlValue::List(xs) | CqlValue::Set(xs)) => xs.iter().map(|x| get_str(Some(x))).collect(),
        _ => Vec::new(),
    }
}
/// UDT value -> `field name -> value` view.
pub(crate) fn udt_map(v: Option<&CqlValue>) -> HashMap<&str, &CqlValue> {
    match v {
        Some(CqlValue::UserDefinedType { fields, .. }) => fields.iter().filter_map(|(n, x)| x.as_ref().map(|x| (n.as_str(), x))).collect(),
        _ => HashMap::new(),
    }
}
fn byte_map(v: Option<&CqlValue>) -> std::collections::BTreeMap<String, Vec<u8>> {
    match v {
        Some(CqlValue::Map(entries)) => entries.iter().map(|(k, x)| (get_str(Some(k)), get_blob(Some(x)))).collect(),
        _ => Default::default(),
    }
}
fn string_map(v: Option<&CqlValue>) -> std::collections::BTreeMap<String, String> {
    match v {
        Some(CqlValue::Map(entries)) => entries.iter().map(|(k, x)| (get_str(Some(k)), get_str(Some(x)))).collect(),
        _ => Default::default(),
    }
}

/// Failure details of a not-applied LWT batch (Go `executeCreateWorkflowBatchTransaction` /
/// `executeUpdateWorkflowBatchTransaction` previous-row scan).
pub(crate) struct CasContext<'a> {
    pub shard: i32,
    pub request_range_id: i64,
    pub workflow_id: &'a str,
    pub run_id: &'a str,
    /// Create path only.
    pub create: Option<&'a WorkflowExecutionInfo>,
    pub current_insert: bool,
    pub requested_current: Option<&'a CurrentWrite>,
    pub previous_next_event_id: i64,
}

fn lwv(row: &RowMap) -> i64 {
    get_i64(cell(row, "workflow_last_write_version")).unwrap_or(-24)
}

pub(crate) fn cas_error(rows: &[RowMap], c: &CasContext<'_>) -> E {
    let mut range_mismatch: Option<i64> = None;
    let mut dup_request: Option<i32> = None;
    let mut current_exists: Option<&RowMap> = None;
    let mut concrete_exists: Option<&RowMap> = None;
    let mut run_mismatch: Option<String> = None;
    let mut state_mismatch: Option<String> = None;
    let mut next_event_mismatch: Option<i64> = None;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(format!("{row:?}"));
        let row_type = get_i32(cell(row, "type"));
        if row_type == ROW_TYPE_SHARD {
            let actual = get_i64(cell(row, "range_id")).unwrap_or_default();
            if actual != c.request_range_id {
                range_mismatch = Some(actual);
                break;
            }
        } else if (ROW_TYPE_REQUEST_START..=ROW_TYPE_REQUEST_START + 3).contains(&row_type) {
            dup_request = Some(row_type);
        } else if row_type == ROW_TYPE_EXECUTION {
            let row_run = get_str(cell(row, "run_id"));
            if row_run == PERMANENT_RUN_ID.to_string() {
                if c.create.is_some() && c.current_insert {
                    current_exists = Some(row);
                } else if let Some(CurrentWrite::Update { current_run_id, previous, .. }) = c.requested_current {
                    let actual_run = get_str(cell(row, "current_run_id"));
                    if !current_run_id.is_empty() && *current_run_id != actual_run {
                        run_mismatch = Some(actual_run);
                    } else if let (Some((version, state)), true) = (previous, c.create.is_some()) {
                        let actual_version = lwv(row);
                        let actual_state = get_i32(cell(row, "workflow_state"));
                        if actual_version != *version {
                            state_mismatch = Some(format!(
                                "Workflow execution creation condition failed. WorkflowId: {}, Expected Version: {}, Actual Version: {}",
                                c.workflow_id, version, actual_version
                            ));
                        } else if actual_state != *state {
                            state_mismatch = Some(format!(
                                "Workflow execution creation condition failed. WorkflowId: {}, Expected State: {}, Actual State: {}",
                                c.workflow_id, state, actual_state
                            ));
                        }
                    }
                } else if c.create.is_none() {
                    // update path: expected current run differs
                    run_mismatch = Some(get_str(cell(row, "current_run_id")));
                }
            } else if row_run == c.run_id {
                if c.create.is_some() {
                    concrete_exists = Some(row);
                } else {
                    next_event_mismatch = get_i64(cell(row, "next_event_id"));
                }
            }
        }
    }
    if let Some(actual) = range_mismatch {
        return E::ShardOwnershipLost { shard_id: c.shard, expected_range_id: c.request_range_id, actual_range_id: actual };
    }
    if let Some(row_type) = dup_request {
        return E::ConditionFailed(format!("Workflow request already exists. WorkflowId: {}, request row type: {row_type}", c.workflow_id));
    }
    if let (Some(row), Some(info)) = (current_exists, c.create) {
        let exec = udt_map(cell(row, "execution"));
        let run = get_str(cell(row, "current_run_id"));
        let message = format!("Workflow execution already running. WorkflowId: {}, RunId: {}", c.workflow_id, run);
        if exec.is_empty() {
            return E::CurrentWorkflowConditionFailed(format!("Workflow execution already running. WorkflowId: {}", c.workflow_id));
        }
        let _ = info;
        return E::WorkflowAlreadyStarted {
            message,
            start_request_id: get_str(exec.get("create_request_id").copied()),
            run_id: run,
            state: get_i32(exec.get("state").copied()),
            close_status: get_i32(exec.get("close_status").copied()),
            last_write_version: lwv(row),
        };
    }
    if let Some(actual) = run_mismatch {
        let expected = match c.requested_current {
            Some(CurrentWrite::Update { current_run_id, .. }) => current_run_id.clone(),
            _ => String::new(),
        };
        return if c.create.is_some() {
            E::CurrentWorkflowConditionFailed(format!(
                "Workflow execution creation condition failed by mismatch runID. WorkflowId: {}, Expected Current RunID: {}, Actual Current RunID: {}",
                c.workflow_id, expected, actual
            ))
        } else {
            E::CurrentWorkflowConditionFailed(format!(
                "Failed to update mutable state. requestConditionalRunID: {expected}, Actual Value: {actual}"
            ))
        };
    }
    if let Some(message) = state_mismatch {
        return E::CurrentWorkflowConditionFailed(message);
    }
    if let (Some(row), Some(info)) = (concrete_exists, c.create) {
        let exec = udt_map(cell(row, "execution"));
        let _ = exec;
        return E::WorkflowAlreadyStarted {
            message: format!("Workflow execution already running. WorkflowId: {}, RunId: {}", c.workflow_id, c.run_id),
            start_request_id: info.create_request_id.clone(),
            run_id: c.run_id.to_owned(),
            state: info.state,
            close_status: info.close_status,
            last_write_version: lwv(row),
        };
    }
    if let Some(actual) = next_event_mismatch {
        return E::ConditionFailed(format!(
            "Failed to update mutable state. previousNextEventIDCondition: {}, actualNextEventID: {}, Request Current RunID: {}",
            c.previous_next_event_id, actual, c.run_id
        ));
    }
    if c.create.is_some() {
        E::ConditionFailed(format!(
            "Failed to operate on workflow execution.  Request RangeID: {}, columns: ({})",
            c.request_range_id,
            columns.join(",")
        ))
    } else {
        E::ConditionFailed(format!(
            "Failed to update mutable state. ShardID: {}, RangeID: {}, previousNextEventIDCondition: {}, requestConditionalRunID: , columns: ({})",
            c.shard,
            c.request_range_id,
            c.previous_next_event_id,
            columns.join(",")
        ))
    }
}

pub(crate) fn finish_batch(rows: Vec<RowMap>, c: &CasContext<'_>) -> Result<(), E> {
    if applied(&rows) {
        Ok(())
    } else {
        Err(cas_error(&rows, c))
    }
}

// ---------------------------------------------------------------- reads

/// Go `parseWorkflowExecutionInfo` (execution UDT -> contract info).
pub(crate) fn parse_execution_info(udt: &HashMap<&str, &CqlValue>) -> WorkflowExecutionInfo {
    let g = |n: &str| udt.get(n).copied();
    let nanos_secs = |n: &str| i64::from(get_i32(g(n))) * 1_000_000_000;
    let blob = |data: &str, enc: &str| {
        let data = get_blob(g(data));
        let encoding = get_str(g(enc));
        if data.is_empty() && encoding.is_empty() {
            None
        } else {
            Some(DataBlob { encoding, data })
        }
    };
    let empty_uuid = |n: &str| get_str(g(n));
    WorkflowExecutionInfo {
        domain_id: get_str(g("domain_id")),
        workflow_id: get_str(g("workflow_id")),
        run_id: get_str(g("run_id")),
        first_execution_run_id: empty_uuid("first_run_id"),
        parent_domain_id: empty_uuid("parent_domain_id"),
        parent_workflow_id: get_str(g("parent_workflow_id")),
        parent_run_id: empty_uuid("parent_run_id"),
        initiated_id: get_i64(g("initiated_id")).unwrap_or(-7),
        completion_event_batch_id: get_i64(g("completion_event_batch_id")).unwrap_or(-23),
        create_request_id: get_str(g("create_request_id")),
        state: get_i32(g("state")),
        close_status: get_i32(g("close_status")),
        next_event_id: get_i64(g("next_event_id")).unwrap_or_default(),
        last_first_event_id: get_i64(g("last_first_event_id")).unwrap_or_default(),
        last_event_task_id: get_i64(g("last_event_task_id")).unwrap_or_default(),
        last_processed_event: get_i64(g("last_processed_event")).unwrap_or_default(),
        last_write_version: 0,
        start_time_unix_nanos: get_ts_nanos(g("start_time")),
        last_updated_time_unix_nanos: get_ts_nanos(g("last_updated_time")),
        workflow_timeout_nanos: nanos_secs("workflow_timeout"),
        decision_start_to_close_timeout_nanos: nanos_secs("decision_task_timeout"),
        execution_context: get_blob(g("execution_context")),
        signal_count: get_i32(g("signal_count")),
        decision_version: get_i64(g("decision_version")).unwrap_or_default(),
        decision_schedule_id: get_i64(g("decision_schedule_id")).unwrap_or_default(),
        decision_started_id: get_i64(g("decision_started_id")).unwrap_or_default(),
        decision_request_id: get_str(g("decision_request_id")),
        decision_timeout_nanos: nanos_secs("decision_timeout"),
        decision_attempt: get_i64(g("decision_attempt")).unwrap_or_default(),
        decision_started_time_unix_nanos: get_i64(g("decision_timestamp")).unwrap_or_default(),
        decision_scheduled_time_unix_nanos: get_i64(g("decision_scheduled_timestamp")).unwrap_or_default(),
        decision_original_scheduled_time_unix_nanos: get_i64(g("decision_original_scheduled_timestamp")).unwrap_or_default(),
        cancel_requested: get_bool(g("cancel_requested")),
        cancel_request_id: get_str(g("cancel_request_id")),
        task_list: get_str(g("task_list")),
        task_list_kind: get_i32(g("task_list_kind")),
        workflow_type_name: get_str(g("workflow_type_name")),
        sticky_task_list: get_str(g("sticky_task_list")),
        sticky_schedule_to_start_timeout_nanos: nanos_secs("sticky_schedule_to_start_timeout"),
        client_library_version: get_str(g("client_library_version")),
        client_feature_version: get_str(g("client_feature_version")),
        client_impl: get_str(g("client_impl")),
        cron_schedule: get_str(g("cron_schedule")),
        cron_overlap_policy: get_i32(g("cron_overlap_policy")),
        expiration_interval_nanos: nanos_secs("expiration_seconds"),
        completion_event: blob("completion_event", "completion_event_data_encoding"),
        auto_reset_points: blob("auto_reset_points", "auto_reset_points_encoding"),
        branch_token: get_blob(g("branch_token")),
        attempt: get_i32(g("attempt")),
        has_retry_policy: get_bool(g("has_retry_policy")),
        initial_interval_nanos: nanos_secs("init_interval"),
        backoff_coefficient: get_f64(g("backoff_coefficient")),
        jitter_coefficient: get_f64(g("jitter_coefficient")),
        maximum_interval_nanos: nanos_secs("max_interval"),
        expiration_time_unix_nanos: get_ts_nanos(g("expiration_time")),
        maximum_attempts: get_i32(g("max_attempts")),
        non_retriable_errors: get_strs(g("non_retriable_errors")),
        memo: byte_map(g("memo")),
        search_attributes: byte_map(g("search_attributes")),
        partition_config: string_map(g("partition_config")),
        active_cluster_selection_policy: blob("active_cluster_selection_policy", "active_cluster_selection_policy_encoding"),
        history_size: get_i64(g("history_size")).unwrap_or_default(),
        is_cron: !get_str(g("cron_schedule")).is_empty(),
    }
}

fn encode_blob(st: &cadence_thrift::value::Struct) -> Result<DataBlob, E> {
    Ok(DataBlob { encoding: Encoding::ThriftRW.as_str().to_owned(), data: Encoding::ThriftRW.encode(st).map_err(internal)? })
}

fn map_entries(v: Option<&CqlValue>) -> Vec<(&CqlValue, HashMap<&str, &CqlValue>)> {
    match v {
        Some(CqlValue::Map(entries)) => entries.iter().map(|(k, x)| (k, udt_map(Some(x)))).collect(),
        _ => Vec::new(),
    }
}

/// Go `SelectWorkflowExecution` + the nosql store conversion.
pub(crate) fn read_execution(
    driver: &CassandraBlockingSession,
    shard: i32,
    domain: &str,
    workflow: &str,
    run: &str,
) -> Result<WorkflowSnapshot, E> {
    let session = driver.session();
    let result = driver
        .run(session.query_unpaged(
            "SELECT execution, replication_state, activity_map, timer_map, child_executions_map, request_cancel_map, signal_map, semaphore_map, signal_requested, buffered_events_list, buffered_replication_tasks_map, version_histories, version_histories_encoding, checksum, next_event_id, workflow_last_write_version, workflow_timer_tasks FROM executions WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ?",
            (shard, ROW_TYPE_EXECUTION, parse_uuid(domain)?, workflow.to_owned(), parse_uuid(run)?, vis_ts(), EXECUTION_TASK_ID),
        ))
        .map_err(internal)?;
    let rows = result.into_rows_result().map_err(internal)?;
    let names: Vec<String> = rows.column_specs().iter().map(|c| c.name().to_owned()).collect();
    let row = rows
        .maybe_first_row::<Row>()
        .map_err(internal)?
        .ok_or_else(|| E::EntityNotExists(format!("Workflow execution not found.  WorkflowId: {workflow}, RunId: {run}")))?;
    let row: RowMap = names.into_iter().zip(row.columns).collect();
    let exec = udt_map(cell(&row, "execution"));
    let mut info = parse_execution_info(&exec);
    let next_event_id = get_i64(cell(&row, "next_event_id")).unwrap_or(info.next_event_id);
    info.next_event_id = next_event_id;
    let last_write_version = get_i64(cell(&row, "workflow_last_write_version")).unwrap_or(-24);
    info.last_write_version = last_write_version;

    let mut snapshot = WorkflowSnapshot { execution_info: info, start_version: last_write_version, last_write_version, condition: next_event_id, ..Default::default() };
    let vh = get_blob(cell(&row, "version_histories"));
    let vh_enc = get_str(cell(&row, "version_histories_encoding"));
    if !vh.is_empty() || !vh_enc.is_empty() {
        snapshot.version_histories = Some(DataBlob { encoding: vh_enc, data: vh });
    }
    for (k, a) in map_entries(cell(&row, "activity_map")) {
        let g = |n: &str| a.get(n).copied();
        let schedule_id = get_i64(g("schedule_id")).unwrap_or_default();
        if schedule_id == ACTIVITY_SENTINEL_SCHEDULE_ID {
            continue;
        }
        let key = get_i64(Some(k)).unwrap_or(schedule_id);
        let info = ActivityInfo {
            version: get_i64(g("version")).unwrap_or_default(),
            scheduled_event_batch_id: get_i64(g("scheduled_event_batch_id")).unwrap_or_default(),
            scheduled_event: get_blob(g("scheduled_event")),
            scheduled_event_encoding: get_str(g("event_data_encoding")),
            scheduled_time_unix_nanos: get_ts_nanos(g("scheduled_time")),
            started_id: get_i64(g("started_id")).unwrap_or_default(),
            started_event: get_blob(g("started_event")),
            started_event_encoding: get_str(g("event_data_encoding")),
            started_time_unix_nanos: get_ts_nanos(g("started_time")),
            activity_id: get_str(g("activity_id")),
            request_id: get_str(g("request_id")),
            schedule_to_start_timeout_seconds: get_i32(g("schedule_to_start_timeout")),
            schedule_to_close_timeout_seconds: get_i32(g("schedule_to_close_timeout")),
            start_to_close_timeout_seconds: get_i32(g("start_to_close_timeout")),
            heartbeat_timeout_seconds: get_i32(g("heart_beat_timeout")),
            cancel_requested: get_bool(g("cancel_requested")),
            cancel_request_id: get_i64(g("cancel_request_id")).unwrap_or_default(),
            timer_task_status: get_i32(g("timer_task_status")),
            last_heartbeat_timeout_visibility_in_seconds: 0,
            attempt: get_i32(g("attempt")),
            task_list: get_str(g("task_list")),
            task_list_kind: get_i32(g("task_list_kind")),
            started_identity: get_str(g("started_identity")),
            has_retry_policy: get_bool(g("has_retry_policy")),
            retry_initial_interval_seconds: get_i32(g("init_interval")),
            retry_maximum_interval_seconds: get_i32(g("max_interval")),
            retry_maximum_attempts: get_i32(g("max_attempts")),
            retry_expiration_time_unix_nanos: get_ts_nanos(g("expiration_time")),
            retry_backoff_coefficient: get_f64(g("backoff_coefficient")),
            retry_jitter_coefficient: get_f64(g("jitter_coefficient")),
            retry_non_retriable_errors: get_strs(g("non_retriable_errors")),
            retry_last_failure_reason: get_str(g("last_failure_reason")),
            retry_last_worker_identity: get_str(g("last_worker_identity")),
            retry_last_failure_details: get_blob(g("last_failure_details")),
            retry_last_failure_category: get_i32(g("last_failure_category")),
            retry_last_retry_interval_seconds: get_i32(g("last_retry_interval_seconds")),
        };
        snapshot.activity_infos.push(KeyedBlob { key, value: encode_blob(&sqlblobs::activity_info_to_struct(&info))? });
        snapshot.activity_heartbeats.insert(
            key,
            ActivityHeartbeat { details: get_blob(g("details")), updated_time_unix_nanos: get_ts_nanos(g("last_hb_updated_time")) },
        );
    }
    for (k, t) in map_entries(cell(&row, "timer_map")) {
        let g = |n: &str| t.get(n).copied();
        let timer_id = get_str(g("timer_id"));
        if timer_id.is_empty() {
            continue;
        }
        let info = TimerInfo {
            version: get_i64(g("version")).unwrap_or_default(),
            started_id: get_i64(g("started_id")).unwrap_or_default(),
            expiry_time_unix_nanos: get_ts_nanos(g("expiry_time")),
            task_id: get_i64(g("task_id")).unwrap_or_default(),
        };
        snapshot.timer_infos.push(KeyedBlob { key: get_str(Some(k)), value: encode_blob(&sqlblobs::timer_info_to_struct(&info))? });
    }
    for (k, c) in map_entries(cell(&row, "child_executions_map")) {
        let g = |n: &str| c.get(n).copied();
        let started_run = match g("started_run_id") {
            Some(CqlValue::Uuid(u)) if *u != Uuid::from_u128(0x30000000_0000_f000_f000_000000000000) => u.as_bytes().to_vec(),
            _ => Vec::new(),
        };
        let info = ChildExecutionInfo {
            version: get_i64(g("version")).unwrap_or_default(),
            initiated_event_batch_id: get_i64(g("initiated_event_batch_id")).unwrap_or_default(),
            started_id: get_i64(g("started_id")).unwrap_or_default(),
            initiated_event: get_blob(g("initiated_event")),
            initiated_event_encoding: get_str(g("event_data_encoding")),
            started_workflow_id: get_str(g("started_workflow_id")),
            started_run_id: started_run,
            started_event: get_blob(g("started_event")),
            started_event_encoding: get_str(g("event_data_encoding")),
            create_request_id: get_str(g("create_request_id")),
            domain_id: get_str(g("domain_id")),
            domain_name: get_str(g("domain_name")),
            workflow_type_name: get_str(g("workflow_type_name")),
            parent_close_policy: get_i32(g("parent_close_policy")),
        };
        snapshot.child_execution_infos.push(KeyedBlob { key: get_i64(Some(k)).unwrap_or_default(), value: encode_blob(&sqlblobs::child_execution_info_to_struct(&info))? });
    }
    for (k, r) in map_entries(cell(&row, "request_cancel_map")) {
        let g = |n: &str| r.get(n).copied();
        let info = RequestCancelInfo {
            version: get_i64(g("version")).unwrap_or_default(),
            initiated_event_batch_id: get_i64(g("initiated_event_batch_id")).unwrap_or_default(),
            cancel_request_id: get_str(g("cancel_request_id")),
        };
        snapshot.request_cancel_infos.push(KeyedBlob { key: get_i64(Some(k)).unwrap_or_default(), value: encode_blob(&sqlblobs::request_cancel_info_to_struct(&info))? });
    }
    for (k, sg) in map_entries(cell(&row, "signal_map")) {
        let g = |n: &str| sg.get(n).copied();
        let info = SignalInfo {
            version: get_i64(g("version")).unwrap_or_default(),
            initiated_event_batch_id: get_i64(g("initiated_event_batch_id")).unwrap_or_default(),
            request_id: get_str(g("signal_request_id")),
            name: get_str(g("signal_name")),
            input: get_blob(g("input")),
            control: get_blob(g("control")),
        };
        snapshot.signal_infos.push(KeyedBlob { key: get_i64(Some(k)).unwrap_or_default(), value: encode_blob(&sqlblobs::signal_info_to_struct(&info))? });
    }
    snapshot.signal_requested_ids = get_strs(cell(&row, "signal_requested"));
    if let Some(CqlValue::List(events)) = cell(&row, "buffered_events_list") {
        for event in events {
            let m = udt_map(Some(event));
            snapshot.buffered_events.push(DataBlob {
                encoding: {
                    let e = get_str(m.get("encoding_type").copied());
                    if e.is_empty() { "thriftrw".to_owned() } else { e }
                },
                data: get_blob(m.get("data").copied()),
            });
        }
    }
    if let Some(CqlValue::Set(tasks) | CqlValue::List(tasks)) = cell(&row, "workflow_timer_tasks") {
        for t in tasks {
            if let CqlValue::Tuple(parts) = t {
                snapshot.workflow_timer_tasks.push(HistoryTaskKey {
                    task_id: get_i64(parts.get(1).and_then(|x| x.as_ref())).unwrap_or_default(),
                    version: get_ts_nanos(parts.first().and_then(|x| x.as_ref())),
                });
            }
        }
    }
    let ck = udt_map(cell(&row, "checksum"));
    snapshot.checksum = checksum_blob(get_i32(ck.get("version").copied()), get_i32(ck.get("flavor").copied()), get_blob(ck.get("value").copied()));
    Ok(snapshot)
}

// ---------------------------------------------------------------- operations

fn snapshot_write(s: &WorkflowSnapshot) -> ExecWrite<'_> {
    ExecWrite {
        info: &s.execution_info,
        version_histories: &s.version_histories,
        checksum: &s.checksum,
        last_write_version: s.last_write_version,
        heartbeats: &s.activity_heartbeats,
        workflow_timer_tasks: &s.workflow_timer_tasks,
    }
}

struct Ids {
    domain: Uuid,
    workflow: String,
    run: Uuid,
}
fn ids(info: &WorkflowExecutionInfo) -> Result<Ids, E> {
    Ok(Ids { domain: parse_uuid(&info.domain_id)?, workflow: info.workflow_id.clone(), run: parse_uuid(&info.run_id)? })
}

/// Go `assertNotCurrentExecution`.
fn assert_not_current(driver: &CassandraBlockingSession, shard: i32, domain: Uuid, workflow: &str, run: &str) -> Result<(), E> {
    let session = driver.session();
    let values = key_values(shard, ROW_TYPE_EXECUTION, domain, workflow, PERMANENT_RUN_ID, EXECUTION_TASK_ID);
    let result = driver
        .run(session.query_unpaged("SELECT current_run_id FROM executions WHERE shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ? and visibility_ts = ? and task_id = ?", &values[..]))
        .map_err(internal)?;
    let rows = result.into_rows_result().map_err(internal)?;
    if let Some((current,)) = rows.maybe_first_row::<(Option<Uuid>,)>().map_err(internal)? {
        if current.map(|u| u.to_string()).as_deref() == Some(run) {
            return Err(E::ConditionFailed(format!("Assertion on current record failed. Current run ID is not expected: {run}")));
        }
    }
    Ok(())
}

pub(crate) fn create_workflow_execution(driver: &CassandraBlockingSession, req: CreateWorkflowExecutionRequest) -> Result<CurrentExecution, E> {
    let snapshot = &req.new_workflow_snapshot;
    let info = &snapshot.execution_info;
    let id = ids(info)?;
    let now = nanos_ts(req.current_timestamp_unix_nanos);
    let row = current_row(info, snapshot.last_write_version);
    let write = match req.mode {
        CreateWorkflowMode::Zombie => CurrentWrite::Noop,
        CreateWorkflowMode::ContinueAsNew => CurrentWrite::Update { row, current_run_id: req.previous_run_id.clone(), previous: None },
        CreateWorkflowMode::WorkflowIdReuse => CurrentWrite::Update {
            row,
            current_run_id: req.previous_run_id.clone(),
            previous: Some((req.previous_last_write_version, 2)),
        },
        CreateWorkflowMode::BrandNew => CurrentWrite::Insert(row),
    };
    let mut st = Stmts::default();
    push_policy(&mut st, req.shard_id, id.domain, &id.workflow, id.run, &info.active_cluster_selection_policy, now);
    push_requests(&mut st, req.shard_id, id.domain, &id.workflow, &info.run_id, snapshot.last_write_version, &snapshot.workflow_requests, req.workflow_request_mode, now)?;
    push_current(&mut st, req.shard_id, id.domain, &id.workflow, &write, now)?;
    push_create_execution(&mut st, req.shard_id, id.domain, &id.workflow, id.run, &snapshot_write(snapshot), snapshot, now)?;
    push_tasks_by_category(&mut st, req.shard_id, &snapshot.tasks_by_category, now)?;
    push_lease(&mut st, req.shard_id, req.range_id, now);
    let rows = run_batch(driver, st)?;
    let ctx = CasContext {
        shard: req.shard_id,
        request_range_id: req.range_id,
        workflow_id: &id.workflow,
        run_id: &info.run_id,
        create: Some(info),
        current_insert: matches!(write, CurrentWrite::Insert(_)),
        requested_current: Some(&write),
        previous_next_event_id: 0,
    };
    finish_batch(rows, &ctx)?;
    Ok(CurrentExecution {
        run_id: info.run_id.clone(),
        create_request_id: info.create_request_id.clone(),
        state: info.state,
        close_status: info.close_status,
        start_version: snapshot.start_version,
        last_write_version: snapshot.last_write_version,
    })
}

pub(crate) fn update_workflow_execution(driver: &CassandraBlockingSession, req: UpdateWorkflowExecutionRequest) -> Result<(), E> {
    let m = &req.update_workflow_mutation;
    let id = ids(&m.execution_info)?;
    let now = nanos_ts(req.current_timestamp_unix_nanos);
    let write = match req.mode {
        UpdateWorkflowMode::IgnoreCurrent => CurrentWrite::Noop,
        UpdateWorkflowMode::BypassCurrent => {
            assert_not_current(driver, req.shard_id, id.domain, &id.workflow, &m.execution_info.run_id)?;
            CurrentWrite::Noop
        }
        UpdateWorkflowMode::UpdateCurrent => {
            let row = match &req.new_workflow_snapshot {
                Some(s) => current_row(&s.execution_info, s.last_write_version),
                None => current_row(&m.execution_info, m.last_write_version),
            };
            CurrentWrite::Update { row, current_run_id: m.execution_info.run_id.clone(), previous: None }
        }
    };
    let mut st = Stmts::default();
    push_requests(&mut st, req.shard_id, id.domain, &id.workflow, &m.execution_info.run_id, m.last_write_version, &m.workflow_requests, req.workflow_request_mode, now)?;
    if let Some(s) = &req.new_workflow_snapshot {
        push_requests(&mut st, req.shard_id, id.domain, &id.workflow, &s.execution_info.run_id, s.last_write_version, &s.workflow_requests, req.workflow_request_mode, now)?;
    }
    push_current(&mut st, req.shard_id, id.domain, &id.workflow, &write, now)?;
    push_mutation(&mut st, req.shard_id, id.domain, &id.workflow, id.run, m, now)?;
    if let Some(s) = &req.new_workflow_snapshot {
        let nid = ids(&s.execution_info)?;
        push_policy(&mut st, req.shard_id, nid.domain, &nid.workflow, nid.run, &s.execution_info.active_cluster_selection_policy, now);
        push_create_execution(&mut st, req.shard_id, nid.domain, &nid.workflow, nid.run, &snapshot_write(s), s, now)?;
    }
    push_tasks_by_category(&mut st, req.shard_id, &m.tasks_by_category, now)?;
    if let Some(s) = &req.new_workflow_snapshot {
        push_tasks_by_category(&mut st, req.shard_id, &s.tasks_by_category, now)?;
    }
    push_lease(&mut st, req.shard_id, req.range_id, now);
    let rows = run_batch(driver, st)?;
    let ctx = CasContext {
        shard: req.shard_id,
        request_range_id: req.range_id,
        workflow_id: &id.workflow,
        run_id: &m.execution_info.run_id,
        create: None,
        current_insert: false,
        requested_current: Some(&write),
        previous_next_event_id: m.condition,
    };
    finish_batch(rows, &ctx)
}

pub(crate) fn conflict_resolve_workflow_execution(driver: &CassandraBlockingSession, req: ConflictResolveWorkflowExecutionRequest) -> Result<(), E> {
    let reset = &req.reset_workflow_snapshot;
    let rid = ids(&reset.execution_info)?;
    let now = nanos_ts(req.current_timestamp_unix_nanos);
    let write = match req.mode {
        ConflictResolveWorkflowMode::BypassCurrent => {
            assert_not_current(driver, req.shard_id, rid.domain, &rid.workflow, &reset.execution_info.run_id)?;
            CurrentWrite::Noop
        }
        ConflictResolveWorkflowMode::UpdateCurrent => {
            let condition = match &req.current_workflow_mutation {
                Some(m) => m.execution_info.run_id.clone(),
                None => reset.execution_info.run_id.clone(),
            };
            let row = match &req.new_workflow_snapshot {
                Some(s) => current_row(&s.execution_info, s.last_write_version),
                None => current_row(&reset.execution_info, reset.last_write_version),
            };
            CurrentWrite::Update { row, current_run_id: condition, previous: None }
        }
    };
    let mut st = Stmts::default();
    push_requests(&mut st, req.shard_id, rid.domain, &rid.workflow, &reset.execution_info.run_id, reset.last_write_version, &reset.workflow_requests, req.workflow_request_mode, now)?;
    if let Some(s) = &req.new_workflow_snapshot {
        push_requests(&mut st, req.shard_id, rid.domain, &rid.workflow, &s.execution_info.run_id, s.last_write_version, &s.workflow_requests, req.workflow_request_mode, now)?;
    }
    if let Some(m) = &req.current_workflow_mutation {
        push_requests(&mut st, req.shard_id, rid.domain, &rid.workflow, &m.execution_info.run_id, m.last_write_version, &m.workflow_requests, req.workflow_request_mode, now)?;
    }
    push_current(&mut st, req.shard_id, rid.domain, &rid.workflow, &write, now)?;
    if let Some(m) = &req.current_workflow_mutation {
        let cid = ids(&m.execution_info)?;
        push_mutation(&mut st, req.shard_id, cid.domain, &cid.workflow, cid.run, m, now)?;
    }
    push_reset(&mut st, req.shard_id, rid.domain, &rid.workflow, rid.run, reset, now)?;
    if let Some(s) = &req.new_workflow_snapshot {
        let nid = ids(&s.execution_info)?;
        push_create_execution(&mut st, req.shard_id, nid.domain, &nid.workflow, nid.run, &snapshot_write(s), s, now)?;
    }
    push_tasks_by_category(&mut st, req.shard_id, &reset.tasks_by_category, now)?;
    if let Some(s) = &req.new_workflow_snapshot {
        push_tasks_by_category(&mut st, req.shard_id, &s.tasks_by_category, now)?;
    }
    if let Some(m) = &req.current_workflow_mutation {
        push_tasks_by_category(&mut st, req.shard_id, &m.tasks_by_category, now)?;
    }
    push_lease(&mut st, req.shard_id, req.range_id, now);
    let rows = run_batch(driver, st)?;
    let ctx = CasContext {
        shard: req.shard_id,
        request_range_id: req.range_id,
        workflow_id: &rid.workflow,
        run_id: &reset.execution_info.run_id,
        create: None,
        current_insert: false,
        requested_current: Some(&write),
        previous_next_event_id: reset.condition,
    };
    finish_batch(rows, &ctx)
}

pub(crate) fn get_workflow_execution(driver: &CassandraBlockingSession, shard: i32, domain: &str, workflow: &str, run: &str) -> Result<WorkflowSnapshot, E> {
    read_execution(driver, shard, domain, workflow, run)
}
