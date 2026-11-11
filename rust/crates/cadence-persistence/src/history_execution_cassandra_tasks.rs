//! Task, DLQ, listing and policy paths of the Cassandra `GoExecutionStore`
//! (Go `nosqlplugin/cassandra/workflow.go` task functions, `workflow_cql.go`
//! task templates and `nosql_execution_store.go` 637-1165).
#![allow(clippy::too_many_arguments)]

use crate::cassandra::CassandraBlockingSession;
use crate::history_contract::*;
use crate::history_execution_cassandra::{
    internal, parse_uuid, vis_ts, DEFAULT_VISIBILITY_MS, PERMANENT_RUN_ID,
    ROW_TYPE_EXECUTION,
};
use crate::history_execution_cassandra_write::{
    applied, cell, decode_struct, get_blob, get_bool, get_i32, get_i64, get_str, get_strs,
    get_ts_nanos, ms_ts, parse_execution_info, push_lease, push_task_row, replication_fields,
    run_batch, timer_task_fields, transfer_fields, udt_map, RowMap, Stmts, POLICY_TASK_ID,
    REPLICATION_DOMAIN, REPLICATION_RUN, REPLICATION_WORKFLOW, ROW_TYPE_POLICY, ROW_TYPE_REPLICATION,
    ROW_TYPE_SHARD, ROW_TYPE_TIMER, ROW_TYPE_TRANSFER, TIMER_DOMAIN, TIMER_RUN, TIMER_WORKFLOW,
    TRANSFER_DOMAIN, TRANSFER_RUN, TRANSFER_WORKFLOW,
};
use crate::matching_cassandra_exec::{bound, BoundValues};
use crate::sqlblobs::{self, Encoding};
use chrono::{DateTime, Utc};
use scylla::frame::types::Consistency;
use scylla::response::query_result::QueryResult;
use scylla::response::PagingState;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::Statement;
use scylla::value::{CqlValue, Row};
use uuid::Uuid;

type E = PersistenceError;

const ROW_TYPE_DLQ: i32 = 5;
const ROW_TYPE_CROSS_CLUSTER: i32 = 6;
const DLQ_DOMAIN: Uuid = Uuid::from_u128(0x10000000_6000_f000_f000_000000000000);
const DLQ_RUN: Uuid = Uuid::from_u128(0x30000000_6000_f000_f000_000000000000);
const CROSS_DOMAIN: Uuid = Uuid::from_u128(0x10000000_7000_f000_f000_000000000000);
const CROSS_RUN: Uuid = Uuid::from_u128(0x30000000_7000_f000_f000_000000000000);
const TRANSFER_TARGET_RUN: &str = "30000000-2000-f000-f000-000000000000";
const DELETE_CHUNK: usize = 100;
const KEY_PREFIX: &str = "shard_id = ? and type = ? and domain_id = ? and workflow_id = ? and run_id = ?";

/// Identity columns of a task row family.
struct Ident {
    row_type: i32,
    domain: Uuid,
    workflow: String,
    run: Uuid,
}

fn transfer_id() -> Ident {
    Ident { row_type: ROW_TYPE_TRANSFER, domain: TRANSFER_DOMAIN, workflow: TRANSFER_WORKFLOW.into(), run: TRANSFER_RUN }
}
fn timer_id() -> Ident {
    Ident { row_type: ROW_TYPE_TIMER, domain: TIMER_DOMAIN, workflow: TIMER_WORKFLOW.into(), run: TIMER_RUN }
}
fn replication_id() -> Ident {
    Ident { row_type: ROW_TYPE_REPLICATION, domain: REPLICATION_DOMAIN, workflow: REPLICATION_WORKFLOW.into(), run: REPLICATION_RUN }
}
fn dlq_id(source_cluster: &str) -> Ident {
    Ident { row_type: ROW_TYPE_DLQ, domain: DLQ_DOMAIN, workflow: source_cluster.into(), run: DLQ_RUN }
}
fn cross_id(target_cluster: &str) -> Ident {
    Ident { row_type: ROW_TYPE_CROSS_CLUSTER, domain: CROSS_DOMAIN, workflow: target_cluster.into(), run: CROSS_RUN }
}

fn base(shard: i32, id: &Ident) -> BoundValues {
    vec![bound(shard), bound(id.row_type), bound(id.domain), bound(id.workflow.clone()), bound(id.run)]
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Transfer,
    Timer,
    Replication,
}

fn kind_of(category: &HistoryTaskCategory) -> Result<Kind, E> {
    match (category.category_type, category.category_id) {
        (1, 1) => Ok(Kind::Transfer),
        (2, 2) => Ok(Kind::Timer),
        (1, 3) => Ok(Kind::Replication),
        _ => Err(E::InvalidArgument("Unknown task category".into())),
    }
}

// ---------------------------------------------------------------- execution helpers

fn rows_of(result: QueryResult) -> Result<Vec<RowMap>, E> {
    let rows = result.into_rows_result().map_err(internal)?;
    let names: Vec<String> = rows.column_specs().iter().map(|c| c.name().to_owned()).collect();
    let mut out = Vec::new();
    for row in rows.rows::<Row>().map_err(internal)? {
        let row = row.map_err(internal)?;
        out.push(names.iter().cloned().zip(row.columns).collect::<RowMap>());
    }
    Ok(out)
}

fn query_page(
    driver: &CassandraBlockingSession,
    text: String,
    values: BoundValues,
    page_size: i32,
    token: &[u8],
) -> Result<(Vec<RowMap>, Vec<u8>), E> {
    let mut statement = Statement::new(text);
    if page_size > 0 {
        statement.set_page_size(page_size);
    }
    let paging = if token.is_empty() { PagingState::start() } else { PagingState::new_from_raw_bytes(token.to_vec()) };
    let session = driver.session();
    let (result, response) = driver.run(session.query_single_page(statement, &values, paging)).map_err(internal)?;
    let next = match response.into_paging_control_flow() {
        std::ops::ControlFlow::Continue(state) => state.as_bytes_slice().map(|b| b.to_vec()).unwrap_or_default(),
        std::ops::ControlFlow::Break(()) => Vec::new(),
    };
    Ok((rows_of(result)?, next))
}

fn query_all(driver: &CassandraBlockingSession, text: String, values: BoundValues) -> Result<Vec<RowMap>, E> {
    let session = driver.session();
    let result = driver.run(session.query_unpaged(Statement::new(text), &values)).map_err(internal)?;
    rows_of(result)
}

/// Delete statements use consistency All (Go `executeWithConsistencyAll`).
fn exec_all(driver: &CassandraBlockingSession, text: String, values: BoundValues) -> Result<(), E> {
    let mut statement = Statement::new(text);
    statement.set_consistency(Consistency::All);
    let session = driver.session();
    driver.run(session.query_unpaged(statement, &values)).map_err(internal)?;
    Ok(())
}

fn batch_plain(driver: &CassandraBlockingSession, items: Vec<(String, BoundValues)>, all: bool) -> Result<(), E> {
    if items.is_empty() {
        return Ok(());
    }
    let mut batch = Batch::new(BatchType::Logged);
    if all {
        batch.set_consistency(Consistency::All);
    }
    let mut values = Vec::with_capacity(items.len());
    for (text, v) in items {
        batch.append_statement(Statement::new(text));
        values.push(v);
    }
    let session = driver.session();
    driver.run(session.batch(&batch, values)).map_err(internal)?;
    Ok(())
}

fn encode_thrift(st: &cadence_thrift::value::Struct) -> Result<DataBlob, E> {
    let data = Encoding::ThriftRW.encode(st).map_err(internal)?;
    Ok(DataBlob { encoding: Encoding::ThriftRW.as_str().into(), data })
}

// ---------------------------------------------------------------- reading task rows

fn transfer_from_udt(u: &std::collections::HashMap<&str, &CqlValue>) -> TransferTask {
    let g = |n: &str| u.get(n).copied();
    let target_run = get_str(g("target_run_id"));
    TransferTask {
        domain_id: get_str(g("domain_id")),
        workflow_id: get_str(g("workflow_id")),
        run_id: get_str(g("run_id")),
        visibility_time_unix_nanos: get_ts_nanos(g("visibility_ts")),
        task_id: get_i64(g("task_id")).unwrap_or_default(),
        target_domain_id: get_str(g("target_domain_id")),
        target_domain_ids: get_strs(g("target_domain_ids")),
        target_workflow_id: get_str(g("target_workflow_id")),
        target_run_id: if target_run == TRANSFER_TARGET_RUN { String::new() } else { target_run },
        target_child_workflow_only: get_bool(g("target_child_workflow_only")),
        task_list: get_str(g("task_list")),
        task_type: get_i32(g("type")),
        schedule_id: get_i64(g("schedule_id")).unwrap_or_default(),
        version: get_i64(g("version")).unwrap_or_default(),
        record_visibility: get_bool(g("record_visibility")),
        original_task_list: get_str(g("original_task_list")),
        original_task_list_kind: get_i32(g("original_task_list_kind")),
    }
}

fn timer_from_udt(u: &std::collections::HashMap<&str, &CqlValue>) -> TimerTask {
    let g = |n: &str| u.get(n).copied();
    TimerTask {
        domain_id: get_str(g("domain_id")),
        workflow_id: get_str(g("workflow_id")),
        run_id: get_str(g("run_id")),
        visibility_timestamp_unix_nanos: get_ts_nanos(g("visibility_ts")),
        task_id: get_i64(g("task_id")).unwrap_or_default(),
        task_type: get_i32(g("type")),
        timeout_type: get_i32(g("timeout_type")),
        event_id: get_i64(g("event_id")).unwrap_or_default(),
        schedule_attempt: get_i64(g("schedule_attempt")).unwrap_or_default(),
        version: get_i64(g("version")).unwrap_or_default(),
        task_list: get_str(g("task_list")),
    }
}

fn replication_from_udt(u: &std::collections::HashMap<&str, &CqlValue>) -> ReplicationTask {
    let g = |n: &str| u.get(n).copied();
    ReplicationTask {
        domain_id: get_str(g("domain_id")),
        workflow_id: get_str(g("workflow_id")),
        run_id: get_str(g("run_id")),
        task_id: get_i64(g("task_id")).unwrap_or_default(),
        task_type: get_i32(g("type")),
        first_event_id: get_i64(g("first_event_id")).unwrap_or_default(),
        next_event_id: get_i64(g("next_event_id")).unwrap_or_default(),
        version: get_i64(g("version")).unwrap_or_default(),
        scheduled_id: get_i64(g("scheduled_id")).unwrap_or_default(),
        event_store_version: get_i32(g("event_store_version")),
        new_run_event_store_version: get_i32(g("new_run_event_store_version")),
        branch_token: get_blob(g("branch_token")),
        new_run_branch_token: get_blob(g("new_run_branch_token")),
        creation_time_unix_nanos: get_i64(g("created_time")).unwrap_or_default(),
    }
}

fn column_of(kind: Kind) -> &'static str {
    match kind {
        Kind::Transfer => "transfer",
        Kind::Timer => "timer",
        Kind::Replication => "replication",
    }
}

/// `(task type, serialized task blob)`. Rows written without a data blob
/// (legacy) are re-serialized from the UDT columns.
fn row_blob(row: &RowMap, kind: Kind) -> Result<(i32, DataBlob), E> {
    let udt = udt_map(cell(row, column_of(kind)));
    let task_type = get_i32(udt.get("type").copied());
    let data = get_blob(cell(row, "data"));
    if !data.is_empty() {
        return Ok((task_type, DataBlob { encoding: get_str(cell(row, "data_encoding")), data }));
    }
    let st = match kind {
        Kind::Transfer => sqlblobs::transfer_task_to_struct(&transfer_from_udt(&udt)),
        Kind::Timer => sqlblobs::timer_task_to_struct(&timer_from_udt(&udt)),
        Kind::Replication => sqlblobs::replication_task_to_struct(&replication_from_udt(&udt)),
    }
    .map_err(internal)?;
    Ok((task_type, encode_thrift(&st)?))
}

struct RawTask {
    task_id: i64,
    visibility_nanos: i64,
    task_type: i32,
    blob: DataBlob,
}

fn select_immediate(
    driver: &CassandraBlockingSession,
    shard: i32,
    kind: Kind,
    min: i64,
    max: i64,
    page_size: i32,
    token: &[u8],
) -> Result<(Vec<RawTask>, Vec<u8>), E> {
    let (id, column) = match kind {
        Kind::Transfer => (transfer_id(), "transfer"),
        Kind::Replication => (replication_id(), "replication"),
        Kind::Timer => return Err(E::InvalidArgument("Unknown task category".into())),
    };
    let text = format!(
        "SELECT task_id, {column}, data, data_encoding FROM executions WHERE {KEY_PREFIX} and visibility_ts = ? and task_id >= ? and task_id < ?"
    );
    let mut v = base(shard, &id);
    v.extend([bound(vis_ts()), bound(min), bound(max)]);
    let (rows, next) = query_page(driver, text, v, page_size, token)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let (task_type, blob) = row_blob(row, kind)?;
        out.push(RawTask { task_id: get_i64(cell(row, "task_id")).unwrap_or_default(), visibility_nanos: 0, task_type, blob });
    }
    Ok((out, next))
}

fn select_timers(
    driver: &CassandraBlockingSession,
    shard: i32,
    min_nanos: i64,
    max_nanos: i64,
    page_size: i32,
    token: &[u8],
) -> Result<(Vec<RawTask>, Vec<u8>), E> {
    let text = format!(
        "SELECT visibility_ts, task_id, timer, data, data_encoding FROM executions WHERE {KEY_PREFIX} and visibility_ts >= ? and visibility_ts < ?"
    );
    let mut v = base(shard, &timer_id());
    v.extend([bound(ms_ts(min_nanos.div_euclid(1_000_000))), bound(ms_ts(max_nanos.div_euclid(1_000_000)))]);
    let (rows, next) = query_page(driver, text, v, page_size, token)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let (task_type, blob) = row_blob(row, Kind::Timer)?;
        out.push(RawTask {
            task_id: get_i64(cell(row, "task_id")).unwrap_or_default(),
            visibility_nanos: get_ts_nanos(cell(row, "visibility_ts")),
            task_type,
            blob,
        });
    }
    Ok((out, next))
}

fn to_task(raw: RawTask) -> Task {
    Task { visibility_timestamp_unix_nanos: raw.visibility_nanos, task_type: raw.task_type, task_id: raw.task_id, data: raw.blob }
}

fn transfer_of(raw: &RawTask) -> Result<TransferTask, E> {
    let mut t = sqlblobs::transfer_task_from_struct(&decode_struct(&raw.blob)?).map_err(internal)?;
    t.task_id = raw.task_id;
    Ok(t)
}

fn replication_of(raw: &RawTask) -> Result<ReplicationTask, E> {
    let mut t = sqlblobs::replication_task_from_struct(&decode_struct(&raw.blob)?).map_err(internal)?;
    t.task_id = raw.task_id;
    Ok(t)
}

pub(crate) fn get_transfer_tasks(driver: &CassandraBlockingSession, shard: i32, min: i64, max: i64, batch: i32) -> Result<Vec<TransferTask>, E> {
    select_immediate(driver, shard, Kind::Transfer, min, max, batch, &[])?.0.iter().map(transfer_of).collect()
}

pub(crate) fn get_replication_tasks(driver: &CassandraBlockingSession, shard: i32, min: i64, max: i64, batch: i32) -> Result<Vec<ReplicationTask>, E> {
    select_immediate(driver, shard, Kind::Replication, min, max, batch, &[])?.0.iter().map(replication_of).collect()
}

pub(crate) fn select_timer_tasks(driver: &CassandraBlockingSession, shard: i32, min: i64, max: i64, batch: i32) -> Result<Vec<TimerTask>, E> {
    select_timers(driver, shard, min, max, batch, &[])?
        .0
        .iter()
        .map(|raw| {
            let mut t = sqlblobs::timer_task_from_struct(&decode_struct(&raw.blob)?).map_err(internal)?;
            t.task_id = raw.task_id;
            t.visibility_timestamp_unix_nanos = raw.visibility_nanos;
            Ok(t)
        })
        .collect()
}

pub(crate) fn get_history_tasks(driver: &CassandraBlockingSession, range: TaskRange, category: HistoryTaskCategory) -> Result<Vec<Task>, E> {
    let page = get_history_task_page_raw(driver, range.shard_id, category, range.min_task_id, range.max_task_id, range.batch_size, &[])?;
    Ok(page.0)
}

fn get_history_task_page_raw(
    driver: &CassandraBlockingSession,
    shard: i32,
    category: HistoryTaskCategory,
    min: i64,
    max: i64,
    page_size: i32,
    token: &[u8],
) -> Result<(Vec<Task>, Vec<u8>), E> {
    let kind = kind_of(&category)?;
    let (raw, next) = if kind == Kind::Timer {
        select_timers(driver, shard, min, max, page_size, token)?
    } else {
        select_immediate(driver, shard, kind, min, max, page_size, token)?
    };
    Ok((raw.into_iter().map(to_task).collect(), next))
}

pub(crate) fn get_history_task_page(
    driver: &CassandraBlockingSession,
    q: crate::history_task_sql::HistoryTaskQuery,
) -> Result<crate::history_task_sql::HistoryTaskPage, E> {
    let kind = kind_of(&q.category)?;
    let (min, max) = if kind == Kind::Timer {
        (q.min_timestamp_unix_nanos, q.max_timestamp_unix_nanos)
    } else {
        (q.min_task_id, q.max_task_id)
    };
    let (tasks, next) = get_history_task_page_raw(driver, q.shard_id, q.category, min, max, q.page_size, &q.next_page_token)?;
    Ok(crate::history_task_sql::HistoryTaskPage { tasks, next_page_token: next })
}

// ---------------------------------------------------------------- creating task rows

fn insert_transfer(st: &mut Stmts, shard: i32, id: &Ident, t: &TransferTask, blob: &DataBlob, now: DateTime<Utc>) -> Result<(), E> {
    push_task_row(st, shard, id.row_type, "transfer", (id.domain, &id.workflow, id.run), transfer_fields(t)?, blob, DEFAULT_VISIBILITY_MS, t.task_id, now);
    Ok(())
}

fn insert_replication(st: &mut Stmts, shard: i32, id: &Ident, t: &ReplicationTask, blob: &DataBlob, now: DateTime<Utc>) -> Result<(), E> {
    push_task_row(st, shard, id.row_type, "replication", (id.domain, &id.workflow, id.run), replication_fields(t)?, blob, DEFAULT_VISIBILITY_MS, t.task_id, now);
    Ok(())
}

pub(crate) fn create_transfer_tasks(driver: &CassandraBlockingSession, shard: i32, tasks: Vec<TransferTask>) -> Result<(), E> {
    let now = Utc::now();
    let mut st = Stmts::default();
    for t in &tasks {
        let blob = encode_thrift(&sqlblobs::transfer_task_to_struct(t).map_err(internal)?)?;
        insert_transfer(&mut st, shard, &transfer_id(), t, &blob, now)?;
    }
    batch_plain(driver, st.items, false)
}

pub(crate) fn create_timer_tasks(driver: &CassandraBlockingSession, shard: i32, tasks: Vec<TimerTask>) -> Result<(), E> {
    let now = Utc::now();
    let mut st = Stmts::default();
    let id = timer_id();
    for t in &tasks {
        let blob = encode_thrift(&sqlblobs::timer_task_to_struct(t).map_err(internal)?)?;
        push_task_row(
            &mut st,
            shard,
            id.row_type,
            "timer",
            (id.domain, &id.workflow, id.run),
            timer_task_fields(t)?,
            &blob,
            t.visibility_timestamp_unix_nanos.div_euclid(1_000_000),
            t.task_id,
            now,
        );
    }
    batch_plain(driver, st.items, false)
}

pub(crate) fn create_replication_tasks(driver: &CassandraBlockingSession, shard: i32, tasks: Vec<ReplicationTask>) -> Result<(), E> {
    let now = Utc::now();
    let mut st = Stmts::default();
    for t in &tasks {
        let blob = encode_thrift(&sqlblobs::replication_task_to_struct(t).map_err(internal)?)?;
        insert_replication(&mut st, shard, &replication_id(), t, &blob, now)?;
    }
    batch_plain(driver, st.items, false)
}

pub(crate) fn create_cross_cluster_tasks(driver: &CassandraBlockingSession, shard: i32, tasks: Vec<CrossClusterTask>) -> Result<(), E> {
    let now = Utc::now();
    let mut st = Stmts::default();
    for t in &tasks {
        let blob = encode_thrift(&sqlblobs::cross_cluster_task_to_struct(t).map_err(internal)?)?;
        insert_transfer(&mut st, shard, &cross_id(&t.target_cluster), &t.transfer_task, &blob, now)?;
    }
    batch_plain(driver, st.items, false)
}

pub(crate) fn get_cross_cluster_tasks(
    driver: &CassandraBlockingSession,
    shard: i32,
    target_cluster: &str,
    min: i64,
    max: i64,
    batch: i32,
) -> Result<Vec<CrossClusterTask>, E> {
    let text = format!(
        "SELECT task_id, transfer, data, data_encoding FROM executions WHERE {KEY_PREFIX} and visibility_ts = ? and task_id >= ? and task_id < ?"
    );
    let mut v = base(shard, &cross_id(target_cluster));
    v.extend([bound(vis_ts()), bound(min), bound(max)]);
    let (rows, _) = query_page(driver, text, v, batch, &[])?;
    let mut out = Vec::new();
    for row in &rows {
        let (_, blob) = row_blob(row, Kind::Transfer)?;
        let mut t = sqlblobs::cross_cluster_task_from_struct(&decode_struct(&blob)?).map_err(internal)?;
        t.transfer_task.task_id = get_i64(cell(row, "task_id")).unwrap_or_default();
        t.target_cluster = target_cluster.to_owned();
        out.push(t);
    }
    Ok(out)
}

/// Go `CreateFailoverMarkerTasks` / replication insert: markers become
/// replication rows; with a range id the batch is conditional on the shard
/// lease. Like Go, only a shard-condition failure is reported; other errors
/// are swallowed.
pub(crate) fn create_failover_markers(driver: &CassandraBlockingSession, shard: i32, range_id: Option<i64>, markers: Vec<Task>) -> Result<(), E> {
    if markers.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    let mut st = Stmts::default();
    for marker in &markers {
        let mut task = sqlblobs::replication_task_from_struct(&decode_struct(&marker.data)?).map_err(internal)?;
        task.workflow_id = REPLICATION_WORKFLOW.into();
        task.run_id = REPLICATION_RUN.to_string();
        task.task_type = marker.task_type;
        task.first_event_id = -23;
        task.next_event_id = -23;
        task.scheduled_id = -23;
        task.branch_token.clear();
        task.new_run_branch_token.clear();
        task.creation_time_unix_nanos = marker.visibility_timestamp_unix_nanos;
        let blob = encode_thrift(&sqlblobs::replication_task_to_struct(&task).map_err(internal)?)?;
        task.task_id = marker.task_id;
        insert_replication(&mut st, shard, &replication_id(), &task, &blob, now)?;
    }
    let Some(range) = range_id else {
        return batch_plain(driver, st.items, false);
    };
    push_lease(&mut st, shard, range, now);
    match run_batch(driver, st) {
        Ok(rows) if applied(&rows) => Ok(()),
        Ok(rows) => {
            let actual = rows
                .first()
                .filter(|r| get_i32(cell(r, "type")) == ROW_TYPE_SHARD)
                .and_then(|r| get_i64(cell(r, "range_id")))
                .filter(|a| *a != range)
                .unwrap_or(-1);
            Err(E::ShardOwnershipLost { shard_id: shard, expected_range_id: range, actual_range_id: actual })
        }
        Err(_) => Ok(()),
    }
}

// ---------------------------------------------------------------- completing task rows

fn delete_key_text() -> String {
    format!("DELETE FROM executions WHERE {KEY_PREFIX} and visibility_ts = ? and task_id = ?")
}

fn delete_one(driver: &CassandraBlockingSession, shard: i32, id: &Ident, task_id: i64) -> Result<(), E> {
    let mut v = base(shard, id);
    v.extend([bound(vis_ts()), bound(task_id)]);
    exec_all(driver, delete_key_text(), v)
}

fn delete_range(driver: &CassandraBlockingSession, shard: i32, id: &Ident, min: i64, max: i64) -> Result<(), E> {
    let text = format!("DELETE FROM executions WHERE {KEY_PREFIX} and visibility_ts = ? and task_id >= ? and task_id < ?");
    let mut v = base(shard, id);
    v.extend([bound(vis_ts()), bound(min), bound(max)]);
    exec_all(driver, text, v)
}

fn delete_keys(driver: &CassandraBlockingSession, shard: i32, id: &Ident, keys: &[(i64, Option<i64>)]) -> Result<(), E> {
    for chunk in keys.chunks(DELETE_CHUNK) {
        let items = chunk
            .iter()
            .map(|(task_id, ts_nanos)| {
                let mut v = base(shard, id);
                let ts = ts_nanos.map(|n| ms_ts(n.div_euclid(1_000_000))).unwrap_or_else(vis_ts);
                v.extend([bound(ts), bound(*task_id)]);
                (delete_key_text(), v)
            })
            .collect();
        batch_plain(driver, items, true)?;
    }
    Ok(())
}

pub(crate) fn complete_transfer_task(driver: &CassandraBlockingSession, shard: i32, task_id: i64) -> Result<(), E> {
    delete_one(driver, shard, &transfer_id(), task_id)
}

pub(crate) fn complete_replication_task(driver: &CassandraBlockingSession, shard: i32, task_id: i64) -> Result<(), E> {
    delete_one(driver, shard, &replication_id(), task_id)
}

pub(crate) fn complete_cross_cluster_task(driver: &CassandraBlockingSession, shard: i32, target_cluster: &str, task_id: i64) -> Result<(), E> {
    delete_one(driver, shard, &cross_id(target_cluster), task_id)
}

pub(crate) fn complete_timer_task(driver: &CassandraBlockingSession, shard: i32, visibility_nanos: i64, task_id: i64) -> Result<(), E> {
    let mut v = base(shard, &timer_id());
    v.extend([bound(ms_ts(visibility_nanos.div_euclid(1_000_000))), bound(task_id)]);
    exec_all(driver, delete_key_text(), v)
}

pub(crate) fn complete_history_task(driver: &CassandraBlockingSession, shard: i32, category: HistoryTaskCategory, task_id: i64) -> Result<(), E> {
    // The scheduled time is unknown for a single-id request; timers use the batch form.
    match kind_of(&category)? {
        Kind::Transfer => complete_transfer_task(driver, shard, task_id),
        Kind::Replication => complete_replication_task(driver, shard, task_id),
        Kind::Timer => delete_keys(driver, shard, &timer_id(), &[(task_id, Some(0))]),
    }
}

pub(crate) fn complete_history_task_batch(driver: &CassandraBlockingSession, shard: i32, category: HistoryTaskCategory, keys: Vec<(i64, i64)>) -> Result<(), E> {
    let kind = kind_of(&category)?;
    let id = match kind {
        Kind::Transfer => transfer_id(),
        Kind::Replication => replication_id(),
        Kind::Timer => timer_id(),
    };
    let keys: Vec<(i64, Option<i64>)> = keys.into_iter().map(|(task_id, time)| (task_id, (kind == Kind::Timer).then_some(time))).collect();
    delete_keys(driver, shard, &id, &keys)
}

fn range_complete(
    driver: &CassandraBlockingSession,
    shard: i32,
    category: HistoryTaskCategory,
    min_task: i64,
    max_task: i64,
    min_nanos: i64,
    max_nanos: i64,
) -> Result<u64, E> {
    match kind_of(&category)? {
        Kind::Transfer => delete_range(driver, shard, &transfer_id(), min_task, max_task)?,
        Kind::Replication => {
            let text = format!("DELETE FROM executions WHERE {KEY_PREFIX} and visibility_ts = ? and task_id < ?");
            let mut v = base(shard, &replication_id());
            v.extend([bound(vis_ts()), bound(max_task)]);
            exec_all(driver, text, v)?;
        }
        Kind::Timer => {
            let text = format!("DELETE FROM executions WHERE {KEY_PREFIX} and visibility_ts >= ? and visibility_ts < ?");
            let mut v = base(shard, &timer_id());
            v.extend([bound(ms_ts(min_nanos.div_euclid(1_000_000))), bound(ms_ts(max_nanos.div_euclid(1_000_000)))]);
            exec_all(driver, text, v)?;
        }
    }
    // Go reports UnknownNumRowsAffected.
    Ok(0)
}

pub(crate) fn range_complete_history_tasks(driver: &CassandraBlockingSession, r: TaskRange, category: HistoryTaskCategory) -> Result<u64, E> {
    range_complete(driver, r.shard_id, category, r.min_task_id, r.max_task_id, r.min_task_id, r.max_task_id)
}

pub(crate) fn range_complete_history_task_page(driver: &CassandraBlockingSession, q: crate::history_task_sql::HistoryTaskQuery) -> Result<u64, E> {
    range_complete(driver, q.shard_id, q.category, q.min_task_id, q.max_task_id, q.min_timestamp_unix_nanos, q.max_timestamp_unix_nanos)
}

// ---------------------------------------------------------------- DLQ

pub(crate) fn put_replication_task_to_dlq(driver: &CassandraBlockingSession, shard: i32, source_cluster: &str, task: ReplicationTask) -> Result<(), E> {
    let blob = encode_thrift(&sqlblobs::replication_task_to_struct(&task).map_err(internal)?)?;
    let mut st = Stmts::default();
    insert_replication(&mut st, shard, &dlq_id(source_cluster), &task, &blob, Utc::now())?;
    batch_plain(driver, st.items, false)
}

pub(crate) fn get_replication_dlq_size(driver: &CassandraBlockingSession, shard: i32, source_cluster: &str) -> Result<i64, E> {
    let text = format!("SELECT count(1) as count FROM executions WHERE {KEY_PREFIX}");
    let rows = query_all(driver, text, base(shard, &dlq_id(source_cluster)))?;
    rows.first().and_then(|r| get_i64(cell(r, "count"))).ok_or_else(|| internal("DLQ count returned no row"))
}

pub(crate) fn delete_replication_task_from_dlq(driver: &CassandraBlockingSession, shard: i32, source_cluster: &str, task_id: i64) -> Result<(), E> {
    delete_one(driver, shard, &dlq_id(source_cluster), task_id)
}

pub(crate) fn range_delete_replication_dlq(
    driver: &CassandraBlockingSession,
    shard: i32,
    source_cluster: &str,
    inclusive_min: i64,
    exclusive_max: i64,
) -> Result<u64, E> {
    delete_range(driver, shard, &dlq_id(source_cluster), inclusive_min, exclusive_max)?;
    Ok(0)
}

pub(crate) fn get_replication_dlq_page(
    driver: &CassandraBlockingSession,
    q: crate::history_task_sql::ReplicationDlqQuery,
) -> Result<crate::history_task_sql::ReplicationDlqPage, E> {
    if q.read_level > q.max_read_level {
        return Err(E::InvalidArgument("ReadLevel cannot be higher than MaxReadLevel".into()));
    }
    let text = format!(
        "SELECT task_id, replication, data, data_encoding FROM executions WHERE {KEY_PREFIX} and visibility_ts = ? and task_id >= ? and task_id < ?"
    );
    let mut v = base(q.shard_id, &dlq_id(&q.source_cluster));
    v.extend([bound(vis_ts()), bound(q.read_level), bound(q.max_read_level)]);
    let (rows, next) = query_page(driver, text, v, q.page_size, &q.next_page_token)?;
    let tasks = rows
        .iter()
        .map(|row| {
            let mut t = replication_from_udt(&udt_map(cell(row, "replication")));
            t.task_id = get_i64(cell(row, "task_id")).unwrap_or_default();
            t
        })
        .collect();
    Ok(crate::history_task_sql::ReplicationDlqPage { tasks, next_page_token: next })
}

pub(crate) fn get_replication_tasks_from_dlq(
    driver: &CassandraBlockingSession,
    shard: i32,
    source_cluster: &str,
    min: i64,
    max: i64,
    batch: i32,
) -> Result<Vec<ReplicationTask>, E> {
    Ok(get_replication_dlq_page(
        driver,
        crate::history_task_sql::ReplicationDlqQuery {
            shard_id: shard,
            source_cluster: source_cluster.to_owned(),
            read_level: min,
            max_read_level: max,
            page_size: batch,
            next_page_token: Vec::new(),
        },
    )?
    .tasks)
}

// ---------------------------------------------------------------- policy, timers, listing

pub(crate) fn get_active_cluster_selection_policy(
    driver: &CassandraBlockingSession,
    shard: i32,
    domain: &str,
    workflow: &str,
    run: &str,
) -> Result<Option<DataBlob>, E> {
    let text = format!("SELECT data, data_encoding FROM executions WHERE {KEY_PREFIX} and visibility_ts = ? and task_id = ?");
    let v: BoundValues = vec![
        bound(shard),
        bound(ROW_TYPE_POLICY),
        bound(parse_uuid(domain)?),
        bound(workflow.to_owned()),
        bound(parse_uuid(run)?),
        bound(vis_ts()),
        bound(POLICY_TASK_ID),
    ];
    let rows = query_all(driver, text, v)?;
    Ok(rows.first().map(|r| DataBlob { data: get_blob(cell(r, "data")), encoding: get_str(cell(r, "data_encoding")) }))
}

pub(crate) fn delete_active_cluster_selection_policy(
    driver: &CassandraBlockingSession,
    shard: i32,
    domain: &str,
    workflow: &str,
    run: &str,
) -> Result<(), E> {
    let v: BoundValues = vec![
        bound(shard),
        bound(ROW_TYPE_POLICY),
        bound(parse_uuid(domain)?),
        bound(workflow.to_owned()),
        bound(parse_uuid(run)?),
        bound(vis_ts()),
        bound(POLICY_TASK_ID),
    ];
    exec_all(driver, delete_key_text(), v)
}

pub(crate) fn list_current_executions(driver: &CassandraBlockingSession, f: ExecutionListFilter) -> Result<ExecutionPage, E> {
    let text = "SELECT domain_id, workflow_id, run_id, current_run_id, workflow_state FROM executions WHERE shard_id = ? and type = ?".to_owned();
    let v: BoundValues = vec![bound(f.shard_id), bound(ROW_TYPE_EXECUTION)];
    let (rows, next) = query_page(driver, text, v, f.page_size, &f.next_page_token)?;
    let permanent = PERMANENT_RUN_ID.to_string();
    let mut executions = Vec::new();
    for row in &rows {
        if get_str(cell(row, "run_id")) != permanent {
            continue;
        }
        executions.push(WorkflowSnapshot {
            execution_info: WorkflowExecutionInfo {
                domain_id: get_str(cell(row, "domain_id")),
                workflow_id: get_str(cell(row, "workflow_id")),
                run_id: get_str(cell(row, "current_run_id")),
                state: get_i32(cell(row, "workflow_state")),
                ..Default::default()
            },
            ..Default::default()
        });
    }
    Ok(ExecutionPage { executions, next_page_token: next })
}

pub(crate) fn list_concrete_executions(driver: &CassandraBlockingSession, f: ExecutionListFilter) -> Result<ExecutionPage, E> {
    let text = "SELECT run_id, execution, version_histories, version_histories_encoding FROM executions WHERE shard_id = ? and type = ?".to_owned();
    let v: BoundValues = vec![bound(f.shard_id), bound(ROW_TYPE_EXECUTION)];
    let (rows, next) = query_page(driver, text, v, f.page_size, &f.next_page_token)?;
    let permanent = PERMANENT_RUN_ID.to_string();
    let mut executions = Vec::new();
    for row in &rows {
        if get_str(cell(row, "run_id")) == permanent {
            continue;
        }
        let execution = cell(row, "execution");
        if !matches!(execution, Some(CqlValue::UserDefinedType { .. })) {
            return Err(internal("invalid execution blob format: missing or invalid 'execution' field"));
        }
        let info = parse_execution_info(&udt_map(execution));
        executions.push(WorkflowSnapshot {
            execution_info: info,
            version_histories: Some(DataBlob {
                data: get_blob(cell(row, "version_histories")),
                encoding: get_str(cell(row, "version_histories_encoding")),
            }),
            ..Default::default()
        });
    }
    Ok(ExecutionPage { executions, next_page_token: next })
}
