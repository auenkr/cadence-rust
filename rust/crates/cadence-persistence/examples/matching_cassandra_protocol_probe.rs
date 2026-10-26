//! Exercise Matching's Cassandra queue CQL through the public persistence
//! session configuration and Scylla Session APIs against a real Cassandra.
//!
//! `CASSANDRA_PROTO_VERSION` is mandatory. Protocol negotiation/configuration
//! errors are intentionally fatal; this probe never silently falls back.
use anyhow::{bail, Context, Result};
use cadence_persistence::cassandra::{CassandraSessionConfig, CassandraSessionOptions};
use scylla::statement::batch::{Batch, BatchType};
use std::{env, time::Duration};

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let protocol: i32 = required("CASSANDRA_PROTO_VERSION")?.parse()?;
    if !matches!(protocol, 2 | 3) {
        bail!("probe only accepts explicitly selected protocol 2 or 3, got {protocol}");
    }
    let host = required("CASSANDRA_HOST")?;
    let keyspace = required("CASSANDRA_KEYSPACE")?;
    let expect_range_rejection = required("CASSANDRA_EXPECT_RANGE_DELETE_REJECTION")?;
    let expect_range_rejection = match expect_range_rejection.as_str() {
        "true" => true,
        "false" => false,
        other => bail!("CASSANDRA_EXPECT_RANGE_DELETE_REJECTION must be true/false, got {other}"),
    };

    let config = CassandraSessionConfig::from_parts(
        [host], 9042, "", "", keyspace,
    )?
    .with_go_options(CassandraSessionOptions {
        timeout_nanos: Duration::from_secs(5).as_nanos() as i64,
        connect_timeout_nanos: Duration::from_secs(10).as_nanos() as i64,
        proto_version: protocol,
        ..Default::default()
    })?;
    let session = config.connect().await.context("connect with requested CQL protocol")?;

    println!("RUN schema-create");
    session.query_unpaged(
        "CREATE TABLE IF NOT EXISTS matching_protocol_oracle (id bigint PRIMARY KEY, payload blob)",
        (),
    ).await.context("schema create result")?;

    const QUEUE_TYPE: i32 = 91;
    println!("RUN queue-cas-enqueue");
    let cas = session.prepare(
        "INSERT INTO queue (queue_type, message_id, message_payload, created_time) VALUES (?, ?, ?, ?) IF NOT EXISTS",
    ).await.context("prepare CAS enqueue")?;
    let cas_result = session.execute_unpaged(
        &cas,
        (QUEUE_TYPE, 1001_i64, b"protocol-oracle".to_vec(), chrono::Utc::now()),
    ).await.context("execute CAS enqueue")?;
    let cas_rows = cas_result.into_rows_result().context("CAS result metadata")?;
    let cas_row = cas_rows.maybe_first_row::<(bool,)>()?.context("CAS returned no [applied] row")?;
    if !cas_row.0 { bail!("CAS enqueue reported not applied for unique probe row"); }

    println!("RUN queue-range-select");
    let select = session.prepare(
        "SELECT message_id, message_payload FROM queue WHERE queue_type = ? AND message_id > ? LIMIT ?",
    ).await.context("prepare queue range SELECT")?;
    let result = session.execute_unpaged(&select, (QUEUE_TYPE, 1000_i64, 10_i32)).await
        .context("execute queue range SELECT")?;
    let rows = result.into_rows_result().context("queue SELECT result metadata")?;
    let mut found = false;
    for row in rows.rows::<(i64, Vec<u8>)>()? {
        let (id, payload) = row?;
        if id == 1001 && payload == b"protocol-oracle" { found = true; }
    }
    if !found { bail!("queue range SELECT did not return the inserted id/payload"); }

    println!("RUN queue-count");
    let count_stmt = session.prepare("SELECT COUNT(1) FROM queue WHERE queue_type = ?").await
        .context("prepare queue count")?;
    let count_result = session.execute_unpaged(&count_stmt, (QUEUE_TYPE,)).await
        .context("execute queue count")?;
    let count_rows = count_result.into_rows_result().context("count result metadata")?;
    let count = count_rows.maybe_first_row::<(i64,)>()?.context("count returned no row")?.0;
    if count != 1 { bail!("queue count expected the single CAS row, got {count}"); }

    println!("RUN queue-batch");
    let mut batch = Batch::new(BatchType::Logged);
    batch.append_statement(
        "INSERT INTO queue (queue_type, message_id, message_payload, created_time) VALUES (?, ?, ?, ?)",
    );
    session.batch(
        &batch,
        ((QUEUE_TYPE, 1002_i64, b"batch".to_vec(), chrono::Utc::now()),),
    ).await.context("execute logged queue batch")?;

    println!("RUN queue-range-delete");
    let range_delete = session.query_unpaged(
        "DELETE FROM queue WHERE queue_type = ? AND message_id > ? AND message_id <= ?",
        (QUEUE_TYPE, 1001_i64, 1002_i64),
    ).await;
    match (expect_range_rejection, range_delete) {
        (true, Err(error)) if error.to_string().contains("Invalid operator") => {
            println!("EXPECTED_SERVER_REJECTION scope=queue-range-delete class=InvalidOperator error={error}");
        }
        (true, Err(error)) => bail!("queue range DELETE failed outside expected Invalid operator scope: {error:#}"),
        (true, Ok(_)) => bail!("server unexpectedly accepted strict range DELETE"),
        (false, Err(error)) => return Err(error).context("queue range DELETE expected to succeed"),
        (false, Ok(_)) => {}
    }

    println!("RUN queue-point-delete");
    session.query_unpaged(
        "DELETE FROM queue WHERE queue_type = ? AND message_id = ?",
        (QUEUE_TYPE, 1001_i64),
    ).await.context("point-delete CAS row")?;
    if expect_range_rejection {
        session.query_unpaged(
            "DELETE FROM queue WHERE queue_type = ? AND message_id = ?",
            (QUEUE_TYPE, 1002_i64),
        ).await.context("point-delete batch row after expected range-delete rejection")?;
    }

    println!("RUN server-error");
    let server_error = session.query_unpaged(
        "SELECT definitely_missing FROM queue WHERE queue_type = ?",
        (QUEUE_TYPE,),
    ).await;
    match server_error {
        Err(error) if error.to_string().contains("undefined") || error.to_string().contains("Undefined") => {
            println!("EXPECTED_SERVER_ERROR scope=missing-column class=UndefinedColumn error={error}");
        }
        Err(error) => bail!("missing-column query failed outside expected server error scope: {error:#}"),
        Ok(_) => bail!("missing-column query unexpectedly succeeded"),
    }

    println!("RUN schema-drop");
    session.query_unpaged("DROP TABLE matching_protocol_oracle", ()).await
        .context("schema drop result")?;
    println!("PASS rust-cadence-persistence protocol={protocol} operations=schema-change,cas,prepare,query,result,batch,range-delete,point-delete,server-error");
    Ok(())
}
