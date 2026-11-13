//! Shard-locked execution transactions, following Go's SQL write ordering.
#![allow(clippy::too_many_arguments)]
use crate::{
    history_contract::*,
    history_sql_backend::{
        internal, timestamp, uuid, uuid_string, Column, SqlBackend, SqlTx, Value,
    },
    history_sql_queries::{
        workflow_state_map_statement, Statement, WorkflowMapOperation, WorkflowStateMap,
    },
    history_task_sql::SqlHistoryTasks,
    sql_execution_store::{
        decode_execution_info, snapshot_to_execution_row, validate_create_transition,
    },
};

#[derive(Clone)]
pub struct SqlWorkflowStore {
    pub backend: SqlBackend,
    pub encoding: crate::sqlblobs::Encoding,
    pub shard_id: i32,
    pub range_id: i64,
    pub target_cluster: String,
}
fn key(shard: i32, info: &WorkflowExecutionInfo) -> Result<Vec<Value>, PersistenceError> {
    Ok(vec![
        Value::I32(shard),
        uuid(&info.domain_id)?,
        Value::Text(info.workflow_id.clone()),
        uuid(&info.run_id)?,
    ])
}
impl SqlWorkflowStore {
    pub fn lock_shard(
        &self,
        tx: &mut dyn SqlTx,
        shard: i32,
        range: i64,
    ) -> Result<(), PersistenceError> {
        let sql = match self.backend.dialect() {
            crate::history_sql_queries::Dialect::MySql => {
                "SELECT range_id FROM shards WHERE shard_id=? LOCK IN SHARE MODE"
            }
            crate::history_sql_queries::Dialect::Postgres => {
                "SELECT range_id FROM shards WHERE shard_id=$1 FOR SHARE"
            }
        };
        let rows = tx.query(sql, &[Value::I32(shard)], &[Column::I64])?;
        let found = rows.first().ok_or_else(|| {
            internal(format!(
                "Failed to lock shard with ID {shard} that does not exist."
            ))
        })?[0]
            .integer()?;
        if found != range {
            return Err(PersistenceError::ShardOwnershipLost {
                shard_id: shard,
                expected_range_id: range,
                actual_range_id: found,
            });
        }
        Ok(())
    }
    fn current_tx(
        &self,
        tx: &mut dyn SqlTx,
        k: &[Value],
        lock: bool,
    ) -> Result<Option<CurrentExecution>, PersistenceError> {
        let statement = if lock {
            Statement::LockCurrentExecution
        } else {
            Statement::GetCurrentExecution
        };
        let columns = if lock {
            vec![
                Column::I32,
                Column::Bytes,
                Column::Text,
                Column::Bytes,
                Column::Text,
                Column::I32,
                Column::I32,
                Column::I64,
                Column::I64,
            ]
        } else {
            vec![
                Column::Bytes,
                Column::Text,
                Column::I32,
                Column::I32,
                Column::I64,
                Column::I64,
            ]
        };
        let rows = tx.query(
            self.backend.dialect().statement(statement),
            &k[..3],
            &columns,
        )?;
        rows.first()
            .map(|row| {
                let row = if lock { &row[3..] } else { row.as_slice() };
                Ok(CurrentExecution {
                    run_id: uuid_string(&row[0])?,
                    create_request_id: row[1].text()?,
                    state: row[2].integer()? as i32,
                    close_status: row[3].integer()? as i32,
                    start_version: row[4].integer()?,
                    last_write_version: row[5].integer()?,
                })
            })
            .transpose()
    }
    fn current_write(
        &self,
        tx: &mut dyn SqlTx,
        shard: i32,
        info: &WorkflowExecutionInfo,
        start: i64,
        last: i64,
        insert: bool,
    ) -> Result<(), PersistenceError> {
        let k = key(shard, info)?;
        let mut params = vec![
            k[3].clone(),
            Value::Text(info.create_request_id.clone()),
            Value::I32(info.state),
            Value::I32(info.close_status),
            Value::I64(start),
            Value::I64(last),
        ];
        let statement = if insert {
            let mut prefix = k[..3].to_vec();
            prefix.extend(params);
            params = prefix;
            Statement::InsertCurrentExecution
        } else {
            params.extend_from_slice(&k[..3]);
            Statement::UpdateCurrentExecution
        };
        let count = tx.execute(self.backend.dialect().statement(statement), &params)?;
        if count != 1 {
            return Err(internal(format!(
                "current_executions affected {count} instead of 1 rows"
            )));
        }
        Ok(())
    }
    fn assert_current(
        &self,
        tx: &mut dyn SqlTx,
        shard: i32,
        info: &WorkflowExecutionInfo,
        previous: &str,
        bypass: bool,
    ) -> Result<(), PersistenceError> {
        let current = self.current_tx(tx, &key(shard, info)?, true)?;
        if bypass {
            if current.as_ref().is_some_and(|v| v.run_id == previous) {
                return Err(PersistenceError::ConditionFailed(format!(
                    "assertRunIDMismatch failed. Current run ID was {previous}, input {previous}"
                )));
            }
            return Ok(());
        }
        let current = current.ok_or_else(|| {
            PersistenceError::EntityNotExists("Unable to load current record.".into())
        })?;
        if current.run_id != previous {
            return Err(PersistenceError::ConditionFailed(format!("assertRunIDAndUpdateCurrentExecution failed. Current run ID was {}, expected {previous}",current.run_id)));
        }
        Ok(())
    }
    fn lock_condition(
        &self,
        tx: &mut dyn SqlTx,
        k: &[Value],
        condition: i64,
    ) -> Result<(), PersistenceError> {
        let rows = tx.query(
            self.backend.dialect().statement(Statement::LockExecution),
            k,
            &[Column::I64],
        )?;
        let found = rows
            .first()
            .ok_or_else(|| PersistenceError::EntityNotExists("execution not found".into()))?[0]
            .integer()?;
        if found != condition {
            return Err(PersistenceError::ConditionFailed(format!(
                "NextEventID condition failed. Expected: {condition}, Actual: {found}"
            )));
        }
        Ok(())
    }
    fn execution_write(
        &self,
        tx: &mut dyn SqlTx,
        shard: i32,
        snapshot: &WorkflowSnapshot,
        insert: bool,
    ) -> Result<(), PersistenceError> {
        let info = &snapshot.execution_info;
        validate_state_close(info.state, info.close_status, insert)?;
        let mut snapshot = snapshot.clone();
        if insert && snapshot.execution_info.start_time_unix_nanos == 0 {
            snapshot.execution_info.start_time_unix_nanos = chrono::Utc::now()
                .timestamp_nanos_opt()
                .ok_or_else(|| internal("creation timestamp out of range"))?;
        }
        let row = snapshot_to_execution_row(&snapshot, shard, self.encoding)?;
        let k = key(shard, info)?;
        let mut values = vec![
            Value::I64(row.next_event_id),
            Value::I64(row.last_write_version),
            Value::Bytes(row.data),
            Value::Text(row.data_encoding),
        ];
        let statement = if insert {
            let mut prefix = k;
            prefix.extend(values);
            values = prefix;
            Statement::InsertExecution
        } else {
            values.extend(k);
            Statement::UpdateExecution
        };
        let result = tx.execute(self.backend.dialect().statement(statement), &values);
        let count = match result {
            Err(PersistenceError::ConditionFailed(_)) if insert => {
                return Err(PersistenceError::WorkflowAlreadyStarted {
                    message: format!(
                        "Workflow execution already running. WorkflowId: {}",
                        info.workflow_id
                    ),
                    start_request_id: info.create_request_id.clone(),
                    run_id: info.run_id.clone(),
                    state: info.state,
                    close_status: info.close_status,
                    last_write_version: snapshot.last_write_version,
                })
            }
            other => other?,
        };
        if count != 1 {
            return Err(internal(format!(
                "execution write affected {count} instead of 1 rows"
            )));
        }
        Ok(())
    }
    fn maps(
        &self,
        tx: &mut dyn SqlTx,
        k: &[Value],
        map: WorkflowStateMap,
        values: Vec<(Value, DataBlob)>,
        delete: Vec<Value>,
        reset: bool,
        heartbeats: &std::collections::BTreeMap<i64, ActivityHeartbeat>,
    ) -> Result<(), PersistenceError> {
        let dialect = self.backend.dialect();
        if reset {
            tx.execute(
                &workflow_state_map_statement(dialect, map, WorkflowMapOperation::DeleteAll, 0),
                k,
            )?;
        }
        for (id, blob) in values {
            let heartbeat = if map == WorkflowStateMap::Activity {
                heartbeats.get(&id.integer()?)
            } else {
                None
            };
            let mut params = k.to_vec();
            params.extend([id, Value::Bytes(blob.data), Value::Text(blob.encoding)]);
            if map == WorkflowStateMap::Activity {
                params.extend([
                    Value::Bytes(heartbeat.map_or_else(Vec::new, |h| h.details.clone())),
                    timestamp(heartbeat.map_or(0, |h| h.updated_time_unix_nanos))?,
                ]);
            }
            tx.execute(
                &workflow_state_map_statement(dialect, map, WorkflowMapOperation::Upsert, 0),
                &params,
            )?;
        }
        if !delete.is_empty() {
            let query = workflow_state_map_statement(
                dialect,
                map,
                WorkflowMapOperation::DeleteKeys,
                delete.len(),
            );
            let mut params = k.to_vec();
            params.extend(delete);
            tx.execute(&query, &params)?;
        }
        Ok(())
    }
    fn signals(
        &self,
        tx: &mut dyn SqlTx,
        k: &[Value],
        insert: &[String],
        delete: &[String],
        reset: bool,
    ) -> Result<(), PersistenceError> {
        let dialect = self.backend.dialect();
        if reset {
            tx.execute(dialect.statement(Statement::DeleteSignalRequested), k)?;
        }
        for id in insert {
            let mut params = k.to_vec();
            params.push(Value::Text(id.clone()));
            tx.execute(dialect.statement(Statement::InsertSignalRequested), &params)?;
        }
        if !delete.is_empty() {
            let query = crate::history_sql_queries::delete_signal_requested_keys_statement(
                dialect,
                delete.len(),
            );
            let mut params = k.to_vec();
            params.extend(delete.iter().cloned().map(Value::Text));
            tx.execute(&query, &params)?;
        }
        Ok(())
    }
    fn tasks(
        &self,
        tx: &mut dyn SqlTx,
        shard: i32,
        tasks: &std::collections::BTreeMap<HistoryTaskCategory, Vec<Task>>,
    ) -> Result<(), PersistenceError> {
        for (category, tasks) in tasks {
            SqlHistoryTasks::insert_tx(tx, self.backend.dialect(), shard, category, tasks)?;
        }
        Ok(())
    }
    pub fn apply_snapshot_tx(
        &self,
        tx: &mut dyn SqlTx,
        shard: i32,
        snapshot: &WorkflowSnapshot,
        reset: bool,
    ) -> Result<(), PersistenceError> {
        let k = key(shard, &snapshot.execution_info)?;
        if reset {
            self.lock_condition(tx, &k, snapshot.condition)?;
        } else if let Some(policy) = &snapshot.execution_info.active_cluster_selection_policy {
            let mut params = k.clone();
            params.extend([
                Value::Bytes(policy.data.clone()),
                Value::Text(policy.encoding.clone()),
            ]);
            tx.execute(&self.query("INSERT INTO active_cluster_selection_policy(shard_id,domain_id,workflow_id,run_id,data,data_encoding) VALUES(?,?,?,?,?,?)"),&params)?;
        }
        self.execution_write(tx, shard, snapshot, !reset)?;
        self.tasks(tx, shard, &snapshot.tasks_by_category)?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::Activity,
            ints(&snapshot.activity_infos),
            vec![],
            reset,
            &snapshot.activity_heartbeats,
        )?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::Timer,
            strings(&snapshot.timer_infos),
            vec![],
            reset,
            &snapshot.activity_heartbeats,
        )?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::ChildExecution,
            ints(&snapshot.child_execution_infos),
            vec![],
            reset,
            &snapshot.activity_heartbeats,
        )?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::RequestCancel,
            ints(&snapshot.request_cancel_infos),
            vec![],
            reset,
            &snapshot.activity_heartbeats,
        )?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::Signal,
            ints(&snapshot.signal_infos),
            vec![],
            reset,
            &snapshot.activity_heartbeats,
        )?;
        self.signals(tx, &k, &snapshot.signal_requested_ids, &[], reset)?;
        if reset {
            tx.execute(
                self.backend
                    .dialect()
                    .statement(Statement::DeleteBufferedEvents),
                &k,
            )?;
        }
        Ok(())
    }
    pub fn apply_mutation_tx(
        &self,
        tx: &mut dyn SqlTx,
        shard: i32,
        m: &WorkflowMutation,
    ) -> Result<(), PersistenceError> {
        let k = key(shard, &m.execution_info)?;
        self.lock_condition(tx, &k, m.condition)?;
        self.execution_write(
            tx,
            shard,
            &WorkflowSnapshot {
                execution_info: m.execution_info.clone(),
                start_version: m.start_version,
                last_write_version: m.last_write_version,
                version_histories: m.version_histories.clone(),
                checksum: m.checksum.clone(),
                ..Default::default()
            },
            false,
        )?;
        self.tasks(tx, shard, &m.tasks_by_category)?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::Activity,
            ints(&m.upsert_activity_infos),
            m.delete_activity_infos
                .iter()
                .copied()
                .map(Value::I64)
                .collect(),
            false,
            &m.activity_heartbeats,
        )?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::Timer,
            strings(&m.upsert_timer_infos),
            m.delete_timer_infos
                .iter()
                .cloned()
                .map(Value::Text)
                .collect(),
            false,
            &m.activity_heartbeats,
        )?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::ChildExecution,
            ints(&m.upsert_child_execution_infos),
            m.delete_child_execution_infos
                .iter()
                .copied()
                .map(Value::I64)
                .collect(),
            false,
            &m.activity_heartbeats,
        )?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::RequestCancel,
            ints(&m.upsert_request_cancel_infos),
            m.delete_request_cancel_infos
                .iter()
                .copied()
                .map(Value::I64)
                .collect(),
            false,
            &m.activity_heartbeats,
        )?;
        self.maps(
            tx,
            &k,
            WorkflowStateMap::Signal,
            ints(&m.upsert_signal_infos),
            m.delete_signal_infos
                .iter()
                .copied()
                .map(Value::I64)
                .collect(),
            false,
            &m.activity_heartbeats,
        )?;
        self.signals(
            tx,
            &k,
            &m.upsert_signal_requested_ids,
            &m.delete_signal_requested_ids,
            false,
        )?;
        if m.clear_buffered_events {
            tx.execute(
                self.backend
                    .dialect()
                    .statement(Statement::DeleteBufferedEvents),
                &k,
            )?;
        }
        if let Some(blob) = &m.new_buffered_events {
            let mut params = k;
            params.extend([
                Value::Bytes(blob.data.clone()),
                Value::Text(blob.encoding.clone()),
            ]);
            tx.execute(
                self.backend
                    .dialect()
                    .statement(Statement::InsertBufferedEvent),
                &params,
            )?;
        }
        Ok(())
    }
    pub fn update(&self, request: &UpdateWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        validate_update(request)?;
        let m = &request.update_workflow_mutation;
        self.backend.transaction(|tx| {
            self.lock_shard(tx, request.shard_id, request.range_id)?;
            match request.mode {
                UpdateWorkflowMode::IgnoreCurrent => {}
                UpdateWorkflowMode::BypassCurrent => self.assert_current(
                    tx,
                    request.shard_id,
                    &m.execution_info,
                    &m.execution_info.run_id,
                    true,
                )?,
                UpdateWorkflowMode::UpdateCurrent => {
                    self.assert_current(
                        tx,
                        request.shard_id,
                        &m.execution_info,
                        &m.execution_info.run_id,
                        false,
                    )?;
                    if let Some(new) = &request.new_workflow_snapshot {
                        if new.execution_info.domain_id != m.execution_info.domain_id {
                            return Err(internal(
                                "UpdateWorkflowExecution: cannot continue as new to another domain",
                            ));
                        }
                        self.current_write(
                            tx,
                            request.shard_id,
                            &new.execution_info,
                            new.start_version,
                            new.last_write_version,
                            false,
                        )?;
                    } else {
                        self.current_write(
                            tx,
                            request.shard_id,
                            &m.execution_info,
                            m.start_version,
                            m.last_write_version,
                            false,
                        )?;
                    }
                }
            }
            self.apply_mutation_tx(tx, request.shard_id, m)?;
            if let Some(new) = &request.new_workflow_snapshot {
                self.apply_snapshot_tx(tx, request.shard_id, new, false)?;
            }
            Ok(())
        })
    }
    pub fn conflict_resolve(
        &self,
        request: &ConflictResolveWorkflowExecutionRequest,
    ) -> Result<(), PersistenceError> {
        validate_conflict(request)?;
        let reset = &request.reset_workflow_snapshot;
        self.backend.transaction(|tx| {
            self.lock_shard(tx, request.shard_id, request.range_id)?;
            match request.mode {
                ConflictResolveWorkflowMode::BypassCurrent => self.assert_current(
                    tx,
                    request.shard_id,
                    &reset.execution_info,
                    &reset.execution_info.run_id,
                    true,
                )?,
                ConflictResolveWorkflowMode::UpdateCurrent => {
                    let previous = request
                        .current_workflow_mutation
                        .as_ref()
                        .map_or(&reset.execution_info.run_id, |m| &m.execution_info.run_id);
                    self.assert_current(
                        tx,
                        request.shard_id,
                        &reset.execution_info,
                        previous,
                        false,
                    )?;
                    let target = request.new_workflow_snapshot.as_ref().unwrap_or(reset);
                    self.current_write(
                        tx,
                        request.shard_id,
                        &target.execution_info,
                        target.start_version,
                        target.last_write_version,
                        false,
                    )?;
                }
            }
            self.apply_snapshot_tx(tx, request.shard_id, reset, true)?;
            if let Some(current) = &request.current_workflow_mutation {
                self.apply_mutation_tx(tx, request.shard_id, current)?;
            }
            if let Some(new) = &request.new_workflow_snapshot {
                self.apply_snapshot_tx(tx, request.shard_id, new, false)?;
            }
            Ok(())
        })
    }
    pub fn create(
        &self,
        request: &CreateWorkflowExecutionRequest,
    ) -> Result<CurrentExecution, PersistenceError> {
        let snapshot = &request.new_workflow_snapshot;
        let info = &snapshot.execution_info;
        check_state(info.state)?;
        let invalid = match request.mode {
            CreateWorkflowMode::Zombie => matches!(info.state, 0..=2),
            _ => matches!(info.state, 2 | 3),
        };
        if invalid {
            return Err(internal(format!(
                "Invalid workflow create mode {}, state: {}",
                request.mode as i32, info.state
            )));
        }
        self.backend.transaction(|tx|{self.lock_shard(tx,request.shard_id,request.range_id)?;let k=key(request.shard_id,info)?;
            let rows=tx.query(&self.query("SELECT ce.shard_id,ce.domain_id,ce.workflow_id,ce.run_id,ce.create_request_id,ce.state,ce.close_status,ce.start_version,e.last_write_version FROM current_executions ce INNER JOIN executions e ON e.shard_id=ce.shard_id AND e.domain_id=ce.domain_id AND e.workflow_id=ce.workflow_id AND e.run_id=ce.run_id WHERE ce.shard_id=? AND ce.domain_id=? AND ce.workflow_id=? FOR UPDATE"),&k[..3],&[Column::I32,Column::Bytes,Column::Text,Column::Bytes,Column::Text,Column::I32,Column::I32,Column::I64,Column::I64])?;
            let current=rows.first().map(|r|Ok::<_,PersistenceError>(CurrentExecution{run_id:uuid_string(&r[3])?,create_request_id:r[4].text()?,state:r[5].integer()? as i32,close_status:r[6].integer()? as i32,start_version:r[7].integer()?,last_write_version:r[8].integer()?})).transpose()?;
            if current.is_some(){validate_create_transition(request.mode,info,&request.previous_run_id,request.previous_last_write_version,current.as_ref())?;}
            if request.mode!=CreateWorkflowMode::Zombie{self.current_write(tx,request.shard_id,info,snapshot.start_version,snapshot.last_write_version,request.mode==CreateWorkflowMode::BrandNew)?;}
            self.apply_snapshot_tx(tx,request.shard_id,snapshot,false)?;Ok(CurrentExecution{run_id:info.run_id.clone(),create_request_id:info.create_request_id.clone(),state:info.state,close_status:info.close_status,start_version:snapshot.start_version,last_write_version:snapshot.last_write_version})})
    }
    pub fn query(&self, sql: &str) -> String {
        if self.backend.dialect() == crate::history_sql_queries::Dialect::MySql {
            return sql.into();
        }
        let mut n = 0;
        sql.chars()
            .map(|c| {
                if c == '?' {
                    n += 1;
                    format!("${n}")
                } else {
                    c.to_string()
                }
            })
            .collect()
    }
    pub fn get_current(
        &self,
        shard: i32,
        domain: &str,
        workflow: &str,
    ) -> Result<CurrentExecution, PersistenceError> {
        let k = vec![
            Value::I32(shard),
            uuid(domain)?,
            Value::Text(workflow.into()),
        ];
        self.backend
            .transaction(|tx| self.current_tx(tx, &k, false))?
            .ok_or_else(|| PersistenceError::EntityNotExists("current execution not found".into()))
    }
    pub fn get(
        &self,
        shard: i32,
        domain: &str,
        workflow: &str,
        run: &str,
    ) -> Result<WorkflowSnapshot, PersistenceError> {
        let k = key(
            shard,
            &WorkflowExecutionInfo {
                domain_id: domain.into(),
                workflow_id: workflow.into(),
                run_id: run.into(),
                ..Default::default()
            },
        )?;
        let mut snapshot = WorkflowSnapshot::default();
        // Go reads maps first and the execution/checksum row last.
        for map in [
            WorkflowStateMap::Activity,
            WorkflowStateMap::Timer,
            WorkflowStateMap::ChildExecution,
            WorkflowStateMap::RequestCancel,
            WorkflowStateMap::Signal,
        ] {
            let mut columns = vec![
                if map == WorkflowStateMap::Timer {
                    Column::Text
                } else {
                    Column::I64
                },
                Column::Bytes,
                Column::Text,
            ];
            if map == WorkflowStateMap::Activity {
                columns.extend([Column::Bytes, Column::Timestamp]);
            }
            let rows = self.backend.transaction(|tx| {
                tx.query(
                    &workflow_state_map_statement(
                        self.backend.dialect(),
                        map,
                        WorkflowMapOperation::Select,
                        0,
                    ),
                    &k,
                    &columns,
                )
            })?;
            for row in rows {
                let value = DataBlob {
                    data: row[1].bytes()?,
                    encoding: row[2].text()?,
                };
                if map == WorkflowStateMap::Timer {
                    snapshot.timer_infos.push(KeyedBlob {
                        key: row[0].text()?,
                        value,
                    });
                } else {
                    let entry = KeyedBlob {
                        key: row[0].integer()?,
                        value,
                    };
                    match map {
                        WorkflowStateMap::Activity => {
                            snapshot.activity_heartbeats.insert(
                                entry.key,
                                ActivityHeartbeat {
                                    details: row[3].bytes()?,
                                    updated_time_unix_nanos: row[4].nanos()?,
                                },
                            );
                            snapshot.activity_infos.push(entry)
                        }
                        WorkflowStateMap::ChildExecution => {
                            snapshot.child_execution_infos.push(entry)
                        }
                        WorkflowStateMap::RequestCancel => {
                            snapshot.request_cancel_infos.push(entry)
                        }
                        WorkflowStateMap::Signal => snapshot.signal_infos.push(entry),
                        _ => unreachable!(),
                    }
                }
            }
        }
        let events = self.backend.transaction(|tx| {
            tx.query(
                self.backend
                    .dialect()
                    .statement(Statement::SelectBufferedEvents),
                &k,
                &[Column::Bytes, Column::Text],
            )
        })?;
        snapshot.buffered_events = events
            .iter()
            .map(|r| {
                Ok(DataBlob {
                    data: r[0].bytes()?,
                    encoding: r[1].text()?,
                })
            })
            .collect::<Result<_, PersistenceError>>()?;
        let signals = self.backend.transaction(|tx| {
            tx.query(
                self.backend
                    .dialect()
                    .statement(Statement::SelectSignalRequested),
                &k,
                &[Column::Text],
            )
        })?;
        snapshot.signal_requested_ids = signals
            .iter()
            .map(|r| r[0].text())
            .collect::<Result<_, _>>()?;
        let rows = self.backend.transaction(|tx| {
            tx.query(
                self.backend.dialect().statement(Statement::GetExecution),
                &k,
                &[
                    Column::I32,
                    Column::Bytes,
                    Column::Text,
                    Column::Bytes,
                    Column::I64,
                    Column::I64,
                    Column::Bytes,
                    Column::Text,
                ],
            )
        })?;
        let row = rows.first().ok_or_else(|| {
            PersistenceError::EntityNotExists(format!(
                "Workflow execution not found.  WorkflowId: {workflow}, RunId: {run}"
            ))
        })?;
        let key = crate::history_sql::WorkflowKey {
            shard_id: shard,
            domain_id: uuid::Uuid::parse_str(domain)
                .map_err(internal)?
                .into_bytes(),
            workflow_id: workflow.into(),
            run_id: uuid::Uuid::parse_str(run).map_err(internal)?.into_bytes(),
        };
        let (execution_info, start_version, version_histories, checksum) =
            decode_execution_info(&crate::history_sql::ExecutionRow {
                key,
                next_event_id: row[4].integer()?,
                last_write_version: row[5].integer()?,
                data: row[6].bytes()?,
                data_encoding: row[7].text()?,
            })?;
        snapshot.execution_info = execution_info;
        snapshot.start_version = start_version;
        snapshot.last_write_version = row[5].integer()?;
        snapshot.version_histories = version_histories;
        snapshot.checksum = checksum;
        Ok(snapshot)
    }
}
fn ints(values: &[KeyedBlob<i64>]) -> Vec<(Value, DataBlob)> {
    values
        .iter()
        .map(|v| (Value::I64(v.key), v.value.clone()))
        .collect()
}
fn strings(values: &[KeyedBlob<String>]) -> Vec<(Value, DataBlob)> {
    values
        .iter()
        .map(|v| (Value::Text(v.key.clone()), v.value.clone()))
        .collect()
}
fn check_state(state: i32) -> Result<(), PersistenceError> {
    if !(0..=4).contains(&state) {
        Err(internal(format!("unknown workflow state: {state}")))
    } else {
        Ok(())
    }
}
fn validate_state_close(state: i32, close: i32, create: bool) -> Result<(), PersistenceError> {
    check_state(state)?;
    if (state == 2) == (close == 0) || !(0..=6).contains(&close) || (create && state == 2) {
        return Err(internal(format!(
            "invalid workflow state {state}, close status {close}"
        )));
    }
    Ok(())
}
fn validate_update(r: &UpdateWorkflowExecutionRequest) -> Result<(), PersistenceError> {
    let state = r.update_workflow_mutation.execution_info.state;
    check_state(state)?;
    let new = r
        .new_workflow_snapshot
        .as_ref()
        .map(|s| s.execution_info.state);
    if let Some(s) = new {
        check_state(s)?;
    }
    let invalid = match r.mode {
        UpdateWorkflowMode::UpdateCurrent => match new {
            None => state == 3,
            Some(s) => matches!(state, 0 | 1) || matches!(s, 2 | 3),
        },
        UpdateWorkflowMode::BypassCurrent => {
            matches!(state, 0 | 1) || new.is_some_and(|s| matches!(s, 0..=2))
        }
        UpdateWorkflowMode::IgnoreCurrent => new.is_some(),
    };
    if invalid {
        Err(internal(format!(
            "Invalid workflow update mode {}, current state: {state}, new state: {new:?}",
            r.mode as i32
        )))
    } else {
        Ok(())
    }
}
fn validate_conflict(r: &ConflictResolveWorkflowExecutionRequest) -> Result<(), PersistenceError> {
    let reset = r.reset_workflow_snapshot.execution_info.state;
    check_state(reset)?;
    let new = r
        .new_workflow_snapshot
        .as_ref()
        .map(|s| s.execution_info.state);
    let current = r
        .current_workflow_mutation
        .as_ref()
        .map(|s| s.execution_info.state);
    for s in [new, current].into_iter().flatten() {
        check_state(s)?;
    }
    let invalid = match r.mode {
        ConflictResolveWorkflowMode::UpdateCurrent => {
            current.is_some_and(|s| matches!(s, 0 | 1))
                || reset == 3
                || new.is_some_and(|s| matches!(reset, 0 | 1) || matches!(s, 2 | 3))
        }
        ConflictResolveWorkflowMode::BypassCurrent => {
            current.is_some()
                || matches!(reset, 0 | 1)
                || new.is_some_and(|s| reset == 3 || matches!(s, 0..=2))
        }
    };
    if invalid {
        Err(internal(format!("Invalid workflow conflict resolve mode {}, reset state: {reset}, new state: {new:?}, current state: {current:?}",r.mode as i32)))
    } else {
        Ok(())
    }
}

#[async_trait::async_trait]
impl GoExecutionStore for SqlWorkflowStore {
    async fn create_workflow_execution(
        &self,
        request: CreateWorkflowExecutionRequest,
    ) -> Result<CurrentExecution, PersistenceError> {
        self.create(&request)
    }
    async fn get_workflow_execution(
        &self,
        shard_id: i32,
        domain_id: String,
        workflow_id: String,
        run_id: String,
        range_id: i64,
    ) -> Result<WorkflowSnapshot, PersistenceError> {
        let mut state = self.get(shard_id, &domain_id, &workflow_id, &run_id)?;
        if state.checksum.is_some() {
            let rows = self.backend.transaction(|tx| {
                tx.query(
                    &self.query("SELECT range_id FROM shards WHERE shard_id=?"),
                    &[Value::I32(shard_id)],
                    &[Column::I64],
                )
            })?;
            if rows.first().ok_or_else(|| internal("shard missing"))?[0].integer()? != range_id {
                state.checksum = None;
            }
        }
        Ok(state)
    }
    async fn update_workflow_execution(
        &self,
        request: UpdateWorkflowExecutionRequest,
    ) -> Result<(), PersistenceError> {
        self.update(&request)
    }
    async fn conflict_resolve_workflow_execution(
        &self,
        request: ConflictResolveWorkflowExecutionRequest,
    ) -> Result<(), PersistenceError> {
        self.conflict_resolve(&request)
    }
    async fn delete_workflow_execution(
        &self,
        shard_id: i32,
        domain_id: String,
        workflow_id: String,
        run_id: String,
    ) -> Result<(), PersistenceError> {
        eprintln!("DIAG delete_workflow_execution shard={shard_id} domain={domain_id} wf={workflow_id} run={run_id}\n{}", std::backtrace::Backtrace::force_capture());
        let k = key(
            shard_id,
            &WorkflowExecutionInfo {
                domain_id,
                workflow_id,
                run_id,
                ..Default::default()
            },
        )?;
        self.backend.transaction(|tx| {
            for statement in [
                Statement::DeleteExecution,
                Statement::DeleteActivityInfoMaps,
                Statement::DeleteTimerInfoMaps,
                Statement::DeleteChildExecutionInfoMaps,
                Statement::DeleteRequestCancelInfoMaps,
                Statement::DeleteSignalInfoMaps,
                Statement::DeleteSignalsRequested,
                Statement::DeleteBufferedEvents,
            ] {
                tx.execute(self.backend.dialect().statement(statement), &k)?;
            }
            Ok(())
        })
    }
    async fn delete_current_workflow_execution(
        &self,
        shard_id: i32,
        domain_id: String,
        workflow_id: String,
        run_id: String,
    ) -> Result<(), PersistenceError> {
        let k = key(
            shard_id,
            &WorkflowExecutionInfo {
                domain_id,
                workflow_id,
                run_id,
                ..Default::default()
            },
        )?;
        self.backend.transaction(|tx| {
            tx.execute(
                self.backend
                    .dialect()
                    .statement(Statement::DeleteCurrentExecution),
                &k,
            )
        })?;
        Ok(())
    }
    async fn get_current_execution(
        &self,
        shard_id: i32,
        domain_id: String,
        workflow_id: String,
    ) -> Result<CurrentExecution, PersistenceError> {
        self.get_current(shard_id, &domain_id, &workflow_id)
    }
    async fn is_workflow_execution_exists(
        &self,
        _shard_id: i32,
        _domain_id: String,
        _workflow_id: String,
        _run_id: String,
    ) -> Result<bool, PersistenceError> {
        Err(internal("Not yet implemented"))
    }
    async fn list_current_executions(
        &self,
        _filter: ExecutionListFilter,
    ) -> Result<ExecutionPage, PersistenceError> {
        Err(internal("Not yet implemented"))
    }
    async fn list_concrete_executions(
        &self,
        filter: ExecutionListFilter,
    ) -> Result<ExecutionPage, PersistenceError> {
        let (shard, workflow) = if filter.next_page_token.is_empty() {
            (filter.shard_id, String::new())
        } else {
            crate::history_tree_token::decode_execution(&filter.next_page_token)?
        };
        let rows=self.backend.transaction(|tx|tx.query(&self.query("SELECT shard_id,domain_id,workflow_id,run_id,next_event_id,last_write_version,data,data_encoding FROM executions WHERE shard_id=? AND workflow_id>? ORDER BY workflow_id LIMIT ?"),&[Value::I32(shard),Value::Text(workflow),Value::I32(filter.page_size)],&[Column::I32,Column::Bytes,Column::Text,Column::Bytes,Column::I64,Column::I64,Column::Bytes,Column::Text]))?;
        let mut executions = Vec::new();
        for row in &rows {
            let key = crate::history_sql::WorkflowKey {
                shard_id: row[0].integer()? as i32,
                domain_id: uuid::Uuid::parse_str(&uuid_string(&row[1])?)
                    .map_err(internal)?
                    .into_bytes(),
                workflow_id: row[2].text()?,
                run_id: uuid::Uuid::parse_str(&uuid_string(&row[3])?)
                    .map_err(internal)?
                    .into_bytes(),
            };
            let (execution_info, start_version, version_histories, checksum) =
                decode_execution_info(&crate::history_sql::ExecutionRow {
                    key,
                    next_event_id: row[4].integer()?,
                    last_write_version: row[5].integer()?,
                    data: row[6].bytes()?,
                    data_encoding: row[7].text()?,
                })?;
            executions.push(WorkflowSnapshot {
                execution_info,
                start_version,
                last_write_version: row[5].integer()?,
                version_histories,
                checksum,
                ..Default::default()
            });
        }
        let next_page_token = rows
            .last()
            .map(|r| {
                Ok::<_, PersistenceError>(crate::history_tree_token::encode_execution(
                    filter.shard_id,
                    &r[2].text()?,
                ))
            })
            .transpose()?
            .unwrap_or_default();
        Ok(ExecutionPage {
            executions,
            next_page_token,
        })
    }
    async fn create_transfer_tasks(
        &self,
        shard_id: i32,
        tasks: Vec<TransferTask>,
    ) -> Result<(), PersistenceError> {
        let tasks = tasks
            .iter()
            .map(|task| {
                Ok(Task {
                    visibility_timestamp_unix_nanos: 0,
                    task_type: task.task_type,
                    task_id: task.task_id,
                    data: DataBlob {
                        data: self
                            .encoding
                            .encode(
                                &crate::sqlblobs::transfer_task_to_struct(task)
                                    .map_err(internal)?,
                            )
                            .map_err(internal)?,
                        encoding: self.encoding.as_str().into(),
                    },
                })
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;
        self.task_store().create(
            shard_id,
            HistoryTaskCategory {
                category_type: 1,
                category_id: 1,
            },
            &tasks,
        )
    }
    async fn get_transfer_tasks(
        &self,
        shard_id: i32,
        min_id: i64,
        max_id: i64,
        batch_size: i32,
    ) -> Result<Vec<TransferTask>, PersistenceError> {
        let page = self
            .task_store()
            .get_page(&crate::history_task_sql::HistoryTaskQuery {
                shard_id,
                category: HistoryTaskCategory {
                    category_type: 1,
                    category_id: 1,
                },
                min_task_id: min_id,
                max_task_id: max_id,
                page_size: batch_size,
                ..Default::default()
            })?;
        page.tasks
            .into_iter()
            .map(|task| {
                let mut value = crate::sqlblobs::Encoding::parse(&task.data.encoding)
                    .and_then(|e| e.decode(&task.data.data))
                    .and_then(|v| crate::sqlblobs::transfer_task_from_struct(&v))
                    .map_err(internal)?;
                value.task_id = task.task_id;
                Ok(value)
            })
            .collect()
    }
    async fn complete_transfer_task(
        &self,
        shard_id: i32,
        task_id: i64,
    ) -> Result<(), PersistenceError> {
        self.task_store().complete(
            shard_id,
            &HistoryTaskCategory {
                category_type: 1,
                category_id: 1,
            },
            task_id,
            0,
        )
    }
    async fn create_replication_tasks(
        &self,
        shard_id: i32,
        tasks: Vec<ReplicationTask>,
    ) -> Result<(), PersistenceError> {
        let tasks = tasks
            .iter()
            .map(|task| {
                Ok(Task {
                    visibility_timestamp_unix_nanos: 0,
                    task_type: task.task_type,
                    task_id: task.task_id,
                    data: DataBlob {
                        data: self
                            .encoding
                            .encode(
                                &crate::sqlblobs::replication_task_to_struct(task)
                                    .map_err(internal)?,
                            )
                            .map_err(internal)?,
                        encoding: self.encoding.as_str().into(),
                    },
                })
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;
        self.task_store().create(
            shard_id,
            HistoryTaskCategory {
                category_type: 1,
                category_id: 3,
            },
            &tasks,
        )
    }
    async fn get_replication_tasks(
        &self,
        shard_id: i32,
        min_id: i64,
        max_id: i64,
        batch_size: i32,
    ) -> Result<Vec<ReplicationTask>, PersistenceError> {
        let page = self
            .task_store()
            .get_page(&crate::history_task_sql::HistoryTaskQuery {
                shard_id,
                category: HistoryTaskCategory {
                    category_type: 1,
                    category_id: 3,
                },
                min_task_id: min_id,
                max_task_id: max_id,
                page_size: batch_size,
                ..Default::default()
            })?;
        page.tasks
            .into_iter()
            .map(|task| {
                let mut value = crate::sqlblobs::Encoding::parse(&task.data.encoding)
                    .and_then(|e| e.decode(&task.data.data))
                    .and_then(|v| crate::sqlblobs::replication_task_from_struct(&v))
                    .map_err(internal)?;
                value.task_id = task.task_id;
                Ok(value)
            })
            .collect()
    }
    async fn complete_replication_task(
        &self,
        shard_id: i32,
        task_id: i64,
    ) -> Result<(), PersistenceError> {
        self.task_store().complete(
            shard_id,
            &HistoryTaskCategory {
                category_type: 1,
                category_id: 3,
            },
            task_id,
            0,
        )
    }
    async fn create_timer_tasks(
        &self,
        shard_id: i32,
        tasks: Vec<TimerTask>,
    ) -> Result<(), PersistenceError> {
        let tasks = tasks
            .iter()
            .map(|task| {
                Ok(Task {
                    visibility_timestamp_unix_nanos: task.visibility_timestamp_unix_nanos,
                    task_type: task.task_type,
                    task_id: task.task_id,
                    data: DataBlob {
                        data: self
                            .encoding
                            .encode(&crate::sqlblobs::timer_task_to_struct(task).map_err(internal)?)
                            .map_err(internal)?,
                        encoding: self.encoding.as_str().into(),
                    },
                })
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;
        self.task_store().create(
            shard_id,
            HistoryTaskCategory {
                category_type: 2,
                category_id: 2,
            },
            &tasks,
        )
    }
    async fn select_timer_tasks(
        &self,
        shard_id: i32,
        min_timestamp: i64,
        max_timestamp: i64,
        batch_size: i32,
    ) -> Result<Vec<TimerTask>, PersistenceError> {
        let page = self
            .task_store()
            .get_page(&crate::history_task_sql::HistoryTaskQuery {
                shard_id,
                category: HistoryTaskCategory {
                    category_type: 2,
                    category_id: 2,
                },
                min_timestamp_unix_nanos: min_timestamp,
                max_timestamp_unix_nanos: max_timestamp,
                page_size: batch_size,
                ..Default::default()
            })?;
        page.tasks
            .into_iter()
            .map(|task| {
                let mut value = crate::sqlblobs::Encoding::parse(&task.data.encoding)
                    .and_then(|e| e.decode(&task.data.data))
                    .and_then(|v| crate::sqlblobs::timer_task_from_struct(&v))
                    .map_err(internal)?;
                value.task_id = task.task_id;
                value.visibility_timestamp_unix_nanos = task.visibility_timestamp_unix_nanos;
                Ok(value)
            })
            .collect()
    }
    async fn complete_timer_task(
        &self,
        shard_id: i32,
        visibility_timestamp: i64,
        task_id: i64,
    ) -> Result<(), PersistenceError> {
        self.task_store().complete(
            shard_id,
            &HistoryTaskCategory {
                category_type: 2,
                category_id: 2,
            },
            task_id,
            visibility_timestamp,
        )
    }
    async fn select_workflow_timer_tasks(
        &self,
        _request: SelectWorkflowTimerTasksRequest,
    ) -> Result<Vec<HistoryTaskKey>, PersistenceError> {
        Ok(Vec::new())
    }
    async fn create_history_tasks(
        &self,
        _request: CreateHistoryTasksRequest,
    ) -> Result<(), PersistenceError> {
        self.task_store().create_history_tasks()
    }
    async fn create_failover_marker_tasks(
        &self,
        shard_id: i32,
        tasks: Vec<Task>,
    ) -> Result<(), PersistenceError> {
        self.create_failover_markers(shard_id, self.range_id, &tasks)
    }
    async fn get_history_tasks(
        &self,
        range: TaskRange,
        category: HistoryTaskCategory,
    ) -> Result<Vec<Task>, PersistenceError> {
        Ok(self
            .task_store()
            .get_page(&crate::history_task_sql::HistoryTaskQuery {
                shard_id: range.shard_id,
                category,
                min_task_id: range.min_task_id,
                max_task_id: range.max_task_id,
                page_size: range.batch_size,
                ..Default::default()
            })?
            .tasks)
    }
    async fn complete_history_task(
        &self,
        shard_id: i32,
        category: HistoryTaskCategory,
        task_id: i64,
    ) -> Result<(), PersistenceError> {
        self.task_store().complete(shard_id, &category, task_id, 0)
    }
    async fn range_complete_history_tasks(
        &self,
        range: TaskRange,
        category: HistoryTaskCategory,
    ) -> Result<u64, PersistenceError> {
        self.task_store()
            .range_complete(&crate::history_task_sql::HistoryTaskQuery {
                shard_id: range.shard_id,
                category,
                min_task_id: range.min_task_id,
                max_task_id: range.max_task_id,
                page_size: range.batch_size,
                ..Default::default()
            })
    }
    async fn get_replication_dlq_size(
        &self,
        shard_id: i32,
        source_cluster: String,
    ) -> Result<i64, PersistenceError> {
        let mut store = self.task_store();
        store.shard_id = shard_id;
        store.dlq_size(&source_cluster)
    }
    async fn delete_replication_task_from_dlq(
        &self,
        shard_id: i32,
        source_cluster: String,
        task_id: i64,
    ) -> Result<(), PersistenceError> {
        let mut store = self.task_store();
        store.shard_id = shard_id;
        store.delete_dlq(&source_cluster, task_id)
    }
    async fn put_replication_task_to_dlq(
        &self,
        shard_id: i32,
        source_cluster: String,
        task: ReplicationTask,
    ) -> Result<(), PersistenceError> {
        let mut store = self.task_store();
        store.shard_id = shard_id;
        store.put_dlq(&source_cluster, &task)
    }
    async fn get_replication_tasks_from_dlq(
        &self,
        source_cluster: String,
        min_id: i64,
        max_id: i64,
        batch_size: i32,
    ) -> Result<Vec<ReplicationTask>, PersistenceError> {
        self.task_store()
            .get_dlq(&source_cluster, min_id, max_id, batch_size)
    }
    async fn range_delete_replication_tasks_from_dlq(
        &self,
        source_cluster: String,
        inclusive_max_id: i64,
        page_size: i32,
    ) -> Result<u64, PersistenceError> {
        self.task_store().range_delete_dlq(
            &source_cluster,
            i64::MIN,
            inclusive_max_id.wrapping_add(1),
            page_size,
        )
    }
    async fn create_replication_tasks_from_dlq(
        &self,
        source_cluster: String,
        tasks: Vec<ReplicationTask>,
    ) -> Result<(), PersistenceError> {
        for task in tasks {
            self.task_store().put_dlq(&source_cluster, &task)?;
        }
        Ok(())
    }
    async fn create_cross_cluster_tasks(
        &self,
        shard_id: i32,
        tasks: Vec<CrossClusterTask>,
    ) -> Result<(), PersistenceError> {
        self.backend.transaction(|tx| {
            for task in tasks {
                let data = self
                    .encoding
                    .encode(
                        &crate::sqlblobs::cross_cluster_task_to_struct(&task).map_err(internal)?,
                    )
                    .map_err(internal)?;
                tx.execute(
                    self.backend
                        .dialect()
                        .statement(Statement::InsertCrossClusterTask),
                    &[
                        Value::Text(task.target_cluster),
                        Value::I32(shard_id),
                        Value::I64(task.transfer_task.task_id),
                        Value::Bytes(data),
                        Value::Text(self.encoding.as_str().into()),
                    ],
                )?;
            }
            Ok(())
        })
    }
    async fn get_cross_cluster_tasks(
        &self,
        shard_id: i32,
        min_id: i64,
        max_id: i64,
        batch_size: i32,
    ) -> Result<Vec<CrossClusterTask>, PersistenceError> {
        let rows = self.backend.transaction(|tx| {
            tx.query(
                self.backend
                    .dialect()
                    .statement(Statement::SelectCrossClusterTasks),
                &[
                    Value::Text(self.target_cluster.clone()),
                    Value::I32(shard_id),
                    Value::I64(min_id),
                    Value::I64(max_id),
                    Value::I32(batch_size),
                ],
                &[Column::I64, Column::Bytes, Column::Text],
            )
        })?;
        rows.into_iter()
            .map(|r| {
                let encoding = crate::sqlblobs::Encoding::parse(&r[2].text()?).map_err(internal)?;
                let mut task = crate::sqlblobs::cross_cluster_task_from_struct(
                    &encoding.decode(&r[1].bytes()?).map_err(internal)?,
                )
                .map_err(internal)?;
                task.transfer_task.task_id = r[0].integer()?;
                task.target_cluster = self.target_cluster.clone();
                Ok(task)
            })
            .collect()
    }
    async fn complete_cross_cluster_task(
        &self,
        shard_id: i32,
        task_id: i64,
    ) -> Result<(), PersistenceError> {
        self.backend.transaction(|tx| {
            tx.execute(
                self.backend
                    .dialect()
                    .statement(Statement::DeleteCrossClusterTask),
                &[
                    Value::Text(self.target_cluster.clone()),
                    Value::I32(shard_id),
                    Value::I64(task_id),
                ],
            )
        })?;
        Ok(())
    }
    async fn get_replication_dlq_page(
        &self,
        request: crate::history_task_sql::ReplicationDlqQuery,
    ) -> Result<crate::history_task_sql::ReplicationDlqPage, PersistenceError> {
        self.task_store().get_dlq_page(&request)
    }
    async fn range_delete_replication_dlq_page(
        &self,
        shard_id: i32,
        source_cluster: String,
        inclusive_min_id: i64,
        exclusive_max_id: i64,
        page_size: i32,
    ) -> Result<u64, PersistenceError> {
        let mut store = self.task_store();
        store.shard_id = shard_id;
        store.range_delete_dlq(
            &source_cluster,
            inclusive_min_id,
            exclusive_max_id,
            page_size,
        )
    }
    async fn get_history_task_page(
        &self,
        request: crate::history_task_sql::HistoryTaskQuery,
    ) -> Result<crate::history_task_sql::HistoryTaskPage, PersistenceError> {
        self.task_store().get_page(&request)
    }
    async fn complete_history_task_batch(
        &self,
        shard_id: i32,
        category: HistoryTaskCategory,
        keys: Vec<(i64, i64)>,
    ) -> Result<(), PersistenceError> {
        self.complete_history_task_keys(shard_id, &category, &keys)
    }
    async fn range_complete_history_task_page(
        &self,
        request: crate::history_task_sql::HistoryTaskQuery,
    ) -> Result<u64, PersistenceError> {
        self.task_store().range_complete(&request)
    }
    async fn create_failover_markers_with_range(
        &self,
        shard_id: i32,
        range_id: i64,
        markers: Vec<Task>,
    ) -> Result<(), PersistenceError> {
        self.create_failover_markers(shard_id, range_id, &markers)
    }
    async fn get_active_cluster_selection_policy(
        &self,
        shard_id: i32,
        domain_id: String,
        workflow_id: String,
        run_id: String,
    ) -> Result<Option<DataBlob>, PersistenceError> {
        let k = key(
            shard_id,
            &WorkflowExecutionInfo {
                domain_id,
                workflow_id,
                run_id,
                ..Default::default()
            },
        )?;
        let rows=self.backend.transaction(|tx|tx.query(&self.query("SELECT data,data_encoding FROM active_cluster_selection_policy WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?"),&k,&[Column::Bytes,Column::Text]))?;
        rows.first()
            .map(|r| {
                Ok(DataBlob {
                    data: r[0].bytes()?,
                    encoding: r[1].text()?,
                })
            })
            .transpose()
    }
    async fn delete_active_cluster_selection_policy(
        &self,
        shard_id: i32,
        domain_id: String,
        workflow_id: String,
        run_id: String,
    ) -> Result<(), PersistenceError> {
        let k = key(
            shard_id,
            &WorkflowExecutionInfo {
                domain_id,
                workflow_id,
                run_id,
                ..Default::default()
            },
        )?;
        self.backend.transaction(|tx|tx.execute(&self.query("DELETE FROM active_cluster_selection_policy WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?"),&k))?;
        Ok(())
    }
}
impl SqlWorkflowStore {
    pub fn task_store(&self) -> SqlHistoryTasks {
        SqlHistoryTasks {
            backend: self.backend.clone(),
            encoding: self.encoding,
            shard_id: self.shard_id,
            target_cluster: self.target_cluster.clone(),
        }
    }
}

impl SqlWorkflowStore {
    pub fn new(
        backend: SqlBackend,
        encoding: crate::sqlblobs::Encoding,
        shard_id: i32,
        range_id: i64,
    ) -> Self {
        Self {
            backend,
            encoding,
            shard_id,
            range_id,
            target_cluster: String::new(),
        }
    }
    pub fn create_failover_markers(
        &self,
        shard_id: i32,
        range_id: i64,
        markers: &[Task],
    ) -> Result<(), PersistenceError> {
        let tasks = markers
            .iter()
            .map(|marker| {
                let mut task = crate::sqlblobs::Encoding::parse(&marker.data.encoding)
                    .and_then(|e| e.decode(&marker.data.data))
                    .and_then(|v| crate::sqlblobs::replication_task_from_struct(&v))
                    .map_err(internal)?;
                task.workflow_id = String::new();
                task.run_id = "30000000-5000-f000-f000-000000000000".into();
                task.task_type = marker.task_type;
                task.first_event_id = -23;
                task.next_event_id = -23;
                task.scheduled_id = -23;
                task.event_store_version = 2;
                task.new_run_event_store_version = 2;
                task.branch_token.clear();
                task.new_run_branch_token.clear();
                task.creation_time_unix_nanos = marker.visibility_timestamp_unix_nanos;
                Ok(Task {
                    visibility_timestamp_unix_nanos: marker.visibility_timestamp_unix_nanos,
                    task_id: marker.task_id,
                    task_type: marker.task_type,
                    data: DataBlob {
                        data: self
                            .encoding
                            .encode(
                                &crate::sqlblobs::replication_task_to_struct(&task)
                                    .map_err(internal)?,
                            )
                            .map_err(internal)?,
                        encoding: self.encoding.as_str().into(),
                    },
                })
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;
        self.backend.transaction(|tx| {
            self.lock_shard(tx, shard_id, range_id)?;
            SqlHistoryTasks::insert_tx(
                tx,
                self.backend.dialect(),
                shard_id,
                &HistoryTaskCategory {
                    category_type: 1,
                    category_id: 3,
                },
                &tasks,
            )
        })
    }
    pub fn complete_history_task_keys(
        &self,
        shard: i32,
        category: &HistoryTaskCategory,
        keys: &[(i64, i64)],
    ) -> Result<(), PersistenceError> {
        let statement = match (category.category_type, category.category_id) {
            (1, 1) => Statement::DeleteTransferTask,
            (1, 3) => Statement::DeleteReplicationTask,
            (2, 2) => Statement::DeleteTimerTask,
            _ => {
                return Err(PersistenceError::InvalidArgument(
                    "Unknown task category".into(),
                ))
            }
        };
        self.backend.transaction(|tx| {
            for (task_id, time) in keys {
                let mut params = vec![Value::I32(shard)];
                if category.category_type == 2 {
                    params.push(timestamp(*time)?);
                }
                params.push(Value::I64(*task_id));
                tx.execute(self.backend.dialect().statement(statement), &params)?;
            }
            Ok(())
        })
    }
}
