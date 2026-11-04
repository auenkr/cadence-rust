//! Shard-locked execution transactions, following Go's SQL write ordering.
#![allow(clippy::too_many_arguments)]
use crate::{history_contract::*, history_sql_backend::{SqlBackend,SqlTx,Value,Column,uuid,uuid_string,internal,timestamp}, history_sql_queries::{Statement,workflow_state_map_statement,WorkflowStateMap,WorkflowMapOperation}, history_task_sql::SqlHistoryTasks, sql_execution_store::{snapshot_to_execution_row,decode_execution_info,validate_create_transition}};

#[derive(Clone)]
pub struct SqlWorkflowStore { pub backend:SqlBackend, pub encoding:crate::sqlblobs::Encoding, pub shard_id:i32 }
fn key(shard:i32,info:&WorkflowExecutionInfo)->Result<Vec<Value>,PersistenceError>{Ok(vec![Value::I32(shard),uuid(&info.domain_id)?,Value::Text(info.workflow_id.clone()),uuid(&info.run_id)?])}
impl SqlWorkflowStore {
    pub fn lock_shard(&self,tx:&mut dyn SqlTx,shard:i32,range:i64)->Result<(),PersistenceError>{
        let rows=tx.query(self.backend.dialect().statement(Statement::LockShard),&[Value::I32(shard)],&[Column::I64])?;
        let found=rows.first().ok_or_else(||PersistenceError::EntityNotExists(format!("shard {shard} not found")))?[0].integer()?;
        if found!=range{return Err(PersistenceError::ShardOwnershipLost{shard_id:shard,expected_range_id:range,actual_range_id:found});} Ok(())
    }
    fn current_tx(&self,tx:&mut dyn SqlTx,k:&[Value],lock:bool)->Result<Option<CurrentExecution>,PersistenceError>{
        let statement=if lock{Statement::LockCurrentExecution}else{Statement::GetCurrentExecution};
        let columns=if lock{vec![Column::I32,Column::Bytes,Column::Text,Column::Bytes,Column::Text,Column::I32,Column::I32,Column::I64,Column::I64]}else{vec![Column::Bytes,Column::Text,Column::I32,Column::I32,Column::I64,Column::I64]};
        let rows=tx.query(self.backend.dialect().statement(statement),&k[..3],&columns)?;
        rows.first().map(|row|{let row=if lock{&row[3..]}else{row.as_slice()};Ok(CurrentExecution{run_id:uuid_string(&row[0])?,create_request_id:row[1].text()?,state:row[2].integer()? as i32,close_status:row[3].integer()? as i32,start_version:row[4].integer()?,last_write_version:row[5].integer()?})}).transpose()
    }
    fn current_write(&self,tx:&mut dyn SqlTx,shard:i32,info:&WorkflowExecutionInfo,start:i64,last:i64,insert:bool)->Result<(),PersistenceError>{
        let k=key(shard,info)?;let mut params=vec![k[3].clone(),Value::Text(info.create_request_id.clone()),Value::I32(info.state),Value::I32(info.close_status),Value::I64(start),Value::I64(last)];
        let statement=if insert{let mut prefix=k[..3].to_vec();prefix.extend(params);params=prefix;Statement::InsertCurrentExecution}else{params.extend_from_slice(&k[..3]);Statement::UpdateCurrentExecution};
        let count=tx.execute(self.backend.dialect().statement(statement),&params)?;if count!=1{return Err(internal(format!("current_executions affected {count} instead of 1 rows")));} Ok(())
    }
    fn assert_current(&self,tx:&mut dyn SqlTx,shard:i32,info:&WorkflowExecutionInfo,previous:&str,bypass:bool)->Result<(),PersistenceError>{
        let current=self.current_tx(tx,&key(shard,info)?,true)?;
        if bypass{if current.as_ref().is_some_and(|v|v.run_id==previous){return Err(PersistenceError::ConditionFailed(format!("assertRunIDMismatch failed. Current run ID was {previous}, input {previous}")));}return Ok(());}
        let current=current.ok_or_else(||PersistenceError::EntityNotExists("Unable to load current record.".into()))?;
        if current.run_id!=previous{return Err(PersistenceError::ConditionFailed(format!("assertRunIDAndUpdateCurrentExecution failed. Current run ID was {}, expected {previous}",current.run_id)));}Ok(())
    }
    fn lock_condition(&self,tx:&mut dyn SqlTx,k:&[Value],condition:i64)->Result<(),PersistenceError>{
        let rows=tx.query(self.backend.dialect().statement(Statement::LockExecution),k,&[Column::I64])?;
        let found=rows.first().ok_or_else(||PersistenceError::EntityNotExists("execution not found".into()))?[0].integer()?;
        if found!=condition{return Err(PersistenceError::ConditionFailed(format!("NextEventID condition failed. Expected: {condition}, Actual: {found}")));}Ok(())
    }
    fn execution_write(&self,tx:&mut dyn SqlTx,shard:i32,snapshot:&WorkflowSnapshot,insert:bool)->Result<(),PersistenceError>{
        let info=&snapshot.execution_info; validate_state_close(info.state,info.close_status,insert)?;
        let row=snapshot_to_execution_row(snapshot,shard,self.encoding)?;let k=key(shard,info)?;
        let mut values=vec![Value::I64(row.next_event_id),Value::I64(row.last_write_version),Value::Bytes(row.data),Value::Text(row.data_encoding)];
        let statement=if insert{let mut prefix=k;prefix.extend(values);values=prefix;Statement::InsertExecution}else{values.extend(k);Statement::UpdateExecution};
        let result=tx.execute(self.backend.dialect().statement(statement),&values);
        let count=match result{Err(PersistenceError::ConditionFailed(_)) if insert=>return Err(PersistenceError::WorkflowAlreadyStarted{message:format!("Workflow execution already running. WorkflowId: {}",info.workflow_id),start_request_id:info.create_request_id.clone(),run_id:info.run_id.clone(),state:info.state,close_status:info.close_status,last_write_version:snapshot.last_write_version}),other=>other?};
        if count!=1{return Err(internal(format!("execution write affected {count} instead of 1 rows")));}Ok(())
    }
    fn maps(&self,tx:&mut dyn SqlTx,k:&[Value],map:WorkflowStateMap,values:Vec<(Value,DataBlob)>,delete:Vec<Value>,reset:bool)->Result<(),PersistenceError>{
        let dialect=self.backend.dialect();
        if reset{tx.execute(&workflow_state_map_statement(dialect,map,WorkflowMapOperation::DeleteAll,0),k)?;}
        for (id,blob) in values{let mut params=k.to_vec();params.extend([id,Value::Bytes(blob.data),Value::Text(blob.encoding)]);if map==WorkflowStateMap::Activity{params.extend([Value::Bytes(Vec::new()),timestamp(0)?]);}tx.execute(&workflow_state_map_statement(dialect,map,WorkflowMapOperation::Upsert,0),&params)?;}
        if !delete.is_empty(){let query=workflow_state_map_statement(dialect,map,WorkflowMapOperation::DeleteKeys,delete.len());let mut params=k.to_vec();params.extend(delete);tx.execute(&query,&params)?;}Ok(())
    }
    fn signals(&self,tx:&mut dyn SqlTx,k:&[Value],insert:&[String],delete:&[String],reset:bool)->Result<(),PersistenceError>{
        let dialect=self.backend.dialect();if reset{tx.execute(dialect.statement(Statement::DeleteSignalRequested),k)?;}
        for id in insert{let mut params=k.to_vec();params.push(Value::Text(id.clone()));tx.execute(dialect.statement(Statement::InsertSignalRequested),&params)?;}
        if !delete.is_empty(){let query=crate::history_sql_queries::delete_signal_requested_keys_statement(dialect,delete.len());let mut params=k.to_vec();params.extend(delete.iter().cloned().map(Value::Text));tx.execute(&query,&params)?;}Ok(())
    }
    fn tasks(&self,tx:&mut dyn SqlTx,shard:i32,tasks:&std::collections::BTreeMap<HistoryTaskCategory,Vec<Task>>)->Result<(),PersistenceError>{for (category,tasks)in tasks{SqlHistoryTasks::insert_tx(tx,self.backend.dialect(),shard,category,tasks)?;}Ok(())}
    pub fn apply_snapshot_tx(&self,tx:&mut dyn SqlTx,shard:i32,snapshot:&WorkflowSnapshot,reset:bool)->Result<(),PersistenceError>{
        let k=key(shard,&snapshot.execution_info)?;
        if reset{self.lock_condition(tx,&k,snapshot.condition)?;}else if let Some(policy)=&snapshot.execution_info.active_cluster_selection_policy{let mut params=k.clone();params.extend([Value::Bytes(policy.data.clone()),Value::Text(policy.encoding.clone())]);tx.execute(&self.query("INSERT INTO active_cluster_selection_policy(shard_id,domain_id,workflow_id,run_id,data,data_encoding) VALUES(?,?,?,?,?,?)"),&params)?;}
        self.execution_write(tx,shard,snapshot,!reset)?;self.tasks(tx,shard,&snapshot.tasks_by_category)?;
        self.maps(tx,&k,WorkflowStateMap::Activity,ints(&snapshot.activity_infos),vec![],reset)?;
        self.maps(tx,&k,WorkflowStateMap::Timer,strings(&snapshot.timer_infos),vec![],reset)?;
        self.maps(tx,&k,WorkflowStateMap::ChildExecution,ints(&snapshot.child_execution_infos),vec![],reset)?;
        self.maps(tx,&k,WorkflowStateMap::RequestCancel,ints(&snapshot.request_cancel_infos),vec![],reset)?;
        self.maps(tx,&k,WorkflowStateMap::Signal,ints(&snapshot.signal_infos),vec![],reset)?;
        self.signals(tx,&k,&snapshot.signal_requested_ids,&[],reset)?;
        if reset{tx.execute(self.backend.dialect().statement(Statement::DeleteBufferedEvents),&k)?;}Ok(())
    }
    pub fn apply_mutation_tx(&self,tx:&mut dyn SqlTx,shard:i32,m:&WorkflowMutation)->Result<(),PersistenceError>{
        let k=key(shard,&m.execution_info)?;self.lock_condition(tx,&k,m.condition)?;
        self.execution_write(tx,shard,&WorkflowSnapshot{execution_info:m.execution_info.clone(),start_version:m.start_version,last_write_version:m.last_write_version,version_histories:m.version_histories.clone(),checksum:m.checksum.clone(),..Default::default()},false)?;
        self.tasks(tx,shard,&m.tasks_by_category)?;
        self.maps(tx,&k,WorkflowStateMap::Activity,ints(&m.upsert_activity_infos),m.delete_activity_infos.iter().copied().map(Value::I64).collect(),false)?;
        self.maps(tx,&k,WorkflowStateMap::Timer,strings(&m.upsert_timer_infos),m.delete_timer_infos.iter().cloned().map(Value::Text).collect(),false)?;
        self.maps(tx,&k,WorkflowStateMap::ChildExecution,ints(&m.upsert_child_execution_infos),m.delete_child_execution_infos.iter().copied().map(Value::I64).collect(),false)?;
        self.maps(tx,&k,WorkflowStateMap::RequestCancel,ints(&m.upsert_request_cancel_infos),m.delete_request_cancel_infos.iter().copied().map(Value::I64).collect(),false)?;
        self.maps(tx,&k,WorkflowStateMap::Signal,ints(&m.upsert_signal_infos),m.delete_signal_infos.iter().copied().map(Value::I64).collect(),false)?;
        self.signals(tx,&k,&m.upsert_signal_requested_ids,&m.delete_signal_requested_ids,false)?;
        if m.clear_buffered_events{tx.execute(self.backend.dialect().statement(Statement::DeleteBufferedEvents),&k)?;}
        if let Some(blob)=&m.new_buffered_events{let mut params=k;params.extend([Value::Bytes(blob.data.clone()),Value::Text(blob.encoding.clone())]);tx.execute(self.backend.dialect().statement(Statement::InsertBufferedEvent),&params)?;}Ok(())
    }
    pub fn update(&self,request:&UpdateWorkflowExecutionRequest)->Result<(),PersistenceError>{
        validate_update(request)?;let m=&request.update_workflow_mutation;
        self.backend.transaction(|tx|{self.lock_shard(tx,request.shard_id,request.range_id)?;
            match request.mode{UpdateWorkflowMode::IgnoreCurrent=>{},UpdateWorkflowMode::BypassCurrent=>self.assert_current(tx,request.shard_id,&m.execution_info,&m.execution_info.run_id,true)?,UpdateWorkflowMode::UpdateCurrent=>{self.assert_current(tx,request.shard_id,&m.execution_info,&m.execution_info.run_id,false)?;if let Some(new)=&request.new_workflow_snapshot{if new.execution_info.domain_id!=m.execution_info.domain_id{return Err(internal("UpdateWorkflowExecution: cannot continue as new to another domain"));}self.current_write(tx,request.shard_id,&new.execution_info,new.start_version,new.last_write_version,false)?;}else{self.current_write(tx,request.shard_id,&m.execution_info,m.start_version,m.last_write_version,false)?;}}}
            self.apply_mutation_tx(tx,request.shard_id,m)?;if let Some(new)=&request.new_workflow_snapshot{self.apply_snapshot_tx(tx,request.shard_id,new,false)?;}Ok(())})
    }
    pub fn conflict_resolve(&self,request:&ConflictResolveWorkflowExecutionRequest)->Result<(),PersistenceError>{
        validate_conflict(request)?;let reset=&request.reset_workflow_snapshot;
        self.backend.transaction(|tx|{self.lock_shard(tx,request.shard_id,request.range_id)?;
            match request.mode{ConflictResolveWorkflowMode::BypassCurrent=>self.assert_current(tx,request.shard_id,&reset.execution_info,&reset.execution_info.run_id,true)?,ConflictResolveWorkflowMode::UpdateCurrent=>{let previous=request.current_workflow_mutation.as_ref().map_or(&reset.execution_info.run_id,|m|&m.execution_info.run_id);self.assert_current(tx,request.shard_id,&reset.execution_info,previous,false)?;let target=request.new_workflow_snapshot.as_ref().unwrap_or(reset);self.current_write(tx,request.shard_id,&target.execution_info,target.start_version,target.last_write_version,false)?;}}
            self.apply_snapshot_tx(tx,request.shard_id,reset,true)?;if let Some(current)=&request.current_workflow_mutation{self.apply_mutation_tx(tx,request.shard_id,current)?;}if let Some(new)=&request.new_workflow_snapshot{self.apply_snapshot_tx(tx,request.shard_id,new,false)?;}Ok(())})
    }
    pub fn create(&self,request:&CreateWorkflowExecutionRequest)->Result<CurrentExecution,PersistenceError>{
        let snapshot=&request.new_workflow_snapshot;let info=&snapshot.execution_info;check_state(info.state)?;
        let invalid=match request.mode{CreateWorkflowMode::Zombie=>matches!(info.state,0..=2),_=>matches!(info.state,2|3)};
        if invalid{return Err(internal(format!("Invalid workflow create mode {}, state: {}",request.mode as i32,info.state)));}
        self.backend.transaction(|tx|{self.lock_shard(tx,request.shard_id,request.range_id)?;let current=self.current_tx(tx,&key(request.shard_id,info)?,true)?;
            if validate_create_transition(request.mode,info,&request.previous_run_id,request.previous_last_write_version,current.as_ref())?{self.current_write(tx,request.shard_id,info,snapshot.start_version,snapshot.last_write_version,current.is_none())?;}
            self.apply_snapshot_tx(tx,request.shard_id,snapshot,false)?;Ok(CurrentExecution{run_id:info.run_id.clone(),create_request_id:info.create_request_id.clone(),state:info.state,close_status:info.close_status,start_version:snapshot.start_version,last_write_version:snapshot.last_write_version})})
    }
    pub fn query(&self,sql:&str)->String{if self.backend.dialect()==crate::history_sql_queries::Dialect::MySql{return sql.into();}let mut n=0;sql.chars().map(|c|if c=='?'{n+=1;format!("${n}")}else{c.to_string()}).collect()}
    pub fn get_current(&self,shard:i32,domain:&str,workflow:&str)->Result<CurrentExecution,PersistenceError>{let k=vec![Value::I32(shard),uuid(domain)?,Value::Text(workflow.into())];self.backend.transaction(|tx|self.current_tx(tx,&k,false))?.ok_or_else(||PersistenceError::EntityNotExists("current execution not found".into()))}
    pub fn get(&self,shard:i32,domain:&str,workflow:&str,run:&str)->Result<WorkflowSnapshot,PersistenceError>{
        let k=key(shard,&WorkflowExecutionInfo{domain_id:domain.into(),workflow_id:workflow.into(),run_id:run.into(),..Default::default()})?;
        self.backend.transaction(|tx|{let rows=tx.query(self.backend.dialect().statement(Statement::GetExecution),&k,&[Column::I32,Column::Bytes,Column::Text,Column::Bytes,Column::I64,Column::I64,Column::Bytes,Column::Text])?;let row=rows.first().ok_or_else(||PersistenceError::EntityNotExists("workflow execution not found".into()))?;
            let key=crate::history_sql::WorkflowKey{shard_id:shard,domain_id:uuid::Uuid::parse_str(domain).map_err(internal)?.into_bytes(),workflow_id:workflow.into(),run_id:uuid::Uuid::parse_str(run).map_err(internal)?.into_bytes()};
            let (execution_info,start_version,version_histories,checksum)=decode_execution_info(&crate::history_sql::ExecutionRow{key,next_event_id:row[4].integer()?,last_write_version:row[5].integer()?,data:row[6].bytes()?,data_encoding:row[7].text()?})?;
            let mut snapshot=WorkflowSnapshot{execution_info,start_version,last_write_version:row[5].integer()?,version_histories,checksum,..Default::default()};
            for map in [WorkflowStateMap::Activity,WorkflowStateMap::Timer,WorkflowStateMap::ChildExecution,WorkflowStateMap::RequestCancel,WorkflowStateMap::Signal]{
                let mut columns=vec![if map==WorkflowStateMap::Timer{Column::Text}else{Column::I64},Column::Bytes,Column::Text];if map==WorkflowStateMap::Activity{columns.extend([Column::Bytes,Column::Timestamp]);}
                let rows=tx.query(&workflow_state_map_statement(self.backend.dialect(),map,WorkflowMapOperation::Select,0),&k,&columns)?;
                for row in rows{let value=DataBlob{data:row[1].bytes()?,encoding:row[2].text()?};if map==WorkflowStateMap::Timer{snapshot.timer_infos.push(KeyedBlob{key:row[0].text()?,value});}else{let entry=KeyedBlob{key:row[0].integer()?,value};match map{WorkflowStateMap::Activity=>snapshot.activity_infos.push(entry),WorkflowStateMap::ChildExecution=>snapshot.child_execution_infos.push(entry),WorkflowStateMap::RequestCancel=>snapshot.request_cancel_infos.push(entry),WorkflowStateMap::Signal=>snapshot.signal_infos.push(entry),_=>unreachable!()}}}
            }
            let signals=tx.query(self.backend.dialect().statement(Statement::SelectSignalRequested),&k,&[Column::Text])?;snapshot.signal_requested_ids=signals.iter().map(|r|r[0].text()).collect::<Result<_,_>>()?;Ok(snapshot)})
    }
}
fn ints(values:&[KeyedBlob<i64>])->Vec<(Value,DataBlob)>{values.iter().map(|v|(Value::I64(v.key),v.value.clone())).collect()}
fn strings(values:&[KeyedBlob<String>])->Vec<(Value,DataBlob)>{values.iter().map(|v|(Value::Text(v.key.clone()),v.value.clone())).collect()}
fn check_state(state:i32)->Result<(),PersistenceError>{if !(0..=4).contains(&state){Err(internal(format!("unknown workflow state: {state}")))}else{Ok(())}}
fn validate_state_close(state:i32,close:i32,create:bool)->Result<(),PersistenceError>{check_state(state)?;if (state==2)==(close==0)||!(0..=7).contains(&close)||(create&&state==2){return Err(internal(format!("invalid workflow state {state}, close status {close}")));}Ok(())}
fn validate_update(r:&UpdateWorkflowExecutionRequest)->Result<(),PersistenceError>{
 let state=r.update_workflow_mutation.execution_info.state;check_state(state)?;let new=r.new_workflow_snapshot.as_ref().map(|s|s.execution_info.state);if let Some(s)=new{check_state(s)?;}
 let invalid=match r.mode{UpdateWorkflowMode::UpdateCurrent=>match new{None=>state==3,Some(s)=>matches!(state,0|1)||matches!(s,2|3)},UpdateWorkflowMode::BypassCurrent=>matches!(state,0|1)||new.is_some_and(|s|matches!(s,0..=2)),UpdateWorkflowMode::IgnoreCurrent=>new.is_some()};
 if invalid{Err(internal(format!("Invalid workflow update mode {}, current state: {state}, new state: {new:?}",r.mode as i32)))}else{Ok(())}
}
fn validate_conflict(r:&ConflictResolveWorkflowExecutionRequest)->Result<(),PersistenceError>{
 let reset=r.reset_workflow_snapshot.execution_info.state;check_state(reset)?;let new=r.new_workflow_snapshot.as_ref().map(|s|s.execution_info.state);let current=r.current_workflow_mutation.as_ref().map(|s|s.execution_info.state);for s in [new,current].into_iter().flatten(){check_state(s)?;}
 let invalid=match r.mode{ConflictResolveWorkflowMode::UpdateCurrent=>current.is_some_and(|s|matches!(s,0|1))||reset==3||new.is_some_and(|s|matches!(reset,0|1)||matches!(s,2|3)),ConflictResolveWorkflowMode::BypassCurrent=>current.is_some()||matches!(reset,0|1)||new.is_some_and(|s|reset==3||matches!(s,0..=2))};
 if invalid{Err(internal(format!("Invalid workflow conflict resolve mode {}, reset state: {reset}, new state: {new:?}, current state: {current:?}",r.mode as i32)))}else{Ok(())}
}
