//! Encodes drained [`GeneratedTask`]s into persistence [`Task`] values
//! (Go `insertTransferTasks` / `insertTimerTasks` entries of
//! `WorkflowMutation.TasksByCategory`). Task ids are assigned by the shard
//! later (left 0 here, as in Go before `shard.AllocateTaskID`).

use std::collections::BTreeMap;

use cadence_persistence::history_contract::{self as pc, HistoryTaskCategory, Task};
use cadence_persistence::sqlblobs::{timer_task_to_struct, transfer_task_to_struct, Encoding};

use super::mutable_state_builder::TypedMutableStateError;
use super::mutable_state_task_generator::{GeneratedTask, TaskKind};

pub const TRANSFER_CATEGORY: HistoryTaskCategory = HistoryTaskCategory { category_type: 1, category_id: 1 };
pub const TIMER_CATEGORY: HistoryTaskCategory = HistoryTaskCategory { category_type: 2, category_id: 2 };

fn enc_err(e: impl std::fmt::Display) -> TypedMutableStateError {
    TypedMutableStateError::Internal(format!("encode task: {e}"))
}

/// Maps a generated transfer task to the persistence transfer task struct.
pub fn to_transfer_task(t: &GeneratedTask) -> Result<pc::TransferTask, TypedMutableStateError> {
    let mut o = pc::TransferTask {
        domain_id: t.workflow.domain_id.clone(),
        workflow_id: t.workflow.workflow_id.clone(),
        run_id: t.workflow.run_id.clone(),
        visibility_time_unix_nanos: t.visibility_timestamp_unix_nanos.unwrap_or_default(),
        version: t.version,
        ..Default::default()
    };
    match &t.kind {
        TaskKind::Decision { target_domain_id, schedule_id, original_task_list, original_task_list_kind } => {
            o.task_type = 0;
            o.target_domain_id = target_domain_id.clone();
            o.task_list = t.task_list.clone();
            o.schedule_id = *schedule_id;
            o.original_task_list = original_task_list.clone();
            o.original_task_list_kind = *original_task_list_kind;
        }
        TaskKind::Activity { target_domain_id, schedule_id } => {
            o.task_type = 1;
            o.target_domain_id = target_domain_id.clone();
            o.task_list = t.task_list.clone();
            o.schedule_id = *schedule_id;
        }
        TaskKind::CloseExecution => o.task_type = 2,
        TaskKind::CancelExecution { target_domain_id, target_workflow_id, target_run_id, child_only, initiated_id } => {
            o.task_type = 3;
            o.target_domain_id = target_domain_id.clone();
            o.target_workflow_id = target_workflow_id.clone();
            o.target_run_id = target_run_id.clone();
            o.target_child_workflow_only = *child_only;
            o.schedule_id = *initiated_id;
        }
        TaskKind::StartChild { target_domain_id, target_workflow_id, initiated_id } => {
            o.task_type = 4;
            o.target_domain_id = target_domain_id.clone();
            o.target_workflow_id = target_workflow_id.clone();
            o.schedule_id = *initiated_id;
        }
        TaskKind::SignalExecution { target_domain_id, target_workflow_id, target_run_id, child_only, initiated_id } => {
            o.task_type = 5;
            o.target_domain_id = target_domain_id.clone();
            o.target_workflow_id = target_workflow_id.clone();
            o.target_run_id = target_run_id.clone();
            o.target_child_workflow_only = *child_only;
            o.schedule_id = *initiated_id;
        }
        TaskKind::RecordWorkflowStarted => o.task_type = 6,
        TaskKind::ResetWorkflow => o.task_type = 7,
        TaskKind::UpsertWorkflowSearchAttributes => o.task_type = 8,
        other => return Err(enc_err(format!("not a transfer task: {other:?}"))),
    }
    // The thrift blob requires a UUID; tasks without a target use their own domain.
    if o.target_domain_id.is_empty() {
        o.target_domain_id = o.domain_id.clone();
    }
    Ok(o)
}

/// Maps a generated timer task to the persistence timer task struct.
pub fn to_timer_task(t: &GeneratedTask) -> Result<pc::TimerTask, TypedMutableStateError> {
    let mut o = pc::TimerTask {
        domain_id: t.workflow.domain_id.clone(),
        workflow_id: t.workflow.workflow_id.clone(),
        run_id: t.workflow.run_id.clone(),
        visibility_timestamp_unix_nanos: t.visibility_timestamp_unix_nanos.unwrap_or_default(),
        version: t.version,
        ..Default::default()
    };
    match &t.kind {
        TaskKind::DecisionTimeout { timeout_type, event_id, schedule_attempt } => {
            o.task_type = 0;
            o.timeout_type = *timeout_type as i32;
            o.event_id = *event_id;
            o.schedule_attempt = *schedule_attempt;
            o.task_list = t.task_list.clone();
        }
        TaskKind::ActivityTimeout { timeout_type, event_id, attempt } => {
            o.task_type = 1;
            o.timeout_type = *timeout_type as i32;
            o.event_id = *event_id;
            o.schedule_attempt = *attempt;
            o.task_list = t.task_list.clone();
        }
        TaskKind::UserTimer { event_id } => {
            o.task_type = 2;
            o.event_id = *event_id;
        }
        TaskKind::WorkflowTimeout => o.task_type = 3,
        TaskKind::DeleteHistoryEvent => o.task_type = 4,
        TaskKind::ActivityRetry { event_id, attempt } => {
            o.task_type = 5;
            o.event_id = *event_id;
            o.schedule_attempt = *attempt;
        }
        TaskKind::WorkflowBackoff { timeout_type } => {
            o.task_type = 6;
            o.timeout_type = *timeout_type;
        }
        other => return Err(enc_err(format!("not a timer task: {other:?}"))),
    }
    Ok(o)
}

fn blob(data: Result<Vec<u8>, impl std::fmt::Display>) -> Result<pc::DataBlob, TypedMutableStateError> {
    data.map(|data| pc::DataBlob { encoding: "thriftrw".to_string(), data }).map_err(enc_err)
}

/// Encodes drained transfer/timer tasks into per-category persistence tasks
/// (empty categories are omitted).
pub fn encode_generated_tasks(
    transfer: &[GeneratedTask],
    timer: &[GeneratedTask],
) -> Result<BTreeMap<HistoryTaskCategory, Vec<Task>>, TypedMutableStateError> {
    let mut out: BTreeMap<HistoryTaskCategory, Vec<Task>> = BTreeMap::new();
    for g in transfer {
        let t = to_transfer_task(g)?;
        let s = transfer_task_to_struct(&t).map_err(enc_err)?;
        out.entry(TRANSFER_CATEGORY).or_default().push(Task {
            visibility_timestamp_unix_nanos: t.visibility_time_unix_nanos,
            task_type: t.task_type,
            task_id: 0,
            data: blob(Encoding::ThriftRW.encode(&s))?,
        });
    }
    for g in timer {
        let t = to_timer_task(g)?;
        let s = timer_task_to_struct(&t).map_err(enc_err)?;
        out.entry(TIMER_CATEGORY).or_default().push(Task {
            visibility_timestamp_unix_nanos: t.visibility_timestamp_unix_nanos,
            task_type: t.task_type,
            task_id: 0,
            data: blob(Encoding::ThriftRW.encode(&s))?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::WorkflowExecutionKey;

    fn g(kind: TaskKind) -> GeneratedTask {
        GeneratedTask {
            workflow: WorkflowExecutionKey { domain_id: "11111111-1111-1111-1111-111111111111".into(), workflow_id: "w".into(), run_id: "22222222-2222-2222-2222-222222222222".into() },
            version: 3,
            visibility_timestamp_unix_nanos: Some(7),
            task_list: "tl".into(),
            kind,
        }
    }

    #[test]
    fn encodes_by_category() {
        let m = encode_generated_tasks(&[g(TaskKind::CloseExecution)], &[g(TaskKind::DeleteHistoryEvent), g(TaskKind::UserTimer { event_id: 5 })]).unwrap();
        assert_eq!(m[&TRANSFER_CATEGORY].len(), 1);
        assert_eq!(m[&TRANSFER_CATEGORY][0].task_type, 2);
        assert_eq!(m[&TIMER_CATEGORY].len(), 2);
        assert_eq!(m[&TIMER_CATEGORY][1].task_type, 2);
        assert_eq!(m[&TIMER_CATEGORY][0].visibility_timestamp_unix_nanos, 7);
    }

    #[test]
    fn rejects_wrong_kind() {
        assert!(to_transfer_task(&g(TaskKind::WorkflowTimeout)).is_err());
        assert!(to_timer_task(&g(TaskKind::CloseExecution)).is_err());
    }
}
