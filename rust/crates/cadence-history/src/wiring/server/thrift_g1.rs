//! Thrift dispatch for the group-1 History RPCs (StartWorkflowExecution,
//! SignalWithStartWorkflowExecution, SignalWorkflowExecution,
//! TerminateWorkflowExecution, RequestCancelWorkflowExecution,
//! ResetStickyTaskList, QueryWorkflow, DescribeWorkflowExecution,
//! ReapplyEvents, RefreshWorkflowTasks, ScheduleDecisionTask).
//!
//! Exception field ids follow the per-method result structs in
//! go/.gen/go/history/history.go. `dispatch_rich` returns the exact exception
//! body (e.g. DomainNotActive's five fields, ShardOwnershipLost's fields 10/20);
//! `dispatch` degrades that to the message-only `Outcome::Exception`.
//!
//! The caller is responsible for the rate-limit gate (as in tchannel.rs).
use super::tchannel::Outcome;
use super::ServerState;
use crate::engine::deps::HistoryError;
use cadence_api::errors::ShardOwnershipLostError;
use cadence_api::CadenceError;
use cadence_thrift::value::{FromThrift, Struct, ToThrift, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    BadRequest,
    Internal,
    EntityNotExists,
    ShardOwnershipLost,
    DomainNotActive,
    LimitExceeded,
    ServiceBusy,
    AlreadyStarted,
    AlreadyCompleted,
    CancelAlready,
    QueryFailed,
    ClientVersion,
}

use Kind::*;

type Table = &'static [(Kind, i16)];

const START: Table = &[(BadRequest, 1), (Internal, 2), (AlreadyStarted, 3), (ShardOwnershipLost, 4), (DomainNotActive, 5), (LimitExceeded, 6), (ServiceBusy, 7)];
const SIGNAL_WITH_START: Table = &[(BadRequest, 1), (Internal, 2), (ShardOwnershipLost, 3), (DomainNotActive, 4), (LimitExceeded, 5), (ServiceBusy, 6), (AlreadyStarted, 7)];
const SIGNAL: Table = &[(BadRequest, 1), (Internal, 2), (EntityNotExists, 3), (ShardOwnershipLost, 4), (DomainNotActive, 5), (ServiceBusy, 6), (LimitExceeded, 7), (AlreadyCompleted, 8)];
const TERMINATE: Table = &[(BadRequest, 1), (Internal, 2), (EntityNotExists, 3), (ShardOwnershipLost, 4), (DomainNotActive, 5), (LimitExceeded, 6), (ServiceBusy, 7), (AlreadyCompleted, 8)];
const CANCEL: Table = &[(BadRequest, 1), (Internal, 2), (EntityNotExists, 3), (ShardOwnershipLost, 4), (CancelAlready, 5), (DomainNotActive, 6), (LimitExceeded, 7), (ServiceBusy, 8), (AlreadyCompleted, 10)];
const STICKY: Table = &[(BadRequest, 1), (Internal, 2), (EntityNotExists, 3), (ShardOwnershipLost, 4), (LimitExceeded, 5), (ServiceBusy, 6), (AlreadyCompleted, 7)];
const QUERY: Table = &[(BadRequest, 1), (Internal, 2), (EntityNotExists, 3), (QueryFailed, 4), (LimitExceeded, 5), (ServiceBusy, 6), (ClientVersion, 7)];
const DESCRIBE: Table = &[(BadRequest, 1), (Internal, 2), (EntityNotExists, 3), (ShardOwnershipLost, 4), (LimitExceeded, 5), (ServiceBusy, 6)];
const REAPPLY: Table = &[(BadRequest, 1), (Internal, 2), (DomainNotActive, 3), (LimitExceeded, 4), (ServiceBusy, 5), (ShardOwnershipLost, 6), (EntityNotExists, 7)];
const REFRESH: Table = &[(BadRequest, 1), (Internal, 2), (DomainNotActive, 3), (ShardOwnershipLost, 4), (ServiceBusy, 5), (EntityNotExists, 6)];
const SCHEDULE: Table = &[(BadRequest, 1), (Internal, 2), (EntityNotExists, 3), (ShardOwnershipLost, 4), (DomainNotActive, 5), (LimitExceeded, 6), (ServiceBusy, 7), (AlreadyCompleted, 8)];

/// A thrift exception with its full body.
#[derive(Debug, Clone)]
pub struct Rich {
    pub name: &'static str,
    /// Field id inside the method's result struct.
    pub fid: i16,
    pub msg: String,
    /// Exception struct (the value of result field `fid`).
    pub body: Struct,
}

impl Rich {
    /// The whole result struct: `{fid: body}`.
    pub fn result_struct(&self) -> Struct {
        let mut s = Struct::default();
        s.fields.push((self.fid, Value::Struct(self.body.clone())));
        s
    }
}

pub enum G1Outcome {
    Ok(Struct),
    Exception(Rich),
}

fn kind_of(e: &HistoryError) -> Kind {
    match e {
        HistoryError::Cadence(c) => match &**c {
            CadenceError::BadRequest(_) => BadRequest,
            CadenceError::EntityNotExists(_) => EntityNotExists,
            CadenceError::DomainNotActive(_) => DomainNotActive,
            CadenceError::WorkflowAlreadyStarted(_) => AlreadyStarted,
            CadenceError::WorkflowAlreadyCompleted(_) => AlreadyCompleted,
            CadenceError::CancellationAlreadyRequested(_) => CancelAlready,
            CadenceError::QueryFailed(_) => QueryFailed,
            CadenceError::ServiceBusy(_) => ServiceBusy,
            CadenceError::LimitExceeded(_) => LimitExceeded,
            CadenceError::ShardOwnershipLost(_) => ShardOwnershipLost,
            CadenceError::ClientVersionNotSupported(_) => ClientVersion,
            _ => Internal,
        },
        _ => Internal,
    }
}

fn msg_body(message: &str) -> Struct {
    let mut s = Struct::default();
    s.fields.push((1, Value::Bytes(message.as_bytes().to_vec())));
    s
}

fn shard_lost_body(x: &ShardOwnershipLostError) -> Struct {
    // history.thrift ShardOwnershipLostError: 10 message, 20 owner.
    let mut s = Struct::default();
    s.fields.push((10, Value::Bytes(x.message.as_bytes().to_vec())));
    s
}

fn exception_name(k: Kind) -> &'static str {
    match k {
        BadRequest => "BadRequestError",
        Internal => "InternalServiceError",
        EntityNotExists => "EntityNotExistsError",
        ShardOwnershipLost => "ShardOwnershipLostError",
        DomainNotActive => "DomainNotActiveError",
        LimitExceeded => "LimitExceededError",
        ServiceBusy => "ServiceBusyError",
        AlreadyStarted => "WorkflowExecutionAlreadyStartedError",
        AlreadyCompleted => "WorkflowExecutionAlreadyCompletedError",
        CancelAlready => "CancellationAlreadyRequestedError",
        QueryFailed => "QueryFailedError",
        ClientVersion => "ClientVersionNotSupportedError",
    }
}

fn body_of(k: Kind, e: &HistoryError) -> Struct {
    if let HistoryError::Cadence(c) = e {
        match (&**c, k) {
            (CadenceError::EntityNotExists(x), EntityNotExists) => return x.to_thrift(),
            (CadenceError::DomainNotActive(x), DomainNotActive) => return x.to_thrift(),
            (CadenceError::WorkflowAlreadyStarted(x), AlreadyStarted) => return x.to_thrift(),
            (CadenceError::ServiceBusy(x), ServiceBusy) => return x.to_thrift(),
            (CadenceError::ClientVersionNotSupported(x), ClientVersion) => return x.to_thrift(),
            (CadenceError::ShardOwnershipLost(x), ShardOwnershipLost) => return shard_lost_body(x),
            _ => {}
        }
    }
    msg_body(&e.to_string())
}

/// Map a handler error onto the method's declared exceptions; undeclared
/// errors become `InternalServiceError` (field 2).
pub fn rich_exception(table: &[(Kind, i16)], e: &HistoryError) -> Rich {
    let mut k = kind_of(e);
    let fid = match table.iter().find(|(tk, _)| *tk == k) {
        Some((_, id)) => *id,
        None => {
            k = Internal;
            2
        }
    };
    let msg = match e {
        HistoryError::Cadence(c) => match &**c {
            CadenceError::ShardOwnershipLost(x) => x.message.clone(),
            _ => e.to_string(),
        },
        _ => e.to_string(),
    };
    Rich { name: exception_name(k), fid, msg, body: body_of(k, e) }
}

fn ok(body: Option<Struct>) -> G1Outcome {
    let mut s = Struct::default();
    if let Some(b) = body {
        s.fields.push((0, Value::Struct(b)));
    }
    G1Outcome::Ok(s)
}

fn finish<T>(r: Result<T, HistoryError>, table: Table, f: impl FnOnce(T) -> Option<Struct>) -> G1Outcome {
    match r {
        Ok(v) => ok(f(v)),
        Err(e) => G1Outcome::Exception(rich_exception(table, &e)),
    }
}

fn request<T: FromThrift>(args: &Struct) -> Result<T, String> {
    let s = match args.get(1) {
        Some(Value::Struct(s)) => s.clone(),
        _ => Struct::default(),
    };
    T::from_thrift(&s).map_err(|e| e.to_string())
}

/// Rich variant. `method` is the bare method name (last `::` segment).
pub async fn dispatch_rich(state: &ServerState, method: &str, args: &Struct) -> Option<Result<G1Outcome, String>> {
    let h = &state.handler;
    Some(match method {
        "StartWorkflowExecution" => match request(args) {
            Ok(r) => Ok(finish(h.start_workflow_execution(Some(&r)).await, START, |v| Some(v.to_thrift()))),
            Err(e) => Err(e),
        },
        "SignalWithStartWorkflowExecution" => match request(args) {
            Ok(r) => Ok(finish(h.signal_with_start_workflow_execution(Some(&r)).await, SIGNAL_WITH_START, |v| Some(v.to_thrift()))),
            Err(e) => Err(e),
        },
        "SignalWorkflowExecution" => match request(args) {
            Ok(r) => Ok(finish(h.signal_workflow_execution(Some(&r)).await, SIGNAL, |()| None)),
            Err(e) => Err(e),
        },
        "TerminateWorkflowExecution" => match request(args) {
            Ok(r) => Ok(finish(h.terminate_workflow_execution(Some(&r)).await, TERMINATE, |()| None)),
            Err(e) => Err(e),
        },
        "RequestCancelWorkflowExecution" => match request(args) {
            Ok(r) => Ok(finish(h.request_cancel_workflow_execution(Some(&r)).await, CANCEL, |()| None)),
            Err(e) => Err(e),
        },
        "ResetStickyTaskList" => match request(args) {
            Ok(r) => Ok(finish(h.reset_sticky_task_list(Some(&r)).await, STICKY, |v| Some(v.to_thrift()))),
            Err(e) => Err(e),
        },
        "QueryWorkflow" => match request(args) {
            Ok(r) => Ok(finish(h.query_workflow(Some(&r)).await, QUERY, |v| Some(v.to_thrift()))),
            Err(e) => Err(e),
        },
        "DescribeWorkflowExecution" => match request(args) {
            Ok(r) => Ok(finish(h.describe_workflow_execution(Some(&r)).await, DESCRIBE, |v| Some(v.to_thrift()))),
            Err(e) => Err(e),
        },
        "ReapplyEvents" => match request(args) {
            Ok(r) => Ok(finish(h.reapply_events(Some(&r)).await, REAPPLY, |()| None)),
            Err(e) => Err(e),
        },
        "RefreshWorkflowTasks" => match request(args) {
            Ok(r) => Ok(finish(h.refresh_workflow_tasks(Some(&r)).await, REFRESH, |()| None)),
            Err(e) => Err(e),
        },
        "ScheduleDecisionTask" => match request(args) {
            Ok(r) => Ok(finish(h.schedule_decision_task(Some(&r)).await, SCHEDULE, |()| None)),
            Err(e) => Err(e),
        },
        // ResetWorkflowExecution is NOT served: engine::types::
        // HistoryResetWorkflowExecutionRequest carries `failed_request`
        // (RespondDecisionTaskFailedRequest) instead of Go's `resetRequest`.
        _ => return None,
    })
}

/// `None` means the method is not handled here. Declared exceptions are
/// reduced to `Outcome::Exception(name, fid, message)`.
pub async fn dispatch(state: &ServerState, method: &str, args: &Struct) -> Option<Result<Outcome, String>> {
    let r = dispatch_rich(state, method, args).await?;
    Some(r.map(|o| match o {
        G1Outcome::Ok(s) => Outcome::Ok(s),
        G1Outcome::Exception(x) => Outcome::Exception(x.name, x.fid, x.msg),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::errors::{BadRequestError, DomainNotActiveError, LimitExceededError};

    fn he(c: CadenceError) -> HistoryError {
        HistoryError::Cadence(Box::new(c))
    }

    #[test]
    fn declared_ids() {
        let r = rich_exception(START, &he(BadRequestError::new("x").into()));
        assert_eq!((r.name, r.fid), ("BadRequestError", 1));
        let r = rich_exception(CANCEL, &he(LimitExceededError::new("x").into()));
        assert_eq!(r.fid, 7);
        let r = rich_exception(SIGNAL_WITH_START, &he(ShardOwnershipLostError::new("lost").into()));
        assert_eq!((r.name, r.fid), ("ShardOwnershipLostError", 3));
        assert_eq!(r.body.get(10), Some(&Value::Bytes(b"lost".to_vec())));
    }

    #[test]
    fn undeclared_becomes_internal() {
        // DomainNotActive is not declared by ResetStickyTaskList.
        let r = rich_exception(STICKY, &he(DomainNotActiveError::default().into()));
        assert_eq!((r.name, r.fid), ("InternalServiceError", 2));
    }

    #[test]
    fn domain_not_active_body_has_five_fields() {
        let r = rich_exception(SIGNAL, &he(DomainNotActiveError::default().into()));
        assert_eq!(r.fid, 5);
        assert_eq!(r.body.fields.len(), 5);
        assert_eq!(r.result_struct().fields[0].0, 5);
    }
}
