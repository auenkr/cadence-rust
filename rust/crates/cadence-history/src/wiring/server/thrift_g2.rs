//! Thrift dispatch for the group-2 History RPCs (history.thrift ids).
use super::tchannel::Outcome;
use super::{errors, ServerState};
use crate::engine::deps::HistoryError;
use cadence_api::CadenceError;
use cadence_thrift::value::{FromThrift, Struct, ToThrift, Value};

const GET_POLL: &[(&str, i16)] = &[
    ("BadRequestError", 1), ("InternalServiceError", 2), ("EntityNotExistsError", 3),
    ("ShardOwnershipLostError", 4), ("LimitExceededError", 5), ("ServiceBusyError", 6),
    ("CurrentBranchChangedError", 7),
];
const STARTED: &[(&str, i16)] = &[
    ("BadRequestError", 1), ("InternalServiceError", 2), ("EventAlreadyStartedError", 3),
    ("EntityNotExistsError", 4), ("ShardOwnershipLostError", 5), ("DomainNotActiveError", 6),
    ("LimitExceededError", 7), ("ServiceBusyError", 8), ("WorkflowExecutionAlreadyCompletedError", 9),
];
const STANDARD: &[(&str, i16)] = &[
    ("BadRequestError", 1), ("InternalServiceError", 2), ("EntityNotExistsError", 3),
    ("ShardOwnershipLostError", 4), ("DomainNotActiveError", 5), ("LimitExceededError", 6),
    ("ServiceBusyError", 7), ("WorkflowExecutionAlreadyCompletedError", 8),
];
const DESCRIBE: &[(&str, i16)] = &[
    ("BadRequestError", 1), ("InternalServiceError", 2), ("EntityNotExistsError", 3),
    ("AccessDeniedError", 4), ("ShardOwnershipLostError", 5), ("LimitExceededError", 6),
];

fn s_bytes(v: &str) -> Value {
    Value::Bytes(v.as_bytes().to_vec())
}

/// Classify a handler error to (declared exception name, message, rich struct).
fn classify(e: &HistoryError) -> (&'static str, String, Struct) {
    let mut st = Struct::default();
    let (name, msg): (&'static str, String) = match e {
        HistoryError::EventAlreadyStarted(x) => ("EventAlreadyStartedError", x.to_string()),
        HistoryError::CurrentBranchChanged { message, current_branch_token } => {
            st.fields.push((10, s_bytes(message)));
            st.fields.push((20, Value::Bytes(current_branch_token.clone())));
            ("CurrentBranchChangedError", message.clone())
        }
        HistoryError::PersistenceShardOwnershipLost(m) => ("ShardOwnershipLostError", m.clone()),
        HistoryError::Cadence(c) => match &**c {
            CadenceError::WorkflowAlreadyCompleted(x) => ("WorkflowExecutionAlreadyCompletedError", x.to_string()),
            CadenceError::EntityNotExists(x) => {
                st.fields.push((1, s_bytes(&x.message)));
                st.fields.push((2, s_bytes(&x.current_cluster)));
                st.fields.push((3, s_bytes(&x.active_cluster)));
                st.fields.push((4, Value::List(x.active_clusters.iter().map(|s| s_bytes(s)).collect())));
                ("EntityNotExistsError", x.to_string())
            }
            CadenceError::DomainNotActive(x) => {
                st.fields.push((1, s_bytes(&x.message)));
                st.fields.push((2, s_bytes(&x.domain_name)));
                st.fields.push((3, s_bytes(&x.current_cluster)));
                st.fields.push((4, s_bytes(&x.active_cluster)));
                st.fields.push((5, Value::List(x.active_clusters.iter().map(|s| s_bytes(s)).collect())));
                ("DomainNotActiveError", x.to_string())
            }
            CadenceError::ShardOwnershipLost(x) => ("ShardOwnershipLostError", x.to_string()),
            _ => errors::thrift_exception(e),
        },
        _ => errors::thrift_exception(e),
    };
    (name, msg, st)
}

fn fail(table: &[(&'static str, i16)], e: &HistoryError) -> Outcome {
    let (mut name, mut msg, mut st) = classify(e);
    if !table.iter().any(|(n, _)| *n == name) {
        name = "InternalServiceError";
        msg = e.to_string();
        st = Struct::default();
    }
    let fid = table.iter().find(|(n, _)| *n == name).map(|(_, i)| *i).unwrap_or(2);
    // Declared exceptions with a rich body must carry the application-error flag (`Outcome::Rich`),
    // otherwise the caller decodes the exception struct as an empty successful reply.
    match name {
        "EntityNotExistsError" | "DomainNotActiveError" | "CurrentBranchChangedError" => {
            let mut r = Struct::default();
            r.fields.push((fid, Value::Struct(st)));
            Outcome::Rich(name, msg, r)
        }
        "ShardOwnershipLostError" => {
            let mut inner = Struct::default();
            inner.fields.push((10, s_bytes(&msg)));
            inner.fields.push((20, s_bytes("")));
            let mut r = Struct::default();
            r.fields.push((fid, Value::Struct(inner)));
            Outcome::Rich(name, msg, r)
        }
        _ => Outcome::Exception(name, fid, msg),
    }
}

fn ok_body(body: Option<Struct>) -> Outcome {
    let mut s = Struct::default();
    if let Some(b) = body {
        s.fields.push((0, Value::Struct(b)));
    }
    Outcome::Ok(s)
}

fn done<T: ToThrift>(table: &[(&'static str, i16)], r: Result<T, HistoryError>) -> Outcome {
    match r {
        Ok(v) => ok_body(Some(v.to_thrift())),
        Err(e) => fail(table, &e),
    }
}
fn done_void(table: &[(&'static str, i16)], r: Result<(), HistoryError>) -> Outcome {
    match r {
        Ok(()) => ok_body(None),
        Err(e) => fail(table, &e),
    }
}

pub async fn dispatch(state: &ServerState, method: &str, args: &Struct) -> Option<Result<Outcome, String>> {
    let table: &[(&'static str, i16)] = match method {
        "GetMutableState" | "PollMutableState" => GET_POLL,
        "RecordDecisionTaskStarted" | "RecordActivityTaskStarted" => STARTED,
        "RespondDecisionTaskCompleted" | "RespondDecisionTaskFailed" | "RespondActivityTaskCompleted"
        | "RespondActivityTaskFailed" | "RespondActivityTaskCanceled" | "RecordActivityTaskHeartbeat"
        | "RecordChildExecutionCompleted" | "RemoveSignalMutableState" => STANDARD,
        "DescribeMutableState" => DESCRIBE,
        _ => return None,
    };
    if !state.limiter.allow() {
        return Some(Ok(fail(table, &errors::busy())));
    }
    let req = match args.get(1) {
        Some(Value::Struct(s)) => s.clone(),
        _ => Struct::default(),
    };
    let dec = |e: cadence_thrift::Error| e.to_string();
    let h = &state.handler;
    macro_rules! call {
        ($ty:ty, $m:ident, void) => {{
            let r = match <$ty>::from_thrift(&req) { Ok(r) => r, Err(e) => return Some(Err(dec(e))) };
            done_void(table, h.$m(Some(&r)).await)
        }};
        ($ty:ty, $m:ident) => {{
            let r = match <$ty>::from_thrift(&req) { Ok(r) => r, Err(e) => return Some(Err(dec(e))) };
            done(table, h.$m(Some(&r)).await)
        }};
    }
    use crate::engine::types::*;
    let out = match method {
        "GetMutableState" => call!(GetMutableStateRequest, get_mutable_state),
        "PollMutableState" => call!(PollMutableStateRequest, poll_mutable_state),
        "DescribeMutableState" => call!(DescribeMutableStateRequest, describe_mutable_state),
        "RecordDecisionTaskStarted" => call!(RecordDecisionTaskStartedRequest, record_decision_task_started),
        "RecordActivityTaskStarted" => call!(RecordActivityTaskStartedRequest, record_activity_task_started),
        "RespondDecisionTaskCompleted" => {
            call!(HistoryRespondDecisionTaskCompletedRequest, respond_decision_task_completed)
        }
        "RecordActivityTaskHeartbeat" => {
            call!(HistoryRecordActivityTaskHeartbeatRequest, record_activity_task_heartbeat)
        }
        "RespondDecisionTaskFailed" => call!(HistoryRespondDecisionTaskFailedRequest, respond_decision_task_failed, void),
        "RespondActivityTaskCompleted" => {
            call!(HistoryRespondActivityTaskCompletedRequest, respond_activity_task_completed, void)
        }
        "RespondActivityTaskFailed" => call!(HistoryRespondActivityTaskFailedRequest, respond_activity_task_failed, void),
        "RespondActivityTaskCanceled" => {
            call!(HistoryRespondActivityTaskCanceledRequest, respond_activity_task_canceled, void)
        }
        "RecordChildExecutionCompleted" => {
            call!(RecordChildExecutionCompletedRequest, record_child_execution_completed, void)
        }
        "RemoveSignalMutableState" => call!(RemoveSignalMutableStateRequest, remove_signal_mutable_state, void),
        _ => return None,
    };
    Some(Ok(out))
}
