//! Thrift dispatch for the group-3 History RPCs (replication, DLQ, failover,
//! cross-cluster, ratelimit, host description). Port of the matching methods in
//! go/service/history/wrappers/thrift/thrift_handler.go; exception sets and
//! field ids come from go/idls/thrift/history.thrift.
use super::tchannel::Outcome;
use super::{errors, ServerState};
use crate::engine::deps::HistoryError;
use cadence_api::errors::CadenceError;
use cadence_thrift::value::{FromThrift, Struct, ToThrift, Value};

/// Declared exceptions of one method: (exception name, result field id).
type Declared = &'static [(&'static str, i16)];

const BR: &str = "BadRequestError";
const IS: &str = "InternalServiceError";
const ENE: &str = "EntityNotExistsError";
const SOL: &str = "ShardOwnershipLostError";
const LE: &str = "LimitExceededError";
const RT: &str = "RetryTaskV2Error";
const SB: &str = "ServiceBusyError";
const AD: &str = "AccessDeniedError";
const CVNS: &str = "ClientVersionNotSupportedError";

const D_REPLICATE_EVENTS_V2: Declared = &[(BR, 1), (IS, 2), (ENE, 3), (SOL, 4), (LE, 5), (RT, 6), (SB, 7)];
const D_SYNC_SHARD_STATUS: Declared = &[(BR, 1), (IS, 2), (SOL, 4), (LE, 5), (SB, 6)];
const D_SYNC_ACTIVITY: Declared = &[(BR, 1), (IS, 2), (ENE, 3), (SOL, 4), (SB, 5), (RT, 7)];
const D_DESCRIBE_HOST: Declared = &[(BR, 1), (IS, 2), (AD, 3)];
const D_GET_REPLICATION: Declared = &[(BR, 1), (IS, 2), (LE, 3), (SB, 4), (CVNS, 5)];
const D_GET_DLQ_REPLICATION: Declared = &[(BR, 1), (IS, 2), (SB, 3), (ENE, 4)];
const D_DLQ: Declared = &[(BR, 1), (IS, 2), (SB, 3), (ENE, 4), (SOL, 5)];
const D_NOTIFY_FAILOVER: Declared = &[(BR, 1), (IS, 2), (SB, 3)];
const D_GET_CROSS_CLUSTER: Declared = &[(BR, 1), (IS, 2), (SB, 3)];
const D_RESPOND_CROSS_CLUSTER: Declared = &[(BR, 1), (IS, 2), (SB, 3), (SOL, 4)];
const D_GET_FAILOVER_INFO: Declared = &[(IS, 1), (SB, 2), (SOL, 3), (ENE, 4)];
const D_RATELIMIT: Declared = &[(BR, 1), (IS, 2), (SB, 3), (SOL, 4)];

/// Thrift names of the methods served by this module.
pub const METHODS: &[&str] = &[
    "ReplicateEventsV2",
    "SyncActivity",
    "SyncShardStatus",
    "NotifyFailoverMarkers",
    "GetReplicationMessages",
    "GetDLQReplicationMessages",
    "ReadDLQMessages",
    "PurgeDLQMessages",
    "MergeDLQMessages",
    "GetFailoverInfo",
    "RatelimitUpdate",
    "DescribeHistoryHost",
    "GetCrossClusterTasks",
    "RespondCrossClusterTasksCompleted",
];

fn msg_struct(field: i16, msg: &str) -> Struct {
    Struct { fields: vec![(field, Value::Bytes(msg.as_bytes().to_vec()))] }
}

/// Map a handler error to (declared exception name, exception struct). Errors
/// without a typed thrift exception map to InternalServiceError.
pub fn classify(e: &HistoryError) -> (&'static str, Struct) {
    if let HistoryError::Cadence(c) = e {
        match &**c {
            CadenceError::BadRequest(x) => return (BR, msg_struct(1, &x.message)),
            CadenceError::Internal(x) => return (IS, msg_struct(1, &x.message)),
            CadenceError::AccessDenied(x) => return (AD, msg_struct(1, &x.message)),
            CadenceError::LimitExceeded(x) => return (LE, msg_struct(1, &x.message)),
            CadenceError::EntityNotExists(x) => return (ENE, x.to_thrift()),
            CadenceError::ServiceBusy(x) => return (SB, x.to_thrift()),
            CadenceError::ShardOwnershipLost(x) => return (SOL, msg_struct(10, &x.message)),
            CadenceError::RetryTaskV2(x) => return (RT, x.to_thrift()),
            CadenceError::ClientVersionNotSupported(x) => return (CVNS, x.to_thrift()),
            _ => {}
        }
    }
    (IS, msg_struct(1, &errors::thrift_exception(e).1))
}

/// Build the call outcome for a handler error, using the method's declared
/// exception table (undeclared kinds fall back to InternalServiceError).
pub fn fail(e: &HistoryError, declared: Declared) -> Outcome {
    let (mut name, mut ex) = classify(e);
    if !declared.iter().any(|(n, _)| *n == name) {
        name = IS;
        ex = msg_struct(1, &errors::thrift_exception(e).1);
    }
    let fid = declared.iter().find(|(n, _)| *n == name).map(|(_, i)| *i).unwrap_or(2);
    exception_outcome(name, fid, ex)
}

fn exception_outcome(name: &'static str, fid: i16, ex: Struct) -> Outcome {
    // Message-only exceptions use the transport's declared-exception path
    // (sets the application-error headers); richer exceptions are encoded
    // directly into the result struct.
    if ex.fields.len() == 1 && ex.fields[0].0 == 1 {
        if let Value::Bytes(b) = &ex.fields[0].1 {
            return Outcome::Exception(name, fid, String::from_utf8_lossy(b).into_owned());
        }
    }
    Outcome::Ok(Struct { fields: vec![(fid, Value::Struct(ex))] })
}

fn ok(body: Option<Struct>) -> Outcome {
    let mut s = Struct::default();
    if let Some(b) = body {
        s.fields.push((0, Value::Struct(b)));
    }
    Outcome::Ok(s)
}

macro_rules! unary {
    ($state:expr, $req:expr, $decl:expr, $ty:ty, $call:ident, |$r:ident| $body:expr) => {{
        let r = <$ty>::from_thrift($req).map_err(|e| e.to_string())?;
        match $state.handler.$call(Some(&r)).await {
            Ok($r) => {
                let _ = &$r;
                ok($body)
            }
            Err(e) => fail(&e, $decl),
        }
    }};
}

async fn run(state: &ServerState, method: &str, req: &Struct) -> Result<Outcome, String> {
    use crate::engine::types as t;
    use cadence_api::admin as a;
    Ok(match method {
        "ReplicateEventsV2" => unary!(state, req, D_REPLICATE_EVENTS_V2, t::ReplicateEventsV2Request, replicate_events_v2, |_x| None),
        "SyncActivity" => unary!(state, req, D_SYNC_ACTIVITY, t::SyncActivityRequest, sync_activity, |_x| None),
        "SyncShardStatus" => unary!(state, req, D_SYNC_SHARD_STATUS, t::SyncShardStatusRequest, sync_shard_status, |_x| None),
        "NotifyFailoverMarkers" => unary!(state, req, D_NOTIFY_FAILOVER, t::NotifyFailoverMarkersRequest, notify_failover_markers, |_x| None),
        "GetReplicationMessages" => unary!(state, req, D_GET_REPLICATION, a::GetReplicationMessagesRequest, get_replication_messages, |x| Some(x.to_thrift())),
        "GetDLQReplicationMessages" => unary!(state, req, D_GET_DLQ_REPLICATION, a::GetDLQReplicationMessagesRequest, get_dlq_replication_messages, |x| Some(x.to_thrift())),
        "ReadDLQMessages" => unary!(state, req, D_DLQ, a::ReadDLQMessagesRequest, read_dlq_messages, |x| Some(x.to_thrift())),
        "PurgeDLQMessages" => unary!(state, req, D_DLQ, a::PurgeDLQMessagesRequest, purge_dlq_messages, |_x| None),
        "MergeDLQMessages" => unary!(state, req, D_DLQ, a::MergeDLQMessagesRequest, merge_dlq_messages, |x| Some(x.to_thrift())),
        "GetFailoverInfo" => unary!(state, req, D_GET_FAILOVER_INFO, t::GetFailoverInfoRequest, get_failover_info, |x| Some(x.to_thrift())),
        "RatelimitUpdate" => unary!(state, req, D_RATELIMIT, t::RatelimitUpdateRequest, ratelimit_update, |x| Some(x.to_thrift())),
        "DescribeHistoryHost" => unary!(state, req, D_DESCRIBE_HOST, a::DescribeHistoryHostRequest, describe_history_host, |x| Some(x.to_thrift())),
        "GetCrossClusterTasks" => unary!(state, req, D_GET_CROSS_CLUSTER, a::GetCrossClusterTasksRequest, get_cross_cluster_tasks, |x| Some(x.to_thrift())),
        "RespondCrossClusterTasksCompleted" => unary!(state, req, D_RESPOND_CROSS_CLUSTER, a::RespondCrossClusterTasksCompletedRequest, respond_cross_cluster_tasks_completed, |x| Some(x.to_thrift())),
        other => return Err(format!("HistoryService::{other} is not a group-3 method")),
    })
}

/// Dispatch a group-3 call; `None` when `method` (or `Service::Method`) is not
/// one of this module's methods.
pub async fn dispatch(state: &ServerState, method: &str, args: &Struct) -> Option<Result<Outcome, String>> {
    let name = method.rsplit("::").next().unwrap_or(method);
    if !METHODS.contains(&name) {
        return None;
    }
    if !state.limiter.allow() {
        let busy = errors::busy();
        let decl = declared_for(name);
        return Some(Ok(fail(&busy, decl)));
    }
    let req = match args.get(1) {
        Some(Value::Struct(s)) => s.clone(),
        _ => Struct::default(),
    };
    Some(run(state, name, &req).await)
}

fn declared_for(name: &str) -> Declared {
    match name {
        "ReplicateEventsV2" => D_REPLICATE_EVENTS_V2,
        "SyncActivity" => D_SYNC_ACTIVITY,
        "SyncShardStatus" => D_SYNC_SHARD_STATUS,
        "NotifyFailoverMarkers" => D_NOTIFY_FAILOVER,
        "GetReplicationMessages" => D_GET_REPLICATION,
        "GetDLQReplicationMessages" => D_GET_DLQ_REPLICATION,
        "ReadDLQMessages" | "PurgeDLQMessages" | "MergeDLQMessages" => D_DLQ,
        "GetFailoverInfo" => D_GET_FAILOVER_INFO,
        "RatelimitUpdate" => D_RATELIMIT,
        "DescribeHistoryHost" => D_DESCRIBE_HOST,
        "GetCrossClusterTasks" => D_GET_CROSS_CLUSTER,
        "RespondCrossClusterTasksCompleted" => D_RESPOND_CROSS_CLUSTER,
        _ => D_NOTIFY_FAILOVER,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::errors::{EntityNotExistsError, ShardOwnershipLostError};

    #[test]
    fn declared_tables_cover_methods() {
        for m in METHODS {
            assert!(declared_for(m).iter().any(|(n, _)| *n == IS), "{m}");
        }
    }

    #[test]
    fn bad_request_uses_declared_id() {
        match fail(&HistoryError::bad_request("x"), D_SYNC_ACTIVITY) {
            Outcome::Exception(n, id, m) => assert_eq!((n, id, m.as_str()), (BR, 1, "x")),
            _ => panic!("expected exception"),
        }
    }

    #[test]
    fn shard_ownership_lost_is_encoded_with_message_field_10() {
        let e: HistoryError = ShardOwnershipLostError::new("lost").into();
        match fail(&e, D_SYNC_SHARD_STATUS) {
            Outcome::Ok(s) => {
                let Some(Value::Struct(ex)) = s.get(4) else { panic!("missing field 4") };
                assert_eq!(ex.get(10), Some(&Value::Bytes(b"lost".to_vec())));
            }
            _ => panic!("expected result struct"),
        }
    }

    #[test]
    fn undeclared_error_falls_back_to_internal() {
        // GetFailoverInfo does not declare BadRequestError.
        match fail(&HistoryError::bad_request("x"), D_GET_FAILOVER_INFO) {
            Outcome::Exception(n, id, _) => assert_eq!((n, id), (IS, 1)),
            _ => panic!("expected exception"),
        }
    }

    #[test]
    fn entity_not_exists_goes_to_declared_field() {
        let e: HistoryError = EntityNotExistsError::new("nf").into();
        match fail(&e, D_GET_DLQ_REPLICATION) {
            Outcome::Ok(s) => assert!(matches!(s.get(4), Some(Value::Struct(_)))),
            _ => panic!("expected result struct"),
        }
    }
}
