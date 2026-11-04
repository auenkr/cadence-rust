//! Thrift converters for the group-2 History request/response types
//! (history.thrift field ids). Impls are on history-local `engine::types`;
//! foreign types (`VersionHistories`) use plain functions.
use crate::engine::types::{
    ActivityLocalDispatchInfo, DescribeMutableStateRequest, DescribeMutableStateResponse,
    GetMutableStateRequest, GetMutableStateResponse, HistoryRecordActivityTaskHeartbeatRequest,
    HistoryRespondActivityTaskCanceledRequest, HistoryRespondActivityTaskCompletedRequest,
    HistoryRespondActivityTaskFailedRequest, HistoryRespondDecisionTaskCompletedRequest,
    HistoryRespondDecisionTaskCompletedResponse, HistoryRespondDecisionTaskFailedRequest,
    PollMutableStateRequest, PollMutableStateResponse, RecordActivityTaskStartedRequest,
    RecordActivityTaskStartedResponse, RecordChildExecutionCompletedRequest,
    RecordDecisionTaskStartedRequest, RecordDecisionTaskStartedResponse,
    RemoveSignalMutableStateRequest, TransientDecisionInfo,
};
use cadence_api::tasks::WorkflowQuery;
use cadence_api::types::{VersionHistories, VersionHistory};
use cadence_thrift::value::{Fields, FromThrift, Struct, ToThrift, Value};
use cadence_thrift::Result;

fn ps(f: &mut Fields, id: i16, v: &str) {
    f.fields.push((id, Value::Bytes(v.as_bytes().to_vec())));
}
fn pi64(f: &mut Fields, id: i16, v: i64) {
    f.fields.push((id, Value::I64(v)));
}
fn pi32(f: &mut Fields, id: i16, v: i32) {
    f.fields.push((id, Value::I32(v)));
}
fn pb(f: &mut Fields, id: i16, v: bool) {
    f.fields.push((id, Value::Bool(v)));
}

/// `shared.VersionHistories` (10 currentVersionHistoryIndex, 20 histories).
pub fn version_histories_from_thrift(s: &Struct) -> Result<VersionHistories> {
    Ok(VersionHistories {
        current_version_history_index: s.opt_i32(10)?.unwrap_or_default(),
        histories: s.opt_list::<VersionHistory>(20)?.unwrap_or_default(),
    })
}

pub fn version_histories_to_thrift(v: &VersionHistories) -> Struct {
    let mut f = Fields::new();
    pi32(&mut f, 10, v.current_version_history_index);
    f.olist(20, &v.histories);
    f.finish()
}

fn vh_opt(s: &Struct, id: i16) -> Result<Option<VersionHistories>> {
    match s.get(id) {
        Some(Value::Struct(x)) => Ok(Some(version_histories_from_thrift(x)?)),
        _ => Ok(None),
    }
}

fn put_vh(f: &mut Fields, id: i16, v: &Option<VersionHistories>) {
    if let Some(v) = v {
        f.fields.push((id, Value::Struct(version_histories_to_thrift(v))));
    }
}

impl FromThrift for DescribeMutableStateRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self { domain_uuid: s.req_string(10, "")?, execution: s.opt_struct(20)? })
    }
}
impl ToThrift for DescribeMutableStateRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.domain_uuid);
        f.ostruct(20, self.execution.as_ref());
        f.finish()
    }
}
impl FromThrift for DescribeMutableStateResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            mutable_state_in_cache: s.req_string(30, "")?,
            mutable_state_in_database: s.req_string(40, "")?,
        })
    }
}
impl ToThrift for DescribeMutableStateResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 30, &self.mutable_state_in_cache);
        ps(&mut f, 40, &self.mutable_state_in_database);
        f.finish()
    }
}

impl FromThrift for GetMutableStateRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            execution: s.opt_struct(20)?,
            expected_next_event_id: s.i64_or_zero(30)?,
            current_branch_token: s.bytes_or_empty(40)?,
            version_history_item: s.opt_struct(50)?,
        })
    }
}
impl ToThrift for GetMutableStateRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.domain_uuid);
        f.ostruct(20, self.execution.as_ref());
        pi64(&mut f, 30, self.expected_next_event_id);
        f.sbytes(40, &self.current_branch_token);
        f.ostruct(50, self.version_history_item.as_ref());
        f.finish()
    }
}

impl FromThrift for GetMutableStateResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            execution: s.opt_struct(10)?,
            workflow_type: s.opt_struct(20)?,
            next_event_id: s.i64_or_zero(30)?,
            previous_started_event_id: s.opt_i64(35)?,
            last_first_event_id: s.i64_or_zero(40)?,
            task_list: s.opt_struct(50)?,
            sticky_task_list: s.opt_struct(60)?,
            client_library_version: s.req_string(70, "")?,
            client_feature_version: s.req_string(80, "")?,
            client_impl: s.req_string(90, "")?,
            is_workflow_running: s.bool_or_false(100)?,
            sticky_task_list_schedule_to_start_timeout: s.opt_i32(110)?,
            event_store_version: s.opt_i32(120)?.unwrap_or_default(),
            current_branch_token: s.bytes_or_empty(130)?,
            workflow_state: s.opt_i32(150)?,
            workflow_close_state: s.opt_i32(160)?,
            version_histories: vh_opt(s, 170)?,
            is_sticky_task_list_enabled: s.bool_or_false(180)?,
            history_size: s.i64_or_zero(190)?,
        })
    }
}
impl ToThrift for GetMutableStateResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.execution.as_ref());
        f.ostruct(20, self.workflow_type.as_ref());
        pi64(&mut f, 30, self.next_event_id);
        f.oi64(35, self.previous_started_event_id);
        pi64(&mut f, 40, self.last_first_event_id);
        f.ostruct(50, self.task_list.as_ref());
        f.ostruct(60, self.sticky_task_list.as_ref());
        ps(&mut f, 70, &self.client_library_version);
        ps(&mut f, 80, &self.client_feature_version);
        ps(&mut f, 90, &self.client_impl);
        pb(&mut f, 100, self.is_workflow_running);
        f.oi32(110, self.sticky_task_list_schedule_to_start_timeout);
        pi32(&mut f, 120, self.event_store_version);
        f.sbytes(130, &self.current_branch_token);
        f.oi32(150, self.workflow_state);
        f.oi32(160, self.workflow_close_state);
        put_vh(&mut f, 170, &self.version_histories);
        pb(&mut f, 180, self.is_sticky_task_list_enabled);
        pi64(&mut f, 190, self.history_size);
        f.finish()
    }
}

impl FromThrift for PollMutableStateRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            execution: s.opt_struct(20)?,
            expected_next_event_id: s.i64_or_zero(30)?,
            current_branch_token: s.bytes_or_empty(40)?,
            version_history_item: None,
        })
    }
}
impl ToThrift for PollMutableStateRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.domain_uuid);
        f.ostruct(20, self.execution.as_ref());
        pi64(&mut f, 30, self.expected_next_event_id);
        f.sbytes(40, &self.current_branch_token);
        f.finish()
    }
}

impl FromThrift for PollMutableStateResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            execution: s.opt_struct(10)?,
            workflow_type: s.opt_struct(20)?,
            next_event_id: s.i64_or_zero(30)?,
            previous_started_event_id: s.opt_i64(35)?,
            last_first_event_id: s.i64_or_zero(40)?,
            task_list: s.opt_struct(50)?,
            sticky_task_list: s.opt_struct(60)?,
            client_library_version: s.req_string(70, "")?,
            client_feature_version: s.req_string(80, "")?,
            client_impl: s.req_string(90, "")?,
            sticky_task_list_schedule_to_start_timeout: s.opt_i32(100)?,
            current_branch_token: s.bytes_or_empty(110)?,
            version_histories: vh_opt(s, 130)?,
            workflow_state: s.opt_i32(140)?,
            workflow_close_state: s.opt_i32(150)?,
        })
    }
}
impl ToThrift for PollMutableStateResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.execution.as_ref());
        f.ostruct(20, self.workflow_type.as_ref());
        pi64(&mut f, 30, self.next_event_id);
        f.oi64(35, self.previous_started_event_id);
        pi64(&mut f, 40, self.last_first_event_id);
        f.ostruct(50, self.task_list.as_ref());
        f.ostruct(60, self.sticky_task_list.as_ref());
        ps(&mut f, 70, &self.client_library_version);
        ps(&mut f, 80, &self.client_feature_version);
        ps(&mut f, 90, &self.client_impl);
        f.oi32(100, self.sticky_task_list_schedule_to_start_timeout);
        f.sbytes(110, &self.current_branch_token);
        put_vh(&mut f, 130, &self.version_histories);
        f.oi32(140, self.workflow_state);
        f.oi32(150, self.workflow_close_state);
        f.finish()
    }
}

impl FromThrift for RecordDecisionTaskStartedRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            schedule_id: s.i64_or_zero(30)?,
            task_id: s.i64_or_zero(40)?,
            request_id: s.req_string(45, "")?,
            poll_request: s.opt_struct(50)?,
        })
    }
}
impl ToThrift for RecordDecisionTaskStartedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.domain_uuid);
        f.ostruct(20, self.workflow_execution.as_ref());
        pi64(&mut f, 30, self.schedule_id);
        pi64(&mut f, 40, self.task_id);
        ps(&mut f, 45, &self.request_id);
        f.ostruct(50, self.poll_request.as_ref());
        f.finish()
    }
}

impl FromThrift for TransientDecisionInfo {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self { scheduled_event: s.opt_struct(10)?, started_event: s.opt_struct(20)? })
    }
}
impl ToThrift for TransientDecisionInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.scheduled_event.as_ref());
        f.ostruct(20, self.started_event.as_ref());
        f.finish()
    }
}

impl FromThrift for RecordDecisionTaskStartedResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            workflow_type: s.opt_struct(10)?,
            previous_started_event_id: s.opt_i64(20)?,
            scheduled_event_id: s.i64_or_zero(30)?,
            started_event_id: s.i64_or_zero(40)?,
            next_event_id: s.i64_or_zero(50)?,
            attempt: s.i64_or_zero(60)?,
            sticky_execution_enabled: s.bool_or_false(70)?,
            decision_info: s.opt_struct(80)?,
            workflow_execution_task_list: s.opt_struct(90)?,
            event_store_version: s.opt_i32(100)?.unwrap_or_default(),
            branch_token: s.bytes_or_empty(110)?,
            scheduled_timestamp: s.opt_i64(120)?,
            started_timestamp: s.opt_i64(130)?,
            queries: s.opt_map_str::<WorkflowQuery>(140)?.unwrap_or_default(),
            history_size: s.i64_or_zero(150)?,
        })
    }
}
impl ToThrift for RecordDecisionTaskStartedResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.workflow_type.as_ref());
        f.oi64(20, self.previous_started_event_id);
        pi64(&mut f, 30, self.scheduled_event_id);
        pi64(&mut f, 40, self.started_event_id);
        pi64(&mut f, 50, self.next_event_id);
        pi64(&mut f, 60, self.attempt);
        pb(&mut f, 70, self.sticky_execution_enabled);
        f.ostruct(80, self.decision_info.as_ref());
        f.ostruct(90, self.workflow_execution_task_list.as_ref());
        pi32(&mut f, 100, self.event_store_version);
        f.sbytes(110, &self.branch_token);
        f.oi64(120, self.scheduled_timestamp);
        f.oi64(130, self.started_timestamp);
        f.omap_str_struct(140, &Some(self.queries.clone()));
        pi64(&mut f, 150, self.history_size);
        f.finish()
    }
}

impl FromThrift for RecordActivityTaskStartedRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            schedule_id: s.i64_or_zero(30)?,
            task_id: s.i64_or_zero(40)?,
            request_id: s.req_string(45, "")?,
            poll_request: s.opt_struct(50)?,
        })
    }
}
impl ToThrift for RecordActivityTaskStartedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.domain_uuid);
        f.ostruct(20, self.workflow_execution.as_ref());
        pi64(&mut f, 30, self.schedule_id);
        pi64(&mut f, 40, self.task_id);
        ps(&mut f, 45, &self.request_id);
        f.ostruct(50, self.poll_request.as_ref());
        f.finish()
    }
}

impl FromThrift for RecordActivityTaskStartedResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            scheduled_event: s.opt_struct(20)?,
            started_timestamp: s.opt_i64(30)?,
            attempt: s.i64_or_zero(40)?,
            scheduled_timestamp_of_this_attempt: s.opt_i64(50)?,
            heartbeat_details: s.bytes_or_empty(60)?,
            workflow_type: s.opt_struct(70)?,
            workflow_domain: s.req_string(80, "")?,
        })
    }
}
impl ToThrift for RecordActivityTaskStartedResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(20, self.scheduled_event.as_ref());
        f.oi64(30, self.started_timestamp);
        pi64(&mut f, 40, self.attempt);
        f.oi64(50, self.scheduled_timestamp_of_this_attempt);
        f.sbytes(60, &self.heartbeat_details);
        f.ostruct(70, self.workflow_type.as_ref());
        ps(&mut f, 80, &self.workflow_domain);
        f.finish()
    }
}

impl FromThrift for HistoryRespondDecisionTaskCompletedRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self { domain_uuid: s.req_string(10, "")?, complete_request: s.opt_struct(20)? })
    }
}
impl ToThrift for HistoryRespondDecisionTaskCompletedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.domain_uuid);
        f.ostruct(20, self.complete_request.as_ref());
        f.finish()
    }
}

impl FromThrift for ActivityLocalDispatchInfo {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            activity_id: s.req_string(10, "")?,
            scheduled_timestamp: s.opt_i64(20)?,
            started_timestamp: s.opt_i64(30)?,
            scheduled_timestamp_of_this_attempt: s.opt_i64(40)?,
            task_token: s.bytes_or_empty(50)?,
        })
    }
}
impl ToThrift for ActivityLocalDispatchInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.activity_id);
        f.oi64(20, self.scheduled_timestamp);
        f.oi64(30, self.started_timestamp);
        f.oi64(40, self.scheduled_timestamp_of_this_attempt);
        f.sbytes(50, &self.task_token);
        f.finish()
    }
}

impl FromThrift for HistoryRespondDecisionTaskCompletedResponse {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            started_response: s.opt_struct(10)?,
            activities_to_dispatch_locally: s
                .opt_map_str::<ActivityLocalDispatchInfo>(20)?
                .unwrap_or_default(),
        })
    }
}
impl ToThrift for HistoryRespondDecisionTaskCompletedResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.started_response.as_ref());
        f.omap_str_struct(20, &Some(self.activities_to_dispatch_locally.clone()));
        f.finish()
    }
}

macro_rules! domain_plus_request {
    ($ty:ident, $field:ident) => {
        impl FromThrift for $ty {
            fn from_thrift(s: &Struct) -> Result<Self> {
                Ok(Self { domain_uuid: s.req_string(10, "")?, $field: s.opt_struct(20)? })
            }
        }
        impl ToThrift for $ty {
            fn to_thrift(&self) -> Struct {
                let mut f = Fields::new();
                ps(&mut f, 10, &self.domain_uuid);
                f.ostruct(20, self.$field.as_ref());
                f.finish()
            }
        }
    };
}
domain_plus_request!(HistoryRespondDecisionTaskFailedRequest, failed_request);
domain_plus_request!(HistoryRecordActivityTaskHeartbeatRequest, heartbeat_request);
domain_plus_request!(HistoryRespondActivityTaskCompletedRequest, complete_request);
domain_plus_request!(HistoryRespondActivityTaskFailedRequest, failed_request);
domain_plus_request!(HistoryRespondActivityTaskCanceledRequest, cancel_request);

impl FromThrift for RemoveSignalMutableStateRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            request_id: s.req_string(30, "")?,
        })
    }
}
impl ToThrift for RemoveSignalMutableStateRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.domain_uuid);
        f.ostruct(20, self.workflow_execution.as_ref());
        ps(&mut f, 30, &self.request_id);
        f.finish()
    }
}

impl FromThrift for RecordChildExecutionCompletedRequest {
    fn from_thrift(s: &Struct) -> Result<Self> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            initiated_id: s.i64_or_zero(30)?,
            completed_execution: s.opt_struct(40)?,
            completion_event: s.opt_struct(50)?,
            started_id: s.i64_or_zero(60)?,
        })
    }
}
impl ToThrift for RecordChildExecutionCompletedRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        ps(&mut f, 10, &self.domain_uuid);
        f.ostruct(20, self.workflow_execution.as_ref());
        pi64(&mut f, 30, self.initiated_id);
        f.ostruct(40, self.completed_execution.as_ref());
        f.ostruct(50, self.completion_event.as_ref());
        pi64(&mut f, 60, self.started_id);
        f.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::service_types::HistoryEvent;
    use cadence_api::types::{TaskList, VersionHistoryItem, WorkflowExecution, WorkflowType};
    use cadence_thrift::value::{read_struct, write_struct};
    use cadence_thrift::{Reader, Writer};
    use std::collections::HashMap;

    fn wire_trip(s: &Struct) -> Struct {
        let mut w = Writer::new();
        write_struct(&mut w, s);
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        read_struct(&mut r).expect("decode")
    }

    fn same<T: FromThrift + ToThrift + serde::Serialize>(v: &T) {
        let back = T::from_thrift(&wire_trip(&v.to_thrift())).expect("from_thrift");
        assert_eq!(serde_json::to_value(v).unwrap(), serde_json::to_value(&back).unwrap());
    }

    fn exec() -> Option<WorkflowExecution> {
        Some(WorkflowExecution { workflow_id: "wf".into(), run_id: "run".into() })
    }
    fn tl(n: &str) -> Option<TaskList> {
        Some(TaskList { name: n.into(), ..Default::default() })
    }
    fn vhs() -> Option<VersionHistories> {
        Some(VersionHistories {
            current_version_history_index: 1,
            histories: vec![VersionHistory {
                branch_token: vec![1, 2],
                items: vec![VersionHistoryItem { event_id: 9, version: 3 }],
            }],
        })
    }
    fn ev(id: i64) -> HistoryEvent {
        HistoryEvent { event_id: id, version: 2, task_id: 7, ..Default::default() }
    }

    #[test]
    fn describe_and_get_mutable_state() {
        same(&DescribeMutableStateRequest { domain_uuid: "d".into(), execution: exec() });
        same(&DescribeMutableStateResponse {
            mutable_state_in_cache: "c".into(),
            mutable_state_in_database: "db".into(),
        });
        same(&GetMutableStateRequest {
            domain_uuid: "d".into(),
            execution: exec(),
            expected_next_event_id: 12,
            current_branch_token: vec![9],
            version_history_item: Some(VersionHistoryItem { event_id: 3, version: 4 }),
        });
        same(&GetMutableStateResponse {
            execution: exec(),
            workflow_type: Some(WorkflowType { name: "t".into() }),
            next_event_id: 5,
            previous_started_event_id: Some(0),
            last_first_event_id: 1,
            task_list: tl("a"),
            sticky_task_list: tl("b"),
            client_library_version: "1".into(),
            client_feature_version: "2".into(),
            client_impl: "go".into(),
            is_workflow_running: true,
            sticky_task_list_schedule_to_start_timeout: Some(10),
            event_store_version: 2,
            current_branch_token: vec![5, 6],
            workflow_state: Some(1),
            workflow_close_state: Some(0),
            version_histories: vhs(),
            is_sticky_task_list_enabled: true,
            history_size: 99,
        });
        same(&GetMutableStateResponse::default());
    }

    #[test]
    fn poll_mutable_state() {
        same(&PollMutableStateRequest {
            domain_uuid: "d".into(),
            execution: exec(),
            expected_next_event_id: 3,
            current_branch_token: vec![1],
            version_history_item: None,
        });
        same(&PollMutableStateResponse {
            execution: exec(),
            workflow_type: Some(WorkflowType { name: "t".into() }),
            next_event_id: 5,
            previous_started_event_id: Some(2),
            last_first_event_id: 1,
            task_list: tl("a"),
            sticky_task_list: None,
            client_library_version: "1".into(),
            client_feature_version: "2".into(),
            client_impl: "go".into(),
            sticky_task_list_schedule_to_start_timeout: Some(4),
            current_branch_token: vec![5],
            version_histories: vhs(),
            workflow_state: Some(2),
            workflow_close_state: Some(1),
        });
    }

    #[test]
    fn record_started() {
        same(&RecordDecisionTaskStartedRequest {
            domain_uuid: "d".into(),
            workflow_execution: exec(),
            schedule_id: 4,
            task_id: 8,
            request_id: "rid".into(),
            poll_request: Some(Default::default()),
        });
        let mut queries = HashMap::new();
        queries.insert("q".to_string(), WorkflowQuery { query_type: "t".into(), query_args: vec![1] });
        same(&RecordDecisionTaskStartedResponse {
            workflow_type: Some(WorkflowType { name: "t".into() }),
            previous_started_event_id: Some(3),
            scheduled_event_id: 4,
            started_event_id: 5,
            next_event_id: 6,
            attempt: 2,
            sticky_execution_enabled: true,
            decision_info: Some(TransientDecisionInfo {
                scheduled_event: Some(ev(4)),
                started_event: Some(ev(5)),
            }),
            workflow_execution_task_list: tl("tl"),
            event_store_version: 2,
            branch_token: vec![7],
            scheduled_timestamp: Some(100),
            started_timestamp: Some(200),
            queries,
            history_size: 33,
        });
        same(&RecordActivityTaskStartedRequest {
            domain_uuid: "d".into(),
            workflow_execution: exec(),
            schedule_id: 4,
            task_id: 8,
            request_id: "rid".into(),
            poll_request: Some(Default::default()),
        });
        same(&RecordActivityTaskStartedResponse {
            scheduled_event: Some(ev(4)),
            started_timestamp: Some(5),
            attempt: 3,
            scheduled_timestamp_of_this_attempt: Some(6),
            heartbeat_details: vec![1, 2],
            workflow_type: Some(WorkflowType { name: "t".into() }),
            workflow_domain: "dom".into(),
        });
    }

    #[test]
    fn respond_family() {
        same(&HistoryRespondDecisionTaskCompletedRequest {
            domain_uuid: "d".into(),
            complete_request: Some(Default::default()),
        });
        let mut act = HashMap::new();
        act.insert(
            "1".to_string(),
            ActivityLocalDispatchInfo {
                activity_id: "1".into(),
                scheduled_timestamp: Some(1),
                started_timestamp: Some(2),
                scheduled_timestamp_of_this_attempt: Some(3),
                task_token: vec![4],
            },
        );
        same(&HistoryRespondDecisionTaskCompletedResponse {
            started_response: Some(RecordDecisionTaskStartedResponse::default()),
            activities_to_dispatch_locally: act,
        });
        same(&HistoryRespondDecisionTaskFailedRequest {
            domain_uuid: "d".into(),
            failed_request: Some(Default::default()),
        });
        same(&HistoryRecordActivityTaskHeartbeatRequest {
            domain_uuid: "d".into(),
            heartbeat_request: Some(Default::default()),
        });
        same(&HistoryRespondActivityTaskCompletedRequest {
            domain_uuid: "d".into(),
            complete_request: Some(Default::default()),
        });
        same(&HistoryRespondActivityTaskFailedRequest {
            domain_uuid: "d".into(),
            failed_request: Some(Default::default()),
        });
        same(&HistoryRespondActivityTaskCanceledRequest {
            domain_uuid: "d".into(),
            cancel_request: Some(Default::default()),
        });
        same(&RemoveSignalMutableStateRequest {
            domain_uuid: "d".into(),
            workflow_execution: exec(),
            request_id: "r".into(),
        });
        same(&RecordChildExecutionCompletedRequest {
            domain_uuid: "d".into(),
            workflow_execution: exec(),
            initiated_id: 5,
            completed_execution: exec(),
            completion_event: Some(ev(9)),
            started_id: 6,
        });
    }

    #[test]
    fn version_histories_helpers_round_trip() {
        let v = vhs().unwrap();
        let back = version_histories_from_thrift(&wire_trip(&version_histories_to_thrift(&v))).unwrap();
        assert_eq!(v, back);
    }
}
