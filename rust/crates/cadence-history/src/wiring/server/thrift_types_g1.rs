//! Thrift converters for the History-local request/response wrappers used by
//! the group-1 RPCs (field ids from go/.gen/go/history/history.go).
use crate::engine::types::{
    HistoryDescribeWorkflowExecutionRequest, HistoryQueryWorkflowRequest,
    HistoryQueryWorkflowResponse, HistoryReapplyEventsRequest, HistoryRefreshWorkflowTasksRequest,
    HistoryRequestCancelWorkflowExecutionRequest, HistoryResetStickyTaskListRequest,
    HistoryResetStickyTaskListResponse, HistorySignalWithStartWorkflowExecutionRequest,
    HistorySignalWorkflowExecutionRequest, HistoryStartWorkflowExecutionRequest,
    HistoryTerminateWorkflowExecutionRequest, ParentExecutionInfo, ScheduleDecisionTaskRequest,
};
use cadence_thrift::protocol::Error;
use cadence_thrift::value::{Fields, FromThrift, Struct, ToThrift};
use std::collections::HashMap;

fn map_in(s: &Struct, id: i16) -> Result<HashMap<String, String>, Error> {
    Ok(s.opt_map_str::<String>(id)?.unwrap_or_default())
}

fn map_out(f: &mut Fields, id: i16, m: &HashMap<String, String>) {
    f.omap_str_str(id, &Some(m.clone()));
}

impl FromThrift for ParentExecutionInfo {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            domain: s.req_string(15, "")?,
            execution: s.opt_struct(20)?,
            initiated_id: s.i64_or_zero(30)?,
        })
    }
}

impl ToThrift for ParentExecutionInfo {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.sstr(15, &self.domain);
        f.ostruct(20, self.execution.as_ref());
        f.oi64(30, Some(self.initiated_id));
        f.finish()
    }
}

impl FromThrift for HistoryStartWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            start_request: s.opt_struct(20)?,
            parent_execution_info: s.opt_struct(30)?,
            attempt: s.opt_i32(40)?.unwrap_or(0),
            expiration_timestamp: s.opt_i64(50)?,
            continue_as_new_initiator: s.opt_enum(55)?,
            continued_failure_reason: s.opt_string(56)?,
            continued_failure_details: s.bytes_or_empty(57)?,
            last_completion_result: s.bytes_or_empty(58)?,
            first_decision_task_backoff_seconds: s.opt_i32(60)?,
            partition_config: map_in(s, 62)?,
        })
    }
}

impl ToThrift for HistoryStartWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.start_request.as_ref());
        f.ostruct(30, self.parent_execution_info.as_ref());
        f.oi32(40, Some(self.attempt));
        f.oi64(50, self.expiration_timestamp);
        f.oenum(55, self.continue_as_new_initiator);
        f.ostr(56, &self.continued_failure_reason);
        f.sbytes(57, &self.continued_failure_details);
        f.sbytes(58, &self.last_completion_result);
        f.oi32(60, self.first_decision_task_backoff_seconds);
        map_out(&mut f, 62, &self.partition_config);
        f.finish()
    }
}

impl FromThrift for HistorySignalWithStartWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            signal_with_start_request: s.opt_struct(20)?,
            partition_config: map_in(s, 30)?,
        })
    }
}

impl ToThrift for HistorySignalWithStartWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.signal_with_start_request.as_ref());
        map_out(&mut f, 30, &self.partition_config);
        f.finish()
    }
}

impl FromThrift for HistorySignalWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            signal_request: s.opt_struct(20)?,
            external_workflow_execution: s.opt_struct(30)?,
            child_workflow_only: s.bool_or_false(40)?,
        })
    }
}

impl ToThrift for HistorySignalWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.signal_request.as_ref());
        f.ostruct(30, self.external_workflow_execution.as_ref());
        f.obool(40, self.child_workflow_only);
        f.finish()
    }
}

impl FromThrift for HistoryTerminateWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            terminate_request: s.opt_struct(20)?,
            external_workflow_execution: s.opt_struct(30)?,
            child_workflow_only: s.bool_or_false(40)?,
        })
    }
}

impl ToThrift for HistoryTerminateWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.terminate_request.as_ref());
        f.ostruct(30, self.external_workflow_execution.as_ref());
        f.obool(40, self.child_workflow_only);
        f.finish()
    }
}

impl FromThrift for HistoryRequestCancelWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            cancel_request: s.opt_struct(20)?,
            external_initiated_event_id: s.opt_i64(30)?,
            external_workflow_execution: s.opt_struct(40)?,
            child_workflow_only: s.bool_or_false(50)?,
        })
    }
}

impl ToThrift for HistoryRequestCancelWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.cancel_request.as_ref());
        f.oi64(30, self.external_initiated_event_id);
        f.ostruct(40, self.external_workflow_execution.as_ref());
        f.obool(50, self.child_workflow_only);
        f.finish()
    }
}

impl FromThrift for HistoryResetStickyTaskListRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            execution: s.opt_struct(20)?,
        })
    }
}

impl ToThrift for HistoryResetStickyTaskListRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.execution.as_ref());
        f.finish()
    }
}

impl FromThrift for HistoryResetStickyTaskListResponse {
    fn from_thrift(_s: &Struct) -> Result<Self, Error> {
        Ok(Self {})
    }
}

impl ToThrift for HistoryResetStickyTaskListResponse {
    fn to_thrift(&self) -> Struct {
        Struct::default()
    }
}

impl FromThrift for HistoryQueryWorkflowRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            request: s.opt_struct(20)?,
        })
    }
}

impl ToThrift for HistoryQueryWorkflowRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.request.as_ref());
        f.finish()
    }
}

impl FromThrift for HistoryQueryWorkflowResponse {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            response: s.opt_struct(10)?,
        })
    }
}

impl ToThrift for HistoryQueryWorkflowResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.response.as_ref());
        f.finish()
    }
}

impl FromThrift for HistoryDescribeWorkflowExecutionRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            request: s.opt_struct(20)?,
        })
    }
}

impl ToThrift for HistoryDescribeWorkflowExecutionRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.request.as_ref());
        f.finish()
    }
}

impl FromThrift for HistoryReapplyEventsRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            request: s.opt_struct(20)?,
        })
    }
}

impl ToThrift for HistoryReapplyEventsRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.request.as_ref());
        f.finish()
    }
}

/// Go's wire name for the domain id field here is the (misspelled) `domainUIID`,
/// but its thrift field id is 10 like every other History request.
impl FromThrift for HistoryRefreshWorkflowTasksRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            request: s.opt_struct(20)?,
        })
    }
}

impl FromThrift for ScheduleDecisionTaskRequest {
    fn from_thrift(s: &Struct) -> Result<Self, Error> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            is_first_decision: s.bool_or_false(30)?,
        })
    }
}

impl ToThrift for ScheduleDecisionTaskRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.workflow_execution.as_ref());
        f.obool(30, self.is_first_decision);
        f.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::WorkflowExecution;

    fn we() -> WorkflowExecution {
        WorkflowExecution {
            workflow_id: "wf".into(),
            run_id: "run".into(),
        }
    }

    #[test]
    fn schedule_decision_round_trip() {
        let r = ScheduleDecisionTaskRequest {
            domain_uuid: "d".into(),
            workflow_execution: Some(we()),
            is_first_decision: true,
        };
        let back = ScheduleDecisionTaskRequest::from_thrift(&r.to_thrift()).unwrap();
        assert_eq!(back.domain_uuid, "d");
        assert!(back.is_first_decision);
        assert_eq!(back.workflow_execution.unwrap().run_id, "run");
    }

    #[test]
    fn cancel_round_trip() {
        let r = HistoryRequestCancelWorkflowExecutionRequest {
            domain_uuid: "d".into(),
            cancel_request: None,
            external_initiated_event_id: Some(7),
            external_workflow_execution: Some(we()),
            child_workflow_only: true,
        };
        let back = HistoryRequestCancelWorkflowExecutionRequest::from_thrift(&r.to_thrift()).unwrap();
        assert_eq!(back.external_initiated_event_id, Some(7));
        assert!(back.child_workflow_only);
        assert_eq!(back.external_workflow_execution.unwrap().workflow_id, "wf");
    }

    #[test]
    fn start_round_trip() {
        let mut pc = HashMap::new();
        pc.insert("k".to_string(), "v".to_string());
        let r = HistoryStartWorkflowExecutionRequest {
            domain_uuid: "d".into(),
            attempt: 3,
            expiration_timestamp: Some(99),
            continued_failure_reason: Some("r".into()),
            continued_failure_details: vec![1, 2],
            last_completion_result: vec![3],
            first_decision_task_backoff_seconds: Some(5),
            parent_execution_info: Some(ParentExecutionInfo {
                domain_uuid: "pd".into(),
                domain: "pn".into(),
                execution: Some(we()),
                initiated_id: 4,
            }),
            partition_config: pc,
            ..Default::default()
        };
        let back = HistoryStartWorkflowExecutionRequest::from_thrift(&r.to_thrift()).unwrap();
        assert_eq!(back.attempt, 3);
        assert_eq!(back.expiration_timestamp, Some(99));
        assert_eq!(back.continued_failure_details, vec![1, 2]);
        assert_eq!(back.first_decision_task_backoff_seconds, Some(5));
        assert_eq!(back.partition_config.get("k").map(String::as_str), Some("v"));
        let p = back.parent_execution_info.unwrap();
        assert_eq!(p.initiated_id, 4);
        assert_eq!(p.domain, "pn");
    }

    #[test]
    fn signal_terminate_round_trip() {
        let s = HistorySignalWorkflowExecutionRequest {
            domain_uuid: "d".into(),
            signal_request: None,
            external_workflow_execution: Some(we()),
            child_workflow_only: true,
        };
        let b = HistorySignalWorkflowExecutionRequest::from_thrift(&s.to_thrift()).unwrap();
        assert!(b.child_workflow_only);
        let t = HistoryTerminateWorkflowExecutionRequest {
            domain_uuid: "d".into(),
            terminate_request: None,
            external_workflow_execution: None,
            child_workflow_only: false,
        };
        let b = HistoryTerminateWorkflowExecutionRequest::from_thrift(&t.to_thrift()).unwrap();
        assert_eq!(b.domain_uuid, "d");
    }

    #[test]
    fn sticky_and_query_round_trip() {
        let r = HistoryResetStickyTaskListRequest {
            domain_uuid: "d".into(),
            execution: Some(we()),
        };
        let b = HistoryResetStickyTaskListRequest::from_thrift(&r.to_thrift()).unwrap();
        assert_eq!(b.execution.unwrap().workflow_id, "wf");
        let q = HistoryQueryWorkflowResponse { response: None };
        assert!(HistoryQueryWorkflowResponse::from_thrift(&q.to_thrift())
            .unwrap()
            .response
            .is_none());
    }
}
