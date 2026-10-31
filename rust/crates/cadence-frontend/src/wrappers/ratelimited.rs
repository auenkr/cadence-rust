//! Frontend request throttling policy (Go: wrappers/ratelimited).

#![allow(clippy::result_large_err, clippy::too_many_arguments)]

use std::time::Duration;

use cadence_api::errors::{CadenceError, ServiceBusyError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestClass {
    User,
    Worker,
    WorkerPoll,
    Visibility,
    Async,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationRule {
    pub class: RequestClass,
    /// Several worker completion methods call `allowDomain` but deliberately
    /// ignore the result, matching Go's generated wrapper.
    pub reject_when_limited: bool,
}

pub fn operation_rule(operation: &str) -> Option<OperationRule> {
    use RequestClass::{Async, User, Visibility, Worker, WorkerPoll};
    let (class, reject_when_limited) = match operation {
        "BackfillSchedule" | "CreateSchedule" | "CreateSemaphore" | "DeleteSchedule"
        | "DescribeSchedule" | "DescribeTaskList" | "DescribeWorkflowExecution"
        | "DiagnoseWorkflowExecution" | "FailoverDomain" | "GetTaskListsByDomain"
        | "GetWorkflowExecutionHistory" | "ListSchedules" | "ListTaskListPartitions"
        | "PauseSchedule" | "QueryWorkflow" | "RefreshWorkflowTasks"
        | "RequestCancelWorkflowExecution" | "ResetWorkflowExecution"
        | "RestartWorkflowExecution" | "SignalWithStartWorkflowExecution"
        | "SignalWorkflowExecution" | "StartWorkflowExecution" | "TerminateWorkflowExecution"
        | "UnpauseSchedule" | "UpdateSchedule" => (User, true),
        "CountWorkflowExecutions" | "ListArchivedWorkflowExecutions"
        | "ListClosedWorkflowExecutions" | "ListOpenWorkflowExecutions"
        | "ListWorkflowExecutions" | "ScanWorkflowExecutions" => (Visibility, true),
        "PollForActivityTask" | "PollForDecisionTask" => (WorkerPoll, true),
        "StartWorkflowExecutionAsync" | "SignalWithStartWorkflowExecutionAsync" => (Async, true),
        "RecordActivityTaskHeartbeat" | "RecordActivityTaskHeartbeatByID"
        | "ResetStickyTaskList" | "RespondActivityTaskCanceled"
        | "RespondActivityTaskCanceledByID" | "RespondActivityTaskCompleted"
        | "RespondActivityTaskCompletedByID" | "RespondActivityTaskFailed"
        | "RespondActivityTaskFailedByID" | "RespondDecisionTaskCompleted"
        | "RespondDecisionTaskFailed" | "RespondQueryTaskCompleted" => (Worker, false),
        _ => return None,
    };
    Some(OperationRule {
        class,
        reject_when_limited,
    })
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QuotaInfo {
    pub domain: String,
    pub task_list: String,
}

pub trait DomainNameLookup {
    fn domain_name(&self, domain_id: &str) -> Result<String, CadenceError>;
}

/// Build the quota dimensions used by Go's generated wrapper. Task-token APIs
/// look up the current domain name from the token's domain UUID; API methods
/// with an explicit task list charge poll/start requests to its base name when
/// it is sticky.
pub fn quota_info(
    operation: &str,
    request: &serde_json::Value,
    domains: &dyn DomainNameLookup,
) -> Result<QuotaInfo, CadenceError> {
    let fields = if matches!(
        operation,
        "StartWorkflowExecutionAsync" | "SignalWithStartWorkflowExecutionAsync"
    ) {
        request.get("request").unwrap_or(request)
    } else {
        request
    };

    let domain = if matches!(
        operation,
        "RecordActivityTaskHeartbeat"
            | "RespondActivityTaskCanceled"
            | "RespondActivityTaskCompleted"
            | "RespondActivityTaskFailed"
            | "RespondDecisionTaskCompleted"
            | "RespondDecisionTaskFailed"
            | "RespondQueryTaskCompleted"
    ) {
        let token: EncodedTaskToken = serde_json::from_value(request.clone()).map_err(|error| {
            CadenceError::Unknown(error.to_string())
        })?;
        let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize(&token.task_token)
            .map_err(|error| CadenceError::Unknown(error.to_string()))?;
        domains.domain_name(&token.domain_id)?
    } else {
        fields
            .get("domain")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };

    let task_list_name = if matches!(
        operation,
        "PollForActivityTask"
            | "PollForDecisionTask"
            | "StartWorkflowExecution"
            | "StartWorkflowExecutionAsync"
            | "SignalWithStartWorkflowExecution"
            | "SignalWithStartWorkflowExecutionAsync"
    ) {
        let task_list = fields.get("taskList").unwrap_or(&serde_json::Value::Null);
        task_list_quota_name(
            task_list
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
            task_list
                .get("baseName")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
            task_list
                .get("kind")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default() as i32,
        )
    } else {
        String::new()
    };

    Ok(QuotaInfo {
        domain,
        task_list: task_list_name,
    })
}

#[derive(serde::Deserialize)]
struct EncodedTaskToken {
    #[serde(
        rename = "taskToken",
        default,
        deserialize_with = "cadence_api::b64::deserialize_bytes"
    )]
    task_token: Vec<u8>,
}

/// A wait result distinguishes caller cancellation from the frontend's
/// configured poll-delay timeout. Go makes that distinction before deciding
/// whether to return the throttling ServiceBusy error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WaitFailure {
    CallerCancelled,
    Deadline,
    Other(String),
}

pub trait Policy {
    fn allow(&self, info: &QuotaInfo) -> bool;
    fn wait(&self, info: &QuotaInfo, timeout: Duration) -> Result<(), WaitFailure>;
}

pub trait CallerBypass {
    fn should_bypass(&self) -> bool;
    fn allow_policy(&self, policy: &dyn Policy, info: &QuotaInfo) -> bool;
}

/// Select the configured Go quota family and preserve worker long-poll wait
/// behavior, including the deadline race re-check.
pub fn allow_domain(
    class: RequestClass,
    info: &QuotaInfo,
    max_worker_poll_delay: Duration,
    user: &dyn Policy,
    worker: &dyn Policy,
    visibility: &dyn Policy,
    asynchronous: &dyn Policy,
    bypass: &dyn CallerBypass,
) -> Result<(), CadenceError> {
    let policy = match class {
        RequestClass::User => user,
        RequestClass::Worker | RequestClass::WorkerPoll => worker,
        RequestClass::Visibility => visibility,
        RequestClass::Async => asynchronous,
    };

    if class == RequestClass::WorkerPoll && !max_worker_poll_delay.is_zero() {
        return match policy.wait(info, max_worker_poll_delay) {
            Ok(()) => Ok(()),
            Err(WaitFailure::CallerCancelled) => Err(CadenceError::Unknown(
                "request context canceled".to_owned(),
            )),
            Err(WaitFailure::Other(message)) => Err(CadenceError::Unknown(message)),
            Err(WaitFailure::Deadline) if bypass.should_bypass() => Ok(()),
            Err(WaitFailure::Deadline) if bypass.allow_policy(policy, info) => Ok(()),
            Err(WaitFailure::Deadline) => Err(rate_limited()),
        };
    }

    if bypass.allow_policy(policy, info) {
        Ok(())
    } else {
        Err(rate_limited())
    }
}

fn rate_limited() -> CadenceError {
    CadenceError::ServiceBusy(ServiceBusyError::new(
        "Too many outstanding requests to the cadence service",
    ))
}

/// Extract task-list quota name using Go TaskList.GetKind semantics. Sticky
/// task lists are charged to their base name; normal lists use their name.
pub fn task_list_quota_name(name: &str, base_name: &str, kind: i32) -> String {
    if kind == 0 {
        name.to_owned()
    } else {
        base_name.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DomainLookup;
    impl DomainNameLookup for DomainLookup {
        fn domain_name(&self, domain_id: &str) -> Result<String, CadenceError> {
            assert_eq!(domain_id, "domain-id");
            Ok("resolved-domain".to_owned())
        }
    }

    #[derive(serde::Serialize)]
    struct TokenRequest {
        #[serde(
            rename = "taskToken",
            serialize_with = "cadence_api::b64::serialize_bytes"
        )]
        task_token: Vec<u8>,
    }

    struct PermitPolicy {
        allow: bool,
        wait: Result<(), WaitFailure>,
    }

    impl Policy for PermitPolicy {
        fn allow(&self, _: &QuotaInfo) -> bool {
            self.allow
        }
        fn wait(&self, _: &QuotaInfo, _: Duration) -> Result<(), WaitFailure> {
            self.wait.clone()
        }
    }

    struct Bypass(AtomicBool);
    impl CallerBypass for Bypass {
        fn should_bypass(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
        fn allow_policy(&self, policy: &dyn Policy, info: &QuotaInfo) -> bool {
            self.should_bypass() || policy.allow(info)
        }
    }

    fn setup(allow: bool, wait: Result<(), WaitFailure>) -> PermitPolicy {
        PermitPolicy { allow, wait }
    }

    #[test]
    fn only_worker_poll_waits_and_deadline_race_rechecks_policy() {
        let p = setup(true, Err(WaitFailure::Deadline));
        let bypass = Bypass(AtomicBool::new(false));
        let info = QuotaInfo::default();
        assert!(allow_domain(
            RequestClass::WorkerPoll,
            &info,
            Duration::from_millis(10),
            &p,
            &p,
            &p,
            &p,
            &bypass
        )
        .is_ok());
        let denied = setup(false, Err(WaitFailure::Deadline));
        assert!(allow_domain(
            RequestClass::WorkerPoll,
            &info,
            Duration::from_millis(10),
            &denied,
            &denied,
            &denied,
            &denied,
            &bypass
        )
        .unwrap_err()
        .to_string()
        .contains("Too many outstanding requests"));
    }

    #[test]
    fn worker_poll_wait_returns_caller_cancellation() {
        let p = setup(false, Err(WaitFailure::CallerCancelled));
        let bypass = Bypass(AtomicBool::new(true));
        let err = allow_domain(
            RequestClass::WorkerPoll,
            &QuotaInfo::default(),
            Duration::from_secs(1),
            &p,
            &p,
            &p,
            &p,
            &bypass,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            CadenceError::Unknown(message) if message == "request context canceled"
        ));
    }

    #[test]
    fn sticky_poll_is_charged_to_base_task_list() {
        assert_eq!(task_list_quota_name("sticky-name", "base", 1), "base");
        assert_eq!(task_list_quota_name("normal", "", 0), "normal");
    }

    #[test]
    fn quota_dimensions_match_nested_async_and_token_requests() {
        let async_request = serde_json::json!({
            "request": {
                "domain": "async-domain",
                "taskList": {"name": "sticky", "baseName": "base", "kind": 1}
            }
        });
        assert_eq!(
            quota_info(
                "StartWorkflowExecutionAsync",
                &async_request,
                &DomainLookup
            )
            .unwrap(),
            QuotaInfo {
                domain: "async-domain".into(),
                task_list: "base".into()
            }
        );

        let bytes = cadence_api::tasktoken::JsonTaskTokenSerializer::serialize(
            &cadence_api::tasktoken::TaskToken {
                domain_id: "domain-id".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let request = serde_json::to_value(TokenRequest { task_token: bytes }).unwrap();
        assert_eq!(
            quota_info("RespondActivityTaskCompleted", &request, &DomainLookup).unwrap(),
            QuotaInfo {
                domain: "resolved-domain".into(),
                task_list: String::new()
            }
        );
    }

    #[test]
    fn operation_categories_and_ignored_worker_limits_match_generated_go_wrapper() {
        assert_eq!(operation_rule("PollForActivityTask").unwrap().class, RequestClass::WorkerPoll);
        assert!(operation_rule("StartWorkflowExecutionAsync").unwrap().reject_when_limited);
        assert!(!operation_rule("RespondActivityTaskCompleted").unwrap().reject_when_limited);
        assert_eq!(operation_rule("CountWorkflowExecutions").unwrap().class, RequestClass::Visibility);
        assert!(operation_rule("RegisterDomain").is_none());
    }

    #[test]
    fn workflow_api_ratelimit_table_covers_each_go_generated_method() {
        use RequestClass::{Async, User, Visibility, Worker, WorkerPoll};
        let expected = [
            ("DiagnoseWorkflowExecution", Some((User, true))),
            ("PollForActivityTask", Some((WorkerPoll, true))),
            ("PollForDecisionTask", Some((WorkerPoll, true))),
            ("RecordActivityTaskHeartbeat", Some((Worker, false))),
            ("RecordActivityTaskHeartbeatByID", Some((Worker, false))),
            ("RespondActivityTaskCompleted", Some((Worker, false))),
            ("RespondActivityTaskCompletedByID", Some((Worker, false))),
            ("RespondActivityTaskFailed", Some((Worker, false))),
            ("RespondActivityTaskFailedByID", Some((Worker, false))),
            ("RespondActivityTaskCanceled", Some((Worker, false))),
            ("RespondActivityTaskCanceledByID", Some((Worker, false))),
            ("RespondDecisionTaskCompleted", Some((Worker, false))),
            ("RespondDecisionTaskFailed", Some((Worker, false))),
            ("RespondQueryTaskCompleted", Some((Worker, false))),
            ("StartWorkflowExecution", Some((User, true))),
            ("StartWorkflowExecutionAsync", Some((Async, true))),
            ("GetWorkflowExecutionHistory", Some((User, true))),
            ("SignalWorkflowExecution", Some((User, true))),
            ("SignalWithStartWorkflowExecution", Some((User, true))),
            ("SignalWithStartWorkflowExecutionAsync", Some((Async, true))),
            ("TerminateWorkflowExecution", Some((User, true))),
            ("ResetWorkflowExecution", Some((User, true))),
            ("RequestCancelWorkflowExecution", Some((User, true))),
            ("RestartWorkflowExecution", Some((User, true))),
            ("GetSearchAttributes", None),
            ("QueryWorkflow", Some((User, true))),
            ("DescribeWorkflowExecution", Some((User, true))),
            ("CountWorkflowExecutions", Some((Visibility, true))),
            ("ScanWorkflowExecutions", Some((Visibility, true))),
            ("ListOpenWorkflowExecutions", Some((Visibility, true))),
            ("ListClosedWorkflowExecutions", Some((Visibility, true))),
            ("ListArchivedWorkflowExecutions", Some((Visibility, true))),
            ("ListWorkflowExecutions", Some((Visibility, true))),
            ("DescribeTaskList", Some((User, true))),
            ("ListTaskListPartitions", Some((User, true))),
            ("GetTaskListsByDomain", Some((User, true))),
            ("ResetStickyTaskList", Some((Worker, false))),
            ("RefreshWorkflowTasks", Some((User, true))),
            ("GetClusterInfo", None),
            ("Health", None),
        ];
        for (name, expected) in expected {
            assert_eq!(
                operation_rule(name).map(|rule| (rule.class, rule.reject_when_limited)),
                expected,
                "Go wrapper rate-limit classification differs for {name}"
            );
        }
    }
}
