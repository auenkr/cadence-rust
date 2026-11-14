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
        "BackfillSchedule"
        | "CreateSchedule"
        | "CreateSemaphore"
        | "DeleteSchedule"
        | "DescribeSchedule"
        | "DescribeTaskList"
        | "DescribeWorkflowExecution"
        | "DiagnoseWorkflowExecution"
        | "FailoverDomain"
        | "GetTaskListsByDomain"
        | "GetWorkflowExecutionHistory"
        | "ListSchedules"
        | "ListTaskListPartitions"
        | "PauseSchedule"
        | "QueryWorkflow"
        | "RefreshWorkflowTasks"
        | "RequestCancelWorkflowExecution"
        | "ResetWorkflowExecution"
        | "RestartWorkflowExecution"
        | "SignalWithStartWorkflowExecution"
        | "SignalWorkflowExecution"
        | "StartWorkflowExecution"
        | "TerminateWorkflowExecution"
        | "UnpauseSchedule"
        | "UpdateSchedule" => (User, true),
        "CountWorkflowExecutions"
        | "ListArchivedWorkflowExecutions"
        | "ListClosedWorkflowExecutions"
        | "ListOpenWorkflowExecutions"
        | "ListWorkflowExecutions"
        | "ScanWorkflowExecutions" => (Visibility, true),
        "PollForActivityTask" | "PollForDecisionTask" => (WorkerPoll, true),
        "StartWorkflowExecutionAsync" | "SignalWithStartWorkflowExecutionAsync" => (Async, true),
        "RecordActivityTaskHeartbeat"
        | "RecordActivityTaskHeartbeatByID"
        | "ResetStickyTaskList"
        | "RespondActivityTaskCanceled"
        | "RespondActivityTaskCanceledByID"
        | "RespondActivityTaskCompleted"
        | "RespondActivityTaskCompletedByID"
        | "RespondActivityTaskFailed"
        | "RespondActivityTaskFailedByID"
        | "RespondDecisionTaskCompleted"
        | "RespondDecisionTaskFailed"
        | "RespondQueryTaskCompleted" => (Worker, false),
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
    /// `cadence-caller-type` header (Go `types.CallerInfo`); only consulted by
    /// the caller bypass, never part of a quota key.
    pub caller_type: String,
}

pub trait DomainNameLookup: Send + Sync {
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
        let token: EncodedTaskToken = serde_json::from_value(request.clone())
            .map_err(|error| CadenceError::Unknown(error.to_string()))?;
        let domain_id = if operation == "RespondQueryTaskCompleted" {
            cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
                &token.task_token,
            )
            .map_err(|error| CadenceError::Unknown(error.to_string()))?
            .domain_id
        } else {
            cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize(&token.task_token)
                .map_err(|error| CadenceError::Unknown(error.to_string()))?
                .domain_id
        };
        domains.domain_name(&domain_id)?
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
        caller_type: String::new(),
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

pub type WaitFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), WaitFailure>> + Send + 'a>>;

pub trait Policy: Send + Sync {
    fn allow(&self, info: &QuotaInfo) -> bool;
    /// Wait for a token without blocking the executor; dropping the future
    /// (caller cancellation) abandons the wait.
    fn wait<'a>(&'a self, info: &'a QuotaInfo, timeout: Duration) -> WaitFuture<'a>;
}

pub trait CallerBypass: Send + Sync {
    fn should_bypass(&self, info: &QuotaInfo) -> bool;
    fn allow_policy(&self, policy: &dyn Policy, info: &QuotaInfo) -> bool;
}

/// Select the configured Go quota family and preserve worker long-poll wait
/// behavior, including the deadline race re-check.
#[allow(clippy::too_many_arguments)]
pub async fn allow_domain(
    class: RequestClass,
    info: &QuotaInfo,
    max_worker_poll_delay: Duration,
    caller_deadline: Option<std::time::Instant>,
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
        // Go `waitForPolicy`: the wait context is bounded by the parent
        // deadline, and an expired parent wins over throttling.
        let timeout = caller_deadline.map_or(max_worker_poll_delay, |d| {
            max_worker_poll_delay.min(d.saturating_duration_since(std::time::Instant::now()))
        });
        let parent_expired = || caller_deadline.is_some_and(|d| std::time::Instant::now() >= d);
        return match policy.wait(info, timeout).await {
            Ok(()) => Ok(()),
            Err(WaitFailure::CallerCancelled) => {
                Err(CadenceError::Unknown("request context canceled".to_owned()))
            }
            Err(_) if parent_expired() => {
                Err(CadenceError::Unknown("context deadline exceeded".to_owned()))
            }
            Err(WaitFailure::Other(message)) => Err(CadenceError::Unknown(message)),
            Err(WaitFailure::Deadline) if bypass.should_bypass(info) => Ok(()),
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

/// Apply the generated method's rate-limit classification. Go deliberately
/// observes but ignores denied worker request limits; user, visibility, async
/// and worker-poll APIs return the policy error.
#[allow(clippy::too_many_arguments)]
pub async fn allow_operation(
    operation: &str,
    info: &QuotaInfo,
    max_worker_poll_delay: Duration,
    caller_deadline: Option<std::time::Instant>,
    user: &dyn Policy,
    worker: &dyn Policy,
    visibility: &dyn Policy,
    asynchronous: &dyn Policy,
    bypass: &dyn CallerBypass,
) -> Result<(), CadenceError> {
    let Some(rule) = operation_rule(operation) else {
        return Ok(());
    };
    let result = allow_domain(
        rule.class,
        info,
        max_worker_poll_delay,
        caller_deadline,
        user,
        worker,
        visibility,
        asynchronous,
        bypass,
    )
    .await;
    if rule.reject_when_limited {
        result
    } else {
        Ok(())
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

/// Concrete adapter that attaches Go's four quota policies and caller bypass
/// to the generic workflow Handler middleware hook.
pub struct HandlerRateLimit {
    domains: std::sync::Arc<dyn DomainNameLookup>,
    user: std::sync::Arc<dyn Policy>,
    worker: std::sync::Arc<dyn Policy>,
    visibility: std::sync::Arc<dyn Policy>,
    asynchronous: std::sync::Arc<dyn Policy>,
    bypass: std::sync::Arc<dyn CallerBypass>,
    /// Go `frontend.maxWorkerPollDelay` evaluated per call with the domain filter.
    max_worker_poll_delay: DomainDelaySource,
}

pub type DomainDelaySource = std::sync::Arc<dyn Fn(&str) -> Duration + Send + Sync>;

impl HandlerRateLimit {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        domains: std::sync::Arc<dyn DomainNameLookup>,
        user: std::sync::Arc<dyn Policy>,
        worker: std::sync::Arc<dyn Policy>,
        visibility: std::sync::Arc<dyn Policy>,
        asynchronous: std::sync::Arc<dyn Policy>,
        bypass: std::sync::Arc<dyn CallerBypass>,
        max_worker_poll_delay: DomainDelaySource,
    ) -> Self {
        Self {
            domains,
            user,
            worker,
            visibility,
            asynchronous,
            bypass,
            max_worker_poll_delay,
        }
    }
}

impl crate::handler_workflow::RequestRateLimit for HandlerRateLimit {
    fn allow(&self, operation: &str, request: &serde_json::Value) -> Result<(), CadenceError> {
        let info = quota_info(operation, request, self.domains.as_ref())?;
        // Synchronous path never waits: Go's non-poll `AllowPolicy` check.
        let Some(rule) = operation_rule(operation) else {
            return Ok(());
        };
        let policy = self.policy_for(rule.class);
        if !self.bypass.allow_policy(policy, &info) && rule.reject_when_limited {
            return Err(rate_limited());
        }
        Ok(())
    }

    fn allow_with_headers<'a>(
        &'a self,
        operation: &'a str,
        request: &'a serde_json::Value,
        headers: &'a std::collections::HashMap<String, String>,
        deadline: Option<std::time::Instant>,
    ) -> crate::handler_workflow::RateLimitFuture<'a> {
        Box::pin(async move {
            let mut info = quota_info(operation, request, self.domains.as_ref())?;
            info.caller_type = headers
                .get("cadence-caller-type")
                .cloned()
                .unwrap_or_default();
            let delay = (self.max_worker_poll_delay)(&info.domain);
            allow_operation(
                operation,
                &info,
                delay,
                deadline,
                self.user.as_ref(),
                self.worker.as_ref(),
                self.visibility.as_ref(),
                self.asynchronous.as_ref(),
                self.bypass.as_ref(),
            )
            .await
        })
    }
}

impl HandlerRateLimit {
    fn policy_for(&self, class: RequestClass) -> &dyn Policy {
        match class {
            RequestClass::User => self.user.as_ref(),
            RequestClass::Worker | RequestClass::WorkerPoll => self.worker.as_ref(),
            RequestClass::Visibility => self.visibility.as_ref(),
            RequestClass::Async => self.asynchronous.as_ref(),
        }
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
        fn wait<'a>(&'a self, _: &'a QuotaInfo, _: Duration) -> WaitFuture<'a> {
            let result = self.wait.clone();
            Box::pin(async move { result })
        }
    }

    struct Bypass(AtomicBool);
    impl CallerBypass for Bypass {
        fn should_bypass(&self, _: &QuotaInfo) -> bool {
            self.0.load(Ordering::SeqCst)
        }
        fn allow_policy(&self, policy: &dyn Policy, info: &QuotaInfo) -> bool {
            self.should_bypass(info) || policy.allow(info)
        }
    }

    fn setup(allow: bool, wait: Result<(), WaitFailure>) -> PermitPolicy {
        PermitPolicy { allow, wait }
    }

    #[tokio::test]
    async fn only_worker_poll_waits_and_deadline_race_rechecks_policy() {
        let p = setup(true, Err(WaitFailure::Deadline));
        let bypass = Bypass(AtomicBool::new(false));
        let info = QuotaInfo::default();
        assert!(allow_domain(
            RequestClass::WorkerPoll,
            &info,
            Duration::from_millis(10),
            None,
            &p,
            &p,
            &p,
            &p,
            &bypass
        )
        .await
        .is_ok());
        let denied = setup(false, Err(WaitFailure::Deadline));
        assert!(allow_domain(
            RequestClass::WorkerPoll,
            &info,
            Duration::from_millis(10),
            None,
            &denied,
            &denied,
            &denied,
            &denied,
            &bypass
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("Too many outstanding requests"));
    }

    #[tokio::test]
    async fn generated_wrapper_ignores_worker_rate_denials_but_rejects_user_denials() {
        let denied = setup(false, Ok(()));
        let bypass = Bypass(AtomicBool::new(false));
        let info = QuotaInfo::default();
        assert!(allow_operation(
            "RespondActivityTaskCompleted",
            &info,
            Duration::ZERO,
            None,
            &denied,
            &denied,
            &denied,
            &denied,
            &bypass,
        )
        .await
        .is_ok());
        assert_eq!(
            allow_operation(
                "StartWorkflowExecution",
                &info,
                Duration::ZERO,
                None,
                &denied,
                &denied,
                &denied,
                &denied,
                &bypass,
            )
            .await
            .unwrap_err()
            .to_string(),
            "Too many outstanding requests to the cadence service"
        );
    }

    #[tokio::test]
    async fn worker_poll_wait_returns_caller_cancellation() {
        let p = setup(false, Err(WaitFailure::CallerCancelled));
        let bypass = Bypass(AtomicBool::new(true));
        let err = allow_domain(
            RequestClass::WorkerPoll,
            &QuotaInfo::default(),
            Duration::from_secs(1),
            None,
            &p,
            &p,
            &p,
            &p,
            &bypass,
        )
        .await
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
            quota_info("StartWorkflowExecutionAsync", &async_request, &DomainLookup).unwrap(),
            QuotaInfo {
                domain: "async-domain".into(),
                task_list: "base".into(),
                caller_type: String::new(),
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
                task_list: String::new(),
                caller_type: String::new(),
            }
        );
    }

    #[test]
    fn operation_categories_and_ignored_worker_limits_match_generated_go_wrapper() {
        assert_eq!(
            operation_rule("PollForActivityTask").unwrap().class,
            RequestClass::WorkerPoll
        );
        assert!(
            operation_rule("StartWorkflowExecutionAsync")
                .unwrap()
                .reject_when_limited
        );
        assert!(
            !operation_rule("RespondActivityTaskCompleted")
                .unwrap()
                .reject_when_limited
        );
        assert_eq!(
            operation_rule("CountWorkflowExecutions").unwrap().class,
            RequestClass::Visibility
        );
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

    #[tokio::test]
    async fn solfix11_expired_parent_deadline_wins_over_throttling() {
        let p = setup(false, Err(WaitFailure::Deadline));
        let bypass = Bypass(AtomicBool::new(false));
        let err = allow_domain(
            RequestClass::WorkerPoll,
            &QuotaInfo::default(),
            Duration::from_secs(60),
            Some(std::time::Instant::now()),
            &p,
            &p,
            &p,
            &p,
            &bypass,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            CadenceError::Unknown(message) if message == "context deadline exceeded"
        ));
    }
}
