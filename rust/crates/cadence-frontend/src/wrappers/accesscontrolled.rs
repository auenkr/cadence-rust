//! Authorization wrapper (Go: wrappers/accesscontrolled).

#![allow(clippy::result_large_err)]

use cadence_api::errors::{AccessDeniedError, CadenceError};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Permission {
    Read,
    Write,
    Admin,
    Process,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Attributes {
    pub operation: String,
    pub permission: Option<Permission>,
    pub actor: String,
    pub domain: String,
    pub task_list: Option<Value>,
    pub workflow_type: Option<Value>,
    pub request_body: Option<Value>,
}

/// Build the authorization input used by Go's generated Workflow API wrapper.
/// Methods absent from that wrapper return `None` and bypass authorization.
pub fn attributes_for_request(operation: &str, request: &Value, actor: &str) -> Option<Attributes> {
    use Permission::{Admin, Process, Read, Write};
    let permission = match operation {
        "CountWorkflowExecutions"
        | "DescribeDomain"
        | "DescribeSchedule"
        | "DescribeTaskList"
        | "DescribeWorkflowExecution"
        | "GetTaskListsByDomain"
        | "GetWorkflowExecutionHistory"
        | "ListArchivedWorkflowExecutions"
        | "ListClosedWorkflowExecutions"
        | "ListOpenWorkflowExecutions"
        | "ListSchedules"
        | "ListTaskListPartitions"
        | "ListWorkflowExecutions"
        | "QueryWorkflow"
        | "ScanWorkflowExecutions" => Read,
        "CreateSchedule"
        | "DeleteSchedule"
        | "FailoverDomain"
        | "PauseSchedule"
        | "RefreshWorkflowTasks"
        | "RequestCancelWorkflowExecution"
        | "ResetWorkflowExecution"
        | "RestartWorkflowExecution"
        | "SignalWithStartWorkflowExecution"
        | "SignalWithStartWorkflowExecutionAsync"
        | "SignalWorkflowExecution"
        | "StartWorkflowExecution"
        | "StartWorkflowExecutionAsync"
        | "TerminateWorkflowExecution"
        | "UnpauseSchedule"
        | "UpdateSchedule" => Write,
        "DeleteDomain" | "DeprecateDomain" | "RegisterDomain" | "UpdateDomain" => Admin,
        "PollForActivityTask" | "PollForDecisionTask" => Process,
        _ => return None,
    };
    let request_fields = if matches!(
        operation,
        "StartWorkflowExecutionAsync" | "SignalWithStartWorkflowExecutionAsync"
    ) {
        request.get("request").unwrap_or(request)
    } else {
        request
    };
    let domain = request_fields
        .get("domain")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let mut request_body = request.clone();
    if matches!(
        operation,
        "SignalWorkflowExecution"
            | "SignalWithStartWorkflowExecution"
            | "SignalWithStartWorkflowExecutionAsync"
            | "StartWorkflowExecution"
            | "StartWorkflowExecutionAsync"
    ) {
        // Go's request structs mark these byte fields json:"-" to keep
        // workflow/query payloads out of authorization audit logs. The async
        // request types wrap the regular request in a nested `request` field.
        strip_hidden_payloads(&mut request_body);
    }
    Some(Attributes {
        operation: operation.to_owned(),
        permission: Some(permission),
        actor: actor.to_owned(),
        domain,
        task_list: request_fields.get("taskList").cloned(),
        workflow_type: request_fields.get("workflowType").cloned(),
        // Go's NewFilteredRequestBody JSON-serializes the full API request.
        request_body: Some(request_body),
    })
}

fn strip_hidden_payloads(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            fields.remove("input");
            fields.remove("signalInput");
            for child in fields.values_mut() {
                strip_hidden_payloads(child);
            }
        }
        Value::Array(values) => values.iter_mut().for_each(strip_hidden_payloads),
        _ => {}
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Allow,
    Deny,
}

pub trait Authorizer {
    type Error: std::fmt::Display;

    fn authorize(&self, attributes: &Attributes) -> Result<Decision, Self::Error>;
}

pub trait AuthorizationMetrics {
    fn authorization_latency(&self, elapsed: std::time::Duration);
    fn authorization_failed(&self);
    fn unauthorized(&self);
}

pub fn authorize<A, M>(
    authorizer: &A,
    metrics: &M,
    attributes: &Attributes,
) -> Result<(), CadenceError>
where
    A: Authorizer,
    M: AuthorizationMetrics,
{
    let start = std::time::Instant::now();
    let result = authorizer.authorize(attributes);
    metrics.authorization_latency(start.elapsed());
    match result {
        Err(error) => {
            metrics.authorization_failed();
            Err(CadenceError::Unknown(error.to_string()))
        }
        Ok(Decision::Allow) => Ok(()),
        Ok(Decision::Deny) => {
            metrics.unauthorized();
            Err(CadenceError::AccessDenied(AccessDeniedError::new(
                "Request unauthorized.",
            )))
        }
    }
}

/// Apply the generated Go API authorization policy to a request. Endpoints
/// without an access-control wrapper pass through unchanged.
pub fn authorize_request<A, M>(
    operation: &str,
    request: &Value,
    actor: &str,
    authorizer: &A,
    metrics: &M,
) -> Result<(), CadenceError>
where
    A: Authorizer,
    M: AuthorizationMetrics,
{
    match attributes_for_request(operation, request, actor) {
        Some(attributes) => authorize(authorizer, metrics, &attributes),
        None => Ok(()),
    }
}

/// Shared decorator adapter for `handler_workflow::Handler`. The generated Go
/// wrapper skips methods without an authorization declaration, so that
/// behavior is kept in `attributes_for_request` rather than in the caller.
pub struct HandlerAuthorization<A, M> {
    authorizer: A,
    metrics: M,
}

impl<A, M> HandlerAuthorization<A, M> {
    pub fn new(authorizer: A, metrics: M) -> Self {
        Self {
            authorizer,
            metrics,
        }
    }
}

impl<A, M> crate::handler_workflow::RequestAuthorization for HandlerAuthorization<A, M>
where
    A: Authorizer + Send + Sync,
    A::Error: Send + Sync,
    M: AuthorizationMetrics + Send + Sync,
{
    fn authorize(&self, operation: &str, request: &Value, actor: &str) -> Result<(), CadenceError> {
        authorize_request(operation, request, actor, &self.authorizer, &self.metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct Auth(Decision);
    impl Authorizer for Auth {
        type Error = &'static str;
        fn authorize(&self, _: &Attributes) -> Result<Decision, Self::Error> {
            Ok(self.0)
        }
    }
    #[derive(Default)]
    struct Metrics {
        calls: Cell<u32>,
        denied: Cell<u32>,
    }
    impl AuthorizationMetrics for Metrics {
        fn authorization_latency(&self, _: std::time::Duration) {
            self.calls.set(self.calls.get() + 1);
        }
        fn authorization_failed(&self) {}
        fn unauthorized(&self) {
            self.denied.set(self.denied.get() + 1);
        }
    }

    #[test]
    fn denial_has_go_error_message_and_metrics() {
        let metrics = Metrics::default();
        let err = authorize(&Auth(Decision::Deny), &metrics, &Attributes::default()).unwrap_err();
        assert_eq!(err.to_string(), "Request unauthorized.");
        assert_eq!(metrics.calls.get(), 1);
        assert_eq!(metrics.denied.get(), 1);
    }

    #[test]
    fn allow_preserves_success() {
        let metrics = Metrics::default();
        assert!(authorize(&Auth(Decision::Allow), &metrics, &Attributes::default()).is_ok());
        assert_eq!(metrics.denied.get(), 0);
    }

    #[test]
    fn generated_api_permissions_and_authorization_bypass_match_go() {
        let request = serde_json::json!({
            "domain": "shop",
            "workflowType": {"name": "checkout"},
            "taskList": {"name": "orders"}
        });
        let start = attributes_for_request("StartWorkflowExecution", &request, "worker").unwrap();
        assert_eq!(start.permission, Some(Permission::Write));
        assert_eq!(start.actor, "worker");
        assert_eq!(start.domain, "shop");
        assert_eq!(start.task_list, request.get("taskList").cloned());
        assert_eq!(start.workflow_type, request.get("workflowType").cloned());
        assert_eq!(start.request_body, Some(request.clone()));

        let private_request = serde_json::json!({"domain": "shop", "input": "secret"});
        let filtered =
            attributes_for_request("StartWorkflowExecution", &private_request, "worker").unwrap();
        assert_eq!(
            filtered.request_body,
            Some(serde_json::json!({"domain": "shop"}))
        );
        let async_request = serde_json::json!({
            "request": {"domain": "shop", "input": "secret", "signalInput": "secret"}
        });
        assert_eq!(
            {
                let attributes = attributes_for_request(
                    "SignalWithStartWorkflowExecutionAsync",
                    &async_request,
                    "worker",
                )
                .unwrap();
                assert_eq!(attributes.domain, "shop");
                attributes.request_body
            },
            Some(serde_json::json!({"request": {"domain": "shop"}}))
        );

        assert_eq!(
            attributes_for_request("PollForActivityTask", &request, "worker")
                .unwrap()
                .permission,
            Some(Permission::Process)
        );
        assert_eq!(
            attributes_for_request("CountWorkflowExecutions", &request, "worker")
                .unwrap()
                .permission,
            Some(Permission::Read)
        );
        assert!(
            attributes_for_request("RespondActivityTaskCompleted", &request, "worker").is_none()
        );
        assert!(attributes_for_request("Health", &request, "worker").is_none());
    }
}
