//! YARPC method table for `WorkflowService`, generated from the IDL
//! (`go/.gen/go/cadence/cadence.go` `rawIDL`). Exception field IDs come
//! from each method's `throws` clause.

/// One served method: exception Thrift-type name -> result field ID.
pub struct MethodDef {
    pub method: &'static str,
    pub arg_type: &'static str,
    pub exceptions: &'static [(&'static str, i16)],
}

pub const METHODS: &[MethodDef] = &[
    MethodDef { method: "RegisterDomain", arg_type: "RegisterDomainRequest", exceptions: &[("BadRequestError", 1), ("DomainAlreadyExistsError", 3), ("ServiceBusyError", 4), ("ClientVersionNotSupportedError", 5), ("AccessDeniedError", 6)] },
    MethodDef { method: "DescribeDomain", arg_type: "DescribeDomainRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("ServiceBusyError", 4), ("ClientVersionNotSupportedError", 5), ("AccessDeniedError", 6)] },
    MethodDef { method: "ListDomains", arg_type: "ListDomainsRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("ServiceBusyError", 4), ("ClientVersionNotSupportedError", 5), ("AccessDeniedError", 6)] },
    MethodDef { method: "UpdateDomain", arg_type: "UpdateDomainRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("ServiceBusyError", 4), ("DomainNotActiveError", 5), ("ClientVersionNotSupportedError", 6), ("AccessDeniedError", 7)] },
    MethodDef { method: "DeprecateDomain", arg_type: "DeprecateDomainRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("ServiceBusyError", 4), ("DomainNotActiveError", 5), ("ClientVersionNotSupportedError", 6), ("AccessDeniedError", 7)] },
    MethodDef { method: "StartWorkflowExecution", arg_type: "StartWorkflowExecutionRequest", exceptions: &[("BadRequestError", 1), ("WorkflowExecutionAlreadyStartedError", 3), ("ServiceBusyError", 4), ("DomainNotActiveError", 5), ("LimitExceededError", 6), ("EntityNotExistsError", 7), ("ClientVersionNotSupportedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "DescribeWorkflowExecution", arg_type: "DescribeWorkflowExecutionRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("LimitExceededError", 4), ("ServiceBusyError", 5), ("ClientVersionNotSupportedError", 6), ("AccessDeniedError", 7)] },
    MethodDef { method: "GetWorkflowExecutionHistory", arg_type: "GetWorkflowExecutionHistoryRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("ServiceBusyError", 4), ("ClientVersionNotSupportedError", 5), ("AccessDeniedError", 6)] },
    MethodDef { method: "SignalWorkflowExecution", arg_type: "SignalWorkflowExecutionRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("ServiceBusyError", 4), ("DomainNotActiveError", 5), ("LimitExceededError", 6), ("ClientVersionNotSupportedError", 7), ("WorkflowExecutionAlreadyCompletedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "SignalWithStartWorkflowExecution", arg_type: "SignalWithStartWorkflowExecutionRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("ServiceBusyError", 4), ("DomainNotActiveError", 5), ("LimitExceededError", 6), ("WorkflowExecutionAlreadyStartedError", 7), ("ClientVersionNotSupportedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "TerminateWorkflowExecution", arg_type: "TerminateWorkflowExecutionRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("ServiceBusyError", 4), ("DomainNotActiveError", 5), ("LimitExceededError", 6), ("ClientVersionNotSupportedError", 7), ("WorkflowExecutionAlreadyCompletedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "RequestCancelWorkflowExecution", arg_type: "RequestCancelWorkflowExecutionRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("CancellationAlreadyRequestedError", 4), ("ServiceBusyError", 5), ("DomainNotActiveError", 6), ("LimitExceededError", 7), ("ClientVersionNotSupportedError", 8), ("WorkflowExecutionAlreadyCompletedError", 9), ("AccessDeniedError", 10)] },
    MethodDef { method: "PollForDecisionTask", arg_type: "PollForDecisionTaskRequest", exceptions: &[("BadRequestError", 1), ("ServiceBusyError", 3), ("LimitExceededError", 4), ("EntityNotExistsError", 5), ("DomainNotActiveError", 6), ("ClientVersionNotSupportedError", 7), ("AccessDeniedError", 8)] },
    MethodDef { method: "RespondDecisionTaskCompleted", arg_type: "RespondDecisionTaskCompletedRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("DomainNotActiveError", 4), ("LimitExceededError", 5), ("ServiceBusyError", 6), ("ClientVersionNotSupportedError", 7), ("WorkflowExecutionAlreadyCompletedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "RespondDecisionTaskFailed", arg_type: "RespondDecisionTaskFailedRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("DomainNotActiveError", 4), ("LimitExceededError", 5), ("ServiceBusyError", 6), ("ClientVersionNotSupportedError", 7), ("WorkflowExecutionAlreadyCompletedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "PollForActivityTask", arg_type: "PollForActivityTaskRequest", exceptions: &[("BadRequestError", 1), ("ServiceBusyError", 3), ("LimitExceededError", 4), ("EntityNotExistsError", 5), ("DomainNotActiveError", 6), ("ClientVersionNotSupportedError", 7), ("AccessDeniedError", 8)] },
    MethodDef { method: "RespondActivityTaskCompleted", arg_type: "RespondActivityTaskCompletedRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("DomainNotActiveError", 4), ("LimitExceededError", 5), ("ServiceBusyError", 6), ("ClientVersionNotSupportedError", 7), ("WorkflowExecutionAlreadyCompletedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "RespondActivityTaskFailed", arg_type: "RespondActivityTaskFailedRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("DomainNotActiveError", 4), ("LimitExceededError", 5), ("ServiceBusyError", 6), ("ClientVersionNotSupportedError", 7), ("WorkflowExecutionAlreadyCompletedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "RespondActivityTaskCanceled", arg_type: "RespondActivityTaskCanceledRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("DomainNotActiveError", 4), ("LimitExceededError", 5), ("ServiceBusyError", 6), ("ClientVersionNotSupportedError", 7), ("WorkflowExecutionAlreadyCompletedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "RecordActivityTaskHeartbeat", arg_type: "RecordActivityTaskHeartbeatRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("DomainNotActiveError", 4), ("LimitExceededError", 5), ("ServiceBusyError", 6), ("ClientVersionNotSupportedError", 7), ("WorkflowExecutionAlreadyCompletedError", 8), ("AccessDeniedError", 9)] },
    MethodDef { method: "QueryWorkflow", arg_type: "QueryWorkflowRequest", exceptions: &[("BadRequestError", 1), ("EntityNotExistsError", 3), ("QueryFailedError", 4), ("LimitExceededError", 5), ("ServiceBusyError", 6), ("ClientVersionNotSupportedError", 7), ("AccessDeniedError", 8)] },
];

#[must_use]
pub fn find_method(procedure: &str) -> Option<&'static MethodDef> {
    let method = procedure.rsplit("::").next().unwrap_or(procedure);
    METHODS.iter().find(|m| m.method == method)
}
/// YARPC exception type names we can produce, for table validation.
#[must_use]
pub fn exception_field(method: &MethodDef, exc_name: &str) -> Option<i16> {
    method
        .exceptions
        .iter()
        .find(|(name, _)| *name == exc_name)
        .map(|(_, id)| *id)
}
