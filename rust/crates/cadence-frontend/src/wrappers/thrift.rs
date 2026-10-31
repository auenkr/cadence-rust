//! Thrift dispatch wrapper boundary (Go: wrappers/thrift).
pub const SERVICE_NAME: &str = "WorkflowService";

/// Return the generated method definition, including the pinned Thrift
/// exception field IDs, for the public WorkflowService surface.
pub fn method(name: &str) -> Option<&'static cadence_thrift::service::MethodDef> {
    cadence_thrift::service::METHODS
        .iter()
        .find(|method| method.service == SERVICE_NAME && method.method == name)
}

pub fn method_names() -> impl Iterator<Item = &'static str> {
    cadence_thrift::service::METHODS
        .iter()
        .filter(|method| method.service == SERVICE_NAME)
        .map(|method| method.method)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thrift_wrapper_uses_pinned_workflow_service_contract() {
        let start = method("StartWorkflowExecution").unwrap();
        assert_eq!(start.arg_type, "StartWorkflowExecutionRequest");
        assert!(start
            .exceptions
            .iter()
            .any(|(name, id)| *name == "BadRequestError" && *id == 1));
        assert!(method("NotAWorkflowMethod").is_none());
        assert!(method_names().any(|name| name == "GetWorkflowExecutionHistory"));
    }
}
