# history-gap-apitypes2 (2026-11-07)
Branch history-gap-apitypes2.
1. state_builder: WorkflowExecutionStarted now sets execution_info["parent_domain_id"] from attributes.parent_workflow_domain_id (Go state_builder.go:139). Name->ID cache fallback not ported (state builder has no domain cache).
2. Thrift IDL: go/.gen/go/shared WorkflowExecutionStartedEventAttributes has neither ParentWorkflowDomainID nor jitterStartSeconds (types-only in Go); Rust decodes None - matches Go. Jitter exists in Thrift for StartWorkflowExecutionRequest/ContinueAsNew decision (already ported).
3. Semaphore*/RequestCancelActivityTaskFailed attrs already in cadence-api with Thrift/proto converters (prior branch).
4. Added tests: thrift round-trip (convert.rs) and proto event conversion (convert/history.rs).
