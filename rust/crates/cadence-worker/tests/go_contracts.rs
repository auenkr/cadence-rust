//! Literal contract checks against the checked-in Go Worker implementation.

const GO_PARENT_CLOSE: &str =
    include_str!("../../../../go/service/worker/parentclosepolicy/workflow.go");
const GO_BATCHER: &str = include_str!("../../../../go/service/worker/batcher/workflow.go");
const GO_BATCHER_V2: &str = include_str!("../../../../go/service/worker/batcher/workflow_v2.go");
const GO_SCANNER: &str = include_str!("../../../../go/service/worker/scanner/workflow.go");
const GO_SCANNER_EXECUTIONS: &str =
    include_str!("../../../../go/service/worker/scanner/executions/concrete_execution.go");
const GO_ARCHIVER: &str = include_str!("../../../../go/service/worker/archiver/client_worker.go");
const GO_FAILOVER: &str = include_str!("../../../../go/service/worker/failovermanager/workflow.go");
const GO_DOMAIN_DEPRECATION: &str =
    include_str!("../../../../go/service/worker/domaindeprecation/workflow.go");
const GO_SCHEDULER: &str = include_str!("../../../../go/service/worker/scheduler/types.go");
const GO_DIAGNOSTICS: &str = include_str!("../../../../go/service/worker/diagnostics/workflow.go");

fn has(source: &str, expected: &str) {
    assert!(source.contains(expected), "Go source lost contract {expected:?}");
}

#[test]
fn parent_close_policy_contract_matches_go_source() {
    has(GO_PARENT_CLOSE, "cadence-sys-processor-parent-close-policy");
    has(GO_PARENT_CLOSE, "cadence-sys-parent-close-policy-workflow");
    has(GO_PARENT_CLOSE, "cadence-sys-parent-close-policy-activity");
    has(GO_PARENT_CLOSE, "ParentClosePolicyProcessorChannelName");
}

#[test]
fn batcher_and_scanner_contracts_match_go_source() {
    has(GO_BATCHER, "cadence-sys-batcher-tasklist");
    has(GO_BATCHER, "cadence-sys-batch-workflow");
    has(GO_BATCHER_V2, "cadence-sys-batch-workflow-v2");
    has(GO_SCANNER, "cadence-sys-tl-scanner-tasklist-0");
    has(GO_SCANNER, "cadence-sys-tl-scanner-workflow");
    has(GO_SCANNER_EXECUTIONS, "cadence-sys-executions-scanner-workflow");
}

#[test]
fn remaining_initial_workflow_contracts_match_go_source() {
    has(GO_ARCHIVER, "cadence-archival-tl");
    has(GO_ARCHIVER, "archivalWorkflow");
    has(GO_FAILOVER, "cadence-sys-failoverManager-workflow");
    has(GO_FAILOVER, "cadence-sys-failoverManager-tasklist");
    has(GO_DOMAIN_DEPRECATION, "domain-deprecation-workflow");
    has(GO_DOMAIN_DEPRECATION, "domain-deprecation-tasklist");
    has(GO_SCHEDULER, "cadence-scheduler");
    has(GO_DIAGNOSTICS, "diagnostics-workflow");
}
