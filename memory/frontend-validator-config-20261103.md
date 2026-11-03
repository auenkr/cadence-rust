# Frontend validator/limits config (2026-11-03)
Branch t3/frontend-validator-config, head bb05fb742cbfc8a6ca89a5d724e36315f86da468
- rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/41679258473 success
- mysql mixed gate (kafka, two_history, rust_ref=branch): https://github.com/auenkr/cadence-rust/actions/runs/41679252499 success
- Rust binary SHA256: a67fa55dc637d2f7a6a9053f57a58ee43efa5a1707aad12a1d1ebd798f45902e  /tmp/cadence-cargo-target-fe-41679252499/debug/cadence-server
Note: gate must be dispatched with -f rust_ref=<branch>; default builds fe-integ.
Gate ID step uses 250 chars (MySQL workflow_id VARCHAR(255)); 1001 chars rejected with "WorkflowID length exceeds limit.".
Wired: limit.*Length (domain/workflowID/workflowType/taskList/requestID/signalName/identity), limit.maxIDWarnLength, limit.blobSize.error/warn, system.disallowQuery, frontend.historyMaxPageSize, validator keys (visibility page sizes, Lockdown, maxRetentionDays, maxFailoverTimeoutInSeconds, requiredDomainDataKeys). Thrift admin ResendReplicationTasks added.
Remaining: sendRawWorkflowHistory, Respond* identity checks, rate-limit keys, warn metrics; two-cluster resend steps not run by this gate.
