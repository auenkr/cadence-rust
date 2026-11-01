# Worker core — 2026-11-01

## Diagnostics pure logic
Ported in `diagnostics.rs` (Go-derived tests): runbook links, root-cause strings, `getIssueType`,
`capNumberOfIssues`, starter/workflow input wire names, usage-data Pinot payload (sorted keys).
Not ported: workflow bodies, activities needing frontend/messaging clients, invariants (timeout, failure, retry, timeoutrisk, antipatterns).
CI: https://github.com/auenkr/cadence-rust/actions/runs/41286336454 — success at `c34015c62`.

## Diagnostics invariants (pure logic)
`diagnostics/invariants.rs`: retry (`checkRetryPolicy`, heartbeat rule), timeoutrisk rules, failure
classification and root cause map, timeout poller/heartbeat root causes and type strings, antipattern burst
detection (sliding window, merge of overlapping clusters) and strings. Not ported: history traversal and
metadata JSON (needs HistoryEvent bindings), cron continue-as-new check, DescribeTaskList call, workflow bodies.
CI: https://github.com/auenkr/cadence-rust/actions/runs/41286623128 — success at `f8aac499a` (before burst detection; rerun below).
Burst detection CI: https://github.com/auenkr/cadence-rust/actions/runs/41286926955 — success at `57a00a8cf`.

## Cadence client/workflow runtime (rust/crates/cadence-worker/src/sdk)

Design:
- `converter`: Go JSON data converter (value + "\n"; side-effect marker details = id + base64 value).
- `core`: per-run state (decision list with keys, shared `GenerateSequence` id counter for activities/timers/side effects), `WorkflowContext`, futures (activity, timer, signal, cancel). Workflows are non-Send async fns polled once per history batch with a no-op waker.
- `replay::process_decision_task`: splits history into decision-task batches, applies markers first, applies reply events, polls the workflow, then matches generated decisions against recorded decision events in order (NonDeterministic error otherwise). The final batch yields the decisions to send; queries are answered from registered handlers.
- `decision_poller` (PollForDecisionTask / RespondDecisionTaskCompleted / Failed / RespondQueryTaskCompleted, sticky disabled), `activity` (poll, heartbeat with cancel flag, completed/failed/canceled responses, retry of transient respond errors, panics reported as `cadenceInternal:Panic`).
- parentclosepolicy `ProcessorWorkflow` ported on this runtime (`parentclosepolicy::processor_workflow`, `register_sdk`); `WorkerService::start_parent_close_policy_poller` runs the poller when `enableParentClosePolicyWorker`. The processor activity (needs history/domain-cache clients) is not yet ported.

Known gaps: child workflows, external signal/cancel, search attribute upsert (Version marker), local activities, sessions, sticky execution, activity WaitForCancellation.

Status: steps 1-4 and parentclosepolicy workflow done. CI green: https://github.com/auenkr/cadence-rust/actions/runs/41288651422 (head 1dedcdd7e, package=cadence-worker).
