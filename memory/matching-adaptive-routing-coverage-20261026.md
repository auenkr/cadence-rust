# Adaptive Matching routing coverage correction (2026-10-26)

## Scope

This is test harness and memory documentation only. It does not alter Go or Rust production code, configuration defaults, SDK assertions, or workflow behavior. The change is isolated to the adaptive scenario in `tests/matching-probe-scenarios.cjs`.

## Corrected phase order and queue

The probe keeps `production-adaptive-scaler-probe` as its sole adaptive decision queue and requires adaptive scaling to increase persisted write partition count and config version. The existing Go SDK subprocess then handles all 24 generated load workflows; each execution is checked for exactly one `WorkflowExecutionCompleted` event after the subprocess exits. Only after those checks does the routing phase reread `DescribeTaskList` and its current persisted `readPartitions`.

For each live read partition ID, the scenario starts a separate ordinary workflow through Go Frontend on the same production root queue, directly polls the corresponding real queue name through the Rust Matching client, confirms the expected workflow was delivered, and completes it through Go Frontend. No read/write maps are cloned and no synthetic routing root is created. Started routing workflow/run IDs are retained; `finally` checks their histories and terminates any execution that has not reached a terminal event. Directly completed results are verified in history.

The returned JSON records each actual workflow/run ID, live read ID, requested queue name, addressed Rust endpoint, delivery, and completion. The SDK result and its 24 completion assertions remain intact.

## What the probe can establish

The RPC response identifies the task's workflow execution, but neither `PollForDecisionTaskResponse` nor the Go Frontend workflow API exposes the internal partition manager/task-list instance that physically served a request after forwarding. Go Matching's `handler/engine.go` logs request task-list name and `RequestForwardedFrom`, and emits `TaskIsForwarded`; these can distinguish forwarded traffic in Go logs but do not identify a Rust internal serving partition to this external gate. Consequently the probe reports the addressed Rust Matching node and requested live read queue, and explicitly disclaims physical-serving-partition proof. A requested child name alone is not that proof.

Exact instrumentation proposal if physical service identity is required: add a test-only observability hook in the Rust Matching poll path, after task acquisition and before response serialization, that emits one structured gate event containing request ID, task-list root, resolved read partition ID, task ID/workflow run ID, and whether the task was forwarded. Have the harness correlate that event with the ordinary Frontend start and the returned task token/workflow run ID. Keep the hook disabled unless a dedicated test environment variable is set; do not add a response field or change production routing semantics. No such seam is implemented here.

## Validation and limits

Syntax-only validation is `node --check tests/matching-probe-scenarios.cjs` plus `git diff --check`. No Cargo/Go builds, mixed-cluster runtime gate, workflow execution, or Actions dispatch is performed in this correction. Therefore this records probe implementation and its evidence limits, not a successful adaptive routing runtime result. Runtime validation remains pending on the approved Actions workflow.

Integration review corrections: routing tasks explicitly complete their workflow with a checked result, poll response run ID must match, cleanup attempts every recorded execution before raising accumulated errors, and evidence names candidate endpoints rather than falsely identifying the responding node after owner retries. Runtime validation remains pending.
