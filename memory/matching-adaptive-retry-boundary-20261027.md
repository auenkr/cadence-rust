# Adaptive workflow task retry boundary diagnosis (2026-10-27)

## Reproduced evidence

Read-only source artifact: `/mnt/storage/service/ci-evidence/40465991711/matching-production-adaptive` (mixed run `40465991711`, source `c268c99cde16c4b12e7ab7edd048c13443329ad3`). The retry-only SDK process began with 59.951s left. History persisted `WorkflowExecutionStarted`, `DecisionTaskScheduled`, `DecisionTaskStarted`, then a `DecisionTaskFailed` with cause `WORKFLOW_WORKER_UNHANDLED_FAILURE` at `07:19:32.014`; the worker panic was intentional (`injected workflow task failure`). The SDK timed out at 59.902s and its fresh history read still had four events. No second decision start or completion was observed.

Go History source shows that a workflow task failure should schedule another transient decision immediately. `HandleDecisionTaskFailed` calls `AddDecisionTaskFailedEvent`; `ReplicateDecisionTaskFailedEvent` calls `FailDecision(true)`, which increments `DecisionAttempt` and leaves a pending decision. On commit, `mutable_state_task_refresher.go:refreshTasksForDecision` invokes `GenerateDecisionScheduleTasks`, which writes a decision transfer task. `transfer_active_task_executor.go:processDecisionTask` then reaches `transfer_task_executor_base.go:pushDecision`, which calls Matching `AddDecisionTask`. This is the failure-to-Matching path that needs observation.

The one-minute constants were misread in an earlier draft of this note. `mutable_state_task_generator.go:GenerateDecisionStartTasks` calls `getNextDecisionTimeout` only after a retry attempt has been started; it sets that attempt's start-to-close timer based on the stored `DecisionStartToCloseTimeout`. It does not delay scheduling or adding the retry. The SDK's 60s context and workflow timeout therefore do not, by themselves, explain the failure. No timing-defect conclusion is supported.

Only the first failed attempt emits a durable `DecisionTaskFailed` event (`AddDecisionTaskFailedEvent` checks `dt.Attempt == 0`). Transient retry scheduling/start events are omitted from history, so the four-event history is compatible with either a retry add/poll that has not started yet or a failure on the path before Matching delivery. The current artifact lacks the boundary trace added here and cannot distinguish those cases. No production cause is established yet.

## Partition growth clue

The same `gate/run/server.log` reports Go Frontend/History updating the adaptive queue from 1 to 102 read/write partitions at `07:19:31`, then Frontend to 434 at `07:19:44`, on 24 load workflows with a 1 RPS threshold and 1s update interval. This can be an overload/metrics feedback effect or legitimate measured throughput; the artifact does not include enough scaler samples to distinguish them. Rust's `matching_registry.rs` intentionally mirrors Go `assumeEvenQPS` as root arrival QPS multiplied by write partition count when isolation assignment is off, so actual sampled QPS and count progression need to be captured before alleging a feedback bug.

## Diagnostic instrumentation added

With `CADENCE_MATCHING_RETRY_TRACE_TASK_LIST=production-adaptive-scaler-probe`, Rust Matching writes `matching_retry_boundary` lines to the matching process log for:

- `add_received`, including workflow/run/schedule IDs and root or child queue name;
- `add_sync_matched`, `add_enqueued`, or `add_forward_failed` disposition;
- `poll_received` and `poll_returned`, with task-token presence and execution identifiers (never token contents);
- `scaler_tick`, including sampled aggregate QPS, old read/write counts, new write count, threshold, and update interval.

`tests/run-go-service-baseline.py` enables this only for the existing adaptive retry-only lane. On the next parent-run gate, no retry `add_received` means investigate the upstream History commit/transfer-queue execution path; a received Add without successful disposition localizes to Matching add/forward handling; successful enqueue with an empty poll localizes further to Matching dispatch/poll or worker polling. Child queue names and scaler samples will show whether partition fanout accompanies the retry. Existing lane assertions remain unchanged.

## Recommended next experiment

Parent may rerun the existing adaptive lane with `--probe-adaptive-retry-only` after building this instrumentation. Keep the 60s assertions unchanged. If no Add reaches Matching, correlate the trace with original History transfer-processor logs and the workflow's persisted transfer task/visibility time; if those don't expose the boundary, add a test-only observer to the existing gate. First inspect those boundaries and the scaler samples; only then choose a production fix.

This change is instrumentation only. No Go History behavior, workflow timeout, retry count, or static listing assertion was modified.

## Validation

Focused Actions run [40468304743](https://github.com/auenkr/cadence-rust/actions/runs/40468304743) completed successfully for `cadence-matching` with the Go SDK fixture checks enabled. It validated instrumentation source head `7d072c25422c165c92e6912d4210de7b8bb741a4`; the subsequent commit `8ea7b0ee20302aec2a5f9e2c900c1545f595327a` only corrected this memory note. No mixed gate was dispatched by this worker.
