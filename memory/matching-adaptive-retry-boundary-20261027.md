# Adaptive workflow task retry boundary diagnosis (2026-10-27)

## Reproduced evidence

Read-only source artifact: `/mnt/storage/service/ci-evidence/40465991711/matching-production-adaptive` (mixed run `40465991711`, source `c268c99cde16c4b12e7ab7edd048c13443329ad3`). The retry-only SDK process began with 59.951s left. History persisted `WorkflowExecutionStarted`, `DecisionTaskScheduled`, `DecisionTaskStarted`, then a `DecisionTaskFailed` with cause `WORKFLOW_WORKER_UNHANDLED_FAILURE` at `07:19:32.014`; the worker panic was intentional (`injected workflow task failure`). The SDK timed out at 59.902s and its fresh history read still had four events. No second decision start or completion was observed.

Go History source explains why those four durable events do not prove retry delivery failed: `go/service/history/execution/mutable_state_task_generator.go` defines `defaultInitIntervalForDecisionRetry = 1m` and `defaultMaxIntervalForDecisionRetry = 5m`; `getNextDecisionTimeout` applies exponential retry delay. `go/service/history/execution/mutable_state_decision_task_manager.go:AddDecisionTaskFailedEvent` persists the failure for attempt zero and advances transient decision-attempt state; subsequent transient failures are not separate durable `DecisionTaskFailed` events. The retry-only SDK context and workflow execution timeout are both one minute (`tests/go-sdk/mixed_matching.go`). The observed initial attempt started around 1.5s after workflow scheduling, so the SDK's 60s wait expires just before the first History retry timer can re-add the decision task. This is a concrete harness timing boundary, not evidence of a Matching production defect. No timeout or panic-retry assertions were changed.

## Partition growth clue

The same `gate/run/server.log` reports Go Frontend/History updating the adaptive queue from 1 to 102 read/write partitions at `07:19:31`, then Frontend to 434 at `07:19:44`, on 24 load workflows with a 1 RPS threshold and 1s update interval. This can be an overload/metrics feedback effect or legitimate measured throughput; the artifact does not include enough scaler samples to distinguish them. Rust's `matching_registry.rs` intentionally mirrors Go `assumeEvenQPS` as root arrival QPS multiplied by write partition count when isolation assignment is off, so actual sampled QPS and count progression need to be captured before alleging a feedback bug.

## Diagnostic instrumentation added

With `CADENCE_MATCHING_RETRY_TRACE_TASK_LIST=production-adaptive-scaler-probe`, Rust Matching writes `matching_retry_boundary` lines to the matching process log for:

- `add_received`, including workflow/run/schedule IDs and root or child queue name;
- `add_sync_matched`, `add_enqueued`, or `add_forward_failed` disposition;
- `poll_received` and `poll_returned`, with task-token presence and execution identifiers (never token contents);
- `scaler_tick`, including sampled aggregate QPS, old read/write counts, new write count, threshold, and update interval.

`tests/run-go-service-baseline.py` enables this only for the existing adaptive retry-only lane. The next parent-run mixed gate can classify: no retry `add_received` before the 60s deadline means History timer/budget boundary; received Add without successful disposition indicates add/forward path; successful enqueue with an empty poll indicates queue/partition delivery; child queue names and scaler samples will show whether fanout accompanies the retry. Existing lane assertions remain unchanged.

## Recommended next experiment

Parent may rerun the existing adaptive lane with `--probe-adaptive-retry-only` after merging/building this instrumentation. Do not increase the 60s budget to make the test pass. If the parent needs to observe the minute-boundary retry while preserving the existing acceptance gate, add a separate opt-in History/Matching boundary observer to the existing probe process with its own bounded observation window, leaving the retry-only result assertion intact. First inspect trace lines and the History task visibility evidence from that run; only then choose a production fix.

This change is instrumentation only. No Go History behavior, workflow timeout, retry count, or static listing assertion was modified.
