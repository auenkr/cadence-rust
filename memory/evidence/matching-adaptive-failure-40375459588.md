# Adaptive mixed run 40375459588 — failed

Run: https://github.com/auenkr/cadence-rust/actions/runs/40375459588

Source: `eba0b437df9096d44b82be91af542d164c5c5123`.
CI binary SHA256: `ce65db111892697fa110ebb66061779a0bcc8338aab9f8ee5cdcddb87f8f5d3f  /tmp/cargo-target-final-40375459588/debug/cadence-matching`.

Artifact exit records:
- `build.exit`: `0`
- `gate.exit`: `1`
- `gate/lane.exit`: `1`
- `gate/run/run.exit`: `1`

The probe failed at its SDK subprocess assertion. The SDK log reports `Completed 24 adaptive scaler load workflows`, then its later `workflow-task-retry` scenario hit the shared context deadline. Decision-task-not-found messages and injected workflow-task failures precede that timeout. This does not establish the cause as a fixture or production defect; diagnosis is assigned to Luna.

The adaptive scale/version/static-listing assertions precede this call in source, but no completed adaptive scenario result was emitted. Subsequent JavaScript per-load terminal-history checks and the live routing phase were not reached. Do not count the full adaptive lane as passed.

## Retry-path diagnosis (2026-10-27)

Inspected the preserved artifact at `/mnt/storage/service/ci-evidence/40375459588/matching-production-adaptive` (no binary was downloaded in this follow-up). The failure is at `tests/matching-probe-scenarios.cjs:273`, where the Go SDK process exits nonzero. In `tests/go-sdk/main.go`, `WorkflowTaskRetryWorkflow` deliberately panics on its first two executions; `runMixedMatchingSDK` awaits its result via `taskRetryRun.Get(ctx)` and then asserts history counts of one DecisionTaskFailed, two DecisionTaskScheduled and two DecisionTaskStarted events.

The artifact's `adaptive-scaler-sdk.log` identifies workflow `production-adaptive-scaler-probe-workflow-task-retry` / run `faefc0ae-dd18-4c57-96cf-e3f8bba745de`. The SDK worker starts at `18:15:22.078Z`; errors `Decision task not found` are logged at `18:15:45.330Z` and `18:15:57.925Z`, but neither includes a workflow ID. The injected panic for the retry workflow is logged at `18:16:12.164Z` and again at `18:16:12.409Z`; the SDK exits at `18:16:22Z` with the shared context deadline. These timestamps prove the suite context expired while awaiting this workflow, but they do not identify whether those `Decision task not found` messages belong to this workflow or establish why its next task was not completed.

The only matching History connection failure for this run is in Go `server.log` at `18:16:24.219Z`, after the probe stopped Rust Matching at `18:16:22Z` (Ringpop removes its address at `18:16:22.123Z`). It is teardown fallout and cannot explain the earlier missing SDK completion. The artifact's results mark `acceptanceEligible:false`; the Go-baseline failure prevents the comparison from establishing Rust/Go parity. `Go task_reader.go` documents retry dispatch after `RecordDecisionTaskStarted` errors, but the production History and task-reader paths alone cannot identify the failing attempt here because the run has no workflow-specific poll/response trace. A source-only Rust fix would be speculative.

Do not accept the old `t3/adaptive-sdk-retry-timeout` edit as a fix: it changes the shared mixed-suite deadline from 60 to 150 seconds but neither isolates nor explains the retry behavior, and no follow-up run proves that this is only a harness budget issue. Preserve all 24 load-workflow assertions, static `ListTaskListPartitions` semantics, and all 24 completion checks.

### Parent-owned experiment required

On a fresh mixed lane (Go Frontend/History/Worker, Rust Matching only), run a dedicated SDK process for the same workflow-task retry against the already-scaled task list, with a bounded scenario-specific deadline. Record the workflow/run ID, every `PollForDecisionTask` request's logical and partition task-list name, the matching partition that receives each task, task tokens / start acknowledgments, and the full resulting workflow history immediately on success or timeout. Also record the actual configured/read partition list separately from Go-compatible static `ListTaskListPartitions` output. Expected observations: the workflow completes and satisfies the existing SDK contract in `tests/go-sdk/main.go` (one DecisionTaskFailed, two DecisionTaskScheduled and two DecisionTaskStarted events); the retry task must be delivered to a queue the worker is actually polling. If History schedules the retry but Rust Matching does not deliver it to a poller on the matching partition, this is a production routing defect. If it is delivered and the SDK response/history succeeds, the original failure was suite-budget contention; then restore the 60-second shared budget and keep the bounded isolated regression. If the task is never scheduled or History reports start/complete errors, retain the corresponding RPC/task identifiers and pursue that exact service boundary. Do not infer a production retry bug from generic task-not-found logs.

Recommended gate dispatch (parent serializes mixed lanes): `gh workflow run matching-production-validation.yml --ref <reviewed-branch> -f lane=adaptive`; artifact observations to check are `adaptive-scaler-sdk.log`, workflow-history event counts, task poll queue names/owners, `probe.log`, and `results.json`. This worker did not dispatch a mixed gate or run local Go/Cargo/runtime commands.

Downloaded artifact root: `/mnt/storage/service/ci-evidence/40375459588`. Original log SHA256:

- `gate/run/probe.log`: `862a590325ce2d0beb2bd8d6c4446bd82484a56a9b5f154a593d357350beac83`
- `gate/run/rust-matching/adaptive-scaler-sdk.log`: `2999799c381d7132448010fc40d93c5273fdc6cece184560d7af7d4d52ee1998`
