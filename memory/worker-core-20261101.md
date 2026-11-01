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

## Steps 1-3 (Message 4)
- parentclosepolicy ProcessorActivity ported; WorkerService::start() starts enabled pollers in Go order, unported modules disabled with log.
- SDK features (sdk/features.rs): child workflows (start/cancel/deferred cancel/results), signal/cancel external, search attribute upsert, Go-faithful GetVersion (Version marker + CadenceChangeVersion upsert), local activities (LocalActivity marker, replay from marker). Replay tests in sdk/tests.rs.
- Known gaps: sessions, sticky, mutable side effects, LA backoff timers/cancellation.
- CI: https://github.com/auenkr/cadence-rust/actions/runs/41290487002 success (package=cadence-worker).
- Pending: step 4 (batcher, scanner workflows).

## Step 4a: batcher (done)
- rust/crates/cadence-worker/src/batcher/{workflow,activity,grpc}.rs: v1 + v2 workflows, both activities (shared run_batch), gRPC backend (frontend, visibility, admin resend), wired into WorkerService::start under enable_batcher on domain cadence-batcher / task list cadence-sys-batcher-tasklist.
- SDK additions: ActivityOptions.wait_for_cancellation, ActivityFailure::CanceledWith(details), ActivityFuture::get_detailed (cancel details).
- Tests: workflow replay (v1 options/result, invalid params, v2 tune-signal cancel + restart with progress), activity logic with fake backend.
- CI: https://github.com/auenkr/cadence-rust/actions/runs/41291322183 success.

## Step 4b: scanner (partial)
- scanner/daemon_workflows.rs: TaskListScannerWorkflow and HistoryScannerWorkflow on the SDK runtime (activity options identical to Go), replay test added.
- Remaining and NOT started (module stays NotPorted in startup_plan):
  - shardscanner scanner/fixer workflows (executions concrete/current, timers): need workflow.Go-style concurrent coroutines + self-signal channels in the SDK, plus the aggregators and 8 query handlers.
  - All scanner ACTIVITIES (tasklist scavenger, history scavenger, scan/fix shard, config/emit-metrics, corrupted keys) need persistence access (task manager, history manager, execution manager, blobstore); the worker crate currently talks to the cluster only over gRPC, so a persistence factory wired from service config is a prerequisite.

## Step 4c: domaindeprecation (done, CI run 41293164627 green at 34c20e2d6)
- domaindeprecation/workflow.rs: DomainDeprecationWorkflow on the SDK (activity options and retry policy equal to Go, force skips the poller check, child BatchWorkflowV2 with Go ids/timeouts, 3 attempts with visibility sleep, open-workflow check). Child id uses a deterministic UUID derived from run id and attempt because the Rust runtime matches child decisions by workflow id (Go uses uuid.New(); a Go-started in-flight deprecation replayed by Rust would not match its recorded child id).
- domaindeprecation/activity.rs: 4 activities over a `Backend` trait + gRPC frontend backend (DescribeDomain, UpdateDomain with update_mask, DeprecateDomain, CountWorkflowExecutions, remote GetTaskListsByDomain). Non-retryable reasons identical to Go.
- service.rs: `start_domain_deprecation` (always on, as in Go) on cadence-system / domain-deprecation-tasklist; also registers batcher workflows+activities there (Go gets them from the SDK global registry because the child runs on the same task list). Added `spawn_worker`, `batcher_backend` helpers.

## Remaining step-4 status (not started)
- scheduler: needs a cron (standard 5-field, robfig semantics) implementation, Go-JSON-compatible serde for types.Schedule* / StartWorkflowExecutionRequest / time.Time, workflow (selector over timer, watcher activity, 5 signal channels, local activities processScheduleFire), watchWorkflowActivity and the per-domain WorkerManager (client_worker.go). No coroutines are required (poll_fn selector is enough).
- failovermanager: workflows + activities (frontend/admin gRPC); Go rebalance/failover v2 use workflow.Go; coroutine support still missing in sdk.
- archiver: workflow uses workflow.Go, buffered channels, selector and a local activity; activities need persistence (DeleteHistoryBranch) and archiver providers (filestore/gcloud/s3), so it depends on the persistence factory.
