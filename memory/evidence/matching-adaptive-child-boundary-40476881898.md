# Matching adaptive probe: child workflow boundary (run 40476881898)

## Finding

The failed SDK assertion is on the Go SDK parent waiting for its child. The
available evidence identifies the child execution and proves the parent
decision that initiated it completed. It does not include the child's own
terminal history, nor enough child-specific poll evidence to prove whether its
first decision task was delivered to the worker. Thus the precise unresolved
boundary is **after `ChildWorkflowExecutionStarted` and before a recorded child
decision-task start/completion or child terminal event**. This run alone does
not establish a concrete Matching code defect.

No SDK deadline or assertion was changed. No production code was changed.

## Immutable identifiers

- CI run: `40476881898`.
- Domain: `service-compat-8f0a6aa9-d662-42e7-8cef-1d4d6cb576fb`
  (`01a17c95-b343-772f-b01e-6f5a72b2e877`).
- Parent workflow/run: `production-adaptive-scaler-probe-child` /
  `f34e04d2-640f-4ba1-92af-2e1275119a67`.
- Actual child workflow/run: `f34e04d2-640f-4ba1-92af-2e1275119a67_0` /
  `3f1523e1-3835-4e02-9f54-af0605083cfa` (parent history event 6).
- Parent child-initiation history event: event 5 is
  `StartChildWorkflowExecutionInitiated` (`initiatedEventId=5` in child-start
  attributes), and event 6 is
  `ChildWorkflowExecutionStarted`.
- Child's first decision schedule/event: child history event 2, hence
  `schedule_id=2`. This is a different task from the parent's later decision
  schedule 7.

## Correlated timeline

All timestamps below are raw Unix nanoseconds from captured histories or
`matching_adaptive_trace` records. IDs in parentheses qualify each record.

| UTC | Event and identity |
| --- | --- |
| 08:59:40.381402255 | Parent `f34e…` event 1 `WorkflowExecutionStarted`; workflow type `main.ParentWorkflow`, root task list `production-adaptive-scaler-probe`. |
| 08:59:43.424052015 | Parent `f34e…` event 3 `DecisionTaskStarted` (`schedule_id=2`). |
| 08:59:43.460483961 | Parent event 4 `DecisionTaskCompleted`; decision initiates child. |
| 08:59:43.460554936 | Parent event 5 `StartChildWorkflowExecutionInitiated`, requested child workflow ID `f34e…_0`, type `main.ChildWorkflow`, task list root. |
| 08:59:43.527712010 | Parent event 6 `ChildWorkflowExecutionStarted`; server assigned child run `3f1523e1-3835-4e02-9f54-af0605083cfa`. |
| 08:59:43.527721344 | Parent event 7 `DecisionTaskScheduled` (`schedule_id=7`) on root list. This remains a **parent** decision. |
| 08:59:43.033884970 | Trace shows parent `f34e…`, `schedule_id=2`, decision Add begins on partition `/__cadence_sys/production-adaptive-scaler-probe/34`. (This request begins before the history's decision start/completion timestamps due to concurrent trace clocking/order; do not infer event order from this unsynchronized request start.) |
| 08:59:43.421439216 | That parent Add's `sync_match` is unmatched; `matched=false` at .421578511. |
| 08:59:43.448183749 | Parent task enqueued successfully on partition 34, `schedule_id=2`, status `ok`. This is the parent decision delivery path, not the child task. |
| 08:59:43.845639763 | Child run `3f1523e1…`, `schedule_id=2` Add begins on partition `/__cadence_sys/production-adaptive-scaler-probe/36`. |
| 08:59:43.874199270 | Child Add sync result says `matched=false`. |
| 08:59:43.928545165 | Child run `3f1523e1…`, `schedule_id=2` was enqueued on partition 36, status `ok`. This establishes Matching accepted/enqueued the child's first decision task. |
| 08:59:44.060483961 | Parent history event 9 `DecisionTaskCompleted` for parent schedule 7 / started event 8. This does **not** establish child task delivery or child completion. |
| 09:00:18.416 | SDK worker stopped after the scenario's remaining suite budget expired. SDK reports parent wait timed out. Its queried parent history has exactly 9 events and no `ChildWorkflowExecutionCompleted/Failed/TimedOut` event. |

The timestamps on trace rows are retained as captured; the Add `sync_start`
record precedes the corresponding history `DecisionTaskStarted` time, so the
trace's event timestamp and history event timestamp are not a single ordered
clock for causality. The enqueue and history events independently establish
that a parent decision completed and the child execution was started.

## Source and evidence

- Trace extractor: `tests/extract-matching-adaptive-trace.py`. It preserves
  workflow/run/schedule IDs and explicitly does not join events by timestamp.
- Extracted trace: `memory/evidence/matching-adaptive-traced-single-40476881898/matching-adaptive-trace.log`.
- Raw SDK + queried parent history: `/mnt/storage/service/ci-evidence/40476881898/matching-production-adaptive-single-diagnostic/gate/run/rust-matching/adaptive-scaler-sdk.log`, lines 52–70. Parent history events 1–9 are logged with full attributes; event 6 supplies the actual child run ID.
- Probe calls the existing SDK subprocess in `tests/matching-probe-scenarios.cjs`, `adaptiveScaler`; it keeps the original assertion that Go SDK subprocess exits zero, then checks all 24 load workflows for terminal completion. Child case is logged as an individual scenario.
- SDK workflow implementation/registrations: `tests/go-sdk/main.go`, `ParentWorkflow` and `ChildWorkflow`; parent uses `workflow.ExecuteChildWorkflow(...).Get(ctx, &result)`.
- Harness result: raw `results.json` says `probeExitCode: 1`, `acceptanceEligible: false`; the failure is not a passing acceptance run.
- Captured `rust-matching-data/executions.json` is empty (`runs`, `children`, and other arrays empty), so it cannot bridge the child execution to its persisted history. Visibility snapshots are for other completed load workflows, not this child.

## Most useful next discriminator

Retrieve the **child's own history** by domain and immutable execution
`workflow_id=f34e04d2-640f-4ba1-92af-2e1275119a67_0`,
`run_id=3f1523e1-3835-4e02-9f54-af0605083cfa`, including pagination and
terminal status. Correlate event 2 (`DecisionTaskScheduled`) with its
`DecisionTaskStarted`/`DecisionTaskCompleted` or timeout, and capture the
matching poll's returned workflow/run/schedule identity and task token event
IDs. If no start exists despite the successful enqueue, next inspect the
persisted matching queue/backlog row for child run + schedule 2 and the poll
partition/owner path at that interval. If a start exists, follow subsequent
decision Adds and child history until a terminal event. This is the smallest
discriminator that separates task delivery/acknowledgement from decision
processing and child terminal signaling.

## Limits

The matching poll trace has multiple consumers and most poll result rows only
contain `started_event_id`; several are blank identity returns. A parent
`poll_result` with `started_event_id=8` maps to parent history event 8, not the
child's `schedule_id=2`. The captured parent history is not child history.
Therefore neither successful parent decision completion nor an enqueued child
task proves child delivery, and absence of child terminal event from the
parent's nine-event history does not prove that the child never ran. No source
evidence here isolates a Matching defect or supports a proposed production
fix.
