# Matching TaskCompleter Go → Rust test port (2026-10-24)

Branch: `matching-task-completer-tests-20261023` (from origin/master c5f95ea13)
Go spec: `go/service/matching/tasklist/task_completer.go`, `task_completer_test.go` (one test func `TestCompleteTaskIfStarted`, 16 subtests).
Rust hooks: `rust/crates/cadence-matching/src/matching_standby.rs`
(`task_started`, `missing_execution_result`, `exhausted`, `checker`). The Rust standby
checker is wired from `matching_registry.rs:1397`; the failover commit 64969c4e1 is
history only and touches no completer code.

No local cargo run (per task brief). Verification is on GitHub CI (see bottom).

## Subtest mapping

| # | Go subtest (task_completer_test.go) | Rust test | Status |
|---|---|---|---|
| 1 | error - could not get domain by ID from cache (:81, retries x4) | — | CANNOT-PORT: Go asserts `Times(retryPolicyMaxAttempts+1)` and the returned error after exhaustion. Rust `checker` builds its retrier internally (`matching_standby.rs:106-109`, 1s to 10s backoff, 300s expiry), so there is no attempt-count seam. Domain-missing retries are not otherwise observable without a 300s wait. |
| 2 | error - domain is active (:100) | `go_domain_active_is_not_completed` | PORTED (e2e). Go returns sentinel `errDomainIsActive`; Rust returns `Ok(false)`. Both mean "not completed" and emit no metric. Representation difference only. |
| 3 | error - could not fetch workflow execution from history (:118, retries x4) | — | CANNOT-PORT: same retry-count seam as #1 (`Times(4)` plus returned error after exhaustion). |
| 4 | error - no WorkflowExecutionInfo (:141) | `go_error_no_workflow_execution_info` | PARTIAL: pure `task_started` error is asserted. The end-to-end error after retries is not asserted (retry seam, see #1). |
| 5 | error - task type not supported (:164, taskType 999) | `go_error_task_type_not_supported` | PARTIAL: pure error asserted. End-to-end error after retries is not asserted (retry seam). |
| 6 | error - decision not started, scheduleID > PendingDecision (:190) | covered by `go_decision_not_started_when_schedule_id_greater_than_pending` | PARTIAL: pure `task_started` false asserted. Error after retries is not asserted (retry seam). |
| 7 | error - decision not started, equal scheduleID but not Started (:221) | `go_decision_not_started_when_same_schedule_id_not_started`, `go_decision_not_started_is_not_completed` | PORTED (pure + e2e first attempt: NOT_STARTED metric, not completed). |
| 8 | error - activity not started (:252) | `go_activity_not_started_when_pending_scheduled`, `go_activity_not_started_is_not_completed` | PORTED (pure + e2e). |
| 9 | error - workflow not found, created < 24h (:285) | `go_missing_execution_younger_than_24h_waits`, `go_missing_execution_younger_than_24h_is_not_completed`, `go_young_missing_execution_counts_completion_failure` | PORTED outcome, with PARITY GAP on returned error (see below). |
| 10 | complete - workflow not found, created > 24h (:307, Times(1), no retries) | `go_missing_execution_older_than_24h_completes` (e2e), `go_missing_execution_result_older_than_24h_completes` (pure) | PORTED (pure + e2e). Both use the real 24h threshold. |
| 11 | complete - workflow closed (:330) | `go_complete_task_workflow_closed` | PORTED (e2e). |
| 12 | complete decision - no pending decision (:359) | `go_complete_decision_no_pending_decision` | PORTED (e2e). |
| 13 | complete decision - scheduleID < PendingDecision (:386) | `go_complete_decision_schedule_less_than_pending` | PORTED (e2e). |
| 14 | complete decision - equal and PendingDecisionStateStarted (:416) | `go_complete_decision_equal_and_started` | PORTED (e2e). |
| 15 | complete activity - no matching scheduleID (:447) | `go_complete_activity_no_matching_pending_activity` | PORTED (e2e). |
| 16 | complete activity - matching scheduleID and Started (:484) | `go_complete_activity_started` | PORTED (e2e). |

Totals (16 Go subtests): 10 PORTED outcome tests (#2, 7, 8, 10, 11-16). 1 PORTED with parity gap (#9). 3 PARTIAL (#4, 5, 6: pure decision asserted, error-after-retries not). 2 CANNOT-PORT (#1, 3: retry-count seam).

## Parity gaps

1. **Young missing execution: returned error differs.** Go `task_completer.go:133` returns
   `errWaitTimeNotReachedForEntityNotExists` after counting a completion failure
   (`:162-166`). Rust `matching_standby.rs` `exhausted()` counts the same failure metric
   but returns `Status::unavailable("task not started")`. Counts match. The reader's
   logging differs: the Rust reader (`matching_reader.rs:597`) skips logging for "task not
   started", while Go logs any non-not-started completer error. Covered by the ignored test
   `go_young_missing_execution_returns_wait_error`.
2. **Retry policy is not injectable in Rust** (see #1, #3 above). Go's test shrinks the policy to
   3 attempts at 100ms. Rust uses production constants. This is a test-seam gap, not a behavior
   change. The Go attempt-count assertions cannot be ported without a seam, and this task does
   not invent one.

## CI evidence

(filled in after the CI run: run URL, head sha, conclusion)
