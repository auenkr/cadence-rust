# History: RespondDecisionTaskCompleted port (branch history-dtcompleted, 2026-11-11)

## Done
- `engine_decision.rs`: `handle_decision_task_completed` ported from Go `HandleDecisionTaskCompleted`. It covers the token check, running and decision_info checks, heartbeat-timeout handling, DecisionTaskCompleted event, sticky set/clear, decision failed (cause mapped), new-decision creation with optional immediate start, and the conflict retry loop (5 attempts).
- `engine_decision_ops.rs` (new): `Operations` over `EngineMutableState` (`EngineOps`).
  - Real: complete/fail/cancel workflow, schedule activity, request-cancel/cancel activity, start/cancel timer, record marker, set execution context, size checks, attribute validation (same-domain only), cron/retry backoff helpers.
  - Unit tests: complete-workflow, schedule-activity, start-timer, invalid attributes lead to decision failed.

## Gaps (explicit "not ported" errors naming the decision type)
continue-as-new, cron/retry continue-as-new, start child workflow, external cancel, external signal, upsert search attributes, local activity dispatch, semaphores, last-completion-result. Also not ported: bad-binary check, auto-reset points, decision attempt-limit terminate, client headers, buffered query handling, search-attribute validation policy, cron validation, cross-domain validation.

## CI
- rust-ci 42985699796 failed (trait import in tests, clippy lints), head 80282cf5.
- rust-ci 42986451504 failed (test state workflow_timeout=0), head e888db73.
- rust-ci 42987382521 GREEN, head 9978cb224cd11a13cbaa02315758e3786dec17d1.

## Mixed gate
history-mixed-gate run 42988554202 (mysql, grpc, two_history=false), head 9978cb22: FAILED, but past the previous blocker. sdk-complete-echo is no longer in the failed list.
Failed: sdk-complete-child, sdk-refresh-workflow-tasks, sdk-sched-backfill-completed, sdk-sched-describe-after-backfill-grpc, sdk-sched-update-grpc, sdk-api-poll-activities, sdk-driver-exit, admin-probe-get-cross-cluster-tasks-deprecated, admin-probe-reapply-events-unknown-workflow.
Root causes seen: the Go scheduler workflow's decisions fail with "search attribute validation is not p(orted)" (upsert search attributes / search policy gap); sdk-complete-child needs StartChildWorkflowExecution. Others not yet root-caused.

## Next first failure
sdk-complete-child (StartChild decision), then upsert-search-attributes + search policy validation (scheduler), then root-cause refresh-workflow-tasks, poll-activities, driver-exit and the admin probes.
