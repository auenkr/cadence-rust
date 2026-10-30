# PostgreSQL queued-task owner SIGKILL audit (2026-10-30)

## Scope

Source review of the coherent harness at `6c73d9aa447171d4eeda37d71dd0a955d0996a9b` (with `565c8fc91893fae1a81cd8e47f10f2ee4744c046` in its ancestry). No runtime, Cargo, Go, CI, or test commands were run. `tests/matching-service-compat.cjs` was not edited.

The PostgreSQL lane runs the Rust Matching binary with Go Frontend/History/Worker and no Go Matching (`tests/gates/run-postgres-lane.sh`). The failure-recovery lane is enabled for production two-node runs by `tests/run-go-service-baseline.py`.

## Findings

### P1 — the queued activity restart probe does not target or prove the killed owner

`tests/matching-probe-scenarios.cjs::durableRestart` configures a three-partition activity list, persists one activity task in each partition, then calls `ctx.restartRust()`. In `tests/matching-service-compat.cjs`, that callback always SIGKILLs the primary process and restarts it. The probe never resolves the owner of any queued partition before the kill. With two Rust Matching members, the primary may own none of the queued partitions.

After restart, each poll goes through `ownerRpc`, which sends requests to candidate endpoints and retries a typed `ABORTED` on the next endpoint. The result proves the queued activity IDs were eventually returned once across the polls, but does not identify which process served them or prove the restarted process loaded/drained a task. This diverges from Go owner semantics: Go Matching's task-list manager is owned by the membership key for that partition and rejects a non-owner; a successful poll via a retrying client cannot establish which owner recovered it.

The durable check here is appropriately task type 1 and keyed by domain/list (`durableTaskCount` in `tests/matching-service-compat.cjs`); the partition name is unique to this probe. It only checks a positive row count, however, and does not capture the range for each activity partition. The separate owner-crash path captures a decision-task range, so it does not close the activity-specific range gap.

**Fix:** resolve and record the Ringpop owner for each activity partition before SIGKILL, select a queued partition owned by the process to be killed (or kill the actual selected owner), and assert that process exits by SIGKILL. After restart and membership convergence, route a drain poll directly to the owner endpoint, record the serving endpoint, and require at least one recovered queued task to be served by the restarted process. Observe exact `(domain, task_list_name, task_type=1)` row identity/count and the matching activity task-list `range_id` before and after drain. Keep Go ForwardPoll behavior in mind: child polls may be forwarded to a parent, so record requested and effective ownership/partition rather than assuming every requested child poll serves locally.

### P1 — the decision owner-crash row checkpoint can be satisfied by the preceding probe task

In `tests/matching-service-compat.cjs` production failure recovery, `crashProbeID` is first started on `crashList`; direct polls establish which Rust member owns the list, validate a typed non-owner response, and the probe decision is completed. Then `crashID` is started on the same `crashList`. `taskObserver.observe(domainId, crashList, ...)` only waits for `queued_rows > 0`. For PostgreSQL, `sqliteTaskState` dispatches to `durableTaskCount(..., taskType=0)`; that query counts rows by domain, list, and type but does not distinguish task ID or workflow/run/schedule ID. Because the completed probe's stored row may remain until ack-based GC, that earlier row can satisfy the later crash workflow's pre-kill assertion.

The owner selected by the direct poll is then SIGKILLed correctly, and the SDK completes `crashID` through Go Frontend. However, the post-SDK assertion requires only one completion event; it does not assert the exact decision/activity event sequence or correlate the pre-crash persisted row with `crashID`. The current evidence therefore does not rule out a stale-row false positive followed by ordinary History/Frontend retry or re-enqueue behavior.

**Fix:** use separate task lists for the owner-oracle task and crash workflow, or wait for the probe list to have zero task rows before starting the crash workflow. For the crash workflow, retain the exact task-row identity (or otherwise prove a zero-to-one transition on its unique list), record type 0 range and ack before SIGKILL, and assert exact expected history counts/attributes after SDK completion. Keep SDK errors/attempts visible so transparent Frontend failover or re-enqueue is not mistaken for proof that the persisted task was drained by the survivor.

## Existing checks that are sound

- The owner-crash scenario selects the process whose direct decision poll returned the task, requires the other member's direct poll to return typed `ABORTED`, and SIGKILLs that selected process.
- The crash workflow runs while the original Go Frontend/History/Worker processes remain in the topology; no Go Matching process is launched in the PostgreSQL lane.
- The post-crash decision path checks one visible workflow completion, nondecreasing task ID block, increasing acknowledgement, empty queued rows, and a strictly higher persisted range. These are useful recovery signals, but they do not fix the stale-row/workflow-correlation issue above.
- `durableRestart` checks that each of the three unique activity IDs is delivered once, consistent with Go's timing-dependent child/root poll forwarding. It does not identify the serving owner, which is the scope of the finding above.

## Validation status

Source audit only. No gate result is claimed and no change was made to the protected candidate harness. These findings are concrete acceptance-test false-pass paths; correct them in a separate harness change after the pending diagnostic candidate is integrated, then validate through the required PostgreSQL mixed-cluster CI lane.
