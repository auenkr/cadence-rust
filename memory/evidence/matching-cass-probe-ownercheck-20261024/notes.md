# Harness fix: paced-list owner check for `DescribeTaskList must report the reported backing rate`

Branch `matching-cass-probe-ownercheck-20261024` from `origin/master` (8d31b6f61).
Implements the fix proposed in `memory/evidence/matching-cass-rate-diag-20261024/notes.md`
(verdict: the describe was answered by a second in-memory owner of `paced-*`; the probe never
checked ring agreement).

**Status: NOT gate-run.** Verified only with `node --check tests/matching-service-compat.cjs`.
No cargo, docker, or gate was run. No Rust production code changed.

## Change summary (`tests/matching-service-compat.cjs` only, +43 lines)

1. **Pre-poll ownership wait.** Right after `const pacedList`, when
   `CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT` is set (two-node lanes), build a primary and a secondary
   direct client and query `directPartitionOwner` for `pacedList.name` on both. Poll every 1 s for up to
   30 s until both agree. If they still disagree at the deadline, assert with
   `paced list owners did not agree before the poll: ...`. Runs before `StartWorkflowExecution`.
2. **Pre-describe re-check.** Right before the `DescribeTaskList` call, re-observe both owners. If they
   disagree, or differ from the pre-poll owners, throw
   `paced list owner changed between poll and describe: ...` with both observations.
3. **Post-describe re-check on rate mismatch only.** If `ratePerSecond !== 1`, re-observe owners. If they
   changed, throw the same owner-changed message (including `ratePerSecond`) instead of the rate assertion.
   Otherwise the original `assert.equal(... ratePerSecond, 1, ...)` runs unchanged.
4. **Per-node owner logging.** Each observation (`before-paced-poll`, `before-paced-describe`,
   `after-paced-describe`) is printed as a JSON line on stdout (`pacedListOwner`) and the full log is
   written to `<work>/paced-owner.json`.

Single-node runs (no secondary port) skip all checks; `pacedOwnershipBefore` is `null` and the
original rate assertion is unchanged.

## Not done / gaps

- **rangeID is not logged.** `ListTaskListPartitions` returns only `key` and `owner_host_name`
  (`go/proto/persistenceblobs/v1/message.proto:793`). The only DB helper (`matching-task-observer.cjs`)
  blocks until rows are queued or empty; it has no non-blocking read, so adding it was out of scope for
  a minimal harness change. Per-node rangeID still has to come from the Rust node logs
  (`rust-server-*.log`, e.g. `Matching task list lost ownership ... rangeID`).
- The describe itself is not bracketed by a re-check on the success path; a change that happens during
  the describe call and still yields rate 1 is not detected. Only a wrong rate triggers the post-check.
- The pre-poll wait hard-fails after 30 s if owners never agree. This is a new failure mode; it is
  intended (the rate probe is not meaningful without agreement), but it is a choice.

## Next step (for the orchestrator, not done here)

Run the Cassandra lane gate once the gate worker is free, from a clean `CARGO_TARGET_DIR` per AGENTS.md.
Check `paced-owner.json` for whether the split happens, and whether the owner-changed message fires
instead of the rate assertion.
