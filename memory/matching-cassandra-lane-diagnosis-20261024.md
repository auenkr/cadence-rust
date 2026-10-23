---
name: matching-cassandra-lane-diagnosis-20261024
description: Gate final2 lane 4 (Cassandra) failure diagnosis: fresh capacity-1 semaphore grants nothing because Go never seeds free token rows; probe expectation is wrong vs Go, not a Rust divergence
metadata:
  type: project
---

# Lane 4 (Cassandra) failure diagnosis, 2026-10-24

Branch read: `origin/matching-gate-final2-20261023` (HEAD bbd8ea86d). Worktree: `wt/cassdiag`, diag branch `matching-cassandra-diag-20261024`.
Attempts read: `4-cassandra` (run.exit=1), `4-cassandra-attempt1-failed`, `4-cassandra-attempt2-failed`. Both attempts fail identically.

## Exact failure

Step: Go-baseline production probe, the Cassandra semaphore block of `tests/matching-service-compat.cjs` (CADENCE_MATCHING_CASSANDRA_GATE=1), after `CreateSemaphore` (capacity 1, bucketCapacity 1) succeeded and two concurrent `AcquireSemaphore` calls went to Rust Matching.

```
PROBE BODY FAILED: AssertionError [ERR_ASSERTION]: Cassandra semaphore capacity one must grant exactly one concurrent owner; grants=[{"outcome":"SEMAPHORE_ACQUIRE_OUTCOME_NO_SLOT","tokenId":0},{"outcome":"SEMAPHORE_ACQUIRE_OUTCOME_NO_SLOT","tokenId":0}]
0 !== 1
    at run (/mnt/storage/service/github/wt/gatefinal2/tests/matching-service-compat.cjs:542:14)
```

Harness assertion: `tests/matching-service-compat.cjs:543` (`assert.equal(acquired.length, 1, ...)`). Run ends at `run-go-service-baseline.py:1060` (`RuntimeError: Go baseline probe failed`), exit 1.

## Environment ruled out (class c)

- Cassandra started and initialised `cadence.semaphore_metadata`, `semaphore_tokens`, `semaphore_tasks` (`4-cassandra-attempt2-failed/cassandra-container.log`). The `OutOfMemory` grep hits are JVM argument strings, not errors.
- The probe got past Frontend `CreateSemaphore`, the Rust gRPC acquire path, and both Rust instances. Failure is an assertion on a valid response, not a timeout or connection error.
- Same failure in attempt1 and attempt2, with no timeout. Not flaky.

## Root cause (verified in source)

A slot's free token row is never created. A fresh semaphore therefore has an empty free set, and any acquire returns NO_SLOT.

**Go (pinned source):**
- `go/service/matching/semaphore/semaphore_manager.go:426-490` `loadTokenOwnership`: the free set is built only from scanned `SemaphoreRowTypeToken` rows with `Holder == ""` (lines 452-456). Nothing else adds ids.
- `semaphore_manager.go:494-505` `reserve()`: `if len(m.freeList) == 0 { return 0, false }`. The only draw source is `freeList`.
- `semaphore_manager.go:319-330` `grant()`: `reserve` fails, then `break`. `semaphore_manager.go:386` returns `NoSlot`.
- `semaphore_manager.go:262-289` `Acquire`: NO_SLOT then `enqueue` (waiter). No other path.
- `semaphore_manager.go:403` comment says "Token rows are seeded once and never deleted". No code in `go/` does that.
- `go/common/persistence/nosql/nosqlplugin/cassandra/semaphore_tokens_cql.go:27-28`: "Token rows are not seeded: a slot's first grant creates its row". The row creation is a side effect of a grant that the manager never issues for a never-seen slot.
- `go/common/persistence/nosql/nosql_semaphore_metadata_store.go:57-76` `CreateSemaphore`: writes metadata only.
- `go/common/persistence/persistence-tests/semaphoreTokenPersistenceTest.go:74-81`: confirms "token rows are not seeded". The persistence-level grant can create a row, but the manager never asks it to for a slot outside `freeList`.
- `go/service/matching/semaphore/semaphore_manager_test.go:54-70`, `:241`, `:261-265`, `:394-429`, `:933`: every manager test feeds free rows through a mocked scan. None covers a fresh semaphore.
- `go/tests/matching-cassandra-interop/main.go` writes owner/token rows only for slots already granted. No seeder for capacity.

**Rust (current head):**
- `rust/crates/cadence-matching/src/matching_semaphore.rs:186-229` `load`: free set starts empty and is filled only from scanned token rows (`:196-206`). Comment at `:196-197` cites Go.
- `matching_semaphore.rs:370-378` `reserve`: `free_list.is_empty()` gives `None`.
- `matching_semaphore.rs:280-345` `grant`: `None` breaks out to `NoSlot` at `:342`.

**Commit that changed it:** `3076d7837` ("restore Go slot handling"). The removed line was `let mut free = (1..=budget)...` seeded from `metadata.bucket_size`. Old Rust fabricated free slots that Go never has, so the old probe passed (run3). Rust now matches the Go behaviour as written.

## Classification

- **(a) Rust divergence from Go: NO.** The current Rust matches Go's `loadTokenOwnership` + `reserve` exactly. The change in `3076d7837` removed a deviation.
- **(b) Harness/script bug: YES, with a caveat.** The probe expects one ACQUIRED on a fresh capacity-1 semaphore. The pinned Go code cannot produce that. The expectation is not Go-faithful. The gate never ran Go Matching (`goMatchingStarted: false`), so Go's own behaviour was never observed on this probe.
- **(c) Environment: NO.** See above.

The caveat: the upstream cause is a **Go defect** (or at least a Go gap). The comment at `semaphore_manager.go:403` promises seeding that nothing implements, so a production Go capacity-N semaphore can never make its first grant. The harness is "wrong" only because it assumes Go works here.

## Proposed fix (decision needed, not applied)

This is not a tiny obvious fix, so nothing was changed in code.

1. **Recommended.** Keep Rust Go-faithful. In `tests/matching-service-compat.cjs` before the race:
   - (i) Assert a fresh capacity-1 semaphore returns NO_SLOT for the first acquire (records the Go gap as evidence).
   - (ii) Seed one free token row through a gocql fixture in `go/tests/matching-cassandra-interop/main.go`: `INSERT INTO semaphore_tokens (domain_id, semaphore_name, bucket, type, token_id, owner_id, holder, held_token, updated_time)` for token 1, `holder` = free sentinel, `owner_id` = none sentinel. This is the row shape Go's comment says exists.
   - (iii) Run the two-owner race and the restart/recovery steps unchanged.
   Record the Go gap as an explicit parity gap in the matching parity notes. The durable-hold section then still runs.
2. **Not recommended:** fix Go to seed token rows in `CreateSemaphore`. That changes Go program logic (out of scope for a no-logic-change port) and needs an upstream decision.
3. **Rejected:** restore metadata seeding in Rust. Reintroduces the deviation `3076d7837` removed and hides the Go gap.

Verification still outstanding: this verdict rests on reading the source. No Go test was run here. A cheap check is the `semaphore_manager_test` pattern with an empty scan under `work-safe` (not run: diagnosis only, no Go/cargo runs in this task).

## Related
- [[matching-cassandra-protocol-parity-20261021]]
- [[matching-cassandra-ttl-expiry-audit-20261021]]
- [[matching-cassandra-validation-runbook]]
