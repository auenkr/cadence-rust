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

## Applied fix (branch `matching-cassandra-probe-fix-20261024`, harness only)

Decision: Rust stays Go-faithful. Harness changed, no Rust or Go program logic changed.

- `go/tests/matching-cassandra-interop/main.go`: new `seed-semaphore {domain-id} {semaphore-name}` subcommand. It inserts token row 1 (`type`=token, `owner_id`=none sentinel, `holder`=free sentinel) with `IF NOT EXISTS`.
- `tests/matching-service-compat.cjs`, Cassandra block: a separate fresh semaphore `durable-gap-${id}` asserts its first acquire returns NO_SLOT. Then `durable-${id}` is seeded through `seed-semaphore` before its first acquire. The two-owner race and recovery steps are unchanged and expect one ACQUIRED. Evidence is recorded as `freshSemaphoreFirstAcquire`.
- MySQL/PG probe paths unchanged.

**Why the order differs from the original proposal:** seeding after the first acquire cannot work. Go's `Start` loads the free set once (`semaphore_manager.go:160-170`, `load` at `:198`), and `reserve` reads only that in-memory `freeList` (`:494`). Rust loads once too (`matching_semaphore.rs:177-186`). A row seeded after a bucket's first acquire is never seen by that bucket, so the race would still get two NO_SLOT. The NO_SLOT assertion therefore needs its own semaphore, and the race semaphore is seeded before its first acquire.

**Not verified:** no cargo, no gate, no Go test run (per task scope). `node --check` and `gofmt -e` passed. The cassandra lane still needs one run to confirm.

**Parity gap (Go-side):** `go/service/matching/semaphore/semaphore_manager.go:403` says "Token rows are seeded once and never deleted", but nothing in `go/` seeds token rows. `CreateSemaphore` writes metadata only, and `semaphore_tokens_cql.go:27-28` says rows are not seeded. A production Go capacity-N semaphore therefore never makes its first grant. Go-side fix needs an upstream decision and is out of scope for a no-logic-change port.

## Options considered (historical)

1. **Chosen.** Keep Rust Go-faithful and fix the harness, as above.
2. **Not recommended:** fix Go to seed token rows in `CreateSemaphore`. That changes Go program logic (out of scope for a no-logic-change port) and needs an upstream decision.
3. **Rejected:** restore metadata seeding in Rust. Reintroduces the deviation `3076d7837` removed and hides the Go gap.
2. **Not recommended:** fix Go to seed token rows in `CreateSemaphore`. That changes Go program logic (out of scope for a no-logic-change port) and needs an upstream decision.
3. **Rejected:** restore metadata seeding in Rust. Reintroduces the deviation `3076d7837` removed and hides the Go gap.

Verification note (2026-10-24): the full Cassandra mixed Go/Rust acceptance lane subsequently passed on GitHub Actions. It observed the fresh semaphore return `NO_SLOT`, then verified a seeded capacity-one race, persisted hold, owner crash/recovery, release and fresh acquire. See [the run evidence](evidence/matching-cassandra-acceptance-20261024/README.md). No Go behavior or Rust program logic was changed for this acceptance fix.

## Related
- [[matching-cassandra-protocol-parity-20261021]]
- [[matching-cassandra-ttl-expiry-audit-20261021]]
- [[matching-cassandra-validation-runbook]]
