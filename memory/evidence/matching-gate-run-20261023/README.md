# Matching mixed gates on the new binary (2026-10-23, branch matching-gate-run-20261023, base 6788e6555)
Binaries (binaries.sha256): cadence-matching 611b75ab..., Go cadence-server f70a0a1a... Built via `run-go-service-baseline.py --rust-services matching --build-only` (build-only.exit 0). Rust Matching runs as `cadence-matching`; Go Frontend/History/Worker; no Go Matching (goMatchingStarted=false in every results.json).
Each gate ran under `flock /tmp/cargo.lock`, one at a time, with a fresh DB container (mysql:8.0.36, postgres:17-alpine cadence-pg-gate; local-postgres untouched), containers removed afterwards. Commands: command-*.txt. Result is "preliminary-passed" (acceptanceEligible=false by design, see matching-mixed-gates-20261023).

## Real exit codes
| Gate | exit file | exit |
| --- | --- | --- |
| build-only | build-only.exit | 0 |
| 1 MySQL single node (--matching-restart) | mysql1.exit | 0 |
| 2 MySQL two node (--matching-failure-recovery, idle check 3s) | mysql2.exit | 0 (4th attempt) |
| 3 PostgreSQL single node (--matching-restart) | pg1.exit | 0 |

## Per-scenario (logs read, not inferred from exit codes)
- SDK samples echo/activity retry/child/workflow retry/decision retry/activity timeout/signal: go-sdk.log "passed" in mysql1, mysql2, pg1.
- Cancellation: go-sdk.log "workflow cancellation ended Canceled through Rust Matching" in all three.
- Query, isolation + CancelOutstandingPoll, native partition config: probe.log result JSON "passed" (queryDispatch, isolationDispatch, nativePartitionConfig) in all three.
- SIGTERM drain + recovery: queuedRowsBeforeSignal 1 -> queuedRowsAfterRecovery 0, new Rust pid, 1 completion (pg1 verified; mysql in results JSON); graceful-drain-sdk.log "resumed and completed".
- SIGKILL restart recovery: durableTaskRowsBeforeKill 1, new pid, completionEvents 1; restart-sdk.log "resumed and completed".
- Final SIGTERM: matching-shutdown.json exitCode 0 in all three.
- MySQL GC + domain metadata + goSchemaOnly (missingGoTables []): passed in mysql1 and mysql2. PG: productionDatabaseEvidence "not collected" (MySQL-only checks).
- Two-node (mysql2): ring maps converge, idle eviction + SDK recovery, typed non-owner ABORTED decoded by the pinned Go client, kill-owner-node SIGKILL then survivor takeover with higher persisted range, ack advance, queue drained, SDK completion, killed node rejoin: all asserted by the probe (probe exit 0). The checkpoint file production-failure-recovery-checkpoints.json lives in the removed work dir and was not copied (harness gap: add to --evidence-dir copy list).

## Failures found and fixed (harness bugs, no Rust bug found)
1. mysql2-attempt1-failed: assert "task-list range was not leased" (cjs:536). Cause: for MySQL/PG sqliteTaskState returned range_id null. Fix: read MAX(range_id) from task_lists in tests/matching-service-compat.cjs (durableTaskCount rangeId mode, MySQL and PG).
2. mysql2-attempt2-failed: ABORTED "task list is not owned by this host" in the isolation step. The direct Matching client always hit the primary, while the secondary can own the list. The non-owner status is correct (matches Go). Fix: in failure-recovery mode follow the owner after a typed ABORTED.
3. mysql2-attempt3-orphan-present: passed, but attempt 2's orphaned Rust Matching (I killed it afterwards; harness leaves survivors after a probe failure) shared the DB port, so I discarded it and reran.
4. mysql2-attempt4-gc-race: GC convergence timed out. With --matching-idle-check-seconds 3, each iteration of the GC check (rows, docker exec reads, Describe) outlasts the idle interval, so Describe re-leased evicted lists and persisted range_id (26) trailed live range (27) in every iteration. Not a Rust bug (Go reloads on Describe too). Fix: tests/matching-mysql-gate.cjs re-reads each list up to 8 times until live and persisted ranges agree; self-check passes.
Earlier reported failures: range-ID shutdown exit 1 did not reproduce (matching-shutdown.json exitCode 0; log line "lost ownership during shutdown" is now a log only). ECONNREFUSED on secondary gRPC and "Decision task not found" did not reproduce in the 4 attempts here.

## Observations (not failures)
- Rust log "flushing Matching metrics failed: Connection refused" at shutdown (no statsd sink), exit 0.
- Range IDs climb quickly under a 3s idle check (reload per Describe), as expected for short idle intervals.
- A failed probe leaves the Rust/Go child processes running (attempt 2); the harness should kill them in a finally block.
