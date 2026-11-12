# history-gatescen (2026-11-12)

Branch `history-gatescen` (from origin/master 1f4654821). Extends the history mixed gate
(`FE_MODE=scen`, driver `tests/frontend-gate/scen.go`, harness `tests/gates/run-frontend-gate.py`)
with named GATE SUMMARY checks.

## Scenarios
1. Retry-policy workflow timeout -> next run attempt+1; cron timeout -> next cron run.
2. History archival (file:// URI): archived by Go Worker archival workflow, archive file exists,
   GetWorkflowExecutionHistory falls back to archive after retention (domain `fe-gate-scen-*`,
   dynamic config `system.minRetentionDays: 0`, `workflowDeletionJitterRange: 1`).
3. Auto-reset on bad binary checksum (UpdateDomain marks binary bad; next signal's close
   transaction schedules ResetWorkflow transfer task; original terminated, new run links original).
4. Strong idempotency (`history.enableStrongIdempotency: true`) with signal request-id dedup.

## Rust bugs fixed (History)
- Timer retry/cron: `preload_start_event` before `retry_workflow_new_run` (queue_deps_timer_active.rs etc.).
- Tasks for deleted workflows (retention 0): `he()` maps "Workflow execution not found" to EntityNotExists.
- thrift_g2 `fail()` returns `Outcome::Rich` (app-error flag) for EntityNotExists, DomainNotActive,
  CurrentBranchChanged, ShardOwnershipLost -> archival fallback works.
- Auto-reset: Go `closeTransactionHandleWorkflowReset` was not ported; added in engine_close.rs.
- Cached `EngineMutableState` used a stale domain snapshot; bad binaries are now refreshed from the
  domain cache on every `load_state` (`live_bad_binaries`), also used by the BAD_BINARY decision check.

## Test-side finding
Go `UpdateDomain` persists a bad-binary-only change only if another config field changes, so the
scenario also sets `EmitMetric` (Rust copies Go behaviour).

## Evidence (final functional head 77f3eb7b4; master merged afterwards as 2aa39b6b2)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43157268699 success
- Gates, all `GATE SUMMARY failed: []`:
  - mysql/grpc/true https://github.com/auenkr/cadence-rust/actions/runs/43157272188
  - cassandra/grpc/true https://github.com/auenkr/cadence-rust/actions/runs/43157275838
  - postgres/grpc/false https://github.com/auenkr/cadence-rust/actions/runs/43157278983
  - mysql/tchannel/false https://github.com/auenkr/cadence-rust/actions/runs/43157282323
- Earlier failing runs (history): 43146761013.., 43149572998.., 43155162147.. (autoreset only).

## Post-merge master validation (master 2311d08df, 2026-11-12)
- rust-ci full workspace: https://github.com/auenkr/cadence-rust/actions/runs/43158787847 success
- All 12 history-mixed-gate combos (mysql/postgres/cassandra x grpc/tchannel x two_history true/false): runs 43158792499..43158842835, all success (GATE SUMMARY failed: []).

## Re-run on final head 8de9e6309 (after master merge)
- rust-ci https://github.com/auenkr/cadence-rust/actions/runs/43158756173 success; rust-check 43158721483 success
- Gates, all `GATE SUMMARY failed: []`: 43158760149, 43158764179, 43158770542, 43160303957 (mysql/grpc/true, dispatched late after a GitHub HTTP 500)
- Branch had been merged to master (2311d08df) and deleted remotely; re-pushed only to dispatch the last lane. Safe to delete again.
