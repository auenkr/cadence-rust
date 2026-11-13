# History review2 fixes — 2026-11-13

Branch `history-review2fix`, head SHA `074e43a945839312e5aa4b38715591c45c4dc07c` (not merged). Source: `memory/history-review2-codex-20261113.md`.

## Fixes
1. P1 shard closed during init (`wiring/assembly/controller.rs`): Go `removeHistoryShardItem` compares the registered item identity. `remove_matches` now compares the registry cell with the acquisition's own cell (`Arc::ptr_eq`), so a shard that closes while initializing is removed from the map. The engine is stopped right after `engine.start()` through a `closed_early` flag. A late callback of a replaced shard still cannot remove its successor. Unit test: `close_callback_removes_only_own_cell`.
2. P1 NDC failover metadata (`ndc_context_hooks.rs`, `ndc_workflow.rs`): the `install_failover_context` helper was extracted from `build_state`. A new `NdcWriter::configure_state` hook calls it, and `load_workflow_execution` runs it after `state_from_snapshot`. A missing task version now uses `EMPTY_VERSION` and still runs StartTransaction (Go `context.go:384-410`, `mutable_state_builder.go:1418`).
3. P1 typed NDC active writes lacked replication tasks (`ndc_state.rs`): for `TransactionPolicy::Active`, `close_transaction_as_snapshot` and `close_mutation` now add category-3 history and sync-activity replication rows (`active_replication_rows`). They follow Go `prepareEventsAndReplicationTasks` and apply only when `can_replicate_events`. Allocation and the continue-as-new merge hooks consume the rows as before.
4. P2 range-renewal owner: `shard_err` now takes the shard id and sets `shard_id: Some(id)`, so `convert_error` looks up the owner (Go `handler.go:2138`). All call sites were updated.

No logic changes beyond the Go parity above. No new unit tests for 2 to 4 (covered by CI and gates).

## CI evidence
- rust-ci `-f package=cadence-history`: https://github.com/auenkr/cadence-rust/actions/runs/43452450208 (head 074e43a94) success. Earlier runs 43448020702 and 43449546672 failed on an import path and a clippy `result_large_err` lint, both fixed.
- history-mixed-gate, 12 runs, all success with `GATE SUMMARY failed: []`:
  mysql, postgres and cassandra, each with grpc and tchannel and two_history true and false.
  Run IDs: 43454340133, 43454346162, 43454352289, 43454357814, 43454369757, 43454408538, 43454416952, 43454422398, 43454429529, 43454452875, 43454471381, 43454478871.
