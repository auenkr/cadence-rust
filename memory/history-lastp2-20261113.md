# History last P2 fixes — 2026-11-13

Branch: history-lastp2 (from master f9e9319d3, master merged). Validated head: 5d2cafdaf12b96a40f9bf6e140a33196e6bc643b (this note is committed after it).

| # | Item | Status | Where |
|---|------|--------|-------|
| 1 | Opus P2 9: DomainNotActiveError active-active message + ActiveClusters | done, unit tests | engine/deps.rs |
| 2 | Codex 8: persistence ShardOwnershipLost carries owner host | done, unit test | assembly/controller.rs, handler/metrics_handler.rs, service.rs, deps.rs variant |
| 3 | Opus P2 12: queuev2 state full-fidelity persistence (virtual queues, predicates) | done, roundtrip test | shard_context.rs, sqlblobs.rs, shard_events.rs, shard_adapters.rs |
| 4 | Cache eviction double-lock test | done | assembly/engine_cache.rs (`evict_unheld`, `evict_tests`) |
| 5 | Gate assertion: global domain write creates replication rows | NOT done (infeasible) | see below |

## Item 5 explanation
The history mixed gate runs a single cluster. A global domain needs a second cluster in cluster metadata, so registration is rejected. The two-cluster harness (`tests/gates/run-frontend-two-cluster-gate.py`) is separate from the 12 history combos. Adding a second cluster to the history gate is a harness change that would not fit the 90-minute budget with safe iteration. Replication rows stay covered by unit tests only. See history-replgen-20261113.md.

## CI evidence (repo auenkr/cadence-rust, head 5d2cafdaf)
- rust-ci (clippy + tests): https://github.com/auenkr/cadence-rust/actions/runs/43415629043 — success. Earlier head bb49ab00b: run 43413727336 — success.
- history-mixed-gate, 12 combos, all conclusion success and every log has `GATE SUMMARY failed: []`:
  - 43417320587, 43417324935, 43417329263, 43417333974 (mysql grpc/tchannel x two_history true/false, dispatch order)
  - 43417337925, 43417342106, 43417346374, 43417350609 (postgres, same order)
  - 43417354797, 43417359634, 43417364226, 43417368197 (cassandra, same order)
  - URLs: https://github.com/auenkr/cadence-rust/actions/runs/<id>

Earlier failed CI runs (fixed): 43411827881 (test in fn scope), 43412444504 (duplicate splice), 43413085021 (unused imports).
