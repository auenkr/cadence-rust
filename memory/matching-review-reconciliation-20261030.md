# Matching source-review reconciliation (2026-10-30)

Source-only reconciliation at source pin `c760e1c57aad64492284b0a8584ef7258c67c955`. No Cargo, Go, Node, build, formatting, tests, runtime, or CI inspection/dispatch was performed. No test or implementation file was changed. This note records bounded review conclusions; it is not Matching completion evidence.

## Findings

1. **Non-durable task status is an adapter limitation.** In `rust/crates/cadence-matching/src/matching_rpc_tests.rs:1603-1621`, `non_durable_fixture` creates `Matching::new(state)` and does not attach a `QueueRegistry`. The ignored test at `:1807-1820` expects `task_id_block` and `empty` from this fixture. Those queue-status fields require the durable queue/ack manager; this adapter does not provide one. The production native engine fills queue status fields through its queue-backed path. Treat the ignored assertion as fixture limitation, not evidence of a production behavior gap.

2. **The ignored `NewConfig` test is a structural single-table gap.** `rust/crates/cadence-matching/tests/matching_partial_upgrade_a.rs:84-88` requires a centralized Rust `Config` equivalent and panics because no such struct exists. The existing [`matching-current-config-reconciliation-20261027.md`](matching-current-config-reconciliation-20261027.md) traces behavior-bearing keys to production readers/defaults and Go consumers; no runtime key gap was demonstrated. This does not establish runtime parity beyond those source mappings.

3. **Secondary target PID/port and zero-row durable-drain/evidence-copier source reviews found no actionable issue.** These were bounded source reviews only; neither behavior was validated at runtime. Do not cite them as runtime evidence.

4. **CI remains pending and uninspected.** Fresh run [41106048247](https://github.com/auenkr/cadence-rust/actions/runs/41106048247) for `c760e1c57aad64492284b0a8584ef7258c67c955` is pending. This note does not infer a result.

## Reproducible source pointers

- `rust/crates/cadence-matching/src/matching_rpc_tests.rs:585-594` shows the durable fixture constructing `QueueRegistry` and using `Matching::with_durable_queues`; `:1603-1621` shows the non-durable fixture using `Matching::new`.
- `rust/crates/cadence-matching/src/matching_rpc_tests.rs:1807-1820` is the ignored non-durable status assertion.
- `rust/crates/cadence-matching/tests/matching_partial_upgrade_a.rs:84-88` is the ignored synthetic `NewConfig` table test.
- `memory/matching-current-config-reconciliation-20261027.md` records the existing per-key reader/default/source reconciliation.

