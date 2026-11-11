# History engine package tests (branch `history-enginepkg`, 2026-11-12)

Port of `go/service/history/engine/engineimpl/*_test.go` into
`rust/crates/cadence-history/src/engine/engine_tests/` (fakes in `engine/test_support.rs`).
Go mocks (gomock) are replaced by hand-written scriptable fakes (`FakeMs`, `FakeCtx`, `FakeCache`,
`FakeUpdater`, `FakeDomainCache`, `Nop` for every unused collaborator, `nop_engine()`).

## Evidence
- Final green run: https://github.com/auenkr/cadence-rust/actions/runs/43088979946
  - head `95758adb94d2d3f7367b6bd64885424ba571c5c9` (origin/master merged in), conclusion success
  - `cadence-history` lib: 486 passed, 0 failed (clippy -D warnings passed)
- Earlier runs: 43085178355 (green, 63 engine tests), 43087740176 (1 failure: Nop domain cache panic, fixed).

## Rust test counts (new engine_tests, 76 total)
| Rust file | Rust tests | Go source |
|---|---|---|
| dlq.rs | 7 | dlq_operations_test.go (4 Go cases + count/purge/merge/convert) |
| describe.rs | 17 | describe_workflow_execution_test.go (+ Map* tests) |
| handlers.rs | 18 | describe_mutable_state, reset_sticky_tasklist, terminate_workflow_execution, refresh_workflow_tasks |
| signal.rs | 9 | signal_workflow_execution / remove_signal_mutable_state |
| reset.rs | 5 | reset_workflow_execution (validation + reset point type) |
| failover.rs | 7 | register_domain_failover_callback (catch-up, predicate, graceful markers) |
| start.rs | 13 | start_workflow_execution (reuse policy, helpers, early exit) |

Go has ~185 engine tests; Rust ports 76 (about 41%) one-to-one where the Rust engine structure allows.

## Deviations
- Go tests that drive the real `historyEngine` with mocked persistence/shard (history_engine_test.go,
  history_engine2_test.go, history_engine3_eventsv2_test.go) do not map directly: Rust engine is trait-based,
  so they are re-expressed against the fakes. Decision/activity handler tests live outside Rust `engine/`
  and are covered by other Rust modules (not ported here).
- Visibility changes only: `map_*` fns in describe_workflow_execution.rs and
  `should_terminate_and_start` / `is_failed_close_state` in start_workflow_execution.rs made `pub(crate)`.
- Real timers are used; no mocked clock.

## Bugs fixed
None found: all ported tests passed against existing Rust logic (no logic change).

## Remaining unported
- Start/signal-with-start full-flow tests (create-workflow persistence paths, cleanup of orphaned branch,
  terminate-and-start) needing a richer WorkflowStartHost fake.
- history_engine_test.go record/respond decision & activity tests, poll mutable state, query workflow,
  reapply events, replicate events tests, history_engine3_eventsv2 tests, queue_operations, notify_tasks,
  lifecycle (start/stop) tests.
- register_domain_failover_callback full callback (domain_change_cb, queue notify) tests.
