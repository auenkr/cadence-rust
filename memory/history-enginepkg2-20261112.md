# History engine package tests, part 2 (branch `history-enginepkg2`, 2026-11-12)

Builds on memory/history-enginepkg-20261112.md. New files in `rust/crates/cadence-history/src/engine/engine_tests/`:

| Rust file | Tests | Go source / area |
|---|---|---|
| domain_cb.rs | 10 | register_domain_failover_callback: domain_change_cb (active-passive, active-active DLQ partitions, local domains, graceful markers, marker failure, lock/unlock, notify_queues, registration) |
| lifecycle_tests.rs | 5 | history_engine Start/Stop ordering, graceful marker notifier, DLQ start failure, cancel token |
| poll_query.rs | 18 | poll_mutable_state (uuid validation, immediate return, branch changed, missing histories, long-poll notify/timeout/deadline, passive-cluster not-exists) and query_workflow (strong-consistency gate, NotOpen/NotCompletedCleanly rejection, no-decision failure, direct matching, error, expired deadline) |
| reapply.rs | 9 | reapply_events (same-version/duplicate skip, apply, empty result, reapplier error, cron, closed w/o decision, reset of closed workflow, pending-active domain) |

Total new: 42 tests (engine_tests now 118).

## Fake extensions (engine/test_support.rs)
FakeMs gained `last_write_version`, `duplicated_resources`, `previous_started_event_id`, `last_first_event_id`,
`version_histories`, `in_flight_decision`; `TestConfig.no_consistent_query`; new `NopShard(shard_id)` start host.

## Deviations
- Record/respond decision & activity tests (history_engine_test.go), history_engine3_eventsv2 and full start/signal-with-start
  flows are NOT ported: they need the real mutable-state/persistence stack, which the trait-based Rust engine does not expose
  through these fakes (decision/activity handlers live in other Rust modules with their own tests).
- Consistent (buffered) query path not covered: FakeMs `get_query_registry` still unimplemented.

## Bugs fixed
None; all tests passed against existing Rust logic.

## Remaining
record/respond decision+activity engine tests, history_engine3_eventsv2, start/signal-with-start full flows,
buffered consistent-query path, sticky-query path.

## Evidence
- Green run https://github.com/auenkr/cadence-rust/actions/runs/43091991865, head code `3ee4156ebf5d62ce3623acc6630c6114350d9ce8`
  (origin/master merged: already up to date), conclusion success (clippy -D warnings + cadence-history tests).
- Earlier failed runs: 43090615593 (HResult import), 43091278403 (clippy type_complexity); both fixed.
