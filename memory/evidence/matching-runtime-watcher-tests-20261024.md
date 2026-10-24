# Matching runtime dynamic config watcher test

The production watcher is `MatchingRuntime::with_dynamic_config_file` in
`rust/crates/cadence-matching/src/matching_runtime.rs`. It clamps polling to
five seconds, compares real filesystem modification time to the previous
attempt time, logs stat/parse failures, and advances the attempt time even on
failure. Go parity is `fileBasedClient.update` and `NewFileBasedClient` in
`go/common/dynamicconfig/file_based_client.go`; the Go polling oracle is
`TestFileBasedOraclePollReload` in
`go/common/dynamicconfig/file_based_oracle_test.go`.

The new `dynamic_config_file_watcher_reloads_retains_last_good_and_stops_on_shutdown`
unit test in the Matching runtime module exercises the actual runtime owner and
shared production overlay. It checks initial value 1, rewrite to 2, malformed
file retention, removed-file retention, later valid rewrite to 3, and that a
rewrite after awaited shutdown does not change the overlay. It honors the
five-second production floor and filesystem wall-clock mtime behavior.

Removed the old ignored `rust/crates/cadence-service-core/tests/matching_partial_upgrade_e.rs`:
it described a one-shot overlay snapshot and claimed runtime polling was
missing, despite an existing production watcher. The runtime unit test above
replaces that misleading assertion.

Related existing cancellation/raw-batch evidence from the previous review:

- `matching_reader_cancelled_wait_does_not_steal_later_task` and
  `matching_reader_stop_disconnects_waiting_poll_and_joins_empty_pump` in
  `rust/crates/cadence-matching/src/matching_reader.rs` cover cancellation and
  stop while a poll is waiting. Go counterparts are the poll cancellation
  implementation `manager.CancelPoll` in `go/service/matching/poller/manager.go`
  and `taskListManagerImpl.CancelPoller` in
  `go/service/matching/tasklist/task_list_manager.go`.
- Raw batch sizing is consumed by `MatchingReaderConfig::raw_batch_size` and
  `MatchingReaderConfig::batch_size` in `matching_reader.rs`; existing
  `matching_reader_config_*` assertions in its unit module cover live and
  configured values. Queue batch persistence/reading lives in
  `go/service/matching/tasklist/task_list_manager.go` and its reader
  implementation; this watcher task makes no changes to active-offer or raw
  batch behavior.

Validation evidence will be appended after CI completes.

CI attempt 1 (head `3c26a11cc`): run
https://github.com/auenkr/cadence-rust/actions/runs/40104128277 failed before Clippy/tests because the runtime test helper needed rustfmt formatting. The failed step output identified only that formatting diff; corrected it in the follow-up commit. No Cargo command was run locally.
CI attempt 2 (head `c3f894740`): run
https://github.com/auenkr/cadence-rust/actions/runs/40104642926 passed formatting but Clippy/type-check found a temporary lifetime in the test accessor (the `PropertyValues` temporary did not live through snapshot extraction). The accessor now binds `PropertyValues` before reading its snapshot; no production code mismatch was reported.
CI attempt 3 (head `a53dd0b33`): run
https://github.com/auenkr/cadence-rust/actions/runs/40105314817 passed formatting and Clippy, then the test revealed the fixture had not performed the documented synchronous initial load. It now initializes `AppState.admin.dynamic_config` from the initial file before attaching the production watcher. The failure was in test setup, not watcher reload behavior.
CI attempt 4 (head `b7a92e4e5`): run
https://github.com/auenkr/cadence-rust/actions/runs/40106652359 stopped at runtime formatting because the chained `.with_dynamic_config_file` indentation needed one more indent level. Corrected before the next CI attempt.
CI attempt 5 (head `1e57f3621`): run
https://github.com/auenkr/cadence-rust/actions/runs/40107402285 passed formatting but Clippy found an unnecessary `mut` on the initial `AppState` binding. Removed it; no watcher or test logic issue was reported.
