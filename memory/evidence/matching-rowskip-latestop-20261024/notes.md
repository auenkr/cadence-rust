# Matching parity gaps: row skip and late stop — 2026-10-24

Branch: `matching-rowskip-lateStop-20261024` (from origin/master). Not merged.

## Scope correction

The brief named "task-list manager Start" and "registry". The two ignored tests are
in the **semaphore** manager, not the task-list manager:

- `TestStartSkipsRowsItCannotClassify`: `go/service/matching/semaphore/semaphore_manager_test.go:227`
  → Rust `matching_semaphore_scan_skips_nil_and_unknown_rows`
  (`rust/crates/cadence-matching/src/matching_semaphore_parity_tests.rs`).
- `TestALateStopUnregistersOnlyItself`: `semaphore_manager_test.go:1028`
  → Rust `matching_semaphore_late_stop_unregisters_only_itself`.

Both gaps were small, so both were closed rather than left ignored.

## Gap 1: skip rows that cannot be classified

Go: `loadTokenOwnership` (`semaphore_manager.go`) counts and skips nil rows, zero-value
rows, unknown row types, and malformed known rows. The scan never fails on them.

Rust before: `SemaphoreRow` had no way to represent an unknown row, and the Cassandra
decoder (`cadence-persistence/src/cassandra.rs`) returned `Err("unknown semaphore row type")`,
which failed the whole load.

Change:
- `cadence-persistence/src/semaphores.rs`: new variant `SemaphoreRow::Unknown`.
- `cadence-persistence/src/cassandra.rs`: an unknown row type now pushes `SemaphoreRow::Unknown`
  instead of erroring. Mirrors Go, where the store returns the row and the manager skips it.
- `cadence-matching/src/matching_semaphore.rs` `load`: `SemaphoreRow::Unknown => {}`.

Not ported: Go's `logger.Warn("Skipped semaphore rows of unknown type ...")`. The
`cadence-matching` crate has no logger wired into this module, and adding one is out of
scope for this gap. The skip count is therefore not logged.

Test: the ignored stub is now a real `#[tokio::test]`. It loads a scan with one good token row,
three `Unknown` rows (the nil / zero-value / type-7 cases all collapse to `Unknown`), and the
malformed known rows, then asserts the free-set is `[1]` and `held` is empty.

## Gap 2: a stopping manager unregisters only itself

Go: `Stop` calls `onStopFn(m)` once, and the registry's `Unregister` removes the entry only if
the map still holds that exact manager.

Rust before: `Manager::stop` only set the state to Stopped. Registration was the caller's job.

Change (`cadence-matching/src/matching_semaphore.rs`):
- `Manager` gains `on_stop: Option<OnStop>` (`Box<dyn Fn(&Manager) + Send + Sync>`) and a
  `with_on_stop` builder. `Manager::new` keeps its 3-argument signature.
- `stop()` runs the teardown once. The first call sets `Stopped`, releases the index lock, and
  calls `on_stop`; a repeat call returns early. The index lock is not held across the callback.
- `Registry::get_or_create_with_idle_ttl` passes a callback that holds a `Weak` to the map and calls
  the free fn `remove_current`. That removes the id only if `Arc::as_ptr` equals the stopping manager,
  matching Go's `current != mgr` check.
- `Registry::unregister` now delegates to `remove_current`, so both paths share one check.

Un-ignored: `matching_semaphore_late_stop_unregisters_only_itself`. Its body already asserted
the right thing; it only needed the callback.

## Verification

No local cargo (per brief). Verification is GitHub Actions only.

- Code head: see the "Runs" section below.
- Run ids and results are filled in after each run.

## Runs

Code head `071544885` (the commit that changes Rust code). Workflow `rust-ci.yml`, branch
`matching-rowskip-lateStop-20261024`.

| Run | Input | Result | Evidence |
|---|---|---|---|
| 40065037785 | `package=cadence-persistence` | success | build, clippy and tests pass |
| 40065214498 | `package=cadence-matching` | success | `649 passed; 0 failed; 15 ignored`. Both un-ignored tests report `ok`: `matching_semaphore::parity_tests::matching_semaphore_scan_skips_nil_and_unknown_rows` and `...matching_semaphore_late_stop_unregisters_only_itself` |
| 40065397912 | whole workspace | **failure** | clippy passed. Test step: `matching_reader::tests::matching_reader_dispatch_success_uses_known_or_default_group` and `matching_reader_pump_falls_back_to_default_for_unknown_isolation_group` failed (`left: 2, right: 1` at `matching_reader.rs:2232` and `:2273`) |
| 40065610715 | whole workspace (rerun, same head) | success | green |

The two reader failures are a pre-existing flake, not this change. The same test
(`matching_reader_dispatch_success_uses_known_or_default_group`, `matching_reader.rs:2232`) failed on
origin/master run 40064723413 (head `e7285da4f`), which is this branch's base. The reader code is
untouched here, and the reader tests do not touch semaphores.

Root cause (test race, not production): `TaskReader::poll_group("a")` selects over the `a` buffer and
the default buffer (`matching_reader.rs`, `poll_group`). The tests waited only for the store read count
(`wait_read(&queue, 2)`), not for dispatch into buffers. So a lone `poll_group("a")` could take task 2
from the default buffer, and the assertion `known.task_id == 1` failed (`left: 2, right: 1`). This was
seen again on the final head, run 40065862331 (`matching_reader_pump_falls_back_to_default_for_unknown_isolation_group`, `:2273`).

Fix (test-only, both tests): two pollers on `poll_group("a")` each take one task, then sort by
`task_id`. Each poller selects over both buffers, so the two tasks are delivered in either order. The
assertions (task 1 stamped `"a"`, task 2 stamped `""`) are unchanged. No production code changed for this.

## Remaining ignores in the two areas

- `matching_semaphore_new_manager_validates_params` (`semaphore_manager_test.go:136`): still ignored.
  Out of scope here.
- `matching_semaphore_scan_threads_page_tokens` (`:180`): still ignored. The persistence boundary returns a
  single Vec with no page token.
- Not ported: the Go warn log for skipped rows. See gap 1 above.
