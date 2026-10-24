# Matching lifecycle gaps v2: merge of master into lifecycle — 2026-10-24

Branch: `matching-lifecycle-gaps-v2-20261024`, based on `origin/matching-lifecycle-gaps-20261024`
(head `72abda2e1`, which includes code head `a574fe5f3`). Merged with `origin/master` (`69a3f72e5`).
Merge base: `e7285da4f`. Not merged to master.

## Conflicts

Two files conflicted, both in `rust/crates/cadence-matching/src/`:

1. `matching_semaphore.rs`: two on-stop mechanisms.
   - Master (rowskip-lateStop): `on_stop: Option<OnStop>` set by `Registry::get_or_create`
     through `with_on_stop`, and a once-only `Manager::stop` that calls it. Unregister goes
     through `remove_current`, which removes only the exact manager.
   - Lifecycle: `managers: Weak<Mutex<HashMap>>` set by `registered_in`, plus a `stop` that
     unregisters itself with no once-only guard.

   Resolution: keep master's single mechanism (`on_stop`, `with_on_stop`, `remove_current`,
   once-only `stop`). Drop lifecycle's `managers` field, `registered_in`, and its `stop` body.
   Re-add lifecycle's cancelled-first-load cleanup, `LoadInFlight` (a `Drop` guard that calls
   `stop` when `start` is dropped mid-load), which is the only lifecycle piece that did not
   survive the take-one-side step.

2. `matching_semaphore_parity_tests.rs`: only the comment above
   `matching_semaphore_late_stop_unregisters_only_itself` differed. Took master's file.
   Both sides had already removed the `#[ignore]` on that test, so no test was duplicated.

Taking master's whole file for both conflicted files also dropped lifecycle's
non-conflicting hunks. For the semaphore file, that was only `LoadInFlight` and its use in
`start`, which were re-added. For the parity test file, lifecycle's diff was only the comment
and the ignore removal, so nothing else was lost.

## Ignored-test check

Union of un-ignored tests on both sides is present in the merged tree:
- Master un-ignored: `matching_semaphore_scan_skips_nil_and_unknown_rows`, `late_stop_unregisters_only_itself`,
  and the nil/unknown row test.
- Lifecycle un-ignored: cancelled-during-startup-load, `isShuttingDown` observation, stop-all
  semaphores, and the no-engine-shutdown-state test.
- No duplicate `fn` names in `matching_semaphore*.rs`. No conflict markers left in `rust/` or `go/`.
- `stop_semaphores` (grpc_matching.rs) calls `all_managers()`, which copies the handles into a
  `Vec` and releases the registry lock before any `stop`. The `on_stop` callback re-locks the
  registry, so this ordering avoids a deadlock.

## CI runs (GitHub Actions `rust-ci.yml`, no local cargo)

- Merge commit: `5e29af050` (author Golden Kumar, trailer Co-Authored-By Claude Haiku 5.5).
- Package run, `-f package=cadence-matching`: https://github.com/auenkr/cadence-rust/actions/runs/40067314780 — success.
- Whole-workspace run (no package input): https://github.com/auenkr/cadence-rust/actions/runs/40067644056 — success (clippy-test job green: `clippy --workspace --all-targets -D warnings` and `cargo test --workspace`).
- Final code head: the merge commit `5e29af050`, which this notes commit follows. This verified code is the code the whole-workspace run tested.
- The push-triggered run `40067314709` was cancelled by the concurrency group when the
  dispatched run started. That is expected, not a failure.
