# Matching semaphore manager test parity — 2026-10-23

Branch: `matching-semaphore-tests-20261023`, based on `origin/master` at
`92668eb2f`. Worktree: `/mnt/storage/service/github/wt/semtests`.
`go/idls` initialized at `bfbf273b41b9e36ce21e47a49600d22d73e4614c`.

Scope: audit P1-5, all 31 tests in
`go/service/matching/semaphore/semaphore_manager_test.go` compared with
`rust/crates/cadence-matching/src/matching_semaphore.rs`. New cases live in
`matching_semaphore_parity_tests.rs`, a private child test module. No persistence
crate changes, SDK/API changes, or work on subsequent services.

Read reference branches with `git show`, without merging them:
- `origin/matching-parity-audit-v4-20261023:memory/matching-go-parity-audit-v4-20261023.md`
- `origin/matching-liveness-tests-20261023:memory/matching-test-parity-gaps-20261023.md`

## Small Go parity fixes

1. Failed grant writes now unreserve the drawn ID before propagating the error,
   matching `go/service/matching/semaphore/semaphore_manager.go:345-351`.
   Previously Rust's `await?` permanently removed one slot per failed write.
   Tests assert one write per call, unchanged free count after three failures,
   no new hold, and exact propagated error category/message.
2. Startup now builds the free set only from scanned token rows, matching
   `semaphore_manager.go:428-470`. Previously Rust prepopulated IDs from metadata,
   treating absent rows as free. The fake deliberately returns metadata with a
   larger budget than its scan, so startup and full-bucket tests catch this.
   The existing metadata existence check is retained.

## Go-to-Rust test map

Go names below omit `Test`; Go line numbers refer to
`go/service/matching/semaphore/semaphore_manager_test.go`. Rust names omit
`matching_semaphore_`; all new names are in the private parity module unless
marked existing. Cases assert call counts, request fields, and index consistency
where relevant. The fake panics on unexpected reads/writes and requires scripted
responses to be consumed.

| Go test:line | Rust test / coverage |
|---|---|
| NewManagerValidatesItsParams:136 | `new_manager_validates_params` — ignored gap |
| StartBuildsTheFreeSetAndTheHeldIndexAcrossPages:180 | `start_builds_free_and_held_indexes`; `scan_threads_page_tokens` ignored for pagination contract |
| StartKeepsAClaimedSlotOutOfTheFreeSet:206 | `start_excludes_owner_claim_even_when_token_is_free` |
| StartSkipsRowsItCannotClassify:227 | `start_skips_malformed_known_rows`; `scan_skips_nil_and_unknown_rows` ignored for unrepresentable rows |
| StartLeavesStateUntouchedWhenTheScanFails:254 | `scan_failure_leaves_indexes_untouched`; partial-page failures reach Rust as aggregate scan errors |
| SecondStartIsANoOp:282 | `second_start_is_noop` |
| AFailedLoadUnregistersTheManager:295 | `failed_load_unregisters_and_allows_fresh_retry` |
| AcquireRejectsAnEmptyOwnerID:307 | Already ported on liveness branch: `rejects_empty_owner_without_touching_free_set`; not duplicated here |
| AcquireBeforeTheBucketIsUsable:320 | `acquire_before_bucket_usable` — all three Go setups, including stop during blocked scan |
| AcquireHonorsItsDeadlineWhileStarting:389 | `acquire_honors_deadline_while_starting` — ignored gap |
| StartDoesNotWaitOnALoadInFlight:420 | `second_start_does_not_wait_for_scan` |
| AcquireHandsBackWhatTheGrantDecided:454 | `acquire_passes_through_grant_result` — confirmed hold and full bucket |
| AcquireSurfacesAFailedGrant:496 | `acquire_surfaces_failed_grant` |
| GrantOnAFreeSlot:509 | `grant_on_free_slot_records_request_and_hold` |
| GrantRetriesADifferentSlotWhenTheWriteSaysTaken:537 | `grant_retries_different_slot_when_taken` |
| GrantGivesUpAfterMaxAttempts:562 | `grant_gives_up_after_max_attempts` — asserts Go's literal limit 3 |
| GrantStopsRetryingOnceTheDeadlineHasPassed:577 | `grant_stops_retrying_at_deadline` — ignored gap |
| GrantWhenTheWriteSaysTheOwnerAlreadyHolds:600 | `write_already_held_returns_reserved_slot` — both subcases |
| GrantRejectsAnAlreadyHeldWriteWithNoToken:651 | `already_held_without_token_is_internal_error` — includes subsequent successful grant |
| GrantReturnsTheSlotWhenTheWriteFails:679 | `failed_writes_do_not_drain_free_set` |
| GrantRejectsAnUnrecognizedWriteOutcome:699 | `grant_rejects_unknown_write_outcome` — ignored gap |
| GrantWhenTheReverseIndexIsStale:716 | `stale_reverse_index_free_taken_and_missing` — all three subcases |
| GrantSurfacesAConfirmingReadFailure:772 | `confirming_read_failure_is_preserved` |
| DropStaleHold:786 | `drop_stale_hold_only_returns_proven_free_slot` — both subcases |
| UnreserveIsIdempotent:829 | Already ported on liveness branch: `unreserve_is_idempotent`; not duplicated here |
| FreeSetSurvivesConcurrentReserveAndUnreserve:854 | `free_set_survives_concurrent_reserve_unreserve` — 8 threads × 50 iterations |
| ConcurrentGrantsHandOutDistinctSlots:880 | `concurrent_grants_are_distinct` — 20 owners |
| LifecycleUnderConcurrentGrants:919 | `lifecycle_under_concurrent_grants` — 10 barrier-synchronized rounds |
| AnIdleManagerUnloadsItselfAndLeavesTheRegistry:981 | Existing `matching_semaphore_sweep_unloads_idle_buckets` |
| AcquireKeepsAManagerLoaded:999 | `acquire_refreshes_idle_time` |
| ALateStopUnregistersOnlyItself:1028 | `late_stop_unregisters_only_itself` — ignored for direct-stop callback contract; existing `matching_semaphore_unregister_ignores_a_stale_manager` covers registry identity guard |

The failed-load/retry test additionally covers registry
`TestSemaphoreRegistry_GetOrCreateHoldsNothingWhenTheBuildFails:129` and
`TestSemaphoreRegistry_GetOrCreateBuildsOnlyWhenThereIsNothingHeld:101` at the
Rust get-or-create boundary. Other registry tests already have counterparts in
`matching_semaphore.rs`.

## Remaining explicit gaps (7 ignored tests)

- **Constructor validation** (`Go semaphore_manager_test.go:136`, implementation
  `semaphore_manager.go:100-135`): Rust constructor is infallible, accepts invalid
  identifier fields, and clamps nonpositive TTL. Store is statically non-null;
  logger/time-source/callback are not Go-style constructor parameters. A faithful
  fix requires redesigning construction and its callers.
- **Paged startup scan** (`Go semaphore_manager_test.go:180`): Rust persistence
  returns `Vec<SemaphoreRow>` with no page tokens; Cassandra uses `query_unpaged`.
  Aggregate free/held behavior is tested; threading Go page tokens cannot be
  tested through this trait. Partial-page errors likewise have no direct seam.
- **Nil / unknown scan rows** (`Go semaphore_manager_test.go:227`): the enum
  represents neither nil nor unknown discriminants. Known malformed fields are
  injected and tested. Cassandra currently returns Internal for unknown row
  types (`cadence-persistence/src/cassandra.rs`, `scan_semaphore_bucket`), whereas
  Go's manager skips them. Needs persistence boundary/decoder work.
- **Startup deadline** (`Go semaphore_manager_test.go:389`, implementation
  `semaphore_manager.go:181-189`): Rust acquire has no context/deadline parameter
  and waits on a watch channel indefinitely. Ignored test bounds the wait with an
  outer watchdog and fails because acquire itself never returns a deadline error.
  This watchdog is explicitly not claimed as deadline parity.
- **Deadline between grant attempts** (`Go semaphore_manager_test.go:577`,
  implementation `semaphore_manager.go:322-324`): no cancellation parameter or
  check exists in Rust. Ignored case observes all three writes rather than Go's
  one write / canceled error; a Go-style cancel-on-first-write cannot be injected
  until the manager and persistence APIs carry cancellation.
- **Unknown grant outcome** (`Go semaphore_manager_test.go:699`): Rust's exhaustive
  `GrantOutcome` cannot represent Go Unknown. No unsafe enum fabrication is used.
  Go's defensive Internal+unreserve branch has no injectable Rust counterpart.
- **Direct stop unregister callback** (`Go semaphore_manager_test.go:1028`,
  implementation `semaphore_manager.go:253-265`): Rust `Manager::stop` has no
  registry callback. Registry teardown explicitly unregisters managers before
  stopping them. The ignored test verifies a late old stop preserves the
  replacement, then fails because directly stopping the replacement leaves it
  registered. Adding callback ownership/lifecycle is beyond a small test fix.

Ignored tests are explicit failing regression markers when manually included;
three representation/API-only cases explain the missing seam with a panic.
They do not claim executable behavioral coverage. Other ignored cases exercise
available behavior and assert Go's expected result.

Idle tests use Rust's explicit registry sweep and controlled liveness timestamps,
not wall-clock sleeps. This covers refresh/eviction decisions, not Go's daemon,
TTL/2 ticker scheduling, callback timing, or goroutine leak detector. Rust has no
Go race-detector equivalent in this run; concurrency tests check completion,
unique grants, and free-list/reverse-index consistency under real threads.

## Validation

Targeted semaphore tests: **37 passed, 0 failed, 7 ignored**, exit **0**
(`semaphore-test.log`, `semaphore-test.exit`). All 23 new runnable cases passed.
Crate all-targets clippy with `-D warnings`: exit **0** (`clippy.log`,
`clippy.exit`), no diagnostics.
Pending: crate library tests. Commands use the requested shared target and
`cargo-safe` from this worktree's `rust/`, inside tmux session `cadence-rust`.
Shared target validation is requested explicitly for this task; this is not a
clean dedicated-target mixed-cluster gate or proof of full service completion.

Checkpoint (2026-10-23 18:45 UTC / 2026-10-24 00:15 IST): implementation
commit `3076d7837` is pushed. The targeted semaphore check waited approximately
34 minutes on the shared lock and is now compiling this worktree's crates.
All validation exits remain pending at this checkpoint; the tmux runners remain
active. This checkpoint keeps the required 30-minute commit/push cadence while
waiting for shared-host validation.

Reproduce from `/mnt/storage/service/github/wt/semtests/rust` (sequentially;
check `uptime`/`free -h` first and respect the host-wide gate/build exclusion):

```sh
CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared /mnt/storage/service/bin/cargo-safe test -p cadence-matching --lib matching_semaphore > /tmp/st.log 2>&1; echo $? > /tmp/st.exit
CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared /mnt/storage/service/bin/cargo-safe clippy -p cadence-matching --all-targets -- -D warnings > /tmp/st-clippy.log 2>&1; echo $? > /tmp/st-clippy.exit
CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared /mnt/storage/service/bin/cargo-safe test -p cadence-matching --lib '' > /tmp/st-test.log 2>&1; echo $? > /tmp/st-test.exit
```

The empty final filter runs every library test. The tmux runner checks load <= 6
and available RAM >= 2 GiB before scheduling each final check. `cargo-safe` owns
`/tmp/cargo.lock`, the capped slice, `nice`/`ionice`, `-j2`, and workspace cache
invalidation. Recorded source hashes are in `source-sha256.txt`.
