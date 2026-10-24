# Matching lifecycle gaps (2026-10-24)

Branch `matching-lifecycle-gaps-20261024` (from `origin/master` e7285da4f). Not merged.

## Gaps and changes

| Gap | Go reference | Rust change | Test (un-ignored or added) |
|---|---|---|---|
| Engine shutdown state (`is_shutting_down`) | `go/service/matching/handler/engine.go` Stop, `isShuttingDown` (~1694) | `Matching` shutdown flag shared by clones, set by `MatchingRuntime::drain` after evict + drain sleep (`matching_runtime.rs`) | `matching_engine_go_tests_b.rs::is_shutting_down_is_false_until_stop` |
| New bucket refused while shutting down | `engine.go` `errIfSemaphoreOwnershipLost` (~1675) → `SemaphoreNotOwnedByHostError` owner "not known" | `Matching::check_semaphore_ownership` (`grpc_matching.rs`), before any load | `matching_engine_go_tests_d.rs::get_or_create_semaphore_manager_refused_while_engine_shuts_down` |
| Semaphore stop on engine stop | `engine.go` Stop loops `semaphoreRegistry.AllManagers()` | `MatchingRuntime::shutdown` → `Matching::stop_semaphores` | `matching_engine_go_tests_d.rs::stop_semaphore_managers_stops_every_bucket_on_engine_stop` |
| Cancelled first load leaves semaphore manager in `Starting` | `go/service/matching/semaphore/semaphore_manager.go` `load` (error path → `Stop`), `Stop` → `onStopFn` | `Manager::start` drop guard (`LoadInFlight`) stops the manager; `Manager::stop` unregisters itself (`registered_in`) | `matching_engine_go_tests_d.rs::get_or_create_semaphore_manager_starts_unstarted_registered_manager` |
| Late stop unregisters only itself | `semaphore_manager.go` `Stop` → `onStopFn` | same `Manager::stop` unregister | `matching_semaphore_parity_tests.rs::matching_semaphore_late_stop_unregisters_only_itself` (byproduct) |

Note on item (3) of the brief: no task-list manager in Rust has a `Starting` state. The only gap test with that symptom is the semaphore manager, so that is what was fixed.

Known difference from Go: Go's `load` runs under its own start context and is not cancelled by the caller. Rust cancels the scan when the caller's future is dropped, then applies Go's load-error cleanup (stop + unregister), so the next request rescans. The test expects this outcome.

Left ignored: `matching_rpc_tests.rs` `matching_rpc_semaphore_rejects_new_buckets_while_shutting_down` (placeholder body; same behaviour now covered by `..._refused_while_engine_shuts_down`). `matching_engine_go_tests_d.rs::acquire_semaphore_request_during_load_gives_up_at_its_own_deadline` (no public seam for the cancellable startup wait).

## CI

| Run | Package | Head | Result |
|---|---|---|---|
| 40065330306 | cadence-matching | a574fe5f3 | clippy pass; test FAILED: `matching_reader::...falls_back_to_default_for_unknown_isolation_group` (left 2, right 1) |
| 40065529838 | cadence-matching | a574fe5f3 | clippy pass; test FAILED: `matching_reader::...dispatch_success_uses_known_or_default_group` and `...falls_back_to_default_for_unknown_isolation_group` |
| 40064723413 | (push) master base | e7285da4f | FAILED: `matching_reader::...dispatch_success_uses_known_or_default_group` |

The failing tests are in `matching_reader.rs`, which this change does not touch. Master's head fails the same module. Master's last green run was 40035674642 at `8d31b6f61`, before the merge11 commits added 407 lines to `matching_reader.rs`.

Not yet run: full-workspace run (no `package` input). Blocked until the reader failure is resolved or excluded on the base.
