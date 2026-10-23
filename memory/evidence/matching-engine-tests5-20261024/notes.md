# Matching engine_test.go parity: functions 19-22 (matching-engine-tests5-20261024)

Go source: `go/service/matching/handler/engine_test.go` (the handler/ subdirectory).
Rust port: `rust/crates/cadence-matching/tests/matching_engine_go_tests_d.rs`.
Functions 1-18 are covered by other `matching_engine_go_tests*.rs` files; this
file covers 19-22 in file order:

- 19 `TestGetOrCreateSemaphoreManager` (8 subtests)
- 20 `TestUnloadSemaphoreManager`
- 21 `TestStopSemaphoreManagers`
- 22 `TestAcquireSemaphore` (4 subtests)

## Evidence

- Commit: `8e0f230f7` on `matching-engine-tests5-20261024`
- CI run (conclusion: success): https://github.com/auenkr/cadence-rust/actions/runs/40033028119
  (`rust-ci.yml`, `package=cadence-matching`, same commit)
- Result for `matching_engine_go_tests_d`: **10 passed, 0 failed, 4 ignored**
  (14 Go subtests: 10 run, 4 ignored with named gaps)
- Earlier run that failed before reaching the integration target:
  https://github.com/auenkr/cadence-rust/actions/runs/40032734549
  `cargo test -p cadence-matching` runs the lib unit tests first and stops there.
  The lib test `grpc_matching::durable_tests::matching_rpc_reports_arrival_qps_by_isolation_group`
  failed (`matching_rpc_tests.rs:2094`). That test is outside this branch's changes;
  the same commit passed on rerun, so the failure looks timing-related. Not investigated further.
- The ignored cases were judged from reading the production code, not by running them.
  Clippy with `-D warnings --all-targets` passed on both runs, so the ignored bodies compile.
- Local cargo was not run; the CI log is the only evidence.

## Per-case table

| Go test | Rust test | Status | Run URL |
| --- | --- | --- | --- |
| TestGetOrCreateSemaphoreManager / second request reuses the manager already built | get_or_create_semaphore_manager_second_request_reuses_built_manager | pass | 40033028119 |
| TestGetOrCreateSemaphoreManager / a registered manager that was never started is started by the next request | get_or_create_semaphore_manager_starts_unstarted_registered_manager | ignored: a request cancelled during the startup load leaves the Rust manager in Starting; `start()` returns Ok for Starting without loading, so the bucket never loads | 40033028119 |
| TestGetOrCreateSemaphoreManager / domain has semaphores disabled | get_or_create_semaphore_manager_domain_has_semaphores_disabled | pass (partial: Go's "registry stays empty" check is not observable through the public API; checked via zero scans and the error) | 40033028119 |
| TestGetOrCreateSemaphoreManager / bucket is owned by another host | get_or_create_semaphore_manager_bucket_is_owned_by_another_host | pass (partial: Rust checks the `Aborted` status and owner identities in the message text, not typed `SemaphoreNotOwnedByHostError` fields; Rust identities are `remote`/`local`) | 40033028119 |
| TestGetOrCreateSemaphoreManager / matching engine is shutting down | get_or_create_semaphore_manager_refused_while_engine_shuts_down | ignored: Rust Matching has no engine shutdown state; `acquire_semaphore` always reaches the store | 40033028119 |
| TestGetOrCreateSemaphoreManager / manager that failed to start is not left registered | get_or_create_semaphore_manager_failed_start_is_not_left_registered | pass | 40033028119 |
| TestGetOrCreateSemaphoreManager / domain lookup fails | get_or_create_semaphore_manager_domain_lookup_fails | pass (partial: Rust returns `NotFound` status from the domain store, not the raw lookup error) | 40033028119 |
| TestGetOrCreateSemaphoreManager / load runs outside the creation lock | get_or_create_semaphore_manager_load_runs_outside_creation_lock | pass | 40033028119 |
| TestUnloadSemaphoreManager | unload_semaphore_manager_stops_it_and_next_request_builds_fresh | pass (partial: Rust has no engine-level `unloadSemaphoreManager`; the test uses `Registry::unregister` + `Manager::stop`) | 40033028119 |
| TestStopSemaphoreManagers | stop_semaphore_managers_stops_every_bucket_on_engine_stop | ignored: no shutdown path stops every semaphore manager; `maintain_membership_until` stops only buckets the ring no longer assigns, and `Registry` has no stop-all | 40033028119 |
| TestAcquireSemaphore / a free slot is granted and named in the response | acquire_semaphore_free_slot_is_granted_and_named_in_response | pass | 40033028119 |
| TestAcquireSemaphore / a full bucket answers no-slot rather than an error | acquire_semaphore_full_bucket_answers_no_slot_rather_than_error | pass | 40033028119 |
| TestAcquireSemaphore / an owner id that cannot be decoded is refused before any store call | acquire_semaphore_undecodable_owner_is_refused_before_any_store_call | pass | 40033028119 |
| TestAcquireSemaphore / a request arriving while the bucket loads gives up at its own deadline | acquire_semaphore_request_during_load_gives_up_at_its_own_deadline | ignored: `Manager::acquire` waits for the startup load with no deadline; the handler applies the gRPC deadline only after `acquire` returns | 40033028119 |

## Notes for the next worker

- Owner IDs use the canonical length-prefixed encoding (`cadence_common::semaphore::Owner::encode`);
  `parse_owner` rejects strings without the length prefix.
- `Registry` and `Manager` are the public seams. `Registry::new` takes an
  `Arc<dyn SemaphorePersistence>`, so a scripted store covers the Go mock.
- The handler path needs a UUID domain ID (`grpc_internal::History::domain_name` parses it)
  and a domain in `AppState::domains`. Enable the flag with
  `system.enableDistributedSemaphore` filtered by `domainName`.
- Mapping from Go: `testSelfHost` is membership identity `local`; `testOtherHost` is `remote`.
- The ignored cases that look like production bugs are the two behavioural gaps:
  a cancelled first load leaves the bucket unloadable (`Starting` state), and
  `acquire_semaphore` does not honour the caller's deadline while waiting for startup.
  Neither was changed here, per the brief.
