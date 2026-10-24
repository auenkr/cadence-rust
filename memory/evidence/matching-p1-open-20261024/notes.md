# Matching P1 open items, code and unit/RPC tests only (2026-10-24)

**Branch:** `matching-p1-open-20261024` (base `origin/master` @ `e7285da4f`).
**Source:** `memory/matching-open-gaps-20261024-v2.md` on `origin/matching-open-gaps-v2-20261024`, section 3 (OPEN items), and audit-v4 P1 text.
**Scope:** at most three OPEN items that close without a live gate. Skipped by instruction: P0-3 (OpenFeature), P1-15 (TLS), and anything already on another branch (describe-gaps, lifecycle-gaps, rowskip-lateStop, panic-recovery, longpoll-fix, hash32).
**Method:** no cargo locally. Verification is GitHub Actions only (`rust-ci.yml`).

## Item 1. P1-9 `matching.errorInjectionRate` alias: closed, not a gap, no code change

- **Claim (audit-v4 P1-9, v2 section 3):** Go has no `matching.errorInjectionRate` key. Rust reads it as a Rust-only alias at `rust/crates/cadence-matching/src/launcher.rs:706`. Proposed fix: remove the alias.
- **Go reference:** the key exists. `go/common/dynamicconfig/dynamicproperties/constants.go:5424-5425` defines `MatchingErrorInjectionRate` with `KeyName: "matching.errorInjectionRate"`. It is read at `go/client/clientfactory.go:183`, which wraps the matching client in `errorinjectors.NewMatchingClient` when the rate is non-zero. The matching client is what partition forwarding uses.
- **Rust:** `launcher.rs:706` feeds `matching_fault_injection::install_client_error_rate`, which wraps the outbound matching client in `inject_client_call`. This is the same behaviour.
- **Decision:** removing the read would drop a real Go key and break parity. No production change. The audit premise ("Go does not have this key") is wrong and is recorded here so nobody removes the alias.
- **Test:** none added. The existing `matching_fault_injection.rs` tests cover the injector behaviour (`client_injection_skips_forwarding_for_busy_and_always_returns_fake`, `fake_error_selection_and_forwarding_follow_go`).

## Item 2. P1-5 (part): semaphore manager parameter validation

- **Go reference:** `go/service/matching/semaphore/semaphore_manager.go:100-122` (`validateParams`) and `:125` (`NewManager`). An invalid identifier or `IdleTTL <= 0` returns `types.BadRequestError` and no manager. Test: `TestNewManagerValidatesItsParams` (`semaphore_manager_test.go:136`). Its nil Tokens, Logger, OnStopFn and TimeSource cases have no Rust counterpart, because those are typed constructor arguments in Rust.
- **Rust before:** `Manager::new` (`rust/crates/cadence-matching/src/matching_semaphore.rs`) was infallible. It clamped the TTL with `.max(1)` in `Manager::new`, `Registry::with_idle_ttl` and `Registry::get_or_create_with_idle_ttl`.
- **Change:**
  - `Identifier::validate` (private) holds the three identifier checks. `Identifier::new` and `Manager::new` both use it, so a struct literal cannot skip them.
  - `Manager::new` returns `Result<Self, ManagerError>`. It returns `BadRequest` for an invalid identifier or a TTL `<= 0` (`"ManagerParams.IdleTTL must be positive"`, the Go message).
  - The registry no longer clamps the TTL. A bad value fails the request with BadRequest, as Go's `NewManager` error does in `engine.go:469-478`.
- **Behaviour change (intended, matches Go):** a `matching.semaphoreIdleTime` dynamic-config value of 0 or below now fails `AcquireSemaphore` with `InvalidArgument`/BadRequest. Before, it was clamped to 1ns. The default (5 min) is unaffected. No production caller passes a TTL other than the config value or the default.
- **Test:** `rust/crates/cadence-matching/src/matching_semaphore_parity_tests.rs`, `matching_semaphore_new_manager_validates_params`. Un-ignored and rewritten. It asserts BadRequest for three bad identifiers and for TTL `0` and `-1s`, asserts the valid case builds, and asserts `Registry::with_idle_ttl(0).get_or_create` returns BadRequest.
- **Still open under P1-5:** page tokens (`matching_semaphore_scan_threads_page_tokens`), nil/unknown rows (`matching_semaphore_scan_skips_nil_and_unknown_rows`, on the rowskip branch), and the Go grant-retry and max-attempts tests.

## Item 3. P1-5 (part): acquire during a slow load gives up at its own deadline

- **Go reference:** `semaphore_manager.go:262-286` (`Acquire`, `awaitStartup` waits on `ctx`), `:160-174` (`Start` returns at once while a load is in flight, so a caller's context is not held), and `handler/engine.go:485-494` (`Start` with its own timeout).
- **Rust:** `Manager::acquire` (`matching_semaphore.rs`, `pub async fn acquire`) already waits on the startup `watch` with `timeout_at(deadline)` (commit `4940e5809`). `Manager::start` returns at once while `Starting`, the same as Go. The gRPC handler `acquire_semaphore` (`grpc_matching.rs`) already maps `ManagerError::Deadline` plus a passed deadline to `Status::internal("context deadline exceeded")`.
- **Finding:** the ignore reason on `tests/matching_engine_go_tests_d.rs` ("the handler applies the gRPC deadline only after acquire returns") describes code that is no longer on master. No production change was needed.
- **Change:** removed the `#[ignore]` attribute from `acquire_semaphore_request_during_load_gives_up_at_its_own_deadline`. The test is the gRPC-level Go-shaped check: the first request holds in the scan, and the second, with a 20ms `grpc-timeout`, must return `Internal "context deadline exceeded"`.
- **Risk:** if CI fails this test, the item is not closed, and the failure output decides whether the gap is real.

## CI evidence

Code head: `0bd8f91415261170c904446b1c84a5e4469be0f2`. Every run below is on that head.

| run | event | scope | result | URL |
|---|---|---|---|---|
| 40066654129 | workflow_dispatch `package=cadence-matching` | `cadence-matching` | success. `matching_semaphore_new_manager_validates_params ... ok`, `acquire_semaphore_request_during_load_gives_up_at_its_own_deadline ... ok` | https://github.com/auenkr/cadence-rust/actions/runs/40066654129 |
| 40066901350 | workflow_dispatch (whole workspace) | whole workspace | failure: 2 tests, both `matching_reader::tests` (see below) | https://github.com/auenkr/cadence-rust/actions/runs/40066901350 |
| 40067098064 | workflow_dispatch (whole workspace, rerun, same head) | whole workspace | success | https://github.com/auenkr/cadence-rust/actions/runs/40067098064 |

The push-triggered run `40066649759` on `0bd8f9141` shows `cancelled`. The dispatched run superseded it.

**Workspace failure in run 40066901350.** Two reader tests failed with the same ordering signature (`left: 2`, `right: 1`, a task id mismatch):

- `matching_reader_dispatch_success_uses_known_or_default_group` (`matching_reader.rs:2232`). This one is not on the known-flake list.
- `matching_reader_pump_falls_back_to_default_for_unknown_isolation_group` (`matching_reader.rs:2273`). This is the known flaky test, which is fixed on `matching-flaky-reader-20261024`. Not touched here.

No code in this branch touches the reader. The rerun on the same head passed, so the failure is intermittent and not caused by the semaphore change. The first-run failure of the non-listed test is recorded here and was not fixed in this branch. It is a reader-ordering race, not part of this item.

The notes commit that records these URLs is docs-only. The code head stays `0bd8f9141`.

## Still open (not touched here)

- P1-1, P1-2, P1-13, P1-14: gate-only.
- P1-3 (metrics matrix): the Rust error classes already exist (`matching_persistence_limiter.rs`, `PersistenceErrorClass`). Closing it needs an emission and tag comparison against a Go run, plus a row-count/payload table. It is larger than one pass and was not taken.
- P1-8: needs a mixed Go/Rust ring-order fixture (gate).
- P1-12: Go matcher test port (large).
