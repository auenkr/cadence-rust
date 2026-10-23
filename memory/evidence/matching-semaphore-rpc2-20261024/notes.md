# Matching AcquireSemaphore RPC: the three unverified behaviours (2026-10-24)

Branch: `matching-semaphore-rpc2-20261024`, based on `origin/matching-semaphore-rpc-20261024` (8f8d672be).
Source audit: `memory/matching-semaphore-rpc-audit-20261024.md`, "Not verified here".

No cargo was run locally. All compile and test evidence is from GitHub Actions `rust-ci.yml`, package `cadence-matching`.

## Result per behaviour

| # | Behaviour | Go reference | Rust result | Test (`rust/crates/cadence-matching/src/matching_rpc_tests.rs`) |
|---|---|---|---|---|
| 1 | Rate limit: `ServiceBusyError "Matching host rps exceeded"` | `go/service/matching/handler/handler.go` `AcquireSemaphore`: `userRateLimiter.Allow` runs before the engine. `errMatchingHostThrottle`. Test: `handler_test.go` `TestHandlerAcquireSemaphore`, "rate limiter not allowed" | **Matches.** Rust `check_rpc_quota` runs before parsing and returns `ResourceExhausted`, message `Matching host rps exceeded`, plus a `ServiceBusyError` detail. Same helper as the other rate-limited RPCs | `go_handler_acquire_semaphore_rate_limited_returns_service_busy` (new, passing). Asserts code, message, and the detail via the shared `assert_host_busy` |
| 2 | Domain not found on a cache miss | `go/service/matching/handler/engine.go` `getOrCreateSemaphoreManager`: a cache miss calls `domainCache.GetDomainName(id.DomainID)` by **ID** and returns the raw error before the enabled flag. `go/common/cache/domainCache.go` `getDomainByID` calls `checkDomainExists("", id)`. `go/common/persistence/sql/sql_domain_store.go` and `nosql_domain_store.go` return `EntityNotExistsError{"Domain <id> does not exist."}`. `go/common/types/mapper/proto/errors.go` maps it to gRPC `NotFound` with the same message | **Matches on code and message.** The ID is the identity, so the message uses the UUID. Rust `History::domain_name` (`grpc_internal.rs`) calls `describe(uuid)`, which returns `Domain <uuid> does not exist.` (`cadence-persistence/src/domains.rs`), then `Status::not_found`. The domain lookup runs before the enabled check in both | `matching_rpc_semaphore_domain_not_found_matches_go` (new, passing). Asserts `NotFound` and `Domain <uuid> does not exist.` |
| 3 | Deadline-expired error | `handler.go` `AcquireSemaphore`: when the engine returns an error and the caller's context is done, it returns `InternalServiceError{ctx.Err()}` and bumps the timeout counter. `semaphore_manager.go` `grant` checks `ctx.Err()` **before its first write** (no grant on an expired caller). Once a grant succeeds, the engine returns `err == nil`, so no deadline error follows a grant | **Differed, now fixed.** Rust had no pre-grant check, so an already-expired caller was granted a slot and the RPC returned success. Fix: `grpc_matching.rs` `acquire_semaphore` returns `Status::internal("context deadline exceeded")` before `manager.acquire` when the request deadline has passed. The existing outer match then counts the timeout metric, as Go does | `matching_rpc_semaphore_expired_deadline_claims_no_slot` (new, passing). Sends `grpc-timeout: 0n`, expects `Internal "context deadline exceeded"`, then checks that both slots are still free (two grants, third `NoSlot`) |

## Code change

- `rust/crates/cadence-matching/src/grpc_matching.rs`: 5 lines added in `acquire_semaphore`, before `manager.acquire`.
- `rust/crates/cadence-matching/src/matching_rpc_tests.rs`: 3 tests added after `matching_rpc_semaphore_disabled_message_matches_go`.

Commits on the branch:
- `5385c6336` Match Go AcquireSemaphore for expired caller deadlines (code and tests).
- `db3b085f6` Drop unneeded mut in expired-deadline semaphore test (clippy `-D warnings` failure from the first run).

## Residual differences (not fixed, not asserted)

- **Deadline check and held owners.** The new pre-check also applies to an owner that already holds a slot. Go's held-owner path (`confirmHold`) does not check `ctx` itself, but the real persistence read honours `ctx`, so an expired caller gets an error in practice too. The in-memory test store has no equivalent. Not separately tested.
- **Expired deadline and a waiting bucket.** Go's `awaitStartup` can return `ctx.Err()` while a bucket is still loading. Rust has no startup timeout. This is the existing ignored gap `matching_semaphore_acquire_honors_deadline_while_starting` and is unchanged.
- **Error detail on NotFound.** Go attaches an `EntityNotExistsError` detail to the gRPC status. Rust `Status::not_found` has no detail. The test asserts code and message only. The detail is shared with other matching domain lookups, so changing it is outside this semaphore fix.
- **Error after a grant.** No RPC-level test drives a store failure after a grant with an expired caller. The in-manager test store does not seem to inject that failure. Not verified.

## CI evidence

| Run | Commit | Result |
|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/40031246241 | `5385c6336` | **Failed** in Clippy: `variable does not need to be mutable` at `matching_rpc_tests.rs:1250` (`-D warnings`). Tests did not run |
| https://github.com/auenkr/cadence-rust/actions/runs/40031425849 | `db3b085f6` | **Success** (`clippy-test`). `cadence-matching` lib tests: **571 passed, 0 failed, 12 ignored**. The 3 new tests are `ok`. Ignored count is the same 12 as before, and every parity-gap ignore is unchanged |

Previous baseline (`origin/matching-semaphore-rpc-20261024`, run 40018160286): 568 passed, 12 ignored. The difference is the 3 new tests.

## Re-run

- Command: `gh workflow run rust-ci.yml --ref matching-semaphore-rpc2-20261024 -f package=cadence-matching`
- Watch: `gh run watch <id> --exit-status`
