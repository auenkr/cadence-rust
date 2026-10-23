# Matching AcquireSemaphore RPC audit — 2026-10-24

Scope: the Matching gRPC `AcquireSemaphore` handler (History calls it) compared with Go. Branch `matching-semaphore-rpc-20261024`, from `origin/master` at e9b8e9a0e.

Not run locally: no cargo. The new tests compile and run only in GitHub Actions (`rust-ci.yml`, package `cadence-matching`). Record the run URL below once it exists.

## Wire surface

- Go matching exposes exactly one semaphore RPC: `rpc AcquireSemaphore` in `go/proto/internal/uber/cadence/matching/v1/service.proto`. There is **no `ReleaseSemaphore` RPC in Go matching**. `service_semaphore.proto` is the frontend API and has only `CreateSemaphore` in this snapshot.
- Release happens only inside Go persistence (`go/common/persistence/semaphore_token_manager.go` `ReleaseSemaphoreToken`). Nothing in `go/service/matching` calls it. The Rust matching crate has no release RPC either. Release validation (`TokenID < 1`, empty `OwnerID`, bucket validation) is persistence-level and is out of this matching audit.
- Outcome enum on the wire: `INVALID = 0`, `ACQUIRED = 1`, `NO_SLOT = 2`. Rust `grpc_matching.rs` hard-codes `1` / `2` / `0` for the same values. Match.

## Go request path (`go/service/matching/handler`)

1. `handler.go` `AcquireSemaphore`: domain name lookup (metrics only, failure becomes `""`), user rate limiter `Allow`, else `ServiceBusyError "Matching host rps exceeded"`.
2. `engine.go` `AcquireSemaphore`: `ParseRequestOwner` first (BadRequest `invalid owner id: <ParseOwner err>`), then the log line, then `NewIdentifier` (BadRequest).
3. Cache miss only: `GetDomainName` (raw error), `EnableDistributedSemaphore(domainName)` else `ErrSemaphoreDisabled` (a BadRequestError), `errIfSemaphoreOwnershipLost` (ring check, `SemaphoreNotOwnedByHostError`), then `GetOrCreate`.
4. Every path calls `Start` with a 30s `semaphoreManagerStartTimeout`.
5. `semaphore_manager.go`: `Acquire` (ownerID empty → BadRequest, MarkAlive, await startup, `grant`, then `enqueue` on NoSlot). `grant` runs at most `maxGrantAttempts = 3`. Returns `ErrNotReady` (ServiceBusy) when stopped.
6. Error after the caller's context ends becomes `InternalServiceError{hCtx.Err()}` and bumps a timeout counter.

## Rust request path (`grpc_matching.rs` `acquire_semaphore`)

Same order. Rate limit via `check_rpc_quota` (ResourceExhausted, `ServiceBusyError "Matching host rps exceeded"`). Owner parse, log line, `Identifier::new`, cache hit → `start()`. Miss → domain name, enabled flag, `membership.check_semaphore`, `get_or_create_with_idle_ttl`. Then `acquire`. Status map in `semaphore_status`. Deadline-expired error → `Status::internal("context deadline exceeded")`.

## Behaviour checks

| Go behaviour | Rust | Status |
|---|---|---|
| Owner decode error text, `invalid owner id: owner_id "x" has no length prefix` | `cadence_common::semaphore::parse_owner` mirrors Go messages | Verified by new test `matching_rpc_semaphore_owner_errors_match_go_text` |
| Non-canonical length prefix text (`"01"`) | same | Verified (same test) |
| Owner decoded before bucket validation | same order | Verified (same test) |
| `domainID is required`, `semaphoreName is required`, `bucket must not be negative, got -1` as InvalidArgument | `Identifier::new` | Verified by `matching_rpc_semaphore_identifier_errors_match_go_text` |
| `ErrSemaphoreDisabled` text, InvalidArgument | `Status::invalid_argument` with same text | Verified by `matching_rpc_semaphore_disabled_message_matches_go` (the older test checked the code only) |
| Acquired / NoSlot wire values | `1` / `2` | Verified by existing `matching_rpc_semaphore_acquire_grants_and_reports_no_slot` |
| Max 3 grant attempts | `MAX_GRANT_ATTEMPTS = 3` | Constant matches Go `maxGrantAttempts = 3`. Give-up behaviour is covered by existing manager test `matching_semaphore_grant_gives_up_after_max_attempts`; the test was not re-read for its assertions |
| Default idle TTL 5m (`matching.semaphoreIdleTime`) | `DEFAULT_IDLE_TIME_NANOS` = 300e9 | Verified by existing `matching_config` test |
| Ring ownership → Aborted `SemaphoreNotOwnedByHostError` | `check_semaphore` | Verified by existing `matching_semaphore_domain_filter_and_ring_admission_precede_creation` |
| Cached manager skips domain and ring checks, `Start` still called | fast path | Verified by existing `matching_rpc_semaphore_acquire_grants_and_reports_no_slot` |
| Not-ready → ServiceBusy text | `semaphore_status` maps `NotReady` | Covered at the manager level only (no RPC-level test) |
| Start timeout 30s | `start()` has no timeout | Parity gap, already ignored: `matching_semaphore_acquire_honors_deadline_while_starting` |

## Parity gaps (ignored tests)

1. **Shutdown branch missing.** Go `errIfSemaphoreOwnershipLost` returns `SemaphoreNotOwnedByHostError{OwnedByIdentity: "not known", ...}` while the engine is shutting down. Rust `Membership::check_semaphore` has no shutdown state, so new buckets are admitted during shutdown. Ignored test: `matching_rpc_semaphore_rejects_new_buckets_while_shutting_down`. Fix needs a shutdown flag on `Membership`, more than 40 lines, so it is not fixed here.
2. **Cancellation not mapped.** Go turns any engine error after the caller's context ends, cancel or deadline, into `InternalServiceError`. Rust only checks the deadline. Ignored test: `matching_rpc_semaphore_cancelled_context_is_internal_error`. tonic drops cancelled handler futures, so the response is never sent and the difference only affects the metric. Low impact.
3. **Start timeout and deadline during load.** Covered by the existing ignored test listed above.

## Not verified here

- The rate-limit path (`ServiceBusyError "Matching host rps exceeded"`) for AcquireSemaphore has no RPC-level test. Go has a handler-level test (`TestHandlerAcquireSemaphore`, "rate limiter not allowed").
- Domain-not-found on cache miss: Go returns the raw domain cache error. The Rust `domain_name` error code was not checked.
- Deadline-expired error mapping after the grant (`context deadline exceeded` as Internal). The Go test (`expired` context) uses a timeout error from persistence; there is no Rust RPC test for it.

## Re-run

- Branch: `matching-semaphore-rpc-20261024`.
- Command: `gh workflow run rust-ci.yml --ref matching-semaphore-rpc-20261024 -f package=cadence-matching`.
- CI run: https://github.com/auenkr/cadence-rust/actions/runs/40018160286 (success, clippy-test). Lib tests: 568 passed, 12 ignored; the three new tests passed and the two gap tests are ignored.
