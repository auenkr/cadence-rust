# Matching domain-cache CI failure follow-up — 2026-10-30

Base: `origin/t3/matching-startup-abort-regression` at
`17ca0308e22973bfc9216e362a679596189907be`.

## Failed workspace run and exact tests

The fresh, uncached workspace run was
<https://github.com/auenkr/cadence-rust/actions/runs/41027247246> at
`17ca0308e22973bfc9216e362a679596189907be` (`package` unset,
`clean_target=true`). Clippy and formatting passed. Workspace tests reported
983 passed, 2 failed, 2 ignored. The two failures were:

- `matching_launch_native::tests::configured_grpc_semaphore_slow_start_deadlines_cleanup_and_retry`
- `matching_runtime_native::tests::manager_config_binder_keeps_constructed_domain_name_and_live_values`

Both RPCs returned `DefaultDomainCache encounter case where domain exists but
cannot be loaded`.

## Root cause

This is a fixture metadata-version defect, not evidence of a production cache
regression. Both fixtures publish a domain record with notification version
`0` while their `metadata_version()` reports `0`. Go's
`DefaultDomainCache.refreshDomainsLocked` sorts domains and stops at the first
record with `domain.notificationVersion >= metadata.NotificationVersion`, so
it deliberately does not cache an equal-version record. Rust
`MatchingDomainCache::refresh_domains_locked` has the same strict ordering.

The failing paths are both consistent with that behavior: the runtime binder
starts its cache synchronously, but its initial refresh skips the fixture's
record; the gRPC test cache is initially empty, and its first domain lookup
checks persistence successfully before the cache refresh also skips the
record. Both then reach the intended “exists but cannot be loaded” error.
The fixture UUID matches the requested ID (`00000000-0000-0000-0000-000000000001`);
name binding/live dynamic config are only reached after the cache lookup, and
were not the defect. These test files being unchanged in the failed run did not
make the failures unrelated: both fixtures violate the source's Go-compatible
version ordering.

## Candidate change

Candidate branch: `t3/matching-domain-cache-ci-fix`.

- In the two affected in-memory sources, metadata version is now `1` while the
  record remains at notification version `0`; comments explain the Go parity
  invariant.
- In the semaphore startup/disconnect scenario, the aborted client task join is
  bounded at two seconds and asserts cancellation. Its semantics are unchanged:
  the server-side startup scan remains alive and the later waiter still proves
  manager reuse and successful grant.

No production behavior, SDK API, retry behavior, ignored test, or assertion was
weakened. No additional test was added because the existing two failures are the
regression coverage once their fixtures represent a valid metadata snapshot.

## Validation

Focused `rust-ci.yml` run dispatched with `package=cadence-matching`:
<https://github.com/auenkr/cadence-rust/actions/runs/41028524128>.
Result to record after inspection: pending.

If focused CI passes, run the exact committed candidate through `rust-ci.yml`
with `package` unset and `clean_target=true`; require sequential workspace
Clippy then workspace tests. PostgreSQL mixed validation remains undispatched
until that fresh workspace run passes.
