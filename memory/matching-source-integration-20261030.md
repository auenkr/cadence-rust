# Matching source integration — 2026-10-30

## Candidate contents

Integrated as one Matching-only source candidate on the current `origin/master`
line, with the separate discovery diagnostic harness change reviewed and
included:

- Semaphore caller cancellation, startup scan handling, persistence error types,
  Cassandra context propagation, and typed gRPC error mapping from source
  `26f6a88535463740d722cdeac922571a7424a943` (`t3/matching-semaphore-parity-review`).
- Invalid liveness ticker recovery from source
  `ffa57359f801aeb2f5f60d6383a3675443f7cfe9` (`t3/matching-lifecycle-recovery`),
  including retaining the partial manager and its Go shutdown behavior.
- Manager-construction domain binding from `1a88d5d46` and the omitted
  additional provider binding from `3d36aef92aab362ba52772868ba3307f11283359`.
  The manager captures its resolved domain name; values still refresh from the
  dynamic config overlay. A regression checks the captured name and live update.
- Parent deadline forwarding from `82f03468b`: the child reader deadline is
  passed through native dispatch and in-process forwarding, capped by the
  existing two-second limit. Regression coverage includes slow parent
  resolution and withdrawal at the caller deadline.
- Configured gRPC semaphore regression and corrected fixture from
  `c37d8a8cb15525baa66735b8b9b82a637ac29f24`: list/get return the same domain,
  and the initial scan wait fails with the premature RPC result. Also retained
  the Matching persistence limiter membership wiring from the combined
  predecessor `f4ffd30798955dabe34bbd0efe5736bacad17f61`.

No History implementation was introduced. `git diff --check` passed. No local
Cargo, Go, rustfmt, build, or runtime validation was run.

## Independent review and diagnostic harness

Reviewed the construction-time domain binding/live dynamic values, forwarded
deadline propagation through local and remote gRPC clients, invalid liveness
TTL creation/cleanup behavior, semaphore waiter cancellation and typed error
mapping, configured TCP gRPC fixture, and persistence limiter membership/store
wiring against the corresponding Go paths. One small Go-parity race was found
in `Registry::get_or_create_with_idle_ttl`: it constructed/validated a manager
before taking the map lock, so a concurrent winner could be present by the
time this request returned a stale invalid-TTL error. Manager construction now
happens synchronously under the registry lock, matching Go `GetOrCreate`; the
lock is released before `Start().await`, and invalid TTL still creates no
partial registration. The semaphore manager resolves the domain name
when it is created, keeps that name for its dynamic-config filters, and reads
overlay values live; existing managers retain their creation-time idle TTL as
Go does. The source regressions cover the slow parent resolver/deadline and
configured gRPC initial scan behavior.

Also reviewed and included diagnostic change
`64fc423836e5366b3d3710a992121ee5a1b8c0ef` in
`tests/matching-service-compat.cjs`. It probes the exact primary Rust endpoint
independently while preserving the Go Frontend discovery readiness requirement
and SDK/restart flow. It adds direct-primary status beside the last Go Frontend
readiness error. If readiness already failed, forced SIGKILL cleanup is logged
and the original readiness error is rethrown. The no-Go-Matching gate assertion
and production SDK assertions remain intact. This is diagnostic only. The
earlier PostgreSQL failure still has no proven root cause: retained logs show a
Ringpop entry was installed, while the later empty-ring error does not identify
the contemporaneous ring contents or establish whether the selected Rust RPC
route completed.

## Candidate CI evidence

- Semaphore candidate: https://github.com/auenkr/cadence-rust/actions/runs/41021199544
  at `26f6a88535463740d722cdeac922571a7424a943`, success with
  `package=cadence-matching`: focused Matching Clippy/test, including 977
  library tests passed and 2 ignored. Artifact records the normal cached target;
  workspace formatting exited 1, so this is not workspace or fresh-target
  evidence. Mixed Go/Rust and live comparison steps were skipped.
- Lifecycle candidate: https://github.com/auenkr/cadence-rust/actions/runs/41021537962
  at `ffa57359f801aeb2f5f60d6383a3675443f7cfe9`, success with
  `package=cadence-matching`: focused Matching Clippy/test on the normal cached
  target, not workspace or fresh-target evidence. Mixed Go/Rust and live
  comparison steps were skipped.
- Earlier run https://github.com/auenkr/cadence-rust/actions/runs/40912963402
  was persistence-only (344 passed, 13 ignored), not workspace or Matching
  transport evidence. Corrected the copied checkpoint claim; detailed scope is
  in `matching-semaphore-ci-scope-40912963402.md`.

## Required validation and remaining gaps

Final fresh-target workspace validation, including the harness change, is
pending dispatch on the immutable reviewed head. Record its exact run URL, head,
conclusion, and retained artifacts. It must complete fresh-target workspace
Clippy followed by workspace tests. Pending runs are not passing evidence.

The older exact-source run https://github.com/auenkr/cadence-rust/actions/runs/41022755953
was still in progress on `d75ecfe9f7a649d5e1f9acfe46e16f1b783ac808` at the last
check (fresh target selected; workspace Clippy running; tests pending). It does
not include the diagnostic change or this review fix. Inspect its logs/artifacts
after it reaches a terminal state; do not transfer its credit to the final head.

Matching acceptance remains incomplete: no mixed-cluster gate or SDK workflow
has exercised this candidate against the other Go services; failure,
restart/recovery, production discovery and shutdown lanes remain unverified.
Record the Rust binary SHA256 for mixed gates. Continue the parity audit before
moving to History.
