# Matching source integration — 2026-10-30

## Candidate contents

Integrated as one Matching-only source candidate on `t3/matching-source-integration`,
based on current `origin/master` `3a9a23bb0`:

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

## Candidate CI evidence

- Semaphore candidate: https://github.com/auenkr/cadence-rust/actions/runs/41021199544
  at `26f6a88535463740d722cdeac922571a7424a943`, success. Job log shows
  workspace Clippy followed by workspace tests; formatting and Go oracle steps
  also passed. The fresh-target step was skipped, so this is not final uncached
  evidence. Mixed Go/Rust and live comparison steps were skipped.
- Lifecycle candidate: https://github.com/auenkr/cadence-rust/actions/runs/41021537962
  at `ffa57359f801aeb2f5f60d6383a3675443f7cfe9`, success. It likewise ran
  workspace Clippy then workspace tests with the normal cached target. Mixed
  Go/Rust and live comparison steps were skipped.
- Earlier run https://github.com/auenkr/cadence-rust/actions/runs/40912963402
  was persistence-only (344 passed, 13 ignored), not workspace or Matching
  transport evidence. Corrected the copied checkpoint claim; detailed scope is
  in `matching-semaphore-ci-scope-40912963402.md`.

## Required validation and remaining gaps

Dispatch `rust-ci.yml` on this branch with `clean_target=true`, no package,
then record the exact run URL, head, conclusion, and artifacts. It must complete
fresh-target workspace Clippy followed by workspace tests. Pending runs are not
passing evidence.

Matching acceptance remains incomplete: no mixed-cluster gate or SDK workflow
has exercised this candidate against the other Go services; failure,
restart/recovery, production discovery and shutdown lanes remain unverified.
Record the Rust binary SHA256 for mixed gates. Continue the parity audit before
moving to History.
