# Matching semaphore cancellation and typed errors review (2026-10-30)

## Candidate and review boundary

- Cancellation source: `origin/t3/matching-semaphore-cancel-parity`, source
  `8fa96afb8cb5463747000bdf96b0f98fcd3a29e8`, with cancellation evidence
  recorded by `dabc3d4a00656eb652f4fac9136b1b760ec9449c`.
- Typed-error source: `origin/t3/matching-semaphore-typed-errors`, head
  `f0e876398862bba9d8cc6d663a46d424474ab7ce`.
- Integrated here as a source-only review candidate over
  `9f020a4666692ab24600f791414d09e55b65a66a` (the current `origin/master` at
  review start). The launch/lifecycle files were left out of this candidate.
- Scope is semaphore caller cancellation, persistence operation completion,
  Cassandra context propagation, and typed errors through the native Matching
  gRPC adapter. No semaphore lifecycle/ticker implementation was changed.

## Go behavior checked

- `go/service/matching/handler/engine.go:400-443` parses and validates the
  request, gets or creates the manager, invokes `Acquire`, and returns its
  result or error unchanged.
- `go/service/matching/semaphore/semaphore_manager.go:174-219` makes startup
  load independent of each waiting caller's context. The caller deadline only
  bounds `awaitStartup`; startup itself has its own timeout.
- `go/service/matching/semaphore/semaphore_manager.go:273-370` checks context
  before each new conditional grant, returns persistence errors, restores a
  reservation after ambiguous write errors, and processes a successful store
  result even if cancellation races with completion. Existing holds are
  confirmed before the grant loop's context check.
- `go/common/persistence/nosql/nosql_semaphore_token_store.go:54-82` maps
  Cassandra plugin errors through common typed-error conversion and preserves
  grant outcomes. Its bucket scan similarly preserves converted errors.
- `go/common/persistence/nosql/nosqlplugin/cassandra/semaphore_tokens.go:35-88`
  applies the token/owner update as one conditional batch using the operation
  context. The token confirmation query also uses that context.
- `go/common/persistence/sql/sql_semaphore_token_store.go:55-93` returns the
  existing explicit SQL-unsupported error for token operations; Rust retains
  that unsupported-backend behavior.
- `go/common/types/mapper/proto/errors.go:28-68,289-304` uses `errors.As`-style
  conversion for wrapped typed errors. Not-found detail fields and ServiceBusy
  reason survive transport conversion; `InternalServiceError` maps to gRPC
  Internal.

## Corrections accepted in the candidate

- `SemaphoreCallContext` carries the caller deadline, explicit cancellation,
  and dropped-wire-request signal through the Matching persistence limiter to
  Cassandra confirmation and conditional grant calls.
- Cassandra races completion against caller termination with result-first
  selection. A completed result is retained; cancellation of an ambiguous
  operation returns an error so the manager restores the reservation and a
  future scan/retry resolves persisted ownership.
- The native RPC handler owns a detached task so Tonic dropping a wire request
  does not abort the independent startup scan. It observes deadline,
  cancellation, and request-drop termination when the operation returns.
- Startup scan errors retain the raw bucket identifier and wrapped source.
  Typed `EntityNotExistsError`, `ServiceBusyError` (including reason), and
  `InternalServiceError` map according to Go's type-aware transport behavior;
  untyped errors retain the `load semaphore bucket …` wrapper.
- Regression tests cover cancellation during a conditional write and existing
  hold confirmation, reservation restoration, startup scan failure and
  replacement, raw-byte identifiers, and typed/untyped error wire detail.

## Review result

No correctness defect found in the reviewed cancellation/context propagation
or typed-error mapping changes. `git diff --check` passed locally; no Cargo or
Go command was run locally.

## CI evidence and limits

- Exact candidate CI: https://github.com/auenkr/cadence-rust/actions/runs/40912963402
- Head: `f0e876398862bba9d8cc6d663a46d424474ab7ce`; conclusion: success.
- The workflow ran Formatting, Clippy, and workspace tests successfully. It
  also ran the non-mixed Go source oracles listed in the run steps.
- Matching live-comparison and mixed Go/Rust gate steps were skipped. This run
  does not establish mixed-cluster acceptance, SDK workflow compatibility,
  Cassandra integration, or Matching parity completion.
- The integrated review branch excludes the candidate's `matching_launch_native.rs`
  launch regression file to avoid taking lifecycle/launch ownership. A focused
  `rust-ci.yml` run is therefore required for this exact pushed review commit.

## Remaining risks / required follow-up

- No mixed Go/Rust gate was run; cancellation at a real wire disconnect and
  workflow recovery still need the existing Matching gates.
- Cassandra behavior has source and unit-regression coverage but no live
  Cassandra gate evidence attached here.
- No SQL runtime test was run; SQL's unsupported operation behavior is source
  parity only.
- Matching remains incomplete. This review does not approve moving on to
  History or claim service completion.
