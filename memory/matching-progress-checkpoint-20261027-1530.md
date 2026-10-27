# Matching progress checkpoint — 2026-10-27 15:30 UTC

Matching is incomplete. This checkpoint supersedes stale current-status claims
in the v5/v6 ledgers; retain those ledgers for historical evidence.

## Source and validation

- Combined source before the latest lint fix: `1ce18d6ed441dd02359aaa3f90fb611532992271`.
- Diagnostic workspace run [40530286975](https://github.com/auenkr/cadence-rust/actions/runs/40530286975)
  failed Clippy before tests: redundant closure in launcher and five
  result_large_err diagnostics in matching_writer. The writer's pre-existing
  module allowance had accidentally moved onto matching_fatal.
- `e375c1c0b69540ff52271f2836ce79a37e402bfd` restores the allowance to
  matching_writer and passes FatalExit::from_error directly to and_then.
- Focused Matching run [40531087779](https://github.com/auenkr/cadence-rust/actions/runs/40531087779)
  is in progress at that exact SHA. It uses the normal CI cache and does not
  count as the fresh final workspace gate. Automatic duplicate 40531085473
  is cancelled. No local Cargo, Go or rustfmt command ran.

The combined tree already contains the reader host-fatal wiring, transient
rewrite retry, Thrift normalization and review corrections, partition error
mapping/retry/deadline/counter fixes, caller-drop cancellation regression,
membership retention and standby error-exhaustion regressions, and partial
concurrent producer/range-fencing tests. Prior delegate messages calling those
changes uncommitted or missing caller cancellation are historical.

## Required next work

1. Review the focused CI artifacts; fix any compile, Clippy or test failures.
2. Reconcile ignored-test exclusions against the existing active runtime tests
   and the Go function map. The old ignored-test audit contains corrected
   claims: the reviewed History wire fixture test exists; production runtime
   ownership tests exist; the two stale harness package selectors are fixed.
3. Concurrent add/poll, zero-dispatch, isolation and two-live-engine stealing
   scenario coverage remains partial; see matching-concurrent-go-parity-20261027.md.
4. Freeze a new immutable candidate after required source changes settle.
   Run fresh uncached workspace Clippy followed by tests on that exact pin.
5. Run all ten mixed lanes serially plus the supplemental dependency oracles,
   download their three-day artifacts, verify source/binary identities,
   topology, SDK scenarios, recovery and cleanup, and record evidence.
   Dispatch commands: matching-acceptance-evidence-readiness-20261027.md.
6. Close the audit and acceptance ledger only with actual same-pin evidence.

No final mixed lane is credited. V5 MySQL success and all other old-pin passes
remain historical. No final v7 tag has been cut.

## Orchestration and resource state

All previously reported workers completed. The Sonnet hour authorization has
expired (started approximately 14:25 UTC); use three Luna medium/default-tier
workers for independent remaining tasks. Worker starts are currently held:
host load repeatedly exceeds 6, with 3.7–4.0 GiB available. No local heavy
validation, polling loop, or new worker was started while above the guard.
Do not terminate unrelated host workloads to evade that guard.

At 15:31 UTC host load fell to 5.74 with 4.0 GiB available. Started three
Luna medium/default-tier delegates (shared checkout, disjoint ownership):
- ecee3cda-35a9-4cc4-a35d-cb2807f85811: complete concurrent delivery/live
  range-stealing regressions; owns matching_concurrent_go_parity.rs and its note.
- ba76decd-660c-4dde-9c0e-5639c4affb92: read-only current NewConfig field audit.
- 8232ab5c-efc8-4675-9b1a-bdbce829028d: read-only ignored-test reconciliation.
Their completions wake the parent automatically; do not poll delegated tasks.
Focused run 40531087779 reached Test after passing Clippy; no result credited yet.
