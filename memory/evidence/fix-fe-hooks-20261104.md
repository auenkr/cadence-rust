# fix-fe-hooks (C4, C5, C6) evidence, 2026-11-04

Branch: t3/fix-fe-hooks. Code head: 618cca72ada4cb2d0f14eb1bacc57e37a7aba5fa

Production `build_workflow_api` now installs the hooks that only tests used before:
- C4: `BadBinaryAdmission` (`with_poll_admission`). Rejects decision polls from bad binaries and increments `Counter::BadBinary`.
- C5: `context_from_headers` supplies the isolation group and partition config. The partition config goes to History (Start field 62, SignalWithStart field 30). `DomainIsolationState` serves as both the drain check and the poll waiter.
- C6: `ProductionPollCanceller` calls Matching `CancelOutstandingPoll`. The poll cancellation request task list types are now 0 (decision) and 1 (activity); they were 1 and 2 before.

New code is in `rust/crates/cadence-frontend/src/production_hooks.rs`, with unit tests.

## Runs
- rust-ci (clippy + tests): https://github.com/auenkr/cadence-rust/actions/runs/41753329353 (success)
- frontend-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/41754391813 (success)

Earlier rust-ci runs failed on clippy and then rustfmt. Both were fixed in 5933c5fd4 and 618cca72a.
