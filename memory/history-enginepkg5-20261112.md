# history-enginepkg5 (2026-11-12)

Branch history-enginepkg5. Only cfg(test) code changed (harness `Harness::build(configure)`, cfg(test) `QueryRegistry::raw_registry_for_test`), so no mixed gate required.

## Ported (real_engine_tests.rs `query_ports` module, +14 tests)
- TestQueryWorkflow_RejectBasedOnNotEnabled (cluster switch half), _RejectBasedOnCompleted, _RejectBasedOnFailed (NotOpen + NotCompletedCleanly), _FirstDecisionNotCompleted, _DirectlyThroughMatching, _ConsistentQueryBufferFull, _DecisionTaskDispatch_Timeout / _Complete / _Unblocked (buffered consistent query via registry SetTerminationState).
- Sticky query: goes to sticky task list via raw matching; StickyWorkerUnavailable falls back to normal task list (resets sticky).
- Activity heartbeat after cancel request returns cancel_requested=true.
- Harness additions: `FakeMatching` (QueryMatchingClient), `SupportsSticky` ClientChecker.

## Bugs fixed
None found.

## Not ported (needs more infra)
- RejectBasedOnNotEnabled by-domain half (TestConfig by-domain always true), ActiveActive query variant (domain cache has no active-active entries).
- Child workflow started/completed success path and timer fire: driven by transfer/timer queue executors and concrete mutable state, not engine RPCs; needs queue test harness.
- Child parent-close-policy decision tests (child info is a blob in MemExecutions), history_engine3_eventsv2 (other worker).

## CI
- Clippy + tests (rust-ci package=cadence-history) green: https://github.com/auenkr/cadence-rust/actions/runs/43160529935
- Final merged head 450654698: https://github.com/auenkr/cadence-rust/actions/runs/43161291597 (success)
- Earlier failed runs were compile/clippy fixes in new tests only.
