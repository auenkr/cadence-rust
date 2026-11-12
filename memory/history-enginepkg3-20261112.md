# history-enginepkg3 (2026-11-12)

Branch history-enginepkg3. Test-only work: no non-test Rust code changed, so no mixed gate was required.

## What was built
- wiring/assembly/real_engine_harness.rs (cfg(test)): the real HistoryEngine over EngineCore/EngineExecutionCache/EngineWorkflowUpdater/EngineStartHost/EngineDecisionHandler/NotifierAdapter, with in-memory persistence instead of trait fakes.
  - MemExecutions implements GoExecutionStore: create-mode rules, update condition = stored next_event_id, task capture, current-run record, failure injection.
  - MemHistory implements GoHistoryV2Store. MemNodeStore is a HistoryNodeStore view over it, for mutable-state event reads.
  - Test domain cache, active-cluster manager, cluster metadata and clock.
- wiring/assembly/real_engine_tests.rs: 31 tests that drive start -> record decision started -> respond... through the engine and assert on persisted state.

## Go vs Rust counts
- Ported: 31 real-engine flow tests (start 3, record decision started 5, respond decision 8, activity started/completed/failed 7, signal and signal-with-start 4, terminate 1, heartbeat/timer/marker/request-cancel 4). Earlier branches: 118 engine_tests.
- Go history_engine_test.go has about 100 tests, history_engine2_test.go about 60, history_engine3_eventsv2_test.go about 8. Ported one-to-one in intent, not name-for-name.

## Deviations (Go-faithful findings, not Rust bugs)
- Unknown schedule id >= next_event_id gives MaxAttemptsExceeded (stale-state retry loop), as in Go. Tests use an id below next_event_id to get EntityNotExists.
- Empty decision schedule id is -23 (EmptyEventID), not 0.
- Persisted state stays CREATED through decision start. Tests assert only not-COMPLETED.

## Bugs fixed
None. No Rust-vs-Go divergence found.

## Remaining (not ported)
- Buffered consistent query and sticky query: need matching and a query registry in the harness.
- history_engine3_eventsv2 (reset, NDC branches): MemExecutions.conflict_resolve is unsupported.
- Respond activity canceled, cancel-requested activity heartbeat, continue-as-new, child workflow, signal-external, retry policy and timer-fire flows.
- Persisted reload after cache clear (Go's cache-miss variants).

## CI evidence (rust-ci.yml package=cadence-history)
- First green on harness: https://github.com/auenkr/cadence-rust/actions/runs/43143602467
- Green with batch 2: https://github.com/auenkr/cadence-rust/actions/runs/43144972810 (head before the master merge)
- Final run on merged head: see the final commit message / below.
