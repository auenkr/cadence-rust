# history-v2rest (2026-11-12)

## Tests added (real_engine_v2_tests.rs, ported from Go engine2/engine3)
conflict_resolve direct unit test; RecordDecisionTaskStarted and RecordActivityTaskStarted variants (incl. resurrected activity);
marker decision + terminate decision tests; signal-with-start NotRunning and Start_DuplicateRequests variants.
Cassandra: cas_error_maps_request_rows_to_duplicate_request (history_execution_cassandra_write.rs).
Harness: get_errors and conflict_hooks fault injection.

## Bugs found / fixed
1. In-memory conflict_resolve mutated current-run before validating all conditions (Go checks first) - fixed (test harness).
2. Rust add_decision_task_scheduled never moved Created->Running (Go ReplicateDecisionTaskScheduledEvent does, unless Zombie) - fixed in engine_decision.rs; real_engine_tests start test updated to expect Running.

## Evidence
- rust-ci (cadence-history), green on final code: https://github.com/auenkr/cadence-rust/actions/runs/43167357484
- Mixed gates on engine fix (GATE SUMMARY failed: []): mysql/grpc/true 43166368695, cassandra/grpc/true 43166399355, postgres/tchannel/false 43166403395
- Later commit was test-only.

## Not ported
Remaining items from history-eventsv2 list unchanged except those above.

## Post-merge master validation (master 9a80035ad, 2026-11-12)
- Full workspace rust-ci + all 12 history-mixed-gate combos: runs 43169177754..43169226481, all success.
