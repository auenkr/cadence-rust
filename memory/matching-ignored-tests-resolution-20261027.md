# Matching ignored-test resolution (2026-10-27, static; no Cargo/Go run)

## Active-offer regression (matching_partial_upgrade_e.rs, go_dispatch_task_active_offer_without_error_reaches_poller)
- Cause hypothesis: fixture History peer was 127.0.0.1:1 (unreachable), so the sync-matched task's RecordDecisionTaskStarted (Go engine.go:808) failed and the poller returned empty.
- Change: new `StartingHistory` tonic mock (pattern from tests/matching_longpoll_expiry_parity.rs `SlowStartingHistory`) answering RecordDecisionTaskStarted with started_event_id = schedule_id+1; `durable_with_history(url)` / `registry_with_history`. `#[ignore]` removed; asserts non-empty task_token and started_event_id == 10.
- UNVERIFIED until CI runs it. If it still fails with a reachable History, the ignore reason becomes a confirmed sync-match bug (then inspect matching_dispatch.rs poll path). No production code touched.

## Remaining actionable exclusions
- postgres_tls_live_oracle (rust/crates/cadence-persistence or cadence-matching tests/matching_postgres.rs:1306, env CADENCE_POSTGRES_ADDR): no harness; actionable (needs Postgres TLS gate).
- generated_go_history_server_accepts_native_tchannel_calls (matching_history_tchannel.rs:2387, env CADENCE_HISTORY_PEER_BIN): no harness; wire behavior indirectly covered by mixed gates; actionable only if a direct lane is wanted.
- go_thrift_handler_internal_error_maps_to_internal_service_error (partial_upgrade_e.rs): seam gap, dispatch takes concrete Matching.
- Other A1 seam-gap ignores (audit): not re-verified here; not proven bugs.

## Section B
Not re-mapped function-by-function in this pass (bounded). Known Rust counterparts by name: src/matching_scaler_go_tests.rs (adaptive scaler), matching_semaphore_parity_tests.rs, matching_handler_go_tests.rs, matching_query_forward_family_tests.rs, tests/matching_engine_go_tests*.rs. Isolation/forwarder/reader/limiter counterparts unconfirmed; not claimed as gaps.
