# Matching ignored tests: disposition (2026-10-25)

Base: t3/matching-integration-20261025 (aa268a8d3). Gap-list item 3 and 7 were stale: the named tests are already un-ignored on this base.

Un-ignored on base (no change needed): decision_started_after_long_poll_window_still_returns_task (tests/matching_longpoll_expiry_parity.rs), matching_semaphore_new_manager_validates_params, matching_semaphore_scan_threads_page_tokens, acquire_semaphore_request_during_load_gives_up_at_its_own_deadline.

Fixed in this branch (cadence-common poller.rs matching Go poller/manager.go): `cancel` returns bool (Go CancelPoll), `set_history_callback` fires on start/end with identity. Un-ignored poller_history_callback_fires_on_start_and_end_with_identity and poller_cancel_reports_whether_the_poll_is_known.

Remaining ignored, left as-is:
- Live/mixed-gate (need external Go/DB, not CI unit lane): matching_delivery_go_history, generated_go_history_server_accepts_native_tchannel_calls, go_history_tchannel_wire_oracle_consumes_reviewed_fixture, rust_executor_and_spectator_interoperate_with_pinned_go_handlers, persistence MySQL/Postgres TLS/Cassandra/SQLite/domain-metadata interop tests.
- Parity gaps needing injectable-failure seams or private-closure access (not implemented, would need production refactors): query dispatch error, update/refresh partition config manager errors, get_task_lists poller identity, list partition owner hosts, errIfShardOwnershipLost, domain change/failover callbacks, NewConfig field table, handleErr, thrift handler internal error, rpc non-durable describe task_id_block/empty, go_dispatch_task_active_offer_without_error_reaches_poller (suspected unreachable History fixture).

CI: https://github.com/auenkr/cadence-rust/actions/runs/40187030595 (package=cadence-matching), conclusion success, head SHA = commit before this memory file (see git log t3/mw-ignored-tests).
