# history-decisiongaps (2026-11-11)

Branch `history-decisiongaps`.

## Done (items 1-4 CI green)
- rust-ci cadence-history, head a3b3352b5: success
  https://github.com/auenkr/cadence-rust/actions/runs/43053350289
- 1 bad-binary check (DomainCacheEntry.bad_binaries) + auto-reset points (Inner::add_binary_checksum_if_not_exists in engine_decision.rs). Advanced-visibility search-attr task generation not ported.
- 2 attempt-limit terminate in handle_decision_task_failed (no new decision after terminate)
- 3 cron validation (validate_cron, cadence_common::cron::validate_schedule)
- 4 cross-domain lookup (per-decision target domain id, DomainCacheEntry.clusters)

## Item 5 buffered queries (CI pending)
- completed_run calls decision::handler::handle_buffered_queries after the update. Client impl/feature headers are not carried into the engine, so the version check is fail-open (Go behavior with absent headers). Go also stores client version on execution info: not ported.

## Remaining
- 6 local activity dispatch (not started)
