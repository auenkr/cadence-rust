# history-decisiongaps (2026-11-11)

Branch history-decisiongaps.

## Done
- 3 cron validation (validate_cron in engine_decision_ops.rs) - CI pending
- 4 cross-domain lookup (target domain id per decision, DomainCacheEntry.clusters, Validation::get_domain_by_id) - CI pending
- 2 attempt-limit terminate in handle_decision_task_failed - CI pending
- 1 bad-binary check (DomainCacheEntry.bad_binaries) + auto-reset points (Inner::add_binary_checksum_if_not_exists) - CI pending; advanced-visibility search-attr task gen not ported

## Remaining
5 buffered queries, 6 local activity dispatch
