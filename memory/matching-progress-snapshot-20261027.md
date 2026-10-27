# Matching progress snapshot 2026-10-27

Candidate v5: tag `matching-acceptance-20261027-v5`, SHA 8d7dac38e. Ledger: matching-v5-final-acceptance-ledger-20261027.md.

Results so far
- Workspace Clippy+test run 40483216431: FAILED at Clippy. 4x `struct update has no effect`
  in crates/cadence-matching/src/matching_cold_domain_metric_tests.rs lines 123,153,180,198
  (test-only code). Fix needed -> new source pin (v6), all gates must rerun on it.
- Two-node MySQL gate (late-name/quota metrics, dual Prometheus) run 40483221504: SUCCESS on v5 SHA
  (superseded if pin changes).
- Not yet run: PostgreSQL, MySQL probes, Thrift, Configstore, shard distributor, Cassandra,
  adaptive two-node, active offer, production TLS.
- Luna audits (RPC inventory, ignored-test, lifecycle/persistence) were running; Codex usage limit hit, status unknown - check outputs.

Next
1. Remove the redundant `..Default::default()` in those 4 test spots, push, cut v6 tag.
2. Dispatch workspace CI on v6, then mixed lanes serially.
3. Finish the 3 audits, fill ledger, only then claim Matching complete.
Matching is NOT complete.
