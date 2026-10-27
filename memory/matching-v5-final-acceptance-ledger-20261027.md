# Matching candidate v5 final acceptance ledger

Final ref: `matching-acceptance-20261027-v5`
Final SHA: `8d7dac38e8d7e5a8114c6da5e0c3cc3afcbc77d0`

Matching remains incomplete. Source pin is stable; subsequent master commits
must be documentation-only while these results are collected. A required source
change invalidates this candidate and must be recorded explicitly. All runs
must be inspected against the actual final SHA and binary hashes, not only the
workflow conclusion. Historical and diagnostic results do not fill these rows.

| Requirement | Run | Reviewed result | Evidence |
| --- | --- | --- | --- |
| Fresh uncached workspace Clippy then tests | [40483216431](https://github.com/auenkr/cadence-rust/actions/runs/40483216431) | Running; Clippy active at last inspection | Pending |
| PostgreSQL | Not dispatched | Pending | Pending |
| MySQL probes | Not dispatched | Pending | Pending |
| Thrift | Not dispatched | Pending | Pending |
| Configstore | Not dispatched | Pending | Pending |
| Shard distributor | Not dispatched | Pending | Pending |
| Two-node MySQL, late-name/quota metrics, dual Prometheus | [40483221504](https://github.com/auenkr/cadence-rust/actions/runs/40483221504) | Running; build active at last inspection | Pending |
| Cassandra | Not dispatched | Pending | Pending |
| Adaptive two-node | Not dispatched | Pending | Pending |
| Active offer | Not dispatched | Pending | Pending |
| Production TLS | Not dispatched | Pending | Pending |
| Source RPC inventory native/Thrift audit | Luna task e2c5cde4-1e17-43bf-8690-03d65cf4a872 | Running | Pending |
| Ignored-test/exclusion audit | Luna task b9ca69a1-1c80-4554-8265-b6a001a4008d | Running | Pending |
| Lifecycle/persistence source audit closure | Luna task 6cda5c04-8eb9-4cef-a4af-c97d03e0d308 | Running | Pending |

Run remaining mixed lanes serially, using the exact command/artifact map in
matching-acceptance-evidence-readiness-20261027.md with FINAL_REF set to v5 tag.
For each mixed row retain source pin, binary SHA256, build/gate/runtime/probe
exits, Go Frontend/History/Worker plus Rust Matching and no Go Matching,
completed applicable SDK/scenario results, actual restart/drain/crash/idle
recovery as applicable, backend/transport/discovery proof and cleanup.
Preserve historical acceptanceEligible:false and preliminary labels; final
review is a separate claim requiring all actual evidence above.

If a runtime error occurs, retain its artifact before diagnosis and compare the
actual Go contract. V4 late-name failure is documented as the cache refresh
minimum, not a Rust source defect; its failed artifacts stay separate.
