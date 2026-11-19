# Performance status — 2026-11-19

Covers merges 0a74e0940..56b58c6c1.

- Validation: master d1dba9370, 29/29 gates + replication + archival + soak PASS (master-validation-20261118.md).
- Flamegraph (perf-history-flamegraph.md): metrics registry hot path fixed; History latency gap is off-CPU.
- Matching LB parity (evidence/matching-lb-parity.md): Go partition LB ported, task-type filters, ReadOnlyPartition invalidation; forwarder audit clean.
- Partitions / ceiling (evidence/perf-ceiling-partitions.md): 4 partitions Rust 6/6 vs Go 3/6 at 30-35/s; ceiling 40/s both (MySQL bound).
- History dispatch (evidence/perf-history-dispatch.md): activity STS was ms timestamp truncation, fixed with ns clock (91b767573).
- Benchmark rerun (evidence/benchmark-rerun-20261119.md, master 9592b349a): Rust lower p50, CPU and RSS.
- History tail (evidence/perf-history-tail.md): block_in_place around sync SQL merged, no p99 gain.

## Remaining gaps
1. History p99 tail: ~195 ms Rust vs ~110 ms Go at 25/s.
2. FE+Matching paired c64 p99: Rust +798 ms.
3. Soak query "init" line (deferred until after History migration).
