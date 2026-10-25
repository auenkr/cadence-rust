# Matching final candidate validation (in progress)

Pinned branch: matching-final-candidate-20261024
Source: 27d3ff5f60f0604553824420da0cf61026eda468

This is a validation tracker, not service sign-off. All implementation slices
and focused tests are integrated. No History migration begins before this
candidate's acceptance and parity audit complete.

| Required lane | Run | State when recorded |
| --- | --- | --- |
| Fresh uncached workspace clippy then tests, ownership/metric oracles | https://github.com/auenkr/cadence-rust/actions/runs/40108643448 | success |
| PostgreSQL SDK, two-node recovery and extended probes | https://github.com/auenkr/cadence-rust/actions/runs/40108645569 | success |
| Thrift/TChannel | https://github.com/auenkr/cadence-rust/actions/runs/40153127238 | success |
| Cassandra | https://github.com/auenkr/cadence-rust/actions/runs/40153493941 | running |
| MySQL extended probes | pending | not started |
| Configstore | pending | not started |
| Real shard distributor | pending | not started |
| Active offer with Go History, zero inserts | pending | not started |

Run mixed lanes serially. Preserve source, binary SHA256, command, exit files
and compact probe output for each. Earlier successes remain scenario evidence
only and cannot stand in for this integrated candidate. Workspace formatting
has known unrelated debt; advisory status must remain explicit, with no
source mutation before checks.

Already integrated behavior evidence includes the root-start remote refresh
production test, typed live OpenFeature reads, Unleash polling/evaluation and
lifecycle, actual Go/Rust metric sink comparison, configstore long-poll expiry,
SDK workflow/restart/drain/crash scenarios, active-offer zero-insert audit, and
file watcher reload/error-retention/shutdown tests. The requirement mapping is
memory/matching-completion-audit-20261024.md; update its stale status lines
with actual final results, not blanket claims inferred from green workspace CI.
