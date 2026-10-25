# Passing corrected MySQL extended lane

Run https://github.com/auenkr/cadence-rust/actions/runs/40155956238 succeeded on source `e06fd8ca63147d7eb5505336c3e2f2ba44a6adce`. Binary SHA256: `1bff6cf6afaf52de1ea0979ad30755635e2328f38e87c2230401a11f3b39b4f8`. Build/gate/lane/run exits0.

Rust production is unchanged from original27d3ff5f candidate; the whole Rust tree hash matches823ff09bee39660110e07c915e449ff84a33bbf0. The harness additionally waits for the restarted primary's own listener, correcting the readiness race in failed run40153901033 (preserved separately).

The run has Go Frontend/History/Worker and no Go Matching. Assertions passed for child-to-root and parent-to-child persisted backlog forwarding, standby queries, agreement of two direct nodes and Go Frontend on partition owners, owner-only Describe, three persisted activity partitions surviving restart and draining, decision/activity AutoConfigHint defaults, crash recovery and graceful drain (one completion event per workflow).

The shared harness labels non-Cassandra successes preliminary-passed; its Cassandra-specific acceptanceEligible remainsfalse. This report establishes the listed assertions only. Full Matching sign-off still requires the remaining lanes and final audit.
