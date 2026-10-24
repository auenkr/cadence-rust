# Exact-source Unleash polling validation

Run: https://github.com/auenkr/cadence-rust/actions/runs/40083216641
Source: 49fa0bf81210c3930fb00e9274149268ea884f2a
Conclusion: success. Fresh uncached /home/runner/work/_temp/cargo-target-final.
Strict service-core clippy and package tests both exited 0. 143 unit tests and
27 integration tests passed; one unrelated integration test remains ignored.
The workspace formatting check is advisory and does not mutate tested source.

Covered polling bootstrap/refresh, Go route/query/header behavior, failure cache
retention, Tokio construction/shutdown and stalled-request shutdown. This proves
the merged repository polling slice only. Unleash flag evaluation and Matching
live reader parity are still separate unfinished work. This is not a workspace
or mixed-cluster completion gate. No local Cargo command ran.
