# Final candidate configstore lane

Run https://github.com/auenkr/cadence-rust/actions/runs/40155523508 passed source `27d3ff5f60f0604553824420da0cf61026eda468`. Fresh Rust binary SHA256: `b224e074588acaf97f65384efd683c6cf6eb4c87ad7be0272de9b30dd8fd5b35`. Build, gate, lane and run exits are0.

The topology has Go Frontend/History/Worker and Rust Matching, with no Go Matching. Configstore seeded long-poll expiry2000ms; the owner-routed empty poll completed in2008ms. SDK/query/isolation, persisted-task crash/restart and graceful drain/recovery checks also passed; each recovery workflow recorded exactly one completion. Original probe and results are preserved.

The shared harness calls non-Cassandra success `preliminary-passed` and leaves Cassandra-specific `acceptanceEligible=false`. This evidence proves this lane's assertions, not full service completion or Cassandra acceptance. No output was relabeled.
