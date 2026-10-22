# matching-integration-20261022 evidence
Logs + `.exit` files (actual exit codes) from cargo runs on t3/matching-integration-20261021.
Tests run with CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0: parallel LTO links of the
cadence-server test binaries exhausted 12 GB RAM (memory PSI ~98%, see test1-aborted-memory-thrash.log).
Fixes: stale test long-poll deadlines (timed_poll), partition-update caller_deadline dropped by merge,
rustls provider ambiguity (ring + aws-lc-rs), metric/limiter test expectations. test8 = green, clippy4 = green.
