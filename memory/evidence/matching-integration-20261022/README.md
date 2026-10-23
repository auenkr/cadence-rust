# matching-integration-20261022 evidence
Logs + `.exit` files (actual exit codes) from cargo runs on t3/matching-integration-20261021.
Tests run with CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0: parallel LTO links of the
cadence-server test binaries exhausted 12 GB RAM (memory PSI ~98%, see test1-aborted-memory-thrash.log).
Fixes: stale test long-poll deadlines (timed_poll), partition-update caller_deadline dropped by merge,
rustls provider ambiguity (ring + aws-lc-rs), metric/limiter test expectations. test8 = green, clippy4 = green.

Merges: dynconfig-ratelimit merge -> clippy6.exit=0, test10.exit=0 (commit 1745afceb).
metrics-gaps merge -> clippy8.exit=0, test11.exit=0. (clippy7 failed on useless_conversion, fixed.)
Note: dynconfig's HostLimits handler wiring dropped in favour of matching_quota; matching_host_limiter module kept (pub, unwired).
