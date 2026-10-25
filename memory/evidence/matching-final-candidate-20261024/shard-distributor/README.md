# Real shard-distributor candidate lane

Run https://github.com/auenkr/cadence-rust/actions/runs/40156296140 succeeded on `e06fd8ca63147d7eb5505336c3e2f2ba44a6adce` (same production Rust tree as27d3ff5f). Binary SHA256: `0bc0b2b89776c129dd67502ddeb6de75aff546de3b3d79e73f6d13eec52e5f29`. All build/gate/lane/run exits0.

The lane runs original Go sharddistributorfx through go/tests/shard-distributor-real with a single-node etcd container, onboarding100%, Go Frontend/History/Worker and only Rust Matching. Results retain an older generic fixture label under shardDistributor.fixture; the real=true flag, shardDistributorFixture process command and etcd fields identify the actual mode.

Assertions observed two executor heartbeats, SIGKILL executor expiry by TTL after6118ms, replacement executor registration and ownership reassignment, graceful executor DRAINING state, and recovery through shard-distributor restarts during both crash and drain flows. Persisted workflows each completed exactly once. Full compact probe/results are preserved.

Non-Cassandra preliminary-passed/acceptanceEligible=false is the harness's existing label and does not negate these assertions or establish full Matching sign-off.
