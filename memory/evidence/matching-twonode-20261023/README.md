# Two-node Matching (branch matching-twonode-20261023)
## Shutdown bug fixed
Go matchingEngineImpl.Stop / taskListManagerImpl.Stop never fail on lease loss (reader stop only logs final ack update errors).
Rust Owner::finish returned ConditionFailed ("range ID was 12 ... 11") from persist_final_ack as Unavailable -> process exit 1.
Fix: tolerate_lost_ownership in matching_registry.rs logs and returns Ok; other errors still propagate.
Test: matching_registry::tests::shutdown_tolerates_lost_task_list_ownership. Command: flock /tmp/cargo.lock cargo test -j3 -p cadence-server --lib shutdown_tolerates; unit-test.exit = 0.
## Gate (harness failures b/c, kill-one-node recovery): NOT RUN yet in this branch. No claim of pass.
