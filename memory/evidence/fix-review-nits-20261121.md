# fix-review-nits (review nits 1,2,3,5; nit 4 handled elsewhere)
Gated SHA: bb20fb0983abaf2d8ef6327ba069f414d58662d0 (branch fix-review-nits)
- Nit 1: shutdown aborts ownership task (drops change channel), awaits pump_task (1m timeout, like Go shutdownWG) which joins acquire workers, then close_all (launcher.rs).
- Nit 2: pump ticker MissedTickBehavior::Delay.
- Nit 3: membership-change log now carries hosts added/removed/updated; removed duplicate AcquireShards counter/timer from on_membership_change (acquire_shards already emits AcquireShardsCounter/Latency(+Histogram), GetEngineForShardErrorCounter, MembershipChangedCounter existed).
  Added GetEngineForShardErrorCounter on acquire path already via engine_for_shard.
- Nit 5: RateLimitedVisibilityWriter sync no-op record_uninitialized/upsert no longer take tokens.
## CI
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/44842610775 success
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44843546773 success
- history-mixed-gate mysql: https://github.com/auenkr/cadence-rust/actions/runs/44842836026 success, failedSteps []
- history-restartgate mysql: https://github.com/auenkr/cadence-rust/actions/runs/44842845766 success, failedSteps [], rust history binary sha256 c6458152ec07f6fc05539fa97fc6f425865e53c57c2986b318f46118e79a376d  /tmp/cadence-history-restartgate-44842845766-1/release/cadence-history
