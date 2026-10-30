## Fresh workspace result

https://github.com/auenkr/cadence-rust/actions/runs/41107169881 completed successfully at exact source `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`.

Downloaded artifacts: `/mnt/storage/service/ci-evidence/matching-workspace-41107169881.eL8mkx/`. The rust-ci-logs artifact records the exact commit, target `/home/runner/work/_temp/cargo-target-final`, clippy.exit=0 and test.exit=0. Workflow source selects --workspace when package is empty; the test log includes workspace crate and doc-test results. Required matching-runtime-format.exit and fmt.exit are both 0. Informational workspace-format.exit, tls-format.exit and query-format.exit are 1; those workflow steps deliberately exit 0. This is not a claim of globally clean formatting.

Both `dropping_sink_drains_pending_timer_and_aggregate` and `sender_drops_failed_packet_and_continues_in_fifo_order` passed in the workspace test log. Go raw-string and DescribeTaskList oracle artifacts also record the exact source and exit 0. No local executable validation was run.

## First mixed gate

Created and pushed immutable source tag `validation/matching-statsd-2c3bc4a9` pointing to the full SHA above. With no other Actions run in progress, dispatched matching-production-validation.yml, lane=postgres:
https://github.com/auenkr/cadence-rust/actions/runs/41108824183

The run is confirmed queued at the exact source. Its outcome is not known. Inspect binary SHA256, nested build/probe/run/lane/gate exits, SDK/recovery/history/queue-drain evidence and cleanup before crediting acceptance. Subsequent mixed lanes must remain serial and use the same validated source. Matching remains incomplete; no History migration is authorized yet.
