# History port: replication/ and failover/ (branch history-port-repl)

Rust: `rust/crates/cadence-history/src/{replication,failover}/`. Collaborators owned by other ports are small traits in `replication/deps.rs`.

| Go file | Rust | Status |
|---|---|---|
| replication/task_reader.go | task_reader.rs | ported |
| replication/dynamic_task_batch_sizer.go | dynamic_task_batch_sizer.rs | ported |
| replication/task_store.go (BoundedAckCache) | task_store.rs | ported |
| replication/task_hydrator.go | task_hydrator.rs | ported |
| replication/task_ack_manager.go | task_ack_manager.rs | ported |
| replication/task_executor.go | task_executor.rs | ported |
| replication/task_fetcher.go | task_fetcher.rs | ported |
| replication/task_processor.go | task_processor.rs | ported |
| replication/dlq_handler.go | dlq_handler.rs | ported |
| replication/metrics_emitter.go | metrics_emitter.rs | ported |
| failover/coordinator.go, marker_notifier.go | failover/*.rs | ported |

## Known approximations / TODO-verify
- Byte size of replication tasks (`replication_task_byte_size`) approximated via size_of + string/blob lengths. TODO-verify vs Go `ByteSize`.
- Go panic-recover logging in handlers omitted (Rust panics propagate).
- Go hydrator double `release()` replaced by a single call.
- `ThrottleRetry` reimplemented as `throttle_retry` without throttle policy.
- `triggerDataInconsistencyScan` is unused in Go too; kept `#[allow(dead_code)]`.

## CI
Last green run: https://github.com/auenkr/cadence-rust/actions/runs/41900413463 (rust-ci.yml, package=cadence-history, clippy + tests, success)
Green head SHA: 95d3f56154dc0a371eec8953ce5363b43c316409
Notes: added `#![allow(clippy::result_large_err)]` in replication/mod.rs (ReplicationError::RetryTaskV2 is ~160 bytes); added `last_worker_identity` to `ActivityInfo` (needed by sync-activity hydration, Go has it).
