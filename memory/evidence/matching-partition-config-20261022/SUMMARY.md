# Matching partition-config finish (branch matching-partition-config-v2-20261022)

Env: `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared`, `flock /tmp/cargo.lock cargo ... -j2`, from `rust/`.

| Command | Exit | Log |
|---|---|---|
| `cargo check -p cadence-server --tests` (first, found 2 missing `base_name`) | 101 | check.log |
| `cargo check -p cadence-server --tests` (after fix) | 0 | check2.log |
| `cargo clippy -p cadence-server --all-targets -- -D warnings` (found pre-existing `drop_non_drop` in cadence-persistence) | 101 | clippy.log |
| `cargo clippy -p cadence-persistence -p cadence-server --all-targets -- -D warnings` | 0 | clippy3.log (clippy2.log: transient IDL-missing build-script failure, 101) |
| `cargo test -p cadence-server --lib -- matching_config matching_rpc_partition_config matching_rpc_manual_partition matching_reader scaled_partition` (21 passed) | 0 | test.log |

Findings:
- Rust already rejected non-Normal kinds in Update/Refresh (proto kind 0/1 = Go Normal; 2 sticky, 3 ephemeral rejected) in Go's order (engine.go:1257, 1292); added `matching_rpc_partition_config_rpcs_reject_non_normal_kinds`.
- Go never consumes `EnableSyncMatch`, `MaxTasklistIdleTime` or `ActivityTaskSyncMatchWaitTime` (only copied in task_list_manager.go:1040-1062; no caller of the fields anywhere in go/). Wiring consumers would be a Go logic change, so they stay config-parity readers; added `activity_task_sync_match_wait_time` (default 50ms).
- Fixed WIP compile errors (`base_name`) and a pre-existing clippy `drop_non_drop`.
