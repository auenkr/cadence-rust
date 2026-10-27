# Matching fixed configuration and GC final review (2026-10-27)

Read-only source audit to close the three named rows in `matching-current-config-reconciliation-20261027.md`: fixed task range size, one-second task GC threshold, and startup `HostName`/`RPCConfig` injection. Traced production launcher/runtime paths, excluding test fixture constructors and test-only range sizes.

## Findings

No confirmed behavior mismatch in the scoped rows.

### Fixed `RangeSize = 100000`

Go `config.NewConfig` sets `RangeSize: 100000` (`go/service/matching/config/config.go:197`); `newTaskListConfig` passes that configured value to each task list (`go/service/matching/tasklist/task_list_manager.go:1030-1035`). Rust's production launcher passes `NonZeroU32::new(100000)` to durable Matching runtime construction for both configured persistent storage and embedded durable storage (`rust/crates/cadence-matching/src/launcher.rs:1303-1317, 1319-1351`). `QueueRegistry` stores that argument (`matching_registry.rs:177-180, 753-758`) and uses it when opening each durable queue (`matching_registry.rs:1133`). In-memory mode does not allocate durable task ID ranges. The `100`, `2`, and similar values passed to test constructors are test fixtures and do not change these production defaults.

### `MaxTimeBetweenTaskDeletes = 1s`, GC call cadence, and shutdown ownership

Go sets the fixed value to `time.Second` (`go/service/matching/config/config.go:248`) and passes it into each task-list config (`task_list_manager.go:1160-1163`). `task_gc.go:65-93` establishes the behavior: ordinary `Run` deletes when backlog reaches batch size, or when positive backlog has been waiting longer than the configured interval; `RunNow` ignores only the time condition. Go task reader calls ordinary `Run` after ack advancement (`task_reader.go:430-431`) and calls `RunNow` after persisting the final ack during `Stop` (`task_reader.go:169-178`).

Rust's queue GC hardcodes the same one-second interval and default batch size 100 in `run_garbage_collection` (`rust/crates/cadence-persistence/src/matching_queue.rs:604-626`). The shared collector uses the corresponding full-batch/positive-backlog/time-or-force predicate and records attempt time before deletion (`matching_gc.rs:25-70`); production ordinary calls pass `false` from reader completion paths (`matching_reader.rs:719, 1017`, plus dispatch completion paths) and shutdown passes `true` from `Owner::finish` (`matching_registry.rs:135-147`). `Owner::finish` persists final ack before forced GC, and the registry shutdown path finishes owners (`matching_registry.rs` shutdown implementation). This matches the Go reader's final-ack then `RunNow` ownership. The one-second interval is a fixed constant, so it needs no dynamic-config reader.

### Startup `HostName` and `RPCConfig`

Go obtains the machine name with `os.Hostname()` and injects it, alongside the selected service's `svcCfg.RPC`, into resource params (`go/cmd/server/cadence/server.go:128-145`); `matching/service.go:57-64` passes both into `config.NewConfig`, which stores them (`go/service/matching/config/config.go:188-189, 244-245`). Rust's production Matching launcher gets `cfg.services["matching"].rpc` (`rust/crates/cadence-matching/src/launcher.rs:979`), applies its `bindOnLocalHost`, `grpcPort`, and `port` with the same Matching-specific listener defaults 7835/7935 (`launcher.rs:980-1000`), and applies that RPC block's TLS configuration to server and peer connections (`launcher.rs:1006-1020`). Thus the RPC startup injection reaches actual listeners and transport TLS.

Rust has no Go-shaped Matching `Config` object for hostname injection. Runtime consumers call `matching_host_name()`, which caches the OS hostname via `gethostname` (`rust/crates/cadence-matching/src/grpc_matching.rs:30-43`); it is used for Matching host attribution (`grpc_matching.rs:2487, 2719`) and the persistence limiter (`launcher.rs:1053`). Go's `os.Hostname()` is the corresponding host identity source. No evidence in these paths establishes a behavioral difference, so this is recorded as equivalent startup wiring rather than a missing reader.

## Validation boundary

This is a static source trace only. No source files outside this review note were edited. No Cargo/Go command, formatter, tests, CI workflow, or mixed-cluster gate was run. The audit does not establish broader Matching parity or runtime interoperability; it closes only the named fixed-config/GC/startup-injection questions.
