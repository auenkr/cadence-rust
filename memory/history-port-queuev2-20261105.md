# History port: queuev2 (branch history-port-queuev2)

Rust: rust/crates/cadence-history/src/queuev2/. Unported Go deps are stubbed in deps.rs
(persistence, clock, log, metrics, shard.Context, task, queue v1); factories reach
executors/queue v1 via `QueueFactoryDeps`.

| Go file | Rust file | Status |
|---|---|---|
| alert.go | alert.rs | ported |
| range.go | range.rs | ported |
| predicate.go, predicate_operation.go | predicate.rs, predicate_operation.rs | ported |
| queue_state.go, convert.go | queue_state.rs, convert.rs | ported |
| pending_task_tracker.go | pending_task_tracker.rs | ported |
| interface.go | interface.rs | ported |
| monitor.go | monitor.rs | ported |
| pause_controller.go | pause_controller.rs | ported |
| queue_reader.go | queue_reader.rs | ported |
| virtual_slice.go | virtual_slice.rs | ported |
| virtual_queue.go | virtual_queue.rs | ported |
| virtual_queue_manager.go | virtual_queue_manager.rs | ported |
| mitigator.go | mitigator.rs | ported |
| queue_base.go | queue_base.rs | ported |
| queue_immediate.go | queue_immediate.rs | ported |
| queue_scheduled.go | queue_scheduled.rs | ported |
| queue_cached.go | queue_cached.rs | ported (update-fn wrapping -> `set_read_level_listener`) |
| queue_mem.go | queue_mem.rs | ported |
| queue_reader_cached_{base,immediate,scheduled}.go | queue_reader_cached_*.rs | ported |
| transfer_queue_factory.go, timer_queue_factory.go | *_queue_factory.rs | ported (deps via trait) |

Notes: mocks/tests (`*_mock.go`, `*_test.go`) not ported. Mitigator picks domains by max-count scan instead of a heap.

Last green CI run: none yet (not compiled).
