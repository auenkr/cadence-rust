# History cached snapshot reader audit, 2026-11-25

Audited origin/master at `76bbc723ddb329c5b82798c2c3339669126e0f00`. The retained persistence snapshot has hot-path readers. Whole-snapshot lazy decoding was ruled out by the task's per-decision read criterion.

| Reader | Persisted fields read | When |
| --- | --- | --- |
| `engine_close.rs`, `EngineMutableState::close_transaction` | `start_version`, `last_write_version`, and the original `version_histories` blob when the working histories are absent | Every persisted mutation. Ordinary decision completion calls `update_as_active` in `engine_decision.rs`, which runs `persist` → `prepare_update` → `close_transaction` in `engine_cache.rs`. Continue-as-new closes both transactions. |
| `engine_checksum.rs`, `persisted_checksum` | `checksum` | Cache load, including reporting a mismatch. |
| `queue_deps_transfer_active.rs` and `queue_deps_timer_active.rs` | `start_version`; active transfer also reads `request_cancel_infos` and `signal_infos` | Queue task processing. |
| `queue_deps_standby.rs` | `request_cancel_infos`, `signal_infos` | Standby queue task processing. |
| `mutable_state_json.rs`, `persistence_json` | `checksum` | Mutable-state JSON rendering, including the administrative path. |
| `engine_cache.rs`, `clean_snapshot_for_ndc` | Entire original snapshot | NDC checkout of a clean cached workflow via `ndc_workflow.rs`. Finishing a transaction clears dirty state while preserving the original snapshot, so checkout remains possible after normal writes. |

The public Rust `EngineMutableState::snapshot()` also exposes a borrowed `&WorkflowSnapshot`. The in-tree callers are the active queue readers above. Keeping the whole decoded struct in an `Arc` would leave its allocations resident while cached. Dropping it on transaction completion would change NDC checkout. The SQL and Cassandra execution stores already expose assembled `WorkflowSnapshot` values through `GoExecutionStore`, with several fields already containing opaque blobs. Neither provides the engine a single raw execution payload that can stand in for that returned snapshot.

This branch records the audit only. No implementation candidate was produced, no CI or mixed gate was dispatched, and there are no new benchmark arms or throughput measurements. The c1 memory target remains unresolved.
