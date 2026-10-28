# Native Matching isolation runtime (2026-10-28)

Implemented in `rust/crates/cadence-matching/src/matching_runtime_native.rs`.
The native registry isolation provider now carries the Go `with_domain_state`
selection inputs through byte APIs: enabled and sticky rules, reload-on-enabled
change, UTF-8 configured group snapshot, per-task-list poller window and
isolation duration, byte-exact poller recency, domain and global drained group
state, parsed read-partition assignment, missing partition behavior, task age,
and Go leak causes. The selector uses `NativePollerManager` and
`NativePartitionCache`; invalid task group bytes never match a configured
replacement rune. `native_queue_registry` keeps its prior signature.

## Source correspondence

- Go selector and dynamic settings: `rust/crates/cadence-matching/src/grpc_matching.rs`
  `with_domain_state` around lines 970-1080. Its `TaskIsolation` policy is
  represented with raw `GoString` comparisons in `select_isolation_bytes`.
- Shared policy constants and behavior: `rust/crates/cadence-common/src/task_isolation.rs`
  (`select_with_cause`, 50ms minimum), and byte poller history:
  `rust/crates/cadence-common/src/poller_bytes.rs` (`has_group_after`).
- Partition name parsing and read assignment: `rust/crates/cadence-common/src/task_list.rs`
  (`TaskListNameBytes`) and `rust/crates/cadence-matching/src/matching_partition_native.rs`
  (`NativePartitionCache::get`).
- Domain/global drain sources: `rust/crates/cadence-persistence/src/domains.rs`
  (`domain_isolation_groups`, `global_isolation_groups`).

## Bounded coverage

Added `byte_isolation_selector_uses_live_poller_and_partition_fixtures` in
`matching_runtime_native.rs`. It constructs the configured group through the
dynamic-config parser, starts a real byte poller manager fixture, seeds the
native partition cache, and exercises successful selection, invalid-byte vs
replacement-rune distinction, drain, partition mismatch, sticky behavior, and
expiration. No local Cargo, rustfmt, Go, build, test, check, CI, commit, or push
was run per assignment; this work is unverified until the parent runs Actions.

## Source-proven remaining gap

The source `InMemoryDomainStore::domain_isolation_groups` has no transient
storage error path, so runtime tests here can cover a missing domain (mapped to
the Go policy's drain-read error) but cannot inject an independent backend
failure without a domain-store abstraction change. The selector intentionally
preserves the same error-to-leak behavior. Mixed Go/Rust SDK, restart, and
recovery validation remains owned by the parent gate.
