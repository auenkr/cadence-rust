# Matching engine API parity: P1-7, P1-8, P1-10 (2026-10-23)

Base: origin/master 92668eb2f. Audit source read directly from
origin/matching-parity-audit-v4-20261023:memory/matching-go-parity-audit-v4-20261023.md.
Worktree: /mnt/storage/service/github/wt/engineapis; branch matching-engine-apis-20261023.
Go IDL submodule initialized at bfbf273b41b9e36ce21e47a49600d22d73e4614c.
The audit's tasklist/poller path is stale: actual Go file is service/matching/poller/manager.go.

## P1-7: poller history, counts and DescribeTaskList

- Go poller/manager.go:111-171 StartPoll/EndPoll refresh an identity-keyed history
  entry, retain the last rate/group, and record outstanding independently of history.
  Empty identities are excluded from history; empty poll IDs create non-outstanding
  entries. Rust cadence-common/src/poller.rs:33-91 matches these transitions.
- Go manager.go:196-206 GetCountByIsolationGroup calls forEachPoller (:225-240):
  skip empty isolation groups; count unique historical identities, not outstanding
  RPC IDs; include an entry when outstanding OR lastAccessTime strictly exceeds
  the supplied threshold. Rust poller.rs:109-141 recent/count_by_group matches.
  No separate per-group ListInfo exists in this Go revision. ListInfo is flat.
- Go manager.go:208-223 ListInfo includes identity, cache creation/update Unix nanos,
  and rate; it deliberately omits isolation group. Rust poller.rs:119-128 matches.
  Go common/cache/lru.go:95-145 traverses MRU to LRU and removes expired entries
  before the after/outstanding filter. Rust recent follows its VecDeque in the
  same order; TTL is five minutes with equality retained. Long-outstanding entries
  are still evicted by TTL; anonymous outstanding RPCs are never listed/counted.
- Go tasklist/task_list_manager.go:767-812 always describes manager name/kind,
  pollers and partition config. includeTaskListStatus=false omits the entire status.
  When true, every configured AllIsolationGroups key appears, including zero counts
  and zero QPS; an unconfigured group contributes pollers but no metrics-map key.
  Isolation being disabled does not suppress that map. Counts use the live poller
  window; QPS uses GroupQPS. Rust grpc_matching.rs:3320-3455 matches this logic,
  cadence-persistence/src/matching_queue.rs:678-683 supplies read/ack/backlog,
  range-ID block, and empty=(ack == maxReadLevel). Limiter/QPS values are supplied
  by the corresponding live queue trackers, as in Go.
- Fixed durable DescribeTaskList response kind: Rust previously echoed request
  kind (including unspecified 0 or a kind differing from the cached manager).
  It now returns the loaded queue's kind (+1 for protobuf), like Go's manager.
- Errors: Go handler/engine.go:1130-1155 constructs the identifier, obtains/starts
  the manager and propagates failures. Rust uses the existing durable_queue path;
  persistence/ownership admission remains the existing queue path. Fixed identifier
  errors to InternalServiceError (Go handler/context.go:83-138 wraps the raw fmt
  error), and moved identifier parsing before domain-cache lookup to match Go's
  error precedence. Omitted request/taskList fields use Go getter defaults instead
  of a Rust-only BadRequest or panic. A dedicated test pins both cases.
- Thrift: thrift_matching.rs:118-164,480-502 converts the same gRPC-path result.
  Go common/types/mapper/thrift/shared.go:1932-1954 and idls/thrift/shared.thrift:1813
  expose pollers/status/taskList ONLY; partition config is intentionally absent
  from this pinned Thrift schema, whereas gRPC exposes it. Poller order, nanosecond
  timestamps/rates and isolation status map are preserved in Thrift conversion.

## P1-8: ListTaskListPartitions

- Go handler/engine.go:1158-1200 computes activity first, then decision, and returns
  both arrays. getAllPartitions (:1321-1347) resolves domain name to ID, parses the
  requested identifier, extracts root, ALWAYS includes root, then ascending 1..n-1
  write partitions. Read count, adaptive config, loaded managers and ring ordering
  do not select or reorder these keys. Child requests enumerate the root's list.
  n <= 1 still returns root. Empty non-reserved names are accepted by Identifier
  (tasklist/identifier.go:65-76,137-153); malformed reserved names fail.
- Rust grpc_matching.rs:972-1004,2802-2852 enumerates that same order with separate
  activity/decision write-count properties (domain/root/type filters already wired
  at construction). Removed Rust-only empty-name rejection for this API.
- Go getHostInfo (:1313-1319) returns HostInfo.GetAddress (TChannel address), not
  identity, hostname or gRPC endpoint. listTaskListPartitions (:1192) explicitly
  discards lookup errors: the key stays in its array with an EMPTY owner address.
  Fixed Rust to discard ring/spectator lookup failures per partition and return
  empty owner addresses, also when no resolver is configured. Removed HOSTNAME
  fallback. Successful owner resolution still uses the shared routing adapter,
  including shard-distributor selection; it does not contact the owner's service.
- Domain-cache failures precede identifier errors, and both fail the RPC; owner
  lookup failure does not fail it. Malformed identifier errors now use Internal
  rather than InvalidArgument, matching Go handler/context.go:133-138. Thrift handler :536-568 preserves activity and
  decision array order and each key/address (partition converter :166-168).

## P1-10: GetTaskListsByDomain

- Go handler/engine.go:1222-1237 resolves request domain NAME first, propagating
  cache errors, and defaults to Normal-only unless EnableReturnAllTaskListKinds.
  getTaskListsByDomainAndKind (:1202-1220) enumerates LOCAL loaded registry managers,
  filters kind, separates decision vs all other types, keys each result by ROOT,
  and calls DescribeTaskList(false). No persistence scan or remote RPC is made.
- Rust grpc_matching.rs:2748-2800 resolves domain via the same domain store and
  reads matching.matchingReturnAllTaskListKinds live; matching_registry.rs:653-709
  snapshots loaded owners, filters domain/kind, chooses the type map, root-keys
  entries and includes pollers, actual manager name/kind and cached partition config,
  omitting status. Durable rows alone do not cause an entry; unloaded managers
  disappear. Another domain's managers are excluded. Existing registry code matches.
- Go registry task_list_registry.go:119-128 iterates a map; root/child managers may
  overwrite the same root key in nondeterministic order. Rust HashMap iteration has
  the same unspecified winner. No invented sorting/merge policy was introduced.
- Fixed legacy no-registry fallback: frontend execution-store task lists were
  incorrectly enumerated and described WITH status. It now resolves the domain
  then returns empty maps because there are no loaded Matching managers.
- Thrift :503-535 delegates to this path, preserving maps and each description;
  map wire order is unspecified in both languages. Missing domains map to the
  existing EntityNotExistsError conversion where declared in the pinned schema.

## Tests and reproducibility

New tests in rust/crates/cadence-matching/src/matching_rpc_tests.rs (engine_apis_):
- poller_groups_and_list_info_match_go_boundaries: identity deduplication, last
  rate/group wins, anonymous/ungrouped exclusion from counts, strict time boundary,
  outstanding exception, MRU order, end timestamp refresh, TTL equality and expiry.
- partition_order_owners_and_lookup_errors_match_go: two-host owner addresses,
  exact ascending keys for child input in both types, empty ring, zero/negative
  count, empty name, domain failure precedence and malformed identifier error.
- durable_describe_and_domain_listing_share_go_manager_fields: loaded SQLite queue,
  actual Normal kind from unspecified request, ordered flat pollers/rates, configured
  group counts/zero group, status omission, exact gRPC description equality between
  the two APIs, partition config version 123, missing domain, unload exclusion,
  and native Thrift GetTaskListsByDomain field/order checks.
- partition_spectator_failure_is_empty_owner_like_go: failed real routing boundary
  yields successful partition lists with empty owners (no remote server required).

Existing complementary tests: matching_rpc_domain_listing_uses_loaded_matching_managers
(:2648), matching_domain_listing_filters_kinds_and_collapses_partition_roots (:3393),
matching_rpc_scopes_partition_counts_to_task_list_and_type (:908),
matching_rpc_durable_describe_tracks_activity_poller_and_queue_status (:746),
matching_rpc_decision_poll_preserves_isolation_group_history (:715).

Validation is unit/RPC-adapter evidence derived from the checked-in Go code, not
new mixed-cluster evidence or a claim of complete Matching service parity.
Cargo uses the shared target directory as explicitly requested for this task;
cargo-safe serializes through /tmp/cargo.lock and applies the capped work slice.

## Validation results

Source commit: 87860c557 (the later evidence commits change only memory files).
Run from /mnt/storage/service/github/wt/engineapis/rust in tmux cadence-rust:

```sh
CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared /mnt/storage/service/bin/cargo-safe test -p cadence-matching --lib engine_apis > /tmp/ea.log 2>&1; echo $? > /tmp/ea.exit
```

Exit **0**: 5 passed, 0 failed, 444 filtered out. See focused.log and focused.exit.
Compilation log names this worktree for every workspace dependency, confirming
cargo-safe rebuilt its sources after the shared cache changed workspaces.
Clippy and full crate tests remain queued; their results will be appended before
handoff. The lock wait included other worktrees' builds and a mixed-cluster gate;
this task never bypassed that lock or ran Cargo alongside the gate.
