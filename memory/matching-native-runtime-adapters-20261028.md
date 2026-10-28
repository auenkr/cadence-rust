# Native Matching runtime adapters — 2026-10-28

## Scope

Only `rust/crates/cadence-matching/src/matching_runtime_native.rs` was edited
for this task. The file now imports `MatchingDomainMetadata` from
`cadence_persistence::matching_domain_metadata`, returns that public type from
the raw-id lookup, and replaces the generic `From<i64>` config provider with
concrete integer, duration, and boolean providers.

`native_queue_registry` installs dynamic providers for read partition count,
partition-cache enablement, local task and poll wait, forwarding degree/polls/
tasks/rate, QPS interval, minimum burst, update ack interval, idle queue check,
delete batch size, writer batch/timeout/outstanding capacity, and reader batch,
read range, dispatch timeout, and ack interval. The builder source in
`grpc_matching.rs` establishes the keys and defaults. Duration values are
nanoseconds. `matching.readRangeSize` is a global unfiltered integer.
Task-list scoped native properties use the existing `*_bytes` accessors and
preserve raw task-list bytes. Forwarding degree retains Go's minimum of one.

Domain activity delegates to `InMemoryDomainStore::is_active_in`, which
implements local-domain activity, global failover-in-progress inactivity, and
active-active attribute ownership. The native hook preserves its established
unknown-domain behavior (`true`), including invalid UTF-8 IDs. The standby
resolver takes `active_active` from the metadata's optional active-clusters
field and resolves its active bit through the same legacy activity method.
Fatal writer errors retain the actual `SqlRunIdBytesError` cause.

Three bounded unit tests cover raw task-list filter distinction (`0xff` versus
U+FFFD), unknown and invalid domain activity defaults, and fatal writer cause
plus reader text retention.

## Validation status

`git diff --check` passed. Cargo, rustfmt, builds, and tests were not run, per
the task instruction and repository rule that all Cargo validation runs on CI.
The new tests and code remain **unverified by compilation/execution**. The
module remains intentionally unregistered; parent owns registration, CI, and
production listener changes.
