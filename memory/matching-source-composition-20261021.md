# Matching source composition (2026-10-21)

Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/t3-db1d9740`, branch
`t3/matching-source-candidate`.

## Composed source heads

- Telemetry/MySQL: `864dcdbe9` (`matching-telemetry-mysql-candidate`), included
  through the source candidate merge.
- PostgreSQL: original candidate `28c062496`, TLS callback/lock checkpoint
  `eeaaf4bb0`, lib/pq behavior correction `58d1d1e32`, and inline TLS/cache
  source `a8db27ab1` (integrated as `e219c4730`).
- Cassandra/semaphore/QPS: `58fb1a17b`, with TTL parity correction
  `4328b58d4`. Recovery harness commits `219758e37` and `21a7095dd` are not yet
  integrated; the active combined runner requires semantic reconciliation.
- Runtime ownership/logger: `c253324be` with logger corrections through
  `806067b117273cad6bcc3c49d22aa679a6ce1ee9`.

The PG `sslinline=true` inline PEM connector is incorporated from `a8db27ab1`.
PG owns the only heavy lane. This source composition did not run Cargo, Go,
containers, databases, or services.

Candidate history after the composed checkpoint: `6d0f70351` is the
two-parent telemetry/runtime merge (runtime input `806067b1`); `2e4696fa4`
fixes listener lifetime through drain; `35464aa49` wires quota rejections into
the configured metrics sink; `799e4770a` incorporates the committed lib/pq
mode correction; `14fd35c14` removes an unnecessary mutable binding and
clarifies the listener shutdown comment. The current branch head is
`0c9fc2d7d55a6cb3ead478f899204b9c3f9e945a`.

## Integration decisions

- Kept the telemetry candidate's inbound metric wrappers and queue/history
  metric wiring. Runtime quota checks now execute inside those wrappers, so
  rejected RPCs remain visible to the request metrics path. `Matching::with_metrics`
  also connects quota rejection events to the same configured metric sink via
  `matching_rpc_quota_rejections`.
- Retained the runtime candidate's quota policy, throttled logger, dynamic
  refresh behavior, poll validation order, and native gRPC metadata bridge.
  Poll quota admission runs before missing/unparseable timeout rejection. The
  existing Thrift `request.set_timeout` bridge remains in place.
- Kept the domain cache's failover notification version tracking and
  registry-based poll release, while retaining the telemetry domain-name
  resolver and trait-object `MatchingTaskStore` constructor.
- Kept the compatibility runner's production database observations and the
  runtime branch's bounded cleanup for primary and secondary Matching
  processes. Primary production SIGTERM evidence is still written before
  shutdown and completed after a clean exit.
- Included Cassandra's Go-adapter parity fix from
  `matching-cassandra-ttl-expiry-audit-20261021.md`: decoded Cassandra task
  expiry remains zero and Cassandra TTL continues to delete expired rows.
- The PostgreSQL `now_nanos` propagation from the composition is preserved.
  The lib/pq mode correction and inline PEM support are incorporated through
  `e219c4730`.

## Production assembly review and remaining gaps

The combined `main.rs` has one configured Matching runtime/store/metrics
assembly, carries the shared quota source, attaches the outbound History client
to the same metrics sink, uses configured discovery, retains listeners during
drain, stops the runtime before exporter flush, and unwinds startup ownership
failures. A follow-up to the runtime checkpoint replaced `try_join!` listener
supervision with independently owned listener tasks: the first listener exit
triggers membership withdrawal and the configured runtime drain while the
other listeners remain alive; all listener tasks are then stopped and joined
before metrics flush. This mirrors the Go shutdown ordering in
`service.go:143`. These are source observations only; lifecycle behavior still
needs the serialized production gate.

The runtime composition checkpoint counts rejected RPCs through the inbound
request wrapper, and the metrics follow-up commit wires rejection details to
the configured sink as `matching_rpc_quota_rejections`, tagging the exact RPC,
`user`/`worker` class, and domain where supplied. PostgreSQL lib/pq behavior
correction `58d1d1e32` is incorporated in `799e4770a`; its owner is still
working on inline PEM (`sslinline=true`) handling and paired compile/oracle
fixes. A Cassandra recovery gate review found that current held-semaphore
recovery evidence does not correlate the held owner with the Rust process that
is killed. Its owner is correcting that scope in the Cassandra worktree; leave
that harness region untouched until the new commit is available. The
Persistence QPS rejections now reach the configured Matching sink through the
shared persistence limiter. Each rejected task/domain/semaphore backend call
increments `matching_persistence_qps_rejections` once, tagged with operation
and `persistence_scope` (`go` or `semaphore`). Task/domain calls also emit the
Go metered-wrapper counters `persistence_requests`, `persistence_errors_busy`,
and `persistence_failures`, or their `_per_domain` variants when a domain name
is available. Go operation labels and `task_category=none` are preserved;
domain values use Go's alphanumeric/underscore sanitizer. Go has no metrics
scopes for the current Rust semaphore persistence API, so semaphore rejections
use only the dedicated counter and are not mislabeled as Go persistence
operations. Metrics do not consume another limiter permit. A source regression
checks both counter families and confirms semaphore events do not invent Go
scopes. This work is source-only; the PG heavy lane remains active.

The root worktree's uncommitted `lib`/registry/runtime/user-owned shard
processor overlay was not copied, replaced, or staged. Careful integration of
that overlay remains a separate required follow-up. The read-only overlay
audit confirmed hashes are unchanged: root `shard_processor` still uses
`Instant` and has no injected-clock tests, while candidate `6f68fb2ca` already
has the injected monotonic clock and two tests; preserve the candidate version.
Root `registry` QPS-by-exact-task-list-name and stop-by-name functionality is
already represented in this candidate, so later integration should retain the
root's exact-name explanatory comments without duplicating methods. The
candidate cache constructors each connect once; when the root duplicate
`with_domain_cache` calls are integrated, preserve the intent with one call.
The candidate export also carries the lint attribute. Do not wholesale-copy
root files over the candidate.

## Source checks

- `node --check tests/matching-service-compat.cjs` passed.
- `git diff --check` passed.
- Targeted `rustfmt --check` parsed the selected Rust sources but reported
  formatting differences across existing large files. No recursive formatter
  pass was applied, to avoid unrelated source churn.
- No build or tests were run. Matching is not complete; the parity audit,
  remaining failure/recovery behavior, PG inline PEM follow-up, and heavy-lane
  gates are outstanding.
