# Matching candidate source and acceptance review — 2026-10-30

Read-only review of candidate `f8739bfbfc9ce1921b3404dd9cf6fd6193cb9db5`
(`origin/t3/matching-parity-fix`). CI run
`41026357394` was reported in its Go-oracle phase; Clippy/tests were not yet
reached. This note does not claim a focused or workspace pass. No Cargo, Go,
formatter, runtime, CI dispatch, or mixed gate was run for this review.

## Source findings

### Semaphore registry

Candidate change `rust/crates/cadence-matching/src/matching_semaphore_manager_native.rs`
only renames the closure's captured weak map from `managers` to `registry`.
This resolves the local name collision with the `MutexGuard` used later and
does not alter runtime behavior.

The reviewed lifecycle is consistent with the Go ordering at
`go/service/matching/semaphore/semaphore_manager.go`: Rust sets `Starting`
under the manager index lock, releases it before persistence awaits, changes to
`Running` only after a successful scan, and stops/unregisters on scan errors.
`LoadInFlight::drop` also calls `stop()` if the startup future is cancelled;
`stop()` changes state under the index lock, releases that lock, invokes the
weak-registry callback, then wakes startup waiters. Registry `start()` releases
its map lock before awaiting manager startup. `sweep_idle()` snapshots first,
then unregisters and stops without holding the registry lock. Identity-checked
removal prevents an old manager's callback from evicting a replacement. These
lock/lifecycle paths show no deadlock or stale-removal defect by inspection.

One source-description correction: Rust constructs `Manager::new` before
acquiring the registry mutex; only lookup/insertion are serialized. Go calls
`create()` under its write lock. Concurrent Rust cold requests can therefore
construct losing, unstarted candidates, but only one candidate is inserted and
started. `Manager::new` currently validates and initializes in-memory state;
no externally visible effect from the discarded candidate was found. The
candidate repair note's statement that the guard spans manager construction is
inaccurate. No production defect is established. The Rust suite contains
`failed_start_releases_waiter_and_next_request_rebuilds_manager` and
`cancelled_conditional_write_restores_reservation_and_restart_scans_consistently`,
but I found no direct test aborting the startup-scan future; add/execute an
abort-during-scan assertion for stop, unregister, waiter release, and successful
replacement before crediting this cancellation path as tested.

### Ringpop logging

The bounded instrumentation is in the prior candidate integration commit
`9bbc0d2ef` (`ringpop_provider.rs`), not the semaphore-only final commit. It
leaves JOIN, membership updates, readiness, counters and the PING response
algorithm unchanged. It logs at most the first eight accepted-app PING request
paths per process, before response encoding, with source, accepted changes and
current members. This is diagnostic receipt evidence; it cannot alone prove
that Go received the encoded response or that its SWIM probe succeeded.

The cap bounds line count, not line size: `accepted_changes` and `members`
serialize all entries, and `request.source` is interpolated without debug
escaping. Large membership lists can produce a large synchronous stderr write;
control characters in the source string can make confusing log lines. These
affect diagnostics/output only, not membership or protocol decisions. Keep the
trace interpretation narrow; if production input sizes are not bounded, cap
the logged vectors/source in a follow-up rather than treating the current trace
as strictly byte-bounded.

## Required validation and priority

1. Let the already-running fresh workspace CI `41026357394` reach terminal
   state; parent owns scheduling and review. Require exact `headSha` equal to
   `f8739bfbfc9ce1921b3404dd9cf6fd6193cb9db5`, fresh uncached target, Clippy
   exit 0 followed by workspace test exit 0, and record run URL/SHA/conclusion.
   The currently reported Go-oracle phase is not a Clippy/test result.
2. On one frozen final SHA, run the ten native mixed lanes serially. For every
   run inspect artifact `commit.txt`, binary SHA256, build/gate/nested exits,
   actual result/probe logs, Go Frontend/History/Worker plus Rust Matching with
   no Go Matching, SDK outcomes, lane assertions, recovery, and cleanup. A
   green workflow or requested flags alone do not prove a lane:

   | # | Exact dispatch | Required assertions beyond common SDK/topology/restart/recovery/cleanup |
   |---|---|---|
   | 1 PostgreSQL | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=postgres` | Shared Go PostgreSQL schema; PG GC/domain, query standby, partitions, durable restart and auto-config hint; inspect actual PostgreSQL cleanup. |
   | 2 MySQL probes | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=mysql-probes` | Go MySQL schema; backlog forwarding, query standby, partitions, durable restart and auto-config hint; actual DB observations and cleanup. |
   | 3 Thrift | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=thrift` | Matching Thrift + History TChannel, SDK and transport/recovery evidence. |
   | 4 Configstore | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=configstore` | Seeded live configstore setting applied; SDK, idle/two-node recovery and teardown. |
   | 5 Real shard distributor | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=shard-distributor` | Real Go fx/leader loop with runner etcd; executor assignment/routing, heartbeat TTL expiry, cleanup, restart and crash recovery. Do not substitute in-memory `--shard-distributor`. |
   | 6 Two-node MySQL | `gh workflow run matching-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f extra_args='--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3 --matching-exporter prometheus --matching-cold-domain-metric-probe'` | Both Rust nodes, dual Prometheus/cold-domain quota assertions, SIGTERM drain, owner SIGKILL/takeover, persisted task/ownership assertions. |
   | 7 Cassandra | `gh workflow run matching-cassandra-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f extra_args='--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3'` | Shared Go Cassandra schema; task/semaphore owner+token interop, two-node takeover, restart/drain and cleanup. |
   | 8 Adaptive | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=adaptive` | Actual adaptive scaler/load/routing scenario plus unchanged SDK and recovery assertions. Do not use single-node diagnostic, Go control, or retry-only as acceptance. |
   | 9 Active offer | `gh workflow run matching-active-offer-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"` | Go History schedules real decision task; assert token delivery and zero persisted fallback rows/inserts, plus SDK/recovery. |
   | 10 Production TLS | `gh workflow run matching-production-tls-mixed.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"` | Verified live Rust gRPC listener identity (hostname, ALPN h2, CA/peer fingerprints and SAN), SDK and recovery. |

3. The supplemental same-pin oracles are separately required by the ledger:
   `gh workflow run matching-dependency-oracles.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"`.
   Inspect the exact Go History peer TChannel test, PostgreSQL TLS modes and
   native PostgreSQL raw-byte/fencing/cross-read test; require each exact
   ignored test to report one executed pass and verify cleanup. They supplement,
   not replace, the ten mixed lanes.
4. Complete the native source parity audit and ignored-test reconciliation
   against executed evidence before declaring Matching complete or moving to
   History. The current ledger still marks all ten native mixed lanes and the
   final parity/ignored-test audit open.

## Ignored-test and assertion gaps to keep visible

- `matching_engine_go_tests_b.rs:193`: injected Go query-dispatch error is still
  not asserted; timeout/no-poller tests are not equivalent.
- `matching_engine_go_tests_c.rs:156`: direct public refresh-RPC propagation of
  a storage/refresh error remains distinct from retry cancellation/update
  persistence tests.
- Failover helper selection/version tests exist, but production callback
  registration, catch-up and eligible blocked-poll release need direct evidence.
- `matching_partial_upgrade_a.rs:85`: Go's single table over 66 config fields
  has no native equivalent; reconcile each native runtime key/assertion.
- `matching_partial_upgrade_a.rs:92`: Go generic `handleErr` identity/wrapping
  cannot map one-for-one; the per-RPC typed error mapping matrix is not
  exhaustive yet.
- `matching_partial_upgrade_e.rs:624`: Go injected handler-error/default
  response matrix is not established solely by dispatch coverage or error
  classification. (Ledger notes a newer native dispatcher regression; require
  its exact assertion and CI execution before closing this row.)
- `tests/matching_delivery.rs::matching_delivery_go_history` remains ignored in
  the normal suite. The dependency-oracle workflow runs a separate real Go
  History TChannel wire test, while mixed SDK lanes must prove configured
  delivery paths; do not count either as execution of the ignored test itself.
- `tests/shard_distributor_go_interop.rs::rust_executor_and_spectator_interoperate_with_pinned_go_handlers`
  remains an ignored focused Go interop test; lane 5's real-distributor
  production scenario is stronger runtime evidence for its required behavior,
  but is not that exact test execution.

## Review outcome

No semaphore correctness defect was proved and no candidate source was edited.
Ringpop logging changes observability only; its bounded-count trace has the
limits above. The candidate is not accepted: CI and all mixed lanes remain
unverified/pending, with the ledger's no-go rule to keep Matching current.
