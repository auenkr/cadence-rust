# Matching production binary gate — 2026-10-19

Source inspected: ac2c3c4fb (no production edits since ec059ca8e). Matching remains incomplete.

The existing mixed-service compatibility harness is valuable but starts the Cargo-reported ignored `matching_delivery_go_history` test executable (`tests/matching-service-compat.cjs`, startRust), not `cadence-server start`. The adapter injects independent History, domain metadata, lifecycle triggers and store dependencies. Its successful SDK/restart evidence does not prove production main wiring.

Historical findings at ac2c3c4fb (see current status update below):

- selects durable SQLite only with `--data-dir`; otherwise uses embedded in-memory Matching;
- hardcodes outbound History to loopback plus configured History port, despite allowing only Matching role;
- spawns maintenance with a pending stop future and drops the JoinHandle;
- loads snapshot-only local domains and executions, not a shared durable domain cache;
- does not construct a Ringpop provider or attach Membership to Matching;
- exposes separate Matching TChannel without Ringpop handler; current public `serve_matching` and `serve_ringpop` cannot serve both on the same service channel (private serve_with already accepts both).

Production gate after bounded protocol slices:

1. Extract a Matching runtime owning registry, configured History transport, domain-cache dependency, membership/provider lifecycle and maintenance/shutdown handles. Preserve live settings already installed by `Matching::with_durable_queues`; do not create a bypass that silently drops them.
2. Wire this owner into actual `cadence-server` Matching-only mode. Use original Go-shaped config for production choices, not test-only injection flags as the final interface. Keep backend constraints explicit until production store parity is implemented; SQLite test backend is not all-backend parity.
3. Bootstrap/listen in Go order, with actual shared TChannel Ringpop+Matching listener, then labels/events, readiness and ownership refresh. Full gossip, state transitions, self-eviction and shutdown parity remain requirements.
4. Extend mixed harness to run Cargo-reported actual `cadence-server` binary, original Go Frontend/History/Worker with no Go Matching, and existing SDK calls. Do not remove adapter regression; label scopes clearly.
5. Verify queued activity/decision delivery, query/cancellation/retry/error details, graceful drain, SIGKILL/restart on same persistence and endpoint, backlog recovery, peer failover and membership changes. Record exact source, config, process roles, command, per-stage exits and artifacts. No completion claim from only successful RPCs.

Immediate independent follow-up once a worker slot is free: runtime owner/lifecycle construction against existing storage and transport boundaries, with main changes isolated from discovery/protocol modules. A source audit of actual Go shutdown/config sampling points is required before implementation. Domain persistence/cache and external store backends are separate substantial remaining slices, not silently replaced by a seeded snapshot in the production test.

## Runtime-owner review, 2026-10-19

The draft main signal branch awaited drain inside a `tokio::select!` arm while listener futures lived in the competing serving arm. Selecting the signal drops that arm, so it cannot establish that listeners continue accepting during drain. Parent requested pinned/owned serving lifetime polled through the drain, cleanup even on signal/drain errors, and an actual Go RPC during a nonzero drain. A port-open check or elapsed shutdown duration does not prove this behavior. This draft remains unaccepted until corrected and validated.

## Accepted component update, 2026-10-19

At root e1b78e5f1, production Matching runtime owns maintenance/drain shutdown
and configured SQLite defaultStore domain-cache startup/refresh/callback teardown.
Actual production binary Go native-Thrift probe passes ordinary/WAL failover
visibility, HTTP/TChannel/TLS compatibility and embedded data-dir boot gates.
The listener remains polled through drain in accepted runtime source. These
replace the historical maintenance/snapshot findings above; they do not
establish full mixed Go Frontend/History/Worker topology or SDK recovery.

Shared Matching+Ringpop TChannel API and owned SWIM runtime are accepted, but
main still does not instantiate/bootstrap the provider. Configured task writes
still depend on data-dir; discovered History placement is not yet injected.
Concrete provider lifecycle is accepted at b07398ac2 with pinned Go peer
self-eviction and same-address restart evidence. History placement/retry routing
is accepted at 219f86054, but remains uninjected in main. Configured SQLite task
factory is still a candidate: Rust gates pass, two-way Go queued-task evidence
remains unresolved. Healer and shard-distributor candidates are unvalidated.
Keep the production mixed gate
open until the actual binary passes the topology and failure cases above.
