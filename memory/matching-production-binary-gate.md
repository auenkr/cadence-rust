# Matching production binary gate — 2026-10-19

Source inspected: ac2c3c4fb (no production edits since ec059ca8e). Matching remains incomplete.

The existing mixed-service compatibility harness is valuable but starts the Cargo-reported ignored `matching_delivery_go_history` test executable (`tests/matching-service-compat.cjs`, startRust), not `cadence-server start`. The adapter injects independent History, domain metadata, lifecycle triggers and store dependencies. Its successful SDK/restart evidence does not prove production main wiring.

Current production main (`rust/crates/cadence-server/src/main.rs`, durable assembly around729-786 and listener join806-894) still:

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
