# Ringpop inbound JOIN/PING handler slice

Date: 2026-10-18. This slice adds stateful inbound `/protocol/join` and
`/protocol/ping` handlers over the existing Ringpop memberlist and
disseminator cores. The handler is exposed as `RingpopNode::handler()` for the
existing injectable TChannel JSON boundary. It is not wired into server main.

## Behavior included

- Explicit `Bootstrapping`, `Ready`, and `Stopped` lifecycle values. Nodes start
  in `Bootstrapping`; only the owner can set Ready. PING returns Ringpop's
  `node is not ready to handle requests` error before readiness.
- JOIN uses the pinned v0.10.0 self/app validation messages, returns all
  memberlist changes with local source identity and the cached memberlist
  checksum, and does not set readiness or merge the joining request.
- PING checks readiness and app name, applies every inbound change via the
  existing merge core, records accepted changes, returns receiver deltas or a
  full membership snapshot on checksum mismatch, and reports the local
  checksum/address/incarnation. Empty app is rejected when configured.
- Propagation bounds are recalculated only after accepted membership changes,
  using Go's count of remote Alive/Suspect members. A local refutation response
  reads the incremented incarnation after merge; the default clock uses
  Ringpop's millisecond units.
- Constructor semantics match Go: cached checksum starts at zero, and the
  initial local change is queued with empty source. Receiver deltas increment
  propagation counts; deterministic test entry accepts injected incarnation
  and insertion callbacks. Production defaults use wall-clock milliseconds and
  random insertion positions.
- Malformed JSON errors are transport/system errors. This matches the Go JSON
  client probe's malformed object handling instead of returning an application
  error.

## Differential fixture and reproducible commands

`tests/run-ringpop-inbound-handler-oracle.py` overlays a Go test into a copy of
the pinned `/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0` module.
That test calls the original unexported `handleJoin` and `handlePing`, not a
mirror implementation. It writes the normalized fixture to
`rust/crates/cadence-server/tests/fixtures/ringpop_inbound_go.json`. A Rust test
consumes that fixture and compares complete JOIN and PING JSON, plus the
original handler's self/app/readiness error strings.

Evidence from the current run:

- `python3 tests/run-ringpop-inbound-handler-oracle.py` — exit 0; original Go
  handler fixture generated (log: `/tmp/cadence-ringpop-go-handler-oracle.log`,
  exit file: `/tmp/cadence-ringpop-go-handler-oracle.exit`).
- First `cargo test -p cadence-server ringpop_provider --no-fail-fast` — exit 0,
  4 tests passed, but this compile began before the full Go fixture consumer
  and snapshot coverage were added. The final-source rerun at
  `/tmp/cadence-ringpop-provider-postlint.log` and
  `/tmp/cadence-ringpop-provider-postlint.exit` exited 0 with all 7 provider
  tests passing, including exact Go fixture comparison, member labels and
  tombstones, checksum-triggered full sync, local dissemination counters, and
  local refutation using an injected clock value.
- `cargo test -p cadence-common ringpop --no-fail-fast` — exit 0; 19 unit cases
  plus the pinned dissemination, member, and memberlist update oracle tests
  passed. Log: `/tmp/cadence-ringpop-common-test.log`.
- The Go Ringpop wire fixture oracle completed successfully (exit 0); its runner is
  `/tmp/cadence-ringpop-wire-oracle.log` with exit recorded in
  `/tmp/cadence-ringpop-wire-oracle.exit`.
- Final strict all-target Clippy for `cadence-common` and `cadence-server`
  (`cargo clippy -p cadence-common -p cadence-server --all-targets -- -D
  warnings`) exited 0 in 9.18s. Log:
  `/tmp/cadence-ringpop-clippy-final.log`; exit:
  `/tmp/cadence-ringpop-clippy-final.exit`.
- Initial mistaken invocation from repository root (`cargo test` where there
  is no root Cargo.toml) exited 101. The corrected command runs from `rust/`.

## Deliberate scope and outstanding gaps

No outbound bootstrap, probe, ping-req, timer, listener/main wiring, config,
discovery, Matching provider integration, or restart/recovery lifecycle is
included. `Stopped` is exposed for lifecycle owners; transport detachment is
still the owner's responsibility. The production handler has no shutdown
barrier around requests already in flight. Full Ringpop parity and Matching
completion remain unclaimed. Next work must complete discovery/bootstrap and
bidirectional mixed Go/Rust behavior before Matching can advertise this node
as ready in production.
