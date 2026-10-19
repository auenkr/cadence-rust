# Matching Ringpop provider and lifecycle validation

This is component-level evidence only. It validates the Rust provider and
lifecycle against focused Rust tests and Cadence's pinned Go Ringpop provider.
It does not complete Matching service migration.

The accepted Rust core source list is
`attempt-20261019T210514Z/rust-core-source.sha256`. The exact same hashes are
recorded at
`attempt-20261019T211342Z/rust-core-source.sha256`; `diff -u` returned zero.
The first file is the Rust source revision used by the passing focused tests and
strict Clippy run. The later run adds the Go-controlled stdin phases to the
probe example; all-target strict Clippy was rerun for that example. The Go test
source was then changed only to log its successful observed phase/incarnation
and endpoint evidence, and the pinned-Go peer runner passed against that final
test source. Final commit is recorded in
`attempt-20261019T211342Z/source-head.txt`.

The Rust focused test gates ran serially with `CARGO_BUILD_JOBS=1`,
`--locked`, and shared target
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-production-domain-cache/target`.
Exact command invocations are in `attempt-20261019T210514Z/run-gates.sh`, with
full logs and timestamps in `attempt-20261019T210514Z/rust-core-gates/`:

- Go signed FarmHash membership vectors: 2 passed.
- Ringpop member filter (matching `ringpop_member` and `ringpop_memberlist` modules): 13 passed, including 4 member primitive tests.
- Ringpop memberlist mutation/update behavior: 9 passed.
- Pinned-Go memberlist update oracle: 1 passed.
- Matching membership lifecycle: 11 passed, including caller-cancellation
  regression tests for withdrawal and teardown.
- Concrete Matching Ringpop provider: 4 passed, including two Rust nodes
  bootstrapping through the shared TChannel listener, discovering each other's
  service roles, self-eviction, and same-address restart/recovery.
- Ringpop provider suite: 41 passed.
- Strict all-target Clippy for `cadence-common` and `cadence-server`: passed.

For the phase-controlled pinned-Go peer run, execute
`CARGO_TARGET_DIR=/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-production-domain-cache/target CARGO_BUILD_JOBS=1 python3 tests/run-matching-ringpop-go-peer.py`.
The final Go test output, Cargo build log, overlay, runner status, and hashes are
copied under `attempt-20261019T211342Z/mixed-runner-raw/`. The test confirmed
initial Rust incarnation `1792444429800` at `127.0.0.1:37789`, the Go-observed
self-eviction, then restarted incarnation `1792444429928` with the same
TChannel endpoint and gRPC endpoint `127.0.0.1:7234`. Rust remained running
until Go validated that higher incarnation and both endpoints. The Rust probe
reported `rust-peer-ready 2` and `rust-peer-recovered 2`.

Earlier failed attempts remain in timestamped folders with diagnostic notes;
they are not counted as passing evidence. Their failures led to the invalid
port fixture correction, malformed-address Go mapping correction, self-member
expectation correction, strict-Clippy cleanup, and synchronized Go/Rust restart
phases.

Production `main` still does not wire `MatchingRingpopProvider` and
`MatchingMembershipLifecycle` together. This does not validate the full mixed
Go Frontend/History/Worker topology, existing SDK workflows, production service
failure/restart/recovery, or the Matching parity audit. Matching remains
incomplete.
