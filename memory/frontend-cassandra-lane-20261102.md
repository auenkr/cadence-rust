# Frontend Cassandra mixed lane — 2026-11-02

## Scope

Bounded mixed acceptance: Rust Frontend + Rust Matching, Go History/Worker, Go
Cadence Cassandra schema, and no Go Frontend/Matching. The existing Cassandra
mixed workflow exercised Rust Matching with Go Frontend/History/Worker and did
not cover Rust Frontend's production persistence wiring.

## Implementation checkpoint

- Added `cassandra` as a selectable backend to
  `.github/workflows/frontend-mixed-gate.yml`.
- Added `tests/gates/run-frontend-cassandra-lane.sh`, which owns one uniquely
  named Cassandra 4.1.7 container with the established resource limits, creates
  the two keyspaces, then runs the shared Frontend SDK/restart harness. Go
  `--auto-setup` creates the Cadence tables/types from Go's schema.
- Extended `tests/gates/run-frontend-gate.py` to write Cassandra configs for Go
  and Rust while retaining the same SDK, TChannel/gRPC, IPv6, and Frontend
  restart assertions. The existing harness starts only Go History/Worker.
- Documented the lane in `tests/gates/README.md`.

Static checks: `git diff --check`, `bash -n` on the wrapper, and Python AST
parsing passed locally. No Cargo, rustfmt, Go build/test, or runtime gate was
run locally.

## Validation state

Actions inspection before dispatch: no active mixed gate. The requested parent
clean workspace run `41429248975` was active and is unrelated. Existing
PostgreSQL evidence is recorded in `memory/frontend-master-sync-20261102.md`
and `memory/frontend-postgres-visibility-20261102.md`; existing Cassandra
Matching evidence and the missing Frontend lane are in
`memory/frontend-cassandra-history-20261102.md`.

Implementation commit: `6ce90c7993a3554d4e32037662898580cb0b1bd8`, branch
`frontend-cassandra-acceptance-20261102` (pushed).

Dispatched after the pre-dispatch inspection found no active mixed gate:
https://github.com/auenkr/cadence-rust/actions/runs/41429904560. Gate head and
`rust_ref` are both `6ce90c7993a3554d4e32037662898580cb0b1bd8`. The run was
queued at handoff; binary SHA256 and step-level outcome are pending. On resume,
run `gh run view 41429904560 --repo auenkr/cadence-rust` once. If complete,
download its `frontend-mixed-gate` artifact and record `out/commit.txt`,
`out/binary.sha256`, `out/gate.exit`, and `out/gate/run/steps.json` here. If it
is still running, leave it alone and retain this exact run/head as the next
status check. A failure must be recorded as an exposed blocker; do not call that
full Frontend parity.
