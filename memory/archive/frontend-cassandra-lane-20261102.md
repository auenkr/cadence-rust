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
https://github.com/auenkr/cadence-rust/actions/runs/41429904560. Gate event
head is `6ce90c7993a3554d4e32037662898580cb0b1bd8`; input `rust_ref` names
`frontend-cassandra-acceptance-20261102`. The checked-out Rust source SHA must
be read from the run artifact because the ref may resolve after this evidence
commit. The run was queued at handoff; binary SHA256 and step-level outcome
are pending. On resume,
run `gh run view 41429904560 --repo auenkr/cadence-rust` once. If complete,
download its `frontend-mixed-gate` artifact and record `out/commit.txt`,
`out/binary.sha256`, `out/gate.exit`, and `out/gate/run/steps.json` here. If it
is still running, leave it alone and retain this exact run/head as the next
status check. A failure must be recorded as an exposed blocker; do not call that
full Frontend parity.

## Runtime boundary repair — 2026-11-02

The failed mixed run `41429904560` completed and artifact inspection confirmed
the panic immediately after `Matching startup: configured stores opened`.
Tracing the configured Matching default-store path to
`CassandraBlockingSession::connect` identified the exact boundary: it creates
a dedicated Tokio multi-thread runtime and calls `block_on(config.connect())`
on Frontend's Tokio startup thread. This reproduces the same class of issue
fixed by `matching_postgres::outside_runtime` for synchronous PostgreSQL
operations. The Cassandra constructor now runs its private-runtime creation
and connection inside that established scoped-thread boundary; it retains the
same config and database connection behavior and returns connection errors
normally. No History/visibility Cassandra reader initializes a nested runtime;
they consume the shared `CassandraBlockingSession`.

Added no-database regression tests that invoke Cassandra session initialization
from Tokio current-thread and multi-thread runtimes against a refused local
endpoint. Both must return an error instead of panicking. The Cassandra mixed
lane runs with `RUST_BACKTRACE=1` for future runtime diagnostics. Validation
was dispatched against source SHA `57fa385c78df8a41f0a1ce5176365d5b59932317`:

- Package CI: https://github.com/auenkr/cadence-rust/actions/runs/41431869939
- Fresh-target Cassandra mixed lane: https://github.com/auenkr/cadence-rust/actions/runs/41431874094

Both were in progress at handoff. The corrected binary hash and gate steps are
pending the mixed-lane artifact. Record its `out/binary.sha256`,
`out/gate.exit`, and `out/gate/run/steps.json` here when available; do not
claim Cassandra acceptance until the SDK/restart gate passes.

## Acceptance sub-agent handoff — 2026-11-02

The lane commits were fast-forwarded into branch `t3/770d8955` in the assigned
isolated worktree; no root checkout was used. Source review confirms the Go
configuration is generated from `go/config/development.yaml`, parameterized
for Cassandra, and passed with `--auto-setup --services history,worker` and
`--zone cassandra`. Go therefore creates Cadence tables/types from its existing
schema definitions. Rust Frontend and Rust Matching use the generated Rust
config; the runner does not start Go Frontend or Matching. The Cassandra
container has CPU, memory, swap, PID and Cassandra heap limits, and its exit
trap saves logs and removes only its uniquely named container. The workflow
uses standard `ubuntu-latest`, a 90-minute timeout and three-day artifact
retention.

One Actions inspection of run `41429904560` showed it in progress: the fresh
target Rust server build was running; the Go build, mixed SDK/restart gate and
artifact upload had not started. No source/runtime failure was exposed, so no
harness implementation change was justified. The prior thread link supplied
at handoff returned `thread_not_found`; this lane note is the available
committed handoff. Do not duplicate-dispatch. The pending owner must inspect
this same run once complete, then record `out/commit.txt`,
`out/binary.sha256`, `out/gate.exit`, and `out/gate/run/steps.json`, including
the resolved Rust SHA and any failure. Frontend remains partial until the mixed
run finishes; this bounded lane does not establish full service parity.
