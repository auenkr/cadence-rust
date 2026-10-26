# Matching final gate readiness review (2026-10-26)

Read-only audit of the final gate plan against the current workflows, harnesses,
acceptance matrix and active checkpoint. No CI was dispatched, no mixed lane was
run, and no completion claim is made. Current reviewed source is `8e302431c`.
The Rust acceptance candidate must remain unpinned until fatalfix
`e69c76633` passes its focused CI and is integrated by the parent.

## Exact acceptance bar

For one immutable final source SHA (after fatalfix integration):

1. Fresh uncached `rust-ci.yml` whole-workspace run on that exact SHA; the run
   must record matching `headSha`, clippy success before workspace tests, and
   both exits. Do not count advisory formatting exits as acceptance. Save the
   Actions URL, SHA and conclusion.
2. Run mixed gates serially, all at the same SHA, with Go Frontend, History and
   Worker enabled and Go Matching absent. Verify `results.json` says
   `goServices=[frontend,history,worker]`, `goMatchingStarted=false`, and the
   Rust binary is the production `cadence-matching` executable. Capture
   `commit.txt`, build/gate/probe/lane exits, topology/config/result artifacts,
   and a fresh `binary.sha256` for every mixed run.
3. Evidence must span Go SDK workflow/activity/query/history interactions;
   discovery/Ringpop membership and ownership; configured persistence and
   recovery; gRPC and legacy Thrift/TChannel; configstore and shard-distributor
   ownership; lifecycle shutdown/restart; two-node failure, reassignment and
   durable recovery. A lane's green exit proves only the scenarios actually
   present in its probe. Pair the mixed evidence with the RPC-by-RPC source/test
   audit in `matching-final-rpc-acceptance-matrix-20261025.md`; it does not
   replace exhaustive error/optional-field/deadline/forwarding parity review.
4. No old green result transfers across semantic Rust changes. Each evidence
   row must have the final SHA and its own binary hash. Preserve failures and
   flakes with artifacts; do not relabel them.

## Serial run sequence after fatalfix is integrated

Before each dispatch, ensure no mixed gate is in progress. Keep one gate at a
time. Use a stable immutable ref at `EXPECTED_SHA`; after every run inspect its
Actions `headSha` and artifact `out/commit.txt`, and reject a mismatch.

1. `gh workflow run rust-ci.yml --ref "$BRANCH" -f clean_target=true` with no
   package filter. Confirm a fresh target path, clippy then workspace tests,
   and record URL/head SHA/conclusion and `clippy.exit` / `test.exit`.
2. `matching-production-validation.yml`: `lane=postgres`.
3. Same workflow: `lane=mysql-probes`.
4. Same workflow: `lane=thrift` (Go Matching client Thrift, Rust History
   TChannel).
5. Same workflow: `lane=configstore`.
6. Same workflow: `lane=shard-distributor`; this rerun is required because
   `40205093375` failed the post-SIGKILL executor-expiry assertion. Accept only
   if the final artifact records the selected executor/owner, TTL expiry and
   assignment cleanup results plus successful recovery at the exact final SHA.
7. `matching-mixed-gate.yml` defaults (MySQL, two Rust Matching nodes, restart
   and failure recovery).
8. `matching-cassandra-mixed-gate.yml` with explicit
   `--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3`.
   The script says Cassandra implies two-node failure recovery; retain the
   explicit flag and confirm the actual recovery scenario in `gate.log` and
   checkpoint JSON.
9. `matching-active-offer-gate.yml` is currently scenario-only. For lifecycle
   acceptance, run its command through the same MySQL wrapper with
   `--probe-active-offer --matching-restart --matching-failure-recovery
   --matching-idle-check-seconds 3`, or change the workflow then re-pin and
   rerun the entire sequence. Do not count its existing workflow's success as
   restart/recovery evidence.
10. Run `matching-http-tls-interop.yml` only as supplemental standalone Rust
    History fixture evidence. It is not a matching mixed lane: it builds
    `cadence-server`, runs a Go YARPC/Thrift client against a Rust History
    fixture, and does not run the Go Frontend/History/Worker cluster with
    Rust Matching. Do not use it to claim Matching gRPC/Thrift listener or
    cluster TLS acceptance.

For each mixed run download `out/`; verify `out/binary.sha256`, source SHA,
all exits, and the actual scenario records. Workflows use standard
`ubuntu-latest`, `timeout-minutes: 90`, `cancel-in-progress: true`, and
three-day artifacts. Workflow concurrency groups are per workflow, so they do
not enforce seriality across different workflows; the operator must do so.

## Why green labels are not full acceptance

- `run-go-service-baseline.py` sets `acceptanceEligible` true only for
  Cassandra and false for shard-distributor. `tests/gates/README.md` also says
  the production lane is false by design. This is a lane classification field,
  not the overall Matching service decision.
- Green Postgres/MySQL/Thrift/configstore/active-offer runs reported
  `preliminary-passed`, `acceptanceEligible=false` because they exercise
  selected scenario suites; the bit is not an assertion of complete RPC
  parity. Conversely, Cassandra's true bit only means that Cassandra's lane
  completed its defined acceptance scenario; it cannot erase a failure in
  another required lane or an incomplete parity row.
- `matching-master-validation-6af476941.md` records the shard-distributor
  post-SIGKILL expiry failure and documents the narrower scopes of all green
  runs. Its runs and the older candidate evidence cannot be inherited after
  Refresh/typed-SQL/fatalfix semantic changes.

## Current substantive gaps and source pointers

- Fatal SQL writer failure lifecycle has no passing real-binary regression yet:
  `memory/matching-progress-20261026.md` says focused run
  `40222090786` failed and the fixture repair is pending. Parent reports
  `e69c76633` unmerged and focused CI `40357713048` pending. No candidate pin
  until it passes and is integrated.
- Shard-distributor recovery is a substantive failed scenario, not a label
  issue: `memory/matching-master-validation-6af476941.md` (run
  `40205093375`) and workflow lane config in
  `.github/workflows/matching-production-validation.yml`.
- Active-offer workflow omits both restart and failure-recovery flags:
  `.github/workflows/matching-active-offer-gate.yml`. Its SDK offer assertion
  is useful scenario evidence only.
- Cassandra workflow default omits the failure-recovery flag even though
  `tests/gates/run-cassandra-lane.sh`/baseline infer Cassandra behavior;
  explicitly pass it and inspect artifacts to avoid relying on an implication.
- HTTP/TLS workflow topology is unrelated to full Matching mixed acceptance:
  `.github/workflows/matching-http-tls-interop.yml` and
  `tests/run-matching-thrift-compat.py`.
- Mixed SDK probe is broad but selected: `tests/gates/README.md` lists echo,
  retries/timeouts, child workflows, signal/cancel, query, isolation,
  partition config, idle recovery and heartbeat/timeouts. The RPC matrix notes
  remaining unpaired typed-error, malformed/optional-field precedence,
  deadline/cancellation and forwarding cases; mixed lanes do not cover all 13
  gRPC RPCs' branches.
- The workflow dispatch list has no explicit dedicated transport matrix for
  matching gRPC vs Thrift combined with both Go History gRPC and TChannel
  beyond the current lane selections. Configstore and MySQL/PG/Cassandra
  persistence are represented by distinct lanes; none alone proves every
  persistence-specific failure/restart path.
- `matching-final-gate-plan-20261025.md` already identifies the lane inventory
  and caveats but its ten-step sequence must be amended by the active-offer
  lifecycle correction above and explicit shard-distributor failure criterion.

Relevant sources: `.github/workflows/rust-ci.yml`,
`.github/workflows/matching-production-validation.yml`,
`.github/workflows/matching-mixed-gate.yml`,
`.github/workflows/matching-cassandra-mixed-gate.yml`,
`.github/workflows/matching-active-offer-gate.yml`,
`.github/workflows/matching-http-tls-interop.yml`, `tests/gates/README.md`,
`tests/gates/run-{mysql,postgres,cassandra}-lane.sh`,
`tests/run-go-service-baseline.py`,
`tests/run-matching-production-failure-recovery.py`,
`memory/matching-final-rpc-acceptance-matrix-20261025.md`, and
`memory/matching-master-validation-6af476941.md`.
