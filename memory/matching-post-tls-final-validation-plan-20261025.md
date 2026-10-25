# Matching post-TLS integration final validation plan

Status: planning only. No workflow was dispatched and no Cargo/Go command was
run locally. TLS implementation and independent TLS review remain active.

## Source pin and dispatch discipline

After the TLS patch and its review are integrated into a single branch, set
`BRANCH` to that integration branch and `EXPECTED_SHA` to its full 40-character
commit SHA. Dispatch every command below with `--ref "$BRANCH"`, then verify
that each run checked out exactly `EXPECTED_SHA` using `gh run view RUN_ID
--json headSha,headBranch,conclusion,url`. If the branch moves or a run reports
another SHA, that result is not final-source evidence and must be repeated.
Do not use the current master SHA or inherit a green result from another
revision. The TLS fix must be included in `EXPECTED_SHA`.

```bash
BRANCH=<tls-integrated-branch>
EXPECTED_SHA=<full-40-character-sha>
```

Run the gates serially. The CI workflows use standard `ubuntu-latest` runners,
fresh run-specific targets for mixed lanes, and three-day artifacts. Retain
each run URL, branch/head SHA, conclusion, command/input, artifact, exit files,
and Matching binary SHA256 in
`memory/evidence/matching-post-tls-<date>/`. Download artifacts immediately;
the workflow retention is short. Inspect the artifacts before recording
success. Keep the existing probe classification labels unchanged.

## Required CI commands

### 1. Fresh workspace gate

```bash
gh workflow run rust-ci.yml --ref "$BRANCH" -f clean_target=true
```

This leaves `package` empty, so `rust-ci.yml` runs strict workspace Clippy
followed by workspace tests on its fresh target. Record `clippy.exit`,
`test.exit`, `fmt.exit`, `workspace-format.exit`, validation identity and
metrics/oracle artifacts. The repository has known unrelated workspace format
drift: report `workspace-format.exit` as an advisory separately from the
focused required formatting check. A workspace-format advisory of 1 is not a
passing format check and must remain visible.

### 2. PostgreSQL production mixed lane

```bash
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=postgres
```

Exercises a fresh Rust Matching binary with Go Frontend/History/Worker, the
PostgreSQL schema, SDK workflow compatibility, restart/failure recovery,
PostgreSQL GC/domain metadata, standby query forwarding, partition listing,
durable restart and AutoConfigHint probes.

### 3. MySQL extended recovery and probes

```bash
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=mysql-probes
```

Exercises the Go MySQL schema and Rust Matching mixed runtime with restart and
failure recovery, backlog forwarding, standby query, partition listing,
durable restart and AutoConfigHint. Use this corrected production-validation
lane rather than treating the old readiness-race run as final.

### 4. Thrift/TChannel transport

```bash
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=thrift
```

Exercises Go Frontend/History/Worker calling Rust Matching via Thrift/TChannel
(including Go History TChannel), with AutoConfigHint. This does not exercise
Matching HTTP TLS or gRPC missing-identity startup parity. Source search found
no current workflow reference to `tests/run-matching-configured-store-validation.py`
or `tests/run-matching-thrift-compat.py`; therefore do not claim these
standalone script checks are included in this dispatch.

### 5. Configstore

```bash
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=configstore
```

Exercises the Go-seeded configstore row and Rust Matching dynamic-config read
through the mixed production runtime. Preserve configured key/value and
expiry observations.

### 6. Real shard distributor

```bash
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=shard-distributor
```

Exercises Rust Matching launcher wiring to the real Go shard distributor,
heartbeat, ownership/reassignment, restart and executor expiry/drain behavior.

### 7. Cassandra

```bash
gh workflow run matching-cassandra-mixed-gate.yml --ref "$BRANCH"
```

The default args enable restart and idle recovery; the workflow's Cassandra
lane additionally enables two-node failure recovery. Preserve database
container cleanup evidence and scenario results.

### 8. Active offer against Go History

```bash
gh workflow run matching-active-offer-gate.yml --ref "$BRANCH"
```

Re-run the successful zero-insert active-offer scenario against the final
integrated source. Record the actual result and retain `preliminary-passed` /
`acceptanceEligible:false` when those are the emitted labels; do not promote
this one scenario to service acceptance.

### 9. gRPC TLS missing-identity behavior

The existing `rust-ci.yml` workspace gate will run the TLS worker's committed
tests, but none of the existing selectable production mixed lanes configures
Rust Matching gRPC TLS with absent or lone server certificate fields. Require
the TLS worker's focused test to demonstrate the Go-compatible distinction:
startup succeeds with no complete server identity and TLS handshakes fail;
invalid or malformed supplied credentials retain the documented Go-equivalent
behavior. Confirm that test is included in the workspace run above. If the
fix is only covered by a private/helper unit and not production listener
construction, the gap remains open: request an integration test before sign
off. Do not describe the unrelated Thrift HTTP TLS lane as covering this gRPC
case.

## Launcher/listener impact and rerun decision

The open mismatch is in shared `cadence-service-core/src/grpc_tls.rs`; Matching
constructs gRPC server and History/client TLS in
`rust/crates/cadence-matching/src/launcher.rs` and binds the production gRPC
listener there. Every mixed lane above launches the freshly built production
Matching executable. Thus PostgreSQL, MySQL, Thrift/TChannel, configstore,
shard-distributor, Cassandra, and active-offer each exercise production
launcher/listener startup on their configured transport/backend. Their current
configs primarily run gRPC without TLS (Thrift/TChannel changes Go outbound
transport; it does not remove Rust's listener). They are regression checks for
the changed executable and ordinary listener startup, not direct tests of the
missing-identity TLS branch.

Rerun all required mixed lanes after the shared TLS change. Prior mixed results
are source-bound and cannot establish that the changed binary still starts,
discovers Go services, serves SDK traffic, recovers persistence, or shuts down.
The Rust HTTP TLS listener tests in `cadence-matching/src/launcher.rs` are
compiled/exercised by the fresh workspace gate, but the existing workflows do
not dispatch the standalone Go HTTP/Thrift TLS interop script. Do not claim
mixed HTTP TLS interop unless a workflow invocation actually ran that script
and its evidence is archived. The fresh workspace gate is required to compile
and exercise committed TLS tests on the exact integrated source.

## Completion checklist

- [ ] TLS implementation and independent reviews are integrated; record the
      integration commit and full SHA as `EXPECTED_SHA`.
- [ ] TLS-specific production-path tests cover no identity, lone identity,
      valid identity and invalid/malformed identity as applicable to the
      reconciled Go behavior; verify test names and inclusion in workspace CI.
- [ ] Fresh workspace gate above reports exact head SHA, success, strict
      Clippy then workspace tests; preserve exit files and explicitly report
      both focused formatting and workspace-format advisory results.
- [ ] PostgreSQL, corrected MySQL extended, Thrift/TChannel, configstore, real
      shard distributor, Cassandra and active-offer workflows each ran
      serially on `EXPECTED_SHA` and concluded successfully.
- [ ] For every mixed workflow, archive run URL, source SHA, exact command,
      build/gate exits, binary SHA256, probe results and cleanup evidence.
- [ ] Keep preliminary/eligibility labels as emitted. Reconcile failures and
      classify each result as scenario evidence or acceptance evidence.
- [ ] Update the final-candidate tracker and Matching completion audit with
      source-specific evidence. Reconcile every required audit row and any
      failed/ignored behavior against Go source.
- [ ] Do not mark Matching complete or start History until the complete parity
      audit and all required evidence are accepted.

## Gaps / parent decisions

1. **Explicit gRPC TLS production integration coverage:** current workflows do
   not enable Matching gRPC TLS with absent/lone identity. The TLS patch must
   provide an executable production-path test included by workspace CI, or the
   parent must decide to add a dedicated CI gate/workflow in a later task.
2. **Go HTTP TLS interop workflow gap:** detailed Go interop assertions live in
   `tests/run-matching-thrift-compat.py`, but no current workflow references
   that script (nor its configured-store wrapper). Current workflow inputs
   provide no standalone HTTP TLS lane. Parent must decide whether committed
   Rust listener tests suffice for the current requirement or whether a
   dedicated/reused CI workflow invocation is needed for Go HTTP TLS interop.
3. **Workspace format debt:** known workspace-wide format check is advisory and
   can remain nonzero under current policy. Parent must retain the distinction
   from required changed-file formatting and avoid claiming whole-workspace
   formatting is clean.
4. **Scope remains service completion:** passing this list does not itself
   close any unresolved parity-audit rows not covered by these gates; resolve
   those against Go behavior before Matching sign-off.
