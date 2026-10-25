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

## CI wiring update (2026-10-25)

The workflow `.github/workflows/matching-http-tls-interop.yml` adds a manual
standard-runner gate for the previously unreferenced
`tests/run-matching-thrift-compat.py`. It checks out the selected source,
initializes `go/idls`, provisions Go from `go/go.mod`, builds a fresh
`cadence-server` with Cargo, records the source commit and server SHA256, then
runs the existing script with the freshly built binary. The script's original
Go YARPC/Thrift client is the peer and covers TLS health, plaintext behavior,
TLS sniffing, malformed/short/stalled handshakes, timeout recovery and Go
compatibility assertions. Artifacts include source SHA, exact commands,
build/interoperability exits and logs, binary digest and script-generated
server/database/certificate evidence; retention is three days. Limits are
`ubuntu-latest`, 90 minutes, and ref-scoped cancel-in-progress concurrency.
The upload explicitly excludes `out/interop/matching-drain.test`, a temporary
Go test executable used by the script; logs and result evidence remain included.

No production code or TLS tests are changed here. The TLS worker branch
`t3/matching-tls-handshake-parity` modifies the production-path test
`missing_rpc_identity_starts_tls_listener_and_rejects_tls_and_plaintext` in
`rust/crates/cadence-matching/src/matching_rpc_tests.rs` to cover missing-both,
lone-cert, lone-key and CA-backed configurations. The unchanged
`.github/workflows/rust-ci.yml` workspace path invokes `cargo clippy
--workspace --all-targets -- -D warnings`, followed by `cargo test --workspace`;
these commands discover the test through the crate's normal test target. The
fresh final workspace invocation remains:

```bash
gh workflow run rust-ci.yml --ref "$BRANCH" -f clean_target=true
```

The new standalone interop lane is intentionally separate and is not
dispatched here. After this workflow is integrated into the TLS integration
branch, dispatch serially with:

```bash
gh workflow run matching-http-tls-interop.yml --ref "$BRANCH"
```

Verify the workflow's `headSha` equals the integration `EXPECTED_SHA`, inspect
and archive the artifact immediately, and record its run URL and conclusion
with the other source-bound evidence. This change does not close the gRPC TLS
test or Matching parity audit by itself.

## Integration status (2026-10-25)

Reviewed branch `t3/matching-http-tls-interop-ci-20261025` at
`8e169976858e88d793394e399b98cbbd76084709` (original workflow commit
`eb3d205c35`) was merged into shared master by merge commit
`9cdd80165773a2813c64d5ce2f1e9a165d9bfa76`. The artifact exclusion is exactly
`!out/interop/matching-drain.test`; the rest of `out/` remains uploaded. Ruby
Psych parsed the workflow YAML successfully, and `git diff --check` passed
before merge. Master was pushed after this memory update.

No Cargo/Go commands or workflow dispatch were run. The new manual interop
workflow is pending validation on the TLS-integrated source. The production
TLS worker branch remains separate; dispatch the fresh workspace gate, all
required mixed lanes, and this HTTP TLS interop lane only after that patch is
integrated and pinned as `EXPECTED_SHA`.

## TLS candidate CI result (2026-10-25)

Run 40159753424 succeeded on exact source
`ad997b07f57a435b26e56c1e5a1c2c9e89ceeede`, with clean runner target,
workspace Clippy before workspace tests (both exit 0), focused formatting
marker 0 and workspace-wide format advisory 1. The production-path missing
identity gRPC listener test passed. This candidate is **not approved for
integration**: final code review found a blocking invalid-DER PEM CA parsing
issue, and source-only final review remains active. See
`memory/evidence/matching-tls-final-ci-20261025/README.md` for compact extracted
proof, artifacts, and formatting limits. The implementation fix requires new
source-pinned CI and review; no workflow was dispatched for it in this task.

### Invalid-DER fix candidate rerun

Run 40161660335 succeeded on exact source
`4b8a6060b341ace8987e73f25e0d146429410950`; it used a fresh target and passed
workspace Clippy then tests (both exit 0), including the production missing-
identity listener test and the server CA/client-auth matrix covering invalid
DER and mixed valid/invalid PEM. The TLS-touched-file format marker is 1 and
the workspace-format advisory is also 1; neither is a pass. The earlier
`ad997b07f` invalid-DER blocker is addressed in this candidate only subject to
the independent final review, which remains active. Do not integrate or begin
mixed validation until that review approves the source and formatting state.
Compact evidence is in `memory/evidence/matching-tls-fix-ci-20261025/`.

## Approved TLS integration candidate (2026-10-25)

Independent code review approved TLS branch commit
`e3c79b3014e80e98f1c1d0a18604588d5c068929`. Its production source and CI
formatter updates were merged into root master as
`357e36567032f4da2a929d86614e12b5cf7a43e6`. The existing
`.github/workflows/matching-http-tls-interop.yml` and this validation plan are
present after the merge. Matching remains incomplete.

Fresh workspace CI already covers that exact source commit:
https://github.com/auenkr/cadence-rust/actions/runs/40164484146. GitHub reports
head `e3c79b3014e80e98f1c1d0a18604588d5c068929`, success, fresh
`/home/runner/work/_temp/cargo-target-final`, skipped cache, Clippy before
workspace tests, both exit 0. The TLS-touched-file formatter marker is 0 and
produced an empty formatting patch; workspace format advisory remains 1. This
fresh equivalent means no separate workspace CI dispatch is needed for this
code source. The master push can trigger automatic `rust-ci`; record that
result if it runs, without duplicating a workspace dispatch.

The pinned serial validation ref is
`t3/matching-validation-tls-e3c79b3-20261025`, created from the integrated
documentation commit after this note is committed. It will remain fixed while
later run evidence is recorded on master. The first and only gate to dispatch
now is:

```bash
gh workflow run matching-http-tls-interop.yml --ref t3/matching-validation-tls-e3c79b3-20261025
```

No other mixed gate was in progress at pre-dispatch check. This workflow uses
`ubuntu-latest`, a 90-minute timeout, three-day artifact retention and
cancel-in-progress concurrency. Record its exact run URL, head SHA and initial
status below. Do not dispatch the next backend/transport mixed lane until this
one finishes and its artifacts have been reviewed.

The TLS branch was independently approved and merged: code commit
`e3c79b3014e80e98f1c1d0a18604588d5c068929`, merge commit
`357e36567032f4da2a929d86614e12b5cf7a43e6`. Fresh workspace run 40164484146
covers the exact code SHA (Clippy 0, workspace tests 0, touched-file format 0,
workspace advisory 1), so no manual duplicate workspace dispatch is needed.
The evidence summary is
`memory/evidence/matching-tls-integration-candidate-20261025/README.md`.

### First serial gate dispatch

Pending dispatch after the pinned ref is pushed. No workflow dispatch is
recorded in this checkpoint.

The initial dispatch request was rejected by GitHub before run creation with
HTTP 422: `runner.temp` is unavailable in job-level `env` for
`matching-http-tls-interop.yml`. Corrected the workflow to select the fresh
target in a step via `$RUNNER_TEMP` and `$GITHUB_ENV`; Ruby YAML parsing and
`git diff --check` passed. The validation branch is advanced once to include
this dispatch-only workflow correction. The failed request did not start a
gate. Master auto workspace run 40165304531 was in progress; no duplicate
workspace dispatch is planned.
