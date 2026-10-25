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
2. **Go HTTP TLS interop result unresolved:** the dedicated
   `.github/workflows/matching-http-tls-interop.yml` now invokes
   `tests/run-matching-thrift-compat.py`, but run 40168201737 failed before TLS
   assertions. Its failure/correction is under separate investigation; do not
   treat the workspace gate or this failed lane as Go HTTP TLS compatibility.
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

## Embedded startup fix integration and validation (2026-10-25)

Independently approved branch `t3/http-tls-embedded-probe-fix-20261025` at
`fade09c501e23212d4d86e64c653057d589d1910` was fast-forwarded to master and
pushed. It adds only the embedded-development guard around shard-manager
startup in `cadence-matching/src/launcher.rs`; explicit-config behavior is
preserved. The same commit retains the exact failure evidence for HTTP TLS
interop run [40166507919](https://github.com/auenkr/cadence-rust/actions/runs/40166507919)
at source `5811c0d8bc14ff57e03004260a83843726ef1291`: fresh locked build passed,
but the interop exited 1 before TLS assertions because embedded startup
required unavailable Ringpop membership. See
`memory/evidence/matching-http-tls-interop-failure-40166507919/`.

Focused run [40167687605](https://github.com/auenkr/cadence-rust/actions/runs/40167687605)
completed successfully on the exact fix SHA, but used the Rust cache path and
is not the required fresh uncached workspace final gate. The automatic master
push run [40168143481](https://github.com/auenkr/cadence-rust/actions/runs/40168143481)
also targets the exact fix SHA, but uses that same cached path. Neither result
replaces a `clean_target=true` workspace run.

Pin the validation branch `t3/matching-validation-http-embedded-20261025` to
the exact source `fade09c501e23212d4d86e64c653057d589d1910`. Dispatch only the
HTTP TLS interop lane and fresh workspace gate first:

```bash
gh workflow run matching-http-tls-interop.yml --ref t3/matching-validation-http-embedded-20261025
gh workflow run rust-ci.yml --ref t3/matching-validation-http-embedded-20261025 -f clean_target=true
```

HTTP TLS interop run
[40168201737](https://github.com/auenkr/cadence-rust/actions/runs/40168201737)
completed with failure on the exact pinned head
`fade09c501e23212d4d86e64c653057d589d1910`; its diagnosis is assigned
separately. Preserve this as a source-bound failure and inspect the artifact
before recording its cause.

After confirming no mixed gate was active, dispatched fresh workspace run
[40168414544](https://github.com/auenkr/cadence-rust/actions/runs/40168414544)
on `t3/matching-validation-http-embedded-20261025` at exact head
`fade09c501e23212d4d86e64c653057d589d1910`, with `clean_target=true`. This
selects `$RUNNER_TEMP/cargo-target-final` and skips `rust-cache`; the required
order is strict workspace Clippy then workspace tests. The run is pending and
its artifacts/exits still need review. Automatic master run 40168143481 uses
the cached target path and does not substitute for this run. No second mixed
lane has been dispatched. Matching remains INCOMPLETE, and the existing
preliminary-passed / acceptanceEligible labels remain unchanged.

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

Run https://github.com/auenkr/cadence-rust/actions/runs/40165433074 was
dispatched on pinned ref `t3/matching-validation-tls-e3c79b3-20261025` at head
`4b5d90458471c36ab8064bb69c695854ffc23de5`; it failed during build with exit
101 because `--locked` prevented Cargo from refreshing `rust/Cargo.lock`.
Interop was skipped; this is a setup failure, not production TLS evidence. At
dispatch, no other mixed gate was active; master workspace run 40165304531 was
still running. Compact failure evidence:
`memory/evidence/matching-http-tls-interop-failure-40165433074/`.

The initial dispatch request was rejected by GitHub before run creation with
HTTP 422: `runner.temp` is unavailable in job-level `env` for
`matching-http-tls-interop.yml`. Corrected the workflow to select the fresh
target in a step via `$RUNNER_TEMP` and `$GITHUB_ENV`; Ruby YAML parsing and
`git diff --check` passed. The validation branch
`t3/matching-validation-tls-e3c79b3-20261025` is pinned at
`4b5d90458471c36ab8064bb69c695854ffc23de5`, including this dispatch-only
workflow correction. The failed request did not start a gate. Master auto
workspace run 40165304531 was in progress; no duplicate workspace dispatch is
planned.

The subsequent first-lane run failed because `--locked` prevented Cargo from
refreshing the stale lockfile; no interoperability assertions ran. The
workflow now uses the repository's standard unlocked CI build and records any
resulting lockfile diff in its artifact. No TLS source/assertion changed. The
retry ref is `t3/matching-validation-tls-e3c79b3-r1-20261025`. Dispatch only
after checking that no mixed gate is active. Run evidence and diagnosis are
preserved at `memory/evidence/matching-http-tls-interop-failure-40165433074/`.

Before retry dispatch the active-run query was empty. Master automatic
workspace run 40165304531 completed successfully on
`389c36527908c20d461576d0868a22ece2519f69`; it was not manually duplicated.
The retry ref is pinned at
`e787524851b06f0f9aab007ae645c5d9cf8c85e5`. Run
https://github.com/auenkr/cadence-rust/actions/runs/40165758848 is currently
`in_progress` on that exact head; only the same first interop lane was
dispatched. Do not dispatch another lane until this run completes and its
artifacts are inspected.

Run 40165758848 completed with failure on source
`e787524851b06f0f9aab007ae645c5d9cf8c85e5`. Its fresh server build succeeded
with exit 0 and SHA256
`ec2582601d96e73cbaf7650b67afeffe0aebeeefa2181ef772f8b12fbe444351`; the
standalone Go interop harness exited 1 before its TLS requests. A negative
startup assertion passed the unmodified Rust development config, which selects
Cassandra; server assembly attempted `127.0.0.1:9042` and failed connection
before reaching the expected domain-cache NoSQL rejection. This is a harness
setup/assertion failure, not evidence of TLS behavior. The CI-generated lock
patch reordered one workspace dependency entry and added `js-sys` and
`wasm-bindgen` to chrono, with no package version/checksum change. Exact
downloaded logs, exits, SHA and patch are saved at
`memory/evidence/matching-http-tls-interop-failure-40165758848/`.

Follow-up branch `t3/http-tls-lockfix-20261025` applies that exact lock delta,
restores `--locked`, and records the lock diff against HEAD. The negative
NoSQL assertion now uses an unsupported plugin name in the same development
config, giving a deterministic startup rejection before any database
connection. No production code or TLS assertions changed. Mixed validation
has not been rerun.

## Lock/harness retry candidate after independent approval

Independent review approved branch `t3/http-tls-lockfix-20261025` at exact
source `204c2414f3707577f5bce921c82f8fcc72b056c7`. Merged to root master as
`1e537b135ad6919cfcb44ad09fbc7fe57f4957fd`. It restores `--locked` after
applying the exact CI-generated lock delta, captures any lock diff against
HEAD, and makes the unsupported NoSQL startup probe deterministic by changing
the development config's plugin name to an unsupported test value. Production
code and TLS assertions are unchanged. The full prior run 40165758848 failure
evidence is retained at
`memory/evidence/matching-http-tls-interop-failure-40165758848/`.

Push of the lockfile change is expected to trigger automatic `rust-ci`; record
that run if it occurs and do not manually duplicate the workspace gate. The
stable candidate ref is
`t3/matching-validation-http-lockfix-20261025`, pinned at
`5811c0d8bc14ff57e03004260a83843726ef1291`. Only the first serial workflow is eligible for dispatch:
`matching-http-tls-interop.yml`. Before dispatch, confirm no other mixed gate
is active. Do not dispatch another lane until that run finishes and its
artifact is reviewed.

### Corrected first-lane run

Run https://github.com/auenkr/cadence-rust/actions/runs/40166507919 completed
with failure on exact source `5811c0d8bc14ff57e03004260a83843726ef1291`.
Master automatic `rust-ci` run 40166476374 succeeded on the same source. The
HTTP TLS lane's locked build passed but its embedded no-config startup probe
failed before TLS requests. See the precise diagnosis and retained evidence
above and in `memory/evidence/matching-http-tls-interop-failure-40166507919/`.

Production finding: no-config embedded mode deliberately skips production
Ringpop setup (`rust/crates/cadence-matching/src/launcher.rs`, around
1059–1104), but the later shard-distributor setup required Ringpop membership
whenever configured development namespaces were present (around 1410–1415).
The embedded defaults include a shard-distributor namespace, so startup
necessarily failed. Follow-up branch
`t3/http-tls-embedded-probe-fix-20261025` now gates shard-distributor setup on
`!embedded_development`. This leaves explicit-config production setup intact.

## Fresh workspace result and current validation status

Fresh uncached workspace run
[40168414544](https://github.com/auenkr/cadence-rust/actions/runs/40168414544)
passed on the exact pinned source
`fade09c501e23212d4d86e64c653057d589d1910` from
`t3/matching-validation-http-embedded-20261025`. The fresh target step selected
`/home/runner/work/_temp/cargo-target-final`; the cache action was skipped.
Strict workspace Clippy completed before workspace tests; both exits were 0.
Focused TLS-touched Rust formatting exit was 0 and changed Matching runtime
formatting passed. The separate formatting step exit was also 0. Workspace
`cargo fmt --all -- --check` remains advisory exit 1 due to unrelated broad
format drift, including `cadence-thrift/tests/go_fixtures.rs`; it is not a
format pass. Details, compact log excerpts and selected artifacts are in
`memory/evidence/matching-workspace-final-40168414544/`; original artifact
ZIPs and hashes are at `/mnt/storage/service/ci-evidence/run-40168414544/`.

The Go owner-order oracle succeeded. All four Rust/Go matching metrics
comparisons returned `match` with series counts 20, 24, 21 and 23. The
metrics-example binary digest is recorded, not a Matching server binary.

HTTP TLS interop run
[40168201737](https://github.com/auenkr/cadence-rust/actions/runs/40168201737)
failed before TLS assertions: parent reports `DescribeTaskList` returned
`expectedBadRequest` in the embedded probe. A proposed guard at `84cadec2` was
rejected by independent source review because Go is nil-safe for the empty
request through downstream handling and domain precedence must be preserved.
The correction worker is investigating the actual Go path. Do not merge that
proposal or dispatch another mixed lane until correction/review and the
pending interop result are resolved. No second mixed lane has run.

This named-source workspace pass does not close the HTTP TLS interop or gRPC
TLS gaps and does not establish Matching completion. Matching remains
INCOMPLETE; preserve existing `preliminary-passed` and
`acceptanceEligible:false` labels. No local Cargo/Go/rustfmt validation was
run.

## DescribeTaskList oracle correction and latest interop status (2026-10-25)

Approved branch `t3/http-tls-go-oracle-correction-20261025` was reviewed at
code source `5cafc2dc941fc676cad66d6756fb812c502aa5bf` (commit
`fefe4c663f08d9b2ded05eb24a14cc5e42e15af5`), then received only documentation
and evidence commits through terminal tip
`d04ee7492a3744c92adc9e8b118351a25303bfe7`. The tip-to-approved-commit delta
was verified documentation-only. The source diff contains CI/oracle and
interop-test harness work, with no Rust production file changes. The branch
also preserves earlier failed and superseded oracle attempts and their
limitations; see its committed evidence directories.

Original Go oracle run
[40171379726](https://github.com/auenkr/cadence-rust/actions/runs/40171379726)
completed successfully on exact code source
`5cafc2dc941fc676cad66d6756fb812c502aa5bf`. It verified absent nested request
and empty task-list success with zero pollers, plus the malformed reserved
task-list `InternalServiceError` and exact message. The run's Go oracle exited
0, followed by successful strict Clippy and workspace tests. It used the
standard cached target; fresh target selection was skipped, so it is not a
fresh final workspace gate. TLS-touched formatting passed (`0`); global
workspace formatting remains advisory `1`. Compact logs, exits, run metadata,
oracle output and artifact hashes are retained at
`memory/evidence/matching-workspace-and-go-oracle-40171379726/`; original
artifact ZIPs are under `/mnt/storage/service/ci-evidence/run-40171379726/`.

The separate HTTP TLS interop run
[40171381762](https://github.com/auenkr/cadence-rust/actions/runs/40171381762)
failed on that same source before TLS assertions. Parent-reported progress:
embedded TChannel passed; the configured decision-task delivery probe using
TChannel/HTTP/trailing behavior then emitted a “not enough hosts” membership
warning, and TLS was not reached. Its original artifact ZIP is retained at
`/mnt/storage/service/ci-evidence/run-40171381762/` and a separate concise
record is at
`memory/evidence/matching-http-tls-interop-failure-40171381762/README.md`.
At that earlier status read, Sonnet diagnosis was still pending. The later
reviewed harness diagnosis and correction are recorded in the History ring
stub section below; the first rerun on that candidate is pending. No additional
workflow has been dispatched after it.

The proposed `84cadec2` Rust guard remains rejected and is absent from the
approved source. Go oracle run 40170886290 established empty/absent-nested
success in its fixture and reserved malformed-name rejection; its synthetic
mock errors for ordinary unknown-domain requests are explicitly not production
error-mapping evidence. Preserve domain lookup/precedence. This integration
does not close HTTP TLS interop, gRPC TLS gaps, or Matching parity; Matching
remains INCOMPLETE and the existing eligibility labels stay unchanged.

## History ring stub candidate and pending HTTP TLS rerun (2026-10-25)

Independently approved stub branch
`t3/matching-history-ring-stub-20261025` was based on code source
`d28a5b22ca87a2978f9db6121510ba927b6179a` and integrated at final tip
`107d5cfced5f8a6d8c29480507aaa75b1df6c792`. Review scope is test/harness
only: the Go history ring member advertises `serviceName=cadence-history` and
the existing Rust History gRPC fixture. It adds no Rust production or
assertion change.

Static review confirmed `ringpopprovider.Provider.Start` iterates and sets only
the supplied port map; there is no required `PortTchannel` entry. The provider
reads the TChannel label optionally and has an address-port fallback. The
revised harness starts `go test` in a new session, writes its release signal,
waits up to 30 seconds, kills the process group if needed, and waits to reap
the group leader. The final approved diff from `d28a5b22` is limited to
dropping the TChannel label from the fixture and this process-group cleanup.
`git diff --check` passed for those code/docs changes.

The fixture limitation is explicit: the Ringpop member is a real Go provider
but it advertises the Rust History fixture's gRPC port. It exercises Matching's
History discovery and request path; it is not validation against the original
Go History service and cannot count as mixed Go-service acceptance.

Merge commit `29bdd2b04b573309112286c57f1c347f0ba0b60c` is pushed. Stable
candidate `t3/matching-validation-history-ring-20261025` pins that exact SHA.
After checking that no interop or mixed gate was active, dispatched only the
HTTP TLS interop workflow:
[40172574953](https://github.com/auenkr/cadence-rust/actions/runs/40172574953)
is in progress on the exact candidate SHA. Automatic cached workspace run
40172228720 was still in progress at that pre-dispatch check and was not
duplicated. No other workflow was dispatched. Inspect the interop artifact,
binary SHA and all scenario outcomes before moving to another lane. Keep all
production Go-service gates pending until standalone interop passes, then run
them serially on a separately pinned final source. Matching remains
INCOMPLETE; retain `preliminary-passed` and `acceptanceEligible:false`.

Earlier integration record (before the History ring stub follow-up): the
reviewed branch terminal tip
`d04ee7492a3744c92adc9e8b118351a25303bfe7` was merged with the routine plan
overlap resolved in merge commit
`80bddb3e94e5871b6da9a1a1f07cc423422b27e9` and pushed to master. The push
automatically started `rust-ci` run
[40172228720](https://github.com/auenkr/cadence-rust/actions/runs/40172228720)
on that exact merge SHA; it was in progress at that status read. No manual
workflow dispatch or mixed gate had been started at that time. That earlier
stable validation branch remains pinned to
`fade09c501e23212d4d86e64c653057d589d1910`; the later History-ring candidate
is separately pinned as documented above. No worker branch/worktree cleanup
was performed.
