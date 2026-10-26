# Matching CI evidence audit — 2026-10-26

Read-only Actions audit and final-lane checklist. No workflow was dispatched, no running run was polled in a loop, and no local Cargo, Go, formatter, or runtime command was run. Scope is exact run SHA, not branch name. Small run metadata and the focused failure log are saved under `/mnt/storage/service/ci-evidence/`.

## Focused Actions failure

- Run [40369748415](https://github.com/auenkr/cadence-rust/actions/runs/40369748415), branch `t3/adaptive-scaler-fix`, exact head `ca64a5a934c6c04daf0f9385c00adbad09e788ea`, concluded failure.
- Failed step: `Clippy` (step 23), package input `cadence-matching`, invoking all-targets Clippy with `-D warnings`.
- Actual error: `E0282 type annotations needed` at `rust/crates/cadence-matching/src/matching_rpc_tests.rs:1896`; `let partitions = (0..4)...` has an unconstrained collection type by the later `partitions.clone()` at line 1912. Rust's suggested correction is an explicit `Vec<_>` annotation. Clippy exited 101; Formatting, Test, and live Go/Rust metrics comparison were skipped.
- This is a Rust test-compilation/type-inference failure, not evidence from an adaptive-scaler production probe. It does not exercise or establish the probe candidate's adaptive behavior. It is relevant as a CI blocker only if the exact failed test source is present in the candidate SHA. Confirm source equality on the candidate before attributing; this audit made no code changes.

## Current authoritative run snapshots (not polled)

- [40369559424](https://github.com/auenkr/cadence-rust/actions/runs/40369559424): dispatch branch `t3/matching-validation-candidate-1`, exact SHA `aff3a3c485af1a2df9b592af325d9c7bf4a27357`. Snapshot taken 2026-10-26: `in_progress`, no conclusion. Clippy and Formatting succeeded; Test was in progress; metrics comparison and later evidence steps were pending. This is not a pass or acceptance result. Do not infer a later result from this snapshot.
- [40370252004](https://github.com/auenkr/cadence-rust/actions/runs/40370252004): dispatch branch `matching-provisional-20261026`, exact SHA `37bca364ce112b693e9bed504409eaa1bc04b18c`. Snapshot: `completed / cancelled`, no jobs. Not validation evidence.
- Machine-readable snapshots are `/mnt/storage/service/ci-evidence/40369559424-status.json` and `/mnt/storage/service/ci-evidence/40370252004-status.json`; failed log is `/mnt/storage/service/ci-evidence/40369748415-failed.log`.

## Final Matching acceptance sequence

The final acceptance audit says Matching remains incomplete and cannot advance to History. Assemble and review the candidate first, including the SQL writer fatal lifecycle fix and retained TLS/active-offer workflow changes. Pin one immutable `BRANCH`/`EXPECTED_SHA`; run only one Actions gate at a time. Every Actions `headSha` and every artifact `out/commit.txt` must equal that pin. The earlier plan's HTTP/TLS History fixture is supplemental only; the acceptance audit's required list is the following ten lanes, all against that same final pin:

1. `gh workflow run rust-ci.yml --ref "$BRANCH" -f clean_target=true` — fresh uncached final target; workspace Clippy then workspace tests. `package` empty means workspace. Require both exit records zero and retain exact head. The older plan called a workspace format exit advisory; current acceptance result must still record format status rather than silently relabel it.
2. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=postgres`
3. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=mysql-probes`
4. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=thrift`
5. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=configstore`
6. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=shard-distributor`
7. `gh workflow run matching-mixed-gate.yml --ref "$BRANCH"` — default `extra_args` already requests restart, failure recovery, idle check 3.
8. `gh workflow run matching-cassandra-mixed-gate.yml --ref "$BRANCH" -f extra_args="--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3"` — make the recovery flag explicit even though the Cassandra harness is documented to add two-node recovery.
9. `gh workflow run matching-active-offer-gate.yml --ref "$BRANCH"` — workflow supplies `--probe-active-offer --matching-restart --matching-failure-recovery --matching-idle-check-seconds 3`.
10. `gh workflow run matching-production-tls-mixed.yml --ref "$BRANCH"` — production CA and hostname-verified TLS in Go Frontend/History/Worker + Rust Matching topology, with Go Matching absent.

Workflow inputs: only Rust CI needs `clean_target=true` (leave `package` empty for workspace). Production validation requires one `lane` from `postgres|mysql-probes|thrift|configstore|shard-distributor`. Cassandra's explicit `extra_args` are above; other no-input workflows need no flags. All use standard hosted runner constraints from workflow files.

## Evidence acceptance checklist

For every run, capture run URL, dispatch event, exact `headSha`, conclusion, and downloaded artifact. For each mixed lane require `out/commit.txt == EXPECTED_SHA`, zero build/gate/lane/probe exits, and record SHA-256 of the exact `cadence-matching` binary (`out/binary.sha256`). Inspect logs/results to prove the scenario actually ran; a green job alone is insufficient. Preserve artifacts/evidence on the pinned candidate.

- Production validation lanes: inspect `out/command.txt`, `out/build.exit`, `out/gate.exit`, nested lane/run exits, `out/gate/run/results.json`, probe logs, and service logs. Confirm Go Frontend/History/Worker with Rust Matching and no Go Matching; assert per-lane features: Postgres GC domain/query standby/list partitions/durable restart; MySQL backlog forward/query standby/list partitions/durable restart; Thrift Matching + TChannel History; configstore; shard distributor executor selection, TTL expiry, assignment cleanup and recovery.
- Two-node MySQL: verify both Rust owners, restart, crash recovery/takeover, and SDK outcome in nested results/logs and exact binary identity.
- Cassandra: inspect two-node recovery, restart, SDK outcome, and scenario exits, not just aggregate success.
- Active offer: inspect `out/commit.txt`, `out/binary.sha256`, build/gate/lane/run exits, `results.json`, `activeOffer.json`, `probe.log`, and Rust Matching logs. Assert active-offer result, History scheduled task/token delivery, zero persistence rows/inserts, restart, graceful drain, idle eviction and two-node crash recovery.
- TLS: inspect `results.json`, `probe.log`, `run.exit`, `matching-shutdown.json`, Rust server logs, redacted TLS config, commit/binary/build/gate/lane/run exits, binaries and config hashes. Assert CA-backed hostname verification and topology with only Rust Matching plus Go Frontend/History/Worker.
- Rust CI: require clean target evidence, clippy/test exit records and logs. Record formatting status as reported by the workflow.

These ten lanes still do not prove adaptive-scaler production registry-to-scaler wiring. Before calling Matching complete, include a mixed production probe with `matching.enableAdaptiveScaler=true`: generate load on a root normal queue, verify read/write partition changes using `ListTaskListPartitions`, verify successful task delivery through new partitions, then verify scaler shutdown on unload/drain. This is an explicit uncovered requirement from the acceptance audit; neither the focused failed run nor the `aff3` in-progress snapshot supplies it.

Prior green results on older SHAs remain preliminary and are not rebound: they cannot replace this serial final-pin sequence. Keep `preliminary-passed` and `acceptanceEligible:false` labels unchanged until the full parity audit and all final artifacts are reviewed.
