# Matching validation evidence for master `6af476941031193317fdd6c44070e9785e6384b9`

<<<<<<< HEAD
Inspected 2026-10-25. Every run below is a completed GitHub Actions run on `master` at the exact target SHA. Run links are authoritative and include runner logs/artifacts. No validation was dispatched during this review.

## Rust CI

- [rust-ci 40195154984](https://github.com/auenkr/cadence-rust/actions/runs/40195154984): success; whole workspace (`PKG` empty), ordinary push event, shared/cache-eligible target at `/home/runner/work/cadence-rust/cadence-rust/rust/target`. This is **not** the required fresh uncached final run (`clean_target` was skipped). Clippy exit 0, workspace test exit 0, separate OpenFeature rustfmt check exit 0. The uploaded logs show `workspace-format.exit=1`, `tls-format.exit=1`, and `query-format.exit=1`; these checks were allowed to fail and the workspace format log has diffs. The job's green conclusion does not establish formatting parity. Matching emission-oracle comparison completed successfully for default/both/name-on/name-off. Its Rust example binary SHA256 is `38e3d7daffe35bb86a74e14d9fa3937ad43ba87ecdf8a98ee3947c773572fbf4` (not the cadence-matching service binary hash). The original Go malformed DescribeTaskList oracle artifact exit is 0.

## Mixed Go/Rust gates

All listed gates built Rust from a fresh run-specific `CARGO_TARGET_DIR`; workflow scripts record binary hash, build exit, and gate exit to their uploaded artifact. The GitHub log proves those files were generated but does not print their contents. Large gate artifacts (33–53 MB) were not downloaded in this review because the temporary filesystem was nearly full. Thus service-binary SHA256 values and exact exit-file contents remain to be copied from artifacts.

| Run | Scenario | Conclusion | Evidence scope |
|---|---|---|---|
| [40204240682](https://github.com/auenkr/cadence-rust/actions/runs/40204240682) | PostgreSQL | success | Go services with Rust Matching; PG GC-domain, query standby, list partitions, durable restart, auto-config hint; Matching restart/failure recovery enabled |
| [40204660498](https://github.com/auenkr/cadence-rust/actions/runs/40204660498) | MySQL, two Rust Matching nodes | success | MySQL lane; default restart/failure recovery; verifies two-node behavior |
| [40195314790](https://github.com/auenkr/cadence-rust/actions/runs/40195314790) | MySQL probes | success | backlog forwarding, query standby, list partitions, durable restart, auto-config hint |
| [40195809764](https://github.com/auenkr/cadence-rust/actions/runs/40195809764) | Thrift | success | Matching Thrift + History TChannel; auto-config hint |
| [40196298460](https://github.com/auenkr/cadence-rust/actions/runs/40196298460) | Cassandra | success | Cassandra mixed acceptance with Go services and Rust Matching |
| [40196794076](https://github.com/auenkr/cadence-rust/actions/runs/40196794076) | Active offer | success | Active offer mixed gate |
| [40197288308](https://github.com/auenkr/cadence-rust/actions/runs/40197288308) | HTTP TLS interoperability | success | Rust `cadence-server` binary, Go HTTP/TLS interop |
| [40197629016](https://github.com/auenkr/cadence-rust/actions/runs/40197629016) | Configstore | success | Matching configstore mixed gate |
| [40205093375](https://github.com/auenkr/cadence-rust/actions/runs/40205093375) | Shard distributor | failure | Failed in Go baseline probe (`RuntimeError: Go baseline probe failed`) before Rust Matching acceptance; not evidence of Rust failure or pass. |
| [40198127804](https://github.com/auenkr/cadence-rust/actions/runs/40198127804) | Shard distributor | failure | Same Go baseline probe failure, before Rust Matching acceptance. |

## Remaining evidence gaps

1. Run the required fresh, uncached whole-workspace Rust CI on this SHA, with clippy then workspace tests; preserve and report formatting results. Current CI was cache eligible and failed the workspace, TLS-file, and query-file format checks despite green aggregate status.
2. Retrieve small selected files (`binary.sha256`, `build.exit`, `gate.exit`, command/commit/target identity and relevant logs) from each green mixed-gate artifact, and verify values match this SHA. Do not infer the binary hash from the container image digest in logs.
3. Diagnose the shard-distributor Go-baseline failure independently; rerun that lane only after baseline is understood. No accepted shard-distributor evidence exists at present.
4. Resolve the Go oracle artifact's exit-file result and any mismatch with the green job step status.

These runs are substantial compatibility evidence, but they do not establish Matching completion or authorize moving to History. The parity audit and all required failure/restart/recovery evidence remain to be completed and recorded.
=======
Inspected 2026-10-25. Runs below target the exact SHA shown in each gate artifact. Links are authoritative run records. CI was neither dispatched nor polled for this evidence task. This is compatibility evidence, not a Matching completion claim.

## Rust CI

- [rust-ci 40195154984](https://github.com/auenkr/cadence-rust/actions/runs/40195154984): success; whole workspace (`PKG` empty), ordinary push, cache-eligible target `/home/runner/work/cadence-rust/cadence-rust/rust/target`. Not the required fresh final run (`clean_target` skipped). Clippy/test and separate OpenFeature rustfmt exited 0. Uploaded artifact reports `workspace-format.exit=1`, `tls-format.exit=1`, `query-format.exit=1`; those checks are allowed to fail and aggregate success is not formatting parity. Metrics emission comparison passed default/both/name-on/name-off; its hash `38e3d7daffe35bb86a74e14d9fa3937ad43ba87ecdf8a98ee3947c773572fbf4` is the example oracle, not cadence-matching. Go malformed DescribeTaskList oracle step ran; its artifact exit still needs separate inspection.
- Fresh uncached whole-workspace validation [40217841072](https://github.com/auenkr/cadence-rust/actions/runs/40217841072) was dispatched on this SHA with `package` empty and `clean_target=true`. Workflow selects `$RUNNER_TEMP/cargo-target-final`, skips cargo-cache action, then runs clippy followed by workspace tests. At inspection it was pending; outcome is intentionally not asserted here.

## Mixed Go/Rust gates

For all scenarios below the topology was Go Frontend/History/Worker with Rust Matching and original Go Ringpop discovery; gate artifacts recorded the exact SHA `6af476941031193317fdd6c44070e9785e6384b9`. Artifacts are under `/mnt/storage/service/ci-evidence/<run-id>/` locally; concise evidence matrix:

| Run | Scenario, gate outcome | Binary SHA256; build / gate / probe exits | Probe result and actual exercised scope |
|---|---|---|---|
| [40204240682](https://github.com/auenkr/cadence-rust/actions/runs/40204240682) | PostgreSQL; success | `fd1c767fc67ee5478c894f8d87088fffc0c2fb9e2ecea69d2438c652e6f6c9c7`; 0 / 0 / 0 | `preliminary-passed`, `acceptanceEligible=false`; workflow/restart and query/isolation dispatch passed; PG GC-domain snapshots converged and acknowledgments covered removed rows. Flags requested query standby, list partitions, durable restart, auto-config hint. Does not represent full acceptance.
| [40204660498](https://github.com/auenkr/cadence-rust/actions/runs/40204660498) | MySQL, two Rust Matching nodes; success | `cba8e6158bf08c061d962312b0a57091b26c450e21ab047be8b1d04e210b7203`; 0 / 0 / 0 | `preliminary-passed`, not acceptance eligible; restart survived Rust process replacement with durable task row/watermark and completed workflow. Two-node lane requested.
| [40195314790](https://github.com/auenkr/cadence-rust/actions/runs/40195314790) | MySQL probes; success | `69bff68ce86e52dc8fe91fbf8c5575d90e56096ef85158bd77ccc53cd1c04733`; 0 / 0 / 0 | `preliminary-passed`, not acceptance eligible; backlog forwarding root-only and child-only both passed; query and isolation dispatch passed; restart completed after process replacement. Requested query standby, list partitions, durable restart, auto-config hint.
| [40195809764](https://github.com/auenkr/cadence-rust/actions/runs/40195809764) | Thrift Matching + TChannel History; success | `aa23839fe0ae2464daeef70948cf891dd0f955c6d67d2e5db36fcf0cbc4d7eef`; 0 / 0 / 0 | `preliminary-passed`, not acceptance eligible; workflow/restart, query dispatch and isolation dispatch passed; auto-config hint requested. Transport fields confirm `thrift` / `tchannel`.
| [40196298460](https://github.com/auenkr/cadence-rust/actions/runs/40196298460) | Cassandra mixed gate; success | `38e23cbd37f78df2284ceef55fde99ab8e2ce01fb87ecab96ce1f5c390e6f2fa`; 0 / 0 / 0 | `passed`, `acceptanceEligible=true`; Cassandra lane completed, restart recovered and query/isolation dispatch passed. This is the listed Cassandra acceptance-eligible run.
| [40196794076](https://github.com/auenkr/cadence-rust/actions/runs/40196794076) | Active offer; success | `bd5dd861c4d2f022591f237f33b2b2c919cf5220938f1c13e585fa9034bf00cc`; 0 / 0 / 0 | `preliminary-passed`, not acceptance eligible; query and isolation dispatch passed. `backlogForward` and additional probe scenarios were not requested.
| [40197288308](https://github.com/auenkr/cadence-rust/actions/runs/40197288308) | HTTP TLS interoperability; success | `a4c2abc02c70e45b05b538bd6a22e8dfc077092745b4e7901187e1bb59217df4` (`cadence-server`); build / interop 0 / 0 | Rust server built and Go HTTP/TLS interop exited 0. This artifact has no `commit.txt`; run metadata links the run, but binary is not the Matching executable.
| [40197629016](https://github.com/auenkr/cadence-rust/actions/runs/40197629016) | Configstore; success | `628f5bb536c222f3aeb46032a55c681a201a5d0f9c91bfc7f8f7be3df9732f8b`; 0 / 0 / 0 | `preliminary-passed`, not acceptance eligible; configstore empty poll elapsed 2021ms against seeded 2000ms expiry, query/isolation dispatch passed, and restart completed after process replacement. Configstore lane requested.

“Probe exit” above is `gate/run/run.exit`; other artifacts also contain `gate.exit` and where present `gate/lane.exit`, all 0 for these successful gate runs. SHA/exit values are copied from run artifacts, not inferred from workflow conclusion.

## Shard-distributor run correction and limits

- [40205093375](https://github.com/auenkr/cadence-rust/actions/runs/40205093375) is **not** a Go-only preflight failure. Its artifact records exact target SHA, built Rust Matching (`1a167dca138969dec4799c7f3a1f621fa4efff92f6855492b056cc3e8996d0bb`, build exit 0), and topology Go Frontend/History/Worker with Rust Matching via production Ringpop. The mixed probe did execute; `results.json` has `acceptanceEligible=false`, and reports the failed shard-distributor after-SIGKILL executor-expiry assertion within 90 seconds. Thus the run exercised Rust Matching but failed recovery validation; it is neither a pass nor evidence of a preflight-only failure. The artifact's `gate.exit` is 1. These exit values were rechecked directly after integration; the worker originally confused a failure value with the build exit. Treat as an actual unresolved mixed-gate failure, not a Rust-independent Go baseline issue.
- [40198127804](https://github.com/auenkr/cadence-rust/actions/runs/40198127804) was a separate earlier shard-distributor failure; do not generalize it as matching-lane preflight without inspecting its own artifact.

## Completion status / limits

The successful mixed runs show specific transport, persistence, SDK dispatch, forwarding, restart, configstore, and Cassandra scenarios; most report `acceptanceEligible=false`. Shard-distributor SIGKILL expiry remains failed. Full parity audit, broader SDK/service interactions, failure/recovery coverage, and pending clean-target workspace CI result remain outstanding. These artifacts do **not** establish Matching completion or permission to start History migration.
>>>>>>> dabefd55a (docs: correct matching mixed-gate evidence)
