# Matching final acceptance audit — 2026-10-26

## Scope and result

Read-only source and evidence audit from `origin/master` / worktree base
`a9e215ddf811a898985272e196f27b8a549c6953`. Compared the Matching RPC
matrix, final gate plan, Go Matching config/handler sources, current ignored-test
annotations, mixed-gate workflows/probes, and completed TLS and active-offer
artifacts. No local Cargo, Go, or formatting commands were run; no gate was
dispatched. No additional production mismatch was established beyond the
already-known SQL writer fatal lifecycle work. Matching is not accepted and
must not advance to History.

## Requirement-by-requirement residuals

| Requirement | Evidence and scope | Remaining action |
|---|---|---|
| All 13 Matching gRPC RPCs; the 10 legacy Thrift procedures | `memory/matching-final-rpc-acceptance-matrix-20261025.md` traces each RPC to Go `go/service/matching/handler/{handler,engine}.go`, Rust `grpc_matching.rs`, and active focused tests. The three new RPCs (partition config update/refresh and semaphore acquire) are gRPC-only in Go too. Existing focused and mixed SDK evidence is meaningful for the tested behavior; it need not be repeated as a direct mixed oracle for every unit branch. | Keep the matrix's explicitly listed optional-field/error-precedence limits as coverage limits; no concrete RPC implementation mismatch found in this audit. Run all required mixed lanes against one final source/binary SHA before sign-off. |
| Production persistence and failure/recovery | Final plan covers MySQL, PostgreSQL, Cassandra, configured dynamic-config store, shard distributor, Thrift/TChannel, multi-node ownership, process restart, graceful drain, crash recovery, and active offer. These are separate scenarios; a green workspace job cannot replace them. | Re-run serially on the final candidate and inspect the scenario files, source SHA, exits, and binary SHA for every lane. Prior green runs are bound to older SHAs. |
| Production gRPC TLS | Run [40364035990](https://github.com/auenkr/cadence-rust/actions/runs/40364035990) succeeded on `01148d6cf9c1e18a26e1e3385337d5f0c53bd534`; binary SHA256 `02f03ea46f3bad48bad63bf07d7ecefca69093d046a3a71dc0839ce5600e536c`. Artifact confirms `goMatchingStarted:false`, Rust Matching plus Go Frontend/History/Worker, exit 0, and the mixed workflow. Source `tests/run-go-service-baseline.py:585-657,1090-1091` creates the CA, configures Go `caFile` + `enableHostVerification:true` + `serverName:localhost`, configures Rust's Matching listener identity, and points the Go typed ownership oracle at the CA. Its emitted result is still `preliminary-passed` / `acceptanceEligible:false`. | Repeat on the final candidate SHA. This closes the prior missing production TLS lane for its recorded SHA. The separate `matching-http-tls-interop.yml` is a Rust History fixture with a Go client, not Matching TLS acceptance. |
| Active offer, task insertion, restart and crash recovery | Run [40361176805](https://github.com/auenkr/cadence-rust/actions/runs/40361176805) succeeded on `12a9a12cfb1cc72a04a2fd3443a8ce964b53cc8c`; binary SHA256 `6552269110cac7eae2f86aafc3d6a231dbfa4bc682e8405f7b68b3dd5213d770`. Artifact has Go Matching absent, active-offer result `passed`, History scheduled task and token delivered, zero persisted rows/inserts; it also records a process restart, graceful drain, idle eviction, and two-node crash recovery. | Repeat on final candidate SHA. Artifact label remains `preliminary-passed` / `acceptanceEligible:false`; treat as scenario evidence, not a standalone sign-off. |
| SQL malformed RunID fatal exit and shutdown race | The reviewed race branch `t3/matching-writer-shutdown-race` has focused fresh-crate CI `40360696410` for deterministic writer-tracker ordering. Its first regression was synthetic. The worker subsequently pushed `t3/matching-writer-shutdown-regression` at `e08e41d1b391d861275008d8efff1a36fd2bdfda`; this changes the test to invoke a real `TaskWriter` append with malformed SQL RunID. Focused run `40364944856` was still in progress during this audit. | Wait for `40364944856` to finish and inspect its test artifact. Its test exercises an actual malformed append but does not hold a real SQL append blocked while SIGTERM races; keep the deterministic tracker ordering test as complementary evidence. Then combine the reviewed lifecycle fix with current master and run the final gates. |
| Adaptive scaler deployment configuration and workflow interaction | Go exposes `matching.enableAdaptiveScaler` and related tuning properties in `go/service/matching/config/config.go`; Rust has matching dynamic readers and extensive scaler algorithm/lifecycle tests in `matching_scaler_go_tests.rs`, plus the manual-update guard in `matching_engine_go_tests_b.rs`. The mixed workflows/config fixtures had not enabled the scaler. `memory/matching-dyncfg-key-matrix-20261023.md` records 59 dynamic keys/defaults/filters matching, but does not prove the live production registry-to-scaler wiring. Rust's scaler reads local/peer Matching `DescribeTaskList` results and persists changes via `update_partition_config` (`matching_registry.rs:1712-1800,1883-1938`). | Production probe is implemented and pushed at `12e7ebdaa46f070a86fd65df1aa9d242c437e2b4`, on the existing `matching-production-validation.yml` workflow's `adaptive-scaler` lane. It requires an increased persisted partition-config version and write-partition count, verifies Go Frontend partition listing, then completes an existing Go SDK workflow and activity on the changed queue. Dispatch only in the final serial plan after authorization; exact command and pending evidence are in `memory/matching-adaptive-scaler-production-probe-20261026.md`. |
| Required configuration inventory | `go_new_config_every_field_has_a_rust_reader` remains ignored in `tests/matching_partial_upgrade_a.rs:80` because Rust has no Go-style `NewConfig` aggregate. The dynamic-config matrix provides a source-level comparison for 59 named dynamic keys; the ignored test does not itself prove a missing runtime field. Spot-tracing the static values in `go/service/matching/config/config.go` found Rust counterparts (range defaults in the queue/reader path, host identity from the process, RPC through shared config, task-dispatch limiter, and deletion cadence). | No concrete missing deployment field found. The ignored aggregate-table test is a structural test-shape limitation. Preserve the existing per-reader/source evidence; only add a focused config assertion if later source tracing exposes a particular field with no Rust consumer. |
| Ignored test claims | `matching-ignored-rpc-audit-20261025.md` correctly scopes non-durable `DescribeTaskList` task-id/empty fields to Rust-only no-config/in-memory constructors; configured launcher paths use durable queues. The ignored live Go-History, TChannel, MySQL/Postgres/Cassandra and shard-distributor tests have corresponding production/mixed lanes. Several ignored private-manager/fault seams (query dispatch error, update-config manager error, failover closures, owner-loss subcases, generic `handleErr`) are test-shape limitations; active RPC tests/source traces cover normal production paths. | Retain these as testability/branch-coverage limits unless a source trace identifies a caller-visible divergence. Do not count ignored annotations alone as missing behavior. |

## Can the current serial plan establish completion?

Not as written and not with existing green runs. The plan's runs are on distinct
SHAs, and the fatal patch plus real-writer regression changes the final source.
The TLS result is on `01148d6`, active-offer on `12a9a12`, and the fatal race
fix/real-writer regression are later separate branch heads. The race branch's
diff from current `origin/master` removes the production TLS workflow and gate
updates, so it cannot be used as the final pin without carrying those current
files forward. Assemble a candidate from current `origin/master` plus the
reviewed fatal commits after their focused CI passes; preserve the TLS and
active-offer workflows. Pin that immutable SHA, then run fresh uncached
workspace Clippy/tests followed serially by PostgreSQL, MySQL probes, Thrift,
configstore, shard distributor, two-node MySQL, Cassandra, active-offer, and
production TLS gates. Inspect every artifact and binary hash. The HTTP/TLS
History fixture can remain supplemental; it does not replace any Matching
lane.

Even that sequence does not include the adaptive-scaler production wiring
assertion above. Add it to an existing mixed lane or add one Actions lane before
calling the service complete. Keep emitted preliminary eligibility labels
unchanged; record the reviewed scenario evidence and limitations separately.

## Source pointers

- Go Matching RPC definitions: `go/proto/internal/uber/cadence/matching/v1/service.proto:44-102`.
- Go RPC handlers/engine: `go/service/matching/handler/handler.go:146-552`, `go/service/matching/handler/engine.go:400-1692`.
- Go deployment config: `go/service/matching/config/config.go:22-240`.
- Rust RPC/lifecycle/config: `rust/crates/cadence-matching/src/grpc_matching.rs`, `matching_runtime.rs`, `matching_registry.rs`, `matching_scaler.rs`, `launcher.rs`; shared config/TLS in `rust/crates/cadence-service-core/src/config.rs` and `grpc_tls.rs`.
- Gate inventory/serial commands: `memory/matching-final-gate-plan-20261025.md` and `memory/matching-acceptance-gate-gap-update-20261026.md`.
- Ignored-test scope: `memory/matching-ignored-rpc-audit-20261025.md`, `memory/matching-ignored-tests-result.md`.
