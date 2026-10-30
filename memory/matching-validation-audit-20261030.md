# Matching validation and source audit — 2026-10-30

## Gate checkpoint

Host check before work: load averages `0.85, 0.80, 0.89`; available memory
`4.4 GiB`. No local Cargo, Go, build, format, test, or runtime command was run.

The existing handoff is
`matching-current-validation-handoff-20261030.md` (commit
`7e42f465277b9dd7bd68d36cda88e75717ce6ca5`, branch
`t3/matching-postgres-gate-evidence`). I inspected PostgreSQL production gate
[`41056791529`](https://github.com/auenkr/cadence-rust/actions/runs/41056791529)
twice, once before and once after the source audit. Both observations were
`in_progress`, branch `validation/matching-postgres-5ee69b5`, exact head
`5ee69b5572688c2dba2245c7b411c7182c5b9b16`, updated
`2026-10-30T12:37:58Z`. The `gate` job remains at `Build fresh Rust Matching`;
`Probe dependencies` and `Mixed Go and Rust validation` are pending. There are
no terminal artifacts to review yet. The immutable base tag remains
`validation/matching-postgres-5ee69b5`.

The prior fresh workspace run [`41055162424`](https://github.com/auenkr/cadence-rust/actions/runs/41055162424)
passed at the base SHA: 1,829 passed, 28 ignored, Clippy and tests exit 0.
That evidence applies to the base only. Supplemental candidate
`565c8fc91893fae1a81cd8e47f10f2ee4744c046` on
`t3/matching-queued-activity-candidate-5ee69b5` remains unverified and must not
be dispatched while gate 41056791529 is active.

## Production workflow audit

Reviewed `.github/workflows/matching-production-validation.yml` and
`memory/matching-acceptance-evidence-readiness-20261027.md`. The production
workflow offers eight `lane` values (`postgres`, `thrift`,
`shard-distributor`, `configstore`, `mysql-probes`, `adaptive`,
`adaptive-single-diagnostic`, `adaptive-go-control`); only the following ten
commands are the required mixed acceptance set. The supplemental TLS, active
offer and MySQL/Cassandra workflows are separate workflow files. Do not count
diagnostic/control choices as acceptance lanes.

All ten require exact frozen-source SHA and artifact commit, Rust binary SHA256,
all nested exits, Go Frontend/History/Worker with Rust Matching and no Go
Matching, actual SDK and scenario results, restart/failure/recovery evidence,
and cleanup evidence. The lane-specific checks and dispatches are:

| # | Lane | Exact command | Additional evidence to inspect |
|---|---|---|---|
| 1 | PostgreSQL | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=postgres` | PostgreSQL shared backend; GC/domain; standby query; `ListTaskListPartitions`; durable restart; auto-config hint; readiness error and direct RPC structured result/stage traces; SDK/recovery and container cleanup. |
| 2 | MySQL probes | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=mysql-probes` | Go-compatible schema, backlog forwarding, standby query, partitions, durable restart, auto-config hint, observed drain/recovery and cleanup. |
| 3 | Thrift | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=thrift` | Matching Thrift + History TChannel, SDK/query/isolation/config hints, restart, two-node recovery, idle recovery and cleanup. |
| 4 | Configstore | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=configstore` | Selected configstore mode and seed/config artifacts, SDK, restart, idle and two-node failure recovery, teardown. |
| 5 | Shard distributor | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=shard-distributor` | Real Go shard-distributor with runner-owned single-node etcd; assignment/routing, heartbeat TTL expiry, executor/assignment cleanup, restart, idle and two-node crash recovery. |
| 6 | Two-node MySQL | `gh workflow run matching-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f extra_args='--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3 --matching-exporter prometheus --matching-cold-domain-metric-probe'` | Two Rust Matching nodes; SDK, SIGTERM drain/recovery, owner SIGKILL, survivor takeover, persisted task/ownership assertions, MySQL cleanup. |
| 7 | Cassandra | `gh workflow run matching-cassandra-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f extra_args='--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3'` | Go schema shared by Go peers and Rust Matching; SDK, two-node owner kill/takeover, restart/drain, persisted task/semaphore owner+token checks, Cassandra cleanup. |
| 8 | Adaptive | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=adaptive` | Adaptive transcript/load partitions/scaler events, SDK, restart, two-node failure recovery, idle recovery and cleanup. Do not substitute diagnostic, control, or retry-only variants. |
| 9 | Active offer | `gh workflow run matching-active-offer-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"` | Go History schedules and delivers active offer; task/token and zero-persistence assertions; SDK, graceful drain/restart, two-node crash recovery, cleanup. |
| 10 | Production TLS | `gh workflow run matching-production-tls-mixed.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"` | Live verified Rust gRPC TLS listener: hostname, ALPN h2, CA/peer fingerprints, subject/issuer/SAN, sanitized config identity; SDK, restart/drain, two-node recovery, cleanup. |

For every mixed lane also retain `out/commit.txt`, `out/binary.sha256`,
`out/build.exit`, `out/gate.exit`, `out/command.txt`, nested run/probe
results/exits, and cleanup records. Dispatch these serially only after fresh
workspace CI passes for the same candidate. Required workspace command is
`gh workflow run rust-ci.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f clean_target=true`
with package unset; inspect exact head, fresh target, Clippy exit 0 followed by
tests exit 0, run URL and conclusion.

## Go source and ignored-test coverage

This is a static coverage audit, using
`memory/matching-current-ignore-reconciliation-20261027.md`,
`memory/matching-v6-ignored-tests-audit-20261027.md`,
`memory/matching-v7-final-acceptance-ledger-20261027.md`, and Go sources under
`go/service/matching/`. No production mismatch was established by those
reviews. Keep these concrete coverage gaps open; they are not claims of a
runtime defect:

1. **Query dispatch error:** Go `go/service/matching/handler/engine_test.go`,
   `TestRespondQueryTaskCompleted`, “failed to dispatch query task” injects
   `some error`. Rust `matching_engine_go_tests_b.rs` counterpart is ignored;
   current public query coverage has no dispatch-manager failure injection.
2. **Refresh failure returned by RPC:** Go engine refresh error test has no
   direct Rust public-RPC assertion. Rust cancellation/retry tests cover a
   different failure boundary (`matching_partition_cancellation_tests.rs`).
3. **Failover callback integration:** Go `handler/engine.go` registers and
   catches up domain failover callbacks. Rust
   `matching_runtime.rs::failover_callback_selection_matches_go_and_advances_baseline`
   covers selection/version logic, but no assertion yet proves production
   callback registration/catch-up releases eligible polls.
4. **Exhaustive typed-error and Thrift defaults:** Go
   `handler/handler_test.go` and `wrappers/thrift/thrift_handler_test.go`
   exercise broad mapping/default behavior. Rust has representative boundary
   mappings, but not the complete Go typed-error set across RPC boundaries or
   injected errors/default response values across each Thrift wrapper method.

Previously listed gaps corrected by the reconciliation include partition
update persistence failure/status, live poller description, owner lookup and
several installed shard-ownership branches. Do not repeat those as open
without new evidence. The ignored Go-compatible cases are at
`rust/crates/cadence-matching/tests/matching_engine_go_tests_b.rs` and
`matching_engine_go_tests_c.rs`; locate exact `#[ignore]` blocks there before
claiming they are still ignored at a later pin.

The broader Go coverage inventory is
`memory/matching-go-test-function-map-20261027.md`. It still requires a
function-level disposition for adaptive scaler, isolation balancer, task
completer, forwarder, reader, limiter, liveness, membership and engine
integration tests. This source map plus the four confirmed gaps is audit
work, not full parity closure.

## Next action

Wait for gate 41056791529 to become terminal, then inspect/download its small
artifact exactly once. Review original readiness error, structured direct
`ListTaskListPartitions` result and traces, build/binary SHA, nested exits,
SDK/restart/recovery assertions, Go Matching absence and cleanup. Fix source
only for a concrete Go-compatible defect, then run fresh package-unset
workspace CI with `clean_target=true` before another mixed gate. If no defect
is established and the gate is terminal, dispatch that same fresh workspace
CI on `565c8fc91893fae1a81cd8e47f10f2ee4744c046`; do not move the frozen base
tag. No supplemental mixed gate may overlap the current one. Do not start
History migration.
