# Matching progress checkpoint

Matching remains incomplete. History migration has not started. This checkpoint saves current work on master without promoting the unaccepted implementation candidate.

## Saved source

- Frozen candidate: `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`, branch `t3/matching-statsd-integrated-validation`, immutable tag `validation/matching-statsd-2c3bc4a9`.
- Checkpoint branch: `t3/matching-statsd-integration-checkpoint`; merged current origin/master (`95f51849e`) as `4e4fd4efb`, pushed. Master's intervening changes were memory documents only; the frozen validation tag is unchanged.
- Candidate contains native Matching implementation and required persistence/transport/discovery/config/lifecycle integration, stronger owner-aware crash/restart/queue-drain assertions, bounded readiness/HTTP2/process/shutdown diagnostics, and StatsD bounded FIFO sender parity fixes. Implementation presence is not a service acceptance claim.

## Verified

Fresh package-unset workspace validation succeeded:
https://github.com/auenkr/cadence-rust/actions/runs/41107169881

Exact head: `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`. Artifacts record fresh target `/home/runner/work/_temp/cargo-target-final`, Clippy exit 0 followed by workspace tests exit 0. Both StatsD failed-packet/FIFO and real sink Drop regression tests passed. Required runtime formatting checks passed; informational workspace/TLS/query formatting checks report differences. Go raw-string and DescribeTaskList oracles passed at the same SHA.

Local artifact directory: `/mnt/storage/service/ci-evidence/matching-workspace-41107169881.eL8mkx/`. No local Cargo/Go/Node validation was run.

## Latest mixed result

PostgreSQL mixed gate:
https://github.com/auenkr/cadence-rust/actions/runs/41108824183

Terminal conclusion: failure at the exact frozen SHA. Build exit 0; probe/run/lane/gate exit 1. Readiness failed before SDK or recovery scenarios, and shutdown exceeded the harness grace deadline before SIGKILL. No current-source mixed lane is credited as passed. Diagnostic syntax and observer selfcheck succeeded; the completed Actions log records Go SDK compile-only output `[no test files]` and subsequent steps. Those preflights do not establish runtime acceptance. Earlier readiness failures do not establish a unique runtime cause, and the StatsD correction is not claimed to explain them.

Compact evidence is committed on master under `memory/evidence/matching-postgres-41108824183/`; see `matching-postgres-failure-41108824183-20261031.md`. Full Actions log is downloaded at `/mnt/storage/service/ci-evidence/matching-postgres-41108824183.AY5Gf8/actions.log`. Three independent high-reasoning Luna analyses are saved as matching-readiness-stall-41108824183.md, matching-postgres-readiness-diagnosis-41108824183.md and matching-postgres-readiness-shutdown-boundary-20261031.md. They establish no unique source cause. A separate Luna implementation task is preparing bounded syscall/kernel-stack/socket attribution and optional GDB diagnostics; no replacement gate is dispatched yet.

Reconciliation: the successful native request precedes the captured READY-but-timeout RPC by about 25 seconds. All sampled threads wait on futexes with zero CPU ticks over 500 ms; the listener has queued connections. This is compatible with a wider progress failure but does not prove a lock cycle. The failed workflow already enabled CADENCE_MATCHING_SHUTDOWN_TRACE; its absent shutdown markers are an observed gap, not a missing workflow flag to fix by reenabling it. Go's later empty Matching ring is a discovery discrepancy, not proof that Ringpop caused the direct transport stall.

## Remaining work

1. Review PostgreSQL terminal artifacts: source and binary SHA256, nested exits, Go Frontend/History/Worker with Rust Matching and no Go Matching, SDK workflows, exact histories, owner crash/takeover, restart participation, durable range growth and zero-row drain, and cleanup.
2. Resolve any demonstrated defects, rerunning fresh workspace validation after source changes.
3. Pass all ten serial mixed lanes on one validated immutable source: PostgreSQL, MySQL probes, Thrift/TChannel, configstore, real shard-distributor/etcd, two-node MySQL, Cassandra, full adaptive, active offer, production TLS. None is currently accepted on this source.
4. Close the final Go-to-Rust behavior/parity audit and requirement-to-evidence matrix; preserve reproducible evidence in memory; integrate accepted code into master. Only then begin History.

An honest percentage is unavailable: code and unit evidence cover much of Matching, but service completion depends on mixed runtime acceptance. Current validation milestone counts are fresh workspace 1/1 passed and mixed lanes 0/10 accepted (PostgreSQL failed readiness). The overall cluster rewrite remains incomplete with History, Frontend and Worker still to follow Matching.

## Latest saved checkpoint (2026-10-31)

Synced the checkpoint branch with origin/master; preserved all unaccepted implementation branches and frozen validation tags. This master update saves progress only.

- Latest validated source: `7b46ef884c7b33f1a57f29a1e1ac4055adfb8dc6`, immutable tag `validation/matching-stall-stack-7b46ef884`.
- Fresh package-unset workspace CI succeeded: https://github.com/auenkr/cadence-rust/actions/runs/41112992201 . Artifacts confirm dedicated fresh target, Clippy exit 0 then workspace tests exit 0, and required formatting checks exit 0.
- PostgreSQL diagnostic run https://github.com/auenkr/cadence-rust/actions/runs/41114430171 remains in progress at that exact source as checked during this save. Optional bounded GDB is enabled with frame arguments disabled. Its result is not yet known; diagnostic interruption must not automatically count as undisturbed acceptance.
- Dynamic-config and Ringpop source audits found no proven lock cycle. No readiness cause or speculative production fix is claimed.
- Matching remains incomplete: workspace validation 1/1 passed; current-source mixed lanes 0/10 accepted. Implementation is substantial, but no defensible overall completion percentage is available.
- Next: collect the diagnostic run artifacts, identify the readiness/shutdown stall from evidence, fix any demonstrated defect, repeat fresh workspace CI after changes, then pass all ten serial mixed lanes on one final immutable source and close the parity audit. History, Frontend and Worker remain subsequent migrations.
