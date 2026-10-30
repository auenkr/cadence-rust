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

## In progress

PostgreSQL mixed gate:
https://github.com/auenkr/cadence-rust/actions/runs/41108824183

Confirmed live at the exact frozen SHA, with no artifact available at this checkpoint. No current-source mixed lane is credited as passed. Earlier readiness failures do not establish a unique runtime cause, and the StatsD correction is not claimed to explain them.

## Remaining work

1. Review PostgreSQL terminal artifacts: source and binary SHA256, nested exits, Go Frontend/History/Worker with Rust Matching and no Go Matching, SDK workflows, exact histories, owner crash/takeover, restart participation, durable range growth and zero-row drain, and cleanup.
2. Resolve any demonstrated defects, rerunning fresh workspace validation after source changes.
3. Pass all ten serial mixed lanes on one validated immutable source: PostgreSQL, MySQL probes, Thrift/TChannel, configstore, real shard-distributor/etcd, two-node MySQL, Cassandra, full adaptive, active offer, production TLS. None is currently accepted on this source.
4. Close the final Go-to-Rust behavior/parity audit and requirement-to-evidence matrix; preserve reproducible evidence in memory; integrate accepted code into master. Only then begin History.

An honest percentage is unavailable: code and unit evidence cover much of Matching, but service completion depends on mixed runtime acceptance. Current validation milestone counts are fresh workspace 1/1 passed and mixed lanes 0/10 accepted (PostgreSQL running). The overall cluster rewrite remains incomplete with History, Frontend and Worker still to follow Matching.
