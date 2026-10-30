# Matching gRPC diagnostic gate inspection (2026-10-30)

## Reviewed candidate

- Mixed run: https://github.com/auenkr/cadence-rust/actions/runs/41094318642
- Workflow ref: `validation/matching-grpc-diagnostic-6c73d9aa`
- Immutable head: `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`
- The candidate is descended from queued-activity recovery commit
  `565c8fc91893fae1a81cd8e47f10f2ee4744c046`. Its only diff from that parent
  is `tests/matching-service-compat.cjs` and the diagnostic note. The Rust
  implementation and acceptance assertions are unchanged.

## Source and acceptance review

The direct `ListTaskListPartitions` diagnostic records its gRPC target and
resolver mode, channel connectivity state, a TCP connect probe bounded to one
second, proxy variable names (without values), `NO_PROXY` presence, and TLS
credential mode. It is collected alongside the RPC outcome and response. This
adds diagnostic evidence without changing readiness or acceptance conditions.

The PostgreSQL workflow builds a fresh `cadence-matching` binary, writes the
candidate commit and its `binary.sha256`, then runs the mixed Go/Rust lane with
process restart, failure recovery, idle eviction/recovery, PostgreSQL GC/domain
probes, query standby, partition readiness, durable restart, and auto-config
hint scenarios. The artifact must be checked for the exact commit and server
binary SHA256; build/probe/run/lane/gate exits; the direct readiness
`transportDiagnostic`; SDK result logs; restart and queued-task recovery
checkpoints; service shutdown/cleanup records; and `acceptanceEligible`. The
Rust oracle SHA from workspace CI is not the server binary SHA.

## One inspection of the active mixed run

At `2026-10-30 17:02:31 UTC`, GitHub reported run `41094318642` as
`in_progress`, head SHA exactly
`6c73d9aa447171d4eeda37d71dd0a955d0996a9b`. The `Build fresh Rust Matching`
and `Probe dependencies` steps had succeeded. `Mixed Go and Rust validation`
was `in_progress`; artifact upload was still pending. No mixed-gate outcome,
server SHA, nested exits, diagnostic values, SDK result, restart/recovery
result, or cleanup result is available yet. No transport cause is inferred.

The workflow's `matching-production-postgres` artifact is therefore not
downloaded while the run is active. On terminal completion, download it once
into a unique, initially empty directory and inspect the evidence listed above
before deciding whether a behavior-preserving fix is supported. If the run
fails, any source change requires a new clean-target, package-unset workspace
CI pass before another mixed run. If it passes, verify every assertion and
scenario before advancing the existing serial Matching lane plan. Matching
remains incomplete; this run alone does not establish parity or authorize
History work.
