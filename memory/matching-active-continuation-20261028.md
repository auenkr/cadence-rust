# Matching active continuation — 2026-10-28

Matching remains incomplete; History migration must not start. Source base is
`9e21e168aacf504f4895853820877748708fc2b6`. Read the FINAL RESUME POINT in
`matching-session-handoff-20261028.md` for the saved implementation contract.

The user now requests **three parallel workers**, using Codex GPT-6.1 Terra
for hard work and Luna for simple work. Three isolated Terra workers were
started with high reasoning and default service tier. Each owns a separate
integration area: launcher/assembly; runtime/shard ownership/lifecycle; and
registry/weak local forwarding/metrics. Their changes are not yet integrated
or validated. No replacement workers or overlapping parent implementation
should be started while these tasks are running. T3 task completion wakes the
parent; do not poll running tasks.

Fresh six-crate clean-target CI was dispatched on the source base:
https://github.com/auenkr/cadence-rust/actions/runs/40727183000 . At inspection
2026-10-28 16:35 UTC its head matched the base and the job was in progress at
the Go raw-string oracle, before Clippy/tests. Conclusion remains pending.
This is a baseline check, not native production activation evidence or final
workspace validation. Runner formatting is enabled; apply its artifact patch
and rerun validation on the resulting final source when appropriate.

## Verified acceptance setup

The existing workflows retain fresh-target binary SHA256 and drive original
Go Frontend/History/Worker with Rust Matching. Source inspection of
`tests/run-go-service-baseline.py` confirms production mode excludes Go
Matching from the Go service command and records both service lists and
`goMatchingStarted`. This is source evidence only; final artifacts must prove
the processes and scenario outcomes actually used.

The final lane map remains
`matching-acceptance-evidence-readiness-20261027.md`, with the corrected
two-node MySQL args in `matching-v7-two-node-dispatch-review-20261027.md`.
It covers PostgreSQL, MySQL probes, Thrift/TChannel, configstore, real shard
distributor, two-node MySQL with both Prometheus endpoints and cold-domain
quota checks, Cassandra, two-node adaptive scaling, active offer and TLS.
The workflow defaults for two-node MySQL omit Prometheus/cold-domain checks;
use the documented complete explicit argument list, retaining restart and
failure recovery because extra_args replaces defaults.

V7 lane passes predate the raw-byte native migration. They remain historical
support and cannot validate the new native production path. After integrating
the native implementation, freeze a new candidate and run fresh uncached
workspace Clippy followed by workspace tests, then mixed gates serially.
Every credited lane needs matching source head/artifact identity, binary
SHA256, Go peers/no Go Matching, SDK and lane-specific assertions, actual
restart/failure/recovery outcomes and cleanup evidence. Review artifacts;
requested flags or a green job alone are insufficient.

No local Cargo, rustfmt, Go validation or mixed runtime was executed in this
continuation. Parent work so far is source/evidence review and orchestration.
