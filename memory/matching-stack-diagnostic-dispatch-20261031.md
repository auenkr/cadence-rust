
## Result of run 41114430171 (recorded 2026-10-30 20:30Z)

Terminal failure at `7b46ef884`. The failure was NOT the readiness stall: the `Mixed Go and Rust validation` step failed in `tests/run-go-service-baseline.py:1305` ("Go baseline probe failed; see out/gate/run/probe.log") after pulling postgres:13, before Rust Matching started. Artifact download returned HTTP 503 "Egress is over the account limit", so probe.log is unavailable; only the actions log is saved at `/mnt/storage/service/ci-evidence/matching-postgres-41114430171.uerafL/actions.log`. No GDB/stack evidence was produced. Not credited as acceptance. A rerun (41123777022) was cancelled because another gate (41123635434, tag validation/matching-map-value-396ca859) was already running; lanes must not overlap. Next: review 41123635434, then rerun postgres diagnostic when idle.
- 2026-10-30T20:31:22Z: my dispatch of 41123777022 cancelled concurrent run 41123635434 via concurrency group (my mistake). Redispatched postgres lane on validation/matching-map-value-396ca859 (reentrancy fix + map-value fix).
# Matching stack diagnostic dispatch

Fresh package-unset workspace CI https://github.com/auenkr/cadence-rust/actions/runs/41112992201 is terminal success at `7b46ef884c7b33f1a57f29a1e1ac4055adfb8dc6`. Downloaded artifacts `/mnt/storage/service/ci-evidence/matching-workspace-41112992201.TFeIkR/` record that exact SHA, target `/home/runner/work/_temp/cargo-target-final`, clippy.exit=0, test.exit=0, matching-runtime-format.exit=0 and fmt.exit=0.

With no Actions job in progress, pushed immutable tag `validation/matching-stall-stack-7b46ef884` and dispatched matching-production-validation.yml, lane=postgres, matching_gdb_diagnostic=true:
https://github.com/auenkr/cadence-rust/actions/runs/41114430171

This rerun is intended to discriminate the readiness stall from41108824183 using bounded per-thread syscall/kernel-stack samples, socket PID/fd attribution and GDB thread backtraces. GDB has frame arguments disabled, no locals requested, a20s timeout, and clipped output. It briefly stops Matching; do not automatically credit this as undisturbed service acceptance. Review actual artifacts, binarySHA256, nested exits, gdb attach outcome and stack evidence before deciding a source fix. No unique cause is yet proved. No other mixed lane may overlap. Matching remains incomplete;0/10 mixed lanes accepted.
