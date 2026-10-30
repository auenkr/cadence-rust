# Matching stack diagnostic dispatch

Fresh package-unset workspace CI https://github.com/auenkr/cadence-rust/actions/runs/41112992201 is terminal success at `7b46ef884c7b33f1a57f29a1e1ac4055adfb8dc6`. Downloaded artifacts `/mnt/storage/service/ci-evidence/matching-workspace-41112992201.TFeIkR/` record that exact SHA, target `/home/runner/work/_temp/cargo-target-final`, clippy.exit=0, test.exit=0, matching-runtime-format.exit=0 and fmt.exit=0.

With no Actions job in progress, pushed immutable tag `validation/matching-stall-stack-7b46ef884` and dispatched matching-production-validation.yml, lane=postgres, matching_gdb_diagnostic=true:
https://github.com/auenkr/cadence-rust/actions/runs/41114430171

This rerun is intended to discriminate the readiness stall from41108824183 using bounded per-thread syscall/kernel-stack samples, socket PID/fd attribution and GDB thread backtraces. GDB has frame arguments disabled, no locals requested, a20s timeout, and clipped output. It briefly stops Matching; do not automatically credit this as undisturbed service acceptance. Review actual artifacts, binarySHA256, nested exits, gdb attach outcome and stack evidence before deciding a source fix. No unique cause is yet proved. No other mixed lane may overlap. Matching remains incomplete;0/10 mixed lanes accepted.
