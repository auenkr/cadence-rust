# Matching readiness process-evidence capture (opt-in, CI-only)

Candidate branch: `t3/matching-proc-evidence-consolidated`. Base: `be7016ef2d04dcb00fda946ddb67e3fbf15bae09` (`origin/t3/matching-acceptance-shutdown-consolidated`). Process-evidence source: `93deac8eb07d8def9cf037b0b4a0a851587e8e86`; workflow-check source: `d7c80954ce188a84050009033b2ecb5c4cb47a4b`. Both source commits were cherry-picked onto the pinned base; their original ancestry was not imported. The candidate preserves those changes and adds sustained-failure qualification plus final-failure fallback.

## Purpose
Failed run 41094318642 showed TCP connect in about 1 ms while grpc-js stayed CONNECTING for 10 s, plus a SIGTERM hang. The 32-record timeline and 8-PING caps mean that trace is partial, so it does not prove a stall. This hook separates a process-wide stall from a listener-only problem.

## Mechanism
`CADENCE_MATCHING_PROC_EVIDENCE=1` enables a hook in `waitForRustReady` (`tests/matching-service-compat.cjs`). It ignores startup failures until at least 20 seconds into a readiness wait, then captures at the first failed direct `ListTaskListPartitions` RPC. If no evidence was captured, it makes a best-effort capture at final readiness failure (including early process exit). The original readiness error and acceptance assertions remain unchanged. Implementation is `tests/matching-process-evidence.cjs`. It writes `rust-process-evidence-<label>.json` into the probe work dir, and `tests/matching_evidence.py` copies it into the evidence bundle. PostgreSQL sets the opt-in variable; no GDB variable is set.

Contents:
- Rust pid thread table (cap 64) from `/proc/<pid>/task/*/{comm,wchan,stat}`, sampled twice 500 ms apart so `cpuTicksDuringGap` shows spinning versus blocked.
- Selected `/proc/<pid>/status` lines.
- `ss -ltn` (listener Recv-Q/Send-Q) and `ss -tn` filtered to the gRPC port.
- Optional `gdb -p <pid> -batch 'thread apply all bt 12'`, only when `CADENCE_MATCHING_PROC_EVIDENCE_GDB=1` and gdb is installed.

Bounds: fixed argv with no shell, 3 s timeout per `ss`, 20 s for gdb, SIGKILL on timeout, output clipped (8 KiB, gdb 24 KiB). Nothing captures environment or command lines. No heartbeats or periodic logging were added.

## Not changed
RPC readiness criteria, retries, deadlines, SDK assertions, production code and crash-scenario sections are untouched. Failures inside the hook are swallowed. The PostgreSQL workflow lane enables the hook. Workflow preflight source paths were reviewed on the integrated candidate.

## Limitations
- GDB remains opt-in in the helper and is not enabled by the workflow.
- The snapshot comes after the 10 s RPC deadline, so a transient stall could already have cleared. Sampling at this point is a single moment, not a trace.
- At most one successful capture is retained per readiness wait; a failed capture is retried at final failure.
- `wchan` may read `0` or be unavailable on some kernels. Without debug symbols, gdb frames can be unsymbolized.
- UNVALIDATED: no Cargo, Go, Node, formatting, build, test, runtime validation, CI dispatch, or CI polling was run, as requested. Review was source-only plus `git diff --check`. Capture runs only on GitHub Actions standard runners.
