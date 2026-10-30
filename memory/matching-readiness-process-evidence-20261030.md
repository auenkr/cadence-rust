# Matching readiness process-evidence capture (opt-in, CI-only)

Candidate branch: `t3/matching-readiness-proc-evidence`. Base: `d0ef6bf3a4fb8056b22a0b10d56ccd16e8f495c0` (`origin/t3/matching-rpc-callback-timestamp`).

## Purpose
Failed run 41094318642 showed TCP connect in about 1 ms while grpc-js stayed CONNECTING for 10 s, plus a SIGTERM hang. The 32-record timeline and 8-PING caps mean that trace is partial, so it does not prove a stall. This hook separates a process-wide stall from a listener-only problem.

## Mechanism
`CADENCE_MATCHING_PROC_EVIDENCE=1` enables a hook in `waitForRustReady` (`tests/matching-service-compat.cjs`). It fires once per readiness wait, at the first failed direct `ListTaskListPartitions` readiness RPC, before the 180 s loop continues. Implementation is `tests/matching-process-evidence.cjs`. It writes `rust-process-evidence-<label>.json` into the probe work dir, and `tests/matching_evidence.py` copies it into the evidence bundle.

Contents:
- Rust pid thread table (cap 64) from `/proc/<pid>/task/*/{comm,wchan,stat}`, sampled twice 500 ms apart so `cpuTicksDuringGap` shows spinning versus blocked.
- Selected `/proc/<pid>/status` lines.
- `ss -ltn` (listener Recv-Q/Send-Q) and `ss -tn` filtered to the gRPC port.
- Optional `gdb -p <pid> -batch 'thread apply all bt 12'`, only when `CADENCE_MATCHING_PROC_EVIDENCE_GDB=1` and gdb is installed.

Bounds: fixed argv with no shell, 3 s timeout per `ss`, 20 s for gdb, SIGKILL on timeout, output clipped (8 KiB, gdb 24 KiB). Nothing captures environment or command lines. No heartbeats or periodic logging were added.

## Not changed
RPC readiness criteria, retries, deadlines, SDK assertions, production code and crash-scenario sections are untouched. Failures inside the hook are swallowed. The workflow file is not edited. A gate run must set the env var(s); that is for a later dispatch.

## Limitations
- gdb attach briefly stops the Rust process and may be blocked by ptrace_scope. A hit or miss there is itself evidence, but it may perturb timing, so enable it as a second run if needed.
- The snapshot comes after the 10 s RPC deadline, so a transient stall could already have cleared. Sampling at this point is a single moment, not a trace.
- Only the first failure is captured per readiness wait.
- `wchan` may read `0` or be unavailable on some kernels. Without debug symbols, gdb frames can be unsymbolized.
- UNVALIDATED: no Cargo, Go, node syntax check, runtime or CI was run, as requested. Capture runs only on GitHub Actions standard runners.

Source SHA: `93deac8eb07d8def9cf037b0b4a0a851587e8e86`
