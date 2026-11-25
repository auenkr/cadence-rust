# c1 permit burstiness: Go vs Rust queue v2 transfer limiter (2026-11-25)

Branch `perf-c1-burst`, base `a26ac0ec2`. No product code change.

## Run

[Traced paired benchmark 45601343763](https://github.com/auenkr/cadence-rust/actions/runs/45601343763)
(head `a26ac0ec2e8c2869a6849a6e54ee6c9971ca7efd`; `source_ref=master`, `control_ref=origin/master`,
`rust_services=frontend,matching,history`, c1, 3 repetitions x 1000, `trace_decision=true`, all arms interleaved).
An earlier dispatch (45600985642) failed at the control checkout because `control_ref=master` is not accepted; use `origin/master`.
Runner: Azure `AMD EPYC 7763`, 4 vCPU, 15 GiB, Ubuntu 26.04.1, kernel 7.0.0-1020-azure (artifact `machine-spec.md`).

Task 1 needed no new code: `tests/frontend-gate/benchmark/go-queuev2-trace.patch` (applied only when `trace_decision=true`) logs a
per-permit `DTRACE queuev2 load wait_ms` with shard id in Go; Rust logs the same with `limiter=<ptr>` (one limiter per shard).
Analysis script: transfer queue only, measured window only, request time = log time - wait. "control" is master Rust (equals the rust arm here).

## Results (per pair; 4 shards each)

| pair | arm | wf/s | permits/wf | permits/s/shard | inter-arrival p50 ms | p5 ms | gaps <5 ms | permits that waited (>1 ms) | waited p50 / p95 ms | max requests in any 1 s (shard) | wf p50 / p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Go | 21.3 | 3.00 | 15.9 | 23 | 8 | 0% | 2.3% | 35 / 50 | 39 | 45 / 56 |
| 0 | Rust ctl | 24.6 | 2.96 | 18.2 | 15.3 | 5.1 | 4.8% | 20.5% | 43 / 50 | 40 | 29 / 100 |
| 0 | Rust | 25.3 | 2.93 | 18.5 | 14.6 | 7.3 | 4.4% | 20.7% | 49 / 51 | 40 | 28 / 100 |
| 1 | Go | 22.9 | 3.00 | 17.2 | 22 | 7 | 0% | 0.6% | 32 / 50 | 36 | 42 / 52 |
| 1 | Rust ctl | 24.4 | 2.92 | 17.8 | 14.8 | 4.9 | 5.2% | 22.8% | 49 / 50 | 40 | 28 / 100 |
| 1 | Rust | 25.1 | 2.92 | 18.4 | 14.3 | 5.1 | 5.0% | 21.1% | 50 / 51 | 40 | 27 / 100 |
| 2 | Go | 22.9 | 3.00 | 17.2 | 22 | 7 | 0% | 0.9% | 30 / 50 | 38 | 42 / 51 |
| 2 | Rust ctl | 25.5 | 2.94 | 18.7 | 14.3 | 4.7 | 5.7% | 21.1% | 49 / 51 | 40 | 27 / 100 |
| 2 | Rust | 24.9 | 2.94 | 18.3 | 14.3 | 4.8 | 5.2% | 22.3% | 49 / 50 | 40 | 27 / 100 |

## Findings

- Permits per workflow are equal (Go 3.00, Rust 2.92-2.96): no extra Rust permits (confirms the earlier audits).
- Rust per-shard permit rate is 18.2-18.7/s, 7-17% above Go (15.9-17.2/s), because Rust completes ~10-15% more workflows/s
  and its p50 is 27 ms against 43 ms. Both are below the 20 rps limit on average.
- The wait is a saturation regime, not smooth. Per-second counts for one Rust shard (pair 0): 26,10,9,12,27,21,24,18,9,3,18,21,31,22,
  then 20/s with almost every permit waiting for seconds 13-21 and again 36-39. Every wait is about 50 ms (1/20 s), the signature of an empty
  bucket drained by a 20-permit burst plus 20 refilled in the same second (max 40 requests/1 s on every Rust shard; Go peaks 36-39 and
  waits on 1-2% of permits).
- Each wait adds ~50 ms to one workflow, and two waits per workflow give the 100 ms Rust p95 (Go p95 51-56 ms; Go's few waits are
  mostly one per workflow at most).
- Rust inter-arrival has 5% gaps under 5 ms (Go none; Go minimum p5 7-8 ms). These are not duplicate notifications: they follow
  a faster DB read/commit path, so the coalesced re-notify (identical to Go: more tasks or a following slice) runs sooner.
  Trace sample (shard 0, seconds): `4.253, 4.265, 4.273, 4.281, 4.294, 4.303, 4.362, 4.377, 4.386 (waited 2.5 ms)`.

## Conclusion

The Rust write path does not produce extra or earlier permits than Go; it produces permits faster because everything upstream
(commit, notify, transfer read) is faster. At c1 the closed loop then pushes individual shards past the 20 rps / burst-20 default
limiter in clusters, which Go (slower, ~16-17/s/shard) rarely does. Any change that reduced the waits would either slow the Rust
path or alter the limiter / load trigger, both deviations from Go. The gap is therefore a throughput-driven effect of the shared default
limiter, not a parity defect, and no code change was made. Consequently `cadence-history` CI, `history-mixed-gate` and the 5-rep
c1/16/64 benchmark were not re-run (nothing changed; master results remain the reference).

Not done: matched-rate (`target_rate`) traced run to show Go saturating at equal permit rates. The dispatch failed with
HTTP 422 ("up to 25 inputs") on `frontend-matching-benchmark.yml`; the workflow has 26 inputs and needs trimming first. The earlier
matched-rate memory evidence (`perf-c1-rate-mem-20261125.md`) covers the memory side only.
