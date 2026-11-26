# perf-load-bursts (2026-11-26): per-load trigger trace, Go vs Rust transfer queue v2

Branch `perf-load-bursts`. Code change = trace only (env-gated `CADENCE_TRACE_DECISION=1`, default off): Rust `virtual_queue.rs` logs `DTRACE queuev2 loaded trig=<bits> tasks page has_more` (trig bits 1 start, 2 has_more, 4 next-slice, 8 new-slice added); `go-queuev2-trace.patch` gets the equivalent Go line (hunk counts fixed after a corrupt-patch failure in run 45771126187).

## Run
[45775605906](https://github.com/auenkr/cadence-rust/actions/runs/45775605906) success, head `df9fc349de6dd6385d6cd09ea2d31cd08a9aaa0c` (workflow ref = branch; note `source_ref` of a branch-only SHA dispatched on master silently built master, run 45766043047 discarded). mysql, frontend,matching,history Rust, echo c1, 3 reps x 1000, trace_decision. Rust `cadence-server` sha256 `7c22d05635b496a6dbfafd4ebf336b6539c411ddc703fe1a5222f668ade8ada9`, `cadence-history` `c2921248eebd1bb92311d17715923c974799e448790f56d49cdcbc484e63fe8b`. Runner AMD EPYC 9V74, 4 vCPU, 15 GiB, Ubuntu 26.04.1, kernel 7.0.0-1020-azure. rust-ci cadence-history run 45766046612 (earlier trace-only head) success.
Analysis: `perf-load-bursts-analysis.py` (transfer queue, all 4 shards).

## Result (per rep: loads / non-empty / tasks / has_more=true / non-empty loads <5 ms after previous non-empty load)
| rep | Go | Rust |
|--|--|--|
| 0 | 3032 / 3028 / 4041 / 0 / 10 | 2977 / 2973 / 4041 / 0 / 180 |
| 1 | 3020 / 3016 / 4041 / 0 / 39 | 3001 / 2997 / 4041 / 0 / 215 |
| 2 | 3029 / 3025 / 4041 / 0 / 8 | 2999 / 2995 / 4041 / 0 / 206 |

Rust triggers: 100% of non-initial loads are trig=8 (a new slice added by processNewTasks, i.e. one NotifyNewTask coalesced burst); 0 has_more re-notifies, 0 next-slice re-notifies. Tasks per load ~1.35 in both. Loads are slightly FEWER in Rust (coalescing works).

## Conclusion
The hypothesis (extra has_more / re-notify loads) is refuted: has_more never fires in either implementation, the Rust load count is equal or lower, and tasks/loads match Go. The <5 ms back-to-back loads (Rust 6-7% vs Go 0.3-1.3%) are separate new-slice loads released close together. A Rust-specific candidate remains untested: `process_new_tasks` first awaits `shard.wait_for_task_writes()` (task_write_gate, no Go equivalent; Go instead relies on its read-level tracking), which can hold several notifications until a write finishes and then release them in a cluster. Changing it risks the read-level correctness it protects, so no fix was made and no p90/p95 benchmark was run (nothing to measure). Next step if pursued: trace gate wait duration and compare loads released within the same gate release.
