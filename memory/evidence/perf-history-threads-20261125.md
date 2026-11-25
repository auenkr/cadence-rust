# History idle thread reduction (perf-history-threads)

Base `19b127e56` (origin/master). Change `642ecca83`: the MySQL domain-query runtime (`QueryRuntimeOwner`, `cadence-persistence/src/matching_mysql.rs`) was an owner thread plus a 1-worker multi-thread runtime (2 OS threads). It is now one `current_thread` runtime driven by the owner thread (`block_on` of a oneshot shutdown signal), 1 OS thread per MySQL pool. Handle/spawn semantics are unchanged. The main tokio runtime and blocking pool were already bounded (available-parallelism workers, blocking cap 64 with 2 s keep-alive and env override), so were not changed. Other candidates (smaller stacks, tcache) were not applied: no measured need.

## CI
| Run | Purpose | Result |
|---|---|---|
| [45573795154](https://github.com/auenkr/cadence-rust/actions/runs/45573795154) | rust-ci cadence-persistence | success |
| [45573800053](https://github.com/auenkr/cadence-rust/actions/runs/45573800053) | rust-ci cadence-history | success |
| [45573804685](https://github.com/auenkr/cadence-rust/actions/runs/45573804685) | history-mixed-gate | success |
| [45573809210](https://github.com/auenkr/cadence-rust/actions/runs/45573809210) | 3-arm benchmark, 5 reps, c1/16/64, rust frontend,matching,history; control `19b127e56` | success |

Rust History SHA256 candidate `c999c5c9e51ea721ce774dc123151b676f29e1c7b8896add03512e9723bd2069`, control `01deeb50c121bf59d3168dff0b6345f217d751a26439ef619180bca4786edb69`.

## History peak anon memory, median of 5 (MiB; per-rep values in artifact)
| conc | Go | control | candidate | cand - control | cand - Go |
|---|---|---|---|---|---|
| 1 | 72.4 | 68.6 | 67.7 | -0.9 | -4.7 |
| 16 | 91.8 | 89.1 | 89.1 | 0.0 | -2.7 |
| 64 | 104.6 | 100.0 | 101.5 | +1.5 | -3.1 |

## Throughput (wf/s, candidate/control median [min,max]) 
c1 25.7 vs 25.2, 1.007 [0.962,1.025], p50 lat 0.992; c16 64.3 vs 63.7, 1.002 [0.987,1.016], 0.999; c64 58.8 vs 59.0, 0.991 [0.989,1.008], 0.961. No regression outside noise.

## Verdict
c1 History memory is below Go (-4.7 MiB) in this run. Against the control the gain is -0.9 MiB, inside the A/A noise (-1 to +3 MiB), so one thread saved is not a resolvable improvement on its own. The control on this base is already below Go at c1 (68.6 vs 72.4), thanks to the merged MySQL framing change. Runner: AMD EPYC 7763, 4 vCPU, 15 GiB, Ubuntu 26.04, image 20261116.202.1; services capped at 1 CPU (History 1.5 GiB mem limit class per machine-spec).
