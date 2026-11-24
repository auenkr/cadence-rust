# History memory and long-poll cancellation

Branch `perf-history-memory` starts from master `ac5df792cce5116c4b96e3c38f78050d0e9a3182`. Implementation head `ca9443232286713214694ece5ffed92eb4f49aa0` adds the final test assertion to executable source `ad82d13990838c5fbb395f1414411eae99ef7bb8`.

- Run 45291978847 sampled total cgroup memory. Its Go → Rust History peaks were 77.4 → 89.2, 98.0 → 114.8, and 111.9 → 137.3 MiB at concurrency 1, 16, and 64. Charged file cache is part of those totals. The resource collector now records independently sampled peaks for total, `memory.stat anon`, and `memory.stat file`. The baseline rerun uses the new collector against unmodified History on the same master base. [Original benchmark](https://github.com/auenkr/cadence-rust/actions/runs/45291978847)
- Standalone History with `jemalloc-default` exports the allocator's prefixed config symbol with background purging, 1-second dirty decay and two arenas. The two-arena default targets the one-CPU benchmark. Deployments can override it with `_RJEM_MALLOC_CONF`. Tokio defaults to a quota-aware worker count, preserves `TOKIO_WORKER_THREADS`, and keeps Tokio's default thread stack (a smaller release stack was removed for stability; its resident saving is negligible because stack pages are untouched virtual memory).
- Dropping a long-poll future now releases its subscriber and closes the channel that drives the notifier adapter's relay task, following Go's deferred unsubscribe. The cancellation test checks that the subscription disappears as soon as the future is dropped.

## CI

| Check | Source SHA | Run | Conclusion |
|---|---|---|---|
| Instrumented baseline, same paired workload | `f1a350cab3bcd5f35aa2ec2f768cf8c310a60548` | [45343398849](https://github.com/auenkr/cadence-rust/actions/runs/45343398849) | Pending |
| History Clippy and tests, fresh target | `ca9443232286713214694ece5ffed92eb4f49aa0` | [45343704132](https://github.com/auenkr/cadence-rust/actions/runs/45343704132) | Success. Clippy and 944 History tests passed, including the cancellation regression. |
| Workspace Clippy and tests, fresh target | `ca9443232286713214694ece5ffed92eb4f49aa0` | [45343704159](https://github.com/auenkr/cadence-rust/actions/runs/45343704159) | Success. Clean-target Clippy and workspace tests passed. |
| MySQL mixed gate, two History hosts, jemalloc, gRPC | `ad82d13990838c5fbb395f1414411eae99ef7bb8` | [45343599787](https://github.com/auenkr/cadence-rust/actions/runs/45343599787) | Pending |
| MySQL soak, two History hosts, jemalloc, 45 min at 4 workflows/s | `ad82d13990838c5fbb395f1414411eae99ef7bb8` | [45343599425](https://github.com/auenkr/cadence-rust/actions/runs/45343599425) | Pending |
| Tuned paired benchmark | `ad82d13990838c5fbb395f1414411eae99ef7bb8` | [45343599527](https://github.com/auenkr/cadence-rust/actions/runs/45343599527) | Pending |

Both benchmark dispatches use `rust_services=frontend,matching,history` with defaults from run 45291978847. Each run uses five paired repetitions, 1,000 workflows per arm at concurrency 1, 16 and 64, 10 warmups, a 1,024-byte payload, and the Go SDK over TChannel.

At 07:46 UTC, the mixed gate and soak had built the jemalloc History binary and entered their workload steps. Both paired jobs had finished building the Go and Rust images and started the SDK workload.
