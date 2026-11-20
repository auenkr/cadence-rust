# Standalone History RSS experiment

Baseline source `d52bfa31e85af73d9eafc5eee8c6c2271cf0ebf5`.
Allocator-only source `657251f6f7f605ada7241e9fc0592261ca7c3398`.
Combined benchmark source `8de933ffa697de7ce8285f85644e88736d8cfba2`.
Validation source `379ad324360b62f1a7884b46790663254cb7c9b1` adds the
capacity regression test to the combined implementation.
The buffer-release-only candidate restores the baseline allocator configuration.

## Allocation audit

- The benchmark image builds standalone History with `cadence-history/jemalloc-default`. The locked tikv-jemalloc-sys 0.6.1 build uses `--with-jemalloc-prefix=_rjem_` unless its unprefixed feature is enabled. Its config declaration resolves to `_rjem_malloc_conf`. History previously exported the unprefixed symbol, so the intended background purge setting did not reach the linked allocator. The candidate corrects the exported symbol, enables background purge, caps automatic arenas at two and sets dirty-page decay to one second. The experiment measures the combined effect of those settings.
- Go's execution and shard event caches default to 512 entries per shard, and Rust uses the same limits. Go starts with an initial capacity of 128 for each cache, while the Rust maps start empty. The benchmark uses four History shards. The execution cache owns mutable state, and the event cache holds decoded events. Their object layouts can affect live heap usage even when entry counts match.
- Queue v2 adapters have no page-sized preallocation. The virtual slice reserves one page of task pointers, as Go does for a slice of task interfaces. At the default 100 tasks this is about 1.6 KiB per request on 64-bit runners. It is scratch space.
- The benchmark gives both services the same MySQL config with 20 maximum open and idle connections for its default and visibility stores. Rust's pool builds an empty idle list and opens on demand. History's Tokio setup uses the default hardware-based worker count and on-demand blocking pool. The candidate leaves the pool, queue semantics, cache limits and runtime settings intact.
- In the prior History run, Rust RSS grew from about 90 MiB at measurement start to about 155 MiB in the last half. Go started around 113–118 MiB and reached about 151 MiB in the last half. That growth suggests looking at retained allocations as workflow count increases.
- Go's `cleanupTransaction` installs a new history builder. Rust's `finish_transaction` cleared the event vector and kept its capacity in cached mutable states. Rust's `HistoryEvent` stores optional attribute structs inline, so unused capacity holds space for every attribute variant. The combined candidate drops the vector after successful persistence when execution is completed. Active workflows continue to reuse the allocation. The regression test verifies both outcomes.

## CI

Benchmarks use `sdk-matching-benchmark.yml`, History only, 25 starts/s,
three alternating fresh-DB pairs, 10 s warmup + 60 s measurement, with
`fresh_build=true mysql_tmpfs=true sql_log_secs=60` and a full source SHA.
Peak RSS is the collector's sampled process RSS maximum. Steady RSS is the
median sampled process RSS over the final 30 seconds of measurement.
Report the median across pairs for each arm, including CPU seconds and SDK
workflow completion p99. Check runner CPU with `Benchmark/agg.py`.

| Check | Run |
|---|---|
| Baseline (degraded runner, excluded) | https://github.com/auenkr/cadence-rust/actions/runs/44701337483 |
| Fresh baseline rerun | https://github.com/auenkr/cadence-rust/actions/runs/44706794088 |
| Allocator only | https://github.com/auenkr/cadence-rust/actions/runs/44701569383 |
| Allocator and closed-workflow buffer release | https://github.com/auenkr/cadence-rust/actions/runs/44702306762 |
| Allocator-only fresh-target History clippy and tests | https://github.com/auenkr/cadence-rust/actions/runs/44701582094 |
| Fresh-target History clippy and tests | https://github.com/auenkr/cadence-rust/actions/runs/44702427944 |
| Fresh-target workspace clippy and tests | https://github.com/auenkr/cadence-rust/actions/runs/44702390685 |
| MySQL mixed gate with History jemalloc | https://github.com/auenkr/cadence-rust/actions/runs/44702404911 |
| MySQL restart gate with History jemalloc | https://github.com/auenkr/cadence-rust/actions/runs/44702417687 |

## Completed validation

- Allocator-only package CI passed at `657251f6f7f605ada7241e9fc0592261ca7c3398`. Package and workspace CI passed at `379ad324360b62f1a7884b46790663254cb7c9b1`. Each used `clean_target=true`.
- The MySQL mixed gate passed at `379ad324360b62f1a7884b46790663254cb7c9b1` with two Rust History instances and the Go Worker. Build and gate exit codes were 0, and the log contained `GATE SUMMARY failed: []` (413 steps). SHA256 for History was `b3e8593ff920e7e11d134b810bc7099adf77dd50662fc1750438b75453a508cd`, and for Frontend and Matching's server binary was `35d168dd4fb3d234486d7f248f926b7a402db1d3ac6088964119d6ddcd5e442e`.
- The MySQL restart gate passed at `379ad324360b62f1a7884b46790663254cb7c9b1` with `GATE SUMMARY failed: []` (52 steps). It completed all 4,500 workflows across 1,500 seconds of load and injected faults. SHA256 for History was `4f7a0238a57635cb10a5efba21fffc138dd74f33e6903519806aa0d6faefb1a3`, and for cadence-server was `61a9d65e4bf87ea1dce08128236186e662ac01892ac95627243f83bdc639e063`.
- The initial workspace CI, mixed gate and restart gate for the allocator-only revision were cancelled after supersession by the combined implementation.

## Benchmark results

| Variant | Arm | Peak RSS MiB | Steady RSS MiB | History CPU s | Workflow completion p99 ms |
|---|---|---:|---:|---:|---:|
| Baseline rerun | Go | 154.89 | 151.14 | 27.28 | 133.43 |
| Baseline rerun | Rust | 154.03 | 147.22 | 14.86 | 103.96 |
| Allocator only | Go | 159.74 | 149.60 | 19.27 | 111.86 |
| Allocator only | Rust | 151.76 | 140.51 | 10.37 | 100.87 |
| Combined | Go | 159.78 | 152.56 | 28.53 | 151.60 |
| Combined | Rust | 130.89 | 127.52 | 16.74 | 106.13 |

Allocator-only run 44701569383 succeeded on Intel Xeon 6975P-C with three pairs
and zero workflow errors in both arms. Rust binary SHA256 was
`fb88c38757b21b42871d806f8d1d7d5e9cece33dfdfc193564c0b321bd5b0464`.


Combined run 44702306762 succeeded on Intel Xeon Platinum 8370C with three pairs
and zero workflow errors in both arms. Rust binary SHA256 was
`94e8b2b510d6be30a716a618b9fc443cca66db6d5784a3645009b5b037659d6c`.
The Rust/Go ratios were 0.82 for peak RSS, 0.84 for steady RSS, 0.59 for
History CPU, and 0.70 for workflow completion p99.

Initial baseline run 44701337483 succeeded on AMD EPYC 7763, but Go p99 was
3097.91 ms and MySQL used 56.90 CPU seconds in the Go arm. All three pairs
were excluded from the before/after comparison and the baseline was rerun
with identical settings. The discarded run's Rust binary SHA256 was
`f5d42a074ec5626806e56f1fcf21360df5792ea55a6b20acf79de4d076cecaab`.


The baseline rerun 44706794088 succeeded on Intel Xeon Platinum 8370C, the
same CPU model as the combined run. It had three pairs and zero workflow
errors in both arms. Its Rust binary SHA256 was
`9a1458fb2656f5ce46eee1f6e4d75535e6b970e9d989aeea1959c309987d9f41`.
The combined variant reduced Rust peak RSS by 15%, but Rust CPU rose from
14.86 to 16.74 seconds (Rust/Go 0.54 to 0.59), and absolute Rust p99 rose
from 103.96 to 106.13 ms (Rust/Go 0.78 to 0.70). The allocator tuning was
dropped pending measurement of the buffer release with the original allocator.

## Buffer-release-only follow-up

| Check | Run |
|---|---|
| Fresh benchmark (degraded runner, excluded) | https://github.com/auenkr/cadence-rust/actions/runs/44712011461 |
| Fresh benchmark rerun | https://github.com/auenkr/cadence-rust/actions/runs/44718002863 |
| Fresh-target workspace clippy and tests | https://github.com/auenkr/cadence-rust/actions/runs/44712051751 |
| MySQL mixed gate | https://github.com/auenkr/cadence-rust/actions/runs/44712064908 |
| MySQL restart gate | https://github.com/auenkr/cadence-rust/actions/runs/44712079519 |

Workspace CI and the MySQL mixed gate passed on
`1354dc608f73459f02483962558f9e7e4b3ecdac`. The mixed gate reported
`GATE SUMMARY failed: []` across 413 steps. Its SHA256 was
`ee1fe3f5ecd4af7fb0e8922344a25cac63a55b83489d43ae8da6ec1c61eebcba` for
History and `e1f7cef24c0954cc791ad04469769d17158084d27c12975d17331d3bc2dfec4b`
for cadence-server.


The first buffer-only run 44712011461 succeeded on AMD EPYC 7763 with zero
workflow errors. Its Go arm used 52.85 MySQL CPU seconds and had 1356.67 ms
p99, so all three pairs were excluded from the qualified comparison and
rerun with identical settings. Its Rust binary SHA256 was
`0431ea9bbf7238103d789ee405ae78ebdd19c714ac3617c08f10630e2165e5cf`.
As a diagnostic, comparing the two excluded EPYC runs gives Rust peak RSS
156.83 to 140.65 MiB, steady RSS 143.03 to 129.54 MiB, History CPU 17.76 to
16.40 s, and p99 113.08 to 107.92 ms. Go p99 differs by more than 2x across
these runners, limiting the comparison.
