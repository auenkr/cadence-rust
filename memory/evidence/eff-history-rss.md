# Standalone History RSS experiment

Baseline source `d52bfa31e85af73d9eafc5eee8c6c2271cf0ebf5`.
Allocator-only source `657251f6f7f605ada7241e9fc0592261ca7c3398`.
Combined benchmark source `8de933ffa697de7ce8285f85644e88736d8cfba2`.
Validation source `379ad324360b62f1a7884b46790663254cb7c9b1` adds the
capacity regression test to the combined implementation.

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
| Baseline | https://github.com/auenkr/cadence-rust/actions/runs/44701337483 |
| Allocator only | https://github.com/auenkr/cadence-rust/actions/runs/44701569383 |
| Allocator and closed-workflow buffer release | https://github.com/auenkr/cadence-rust/actions/runs/44702306762 |
| Allocator-only fresh-target History clippy and tests | https://github.com/auenkr/cadence-rust/actions/runs/44701582094 |
| Fresh-target History clippy and tests | https://github.com/auenkr/cadence-rust/actions/runs/44702427944 |
| Fresh-target workspace clippy and tests | https://github.com/auenkr/cadence-rust/actions/runs/44702390685 |
| MySQL mixed gate with History jemalloc | https://github.com/auenkr/cadence-rust/actions/runs/44702404911 |
| MySQL restart gate with History jemalloc | https://github.com/auenkr/cadence-rust/actions/runs/44702417687 |
