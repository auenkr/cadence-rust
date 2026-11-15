# Default jemalloc for cadence-server (2026-11-15)

Branch: `server-jemalloc-default`
Implementation head: `8e61c5f95cefd3039d409163bbf13ef557692928`.

## Change

`cadence-server` now enables the optional `tikv-jemallocator` dependency by
default and installs it as the global allocator. The `heap-profile` feature
continues to add jemalloc profiling and the stats controller; it does not turn
profiling on in normal builds. The `system-allocator` feature can suppress the
global jemalloc allocator. Jemalloc is built with `background_threads`, and
`malloc_conf` enables its background purger while retaining default decay
intervals. No service logic changed.

## CI

- Targeted `rust-ci.yml`, package `cadence-server`: SUCCESS
  https://github.com/auenkr/cadence-rust/actions/runs/43670808320
  head `8e61c5f95cefd3039d409163bbf13ef557692928`.
- Full `rust-ci.yml`, clean target: SUCCESS (Clippy, formatting, full workspace
tests and supplemental checks)
  https://github.com/auenkr/cadence-rust/actions/runs/43671275242
  head `8e61c5f95cefd3039d409163bbf13ef557692928`.

## Default frontend soak

Run: https://github.com/auenkr/cadence-rust/actions/runs/43672414397
Workflow: `frontend-soak.yml`; lanes `go-baseline,rust`; defaults for rate and
duration; `heap_profile=false`, `system_heap_profile=false`, and no
`matching_malloc_env` override. The run is pending at the time of this note.

RUST OVERALL verdict: pending.
Matching RSS slope vs Go: pending.
Rust binary SHA256: pending.

### First default soak result

Run `43672414397` completed SUCCESS at implementation head
`8e61c5f95cefd3039d409163bbf13ef557692928`, but the analyzer verdict was
**RUST OVERALL: FAIL**. The uninstrumented default jemalloc build SHA256 was
`5fc729a865453b803dd401a711d9b97e72eaf67bda7b65a03ea7e6f42f4155d1`.
Matching RSS slope was `6.8183 MiB/h` versus Go `0.49 MiB/h`, exceeding the
Go+5 allowance. Frontend RSS passed at Rust `4.1590` vs Go `4.78 MiB/h`.
Both lanes completed all 42,000 workflows with no errors, panics, or service
deaths. The Rust lane's query p99 (94 ms vs 25 ms) and start p99 (185 ms vs
8 ms) thresholds also failed; other reported latency and FD criteria passed.
The run took 4,145 seconds, including the scheduled Matching and Frontend
restart. Analyzer evidence was downloaded from `soak-verdict` and `soak-rust`;
the artifact records the above source SHA and executable checksum.

Because the default-decay evidence missed the Matching RSS allowance, the
allocator configuration now keeps background purging enabled and sets dirty
and muzzy decay to 1,000 ms for the next build. This uses the remaining
permitted soak attempt to evaluate the measured RSS issue.

## All-Rust recovery gate

Workflow: `history-mixed-gate.yml`, MySQL, gRPC, `two_history=true`,
`all_rust_recovery=true`. Pending dispatch after soak completion.

Run URL, conclusion, head SHA and cadence-server binary SHA256: pending.

## Final verdict

Pending soak and mixed gate.

Checkpoint 2026-11-15 04:00 UTC: the soak's Go baseline and Rust workload lanes
are still running; neither has reported a failure. No conclusion or RSS slope is
available until the 70-minute workload and analyzer finish.

### Tuned decay validation

- Targeted `rust-ci.yml`, package `cadence-server`: SUCCESS
  https://github.com/auenkr/cadence-rust/actions/runs/43678579042
  head `57eab7a129197bcdc04eb03dd9211386b652d96e`.
- Full `rust-ci.yml`, fresh clean target: SUCCESS
  https://github.com/auenkr/cadence-rust/actions/runs/43679021081
  head `57eab7a129197bcdc04eb03dd9211386b652d96e`.
- Final allowed default soak is running:
  https://github.com/auenkr/cadence-rust/actions/runs/43680073670
  lanes `go-baseline,rust`; default rate/duration; no profiler or malloc env
  override inputs.

Checkpoint 2026-11-15 05:48 UTC: final allowed soak 43680073670 has both Go
baseline and Rust lanes active in their workload steps. The second soak has not
reported a failure; its analyzer verdict is not yet available.
