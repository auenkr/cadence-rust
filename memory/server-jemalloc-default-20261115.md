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

## All-Rust recovery gate

Workflow: `history-mixed-gate.yml`, MySQL, gRPC, `two_history=true`,
`all_rust_recovery=true`. Pending dispatch after soak completion.

Run URL, conclusion, head SHA and cadence-server binary SHA256: pending.

## Final verdict

Pending soak and mixed gate.
