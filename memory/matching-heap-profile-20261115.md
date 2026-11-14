# Matching live-heap investigation (2026-11-15)

Branch: `matching-heap-profile`, based on `3a6a5a44fcb20fe871b6093526d447d01417a0ff`.

## Measurement plan

No local Cargo, Go or Docker invocations. At most three measured soaks.
Default builds keep the system allocator. `frontend-soak.yml` has a new boolean
`heap_profile` dispatch input (default false). Opt-in builds use jemalloc with
profiling and release debug symbols. Only the Matching child enables sampled
profiling (`prof:true,prof_active:true,lg_prof_sample:19`) and snapshot collection.
Snapshots at 180, 1200, 2400 and 4000 seconds capture live sampled allocations;
allocated/active/resident statistics distinguish live growth from page retention.
The lane artifact includes raw profiles, stats and jeprof early/late diffs,
with three-day retention. The unstripped executable stays in soak-binaries.

## Runs

1. Profiled 70-minute/rate-10 Go baseline + Rust soak:
   https://github.com/auenkr/cadence-rust/actions/runs/43647087517
   source `f1dd5dc4b` — queued; no conclusion yet.
2. Targeted default-build server CI (fresh target):
   https://github.com/auenkr/cadence-rust/actions/runs/43647125592
   source `f1dd5dc4b` — queued.

Redundant automatic full CI 43647086757 canceled while queued; final workspace
validation will follow the measured fix. No root cause or parity verdict yet.

## Replacement dispatch (before any soak lane)

The first profile dispatch 43647087517 and targeted CI 43647125592 were
canceled while queued, before a build or soak lane. Inspecting the allocator
crate showed its statistics API requires the explicit `stats` feature; enabled
that feature and cumulative jeprof ordering in `25b510f0798afe6475da89fada649bf4ef60919b`.
The current first measured soak is
https://github.com/auenkr/cadence-rust/actions/runs/43647363855 and targeted
server CI is https://github.com/auenkr/cadence-rust/actions/runs/43647366793.
Both have started on standard hosted runners. Redundant automatic push CI
43647363234 was canceled. Measured-soak count remains zero until lanes start.

Targeted server CI 43647366793 completed SUCCESS on source `25b510f07`:
fresh `/home/runner/work/_temp/cargo-target-final`, clippy exit 0, tests exit 0.
Retrieved the runner-generated Cargo.lock without invoking Cargo locally.
It adds the optional profiler dependencies and reconciles pre-existing workspace
manifest/lock drift. Full logs are under
`/mnt/storage/service/matching-heap-profile/ci-targeted/`.

Profile build 43647363855 succeeded; Go baseline and Rust lanes are running.
Measured-soak count is now 1/3. The diagnostic executable artifact is ~208 MB
compressed (includes debug symbols and the existing Go binaries); heap profiles
and derived reports remain in the small lane artifact. Automatic full default
CI at lockfile head `917c5829d` is run 43648251008, still active.

## 23:00 UTC checkpoint

Both profile lanes remain in their soak steps. Automatic workspace CI
https://github.com/auenkr/cadence-rust/actions/runs/43648251008 passed at
`917c5829d904ff6c7070577767c1806c7a193220` (clippy/test exits 0).
This cached automatic run is interim coverage, not final fresh-target evidence.
No second soak, runtime fix, or leak-free claim. Dashboard updated.
