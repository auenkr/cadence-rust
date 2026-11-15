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

## 23:27 UTC checkpoint

Profile soak 43647363855 still has both Go baseline and Rust lane jobs active
in their Soak steps (~53 minutes since lane step start). No early exit or
uploaded lane reports yet. One measured soak has been started; two slots remain.
No further code changes, local builds, tests, or runtime processes.

## Profiled jemalloc soak result (measured soak 1/3)

- Run 43647363855: workflow SUCCESS, **RUST OVERALL: PASS**, 42,000/42,000
  completed, zero failures/panics. Matching RSS slope 4.01 MiB/h vs Go 1.29.
- Source: `25b510f0798afe6475da89fada649bf4ef60919b`; binary SHA256
  `afcd05b7428c313827bac70a5f5d38c23f3c71b08a6bf7640fa72f3ec665a603`.
- All four `prof.dump` calls succeeded. Exact jemalloc allocated bytes:
  180s 117928912; 1200s 113299488; 2400s 113667600; 4000s 113623504.
  After 1200s, allocated bytes grew only 324016 bytes, then fell between 2400s
  and 4000s. Resident bytes fell from 135507968 to 133832704 (180→4000s).
- Sampled live-heap diffs vs 180s: 1200s -2692801 B; 2400s +4991749 B;
  4000s -2251919 B. Sampling at 512 KiB has substantial variance; exact stats
  are stronger evidence that live jemalloc allocations plateaued in this run.
- Largest positive late cumulative stack: TChannel serve_listener +2635547 B,
  including AppState/cluster metadata BTreeMap clones +2102277 B; Thrift value
  decoding +524312 B. Earlier positives included active activity-poll futures.
  These are concurrent connection/RPC state, not a monotonic live-byte trend.
  Total late live estimate ~101.6 MB; ~83.9 MB was connection/pool initialization
  and ~75.5 MB was owner lease/connect initialization (overlapping stacks).
- Raw profiles, derived reports, full samples, summary, verdict and binary
  identity committed under `memory/evidence/matching-heap-profile-20261115/jemalloc/`.
  Raw profiles are ~25 KB each; the 43 MB lane artifact is mostly service logs.

This proves a live-heap plateau **with the diagnostic jemalloc allocator**,
not the default system allocator. It does not yet establish the cause of the
original default-build slope. A second, opt-in System-allocator measurement
adds exact requested-live-byte counters, 64-KiB byte-interval sampled stacks,
glibc mallinfo2 and smaps_rollup snapshots, without replacing the allocator.
Symbolization runs after process exit to avoid caching debug data in the server.
Feature-specific clippy and a sample lifetime/reallocation regression test are
queued in https://github.com/auenkr/cadence-rust/actions/runs/43655373681.
No second soak has started yet. Default builds remain unchanged.

## System-allocator measurement dispatch

- https://github.com/auenkr/cadence-rust/actions/runs/43655943235
- Source `035fa2506`, 70 minutes, rate 10, Go baseline + Rust, only
  `system_heap_profile=true`; no malloc tuning. This is soak 2/3 once lanes start.
- Feature-specific CI 43655373681 passed both profiler Clippy checks and the
  allocation/reallocation/free regression test, and is continuing default checks.
- System sampling uses fixed 64-KiB per-thread byte intervals. Sample counts are
  diagnostic ownership observations, not unbiased estimated byte totals. Exact
  requested-live-byte counters and mallinfo2/smaps are the main trend evidence.
- Sampling excludes recursive profiler allocations; exact counters include them.
  Profile state is bounded by currently sampled live pointers and map capacity.

Profiler CI 43655373681 completed SUCCESS on `b430de917` with CI-applied
formatting (committed unchanged in `035fa2506`). Both feature Clippy checks,
System sample-lifetime regression, default server Clippy/tests and configured
metric comparisons passed. Imported its Cargo.lock (backtrace dependencies)
without invoking Cargo locally. Redundant automatic CI 43655942178 canceled.

## 00:25 UTC checkpoint

System-allocator soak 43655943235 built successfully and both lanes started
at 00:09:58 UTC. Measured-soak count is 2/3. The final slot is reserved for
uninstrumented default-build confirmation after interpreting these snapshots.
Fresh workspace CI https://github.com/auenkr/cadence-rust/actions/runs/43657018303
at `4802e9251` passed Clippy and is running workspace tests, using clean_target=true.
Redundant automatic lockfile CI 43656047369 was canceled. No production fix or
second-soak measurement conclusion yet; no local Cargo/Go/Docker commands.

Fresh workspace run 43657018303 completed SUCCESS at
`4802e92518a36fd53e4181b6a02df83462c0c088`. Its recorded target is
`/home/runner/work/_temp/cargo-target-final`; Clippy and workspace test exits
are both 0. Compact identity/exit evidence is committed under `ci-final/`.
No runtime code has changed above this tested source. Full artifact logs are
preserved on storage under `/mnt/storage/service/matching-heap-profile/ci-final/`.

## 00:55 UTC checkpoint

Second soak 43655943235 continues in both lanes (~45 minutes since job start).
No late snapshots uploaded yet; no root-cause conclusion, production change,
or third dispatch. All source above fresh workspace CI `4802e9251` is docs-only.
Updated dashboard with the fresh workspace validation result. Two measured
soaks started so far, one remaining.

## 01:25 UTC checkpoint

Second workload steps finished; Go baseline job succeeded. Rust lane is in
`Analyze system allocator snapshots`, resolving recorded PCs on the runner.
Awaiting artifact upload and comparison. Still 2/3 measured soaks; no production
change or third soak dispatched. Fresh workspace CI remains successful.

## Lost System snapshots / final-slot recovery

The System workload ran to completion in both lanes, but post-processing was
too slow: the initial symbolizer invoked addr2line separately for each of up to
160 stacks. At 01:29 UTC, canceled run 43655943235 to try to preserve artifacts
before the 90-minute job deadline. This was an implementation mistake: its
`always()` analysis continued until cancellation hard-terminated the job; the
pending artifact upload never ran. Run conclusion CANCELLED. Only Go baseline,
binaries and an empty comparison artifact survived. No System measurements are
available, and no System plateau or root cause can be claimed from this run.

Changed symbolization to one addr2line process for all unique PCs, added an
8-minute analysis step timeout, and upload raw snapshots **before** analysis.
An artifact-only replay job is available through rust-ci.yml's
`heap_profile_run_id` input (skips clippy/test builds). No fourth soak allowed.
The last remaining measured slot will recover System live-heap evidence; a
separate uninstrumented default confirmation is no longer available within cap.

Final allowed recovery soak:
https://github.com/auenkr/cadence-rust/actions/runs/43663938440,
source `50f8b76de`, system_heap_profile=true, 70 minutes/rate 10,
Go baseline + Rust. Once lanes start, all three measured slots are exhausted.
Raw snapshots upload before any analysis; batched symbolization has an explicit
step timeout. No further soak will be dispatched in this task.

## Final measurement running / diagnostic CI success

Recovery soak 43663938440 built successfully; both lanes are running. Three
measured workloads have now started; the cap is exhausted. Do not dispatch more.

The first symbolizer regression invocation (43664022279) failed because unittest
discovery skipped the hyphenated filename (zero tests, exit 5). Changed CI to
invoke the test file directly. Run
https://github.com/auenkr/cadence-rust/actions/runs/43664502873 completed SUCCESS
at `862fa042e2293751ad9ecd260916cfc4debc992b`: symbolizer regression, both
profiler feature Clippy checks, allocation lifetime test, default server
Clippy/tests and metric comparisons. Compact source/exit evidence is under
`ci-profilers-final/`. No Rust source changed since fresh workspace run 43657018303.

## 02:34 UTC resumed checkpoint

Resumed from clean pushed head `fa4419df3`. Recovery soak 43663938440 still
has both lanes in their workload steps (started 01:48 UTC); build succeeded.
All three measured slots are exhausted. No new dispatch or production change.
Reviewed profiler counters and post-processing: exact live bytes include
recursive diagnostic allocations; sampled stacks exclude them and are ownership
observations rather than unbiased byte totals. Awaiting late snapshots and
comparison before deciding whether live state or allocator retention grows.

## Final System result (measured soak 3/3; cap exhausted)

- https://github.com/auenkr/cadence-rust/actions/runs/43663938440 completed
  SUCCESS at source `50f8b76de5ee0d18d9677a43c23b3d4de62d7064`.
  Binary SHA256 `9775c223cd86a01d59dcb70b215eac8be1219e742a4c933d8762c1dfd527f71f`.
- Analyzer **RUST OVERALL: FAIL**, solely Matching RSS: 6.3687 MiB/h vs
  Go 0.920 (allowed Go+5). Every latency/error/FD/crash criterion passed;
  42,000/42,000 workflows completed, zero failures/panics.
- This build delegates to the production System allocator, with opt-in counters
  and sampled stacks; it is **not** an uninstrumented default-build confirmation.
  Raw snapshot upload and batched symbolization both succeeded.

| seconds | exact Rust requested live B | glibc arena B | glibc used arena B | glibc free arena B | mmap B | RSS KiB | anonymous KiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| 180 | 94,852,750 | 29,765,632 | 22,980,480 | 6,785,152 | 75,571,200 | 81,140 | 33,464 |
| 1200 | 95,061,826 | 31,850,496 | 23,601,168 | 8,249,328 | 75,571,200 | 84,668 | 36,928 |
| 2400 | 94,803,345 | 32,915,456 | 23,425,168 | 9,490,288 | 75,571,200 | 87,980 | 40,240 |
| 4000 | 94,762,837 | 32,395,264 | 23,500,048 | 8,895,216 | 75,571,200 | 90,300 | 42,560 |

### Growing stacks and interpretation

The largest late positive sampled stack is `NativeMatchingService::poll_for_decision_task`
boxed futures: +242,688 sampled requested bytes, 40→42 samples. It is already
+242,688 at 1200s and remains there at 2400s and 4000s. Activity poll futures
have +169,560 sampled bytes at all three later snapshots (different concurrent
stack variants). The next late increases are only +32,128 B for an activity
poll variant, +22,128 B decision dispatcher, +19,616 B activity dispatcher,
+15,232 B TChannel RPC task and +11,960 B cluster metadata clones. These are
concurrent poll/connection state, with no multi-megabyte accumulating stack.
Sample counts are diagnostic observations, not unbiased allocation estimates;
full early/late stacks and raw PCs are preserved for independent inspection.

Exact live requested bytes decrease 89,913 B over 180→4000s, and stay within
299 KB across all snapshots. This independently corroborates the jemalloc
live-heap plateau. **Growing retained Rust object bytes are not the explanation
for the reproduced RSS slope in this instrumented run.** Glibc's mmap total is
constant, arena size is up 2.63 MB and free arena space up 2.11 MB. RSS is up
9,160 KiB, almost entirely anonymous (+9,096 KiB); file RSS changes by only 64 KiB.
This is consistent with retained/touched allocator/native pages rather than
increasing live Rust state. The counters do not identify page residency per
allocation or account for every native allocation/stack page, so they do not
prove an exact glibc fragmentation mechanism or an RSS plateau. The earlier
arena/trim experiments also failed, so no allocator tuning is proposed as a fix.

### Verdict and scope

No production logic/API change or speculative leak fix was made. The bounded
quantity demonstrated by both allocators is **live Rust heap**, not process RSS.
Default uninstrumented acceptance remains unproven, and the final System
instrumented RSS criterion still fails. All three allowed measured workloads
were used (jemalloc success, canceled System run with lost artifacts, final
System recovery); no fourth soak or threshold change was made.

Fresh uncached full workspace Clippy then tests passed in run 43657018303 on
`4802e92518a36fd53e4181b6a02df83462c0c088`; the Rust source is unchanged since.
Targeted default server CI passed 43647366793; profiler feature Clippy,
allocation/reallocation/free unit test, and batched-symbolizer regression passed
43664502873 on `862fa042e2293751ad9ecd260916cfc4debc992b`.
All Cargo/Go/Docker execution stayed on GitHub Actions.

Evidence: `memory/evidence/matching-heap-profile-20261115/system/` contains raw
snapshots, compressed full symbolized stacks, compact top-stack deltas, both
lanes' samples/summaries, verdict and binary identity. Full service logs remain
in `/mnt/storage/service/matching-heap-profile/system-final/rust/` and the
three-day Actions artifacts. Artifact downloads used the API streamed to storage
when gh's temporary zip exceeded the /tmp quota. No service/branch was deleted.
