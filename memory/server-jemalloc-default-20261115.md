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

### Tuned decay soak result

Run `43680073670` completed SUCCESS at source head
`57eab7a129197bcdc04eb03dd9211386b652d96e`; analyzer verdict **RUST OVERALL:
FAIL**. The binary SHA256 was
`5b9874f329eb1d8fc5923800b784a02ce72307acc387e2941900e8151b1124cb`.
Matching RSS slope regressed to `11.13 MiB/h` vs Go `1.33 MiB/h`. Frontend FD
slope also failed (`16.88` vs `0.48` fds/h); query p99 and start p95/p99 failed.
All 42,000 workflows completed, with no service death, panic, or workflow error.
This 1-second decay configuration did not establish an allocator win.

The implementation restores jemalloc's default decay intervals and retains
`background_thread:true`. The requested process `RssAnon`/`RssFile`/`RssShmem`
sampling and start/mid/end Mapping snapshots are prepared in commit
`7ce9969e9b6fed75e5d73b383b7abbdae4ef21c5`; the next soak will use them to
attribute Matching RSS growth. No soak has yet run with this instrumentation.

Checkpoint 2026-11-15 05:48 UTC: final allowed soak 43680073670 has both Go
baseline and Rust lanes active in their workload steps. The second soak has not
reported a failure; its analyzer verdict is not yet available.

## RSS attribution instrumentation prepared

The soak harness now samples `RssAnon`, `RssFile`, and `RssShmem` from process
status for Frontend and Matching and appends those values to `samples.csv`
without changing existing columns. Its summary records slopes for each component.
The Rust and Go Matching processes each capture `smaps_rollup` and the top 20
`smaps` regions by RSS at workload start, midpoint, and end. These diagnostics
are copied into each lane artifact and do not gate the workload. Python AST
parsing and `git diff --check` passed locally; no Cargo, Go, or Docker command
was run locally.

These changes were committed after tuned soak 43680073670 was dispatched, so
that run validates only the 1,000 ms decay config. That run completed with a
FAIL verdict as recorded above. The next soak has restored default decay and
includes the diagnostic instrumentation.

Diagnostic soak 3 is now running at
https://github.com/auenkr/cadence-rust/actions/runs/43686161316 on head
`4e16d751da14ae652fd2c4462950665e95ef50ef`. It uses the default
`go-baseline,rust` lanes with no profiler or malloc environment inputs. Its
result will determine whether the Matching RSS slope is anonymous or
file-backed before changing the parity criterion or allocator default.

Checkpoint 2026-11-15 07:24 UTC: diagnostic soak 43686161316 remains active;
both `go-baseline` and `rust` are in their soak workload steps. No analyzer
verdict or RSS-component samples are available until the lanes complete.

## Diagnostic soak 43686161316 result and allocator decision

- Run: https://github.com/auenkr/cadence-rust/actions/runs/43686161316
- Tested source: `4e16d751da14ae652fd2c4462950665e95ef50ef`; workflow conclusion
  `success`, but analyzer verdict **RUST OVERALL: FAIL**.
- Rust Matching binary SHA256:
  `d2ff8072bb3183fabd3da5226c25907a31a87ce16fe9db0d4feb2cb1b9fc6bae`.
- All 42,000 workflows completed with zero failures, panics, or process deaths.
  Rust e2e p50/p95/p99 were 3078/4180/4902 ms vs Go 3090/4096/4117;
  query p99 was 158 vs 45 ms; start p95/p99 were 106/275 vs 14/20 ms.
  Thus jemalloc with the background purger did not show a latency win; earlier
  System allocator run 43663938440 had query p99 31 vs 36 ms and start p95/p99
  13/18 vs 9/16 ms.
- Matching total RSS slope was 11.15 MiB/h vs Go 1.50. Component slopes show
  Rust `RssAnon` 11.11 MiB/h and `RssFile` 0.00; Go `RssAnon` 0.86 and
  `RssFile` 0.74. The suspected binary-page explanation is disproved for this
  run. Matching RSS criteria therefore remain total RSS; do not replace them
  with anonymous RSS criteria because anonymous growth is the failing signal.
- Rust Matching `smaps_rollup` at start/mid shows anonymous RSS increasing from
  26,720 to 46,312 KiB while file-backed PSS changes 19,622 to 21,190 KiB.
  The main executable's executable mapping grows from 29,792 to 32,264 KiB.
  Anonymous mappings grow across multiple regions: a new listed region had
  6,560 KiB resident at midpoint, and a persistent 14 MiB region rose from
  4,884 to 5,332 KiB. Process threads increase from 33 to 35 late in the run.
  These are anonymous allocator/stack mappings, not the binary text mapping.
  The top-mapping snapshots alone do not identify which allocations own the
  pages. This aligns with the earlier instrumented
  System allocator profile: live Rust allocations stayed flat while anonymous
  RSS and glibc free arena bytes rose. No program-logic fix is justified by
  these observations.
- The requested end snapshots were absent: the SDK driver ended around 55 sec
  before the sampler's nominal `SOAK_SECONDS`, ending the sampling loop before
  its threshold check. Fixed the harness to capture Matching mappings once
  after the driver exits and before service teardown. This fix will be covered
  by the next permitted soak.
- As directed, normal cadence-server builds now use the System allocator again:
  jemalloc's RSS slope was worse and its latency tails regressed. `heap-profile`
  remains opt-in and explicitly enables jemalloc profiling; the separate
  `jemalloc-default` feature remains available for controlled comparison.
  This is allocator selection only; no service logic changed.
- Artifacts (samples, summaries, binary hash, verdict, start/mid `smaps_rollup`
  and top-20 mappings for Rust and Go) are in
  `memory/evidence/server-jemalloc-default-20261115/run-43686161316/`.

## System-default validation in progress

- Targeted `rust-ci.yml`, package `cadence-server`: SUCCESS
  https://github.com/auenkr/cadence-rust/actions/runs/43692936637 at
  `324842c6b38319df7691aaf33fa72a2568fc4647`.
- Full fresh-target `rust-ci.yml`: SUCCESS
  https://github.com/auenkr/cadence-rust/actions/runs/43693444833 at the same
  source head.
- Final allowed soak dispatched with default inputs (lanes `go-baseline,rust`,
  no profiling and no malloc environment input):
  https://github.com/auenkr/cadence-rust/actions/runs/43693675128. Build
  succeeded; both workload lanes are active. It will check the System allocator
  default and must contain start/middle/end Matching mapping snapshots.
- Matching RSS/anonymous slopes, latency verdict, Rust binary SHA256, and end
  snapshot completeness: pending soak artifact upload.

Checkpoint 2026-11-15 09:00 UTC: final soak 43693675128 has both lanes active
in their soak workload steps. Full Rust CI is green; no run conclusion or final
mapping capture is available yet.

### Final System-default soak result

- Run: https://github.com/auenkr/cadence-rust/actions/runs/43693675128,
  workflow conclusion SUCCESS, analyzer **RUST OVERALL: PASS**. The source
  under test was `324842c6b38319df7691aaf33fa72a2568fc4647`; the later commits
  before the mixed gate only record memory evidence/checkpoints.
- Rust cadence-server binary SHA256:
  `248df7d4d75c9f1555a1dc1c38b9a3077a1ab26f4b4f128ef06929189718f7cf`.
- Matching total RSS slope: Rust `5.47 MiB/h`, Go `1.18 MiB/h`; the maximum
  allowed is Go+5 = `6.18 MiB/h`. Matching anonymous RSS slope was Rust
  `5.35 MiB/h`, Go `0.45 MiB/h`; file-backed RSS was Rust `0.00 MiB/h`, Go
  `0.76 MiB/h`. Since file-backed growth did not cause the observed slope, the
  analyzer continues to gate on symmetric total RSS. No anonymous-only
  criterion change was made.
- All 42,000 workflows completed in both lanes, zero failures, no panics or
  process deaths. Latency p99s: e2e 4100 vs Go 4108 ms, query 29 vs 50 ms,
  start 15 vs 41 ms. Every verdict criterion passed.
- The start/middle/end `smaps_rollup` and top-20 mappings are present for both
  Rust and Go. Rust Matching has 27 threads throughout the warmed workload.
  Its largest anonymous mapping at `747e70000000` had RSS 7,260/9,072/8,868
  KiB at start/middle/end; other large anonymous mappings at `747e68000000`,
  `747e74000000`, and `747e78000000` also grew over startup and partially
  receded by end. The main executable's file-backed mapping is not the source
  of sustained RSS growth. The earlier System-allocator heap profile found
  stable live requested Rust heap bytes and increasing free glibc arena bytes,
  consistent with allocator retention rather than a live-object leak. The
  warmed anonymous slope is within the existing Go+5 allowance; no program
  logic change is indicated.
- Complete samples, summaries, binary identity, verdict and all six Matching
  snapshots are saved under
  `memory/evidence/server-jemalloc-default-20261115/run-43693675128/`.

### MySQL all-Rust recovery gate

- Dispatched `history-mixed-gate.yml` with `database=mysql`, `outbound=grpc`,
  `two_history=true`, `all_rust_recovery=true`:
  https://github.com/auenkr/cadence-rust/actions/runs/43700466749.
- Tested head: `bd5627046183c82746e8e9057b1c49f1c5c0aaee`. Result and
  workflow conclusion SUCCESS. `cargo-build.exit=0`, `gate.exit=0`, and
  `GATE SUMMARY failed: []`.
- cadence-server binary SHA256:
  `40cdb21e8539c2ff826cc6fcde654b73ecc98f0ec62b724594843979686e3c10`;
  cadence-history binary SHA256:
  `7ed1c5f961456cad9cf6b8869fbaee04f1dff5c876ffbdd7b04e5e93f1bd5b1d`.
- Gate logs, exit codes, source SHA and binary hashes are saved in
  `memory/evidence/server-jemalloc-default-20261115/gate-43700466749/`.

## Final status

The default-System soak passed `RUST OVERALL: PASS`, and the MySQL all-Rust
recovery gate passed. Targeted and full fresh-target Rust CI also passed. The
normal cadence-server build uses System allocation; jemalloc remains an
opt-in feature, including for heap-profile builds. No RSS analyzer threshold
was changed because Matching file-backed RSS was flat and the observed anonymous
growth remained within the existing symmetric total-RSS allowance.
