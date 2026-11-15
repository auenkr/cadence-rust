# Frontend soak anonymous RSS investigation (2026-11-15)

Branch: `soak-rss-anon`, based on `23b4caa6f7c0063912eaaa8bce2948a2af4460d4`.

## Input runs

Both failed runs used source `77dbaa379eaa88bc7b62b48fc3d711059e2dd93e` and the same Rust binary SHA256 `248df7d4d75c9f1555a1dc1c38b9a3077a1ab26f4b4f128ef06929189718f7cf`.

| Run | Rust Frontend RSS slope | Go Frontend | Rust Matching | Go Matching | Rust anon slopes (FE / MT) |
|---|---:|---:|---:|---:|---:|
| [43702876870](https://github.com/auenkr/cadence-rust/actions/runs/43702876870) | 11.67 MiB/h | 4.74 | 6.27 | 1.22 | 11.48 / 6.19 MiB/h |
| [43710549327](https://github.com/auenkr/cadence-rust/actions/runs/43710549327) | 10.63 MiB/h | 4.93 | 7.77 | 1.28 | 10.48 / 7.77 MiB/h |

Both completed 42,000 workflows with zero failures and passed latency checks. The anonymous component explains nearly all Rust RSS slope; file-backed slope was 0.00–0.27 MiB/h.

## Mapping evidence

The uploaded artifacts contained start/middle/end smaps only for Rust Matching. They did not contain Frontend mappings, despite both process RSS components being sampled. These are preserved under `memory/evidence/soak-rss-anon-20261115/run-*`.

In the Matching top-20 mappings, anonymous regions whose base is 64 MiB aligned (the expected glibc secondary arena heap alignment) carried most of the anonymous mapping RSS growth. Their visible count and summed RSS were:

| Run | Start | Middle | End |
|---|---:|---:|---:|
| 43702876870 | 9 / 16,060 KiB | 8 / 27,788 KiB | 8 / 27,828 KiB |
| 43710549327 | 8 / 15,920 KiB | 8 / 27,568 KiB | 8 / 28,252 KiB |

Counts are lower bounds because snapshots retain only the top 20 RSS mappings. The main `[heap]` RSS was 396→400 KiB and 392→396 KiB, respectively. The main stack stayed at 236 KiB. Matching thread count grew from 15 during startup to 26/27 by midpoint and stayed flat. Thus the RSS rise is in anonymous mmap regions, not the brk heap or the main stack; it is consistent with pages retained/touched in glibc per-thread arena heaps. The stable count in the top-20 list does not imply arena count is stable process-wide.

## Change under evaluation

`cadence-server` now sets glibc `M_ARENA_MAX=2` before building the Tokio runtime and runs `malloc_trim(0)` every 60 seconds when the System allocator is active on Linux GNU. Jemalloc builds are unaffected. This is limited to allocator page retention; workflow behavior is unchanged. The soak harness now captures FE and MT start/middle/end snapshots. For Frontend, the middle sample is captured after the scheduled restart so it belongs to the process used for the second-half RSS slope.

The repeated anonymous mapping evidence justifies an allocator-level experiment. The earlier Matching-only `MALLOC_ARENA_MAX=2` experiment reduced the total RSS slope only slightly, so this change also periodically returns free pages and applies to both Rust services through the shared server binary. The first two post-change runs are inconsistent (one PASS, one FAIL on Frontend RSS). Treat the allocator change as unconfirmed until the remaining capped soak attempt is complete.

## Validation

- Targeted `rust-ci.yml`, `package=cadence-server`: SUCCESS, source `df0aed30a10e8874c666e50a0febc8ae98b58c72`: https://github.com/auenkr/cadence-rust/actions/runs/43718985054.
- Full `rust-ci.yml`, `clean_target=true`: SUCCESS, fresh-target Clippy, format and workspace tests, same source: https://github.com/auenkr/cadence-rust/actions/runs/43719378837.
- Two parallel default `frontend-soak.yml` runs were dispatched at source `df0aed30a10e8874c666e50a0febc8ae98b58c72`: https://github.com/auenkr/cadence-rust/actions/runs/43720550241 and https://github.com/auenkr/cadence-rust/actions/runs/43720552710. Both used default rate/duration, `go-baseline,rust`, and no profiler or malloc environment overrides. One passed and one failed on FE RSS, so one remaining soak is being used for repeatability evidence.

### Checkpoint (2026-11-15 14:32 UTC)

Both runs were active with no early service death at this checkpoint; their final outcomes are recorded below.


### Default soak results

- [Run 43720552710](https://github.com/auenkr/cadence-rust/actions/runs/43720552710) completed SUCCESS; **RUST OVERALL: PASS**. Tested source `df0aed30a10e8874c666e50a0febc8ae98b58c72`; cadence-server SHA256 `5599ae802463f7390eead6f260e31c7dbb2cfe476ecdb469e7ebd6fd2c70e440`.
- RSS slopes: Frontend `0.94 MiB/h` vs Go `6.30`; Matching `4.44` vs Go `0.89`. FE anon slope `0.71`; Matching anon `4.42 MiB/h`. Both Rust and Go completed 42,000/42,000 workflows, zero failed, no panics or service deaths. All latency, error, FD and RSS verdict rows passed.
- Six Rust FE/Matching smaps snapshots are preserved in `memory/evidence/soak-rss-anon-20261115/run-43720552710/`. Each top-20 snapshot shows one 64 MiB-aligned anonymous mapping (lower bound) at start/mid/end, consistent with the `M_ARENA_MAX=2` cap. FE's post-restart middle snapshot is the warm process at 2105s; its RSS/anon rises from 55.2/12.5 MiB there to 75.8/30.0 MiB by end, then samples after the 2400s analyzer window start are flat around 75.2–75.6/31.2–31.6 MiB. That is restart warmup, not sustained second-half growth. Matching RssAnon rose 20.8→37.3 MiB from process start to end; the measured slope remains under the 5 MiB/h absolute guard and within Go+5. The `[heap]` and stack are not the dominant contributor; the aligned anonymous arena mapping RSS grows, while the count is held at one visible mapping.
- [Run 43720550241](https://github.com/auenkr/cadence-rust/actions/runs/43720550241) completed SUCCESS at the workflow level but **RUST OVERALL: FAIL** on Frontend RSS only. FE `12.49 MiB/h` vs Go `5.03`; Matching `4.17` vs Go `1.49`. FE anonymous slope was `12.27 MiB/h`, file-backed `0.27`; Matching anon `4.04`, file-backed `0.16`. All 42,000 workflows completed with no failure, panic or service death; all latency, FD, error and Matching RSS criteria passed. Rust binary SHA256 matched run 43720552710: `5599ae802463f7390eead6f260e31c7dbb2cfe476ecdb469e7ebd6fd2c70e440`. Evidence is preserved under `memory/evidence/soak-rss-anon-20261115/run-43720550241/`.
- FE samples show a discrete anonymous RSS jump from 27.7 MiB at 3005s to 30.6 MiB at 3065s, then a plateau at ~31.3–32.0 MiB. The FE restart happened at ~2100s; its 2400s analyzer window therefore includes this one-time jump. In the end top-20 smaps, the main `[heap]` is 9,432 KiB and main `[stack]` 208 KiB. Three anonymous 8,200-KiB VMAs each have 4,104 KiB RSS; none is labeled `[stack]` or has `gd` (grow-down) in VmFlags. The exact owner is not proven from top-20 records. Matching stays below the RSS guard. This makes the remaining problem a late anonymous mapping warmup/retention event in FE, not evidence of a sustained linear leak.

Because the first two outcomes differ, the harness is being extended to preserve a compact CSV of every VMA (address, size, RSS, anonymous RSS, VmFlags) alongside top-20 summaries. One final default soak attempt remains under the three-soak cap; it will use the same server binary and improved mapping evidence. Do not claim stable parity unless that run also passes.
