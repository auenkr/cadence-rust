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

The repeated anonymous mapping evidence justifies an allocator-level experiment. The earlier Matching-only `MALLOC_ARENA_MAX=2` experiment reduced the total RSS slope only slightly, so this change also periodically returns free pages and applies to both Rust services through the shared server binary. Do not consider the issue fixed until both requested default soaks pass and the new FE/MT maps and RSS slopes are recorded.

## Validation

- Targeted `rust-ci.yml`, `package=cadence-server`: SUCCESS, source `df0aed30a10e8874c666e50a0febc8ae98b58c72`: https://github.com/auenkr/cadence-rust/actions/runs/43718985054.
- Full `rust-ci.yml`, `clean_target=true`: SUCCESS, fresh-target Clippy, format and workspace tests, same source: https://github.com/auenkr/cadence-rust/actions/runs/43719378837.
- Two default parallel `frontend-soak.yml` runs are active, both source `df0aed30a10e8874c666e50a0febc8ae98b58c72`: https://github.com/auenkr/cadence-rust/actions/runs/43720550241 and https://github.com/auenkr/cadence-rust/actions/runs/43720552710. Both use default rate/duration, `go-baseline,rust`, and no profiler or malloc environment overrides. Record completion, RUST OVERALL verdicts, slopes, snapshots, and Rust binary SHA256 here.
