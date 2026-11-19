# Platform CI speedup (2026-11-19, branch platform-speedup)

## Measured baseline (gh run list -L 300, 2026-11-15..19, completed runs)

| Workflow | n | median wall | notes |
|---|---|---|---|
| rust-ci | 62 | 7.2 min | 11 failures: 10 = rustfmt ("Formatting"/"Check changed Matching runtime formatting"), 1 test. Formatting ran AFTER Go oracles (~90s) + Clippy (72s+) so a format error cost ~4-7 min + queue. Queue wait up to 246s. |
| SDK service fixed arrival benchmark | 43 | 34 min | 12 cancelled (concurrency group = ref+service, so parallel dispatches on one ref cancelled each other -> workers created temp branches). |
| Benchmark <svc> source <sha> (frontend-matching-benchmark sdk_openloop) | ~40 | 45-80 min | Run 44395950242: one 4280s step; inside it `docker build --no-cache` of the Rust image = **1444s (24 min)** (`#17 DONE 1444.3s`, cargo -j2 on a 4-vCPU runner), Go image 134s. Same SHA benchmarked up to 8 times -> each rebuilt the identical binary. |
| History mixed gate | 36 | 13.1 min | build 187s, Go 27s, test 475s (44417359681). Test dominates. |
| Frontend mixed gate | 21 | 16.0 min | build 164s, Go 36s, gate 437s (44426984365). |
| Matching mixed gate (MySQL) | 4 | 12.6 min | build 192s, gate 122s. |
| Frontend soak | 8 | 84 min | build job 452s, then 2 x 71-min soak lanes (parallel); job queue 192-759s. |
| History ES visibility | 4 | 27 min | |

Other findings:
- **GitHub free plan = max 20 concurrent jobs per account.** With ~11 workers there were 28 runs in progress + 9 queued at 01:00Z; queue wait is now the largest single sink for short runs. "Unlimited minutes" != unlimited concurrency: don't dispatch speculative/duplicate runs, cancel your own superseded runs.
- **REST API quota exhausted** (2026-11-19 00:38Z, 0 left, reset 01:13Z): all workers share one 5000/h token quota; tight `gh run watch`/`gh run view` loops by ~11 workers burn it, after which every gh call (dispatch, watch, download) fails with HTTP 403 for up to an hour. gh-run.sh polls once per 120s (`CI_POLL_SECONDS`) and tolerates errors; avoid `gh run watch` (polls every 3s).
- Concurrency groups without run_id (matching-production-validation was GLOBAL: any dispatch cancelled every other worker's run; matching-mixed/cassandra/active-offer/... per ref; rust-ci dispatch per ref+package+sha).
- Cache quota (10GB) is mostly setup-go caches (~640MB each, many duplicate per-branch copies); branch caches are only readable by that branch, master caches by everyone.
- Gates intentionally build in a fresh target (~3 min); kept as is (evidence rule).

## Changes (branch platform-speedup)
1. `Benchmark/Dockerfile` + `Benchmark/ci.sh` + both benchmark workflows: Rust binaries cached in the GitHub cache keyed on
   `git rev-parse HEAD:rust`, `HEAD:go/idls`, `HEAD:go/proto` of source_ref, service, profile and Dockerfile hash
   (`bench-bin-v1-...`). Hit -> `BIN_STAGE=prebuilt` (builder stage skipped), miss -> fresh `--no-cache` build then saved.
   Cargo now uses `-j$(nproc)` (was -j2; codegen-units fixed at 16 so output is the same). Evidence file
   `rust-bin-source.txt` says "cache hit (<sha256>)" or "fresh build"; image still carries /etc/cadence/cadence-server.sha256.
   Input `fresh_build=true` ignores the cache (use for final evidence).
2. Concurrency: every dispatch-only workflow group now includes `github.run_id` (rust-ci dispatch too; pushes still cancel
   superseded pushes). Parallel workers no longer cancel each other -> no temp benchmark branches needed:
   dispatch on master with `source_ref=<sha>` (and then master-scoped cache entries are shared by all branches).
3. rust-ci: new `fmt` job (checkout + rustfmt only, enforced file lists from `scripts/ci/rust-fmt-check.sh`, single source
   also used by the clippy-test steps). On failure it `gh run cancel`s the run, so format errors surface in ~30s + queue.
4. `scripts/ci/gh-run.sh`: dispatch (run id from `gh workflow run` output, gh>=2.100 prints the URL; fallback list filter by
   actor/ref/createdAt), wait (60s poll, per-job queue/run and per-step timings), summary (downloads artifacts to
   /mnt/storage/service/tmp/run-<id>-XXXX, prints *.exit, *summary*/verdict/sha256 files, p50/p95/p99/cpu/throughput
   values from JSON, failed log tail, then deletes the dir unless --keep), go = all three.

## Before/after proof
PROOF_PLACEHOLDER

## How to use (workers)
```
scripts/ci/gh-run.sh go rust-ci.yml <branch> package=cadence-history      # dispatch+wait+summary
scripts/ci/gh-run.sh go sdk-matching-benchmark.yml master service=history source_ref=<sha> duration=300
scripts/ci/gh-run.sh dispatch history-mixed-gate.yml <branch> database=mysql   # -> "<id> <url>"
scripts/ci/gh-run.sh wait <id>; scripts/ci/gh-run.sh summary <id> [--keep]
```
- Benchmarks: no temp branches; dispatch at `master` with `source_ref`. Repeats of the same rust tree reuse the binary.
  Final benchmark evidence: add `fresh_build=true`.
- rust-ci: a red `fmt` job (run shows cancelled) = formatting; fix via the existing format_* dispatch inputs
  (e.g. `format_all=true`, download the patch artifact) since formatting also runs on CI only.
- Final validation still: `clean_target=true` for rust-ci, gates already build fresh.
- Max 20 concurrent jobs across all workers: cancel your superseded runs (`gh run cancel <id>`, only your own).
