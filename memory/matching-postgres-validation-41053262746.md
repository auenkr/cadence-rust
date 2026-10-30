# Matching PostgreSQL validation checkpoint — 2026-10-30

## Candidate CI proof

- Repository: `auenkr/cadence-rust` (public, standard GitHub-hosted runner).
- Source branch: `t3/diagnose-matching-gate-failure`.
- Exact source head: `a534baae8c921e0ec8109527c6bc3184937b0323`.
- CI run: https://github.com/auenkr/cadence-rust/actions/runs/41040870450
- Conclusion: `success` (completed 2026-10-30; rust-ci job 124013046223).
- Downloaded artifact `rust-ci-logs` (artifact ID 12969689288). `rust/commit.txt` matches the exact source head; `rust/target-dir.txt` is `/home/runner/work/_temp/cargo-target-final`, a fresh dedicated target. Job log records `PKG:` empty. `rust/clippy.exit` is `0`, then `rust/test.exit` is `0`. Formatting gates also exited zero.
- `rust/test.log` contains 57 successful test-result rows: 1,813 passed, 0 failed, 30 ignored. This is the workspace run, not a single-package run.
- The validation job used GitHub Actions standard `ubuntu-latest`; no local Cargo/Go validation was run.

## Pinned PostgreSQL mixed gate dispatched

- Workflow: `.github/workflows/matching-production-validation.yml` from exact source head above.
- Branch/ref: `t3/diagnose-matching-gate-failure`, verified to point to the exact SHA at dispatch.
- Lane input: `postgres`.
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41053262746
- Head SHA: `a534baae8c921e0ec8109527c6bc3184937b0323`.
- Dispatch response and the single immediate run-list inspection showed status `queued`; no conclusion is available yet. Do not poll. Resume by inspecting this run once after it becomes terminal; if it failed and artifacts exist, use the request traces capped at 32 lines to identify the last completed boundary. Record actual binary SHA256, nested exit codes, and assertions reached from the artifact.
- Before dispatch, the workflow run list showed no active matching-production-validation run; only completed runs were listed. The workflow concurrency group is `matching-production-validation`, with cancel-in-progress enabled.
- The pinned workflow defines job-level `CADENCE_MATCHING_LIST_PARTITIONS_TRACE: "1"`, so the variable is inherited by the Python harness and its Node/Rust child processes. It builds cadence-matching in `CARGO_TARGET_DIR=/tmp/cargo-target-final-${{ github.run_id }}`, records `out/binary.sha256`, then runs the PostgreSQL lane with `--probe-list-partitions` and the other configured probes.

## Corrected prior-run interpretation

The prior run 41039044942 reached startup complete. The direct probe was `MatchingAPI/ListTaskListPartitions`, not a health check. The ring-removal lines were teardown. Go parity uses a 3-second cache deadline. No pipe/watch failure was demonstrated.

## Evidence provenance

Artifacts were downloaded locally under `/tmp/run41040870450-artifacts` for read-only inspection. This note is the only repository change in this tested-candidate evidence commit; no source or workflow files were changed.
