# Mixed run 41331739932: infrastructure failure

Run https://github.com/auenkr/cadence-rust/actions/runs/41331739932 ended at approximately 17:43 UTC on 2026-11-01. Both workflow and Rust source head were `f5d2151f9dd0aa0237fda69072cb2a451eefb609`.

Run conclusion is failure; job conclusion is cancelled. The check annotation states: `GitHub Actions has encountered an internal error when running your job.` Runtime Gate step was terminated with exit 143 after approximately nine minutes. Upload and post steps were skipped. `gh run download -n frontend-mixed-gate` returned `no valid artifacts found to download`. Consequently no SDK, service-survival or recovery outcome can be established from this run. Do not infer a Frontend behavior failure from the runner termination.

Fresh Rust build succeeded; build log records cadence-server SHA256 `55beb22dbf3a9e2780480ba912b94910d8969ef49b526633d1f83330689f1192`. Go build also succeeded. Full available job log was inspected via gh; no runtime scenario output was emitted before termination because the gate redirects it to a file.

No live mixed gate remains. Do not rerun the stale-state/visibility candidate while the two assigned Luna implementation tasks are still pending. Integrate their reviewed fixes, then dispatch one new pinned mixed candidate and preserve its actual artifacts.

Separately, async version framing fix `3e573ec0857308b9e47b51cf62810d611b9dad5d` is under Frontend package CI https://github.com/auenkr/cadence-rust/actions/runs/41332920846 with format_all=true; last observed in progress. Redundant automatic push run 41332920687 was cancelled in favor of this explicit package run. This is not mixed validation or final workspace acceptance.
