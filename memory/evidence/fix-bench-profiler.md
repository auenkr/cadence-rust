# fix-bench-profiler (2026-11-19)
profile.py checks perf record/script/stackcollapse/flamegraph exit codes and fails on zero samples; run.py records profiler exit in EVENTS, writes -failure.json + profile-failures.json and fails the run when PROFILE=1 profiler fails; teardown terminates+reaps profiler and perf.
- profile=1 (matching): https://github.com/auenkr/cadence-rust/actions/runs/44427899768 — success, head 49ef7e4; 00-rust-profile.svg non-empty, top30 "total samples 1622369880", no profile-failures.json
- profile=0 (frontend): https://github.com/auenkr/cadence-rust/actions/runs/44427904319 — success, head 49ef7e4
