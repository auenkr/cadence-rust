# history-port-task CI status (2026-11-05)

Port of Go go/service/history/task/ and taskdlq/ into rust/crates/cadence-history/src/{task,taskdlq}.

- Branch: history-port-task (master merged in; not merged back, branch kept)
- Green run: https://github.com/auenkr/cadence-rust/actions/runs/41902888431
- Head SHA: 16241c1a16d05821522bcdf7b142309f0eec2e74
- Conclusion: success (workflow rust-ci, package=cadence-history; Format check, Clippy, Test all success)
- Fixes in the final iterations: Cargo.toml merge/dup deps and bin name, jit_duration arg count, clippy lints (needless borrows, question_mark, identical if blocks, while_let_loop, unnecessary_min_or_max, dead time_source field).
