# Backlog forward-to-parent runtime evidence (2026-10-23)

Probe scenario: `CADENCE_MATCHING_BACKLOG_FORWARD=1` (`--matching-backlog-forward`) in
`tests/matching-service-compat.cjs`. Native two-partition config (read/write = 2) on an activity
task list. The child `/__cadence_sys/<root>/1` gets a task with no poller and must persist in its
backlog (`childBacklogRowsBeforeRootPoll`). Only the root is then polled. A second task is put on the
root backlog and only the child is polled (Go `ForwardPoll`, forwarder.go:211).

Go references: matcher.go:216-236 and 373-427 (forward on offer), forwarder.go:117 (ForwardTask),
forwarder.go:211 (ForwardPoll).

| Lane | Exit codes (gate / run) | Probe result | Evidence dir |
| --- | --- | --- | --- |
| MySQL single Rust node | gate.exit 0, run.exit 0 | `preliminary-passed`, `backlogForward` object present | single-node-mysql/ |

Build: `tests/run-go-service-baseline.py --rust-services matching --build-only`, exit 0 (build.exit).
Binary: `cadence-matching` debug, sha256 in `rust-sha256.txt`. Built with
`CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-final`. First build attempt failed (exit 1) because
the worktree lacked the `go/idls` submodule; fixed with `git submodule update --init go/idls`.
The first attempt's log is not kept.
