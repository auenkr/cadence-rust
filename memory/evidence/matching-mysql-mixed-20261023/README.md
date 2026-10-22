# MySQL mixed gate (Rust Matching + Go Frontend/History/Worker, no Go Matching), 2026-10-23

Candidate source: origin/matching-candidate-20261023 @ 4f5fb6722 plus 2 small fixes (below). MySQL 8.0.36 container (cadence-mysql-test), Go's v8 schema.
Binaries (sha256 in binaries.sha256): Go server built from this tree (go build -p 1); Rust dev build of cadence-server (cargo build -j2, shared target).
Harness: tests/run-go-service-baseline.py --rust-matching --matching-restart --mysql --history-transport grpc (commands in command-run*.txt).
Node deps (@grpc/grpc-js, @grpc/proto-loader) installed outside the tree at /mnt/storage/service/node-grpc via NODE_PATH (cadence-web submodule is not checked out).

## Exit codes (real)
- docker-pull.exit 0
- go-build.exit 0
- mysql-run.exit 0
- npm-install.exit 0
- run1-failed-nomodule.exit 1
- run2.exit 1
- run3.exit 1
- run4.exit 1
- run5.exit 0
- rust-build2.exit 0
- rust-build.exit 0

## Runs
- run1: exit 1, harness env: @grpc/grpc-js missing (fixed by NODE_PATH).
- run2: exit 1, Rust Matching exited 1 at final SIGTERM: "flushing Matching metrics: Connection refused" (UDP statsd sink 127.0.0.1:8125 absent). Real Rust bug; Go tally never fails shutdown on sink errors. Fixed in main.rs (log to stderr, exit 0). It also masked the next failure in the finally block.
- run3/run4: exit 1, tests/matching-service-compat.cjs:1013 expected gRPC CANCELLED from a poll cancelled by CancelOutstandingPoll. Candidate returns an empty poll response, which is Go behaviour (engine.go ErrNoTasks -> empty response; parity item 4). Stale harness assertion, fixed.
- run5: **exit 0**, result "preliminary-passed" (acceptanceEligible false), goMatchingStarted false.

## run5 scenario results
- SDK sample workflows start/complete (echo, activity retry, child, workflow run retry, workflow task retry, activity timeout, sticky): passed (go-sdk.log, probe exit 0)
- signal workflow (SignalWorkflow "wake"): passed
- activity: passed (heartbeat, retry, timeout)
- query (QueryWorkflow through Rust Matching, RespondQueryTaskCompleted): passed
- isolation-group dispatch + CancelOutstandingPoll: passed
- SIGTERM graceful drain mid-workflow with recovery: passed (1 queued row before signal, 0 after recovery, 1 completion event; Rust pid 465682 -> 465900); final SIGTERM shutdown exitCode 0
- SIGKILL restart mid-workflow, same MySQL: passed (1 durable row before kill, workflow completed after, pid 465563 -> 465682)
- task GC + domain metadata + Go-schema-only MySQL checks: passed
- NOT covered: workflow cancellation (RequestCancelWorkflowExecution) - no scenario exists in tests/go-sdk; not added for lack of time.
