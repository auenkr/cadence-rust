# PostgreSQL mixed gate (Rust Matching + Go Frontend/History/Worker, no Go Matching), 2026-10-23

Tree: branch matching-pg-mixed-20261023 (a34060ddf + cancel scenario). PostgreSQL 17-alpine container cadence-pg-test (127.0.0.1:5432), empty DBs `cadence`/`cadence_visibility`; Go `--auto-setup` applied Go's own PG schema (postgres plugin). Binaries: Rust dev build (cargo build -j2 -p cadence-server), Go built with go build -p 1; sha256 in binaries.sha256. Commands: command-run*.txt.

## Exit codes (real)
- rust-build.exit 0; go-build.exit 0
- run1-failed.exit 1: my error - pre-loaded schema.sql conflicted with Go --auto-setup ("relation domains already exists"). Not a Rust bug. (pg-schema-preload-unneeded.exit 0 is that preload.)
- run2.exit 0: result "preliminary-passed" (acceptanceEligible false), goMatchingStarted false, probeExitCode 0.

## run2 scenarios (all within the one exit-0 run; evidence in run2/)
- SDK samples (echo, activity retry, child, workflow run retry, workflow-task retry, activity timeout) + signal: passed (go-sdk.log)
- NEW workflow cancellation (RequestCancelWorkflowExecution with pending activity -> run.Get CanceledError, DescribeWorkflowExecution closeStatus CANCELED): passed (go-sdk.log "workflow cancellation ended Canceled")
- Activity heartbeat/timeout, query, isolation + CancelOutstandingPoll, SIGTERM drain+recovery, SIGKILL restart recovery: probe exit 0 (probe.log, graceful-drain-sdk.log, restart-sdk.log "resumed and completed")
- Final SIGTERM: matching-shutdown.json exitCode 0
Caveat: per-scenario detail beyond these logs was not re-audited line by line; Go-schema-only check (check_go_schema_only) is MySQL-specific and not run for PG.
