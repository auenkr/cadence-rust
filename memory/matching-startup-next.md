# Matching queue startup concurrency (2026-10-16)

Prior writer checkpoint 0fc071855 is pushed; read matching-writer-next.md.

Implemented change: QueueRegistry serializes creation/cleanup per task-list key
using weakly retained mutex gates, and releases registry.state before lease I/O.
No duplicate same-key acquisition; unrelated loaded owners remain available.
Shutdown closes admission immediately, and a startup finishing after shutdown
must stop/clean its unpublished owner and return Unavailable instead of inserting.
The blocking worker remains alive when the caller cancels, preserving ownership.

New real-SQLite contention test blocks a lease behind BEGIN IMMEDIATE and checks
loaded-owner availability plus shutdown/publication fencing. Existing concurrent
same-key acquisition, stale teardown, fencing, idle/controller tests also run.
Validation PASSED: 15 registry tests and strict workspace all-target Clippy. Evidence: /tmp/cadence-startup-check.log and .exit (registry tests),
/tmp/cadence-startup-lint.log and .exit (strict all-target workspace Clippy).
Mixed Go Frontend/History/Worker + Rust Matching and seven SDK scenarios also
PASSED: /tmp/cadence-startup-mixed.exit = 0; artifacts:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-startup-mixed/results.json.
Full 345-test/26-stage writer regression is the preceding checkpoint; only focused
registry/lint/mixed validation was rerun for this incremental concurrency fix.

This is an incremental concurrency fix. It does not implement Go's complete
registered-but-loading manager API/startWG or root partition initialization.
Reader buffering, dynamic config, lease retry, sync/query/forwarding, discovery,
backend selection and transports remain incomplete. All four roles incomplete.
