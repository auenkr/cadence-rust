# Matching lifecycle recovery candidate — 2026-10-30

Candidate branch: `t3/matching-lifecycle-recovery` (isolated from
`origin/master` at `9f020a466`). Matching remains incomplete. No merge or
service-completion claim is made.

## Source review

- Reviewed `origin/t3/matching-semaphore-startup-repair` at source
  `f4ffd30798955dabe34bbd0efe5736bacad17f61`. The source commit itself records
  formatting corrections in `launcher.rs`, `matching_runtime.rs`,
  persistence `dynamic_config.rs`, service-core `admin.rs`, and OpenFeature
  `runtime.rs`; its ancestry contains much broader Matching work. The combined
  candidate was not merged wholesale. Run
  https://github.com/auenkr/cadence-rust/actions/runs/40912650991 failed
  Clippy at `matching_reader.rs:792` and `matching_reader_native.rs:669` with
  `clippy::let_and_return`; the test step did not run.
- Reviewed `origin/t3/invalid-ticker-recovery` at
  `4ec0bb103f98b3fc5065e861223536ac0be0e229` and its behavior commit
  `2e2b400f955433402c8f7e0f0413904ba29489f1`. Run
  https://github.com/auenkr/cadence-rust/actions/runs/40912942632 passed its
  Go oracles and earlier format steps, then failed the Matching runtime format
  gate in `admin.rs` and OpenFeature `runtime.rs`; Clippy and Rust tests were
  skipped. The head's atomic import correction alone is not runtime evidence.
- Go source review: `go/service/matching/liveness/liveness.go` sets liveness to
  started, increments its wait group, then constructs a ticker at `ttl / 2`.
  `task_list_manager.go` starts the writer before liveness and starts the task
  reader afterward. A non-positive ticker interval therefore panics after
  writer start but before reader start; `Stop` waits on the unmatched wait
  group before stopping children. Rust now retains that partially started
  manager, returns Go's recovered panic text, reuses its lease after config
  recovery, and leaves shutdown waiting on that manager.
- The ticker fix was adapted to the current registry's ack-schedule and owner
  cleanup code instead of importing its older branch wholesale. Native reader
  start is gated so it does not run after the simulated liveness panic; normal
  reader construction still uses the started path. The regression covers
  zero, negative, and one-nanosecond intervals, lease/queue reuse, the blocked
  shutdown boundary, explicit test cleanup, and normal shutdown.
- `3d36aef92` on the ticker branch binds additional dynamic configuration to
  the domain name resolved when a Go manager is constructed. That behavior was
  reviewed but not ported in this narrow patch. Current Rust provider closures
  may resolve a renamed domain on later reads; domain rename semantics remain
  an open parity item.
- The typed semaphore-errors branch was not integrated. Semaphore startup
  fixture corrections from `c37d8a8` were reviewed but are not part of this
  patch; they address a separate configured-semaphore test fixture path.

## Candidate changes

- Removed the two Clippy `let_and_return` bindings while preserving timeout
  calculation and deadline arithmetic.
- Added a start-state seam to the native reader and retained invalid-liveness
  startup state through registry lookup, owner reuse, idle maintenance and
  shutdown.
- Added `invalid_liveness_ticker_keeps_partial_manager_for_recovery` as a
  focused regression.

## Evidence and remaining work

No local Cargo, Go, formatting, server, or runtime command was run. Only
`git diff --check` was used as a source hygiene check; it does not validate
compilation or behavior. Focused CI was dispatched on GitHub Actions:

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41021537962
- Head: `ffa57359f801aeb2f5f60d6383a3675443f7cfe9`
- Scope: `rust-ci.yml`, package `cadence-matching`, standard hosted runner
- Observation: in progress at dispatch inspection; no conclusion available.

Do not treat focused CI as Matching completion: fresh workspace gates and the
mixed Go-service/Rust-Matching SDK failure, restart, and recovery lanes remain
unverified, along with the broader Matching parity audit.
