# Matching CI recheck evidence — 2026-10-29

Run [40841353614](https://github.com/auenkr/cadence-rust/actions/runs/40841353614)
completed successfully on exact source head
`e84e51a1c5a08cc89df0a04ccf69a77e284bd1c4` (workflow
`rust-ci`, job `clippy-test`; started 08:19:55Z, completed 08:28:13Z).
GitHub job records show Clippy, Formatting, and Test all succeeded. The sole
`rust-ci-logs` artifact was downloaded and inspected; its retained `clippy.exit`,
`fmt.exit`, `format.exit`, `matching-runtime-format.exit`,
`workspace-format.exit`, `tls-format.exit`, `query-format.exit`, and
`test.exit` are all `0`. The Rust target was the run's fresh
`/home/runner/work/_temp/cargo-target-final` directory.

## Exact test-log evidence

The artifact's `rust/test.log` reports the Matching library test binary at
**975 passed, 0 failed, 2 ignored**. This is that binary's result only, not a
workspace-wide aggregate. Separate test binaries reported their own totals;
there is no combined aggregate claimed here.

Named passing assertions relevant to this recheck:

- Six `grpc_matching::cold_domain_metric_tests` cases passed: empty-name
  bad-request handling, panic capture from blocking resolver work, metric
  resolution for a domain created after cache scan, ordinary resolver failure,
  cache error classification, and read-through for name-based listing.
- `matching_service_native::tests::two_thread_cold_lookup_does_not_invert_overlay_and_limiter_locks`
  passed (the coordinated two-thread lock-order regression).
- The semaphore manager **test binary** reported **14 passed, 0 failed,
  0 ignored**. Its cases include loading outside the creation lock, a request
  during load obeying its own deadline, and starting an unstarted registered
  manager. These 14 cases are not the service-wide or CI test total.
- Four native semaphore startup tests passed: independent startup timeout and
  cleanup/reacquire, raw ownership error detail, cached raw owners with fast
  path disabled and cancellation, and slow-start continuation/reuse after
  caller cancellation.
- `go_handler_long_polls_without_deadline_return_context_timeout_not_set`
  passed in the matching poller test binary (that binary: **15 passed**).

Attribution boundary: a different `matching_engine_go_tests_c` binary reported
**16 passed, 0 failed, 4 ignored** and finished in **30.01 seconds**. That
30.01-second duration is the native test binary's wall time, not a passing
30-second no-deadline service regression. The named no-deadline test above
passed separately in 0.03 seconds and exercises handler behavior. Do not
conflate either result with the semaphore manager's 14-case total. The log
contains no standalone native-service 30-second no-deadline test result, so
this run does not establish that regression as covered.

## Scope and remaining work

This is CI evidence for the exact `e84e51a` source only. It does not validate
later source changes, including ownership regression `12c7099c3` or the future
native scenario/configuration fixes. It does not establish a native mixed-Go
lane pass or Matching service completion. The focused formatted run
[40842468415](https://github.com/auenkr/cadence-rust/actions/runs/40842468415)
was dispatched at exact source
`721e196d31b2cd7876fc09086c8ac4b2a8995ba3`; this checkpoint does not inspect
or claim its result. The formatting dispatch source
`1228a9e3f5bd05f486aae2e3ca72bede0d0e94a2` is not the exact validated source.

Frozen baseline `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` remains unchanged;
no native mixed-lane pass is credited. Terra workers `b07bde6f` (config-map
coverage) and `baad2545` (native scenarios) remain active. Source freeze and
fresh workspace validation must wait for their integration. No source, workflow,
or mixed-cluster changes are included in this evidence update.
