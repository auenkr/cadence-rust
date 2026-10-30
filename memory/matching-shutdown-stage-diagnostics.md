# Matching shutdown stage diagnostics (unvalidated source change)

Base: 6c73d9aa447171d4eeda37d71dd0a955d0996a9b. Context: memory/matching-shutdown-failure-41094318642.md (commit 822bda9c4, branch origin/t3/matching-shutdown-diagnosis).

Existing unconditional markers kept untouched: launcher membership withdrawal / runtime shutdown / membership teardown begin+complete.

Added, opt-in via env `CADENCE_MATCHING_SHUTDOWN_TRACE` (any value), format `Matching shutdown trace unix_ns=<ns>: <fixed stage>`:
- stop trigger received (signal vs listener/fatal; replaces missing signal-receipt marker)
- resolved shutdownDrainDuration_ms (logged each time the live provider is read, i.e. once per drain)
- provider self_evict begin/complete; membership worker abort; provider stop begin/complete
- listener abort begin / join complete / tchannel abort complete; metrics flush begin/complete

Behavior, timeouts and cancellation unchanged. Stages inside MatchingRuntime::shutdown (registries, writers, semaphores) are NOT instrumented (scope: launcher/lifecycle only).

Limitations: no Cargo/build/test/CI run; compile unverified. The workflow/harness does not yet set the env var; the next gate must export CADENCE_MATCHING_SHUTDOWN_TRACE=1. A long drain may be expected (drain unknown); this does not assert a deadlock.
