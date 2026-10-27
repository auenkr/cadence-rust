# Matching partition-config deadline/counter implementation (2026-10-27)
Implements the fixes from matching-partition-deadline-metrics-audit-20261027.md (Go retry.go Do/prevErr verified: first attempt always runs, ctx.Done only in the backoff wait, returns prevErr else err).
- `matching_retry.rs`: `persistence_until(op, cancelled)`; `run` takes `wait`->bool; ServiceBusy (Throttled) delay = max(policy, throttle 1s..10s); tests with fake wait/no sleep.
- `matching_partition.rs`: `update_until(.., cancelled, on_persist_failure)`; callback only on persistence failure (not validate); BadRequest backend kind -> InvalidArgument.
- `matching_registry.rs`: counter emitted inside spawn_blocking (survives dropped gRPC future); cancel = caller deadline, scaler path also queue stopped (Go a.ctx); scaler failures now count. New `update_partition_config_for_domain` passes the resolved domain name.
- `grpc_matching.rs`: removed post-await counter on the queue path; in-memory fallback no longer counts InvalidToken/BadDecision validation errors.
Not validated locally (CI only). Test: `gh workflow run rust-ci.yml --ref <branch> -f package=cadence-matching` and `-f package=cadence-persistence`.
Gaps: no registry-level deadline/scaler counter test with failing fake store yet (needs a fault-injecting store; none located).
