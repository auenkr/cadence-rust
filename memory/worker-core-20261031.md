# Worker migration progress — 2026-10-31

Branch: `worker-core`, based on `origin/master` at
`94e79ff9835ec0f0f32b2fa2728510a81892f312`.

## Current checkpoint

This is an initial crate bootstrap and contract inventory, not Worker parity.
Cargo was not run locally. `git diff --check` passed before this checkpoint.

Implemented in this slice:

- Replaced the placeholder Worker crate with modules matching the requested Go
  subsystem boundaries: archiver, asyncworkflow, batcher, diagnostics,
  domaindeprecation, esanalyzer, failovermanager, indexer, parentclosepolicy,
  replicator, scanner, scheduler, workercommon, service, runtime and client.
- Added a Worker-specific configuration facade and the shared CLI/config entry
  point. `version` and `check-config` work against the shared Go-shaped config.
- Added access to the generated public Cadence `WorkerAPI` gRPC client with
  plaintext and TLS endpoint constructors.
- Added explicit lifecycle and workflow-registration primitives.
- Recorded initial Go workflow/task-list/activity/signal/query identifiers.
  Parent-close-policy and batcher contracts include test assertions for Go's
  literal names; more comprehensive Go-source differential fixtures remain.
- Worker startup intentionally errors before polling. The Cadence SDK's
  deterministic history replay, workflow decision runtime, activity execution,
  heartbeat/retry behavior and service dependencies are not ported yet. This
  prevents the new process from accepting tasks it cannot safely execute.

## Remaining work

1. Replace the facade config with Go `worker.NewConfig` dynamic-property
   bindings and validate defaults against `dynamicproperties`.
2. Complete the Cadence client: TLS/auth, discovery, retry/metrics, Frontend
   gRPC and TChannel/Thrift methods needed by internal workers.
3. Implement deterministic replay and decision/activity pollers with exact
   Go SDK history, payload, query, signal, heartbeat, retry, cancellation and
   failure behavior. Add Go-generated task/history fixtures.
4. Port each module's algorithms and dependencies in priority order:
   parentclosepolicy, batcher, scanner/executions, archiver, failovermanager,
   domaindeprecation, scheduler, esanalyzer, indexer, replicator, diagnostics,
   asyncworkflow.
5. Add mixed Go/Rust integration gates for SDK workflows and service calls,
   including failure, restart and recovery. Worker is not complete until those
   gates and the per-module Go parity audit pass.

## Validation evidence

- Local: `git diff --check` passed; no Cargo command was run.
- CI: pending first `rust-ci.yml` dispatch. Record run URL, exact head SHA and
  conclusion here after `gh run view`.

