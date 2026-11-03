# Frontend audit residuals (2026-11-04)

Branch `t3/frontend-audit-residuals` (master merged in). Head under test: fccbd6567.

## Dynamic-config keys wired (Go defaults)
- `frontend.disableListVisibilityByFilter` (false) -> `DynamicVisibilityFilterPolicy`; ListSchedules falls back to ListWorkflowExecutions query like Go.
- `frontend.emitSignalNameMetricsTag` (false) -> `signalName` metric tag on SignalWorkflowExecution.
- `frontend.decisionResultCountLimit` (0) -> RespondDecisionTaskCompleted rejects `decisions > limit` with BadRequest "Decision result count exceeds limit."
- `frontend.enableActiveClusterSelectionPolicyInStartWorkflow` (false) -> redirection resolver drops requested cluster attribute when off.
- Not done (documented gaps): graceful failover watcher keys (`system.enableGracefulFailover`, `frontend.domainFailoverRefreshInterval*`), `system.enableDomainAuditLogging`; keys with no Go Frontend consumer (e.g. `frontend.visibilityListMaxQPS`) left unread.

## Metrics diffs fixed
- Sink emits only `operation`, `domain` (blank -> `_unknown_`), optional `signalName`; no more `wf-*` log tags.
- Added `cadence_requests_per_tl`, `cadence_latency_per_tl(_ns)` for the 8 per-tasklist operations; domain resolved from task token for Respond*/heartbeat ops.
- Unit tests in `wrappers/metrics_sink.rs`.

## Timestamp unit bug (root cause of Cassandra/MySQL gate flake)
Go worker panic `timestamp ... after 10000-01-01` (`Seconds: 1793743359616951, Nanos: 114000000`) = ns passed to ms-based `timestamp()`. Fixed to `timestamp_nanos` (Go `unixNanoToTime`) for: HistoryEvent.event_time, expiration/first_run_at (history.rs), PollForDecisionTask/ActivityTask scheduled/started/attempt times (mod.rs), cross-cluster first_run_at/event_time (ns). Unit test `nanosecond_timestamp_tests`.

## Evidence
- rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/41728534783 success (clippy+fmt+tests).
- Cassandra gate (kafka, two_history): https://github.com/auenkr/cadence-rust/actions/runs/41729951504 success, head fccbd6567, rust binary sha256 2b643beaca1f3bac88093c16a6fa52438a720221070dd1918be962a6ff5bd93a.
- MySQL gate (kafka, two_history): https://github.com/auenkr/cadence-rust/actions/runs/41731905202 success, head fccbd6567, rust binary sha256 105af0183f19e4097f57b3ada0b25c63c43cda945ae9bed0d3050315f70c8108.
- Prior failing master gate: run 41725448591 (global-shadow-local steps), same panic.
