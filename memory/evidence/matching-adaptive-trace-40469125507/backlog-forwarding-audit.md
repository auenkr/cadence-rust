# Backlog forwarding audit: activity-timeout trace

Source audited: `34ea602c6ceba7fd5da9bc96d28a5a91b7e8fbac` (`matching-retry-trace-20261027-v1` diagnostics). This is the reader/forwarder lane only; refresh fanout and QPS/scaler behavior are out of scope.

## Trace result

The adaptive production probe completed its isolated retry in 37.769s and its full retry case in 23.434s. The failing `activity-timeout` case is workflow run `227957da-d0c6-4d30-b77f-577edf0d381d`. History shows its first decision completed, activity `schedule_id=5` timed out schedule-to-start, and a new decision was scheduled (`event_id=7`). The schedule-7 decision Add was received by child partition 47 at `1793087511132634988` and enqueued at `1793087511964945194`; no matching `poll_returned` with a task token for this workflow appears before SDK failure. The root/parent partition 2 received repeated Adds; at least one child-to-parent forwarding attempt logged `add_forward_failed` at `1793087515939774264`. Subsequent retries continued after the SDK scenario had failed.

The earlier decision's successful `poll_returned` on partition 72 does not establish delivery of schedule 7. The 60-second `getNextDecisionTimeout` seen in the SDK is the already-started timeout while waiting for a decision, not a retry delay. This trace also records many partition-config refresh RPC timeout warnings, but refresh fanout is owned by another investigation and cannot be used to infer a task-reader defect.

## Go/Rust source comparison

- Go `task_reader.go:470-523` keeps the same durable task at the head and retries failed dispatch attempts. `context.DeadlineExceeded` is retryable; cancellation exits only when the tasklist reader is stopping. Its `newDispatchContext` / `getDispatchTimeout` (`:434-468`) calculates a task dispatch deadline from the current tasklist rate, number of buffers, configured async dispatch timeout, and isolation duration.
- Go `forwarder.go:119-166` calls parent `AddDecisionTask` / `AddActivityTask` using that dispatch context. `ForwardTask` has no independent fixed two-second RPC deadline. The forwarder’s outstanding token is held across the RPC and released when it returns.
- Rust `matching_reader.rs` retains the durable head while dispatching. It first offers locally for `local_wait`; it then uses `offer_or_token` to arbitrate between a local poller and an add token. It holds the permit across its RPC and drops it after completion. On a failed attempt it offers locally until the same forward deadline, then retries. Stop cancels the task path.
- Rust `matching_reader.rs` currently sets `forward_deadline = now + 2s` around each backlog RPC and local-offer window. The outer active-domain dispatch deadline can still bound the full attempt. This fixed per-RPC cap is a concrete source difference from Go's use of the computed dispatch context, but this artifact does not reveal whether an RPC exceeded 2s, which status ended each attempt, or whether a parent accepted and enqueued any particular retry. Therefore causality is unproven and no timeout change is justified from this run.
- `matching_sync.rs` carries `forwarded` as transient request state; it is not durable task state. The traced Adds are durable backlog operations; do not conflate History's transient sync-match retries with persisted task reader retries.

## Conclusion and next discriminating trace

Bounded audit: no demonstrated reader/forwarder correctness defect yet. The specific candidate is Rust's fixed 2s per-forward timeout versus Go's dynamically computed dispatch-context deadline, not the SDK's 60s decision wait. To determine whether this explains the incident, record for this task and each attempt: child/parent tasklist, attempt start and end timestamps, exact gRPC status including deadline/cancellation, whether the parent Add accepted/enqueued it, and whether the durable row was completed/acked. Correlate those records with task token return and the computed Go-equivalent dispatch deadline. A Rust attempt that is canceled at 2s while the Go-equivalent deadline remains live would establish the behavioral divergence; absence of that condition would rule it out for this trace. Keep all existing scenario budgets and routing assertions unchanged.

Validation: read-only source and captured-artifact audit; no Cargo/Go commands and no mixed-cluster run were started. Existing diagnostic evidence is from Actions run 40469125507, explicitly `acceptanceEligible:false`; it is not acceptance evidence for a code change.
