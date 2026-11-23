# FE gap #3: schedule APIs run the local handler inside the redirection policy

Branch `fe-gap-3-schedule-redirect`. No local cargo, go or docker was run.

Go behavior: the schedule wrapper calls the local handler from the policy callback, so a local `DomainNotActive` can be retried on the active cluster.

Changes:
- `workflow_api.rs`: new `LocalCall` (owned `Arc` closure), `RedirectedReply`, `WorkflowApi::redirect_with_local` (default: serve locally) and `dispatch_redirected_with_local`. The error encoding is shared as `encode_call_error`.
- `yarpc.rs`: `dispatch_with_transport` builds an owned local closure over the existing handler dispatch (`dispatch_local`, the old body) and passes it to the policy for redirected schedule ops.
- `redirection_runtime.rs`: `ScheduleRedirect::route_with_local` runs local or forwards per policy. `local_reply_to_result` turns a local `DomainNotActiveError` into a typed error for the retry. Any other local application or transport error is kept and returned unchanged unless a later forward replaced it.
- `workflow_backend.rs`: `redirect_with_local` override.
- Tests: `local_domain_not_active_is_typed_for_the_policy`, `local_success_and_other_errors`, `local_domain_not_active_is_forwarded_by_policy` (local "a" DomainNotActive with active "b" is forwarded to "b").

## CI (head d38a7b852)
- rust-ci cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/45215590700 success
- rust-ci cadence-service-core: https://github.com/auenkr/cadence-rust/actions/runs/45215599026 success
- frontend-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/45216877211 success, `GATE SUMMARY failed: []`
- Earlier failed runs: 45214766973 (lifetime errors, fixed by the owned closure), 45214606050 (rustfmt).
