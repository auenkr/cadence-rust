# Matching failure-path test coverage follow-up (2026-10-30)

Base: immutable Matching candidate `22c695d65af367a0b49b605877f72ec33fa2beb8` (the code SHA associated with CI run `41028655670`; validation remains owned by the parent).

Added `refresh_rpc_propagates_engine_error_and_recovers_after_fault_clears` to the crate's `matching_partition_cancellation_tests` unit module. It uses the existing `#[cfg(test)]` `Matching.engine_fault` hook, calls the public `MatchingApi::refresh_task_list_partition_config` RPC, checks the injected message and `Internal` status, confirms the failing request created no durable task list, clears the fault, and confirms the same request succeeds. The Go-style ignored manager-error stub was removed from `matching_engine_go_tests_c.rs` because this internal unit test now supplies deterministic public error-propagation and cleanup/recovery coverage without adding production APIs or changing behavior.

The ignored query-dispatch failure case remains. Its Go oracle fails `DispatchQueryTask` directly; Rust still has no injected dispatch seam, and substituting a no-poller deadline would assert different behavior. A faithful deterministic version requires a narrow test-only dispatch failure hook at the dispatch boundary and observable assertions for the returned dispatch status plus removal of that query's waiter/index/pending offer. No such seam was added in this change.

Validation: no Cargo, Go, formatter, runtime, CI, or PR command was run. The test candidate therefore needs CI validation before its result is treated as established evidence.

## Follow-up candidate

Based on `741dafd86f52ba7909811a52f49a04603bb592d6`, I reviewed the refresh test above: it exercises the public RPC, asserts the exact `Internal` message, verifies that the injected engine failure occurs before durable queue creation, then clears the fault and verifies a successful refresh of the same partition request.

Added a `#[cfg(test)]` query dispatch failure closure on `Matching`. The public `QueryWorkflow` path invokes it after registering the result waiter and immediately before offering the query to a poller. The new deterministic unit test asserts the returned Internal status, verifies `deliver_query_result` returns NotFound for the captured task ID (the waiter/index was removed), and confirms the queue has no pending query offer. Removed the formerly ignored `query_workflow_failed_to_dispatch_query_task_is_error` integration stub now that the unit test provides executable equivalent behavior and cleanup assertions.

Validation dispatched on the exact pushed source candidate `1c1c6e94d0d7384f89f09920a1452e58643b02e0` using `rust-ci.yml`, with no package input and `clean_target=true`: https://github.com/auenkr/cadence-rust/actions/runs/41029943649 . Conclusion was pending at dispatch; no result was polled. No local Cargo, Go, formatter, or runtime command was run. The task-specific source review used `git diff --check`; it reported no whitespace errors.
