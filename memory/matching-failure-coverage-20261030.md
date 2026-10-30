# Matching failure-path test coverage follow-up (2026-10-30)

Base: immutable Matching candidate `22c695d65af367a0b49b605877f72ec33fa2beb8` (the code SHA associated with CI run `41028655670`; validation remains owned by the parent).

Added `refresh_rpc_propagates_engine_error_and_recovers_after_fault_clears` to the crate's `matching_partition_cancellation_tests` unit module. It uses the existing `#[cfg(test)]` `Matching.engine_fault` hook, calls the public `MatchingApi::refresh_task_list_partition_config` RPC, checks the injected message and `Internal` status, confirms the failing request created no durable task list, clears the fault, and confirms the same request succeeds. The Go-style ignored manager-error stub was removed from `matching_engine_go_tests_c.rs` because this internal unit test now supplies deterministic public error-propagation and cleanup/recovery coverage without adding production APIs or changing behavior.

The ignored query-dispatch failure case remains. Its Go oracle fails `DispatchQueryTask` directly; Rust still has no injected dispatch seam, and substituting a no-poller deadline would assert different behavior. A faithful deterministic version requires a narrow test-only dispatch failure hook at the dispatch boundary and observable assertions for the returned dispatch status plus removal of that query's waiter/index/pending offer. No such seam was added in this change.

Validation: no Cargo, Go, formatter, runtime, CI, or PR command was run. The test candidate therefore needs CI validation before its result is treated as established evidence.
