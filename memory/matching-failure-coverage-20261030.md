# Matching failure-path test coverage reconciliation (2026-10-30)

## Exact candidate and validation boundary

This ledger was reconciled against supplemental candidate
`565c8fc91893fae1a81cd8e47f10f2ee4744c046` on
`t3/matching-queued-activity-candidate-5ee69b5`, whose ancestry includes the
frozen source `5ee69b5572688c2dba2245c7b411c7182c5b9b16`, `741dafd86`, and
`1c1c6e94d`. It is source-review evidence only. No Cargo, Go, build, format,
test, or runtime command was run locally.

The matching PostgreSQL mixed gate is
https://github.com/auenkr/cadence-rust/actions/runs/41056791529 on the frozen
tag and was still `in_progress` at the post-review check, running
`Mixed Go and Rust validation`. Do not dispatch supplemental CI or a second
mixed gate until that run is terminal. The prior fresh workspace pass
41055162424 applies to the frozen base only, not this supplemental candidate.

## Reconciled findings

1. **Query dispatch error and cleanup — covered in source, candidate CI pending.**
   The formerly ignored integration stub is removed. In
   `matching_partition_cancellation_tests.rs`,
   `query_workflow_dispatch_failure_returns_error_and_cleans_pending_query`
   injects failure through the `#[cfg(test)]` dispatch callback after waiter
   registration and checks the public RPC's `Internal` code and exact message,
   `deliver_query_result` returning `NotFound` for the failed query, and the
   absence of a queued query offer. The seam and test were added by `1c1c6e94d`.
   This corrects the stale audit claim that no dispatch-manager failure
   injection existed.

2. **Refresh error returned by RPC and recovery — covered in source, candidate
   CI pending.** `refresh_rpc_propagates_engine_error_and_recovers_after_fault_clears`
   injects an engine error through the existing test-only fault hook, asserts
   the public RPC returns `Internal` with the exact message, checks that the
   failed refresh did not create a durable task list, clears the fault, and
   succeeds on the same request. The former ignored Go-compatible stub is
   removed. The test was added by `741dafd86`.

3. **Failover callback registration and catch-up — covered in source.**
   Go `handler/engine.go::registerDomainFailoverCallback` seeds the callback
   baseline from all cached domains; later `domainChangeCallback` only
   disconnects eligible global, non-active-active domains whose failover
   notification version advanced. Rust's
   `matching_failover_callback_tests.rs` calls the production
   `MatchingRuntime::start_domain_cache` registration path. It proves initial
   catch-up does not release a blocked poll, filtered local/active-active/stale
   changes do not release it, and an eligible advanced version does release it
   while retaining the same queue owner. A second test covers the native
   registry attached after cache catch-up. The previous selector-only-gap
   claim is stale; no additional failover test is needed from this audit.

4. **Typed error matrix and Thrift wrapper defaults — incomplete as an
   exhaustive source-parity audit; no concrete runtime mismatch established.**
   Go `handler/handler_test.go` has per-method cases for host throttling,
   internal errors, poll timeout configuration, domain lookup, and manager
   failures. Go `wrappers/thrift/thrift_handler_test.go` asserts error
   conversion and returned default values for Health, AddActivityTask,
   AddDecisionTask, CancelOutstandingPoll, DescribeTaskList,
   ListTaskListPartitions, PollForActivityTask, PollForDecisionTask,
   QueryWorkflow, and RespondQueryTaskCompleted. Rust has public RPC engine
   error tests and Thrift exception mapping tests for untyped NotFound,
   ResourceExhausted, DomainNotActive, RemoteSyncMatched, StickyWorkerUnavailable,
   and QueryFailed errors, including method-specific exception fields and
   required Thrift fields. These are representative, not a cross-product of
   every Go error case over every Rust transport method. Rust's transport
   encodes a Thrift application exception rather than exposing Go's internal
   `(default response, error)` pair, so the Go wrapper's default-value
   assertions are not directly observable in that same form. Keep this item as
   audit-only coverage debt; add a narrowly scoped test only if a specific
   externally visible mapping or default discrepancy is identified.

The corrected source audit closes stale items 1–3 from
`matching-validation-audit-20261030.md`. Item 4 remains an explicit, bounded
coverage limitation. This ledger does not establish full Matching parity or
service completion.

## Validation evidence

The source test candidate still requires fresh, package-unset
`rust-ci.yml` CI with `clean_target=true` after gate 41056791529 is terminal.
Do not treat source presence as passing test evidence. No mixed-cluster result
or Matching completion is claimed here.
