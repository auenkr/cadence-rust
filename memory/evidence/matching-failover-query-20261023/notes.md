# Matching failover / standby query parity — 2026-10-23

Base: `92668eb2f` (`origin/master` at worktree creation). Worktree:
`/mnt/storage/service/github/wt/failoverq`, branch `matching-failover-query-20261023`.
Audit input: `origin/matching-parity-audit-v4-20261023:memory/matching-go-parity-audit-v4-20261023.md`, P1-9/P1-11/P1-16.
`go/idls` initialized at `bfbf273b41b9e36ce21e47a49600d22d73e4614c`.

## P1-9: audit premise corrected; existing key retained

Go does define `matching.errorInjectionRate`. This is not an alias for persistence injection:

- Go `go/common/dynamicconfig/dynamicproperties/constants.go:2514` declares MatchingErrorInjectionRate; `:5424` defines the exact key and default 0. The suggested older path `go/common/dynamicconfig/constants.go` does not exist at this revision.
- Go `go/client/clientfactory.go:183` reads it once and wraps the outbound Matching client with `errorinjectors.NewMatchingClient` (after the timeout wrapper). Matching uses this client for partition forwarding.
- Go `go/client/wrappers/errorinjectors/matching_generated.go:203` wraps PollForDecisionTask; `go/common/errors/fake_errors.go:49` implements forwarding selection, `:69` random error selection. ServiceBusy/Internal do not forward; Timeout/Unhandled forward with probability 1/2; the injected error wins even if the underlying call succeeds.
- Rust `rust/crates/cadence-matching/src/launcher.rs:650` installs that same client rate once. `matching_fault_injection.rs:242` stores it, `:248` draws errors, `:260` implements forwarding and always returns the injected error. `matching_dispatch.rs:151`, `:204`, `:363` apply it to decision/activity polls and task forwarding. The independent persistence key is read at `launcher.rs:654`.
- No key removal or behavior change is warranted. Existing tests `fake_error_selection_and_forwarding_follow_go` and `client_injection_skips_forwarding_for_busy_and_always_returns_fake` exercise selection and forwarding behavior; included in the final crate test run below.

## P1-11: standby query forwarding and completer counter

- Go `go/service/matching/tasklist/matcher.go:490` PollForQuery serves queryTaskC and propagates matcher cancelCtx, then `:511` pollOrForward acquires a forwarding token and forwards to the parent; on failure it returns to local polling. Go `task_list_manager.go:651` configures standby completion separately from worker polling.
- Rust `grpc_matching.rs:1783` selects the query-only forward path for standby domains; the local query channel remains in the enclosing select (`:1747`). `matching_dispatch.rs:897` forwards the decision poll with poller/domain/parent identity and does not claim durable backlog. The new parent-server regression verifies the returned parent response and forwarded fields with an existing durable task remaining untouched.
- Fixed: the entire query-forward future now subscribes to the dispatcher's blocked-poller release before it starts waiting. Local wait, request-token wait, parent resolution and the parent call can all be cancelled by failover; the overall expiration also covers resolution. Go propagation is at `matcher.go:501`.
- Go `go/service/matching/tasklist/task_completer.go:125` retains young missing executions (strictly greater than 24h is required to complete); `:162` counts final errors except domain-active and task-not-started, including context cancellation/deadline errors. Rust previously represented young-missing as Ok(None), excluding it from the failure counter on retry exhaustion.
- Fixed in Rust `matching_standby.rs:40`: young-missing has a distinct error sentinel, old missing completes, and `:50` counts final failure once. `:66` counts dropped in-flight checker futures (reader shutdown/context cancellation); normal completion and exhaustion disarm that cancellation guard. Active-domain results return without failure; task-not-started remains excluded.
- Added tests: `standby_cancelled_checker_counts_one_completion_failure`, `standby_completion_failure_excludes_not_started_but_counts_young_missing`, `matching_standby_query_poll_forwards_to_parent_without_claiming_backlog`, `matching_standby_query_failover_cancels_each_forward_wait`, `matching_standby_query_failover_releases_wait_and_refreshes_for_next_poll`.

## P1-16: callback effects

- Go `go/service/matching/handler/engine.go:1707` advances the callback baseline and selects eligible domains (`:1813`: global, not active-active, newer failover notification version). `:1721` enumerates normal decision/activity and sticky decision root names; `:1762` obtains/loads their managers and releases blocked pollers.
- Go `go/service/matching/tasklist/task_list_manager.go:807` checks stopped state, calls DisconnectBlockedPollers, then RefreshCancelContext. It does **not** stop/reset/unload managers, clear their backlog, or reset the task-list registry. `matcher.go:508` refreshes the cancellation context for subsequent polls.
- Rust `matching_runtime.rs:471` catches up to the current maximum version and `:482` handles notifications; `:705` has the same eligibility/version selection. `matching_registry.rs:583` derives root names from loaded managers and releases normal decision/activity and sticky decision managers. `:557` documents retained state. No manager reset is needed for parity.
- The missing query-forward release listener is fixed at `matching_dispatch.rs:897`. Notify waiters registered for existing polls are released; new waits register independently, providing the refreshed-context behavior.
- The active forwarded-poll leg needed the same release propagation: `matching_dispatch.rs:1050` now closes the waiting response receiver when its poller is still registered as blocked. The detached worker remains responsible for task recovery; the remote call already observes receiver closure, and `:713` now makes parent resolution observe closure too. Local History delivery after EndPoll is allowed to finish, as Go only disconnects matcher waits. New regressions: `matching_failover_releases_active_forward_resolver_and_poller_registration` and `matching_failover_keeps_local_history_delivery_after_end_poll`.
- The focused standby evidence predates this additional active-forward fix; the final full crate test and clippy below must cover the final source hashes.
- Existing regression evidence included in final crate test: `failover_callback_selection_matches_go_and_advances_baseline`, `matching_rpc_domain_failover_releases_blocked_polls`, `matching_failover_callback_disconnects_sticky_decision_and_keeps_manager`, `matching_failover_callback_excludes_sticky_activity_and_other_domains`. New query tests exercise reusable release signaling, local wait, exhausted request tokens, pending resolver, and deadline handling.

## Validation

Focused command: `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared /mnt/storage/service/bin/cargo-safe test -p cadence-matching --lib standby` exited **0**, 10 passed, 0 failed (`focused.log`, `focused.exit`). Executed against source commit `016462f42` (full SHA in `focused-source-commit.txt`; source hashes in `focused-source-sha256.txt`).

Final crate test: `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared /mnt/storage/service/bin/cargo-safe test -p cadence-matching` exited **0** on 2026-10-24 (Asia/Kolkata). Library: 448 passed, 0 failed, 3 ignored; main/doc tests: 0; two Go-dependent integration tests ignored. `test.log` and `test.exit` record the run. The seven new regressions all passed, including active forwarding and preserving local History delivery. Final code is `fb380b574` (full SHA in `source-commit.txt`, hashes in `source-sha256.txt`; hashes rechecked after the run).

The ignored library tests are the existing activity source-domain parity gap and two Go TChannel oracle tests. Integration ignores are Go History delivery and Go shard-distributor interoperability. They were not enabled or claimed as evidence here.

Pending: final clippy. All cargo commands run from the worktree `rust/` directory through cargo-safe with the user-requested shared target, in tmux session `cadence-rust`. No mixed-cluster gate is claimed by this change. This is unit and local RPC-server evidence, not a full service parity or deployment claim.
