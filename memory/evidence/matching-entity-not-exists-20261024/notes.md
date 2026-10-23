# Matching domain-not-found: EntityNotExistsError gRPC detail (2026-10-24)

Branch: `matching-entity-not-exists-detail-20261024`, based on `origin/matching-semaphore-rpc2-20261024` (b6eee17ac).
Residual gap from `memory/evidence/matching-semaphore-rpc2-20261024/notes.md`, "Error detail on NotFound".

No cargo was run locally. All compile and test evidence is from GitHub Actions `rust-ci.yml`, package `cadence-matching`.

## Go reference

- `go/common/persistence/sql/sql_domain_store.go` (~L224) and `nosql_domain_store.go` (~L148) return
  `EntityNotExistsError{Message: "Domain <id or name> does not exist."}` with no cluster fields.
- `go/common/types/mapper/proto/errors.go` `fromEntityNotExistsError` maps it to gRPC `NotFound` with
  `apiv1.EntityNotExistsError{CurrentCluster, ActiveCluster, ActiveClusters}`. The domain path sets none of
  them, so the detail is empty. The type URL is `type.googleapis.com/uber.cadence.api.v1.EntityNotExistsError`.

## Rust change

- `rust/crates/cadence-matching/src/grpc.rs`: new `domain_not_found(message)`, built on `detailed_status`
  with `p::EntityNotExistsError::default()`. Status code and message are unchanged.
- The domain-store error (`EntityNotExistsError`, from `DomainStore::describe`, `domain_id`, `is_active_in`,
  and `execution_store`, which resolves the name through `describe`) is now mapped with that helper at every
  matching-crate site that previously called `Status::not_found(e.message)`:

| File | Sites | Lookup |
|---|---|---|
| `grpc_internal.rs` | `History::domain_name` (L23, used by MatchingApi quota and payload checks); `execution_store` sites at L117, 237, 281, 328, 357, 557, 697, 765, 830, 1048 | describe by ID / name |
| `grpc_matching.rs` | `is_active_in` at L1747, 1982, 2205, 2420; `execution_store` at L2265, 2478; `domain_id` at L2777, 2838 | active-cluster, name-to-ID, store |
| `grpc_admin.rs` | `domain_id` at L549 (`UpdateTaskListPartitionConfig`) | name-to-ID |
| `matching_standby.rs` | `is_active_in` L114, `describe` L123 | active-cluster, describe |
| `grpc.rs` | `execution_store` in `domain()` L222; Frontend `describe_domain` L516 | describe / store |

Not changed (not domain-store errors): `ManagerError::NotFound` (semaphore), `RecordError`/`TaskError`/`WorkflowMutationError`
NotFound (execution or task), `mutable_state_view` and `describe_admin` (execution), and `matching_partition` storage NotFound.

## Tests

- `matching_rpc_tests.rs`: new helper `assert_domain_not_found` checks the NotFound code, the
  `Domain <uuid> does not exist.` message, and that the detail decodes as an empty `EntityNotExistsError`
  under the api.v1 type URL.
- `matching_rpc_semaphore_domain_not_found_matches_go` (existing, an RPC path through `History::domain_name`) now uses the helper.
- `domain_not_found_status_carries_entity_not_exists_detail` (new): unit test of the helper's wire format.

## Residual differences (not changed)

- `"Domain not set on request."` (the `describe` store error for a request with neither name nor UUID) now also
  carries the detail. Go's behaviour for this input was not checked. Not verified whether any matching RPC can reach it.
- Only the semaphore RPC path and the helper are tested end to end. The other sites use the same helper
  but have no per-site test.

## CI evidence

| Run | Commit | Result |
|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/40032279886 | `89727371a` | **Success** (`clippy-test`). `cadence-matching` lib tests: **572 passed, 0 failed, 12 ignored**. `domain_not_found_status_carries_entity_not_exists_detail` and `matching_rpc_semaphore_domain_not_found_matches_go` are `ok`. Baseline before this change was 571 (`matching-semaphore-rpc2-20261024`), so the only addition is the helper test |

Re-run: `gh workflow run rust-ci.yml --ref matching-entity-not-exists-detail-20261024 -f package=cadence-matching`.

## Trim review (2026-10-24, branch `matching-entity-not-exists-trim-20261024`)

Scope rule: this service is Matching only. A changed site stays only if it is Matching-reachable and Go's
equivalent path returns `EntityNotExistsError` for the same condition. Otherwise it is reverted.
Line numbers are from the pre-trim branch (`660ac74db`). Go lines are from `go/service/matching/handler/engine.go`
unless noted.

### Decision table (24 sites)

| # | Rust site | Enclosing RPC | Go equivalent | Decision |
|---|---|---|---|---|
| 1 | `grpc.rs` L222 `Frontend::domain` (`execution_store`) | Frontend query/list RPCs (`grpc.rs` L646–1021) | Frontend-owned, Go frontend API. Not a Matching path | **Revert** (Frontend-only) |
| 2 | `grpc.rs` L516 `describe_domain` | Frontend `DomainApi` | Go frontend rejects an empty domain with `BadRequestError "Domain not set on request."` (`go/service/frontend/validate/errors.go` L32), not NotFound | **Revert** (Frontend-only; Go's class differs for the empty-name case) |
| 3 | `grpc_internal.rs` L23 `History::domain_name` | Shared by `Matching::domain_name` (`grpc_matching.rs` L1596), which ~20 Matching RPCs call (poll, add, query, semaphore, partitions, cancel-poll, respond-query, describe-task-list) | `handler.go` `domainName()` swallows the error, but the first engine domain lookup returns the store's `EntityNotExistsError`, e.g. `getOrCreateSemaphoreManager` L454 and `AddDecisionTask` L540 | **Keep** |
| 4 | `grpc_internal.rs` L117 `start_workflow_execution` | History `WorkflowApi` adapter | Go History service | **Revert** (History-only, not reachable from Matching) |
| 5 | `grpc_internal.rs` L237 `refresh_workflow_tasks` | History adapter | Go History service | **Revert** |
| 6 | `grpc_internal.rs` L281 `describe_mutable_state` | History adapter | Go History service | **Revert** |
| 7 | `grpc_internal.rs` L328 `schedule_decision_task` | History adapter | Go History service | **Revert** |
| 8 | `grpc_internal.rs` L357 `remove_signal_mutable_state` | History adapter | Go History service | **Revert** |
| 9 | `grpc_internal.rs` L557 `reapply_events` | History adapter | Go History service | **Revert** |
| 10 | `grpc_internal.rs` L697 `record_decision_task_started` | History adapter | Go History service | **Revert** |
| 11 | `grpc_internal.rs` L765 `record_activity_task_started` | History adapter | Go History service | **Revert** |
| 12 | `grpc_internal.rs` L830 `record_child_execution_completed` | History adapter | Go History service | **Revert** |
| 13 | `grpc_internal.rs` L1048 `mutable_state_response` | History adapter helper (only History callers, L254/266/276) | Go History service | **Revert** |
| 14 | `grpc_matching.rs` L1747 `poll_for_decision_task` (`is_active_in`) | Matching poll | `PollForDecisionTask` → tasklist `NewManager` → `task_list_manager.go` L169 `GetDomainName` error returned | **Keep** |
| 15 | `grpc_matching.rs` L1982 `poll_for_activity_task` (`is_active_in`) | Matching poll | Same path as row 14 | **Keep** |
| 16 | `grpc_matching.rs` L2205 `add_decision_task` (`is_active_in`) | Matching add | `AddDecisionTask` L540 `GetDomainName` error returned | **Keep** |
| 17 | `grpc_matching.rs` L2265 `add_decision_task` (`execution_store`, requeue) | Matching add | Guarded by the same L540 check; this is the Rust-only persistence step | **Keep** |
| 18 | `grpc_matching.rs` L2420 `add_activity_task` (`is_active_in`) | Matching add | `AddActivityTask` L623 `GetDomainName` error returned | **Keep** |
| 19 | `grpc_matching.rs` L2478 `add_activity_task` (`execution_store`, requeue) | Matching add | Guarded by the same L623 check | **Keep** |
| 20 | `grpc_matching.rs` L2777 `get_task_lists_by_domain` (`domain_id`) | Matching admin read | `GetTaskListsByDomain` L1226 `GetDomainID` error returned | **Keep** |
| 21 | `grpc_matching.rs` L2838 `list_task_list_partitions` (`domain_id`) | Matching admin read | `getAllPartitions` L1325 `GetDomainID` error returned | **Keep** |
| 22 | `grpc_admin.rs` L549 `update_task_list_partition_config` (`domain_id`) | Matching admin write (via Admin server) | `UpdateTaskListPartitionConfig` L1250 `GetDomainName` error returned | **Keep** |
| 23 | `matching_standby.rs` L114 `checker` (`is_active_in`) | Background completion check, not an RPC | `task_completer.go` L94 `GetDomainByID`, consumed in-process by `errors.As` (L121) | **Revert** (see below) |
| 24 | `matching_standby.rs` L123 `checker` (`describe`) | Same | Same | **Revert** |

Totals: **10 kept** (rows 3, 14–22), **14 reverted** (rows 1, 2, 4–13, 23, 24).

### Why the standby checker is reverted (row 23–24)

Matching-owned, and Go returns the typed error in-process. But the checker's `Status` feeds the task-reader retry
loop (`matching_standby.rs` L51–61, `matching_registry.rs` ~L1397) and never becomes an RPC response. Go's
`EntityNotExistsError` is not a wire-visible value here either. The detail would not be observable, so the
change is reverted.

### "Domain not set on request." (residual gap from the first pass)

Go returns `BadRequestError` (InvalidArgument), not NotFound, for an empty domain (`validate.ErrDomainNotSet`).
The only Rust path that reaches the empty-name describe error is `describe_domain` (row 2), which is now
reverted. No Matching RPC reaches it: `Matching::domain_name` always passes a UUID, and `History::domain_name`
rejects a non-UUID first. So this residual is gone, and the existing (pre-branch) NotFound code for that Frontend
path is unchanged. Fixing it is a Frontend task.

### Tests

- `matching_rpc_semaphore_domain_not_found_matches_go` still exercises row 3 (`Matching::domain_name` →
  `History::domain_name`), so it still covers a kept site.
- `domain_not_found_status_carries_entity_not_exists_detail` tests the shared helper, which Matching still uses.
- No test referenced a reverted site.

### CI evidence

| Run | Commit | Result |
|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/40033588757 | `06f9b0fa5` | **Success** (`clippy-test`, `gh run watch --exit-status` exit 0). `cadence-matching` lib tests: **572 passed, 0 failed, 12 ignored**, same as the untrimmed branch. `domain_not_found_status_carries_entity_not_exists_detail` and `matching_rpc_semaphore_domain_not_found_matches_go` are `ok` |

Re-run: `gh workflow run rust-ci.yml --ref matching-entity-not-exists-trim-20261024 -f package=cadence-matching`.
