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
