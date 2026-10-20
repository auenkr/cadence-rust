# Production Matching failure/recovery harness

The source-only harness is `tests/run-matching-production-failure-recovery.py`. It invokes the original Go Frontend, History, and Worker services with two Rust Matching processes joined through the configured Ringpop bootstrap and backed by the same SQLite task database. No Go Matching service is started.

Run it after building the production Rust binary:

```sh
python3 tests/run-matching-production-failure-recovery.py \
  --rust-production-binary /absolute/path/to/cadence-server \
  --artifacts-dir /absolute/path/to/matching-production-failure-recovery
```

The probe checks that a request sent directly to the non-owner returns the typed `ABORTED` `TaskListNotOwnedByHostError`, that an actual production idle-manager eviction releases a Go Frontend long poll, and that a queued SDK workflow completes exactly once after crashing the matching owner. It inspects shared SQLite task rows and task-list `range_id`, then verifies the public acknowledgement level advances after recovery. It also requires Go Frontend to serve task-list requests after membership converges on the survivor.

The expected Go behavior was checked against `go/service/matching/handler/engine.go` (`getTaskListManager` rejects a non-owner with `newNotOwnedByHostError`) and `go/service/matching/handler/membership.go` (membership changes unload task-list managers no longer owned). Idle teardown is driven by `taskListManagerImpl` in `go/service/matching/tasklist/task_list_manager.go`, which stops the manager after `IdleTasklistCheckInterval` without recent events; the stop path releases active polls and removes it from the registry. Queue ownership persistence uses task-list range fencing in `go/common/persistence/sql/sqlplugin/sqlite` and the matching SQLite implementation.

The harness source has passed Python compilation, Node syntax validation, and `git diff --check`. No Rust or Go builds, or integration runs, have been performed for this source slice yet; its behavioral evidence remains pending the shared validation lane.
