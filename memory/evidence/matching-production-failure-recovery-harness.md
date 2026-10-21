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

## Mixed production evidence and open routing issue (2026-10-20)

A production Rust binary was rebuilt from frozen Matching server sources at HEAD `27268085f442ad21bd4fa431441eb095af216eaf`. The source and Cargo manifest hashes before and after the build matched. Its SHA-256 is `679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa`; a verified copy is preserved at `/tmp/cadence-server-679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa`. Build provenance is in `memory/evidence/matching-dns-build-20261020-2726808/`.

The configured shard-distributor production probe exited 0 against that binary. It ran Go Frontend, History, and Worker with Go Matching absent and Rust Matching using the original Go Ringpop discovery. SDK workflow coverage, query and isolation dispatch, native partition configuration, a durable queued task across SIGKILL/restart, and SIGTERM drain/recovery completed. Go services remained alive. This is bounded mixed-service evidence only (`acceptanceEligible: false` in the probe result), not the Matching parity audit. Raw run evidence is in `memory/evidence/matching-dns-build-20261020-2726808/configured-sd/`.

The first valid two-host run reached the dedicated idle recovery and owner-crash recovery SDK paths, checked both Rust members' ownership maps, and passed persisted range/acknowledgement/task-row assertions before the generic compatibility workflow. It then returned a typed `ABORTED` ownership response for an `isolated-*` task list, with owner `127.0.0.1:44237` and responding host `127.0.0.1:41829`. The original probe trace only retained the generic RPC wrapper stack, so the failing method is unknown. Treat this public routing error as unresolved. The run is preserved under `memory/evidence/matching-dns-build-20261020-2726808/two-host-recovery-rerun/`.

A second diagnostic run did not return a task from the ordinary workflow poll within one response. The probe used a global 3-second `matching.idleTasklistCheckInterval`, so the initial long poll could legitimately be evicted before History enqueued its task. The diagnostic failed at the dereference of the empty response after StartWorkflowExecution, not on an ownership error. Go file-based dynamic config and Rust `task_list_duration` both support `domainName` plus `taskListName` constraints; the override is now scoped to the dedicated production-idle task list. The ordinary SDK probe now retries empty successful long-poll responses for up to 30 seconds and still requires the exact started workflow run.

The Go source audit found matching ownership behavior on both sides: Frontend resolves a matching partition with `membership.Lookup(service.Matching, taskList)` and retries typed ownership errors as transient; Rust reports owner and self from its matching membership resolver. Go Ringpop and Rust use FarmFingerprint32, 100 replica points, and the same identity/address replica-key format. Rust emits the shared-v1 `TaskListNotOwnedByHostError` Any in a google.rpc.Status, and the pinned Go mapper reconstructs that typed error from the same URL and fields. The harness now includes a pinned Go YARPC client oracle that asserts `errors.As` and the owner/self/task-list fields against a production Rust non-owner response.

The corrected harness is committed at `3e9ec5c`. Python compilation, Node syntax validation, gofmt, and diff whitespace checks passed. The scoped idle configuration, empty-poll retry, persisted recovery checkpoints, and Go typed-error oracle have not yet run in the mixed environment; schedule them after the shared MySQL validation lane releases. No service-completion claim is made until that run and the remaining Matching parity audit pass.

## Related focused forwarding and discovery gates

The production Go YARPC forwarding oracle passed three focused Rust tests against the original Go peer: forwarded polls, production handler add/query routing, and spectator failure propagation without Ringpop fallback. Evidence: `artifacts/matching-production-runs/forward-oracle-rerun-2/run.log` (exit 0). This validates those forwarding paths only; it does not establish multi-host public routing or whole-service parity.

The configured shard-distributor gate initially timed out on readiness because its probe accepted `READY` spellings but not the pinned Go enum string `ShardStatusREADY`. The probe now accepts that spelling. The subsequent rebuilt-binary configured run passed as described above. Preserve the initial readiness timeout artifact as a failed attempt when citing the passing rerun.
