# Matching PostgreSQL gate: readiness failure shutdown boundary (2026-10-31)

## Evidence identity and result

- Source pin supplied for this investigation: `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`.
- Evidence directory: `/mnt/storage/service/ci-evidence/matching-postgres-41108824183.AY5Gf8/matching-production-postgres/`.
- Build exit `0` (`build.exit`); gate exit `1` (`gate.exit`). Recorded Rust binary SHA256: `bc45dd2dc600599fdd2f1135a58a918c878bb4f9ca93eaac5692f6a5eece778d` (`binary.sha256`).
- `gate/run/probe.log:1-4` says readiness failed through Go Frontend (20s deadline) and direct gRPC (10s deadline), before later probe steps. `:5-7` reports graceful-shutdown deadline exceeded and SIGKILL.
- `gate/run/rust-matching/matching-shutdown.json` records `signal=SIGTERM`, then `childSignal=SIGKILL`, `exitCode=null`, at `completedAtUnixNs` about 20s after `sigtermSentUnixNs`.

## Observed shutdown boundary

The final completed application event in `gate/run/rust-matching/rust-server-before-restart.log` is successful `ListTaskListPartitions`: handler stage `engine-complete` at line 59 and completion `category=ok` at line 60. Startup reached `admission opened` at line 35. There are no `Matching shutdown stage:` lines anywhere in this captured log. The first expected shutdown stage in the pinned launcher path is `membership withdrawal begin` (`rust/crates/cadence-matching/src/launcher.rs:1802-1809`); it is not observed. The stop-trigger marker immediately before it is a separate opt-in trace (`launcher.rs:155-165, 1791-1795`).

Thus the useful observed boundary is: last completed app operation = readiness RPC completion; first expected but unobserved shutdown operation = membership withdrawal. The artifacts do **not** establish whether the process handled SIGTERM, reached the signal-selection branch, or entered a blocked/slow operation before emitting the next ordinary stage line. The JSON proves the harness sent SIGTERM and later SIGKILLed the child, not where Rust was executing. The 20s supervisor deadline bounds elapsed time only; this capture has no shutdown-stage trace samples to localize it. In particular, it does not prove a SIGTERM delivery/handling failure, a metrics flush, or a lock deadlock.

## Source path and waits/locks inside the unresolved interval

After the serving loop selects `shutdown_signal()` (`launcher.rs:1773-1795`; on Unix it waits on Ctrl-C or `terminate.recv()` at `:167-175`), the first normal stage is `matching_membership_lifecycle.withdraw().await` (`launcher.rs:1802-1809`). That awaits a spawned provider `self_evict()` task and its join (`matching_membership_lifecycle.rs:183-200`), then Ringpop self-eviction (`matching_ringpop_provider.rs:400-405`, `ringpop_provider.rs:720-768`). Self-eviction briefly takes the synchronous Ringpop runtime and state mutexes (`ringpop_provider.rs:721-760`), marks the local member faulty, and awaits a JoinSet of direct pings to a randomized bounded subset of alive/suspect peers. Each ping uses the node's configured `ping_timeout` (`:760-767`; timeout handling is in `:1313+`). The evidence does not show whether withdrawal began or which peers/timeout values applied.

If withdrawal completes, launcher next awaits `runtime.shutdown()` (`launcher.rs:1813-1821`). Its initial drain awaits membership eviction if configured, reads live `matching.shutdownDrainDuration` under the admin dynamic-config mutex, and sleeps for that duration when nonzero (`matching_runtime.rs:192-210, 1020-1048, 1213-1225`). The production launcher binds that live provider at `launcher.rs:1324-1334`. Runtime shutdown then joins/stops the dynamic-config watcher, shard distributor tasks, membership maintenance, registry owners and writer actors (`matching_runtime.rs:1053-1130+`). None of these later waits is implicated by the captured log because their preceding `Matching shutdown stage: ... begin` markers are absent.

No implementation change is proposed from this evidence alone. Smallest useful follow-up is another mixed gate capture that retains shutdown markers (including `CADENCE_MATCHING_SHUTDOWN_TRACE`) and the process-level SIGTERM/SIGKILL timestamps; this can distinguish waiting for the signal from withdrawal and later runtime drain/teardown without assigning a cause prematurely.
