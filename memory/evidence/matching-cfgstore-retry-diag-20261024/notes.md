# Lane 9 (configstore) workflow-task-retry stall — code-reading diagnosis

Branch: `matching-cfgstore-retry-diag-20261024` (base `origin/matching-gate-final3-20261024`, HEAD 41b680a0f).
Scope: code reading and evidence comparison only. No cargo, no docker, no gate runs, no production change.
Subject evidence: `memory/evidence/matching-gate-final3-20261024/9-configstore/` (run.exit = 1).
Comparison evidence: `memory/evidence/matching-gate-final2-20261023/1-mysql-single/` (exit 0).

## Verdict

**Not a proven Rust decode divergence, and the previous worker's hypothesis is refuted.**
The lane cannot yet be attributed to the seed alone or to Rust alone, because the failing run differs from the
passing run in three ways at once: the Rust matching binary (different source commit), the dynamic-config source
(configstore vs file), and the `--matching-restart` flag. No control run exists that isolates them.

- The hypothesis "the seed turns on tasklist isolation and isolation groups for Rust only" is **false**. Go Frontend/History
  receive the same isolation keys from the file-based config the harness writes for them.
- The only Rust-visible value the configstore changes for matching that differs from the file-based run is
  `matching.longPollExpirationInterval = "2s"` (default 1m). This is a harness-chosen scenario change, asserted by the
  probe. Its link to the stall is not demonstrated (see "Mechanism status").
- The Rust configstore **decode** works: the seed row is read, no decode error is logged, and the seeded values are
  the ones Rust reads.
- No reproducer test was added. There is no demonstrated Rust divergence to pin with an ignored test.

## Hypothesis check: "isolation is seeded for Rust only"

| Claim | Evidence | Result |
|---|---|---|
| Go nodes lack isolation keys | `tests/run-go-service-baseline.py:477-482` writes `system.enableTasklistIsolation: true` and `system.allIsolationGroups: ["zone-a","zone-b"]` into `dynamic_text` whenever `production_matching` is true | **False** — Go gets it too |
| `production_matching` is true in lane 9 | `run-go-service-baseline.py:327` (`'matching' in rust_services`); lane 9 runs `--rust-services matching` (`9-configstore/command.txt`, `tests/gates/run-mysql-lane.sh`) | true |
| Go reads that file | `run-go-service-baseline.py:497` `go_dynamic_text = dynamic_text`; `:504` writes it to `work/dynamic-config.yaml`, which the Go config references (`:534`) | Go reads the same keys |
| Seed isolation values equal file values | seed `:1029-1030` (`"true"`, `'["zone-a","zone-b"]'`) vs file `:479-482` | identical |
| Lane 1 (passing) also had isolation on | `1-mysql-single/evidence/command.txt` has `--rust-services matching`, so `production_matching` true | passing run had isolation on in file mode |

So isolation is **not** a configstore-only difference. The seed matches what Go reads for isolation.

## What configstore changes for Rust (the real delta)

Seed values (`run-go-service-baseline.py:1025-1031`, mirrored in `9-configstore/evidence/results.json` `configstoreSeed.values`):

| Key | Seed value | File-based value (Go and Rust, lane 1) | Rust-visible effect |
|---|---|---|---|
| `matching.longPollExpirationInterval` | `"2s"` (`:1026`) | unset → default | long polls expire every 2 s instead of 60 s |
| `matching.enableStandbyTaskCompletion` | `true` (`:1027`) | `true` (`development.yaml:7-8`) | none |
| `matching.enableClientAutoConfig` | `true` (`:1028`) | `true` (`development.yaml:83-84`) | none |
| `system.enableTasklistIsolation` | `true` (`:1029`) | `true` (`run-go-service-baseline.py:479-480`) | none |
| `system.allIsolationGroups` | `["zone-a","zone-b"]` (`:1030`) | same (`:481-482`) | none |
| `system.enableGRPCOutbound` | `true` (`:1031`) | `true` (`:581-585`, rust file) | none |

Everything else in `development.yaml` (frontend.*, history.*, system.* audit/retention) is **absent** from the Rust
configstore view. Rust matching does not read those keys (`grep` of matching/persistence/service-core readers; the
`system.minRetentionDays` and `frontend.*` reads are outside the matching dispatch path).

Structural difference (not a value difference): in configstore mode Rust drops the file entirely.
- `rust/crates/cadence-matching/src/launcher.rs:564-611`: the ConfigStore arm returns `None` for `dynamic_config_path`,
  so no file overlay is loaded.
- `rust/crates/cadence-service-core/src/admin.rs:61-67` `from_config_store` builds a fresh overlay with only the snapshot's
  entries. Unseeded keys fall back to code defaults. Rust log confirms: `dynamic config integer read failed (value was not found); using default`
  (`9-configstore/evidence/rust-server-logs/rust-server-before-restart.log`).
- Go's configstore client does the same full replacement (`go/common/dynamicconfig/configstore/config_store_client.go:507-529`
  `update()` → `storeValues`), and `dynamicconfigfx/fx.go:93-100` has no file fallback. So this is Go/Rust **parity**,
  not a divergence. The Go nodes in this lane use file config by design (mixed topology), which is a harness property.

## Decode check (Rust configstore reader)

- Seed writes version `-1`, thriftrw with the 0x59 preamble (`go/tests/configstore-seed/main.go:40-49,72-73`).
- Rust reads it: `rust/crates/cadence-persistence/src/config_store.rs:115-139` (decode), `:143-152` (preamble strip),
  `:190-239` (blob → snapshot), `:242-260` (`ORDER BY version LIMIT 1`, sign restored).
- Evidence: `configstore-seed.log` reports `seeded 6 entries version=1 encoding=thriftrw`; Rust log shows
  `initialising ConfigStore dynamic config client` with no decode error. Matches gate-final2's fix (commit 6a6d9fa87).
- Value reads: `system.enableTasklistIsolation` is JSON `true` → `dynamic_config::boolean` (`dynamic_config.rs:173-181`);
  `system.allIsolationGroups` → `string_list` (`dynamic_config.rs:260`). Both decode correctly.
- Snapshot refresh: `rust/crates/cadence-matching/src/matching_runtime.rs:403-430` replaces the overlay every
  `pollInterval` (10s, `rust-matching.yaml`), only on successful fetch. No transient empty overlay.

**Conclusion: the Rust decode is correct for every seeded key. Decode is not the stall.**

## The binaries differ (uncontrolled confound)

| | gate-final2 lane 1 (passed) | gate-final3 lane 9 (failed) |
|---|---|---|
| Rust source commit | `4fef002510` | `1c45b6997` |
| Rust binary sha256 | `6235e025…2547` | `02b78e64…9531c` (`binary-shas.txt`) |
| Go server sha256 | `ecd4eca4…f6eb0bb1` | `86e67bb9…af818e` |
| Dynamic config source | file | configstore |
| `--matching-restart` | yes | no |

- `git diff --stat 4fef002 1c45b69 -- rust go` touches **26 files, ~3,600 insertions**, including
  `matching_dispatch.rs`, `grpc_matching.rs`, `matching_reader.rs`, `matching_registry.rs`, `matching_partition.rs`,
  `task_isolation.rs`, `matching_standby.rs`, `matching_persistence.rs`, `config_store.rs`.
- Notable hot-path changes in that range:
  - `grpc_matching.rs:116-124` `check_poll_context` added to both poll handlers (`~1668`, `~1902`): a poll whose remaining
    deadline is under 10 ms fails with `Internal`.
  - `matching_dispatch.rs:482-512` `accept_forwarded_backlog` second hop now forwards `source_domain` instead of
    `self.domain_id`.
- **final3 ran only lanes 4 and 9** (`matching-gate-final3-20261024/` has `4-cassandra` and `9-configstore` only).
  There is no final3-binary file-mode run. So the failure cannot be attributed to configstore by this evidence.

## The stall in the logs

Go SDK log, `9-configstore/evidence/go-sdk.log`:

| Time (+0530) | Event |
|---|---|
| 04:36:34.858 | Workflow worker started |
| 04:36:35.494 | `activity-retry` activity error (expected) |
| 04:36:51.357 | `workflow-task-retry` panic #1 (expected injected failure) |
| 04:36:51.395 | `workflow-task-retry` panic #2 (expected) — **38 ms later** |
| — | **no further events** |
| 04:37:31.285 | worker stopped; `panic: workflow-task-retry: ... context deadline exceeded` at `tests/go-sdk/mixed_matching.go:84` |

Passing lane 1, `matching-gate-final2-20261023/1-mysql-single/evidence/go-sdk.log`:

| Time (+0530) | Event |
|---|---|
| 01:50:47.9 | worker started |
| 01:50:48.362 | `activity-retry` activity error |
| 01:50:53.158 | `workflow-task-retry` panic #1 |
| 01:50:53.296 | panic #2 (138 ms later) |
| 01:51:07.335 | `cancel` scenario completes, worker stopped, exit 0 |

Observations:
1. Both lanes issue the retry immediately (38 ms vs 138 ms). The third decision in lane 9 is never dispatched to a poller
   within the window. So the stall is in dispatch of the third decision, not in the workflow code (`main.go:83-88`
   returns success on attempt 3).
2. **The harness budget is 60 s for the whole SDK sequence** (`tests/go-sdk/main.go:147`
   `context.WithTimeout(..., 60*time.Second)`). The `context deadline exceeded` is that context, not the workflow's own
   1-minute execution timeout (`mixed_matching.go:78`). The process started ~04:36:31; the failure is at ~04:37:31.
3. **Lane 9 is about 14 s slower before the failing step.** Lane 9 reaches `workflow-task-retry` at 51.36 s, 20 s after
   process start. Lane 1 reaches it at 53.16 s, about 6 s after start. Between activity error and the first retry panic,
   lane 9 takes ~16 s (child + workflow-run-retry run in that gap) versus ~5 s in lane 1. So lane 9 was already slow
   before the stall. That makes the 60 s budget a real constraint, and "slower" and "stalled" are not yet separated.

## Mechanism status (what was checked and ruled out)

- **Isolation routing**: same in both lanes (identical keys). Ruled out as a configstore-only cause.
- **Poller autoscaler reacting to `AutoConfigHint`**: the Go SDK autoscaler is disabled by default
  (`go/.../internal/internal_poller_autoscaler.go:38-43`, `Enabled` false), and the harness does not enable it. Ruled out.
- **Poller history window** (`matching.taskIsolationPollerWindow` = 2 s, read at `grpc_matching.rs:~3420`):
  `cadence-common/src/poller.rs:91-93` and `:109-117` keep outstanding polls regardless of the window, and group counts
  only include non-empty isolation groups. The SDK sends none (`harness worker.Options` has no `IsolationGroup`), so this
  path is identical in both lanes. Ruled out for config-only causes.
- **`stop_if_isolation_changed`** (`matching_registry.rs:328-334`): fires only when `enableTasklistIsolation` changes between
  capture and check. Stable in lane 9. Ruled out.
- **Overlay replacement every 10 s**: successful fetches are identical; no empty window. Ruled out.
- **2 s long-poll expiry** (`grpc_matching.rs:1257-1282`; `poll_timeout = min(configured, deadline - 1s)`; Rust default 60 s at
  `grpc_matching.rs:366`; Go default `time.Minute`, `go/common/dynamicconfig/dynamicproperties/constants.go:2861-2866`):
  this is the **only** config-level delta. No code path found that strands a backlog task when a poll expires after 2 s,
  so the mechanism is **unproven**. It does change poll cadence (empty poll every 2 s), and it changes the scenario the
  SDK sees compared with lane 1. The probe asserts this on purpose (`tests/matching-service-compat.cjs:1169-1179`).

## Why this is not a pure seed/harness verdict either

- The seed changes the scenario: the 2 s long poll is a deliberate harness-chosen value, not what the passing lane ran.
  That is a harness-caused difference, and it is the first thing to bisect.
- The binary changed in the same window. The stall may be a Rust regression in the 4fef002..1c45b69 range, or a
  consequence of the 2 s poll, or both. Neither has been run in isolation.

## Controls that would settle it (not run; no gates per brief)

Run one at a time, in this order, with the same SDK scenarios and the same MySQL lane wrapper:

1. **C0 (discriminator):** final3 binary `02b78e64…` with the **file** config and lane-1 flags (no `--matching-configstore`).
   - Pass → the regression is configstore-specific → go to C1.
   - Fail → regression is in the code range `4fef002..1c45b69` → bisect that range on the workflow-task-retry scenario only.
2. **C1:** final3 binary + configstore seed **without** `matching.longPollExpirationInterval`. If it passes, the 2 s long poll is the
   trigger.
3. **C2 (diagnosis only, separate branch, not a fix):** raise the SDK context from 60 s to 120 s. If the retry then completes,
   the lane is slow, not stalled. The 60 s budget is a harness assumption and should be recorded as such.

Note the constraint: `--matching-restart` is in lane 1 but not lane 9. Keep it out of the C0 control only if lane 1's
restart step is not needed. The current `run-go-service-baseline.py` path that enables isolation does not depend on it
(`:477`), so C0 can omit it.

## Proposed fix (not applied; this brief is diagnosis only)

- If C0 passes and C1 passes: **harness fix.** Stop seeding `matching.longPollExpirationInterval` into the SDK-scenario
  lane. Assert the 2 s expiry in a dedicated probe that does not run the SDK scenarios, or key it to a separate domain.
  The seeded isolation and client-auto-config keys already match the file-based run and do not need changing.
- If C0 fails: **Rust regression fix** in the 4fef002..1c45b69 range. Start with the `check_poll_context` change
  (`grpc_matching.rs:116-124`, `~1668`, `~1902`) and the matching_registry/matching_reader changes. Re-run C0 with the fix.
  The second-hop source-domain change (`matching_dispatch.rs:482-512`) is unlikely to be on this path with one write partition,
  but that depends on whether isolation creates child partitions here, which was not checked.
- Either way, the 60 s SDK budget should be recorded as an assumption in the lane evidence, so a slow run is not reported as
  a stall.

## Reproducer

Not added. The brief asks for an ignored reproducer only when a Rust divergence is established. It is not, so no test
was written. If C0 or C1 later isolates a Rust-side cause, the reproducer should be a Rust-side unit test in
`rust/crates/cadence-matching` that feeds a transient decision through a 2 s long-poll and a backlog task.

## Files read

- `tests/run-go-service-baseline.py`: 327, 475-504, 534, 580-585, 732-740, 1024-1044
- `tests/go-sdk/mixed_matching.go`: 20-87 (scenario list, `:84` error site)
- `tests/go-sdk/main.go`: 83-88 (workflow-task-retry), 147 (60 s ctx)
- `tests/matching-service-compat.cjs`: 1169-1179 (probe asserts 2 s expiry)
- `tests/gates/run-mysql-lane.sh`: lane wrapper (Docker limits, `--rust-services matching --mysql`)
- `go/tests/configstore-seed/main.go`: 40-79
- `go/common/dynamicconfig/configstore/config_store_client.go`: 507-529
- `go/common/dynamicconfig/dynamicconfigfx/fx.go`: 85-100
- `go/config/dynamicconfig/development.yaml`: 1-111 (matching keys: 7-8, 83-84)
- `rust/crates/cadence-persistence/src/config_store.rs`: 26-260
- `rust/crates/cadence-persistence/src/dynamic_config.rs`: 1-200, 260
- `rust/crates/cadence-service-core/src/admin.rs`: 40-100, 167
- `rust/crates/cadence-matching/src/launcher.rs`: 530-690, 1300-1325
- `rust/crates/cadence-matching/src/matching_runtime.rs`: 395-430
- `rust/crates/cadence-matching/src/grpc_matching.rs`: 760-840, 1180-1290, 1665, 1901, 3405-3440
- `rust/crates/cadence-matching/src/matching_registry.rs`: 300-340
- `rust/crates/cadence-matching/src/matching_dispatch.rs`: 462-520
- `rust/crates/cadence-common/src/poller.rs`: 1-140
- Go SDK (`go.uber.org/cadence@v1.4.0-rc.3`): `internal/internal_poller_autoscaler.go`, `internal/internal_worker.go` (isolation wrapper only when `IsolationGroup` set)
- Evidence: `9-configstore/{NOTES.md,EVIDENCE.md,command.txt,run.exit,probe.log,results.json,go-sdk.log,rust-server-logs/*,config-redacted/rust-matching.yaml}`, `1-mysql-single/{command.txt,go-sdk.log}`, `binary-shas.txt`, `binaries.json`, `source.json`
