# Matching partition-config kind gate (audit P0-2) — 2026-10-23

Branch: `matching-partition-kind-20261023` (from origin/master). Audit source:
`memory/matching-go-parity-audit-v4-20261023.md` on `origin/matching-parity-audit-v4-20261023` (P0-2).

## Enum numbering (verified, not assumed)

| Value | Thrift `go/idls/thrift/shared.thrift:333-337` | Proto `go/idls/proto/uber/cadence/api/v1/tasklist.proto:50-55` (used by Rust) | Go types (`go/common/types/shared.go:7893-7897`) |
|---|---|---|---|
| 0 | `NORMAL` | `TASK_LIST_KIND_INVALID` | `TaskListKindNormal` (iota 0) |
| 1 | `STICKY` | `TASK_LIST_KIND_NORMAL` | `TaskListKindSticky` |
| 2 | `EPHEMERAL` | `TASK_LIST_KIND_STICKY` | `TaskListKindEphemeral` |
| 3 | — | `TASK_LIST_KIND_EPHEMERAL` | — |

The proto numbering is shifted by +1 relative to Thrift/Go types, and proto 0 is INVALID.

## What Go actually accepts (gRPC path)

1. gRPC handler: `go/service/matching/wrappers/grpc/grpc_handler_generated.go:83-85` calls
   `proto.ToMatchingUpdateTaskListPartitionConfigRequest` (`go/common/types/mapper/proto/matching.go:657-667`).
2. `ToTaskList` (`go/common/types/mapper/proto/api.go:4029-4034`) calls `ToTaskListKind` (`api.go:4051-4062`):
   - INVALID (0) → `nil`
   - NORMAL (1) → Normal; STICKY (2) → Sticky; EPHEMERAL (3) → Ephemeral
   - **any other value (negative or >3) → `nil`** (default `return nil`)
3. Handler: `go/service/matching/handler/engine.go:1257` reads `request.TaskList.GetKind()`;
   `go/common/types/shared.go:7827-7832` returns the zero value (Normal) when Kind is nil.
   So `engine.go:1257-1259` rejects only Sticky and Ephemeral.
4. Refresh: `engine.go:1292-1293`, same rule.
5. Error text: `&types.BadRequestError{Message: "Only normal tasklist's partition config can be updated."}`.

Conclusion for gRPC matching: **Go accepts proto 0, 1, and every unknown value; rejects 2 and 3.**

## Thrift path

- `go/idls/thrift/matching.thrift` has no `UpdateTaskListPartitionConfig` or `RefreshTaskListPartitionConfig`
  procedure (grep of `go/idls/thrift/*.thrift` for `PartitionConfig` finds only `sqlblobs.thrift`).
- Go TChannel client returns `BadRequest "Feature not supported on TChannel"`:
  `go/client/wrappers/thrift/matching_generated.go:80-82`.
- No Go Thrift server handler exists (`go/service/matching/wrappers/thrift/` has no such method).
- Rust: `rust/crates/cadence-matching/src/thrift_matching.rs` `dispatch_inner` resolves the procedure with
  `cadence_thrift::service::find_method`, which fails with `Unknown Matching procedure ...`. The gate is
  therefore unreachable over Thrift in both implementations. Covered by the new test
  `matching_thrift_has_no_partition_config_procedure`.

## Rust before this change

- `rust/crates/cadence-matching/src/grpc_matching.rs:3056` (update): `matches!(task_list.kind, 0 | 1)`.
  Accepted 0 and 1, rejected 2, 3, **and unknown values** (Go accepts unknown values). Differs from Go on unknown values.
- `grpc_matching.rs:3182` (refresh): same rule. Same difference.

## Change made

- `grpc_matching.rs` update: `let kind_is_normal = !matches!(task_list.kind, 2 | 3);`
  (`kind_is_normal` also feeds the non-registry store path, so that path now matches Go too.)
- `grpc_matching.rs` refresh: `if matches!(task_list.kind, 2 | 3) { return invalid_argument(...) }`.
- Tests (`rust/crates/cadence-matching/src/matching_rpc_tests.rs`):
  - `matching_rpc_partition_config_kind_gate_matches_go`: table over kinds
    `0 (INVALID) accept`, `1 (NORMAL) accept`, `2 (STICKY) reject`, `3 (EPHEMERAL) reject`,
    `4 (unknown) accept`, `-1 (unknown) accept`. Checks both update and refresh on gRPC, using
    code `InvalidArgument` and the Go message.
  - `matching_thrift_has_no_partition_config_procedure`: Thrift dispatch rejects both procedures as unknown.
- Previous test `matching_rpc_partition_config_rpcs_reject_non_normal_kinds` only covered 2 and 3, and its
  comment said "invalid maps to normal" without a test for it. Replaced by the table above.

## Go-side observations outside this task (not changed)

- Frontend admin path `go/service/frontend/admin/handler.go:1933-1938` rejects a nil kind ("Task list kind not set.")
  and any kind other than Normal before calling matching. Rust admin proxy `grpc_admin.rs:535-563` does not check
  kind at the frontend and forwards `unwrap_or_default()`, so it reaches the matching gate. That is a frontend
  parity gap and belongs in the Frontend audit.
- Go matching gRPC: a nil `TaskList` gives name `""` and kind Normal. Rust returns `"TaskList is not set."`
  (`grpc_matching.rs:3034`). Not a kind-only difference; left for the matching audit.

## Verification

- Test: `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared /mnt/storage/service/bin/cargo-safe test -p cadence-matching --lib partition_config`
  (log: `/tmp/pk.log`, exit: `/tmp/pk.exit`) — see result below.
- Filtered (`partition_config`): 2 passed, 0 failed (exit 0).
- Full: `cargo-safe test -p cadence-matching`: 442 passed, 0 failed, 3 ignored (exit 0), log `/tmp/pkfull.log`.
- `cargo-safe clippy -p cadence-matching --all-targets -- -D warnings`: exit 0, log `/tmp/pkclippy.log`.
- Target dir: shared (`/mnt/storage/service/cargo-target-shared`), not the final dedicated dir. This is a
  parity fix, not a final gate; rerun in `cargo-target-final` before any gate claim.
