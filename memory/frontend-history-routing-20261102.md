# Frontend History owner routing — 2026-11-02

Branch: `frontend-history-routing-20261102`. Scoped implementation and validation complete; no Frontend service-completion claim.

## Behavior and source references

- `go/common/util.go:WorkflowIDToHistoryShard`: `farm.Fingerprint32([]byte(workflowID)) % uint32(numberOfShards)`; use configured shard count and the existing Go-compatible signed-byte FarmHash helper.
- `go/service/history/lookup/lookup.go:HistoryServerByShardID`: ring key is **string(rune(shardID))**, not decimal digits. The live Ringpop provider filters `cadence-history`, retains membership identity for ring placement, then resolves named TChannel/gRPC endpoints.
- Workflow calls select the owner on every request, including start/signal/query/describe/reset, task-token completion/heartbeat, and PollMutableState. A missing resolver or empty ring falls back to the configured History endpoint. Membership errors propagate instead of silently falling back.
- Production Admin CloseShard/RemoveTask/ResetQueue/DescribeQueue route by shard. DescribeHistoryHost follows Go selector precedence (shard, workflow, explicit membership address) and resolves the gRPC named port. Routed Admin channels retain the configured History TLS policy.
- DescribeShardDistribution uses Go PageID * PageSize bounds, membership identities and `unknown` on lookup failure. Both Thrift and gRPC preserve pagination; HTTP uses query parameters.
- `handler_admin_history.rs:HistoryProxy` is an isolated typed helper, constructed only by its tests in the current tree. The production Admin paths are service-core handlers reached by TChannel/gRPC.

## Tests

- Frontend `history_routing_tests`: hash vectors, live membership changes, empty/uninstalled-ring fallback, resolver failures, start/execution/reset-sticky/task-token selectors.
- Hash expected values were calculated using a small Python transcription after **reading** Go source, without executing Go: go-farm `3414d57e47da` `fp_generic.go`, `farmhashcc.go:hash32Len0to4`, `farmhashmk.go:hash32Len5to12`, `basics.go:mur/fmix`; signed int8 bytes, u32 wrapping. Inputs include ASCII, UTF-8 `é`/replacement rune and 5–12 byte workflow IDs; shard counts 1/4/1024/16384.
- Service-core `shard_distribution_paginates_and_reports_unknown_membership`: page bounds, final partial page, empty/default page and absent membership.
- No local Cargo/rustfmt/Go build or validation commands were run.

## CI evidence

- Initial https://github.com/auenkr/cadence-rust/actions/runs/41412292178 — head `4038f2de1`; failed compile because AppState's Debug derive required ClientTls Debug. Corrected with a redacted Debug implementation; CI formatting patch applied.
- Follow-up https://github.com/auenkr/cadence-rust/actions/runs/41412822094 — head `1debbeaff`; Clippy passed, then superseded/cancelled for final workspace validation.
- https://github.com/auenkr/cadence-rust/actions/runs/41413364086 — head `9f3681186`; superseded/cancelled after recovering the CI formatting patch.
- https://github.com/auenkr/cadence-rust/actions/runs/41413822026 — head `a224ff5b9bfcaefde8ed00ec4a25c4407e64309b`; fresh workspace Clippy passed and all five new routing tests passed; workspace tests exposed two stale, unchanged local Admin assertions. Tests expected fabricated membership without a resolver and successful replication without a History client. Updated only those expectations to the existing production behavior; no runtime logic changed for these failures.
- Final fresh workspace run https://github.com/auenkr/cadence-rust/actions/runs/41415396963 — head `e0ac162ba9cec2a77fea8aa1c769ceec28973c19`; **success**. Fresh uncached `/home/runner/work/_temp/cargo-target-final`; workspace Clippy followed by workspace tests. Artifacts confirm workspace formatting, runtime formatting, Clippy, Frontend formatting and tests all exit **0**.
- Final mixed run https://github.com/auenkr/cadence-rust/actions/runs/41415440353 — **success**, Rust and gate head both `e0ac162ba9cec2a77fea8aa1c769ceec28973c19`; **34/34 checks pass**, including SDK workflows/query/signal, Frontend SIGKILL/restart, both CLI transports. No Go Frontend runs.
- Mixed binary SHA256: `f0274bf5c58c62f126b5fb9d3ac9215b046c220afa895fea13f297749abfadbe`, fresh `/tmp/cadence-cargo-target-fe-41415440353/debug/cadence-server`.
- Retained [gate steps](frontend-history-routing-20261102-steps.json). Reproduce with `gh workflow run frontend-mixed-gate.yml --ref frontend-history-routing-20261102 -f rust_ref=frontend-history-routing-20261102`; retrieve `gh run download 41415440353 -n frontend-mixed-gate -D <directory>` while retention permits.
- Reproduce final CI: `gh workflow run rust-ci.yml --ref frontend-history-routing-20261102 -f clean_target=true` (package omitted for final workspace validation). The earlier targeted Frontend runs were superseded by this stronger full-workspace gate.
- Retained [validation identity and exits](frontend-history-routing-20261102-evidence.json). The final documentation-only commit follows the validated source head above; Rust source is unchanged.

## Scope limits

This checkout’s existing mixed gate records 34 checks (not the earlier audit’s 46): SDK/Frontend-restart regression compatibility with one Go History instance. It does not exercise Matching restart, Schedule APIs, multi-History churn or owner migration. ShardOwnershipLost retry loops and replication fanout are not added by this change. Frontend parity audit remains open for those broader distributed/failure cases.
