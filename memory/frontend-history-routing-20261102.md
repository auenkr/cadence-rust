# Frontend History owner routing — 2026-11-02

Branch: `frontend-history-routing-20261102`. Implementation validation in progress; no service-completion claim.

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

## CI evidence (pending)

- Initial https://github.com/auenkr/cadence-rust/actions/runs/41412292178 — head `4038f2de1`; failed compile because AppState's Debug derive required ClientTls Debug. Corrected with a redacted Debug implementation; CI formatting patch applied.
- Follow-up https://github.com/auenkr/cadence-rust/actions/runs/41412822094 — head `1debbeaff`; pending.
- Final clean workspace CI and mixed gate: pending. Binary SHA256: pending.

## Scope limits

The existing mixed gate uses one Go History instance and proves SDK/restart regression compatibility, not multi-History churn/owner migration. ShardOwnershipLost retry loops and replication fanout are not added by this change. Frontend parity audit remains open for those broader distributed/failure cases.
