# History startup gap work (2026-11-06, branch history-gap-startup)

Ported: go/service/history/service.go -> rust/crates/cadence-history/src/service.rs; lookup/*.go -> src/lookup.rs; real main.rs + src/launcher.rs (config, HistoryEnv, ShardRegistry, HandlerImpl, HistoryServer gRPC+TChannel, ringpop membership, graceful stop).

Fixes:
- Shard ring key now `string(rune(shardID))` (Go) instead of decimal string (controller.rs).
- JSON `ringpop` service served on the History TChannel port (tchannel.rs) so gossip bootstraps.
- HistoryServer::start_with_listeners, RunningServer::stop_now added.

Wrapper audit: ratelimited wrapper has every Handler method; allowFunc on describe/signal/signal-with-start/start matches Go.
(Domain/workflow-ID empty validation is in allow_wf_id, same order as Go.)

Remaining gaps:
- Queue processors unwired: `UnwiredQueueFactory` returns empty processors (no QueueFactory impl exists; needs visibility writer + archival client).
- history `rpc.tls.enabled` rejected at startup.
- ConfigStore / OpenFeature dynamic config unsupported (FileBased/Nop only).
- grpc / thrift wrapper audit not completed in this pass.

CI evidence: see below (filled after run).
