# Matching native checkpoint — 2026-10-28

Matching remains the current service and is incomplete. Do not start History migration or claim SDK compatibility from additive component tests.

## Saved implementation

Origin/master was at `a56aece3a0db43625d1f887fdeaaafd9ed32aabb` before this checkpoint. The pushed foundation includes byte-valued persistence backends, durable queue, writer/reader/sync actors, partition cache, query delivery, History gRPC/TChannel clients, forwarding, registry, scaler, raw status transport and native add/poll/query engine cores. Their presence does not mean the production launcher uses them end to end.

This checkpoint saves the completed spectator transport regression, query rendezvous edits and control RPC cores with their notes. Parent registered the control module and its formatter entry, exposed the awaited query forwarding-token API, and made the spectator fixture use distinct owners for ff, fe and U+FFFD so cache aliasing is detectable. All checkpoint code remains unverified by execution.

## Validation pin

- Active CI: https://github.com/auenkr/cadence-rust/actions/runs/40685362770
- Exact head: `a56aece3a0db43625d1f887fdeaaafd9ed32aabb`.
- Observed in progress: oracle/format preparation passed, Clippy running, tests pending. No overall pass claimed; this excludes checkpoint edits.
- Previous selected CI success: https://github.com/auenkr/cadence-rust/actions/runs/40681965498 at `adc67085f487975d4c7517a54908b797c0e6e322`. This is an earlier source pin, not validation of current master or service completion.
- Previous run https://github.com/auenkr/cadence-rust/actions/runs/40684693575 at `59c82e795912153df27cb30726531759f4f51fc1` failed. Preserve the newer run's result before dispatching a replacement.

No local Cargo, Go, rustfmt or heavy validation ran while saving this checkpoint. `git diff --check` is the local source check. Commit uses `[skip ci]` to preserve the active earlier-pin run. Next integration needs fresh-target CI with runner formatting; final acceptance needs workspace Clippy followed by workspace tests and mixed gates with Rust binary SHA256, exact head, URL and conclusion recorded.

## Worker handoff

Both existing Codex GPT-6.1 Terra workers completed: query rendezvous (`8abc84a7-e129-43a7-8e7f-ee09c94072fc`) and native control cores (`7d664a0d-fbbb-42bc-94f0-bdb643dc3e25`). No replacement worker started during checkpoint. Latest model preference: Codex GPT-6.1 Terra for hard tasks, Luna for simple tasks. Keep progress updates asynchronous through delegation completion notifications; do not poll running delegates.

## Remaining work

1. Resolve the query selection protocol: an already parked local poller must win Go's initial nonblocking handoff before a ready forwarding token. The cancellable offer/token race alone does not guarantee this. See `matching-native-query-rendezvous-20261028.md`.
2. Complete real native handler and launcher integration: startup/admission, quota, domain cache, failover, ownership checks in Go order, live configuration, discovery, scaler callbacks, metrics and both transports. Registering engine cores does not wire production RPCs.
3. Run CI for this checkpoint, fix compile/test/lint failures, and apply runner-produced formatting. Preserve exact immutable validation pins.
4. Run mixed Go/Rust SDK and service gates with Rust Matching replacing Go Matching, including forwarding, cancellation, persistence backends, failure, restart and recovery. Complete the service parity audit before moving to History.

Detailed source seams: `matching-native-production-seams-20261028.md`, `matching-native-engine-control-20261028.md`, `matching-native-engine-poll-20261028.md`, `matching-native-engine-query-20261028.md`, and the query rendezvous note. Older notes describe intermediate state and must be reconciled with the later commits and CI evidence.
