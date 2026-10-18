# Outbound JOIN refutation repair (2026-10-18)

Current branch started at parent `9fb9a46cc7636e0bbd083d6eb45ebb40fbb63e29`; candidate reviewed: `9770e3870f1b6994ad6014c1e4e05e0a496732a`.

The candidate's `join_seed` applied `Memberlist::update`, then cleared accepted remote addresses from the dissemination queue. It omitted the state-change handling that Go runs between those operations. In the pinned `github.com/uber/ringpop-go v0.10.0` source, `swim/memberlist.go:AddJoinList` calls `Update`; `Update` calls `Node.handleChanges(applied)` for a nonempty accepted set; `swim/node.go:handleChanges` first calls `disseminator.AdjustMaxPropagations()`, then records each applied change. `swim/disseminator.go:AdjustMaxPropagations` calculates `pFactor * ceil(log10(NumPingableMembers()+1))`. `memberlist.go:Pingable` counts nonlocal `Alive` and `Suspect` only. Finally, `AddJoinList` clears each accepted address except the local node. Therefore an accepted local suspect/faulty update must remain queued as a local refutation, accepted remote snapshot entries are removed, unrelated pending entries remain, and maxP is recomputed after merge.

Rust `join_seed` now follows that order and skips applying a successful response if the local node stopped while JOIN was in flight, matching Go `memberlist.Update`'s stopped guard. A deterministic Go generator calls original `AddJoinList` for equal/newer local suspect/faulty changes with labels and preexisting dissemination entries; its fixture captures final members, checksum, queued changes, propagation counters, and maxP. A Rust differential test replays the exact inputs and compares all of those outputs. Separate tests cover stopped response, dropped future, deadline, malformed body, and nil/empty membership.

The parent-integrated JSON TChannel client was preserved byte-for-byte. SHA-256 for both `HEAD:rust/crates/cadence-server/src/tchannel_json_client.rs` and the worktree file: `68f2a75e399393c2849e7468fb41aaa7040c2188490ffac339236ce40c4ebf89`.

Candidate integration order: original candidate `9770e3870f1b6994ad6014c1e4e05e0a496732a` was transplanted onto parent `9fb9a46cc7636e0bbd083d6eb45ebb40fbb63e29`; the transport client was retained exactly from that parent. The Go fixture SHA-256 is `3a986174860269d6143e019e5d3226389c9cf8f5ed2cc5a970271b9e49ab92b8`.

Validation completed in the `cadence-rust` tmux session. Exact commands and saved outputs:

- `python3 tests/run-ringpop-join-addjoinlist-oracle.py` — passed; log `original-go-oracle-generate.log`.
- `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server ringpop_provider::tests --lib` — 12 passed; `provider-focused.log`.
- `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server tchannel_json_client::adversarial_tests --lib` — 4 passed; `json-client-adversarial.log`.
- `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server ringpop_provider::tests::join_addjoinlist_matches_original_go_refutation_fixtures --lib` — 4 original-Go cases passed; `go-differential.log`.
- `CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-server --all-targets -- -D warnings` — passed; `server-all-target-clippy.log`.
- `CARGO_BUILD_JOBS=1 python3 tests/run-ringpop-outbound-join-compat.py` — Rust outbound JOIN passed against real Go Ringpop v0.10.0; `original-go-join.log`.
- `CARGO_BUILD_JOBS=1 python3 tests/run-ringpop-node-compat.py` — Go client passed against the Rust node's real TChannel JOIN/PING endpoint; `inbound-go-transport.log`.
- `rustfmt --edition 2021 --check rust/crates/cadence-server/src/ringpop_provider.rs` and `gofmt -d tests/ringpop-join-addjoinlist-oracle_test.go` — passed.

Every corresponding `.exit` file records `0`. Full Matching service parity remains incomplete; do not advance to History.
