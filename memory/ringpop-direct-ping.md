# Outbound direct Ringpop PING (bounded slice)

Pinned implementation reference: `/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0/swim/ping_sender.go`, `ping_handler.go`, `node.go`, `memberlist.go`, and `disseminator.go`.

`sendPing` calls `Disseminator.IssueAsSender`, constructs the PING from the current memberlist checksum, local address/incarnation, app and changes, then calls exact service `ringpop` / method `/protocol/ping`. `sendPingWithChanges` only emits success after the complete JSON response has decoded. On error it returns without invoking the counter callback. On success `sendPing` invokes the callback before returning. The caller `Node.pingNextMember` then applies `res.Changes` through `Memberlist.Update`. Therefore Rust must bump the sent snapshot before applying response changes: an accepted change replacing a sent address starts with its new zero counter. `Memberlist.Update` returns without applying after stop; the completed send callback still runs.

For a nonempty accepted set, `Memberlist.Update` recomputes the checksum and calls `Node.handleChanges`; the relevant Rust-parity effects are `AdjustMaxPropagations()` then `RecordChange` for each accepted change. The original change's source/source incarnation are retained except local refutation, which creates a new local alive change at the bumped incarnation. Receiver `IssueAsReceiver` behavior is not implemented in this slice; this slice only sends direct PING and applies its response. No ping scheduling, ping-req, retry, bootstrap, or readiness policy was added.

The actual Go peer probe confirmed the receiver returns both its own and the requester's initial local change in the response: the initial local `Change` has empty `source` and source incarnation `0`, so filtering by the top-level PING source does not filter that delta. Rust accepts the Go member and ignores the equal-incarnation change about its own local member. The Go test compares response addresses, membership checksum, receiver-side counters, Rust queued counters, and maxP against the pinned node.

Rust implementation: `RingpopNode::ping` in `rust/crates/cadence-server/src/ringpop_provider.rs`. The pinned Go live peer probe is `tests/run-ringpop-outbound-ping-compat.py` plus `tests/ringpop-outbound-ping-go_test.go`, with the Rust probe in `rust/crates/cadence-server/examples/ringpop_ping_probe.rs`. The focused Rust test checks exact request identity/checksum/change metadata, nil/empty response changes, accepted remote source metadata, local suspicion refutation, maxP and counters, malformed JSON, app errors, timeout, dropped future, and stop-during-successful-response behavior.

Final successful validation (all listed `.exit` files contain `0`) is recorded under `memory/evidence/ringpop-direct-ping/`:

- `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server outbound_ping_applies_only_decoded_response_and_bumps_only_successful_sends --lib` — `provider-focused-verified.log`.
- `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server ringpop_provider::tests --lib` — 13 passed, `provider-regressions.log`.
- `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server tchannel_json_client::adversarial_tests --lib` — 4 passed, `client-regressions.log`.
- `CARGO_BUILD_JOBS=1 python3 tests/run-ringpop-outbound-ping-compat.py` — actual Go v0.10.0 peer, `go-peer-final.log`.
- `CARGO_BUILD_JOBS=1 python3 tests/run-ringpop-outbound-join-compat.py` — JOIN sender regression against Go v0.10.0, `outbound-join-regression.log`.
- `CARGO_BUILD_JOBS=1 python3 tests/run-ringpop-node-compat.py` — Go client against Rust inbound JOIN/PING, `inbound-go-regression.log` (probe artifacts: `/tmp/ringpop-node-compat-n2b2ury8`).
- `CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-server --all-targets -- -D warnings` — `server-all-target-clippy-final.log`.
- `rustfmt --edition 2021 --check` for the touched Rust files, `gofmt -d` for the touched Go test, and `git diff --check` — `format-and-diff-check.log`.

The checked-in `tchannel_json_client.rs` was not changed; its SHA-256 remains `68f2a75e399393c2849e7468fb41aaa7040c2188490ffac339236ce40c4ebf89`. This slice does not complete Matching service parity; do not begin History migration.
