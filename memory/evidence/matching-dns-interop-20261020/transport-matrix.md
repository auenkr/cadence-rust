# Matching shard-distributor and mixed transport evidence (2026-10-20)

The Rust Matching shard-manager interop fix and production mixed probes passed
on this worktree. This evidence does not complete the Matching parity audit.

## Frozen source and binary

- Rust service source commit: `7030f2ef62b9cd2c402569ea3b5b2f67806a64e7`
  (tree `3039c36e9bd26afad3632e0d9d9dc878f99ec59e1e`, source archive SHA-256
  `3259bf2fe0007f96ca4a88e7ce41bad0465498f6a0404989d6516cc0ae17044c`).
- Production binary: `/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target/debug/cadence-server`, SHA-256
  `19a97efebf19183c589e96a56a61119425e7ef0752fdcadf089e4a0fdf31956a`.
  Build exited 0; see [final-build](final-build/).
- Final harness commit: `9e4559a92b6c61786da52fda13d19e3006268950`
  (tree `4fdb446517e494b0e84c3a5b9778a9670cd3f912`, source archive SHA-256
  `5e45980c93da20033b4bc4cd4f9408b44efdd98291e6095b837eb1c5463d9e76`).
- Harness changes after the Rust build only split the Go and Rust dynamic-config
  files and correct the reported config path/metadata; Rust source and binary
  remained unchanged. Rust reads `rust-dynamic-config.yaml`; Go services keep
  `dynamic-config.yaml` and its default `system.enableGRPCOutbound=true`.
- Each runner result records `goMatchingStarted: false`, the original Go
  Frontend/History/Worker topology, the selected Rust History transport, and
  `acceptanceEligible: false`.

## Results

- The bounded pinned-Go YARPC shard-manager interop gate passed (exit 0), as did
  seven focused Rust distributor tests, strict Clippy, and the production
  binary build. Logs and exit markers are in `final-go-wire-bounded/`,
  `final-unit/`, `final-clippy-strict-3/`, and `final-build/`.
- The isolated gRPC mixed probe passed (exit 0): Go Frontend/History/Worker
  discovered Rust Matching through Go Ringpop, and the existing Go SDK workflow,
  query dispatch, isolation-group dispatch, native partition config,
  SIGKILL/restart recovery, and SIGTERM drain/recovery completed. Go config
  retained its default transport; Rust's isolated config selected gRPC.
  Artifacts: `mixed-grpc-isolated/`.
- The isolated TChannel mixed probe passed the same checks (exit 0), with Rust's
  isolated config selecting TChannel for outbound History calls and Go services
  retaining their default transport. Artifacts: `mixed-tchannel-isolated/`.
- The `config-load-check.json` files verify the Rust config references the
  isolated file, the selected value is present, the Go file has no outbound
  override, the Rust startup log exists, and no missing-file dynamic-config
  warning appeared. They are corroborated by the workflows and History task
  completions in each probe. This is a missing-config-warning check, not a
  separate introspection RPC for the loaded property value.

Commands, exact process results, transport values, and logs are kept in each
`mixed-{grpc,tchannel}-isolated/` directory. They were run with:

```sh
NODE_PATH=/home/ubuntu/github/auenkr/cadence-rust/cadence-web/node_modules \
  CARGO_BUILD_JOBS=1 python3 tests/run-go-service-baseline.py \
  --rust-matching --matching-restart --history-transport grpc \
  --rust-production-binary /home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target/debug/cadence-server \
  --artifacts-dir /home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider/memory/evidence/matching-dns-interop-20261020/mixed-grpc-isolated/run

NODE_PATH=/home/ubuntu/github/auenkr/cadence-rust/cadence-web/node_modules \
  CARGO_BUILD_JOBS=1 python3 tests/run-go-service-baseline.py \
  --rust-matching --matching-restart --history-transport tchannel \
  --rust-production-binary /home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target/debug/cadence-server \
  --artifacts-dir /home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider/memory/evidence/matching-dns-interop-20261020/mixed-tchannel-isolated/run
```

## Limits and next work

The mixed probes establish SDK and service-to-service operation across both
outbound History transports, plus forced restart and graceful recovery. They do
not establish full Matching parity: the production idle-unload behavior remains
adapter-only in this harness, and the complete parity audit and remaining
failure/recovery cases are outstanding. Keep Matching as the current service;
do not start History migration from this evidence alone.

Go's generated `DescribeTaskListResponse` and shared Thrift mapper do not contain
`partition_config`. The TChannel mixed gate therefore deliberately keeps Go
Frontend-to-Matching on its default gRPC path so its native partition assertion
does not test a Go wire field that does not exist. The tested TChannel direction
is Rust Matching outbound to Go History. This incoming Go-to-Matching Thrift
field limitation is documented separately from the outbound History result.
