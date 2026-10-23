# Configstore 0x59 preamble fix (gate final2 lane 9)

Branch: `matching-configstore-preamble-20261024` (from `origin/matching-gate-final2-20261023` @ bbd8ea86d)
Fix commit: `6a6d9fa87` (not merged)

## Failure being fixed

Gate final2 lane 9 (`memory/evidence/matching-gate-final2-20261023/9-configstore/BUG.md`):
Rust Matching logged `DeserializeBatchEvents encoding: "thriftrw", error: invalid thrift data: unknown ttype 89`
and fell back to a no-op dynamic config. The seeded `matching.longPollExpirationInterval=2s` was not applied.
Cause: Go's `codec.ThriftRWEncoder` prefixes every ThriftRW payload with `0x59`
(`go/common/codec/interface.go` `preambleVersion0`, `version0_thriftrw.go`), and the Rust decoder read the struct from byte 0.

## Go behaviour (reference)

- `ThriftRWEncoder.Encode`: writes `0x59`, then the struct.
- `ThriftRWEncoder.Decode`: empty input -> `Missing binary encoding version.`; first byte != `0x59` -> `Invalid binary encoding version.`
- `thriftrwDecode` (serializer.go) goes through the encoder for `DynamicConfigBlob`.
- `thriftrwsnappyDecode`: snappy-decompress first, then the same `thriftrwDecode` (so the preamble sits inside the compressed payload).
- Empty `DataBlob.Data` never reaches the decoder: `DeserializeDynamicConfigBlob` returns an empty blob.

## Rust change

`rust/crates/cadence-persistence/src/config_store.rs`:
- `strip_thriftrw_preamble` validates and strips the `0x59` byte with Go's two error messages.
- `decode_struct` applies it, so both the `thriftrw` and `thriftrw_snappy` paths get it.
- No other behaviour change. Error prefix stays `DeserializeBatchEvents encoding: "thriftrw", error: ...`.

Scope check:
- All backends (SQLite, MySQL, PostgreSQL, Cassandra) share `decode_snapshot`, so one fix covers them.
- The Rust port has no config-store writer (`cluster_config` is only read), so no writer change.
- `matching_domain_cache.rs` and `launcher.rs` `thriftrw` hits are domain-metadata fixtures, not config-store rows; unaffected.
- `matching_domain_metadata.rs` already handled the preamble for its nested blobs; unchanged.

## Tests

- `encode_blob` test helper now emits the preamble (Go encoder output); `encode_struct` emits the bare struct.
- `go_seeded_blob_with_preamble_decodes`: hex fixture from BUG.md (Go output for `{"a.b":"1s"}`); asserts version 1, schema 1, entry `a.b`, JSON value `"1s"`.
  The fixture was copied from BUG.md, not regenerated here (no local Go run).
- `thriftrw_blob_without_preamble_is_rejected_like_go`: bare struct (first byte 0x0a) and wrong preamble (0x58) both rejected.
- `snappy_blob_without_preamble_is_rejected_like_go`: snappy of bare struct, and snappy of empty payload -> `Missing binary encoding version.`
- Existing `decodes_thriftrw_blob_with_filters_in_order`, `snappy_blob_decodes`, and update tests now pass through the preamble-bearing helper.

## CI evidence (GitHub Actions, no local cargo)

Workflow: `rust-ci.yml`, dispatched on `matching-configstore-preamble-20261024` at head `6a6d9fa87`.
Dispatched one at a time: the workflow's concurrency group (`rust-ci-<ref>`, cancel-in-progress) would cancel the first run if both ran together.

| package | run | clippy | tests |
| --- | --- | --- | --- |
| cadence-persistence | https://github.com/auenkr/cadence-rust/actions/runs/40031040997 | exit 0 | 258 passed, 0 failed; the 5 config_store tests ran and passed |
| cadence-matching | https://github.com/auenkr/cadence-rust/actions/runs/40031219062 | exit 0 | 542 passed, 0 failed |

First attempt passed; no fix iterations were needed.

## Not done here

- The mixed-cluster gate lane 9 (Go seed + Rust Matching probe) has NOT been rerun. Running it needs a Rust server build, which is outside this CI-only check. Next step: rerun lane 9 with a binary built from `6a6d9fa87`, then confirm the probe sees ~2s long-poll expiry.
- No merge into master or the gate branch (per instructions).
