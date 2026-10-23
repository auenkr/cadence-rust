# Lane 9 (configstore) FAILED (exit 1): REAL Rust bug in the configstore decoder

Harness part works: `go/tests/configstore-seed` wrote a Go-format `cluster_config` row (`configstore-seed.log`:
`seeded 6 entries version=1 encoding=thriftrw bytes=453`) and Rust Matching switched to `dynamicconfig.client: configstore`.

Rust Matching log (`evidence/rust-server-logs/*.log`):
`warning: creating ConfigStore dynamic config client: failed to fetch dynamic config snapshot DeserializeBatchEvents encoding: "thriftrw", error: invalid thrift data: unknown ttype 89; using no-op configuration`
=> no-op config, so the seeded matching.longPollExpirationInterval=2s was NOT applied: the probe's empty direct poll
took 9006 ms (the probe's 10 s deadline cut it), expected ~2000 ms (`evidence/probe.log`, assertion at tests/matching-service-compat.cjs ~1159).

Cause: Go's thriftrw encoder prepends a 1-byte preamble 0x59 (go/common/codec/interface.go:50 `preambleVersion0`, applied by
`codec.NewThriftRWEncoder().Encode`, used by serializer.go:445 for DynamicConfigBlob). The seeded bytes start `59 0a 00 0a ...`.
`decode_struct` in rust/crates/cadence-persistence/src/config_store.rs (~line 143) reads the struct from byte 0 and so sees ttype 0x59 = 89.
The Rust unit tests built blobs without the preamble, so they never caught it. The same applies to the thriftrw_snappy path
(snappy payload decompresses to preamble + struct). Fix (not applied; Rust bugs are only documented here): strip and validate the
0x59 byte before `read_struct`, as matching_domain_metadata.rs:1110-1117 already does; add a unit test with a Go-produced blob
(`SEED_DRY=1 go run ./tests/configstore-seed u:p@h:1 d '{"a.b":"1s"}'` prints one, hex
590a000a00000000000000010f00140c000000010b000a00000003612e620f00140c000000010c000a08000a000000010b0014000000042231732200000000).
Lane result: FAIL. Run once only (the failure is a Rust bug, not a harness bug).
