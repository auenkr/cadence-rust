# Nested Go ThriftRW DataBlob version framing

The Go implementation in `go/common/codec/version0_thriftrw.go` prepends
`0x59` to each `thriftrw` encoded persistence DataBlob and removes/validates
that byte before decoding. `domains.data` is different: `sqlblobs.DomainInfo`
is serialized directly with ThriftRW and remains unprefixed.

Go oracle output was generated with `persistence.NewPayloadSerializer()` and
`constants.EncodingTypeThriftRW` for BadBinaries, IsolationGroupConfiguration,
and AsyncWorkflowConfiguration. Their complete hex values are asserted in the
Rust persistence regression test `decodes_nested_blobs_emitted_by_go_payload_serializer`:

* BadBinaries: `590d000a0b0c0000000000`
* IsolationGroupConfiguration: `5900`
* AsyncWorkflowConfiguration: `5902000a000b0014000000000b001e0000000000`

After applying the decoder fix, `cargo test -p cadence-persistence
matching_domain_metadata::tests` passed (12 passed, 1 ignored). The production
binary rebuilt with the shared target and started against the Go-written
SQLite row that previously failed at startup with `unknown ttype 89`. The
mixed run then resumed and completed its pre-restart workflow through the Go
SDK after a SIGKILL/restart, and completed its graceful-drain recovery SDK
workflow. The broader compatibility script still failed later at its isolated
poller registration stage; the production harness did not configure the Rust
instance's `system.enableTasklistIsolation` and `system.allIsolationGroups`
dynamic config, unlike the adapter fixture. The harness now configures those
same values through the Rust admin HTTP API before each process stage so the
isolation assertion remains active in production mode.

See `mixed-artifacts-codec-fix/` for the failed first full run and
`mixed-domain-codec-restart.log` for its exit status. A rerun with the harness
configuration change is required before recording the full mixed gate as
passed.
