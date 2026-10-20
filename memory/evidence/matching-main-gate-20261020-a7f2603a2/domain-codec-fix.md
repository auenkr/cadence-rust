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
dynamic config, unlike the adapter fixture. An attempted test-only setup via
the Rust Matching HTTP port returned HTTP 400 because Matching-only mode serves
the health router there; the public admin HTTP router is attached only when the
Rust Frontend role runs. That harness change was reverted rather than adding a
Rust Frontend or weakening the production isolation assertion.

See `mixed-artifacts-codec-fix/` for the restart/SDK recovery run and its later
isolation failure, `mixed-artifacts-codec-fix-rerun2/` for the unreachable
admin HTTP attempt, and `mixed-domain-codec-restart-rerun2.log` for that exit
status. The full mixed gate remains incomplete; it requires a service-correct
way to provide Rust Matching's dynamic configuration before its isolation
check can pass.
