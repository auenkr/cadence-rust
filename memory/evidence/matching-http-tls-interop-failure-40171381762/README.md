# HTTP/TLS interop run 40171381762 failure diagnosis

- Source: 5cafc2dc941fc676cad66d6756fb812c502aa5bf (oracle run 40171379726 SUCCESS on same source).
- Artifact `matching-http-tls-interop`: build.exit=0, interop.exit=1,
  binary sha256 500e4ee26df85af47f0182a34c568ae13c29377590628a05a56c5e55547997a7.
- Failure (all three configured subtests tchannel/http/http-trailing-slash, 29s each):
  `rust_native_test.go:139: decision delivery: PollForDecisionTaskResponse{TaskToken: [], StartedEventId: 0 ...} <nil>`
  after `AddDecisionTask`. Embedded-mode run passed; TLS assertions were never reached.
- Server log shows only "Not enough hosts to serve the request" (at shutdown).

## Diagnosis (source-backed, circumstantial: server log lacks the per-poll History error)
Explicit-config Rust Matching uses a Ringpop History resolver
(`launcher.rs` ~1074-1201, `matching_history_membership.rs` `history_host`,
`matching_history_peer_resolver.rs:139`): only members labeled `serviceName=cadence-history`
with a `grpc` port label count. The harness ring was one node (Matching itself), so
RecordDecisionTaskStarted fails and, like Go's long poll (loops to deadline on errors other than
not-found/already-started), returns an empty poll. Embedded mode uses a local History adapter, hence passed.
Not a TLS or protocol defect; no Rust production change.

## Fix (harness only, no assertion weakened)
`tests/matching-history-ring-stub-go_test.go` runs the real Go `ringpopprovider` as a
`cadence-history` member (grpc label = Rust History gRPC port); `run-matching-thrift-compat.py`
joins it for the main configured and permissive blocks (`history_ring_member`).
Validation: needs a mixed HTTP/TLS interop rerun after parent review (not run).

## Preserved limitations
Nil-outer DescribeTaskList panic in Go and synthetic domain errors remain as documented in the
DescribeTaskList README; the rejected 84cadec2 Rust guard stays absent.
