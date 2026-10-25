# Matching Go HTTP/TLS interop — run 40172574953

Run: https://github.com/auenkr/cadence-rust/actions/runs/40172574953

Branch: `t3/matching-validation-history-ring-20261025`  
Exact source: `29bdd2b04b573309112286c57f1c347f0ba0b60c`  
Conclusion: success. Fresh CI target selected; locked build and interop exits
were both 0; generated Cargo.lock patch was empty.

Matching server binary SHA256:
`500e4ee26df85af47f0182a34c568ae13c29377590628a05a56c5e55547997a7`.
The original artifact ZIP SHA256 is recorded in `artifact-archive.sha256` and
the ZIP is retained outside the repository at
`/mnt/storage/service/ci-evidence/run-40172574953/matching-http-tls-interop.zip`.
Selected logs, commands, source identity, exits and probe summaries are kept
here; generated keys, databases and executable binaries were not copied into
the repository.

## Observed outcomes

- The pinned Go TLS record classifier passed its cases, including short and
  fragmented TLS header behavior.
- The history ring stub passed for both the configured `main` and permissive
  runs. With the `main` stub active, the Go `TestRustMatchingNativeThrift`
  suite passed; it calls AddDecisionTask, then PollForDecisionTask, and asserts
  a nonempty task token, nonzero StartedEventId and matching workflow RunId.
  The previous “not enough hosts” decision-delivery failure did not recur.
  This supports the History-discovery diagnosis, while the fixture limitation
  below remains.
- All ten Matching Thrift methods passed over TChannel, HTTP, and HTTP with a
  trailing slash. The permissive run also passed all ten over HTTP TLS.
- Enforced TLS passed HTTPS YARPC Meta::health; the plaintext YARPC call was
  rejected, and the raw plaintext probe accepted only a TLS alert or orderly
  close/reset. The Go TLS compatibility test passed.
- Permissive mode passed both plaintext and TLS health. A silent peer and 129
  pending peers did not block later HTTP requests; fragmented TLS sniffing
  followed the pinned Go plaintext classification; stalled/partial/failed
  handshakes and the 15-second sniff timeout did not prevent listener recovery.
- Default and disabled TLS modes passed plaintext health. SQLite interop and
  drain-health probes passed.

## Scope and remaining lanes

The History ring member is a real Go Ringpop provider advertising
`serviceName=cadence-history` and the **Rust History fixture's** gRPC port. It
exercises Matching History discovery and decision delivery, but it does not
exercise the original Go History service or establish production mixed-cluster
acceptance. This is standalone HTTP/TLS compatibility evidence, not full
Matching sign-off. Preserve the existing `preliminary-passed` and
`acceptanceEligible:false` labels; the remaining production Go-service lanes
must still run serially on the pinned candidate.
