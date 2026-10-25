# Matching HTTP TLS interop failure — run 40168201737

Run: https://github.com/auenkr/cadence-rust/actions/runs/40168201737  
Artifact: https://github.com/auenkr/cadence-rust/actions/runs/40168201737/artifacts/12589710329  
Artifact ZIP SHA256: `1b75f95935e8a1d053125bb5379ea49964d26820d0afc575072d9ad37f491fe2`

Pinned source: `fade09c501e23212d4d86e64c653057d589d1910`. Fresh locked build
passed (`build.exit=0`) with binary SHA256
`500e4ee26df85af47f0182a34c568ae13c29377590628a05a56c5e55547997a7`; lock
diff was empty. The Go TLS classifier and persistence probes passed. Embedded
server startup and health/domain operations passed. The probe stopped at its
malformed `DescribeTaskList` expectation: Go's client-side compatibility test
expected `BadRequestError`, while the Rust server returned success. No TLS
HTTP assertion ran.

Review correction: this client expectation did not establish the original Go
server's behavior. The Go handler resolves domain name before calling the
engine (`go/service/matching/handler/handler.go:388`); the engine uses nil-safe
getters, then `tasklist.NewIdentifier` (`engine.go:1137`, `identifier.go:65,
137`), which accepts an empty task-list name. The Thrift mapper preserves an
absent nested request as nil (`go/common/types/mapper/proto/matching.go:181`).
Therefore the prior Rust guard and regression expectation in unmerged commit
`84cadec2d83e1dfa92651c718c145117946c29d5` are unjustified and must not be
merged. CI run 40168671087 on that branch was cancelled. Exact Go downstream
results and invalid-domain precedence are being measured by the Actions-only
oracle on branch `t3/http-tls-go-oracle-correction-20261025`.

The remainder of the HTTP/TLS harness was reviewed but not executed in this
run: occupied TChannel and gRPC startup probes; configured persistence and
failover; HTTP routes; enforced TLS/plaintext rejection; permissive sniffing,
pending connections, stalled/failed handshake recovery and timeouts; graceful
drain; default/disabled plaintext. Their assertions remain intact pending a
Go oracle result and parent review. Captured source, commands, exits, logs,
binary hash and empty lock diff are preserved alongside this summary.
