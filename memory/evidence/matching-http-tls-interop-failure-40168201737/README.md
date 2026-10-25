# Matching HTTP TLS interop failure — run 40168201737

Run: https://github.com/auenkr/cadence-rust/actions/runs/40168201737  
Artifact: https://github.com/auenkr/cadence-rust/actions/runs/40168201737/artifacts/12589710329  
Artifact ZIP SHA256: `1b75f95935e8a1d053125bb5379ea49964d26820d0afc575072d9ad37f491fe2`

Pinned source: `fade09c501e23212d4d86e64c653057d589d1910`. The fresh locked
build succeeded (`build.exit=0`), binary SHA256
`500e4ee26df85af47f0182a34c568ae13c29377590628a05a56c5e55547997a7`; the
lock diff was empty. The interop script exited 1 during the embedded
`TestRustMatchingNativeThrift/tchannel` probe. Health, domain registration and
description succeeded. On a deliberately malformed `DescribeTaskList` request
with no nested TaskList, Go expected `BadRequestError`; Rust returned success,
so the generated client reported `bad request type: <nil> <nil>`. No TLS HTTP
request ran.

Production source finding: `rust/crates/cadence-matching/src/grpc_matching.rs`
in `describe_task_list` unwraps a missing nested request to a default request,
then maps an absent task list to an empty name. `TaskListName::parse("")`
allows that unpartitioned name, and the handler can return success. The
compatibility expectation is already explicit in
`tests/matching-thrift-go_test.go` at the `DescribeTaskList` bad-request check.
The narrow fix rejects a missing or empty TaskList as `InvalidArgument` before
queue lookup; the Thrift adapter maps that to `BadRequestError`. A Rust
regression test covers the missing nested request.

Harness path review: after embedded Thrift, the script checks occupied
TChannel and gRPC startup failures, then runs the explicit-config normal
server, configured task/cache persistence and failover probes, HTTP route
allowlisting, enforced TLS plus plaintext rejection, permissive TLS/plaintext
sniffing and pending connections, sniff/handshake failures and timeouts,
graceful listener drain, then default/disabled plaintext HTTP. Port selection,
TLS config generation, certificate trust and per-mode startup paths were
reviewed against the script; execution did not reach them in this run. No
assertion was skipped or weakened. Exact source, commands, exits, logs, binary
hash, and empty lock diff are retained here. Focused CI validation of the new
fix is pending; no mixed gate was dispatched.
