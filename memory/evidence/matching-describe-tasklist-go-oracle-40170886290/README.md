# Original Go DescribeTaskList oracle — run 40170886290

Run: https://github.com/auenkr/cadence-rust/actions/runs/40170886290
Source SHA: `d68fc736661656de3d7fe45b5ceb6bf5c9c2ac4a`
Oracle exit: 0. Artifact `matching-describe-tasklist-go-oracle` was downloaded
from the Actions run; `go-oracle.log`, `go-oracle.exit`, `source-sha.txt`,
`command.txt`, and `overlay.json` retain the raw evidence.

Observed cases:

- absent outer request: recovered `runtime.errorString` for nil pointer dereference;
- absent nested request: success (`response_nil=false`);
- present empty TaskList: success (`response_nil=false`);
- malformed reserved task-list name `/__cadence_sys//0`: `*types.InternalServiceError`,
  message `invalid partitioned task list name /__cadence_sys//0`, even when the
  domain ID was configured as unknown. This demonstrates identifier validation
  precedes manager/domain lookup for this malformed name.
- unknown-domain cases with absent nested, empty, or ordinary task-list name:
  `*types.InternalServiceError` carrying the mock's synthetic `oracle unknown
  domain` error. This is not evidence for the production domain-cache error
  type or exact message.

The empty-name success shares the suite's test persistence manager and may
reuse manager state after the absent-nested case. Treat it as evidence that the
original Go engine accepts these nil/empty shapes in this harness, not as
isolated manager lifecycle coverage. The absent outer panic and synthetic
unknown-domain error must not be copied as Rust error type/message assertions.
