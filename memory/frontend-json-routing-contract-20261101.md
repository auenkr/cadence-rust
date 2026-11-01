# JSON workflow adapter contract

Source audit at master `f2ff7d27a`; implementation remains pending. Two Luna delegates already own async grpc.rs and configured visibility/final launcher state. Do not add a third worker.

The next JSON routing task must preserve existing route shapes while invoking the installed shared WorkflowApi. `handlers.rs::router` currently invokes local workflow handlers directly. YARPC already consults the hook in dispatch_with_headers; do not duplicate that dispatcher.

- `list_workflows` returns a bare array, whereas shared open/closed listing returns a response object. Preserve the public array shape. GET selects `status=open|closed` (default open), optional workflowId/workflowType and numeric closeStatus. Preserve filters and invalid-status errors when adapting typed requests.
- History GET accepts a base64 nextPageToken and rejects malformed values with `Invalid NextPageToken.` Its enum query key is explicitly `HistoryEventFilterType`, with that capitalization. Construct nested execution and decoded token bytes for the shared API. Inspect missing wait/archival options against the API model rather than silently dropping them.
- Describe GET has flat workflowId/runId; shared API requires nested execution. Count GET uses domain/query. Archived listing already takes a typed POST body; search attributes has no body.
- POST request types already use Cadence API shapes, including the outer async request wrapper. Retain missing-wrapper checks, typed JSON rejection and body-size limits. Existing void handlers return HTTP 200 with no body; do not serialize an empty object everywhere.
- Shutdown admission middleware wraps explicit routes and the YARPC fallback. Keep it outside hook dispatch. Propagate original metadata and deadlines, preserve shared Cadence error details, and retain no-hook fallback.

grpc.rs::hook_operation is incomplete for these GET routes and lives in cadence-matching, which depends on service-core. A reverse dependency is invalid. Shared mapping may live in service-core, with route-specific input/output adaptation.

Required actual-Router tests: array response compatibility, filters, nested execution, base64 token/error handling, malformed JSON and body limits, void responses, headers/deadlines, shutdown admission and no-hook behavior. Routing tests do not establish backend or mixed-cluster parity. All Cargo/rustfmt validation runs on GitHub Actions only.
