# Frontend core skeleton — 2026-10-31

Branch: `frontend-core`.

## Implemented in this slice

- Replaced the five-line placeholder with a `cadence-frontend` library and
  executable. The executable runs the shared CLI/runtime restricted to the
  Frontend service, preserving the Go-compatible root/config/env/zone and
  listener flags already used by Matching.
- The shared runtime provides Frontend's configured gRPC and TChannel/Thrift
  listeners and the existing shared health, search-attributes and HTTP API
  handlers. Go YAML interpolation/layering and dynamic-config provider loading
  are also performed by that runtime.
- Added Frontend config facade and a canonical dynamic-property key registry
  sourced from `go/service/frontend/config/config.go` and
  `go/common/dynamicconfig/dynamicproperties/constants.go`.
- Added lifecycle state, request-validation entry points, History/Matching
  lazy gRPC channel constructors, Workflow/Domain/Admin/Schedule handler
  boundaries, and wrapper module boundaries matching the Go package layout.
- Updated the crate manifest and lockfile for the implementation dependencies.

## Deliberate skeleton boundaries / remaining work

- `cadence-frontend` temporarily depends on `cadence-matching` to reuse its
  shared launcher and current Frontend transport implementations. Extract the
  generic launcher and public gRPC/TChannel transport into `cadence-service-core`
  before Frontend can be independently built without the Matching crate.
- The Frontend `Config` facade currently exposes the Go property names but does
  not itself construct all of Go `config.Config`'s dynamic-property closures.
  The shared launcher loads providers; Frontend-specific consumers still need
  to bind the full defaults, filters and live updates.
- `RequestValidator` currently wraps the shared StartWorkflowExecution and
  execution validators only. Port all methods in `go/service/frontend/api/
  request_validator.go`, including domain/task-list/visibility/admin checks,
  mutation of default page sizes, metrics tagging and length warnings.
- Handler boundaries return `InternalServiceError` for unfinished operations.
  This is an explicit temporary Rust skeleton response; compare each operation
  against Go's concrete handler/error behavior while implementing the modules.
- History and Matching constructors create lazy gRPC channels only. Add the
  generated RPC clients, TLS/auth options, discovery, retries, and TChannel
  client behavior required by each Go call site.
- Wrapper files are boundaries only: access control, cluster redirection,
  metering, rate limiting, Thrift conversion and version checking still need
  full Go semantic ports.
- No runtime smoke test has yet been recorded. CI must establish crate clippy
  and tests; a later gate must launch with `go/config/development.yaml` and
  verify Frontend health and GetSearchAttributes over the public transports.
- Full Frontend parity additionally requires persistence integration, complete
  workflow/domain/admin/schedule handlers, lifecycle drain/recovery, mixed
  Go History/Matching service compatibility and SDK failure/restart/recovery
  gates. This branch is a bootstrap skeleton, not Frontend completion.

## Validation evidence

- Source checks before CI: `git diff --check` passed.
- Cargo was not run on the development host, per project instructions.
- CI workflow URL, head SHA and conclusion: pending dispatch.
