# Frontend review part 2 (Opus, 2026-11-04, master 3ace896eb)

Read-only review. Scope this pass: cluster redirection (area 3) checked fully.
Areas 1, 2, 4, 5, 6 (Thrift/proto mapping, domain handler, global ratelimiter,
ES query translation, archive readers) were NOT reviewed in this pass; they need a follow-up.

## O2 — HIGH — request-body STRONG consistency ignored by redirection
- Rust: rust/crates/cadence-frontend/src/wrappers/redirection_runtime.rs:229 (`consistency: Consistency::Eventual` always),
  rust/crates/cadence-frontend/src/wrappers/clusterredirection.rs:508 (header value replaces route value).
- Go: go/service/frontend/wrappers/clusterredirection/api_generated.go:324, 424, 833
  (DescribeWorkflowExecution / GetWorkflowExecutionHistory / QueryWorkflow: `if req.GetQueryConsistencyLevel()==Strong { level = Strong }`).
- Difference: Rust only reads the `cadence-client-feature-flags` header. A request with `queryConsistencyLevel=STRONG`
  in its body stays Eventual.
- Failure: global domain, auto-forwarding on, call goes to the passive cluster with
  QueryWorkflow / DescribeWorkflowExecution / GetWorkflowExecutionHistory and queryConsistencyLevel=STRONG. Go forwards
  it to the active cluster. Rust serves it locally (stale result, or an error from history). This is wrong for any
  redirection policy other than noop.
- Fix: in `DomainCacheResolver::resolve`, set `consistency = Strong` for those three ops when
  `fields.queryConsistencyLevel == "STRONG"`. In `route`, combine the two values (Strong if either is Strong);
  do not let the header replace the route value.

## O3 — MEDIUM — deprecated-domain rejection applied under noop/default policy
- Rust: rust/crates/cadence-frontend/src/wrappers/clusterredirection.rs:246-255 and 343-352 (the deprecated check runs before the `Noop` short-circuit).
- Go: go/service/frontend/wrappers/clusterredirection/policy.go:263-274 (noopRedirectionPolicy.Redirect only does `call(current)`).
  The deprecated check exists only in selectedOrAll...Redirect (policy.go:307).
- Failure: default config (`policy: ""` or `noop`, the common single-cluster setup) with a DEPRECATED domain.
  Calling DescribeWorkflowExecution, GetWorkflowExecutionHistory, Poll*, Signal, etc. returns
  `DomainNotActiveError("domain is deprecated or deleted.")` from Rust. Go hands the call to the handler, which applies its own rules.
  Drain or read operations on deprecated domains therefore break after the image swap.
- Fix: move the `Noop` early return above the deprecated check in both `redirect` and `redirect_async`.

## Checked correct (area 3)
- Policy-name parsing ("" / noop / selected v1-v3 / all-domain v1-v3) and allow-lists, including schedule writes and the V3 async start APIs.
- The set of redirected APIs matches the Go generated wrapper. Domain-admin, health and search-attribute calls stay local.
- Domain lookup by task token for RecordActivityTaskHeartbeat / Respond*; *ByID calls look up by name.
- The target-cluster decision order (local domain, auto-forward flag, active-active selection gate, all-domain target, strong, allow-list) matches Go.
- The DomainNotActive retry (forward only when enabled, a non-empty active cluster, and a different target) and the metric tags match Go.
- Active-active resolution for SignalWithStart (current-running policy override), Start (cluster attribute) and other calls (by workflow) matches Go.
- The feature-flag header parse (autoforwardingEnabled -> Strong) matches Go.
