# Matching parity continuation (2026-10-17)

User explicitly requires the WHOLE Matching service, with tests, before moving
on. Earlier notes declaring a single-node subset complete do not satisfy that
request. Do not describe all Matching as complete while the transport/routing
items below remain.

Changes in this continuation:

- GetTaskListsByDomain in the durable path snapshots loaded Matching managers,
  not frontend-local workflow queues. It does not create leases. It filters
  sticky managers by default, reads matching.matchingReturnAllTaskListKinds on
  each call, uses root map keys like Go, and includes pollers, task-list identity,
  and partition config without task-list status.
- Standby worker polls only receive queries; they cannot claim or acknowledge
  durable workflow backlog. Decision queries still use the same query channel.
- matching_standby.rs ports task_completer.go. The reader checks domain activity,
  the runtime matching.enableStandbyTaskCompletion flag (default true), and the
  active-active exemption. History DescribeWorkflowExecution proves task start:
  closed execution, absent pending task, later decision schedule, or Started
  state. A missing execution is acknowledged only after strictly more than 24h.
  Retry policy is 1s initial / 10s max / 5m expiration. Shutdown cancels the
  check, and a domain becoming active returns to ordinary dispatch. Successful
  completion acknowledges and runs normal GC without SDK polling.
- Domain store publishes per-domain failover generations on eligible global,
  non-active-active failovers. Durable decision/activity long polls subscribe
  before reading the domain, return empty on notification, retain their manager,
  and new polls do not replay an old notification. UpdateDomain failovers and
  FailoverDomain both publish. This is a local cache notification source; remote
  domain replication/refresh still depends on the broader domain-cache port.
- Query registration no longer enqueues before try_offer, which previously
  enqueued the same query twice on successful local delivery. Admission accounts
  for pending offers. A cancelled QueryChannel::next drops its waiter count via
  RAII (previously cancellation permanently inflated it).
- Query tokens use the accepting partition's actual name. Previously child-local
  queries used the root name and completion went to a different queue's map.

New tests exercise manager listing/unload/filtering, passive polls retaining
backlog, task-completer decision/activity state rules, actual History RPC-driven
standby acknowledgements without workers, failover notifications and blocked
poll release, cancelled query poll accounting, single delivery, and queries
completed on both parent and child partitions.

Validation is in progress in tmux cadence-rust:
/tmp/matching-full.log and /tmp/matching-full.exit;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-standby-failover-query.
The intermediate 139-server-test run passed before the domain notification and
query fixes. An intermediate failover test failed on the intentional Go domain
update cooldown; the regression fixture now explicitly disables the cooldown,
without changing production defaults. Do not claim latest validation passed
until inspecting the full results.

Remaining whole-service audit items (not waived by single-node scope):

- MatchingService + MetaService Thrift wrappers and actual TChannel/HTTP service
  registration. Current TChannel listener accepts only cadence-frontend.
- Cross-host forwarding transport and membership/ownership/ring changes. Current
  ancestor forwarding is in-process and all partitions belong to the local host.
- Shard-distributor-backed partition management/ownership and autoscaling across
  hosts, including failover/rebalance.
- Handler/queue metrics and production config/cache source integration.
- Source-by-source and original-Go differential coverage for these remaining
  paths; existing mixed SDK tests prove only the exercised topology.

Audit correction: sticky-first query dispatch and non-sticky fallback belong to
History/frontend, not Matching.QueryWorkflow. The Go Matching engine dispatches
on the request's task list and checks sticky poller availability, as Rust does.
Do not list History's sticky routing as an unimplemented Matching engine path.

Latest validation checkpoint: workspace tests pass (including 145 server tests)
and strict all-target workspace Clippy passes. The new differential stage passes
all 72 Go completer decisions plus Go query-version edge cases. Full 32-stage
runner continues in cadence-rust: /tmp/matching-verified.log and
/tmp/matching-verified.exit; artifacts matching-standby-verified. The previous
final-named run stopped at Clippy's tonic::Status size lint, now handled with the
same module-level allowance used by other RPC modules. Query-version parsing now
also matches Go's release constraints (v prefix/omitted components/metadata,
malformed values and all prereleases), with Rust and original-Go tests.

The persistent whole-Matching goal is ACTIVE. Next continuation must finish
checking the current runner, record its result, and proceed directly to the
remaining Matching transport/routing work; do not mark the goal complete.
