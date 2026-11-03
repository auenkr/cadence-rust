# Frontend two-cluster redirection gate (2026-11-03)

Branch `t3/frontend-two-cluster-gate`. Harness: `tests/gates/run-frontend-two-cluster-gate.py`, lane `run-frontend-two-cluster-lane.sh`, dispatched via `frontend-mixed-gate.yml -f two_cluster=true`.
Topology: A = Rust Frontend + Rust Matching + Go History/Worker; B = all Go (cluster1 is primary). One MySQL, 4 schemas.

## Green run
- Gate run: https://github.com/auenkr/cadence-rust/actions/runs/41598950223 (success), Rust cadence-server sha256 `11c165f6e38a863ca480e16b2c3ba30ca901ace30a81338d642b942099fb4a77`
- rust-ci cadence-frontend: run 41598940098 success. Full-workspace rust-ci on final head: run 41600128357 (see below).
- Proven: selected + all-domain forwarding to B (start/signal/list, `cadence-forwarding-cluster`), noop returns Go's DomainNotActive message, failover (update on B, replicates to A, local run, no further forwards), standby B rejects, `admin kafka rereplicate` over gRPC replays history into A, unknown cluster fails, forwarding metrics emitted.

## Bugs fixed
1. Forwarded requests carried the internal History/Matching shape (empty domain). Now `invoke_remote_public` forwards the public request.
2. Shared a single metrics sink (redirection metrics were lost).
3. gRPC DescribeWorkflowExecution pending decision/activity timestamps are nanos (caused Go CLI panic).

## Not covered
Schedule APIs in resolver, strong-consistency header, gRPC remote forwarding client, active-active routing.
