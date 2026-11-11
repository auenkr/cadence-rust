# history-parentclose (2026-11-11)

Branch `history-parentclose` (from master 8e0b1064c).

## 1. Parent close policy
The executor logic (`process_parent_close_policy`, `apply_parent_close_policy`, threshold/batch) was already ported. The missing piece was the worker client: `send_parent_close_policy_request` returned a TODO error.
New `wiring/assembly/parent_close_policy_client.rs` ports Go `parentclosepolicy/client.go`:
SignalWithStart to Frontend (current cluster rpcAddress, plaintext gRPC), domain `cadence-system`, workflow id `parent-close-policy-workflow-<rand(NumParentClosePolicySystemWorkflows)>`, type `cadence-sys-parent-close-policy-workflow`, task list `cadence-sys-processor-parent-close-policy`, signal `ParentClosePolicyProcessorChannelName`, 20y execution timeout, 60s decision timeout, AllowDuplicate, 400ms signal timeout. The input is Go-SDK-JSON of `parentclosepolicy.Request` (policy as text e.g. "TERMINATE"). Wired in `queue_deps_transfer_active.rs` (one-line change).
Limitation: no TLS and uses cluster rpcAddress instead of `publicClient.hostPort` (not available in HistoryEnv; env.rs deliberately untouched). Not exercised by the gate (needs >= threshold children).

## 2. "Operation is not supported" for cron/schedule workflows
Gate logs (run 43065888204) show the failing tasks are `UpsertWorkflowSearchAttributesTask` for `cadence-scheduler:*-schedule` workflows (keys BinaryChecksums, CadenceSchedule*; no CadenceChangeVersion). Go's mutable state generates this task unconditionally in `ReplicateUpsertWorkflowSearchAttributesEvent`, and the Go SQL visibility store returns `ErrVisibilityOperationNotSupported` for non-nop upserts, so Go behaves identically (not CloseExecution, not a Rust bug). No code change; behavior is parity.

## CI
- rust-ci (cadence-history) dispatch https://github.com/auenkr/cadence-rust/actions/runs/43065880019 success (head 60ad497).
- Gate mysql/grpc/false https://github.com/auenkr/cadence-rust/actions/runs/43065888204 success (head 60ad497, includes diagnostic log).
- Final head: see git log; final runs recorded below.

## Final (code head d52193c270bebbca7559620a12b117e4c122ce77, includes master e50090050 merge)
- rust-ci (cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/43067963814 success
- Gate mysql/grpc/false: https://github.com/auenkr/cadence-rust/actions/runs/43067971890 success
Remaining: parent-close client has no TLS/publicClient.hostPort; no gate coverage for the >= threshold path.
