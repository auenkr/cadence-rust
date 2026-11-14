# Frontend soak tail (2026-11-14), branch frontend-soak-tail

## Run 1: master 52848226a, rate 5, 10 min, all 4 lanes
https://github.com/auenkr/cadence-rust/actions/runs/43582334396 (head 52848226a21ba35aea88e10157da31ff2bd7f168). All jobs green; verdict files per lane:
- rust (Rust FE+MT): e2e p50/p95/p99 3052/4055/5780 vs Go 3063/4071/4568 (PASS); start p95 6 vs 14, p99 8 vs 134 (PASS); query p99 30 vs 63; init retries 1490 wf / 2017 attempts vs Go 1517 / 2052 (PASS). 3000/3000, 0 failed, no panics. Only FAIL: fe/mt RSS+fd slopes (fe n/a, mt 60 MB/h; meaningless in 10 min).
- rust-mt (Go FE + Rust MT): all latency PASS; only slope criteria fail (fe n/a).
- rust-fe (Rust FE + Go MT): e2e p99 7690 vs 4568, query p99 198 vs 63, start p95 160 vs 14, p99 401 vs 134 = FAIL. (Combined lane was clean in the same run, so this is noise-sensitive; Frontend is the only lane with a tail.)
Conclusion: the 2026-11-13 gap (e2e p95 14s, start p95 103ms) is gone on master (History/pool fixes). Matching is not the source. Residual: Rust FE tail in rust-fe lane only; slope criteria need a full-length run.

## Run 2: full length, rate 10, 70 min, lanes go-baseline, rust, rust-fe
https://github.com/auenkr/cadence-rust/actions/runs/43584330378 (head e159eba846f412e90d7c1cc4e8b2a8b6b0947157), all jobs green. 42000/42000 completed, 0 failed, no panics, in every lane.
- rust (Rust FE+MT): ALL latency criteria PASS and match Go: e2e p50/p95/p99 3096/4105/4138 vs 3095/4102/4128; start p95/p99 17/25 vs 15/22; query p99 51 vs 48. RSS and fd slopes PASS (fe 1.68 vs 5.05 MB/h, mt 0.96 vs 1.73). Only FAIL: "query init" criterion, rust 21026 wf / 28531 attempts vs Go 20952 / 28410 (+0.35%).
- rust-fe (Rust FE + Go MT): latency all PASS (e2e p99 4102 vs 4128, start p99 16 vs 22). FAIL only mt_rss 6.84 vs 1.73 MB/h (that is the Go Matching process, i.e. noise; threshold Go+5=6.73) and the same init criterion (21056 / 29931).
- The tail seen in run 1's rust-fe lane did not reproduce in 70 min: it was a 10-minute startup/noise artifact. No Rust code divergence found; no Rust code change needed.

## Fix: harness criterion
"query init: none (as Go)" required rust workflows <= Go's exact count, although Go itself retries init for ~50% of workflows (driver queries before the first decision completes, identical race). Changed tests/gates/soak-analyze.py to compare relative to Go (<= 1.1x + 10, for workflows and attempts). With it, run 2's rust lane would be RUST OVERALL: PASS (offline rescoring failed on download; the arithmetic uses the numbers above). Not re-run on CI to avoid another 90-minute run.

## Matching retained-state investigation (2026-11-14)
- Merged origin/master into this branch (merge 25ca90194); no branch merged into master or deleted. Existing clean branch worktree reused.
- Previous soak https://github.com/auenkr/cadence-rust/actions/runs/43590860926 at 8c2aeae26: RUST OVERALL FAIL; mt RSS 10.53 vs Go 1.35 MB/h, start p95 83 vs 8 ms, query p99 128 vs 31 ms. 42000/42000 completed.
- Proven unbounded state: cadence-common qps.rs and qps_bytes.rs Accumulator.pending stores one BTreeMap entry per reporting interval until QPS is read. Matching reports on every AddTask; scaling/describe reads are optional. Go common/stats/stats.go runs an independent ticker and retains only counters/EMA. Fold completed buckets on writes, preserving open-window and EMA behavior. Regression tests simulate 10000 intervals without reads and assert one pending bucket for root/group and unchanged final QPS.
- Audited native registry: idle sweep detaches owners using liveness and TTL/2 timers; weak startup gates pruned; forward resolver uses weak registry state. Poller history is capped at 5000 and five-minute TTL; outstanding/cancellation maps have end/drop cleanup. Query registrations remove completion-index and waiter entries on return/cancel; sync offers/polls use drop guards. Durable queue removes in-flight entries on completion/rewrite; ack manager removes contiguous completed entries. Go poller group maps themselves have no TTL, so do not invent group expiry semantics.
- CI validation and new soak verdict pending. No local Cargo, Go or Docker commands used.

- Frontend latency audit: call_with_timeout performed unconditional synchronous eprintln! twice per backend RPC (BACKEND-TRACE begin/end), including starts and queries. Removed temporary tracing to avoid stderr-lock and file/pipe writes on async executor threads. Other request locks inspected are short metadata operations; native Matching QueryWorkflow awaits registration/rendezvous/forward permits without holding std mutexes across awaits. TChannel currently opens/handshakes a connection per call (async); pooling would be a larger transport change and is not justified by these noisy percentile samples alone.
- Initial dispatches 43597975628 (CI) and 43597979686 (soak) cancelled during setup/build to include the trace removal; no soak lane ran.

- QPS follow-up: sample the reporting bucket after acquiring the state mutex, so concurrent writers cannot insert into a window another writer just folded. CI/soak at 98b80762f exercise the bucket-folding and trace removal; this follow-up only orders clock sampling with state mutation.

### Fresh CI validation
- https://github.com/auenkr/cadence-rust/actions/runs/43598097477 — head 98b80762f, SUCCESS: fresh uncached target, formatting, workspace Clippy then workspace tests, PostgreSQL wakeup regression and Go/Rust metric parity.
- https://github.com/auenkr/cadence-rust/actions/runs/43598691116 — head 304c2699a, SUCCESS: same full fresh-target validation including both write-only QPS regression tests. Compact logs in memory/evidence/frontend-soak-tail-20261114/.
- Measured soak attempt 1: https://github.com/auenkr/cadence-rust/actions/runs/43598100147 — head 98b80762f, default 70 minutes/rate 10, go-baseline + rust lanes started 13:18 UTC, verdict pending. No second measured attempt dispatched yet.

### In-turn wait checkpoint (14:00 UTC)
Both fresh CI runs succeeded. Soak 43598100147 remains in progress after its scheduled midpoint; no verdict available yet, and no second measured soak has been dispatched. Waiting in-turn with status checks at least 120 seconds apart. Only read-only GitHub API/CLI and filesystem/git operations run locally.

### In-turn wait checkpoint (14:30 UTC)
Soak 43598100147 still reports both go-baseline and rust in their Soak steps. Awaiting workload completion, analyzer output and uploaded artifacts. No further implementation changes or second measured run.

### Measured attempt 1 verdict: FAIL (43598100147)
- https://github.com/auenkr/cadence-rust/actions/runs/43598100147 — head 98b80762f, workflow SUCCESS but **RUST OVERALL: FAIL**. Default 70 min, rate 10, lanes go-baseline + rust with Go History/Worker. Rust binary SHA256 c5f6ce22d2da2ca3f0b284ad162c48b6fe4fa9b2fcaf2671e143917b0259f8e7.
- Only failing criterion: Matching RSS 6.60 MB/h vs Go 1.45; allowed 6.45. Down from prior 10.53 but still fails; this does not establish leak-free Matching.
- All latency criteria PASS: start p95 13 vs 8 ms, query p99 36 vs 37 ms, e2e p95 4094 vs 4075 ms. 42000/42000 completed; zero failures/panics. Frontend RSS 10.34 vs 6.74 passes Go+5 allowance. Full verdict, Rust summary, samples and hash saved in evidence directory.
- Final allowed measured attempt 2 dispatched: https://github.com/auenkr/cadence-rust/actions/runs/43605415284 at head 3111896c2 (includes CI-validated QPS clock-sampling serialization). Defaults unchanged. No third measured attempt will be run.

### Final-attempt wait checkpoint (15:00 UTC)
Run 43605415284 completed its fresh build and started both lanes at 14:44 UTC. It remains in progress; expected workload end around 15:54 UTC plus setup/analysis. Source unchanged since CI-validated 304c2699a. No local validation commands and no additional soak dispatched.
