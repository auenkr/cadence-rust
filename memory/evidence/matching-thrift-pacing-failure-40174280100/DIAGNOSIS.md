# Thrift/TChannel production lane failure, run 40174280100 (pin 29bdd2b04)
- Lane: `--matching-transport thrift --history-transport tchannel`, original Go Frontend/History/Worker, no Go Matching, 2 Rust members.
- Rust binary sha256 f9e1735c2f7c01c8bf7fb899512f95c2ea9b0df96e8cdec382c66abd6675cd1b; gate.exit/run.exit = 1.
- Error: `tests/matching-service-compat.cjs:1242` `paced activity dispatch was not rate limited: [8,6,7]` (need sum of polls 2..3 >= 500 ms). Both Rust members agreed on owner 127.0.0.1:11556 before the poll, so not an ownership split.
- Harness vs production: NOT established. Thrift wire path checked and correct: Go `FromPollForActivityTaskRequest` sends TaskListMetadata (shared.thrift 400/1447); Rust `cadence-thrift convert.rs` reads field 40 -> field 10 double; `thrift_matching.rs` forwards `max_tasks_per_second` into the same `Matching::poll_for_activity_task`/`poll_rate`/`report_limit` path the gRPC lanes use.
- Same command passed at 27d3ff5f6 (evidence matching-final-candidate-20261024/thrift); 27d3ff5f6..29bdd2b04 touched no limiter/poll_rate code (TLS, formatting, tests). gRPC Postgres 40173134481 / MySQL 40173683827 passed the same pin.
- Cause therefore unproven (candidate: timing race, since the first dispatch head may hold a default-rate token; see memory/matching-rate-next.md). Not inferred further without a trace.
- Change: trace-only diagnostics in the probe. On a failed pacing check it writes `paced-failure-trace.json` (latencies, DescribeTaskList rate/status, owners) before the unchanged assertion. No production change, no assertion weakened.
- Next (after parent review): focused rerun of the thrift lane; if it fails again, `paced-failure-trace.json` shows whether the limiter rate was 1 (dispatch timing) or unreported (metadata lost).
