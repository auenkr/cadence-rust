# V7 Thrift/TChannel mixed lane — reviewed pass

Run: https://github.com/auenkr/cadence-rust/actions/runs/40543897866

Head and artifact commit: f5963951befb12dc64b72bb366c44a464939b87a.
Rust binary SHA256: da769e084c1a3e8a7bcdba94afae36c3192d4f673aed4438e22e2019d730913c.
Build/gate/lane/runtime exits are zero. The actual harness command selects
Thrift Matching and TChannel History; the running server log begins
`starting matching (api v1-thrift)`. Results identify Go F/H/W, two Rust
Matching hosts, original Go Ringpop discovery and no Go Matching.

Reviewed actual SDK log success for cancellation, echo, activity retry, child,
workflow retry, workflow-task retry, activity timeout and signal. Probe records
query, isolation, native partition config and both auto-config hints passing.
Restart preserves a durable task (PID9038 to9144) and completes once. Graceful
drain recovers one queued row to zero on replacement9486 and completes once.
Crash8683 is taken over by survivor8767, advancing range1 to2 and ack0 to1,
completing once and leaving zero task rows. Idle eviction releases the poll
and the subsequent SDK recovery completes once. Host ownership maps agree.
Final SIGTERM exits0; cleanup names the uniquely owned MySQL container removed.

This is a bounded mixed-runtime transport pass. It does not claim exhaustive
method/error/optional-field coverage or resolve invalid-UTF-8 string handling.
The legacy preliminary-passed label and collection inventory are not
whole-service acceptance. Other exact-pin requirements remain in the ledger.
