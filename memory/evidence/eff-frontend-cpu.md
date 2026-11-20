# eff-frontend-cpu (2026-11-21)
Branch eff-frontend-cpu from origin/master f70689fbc. Change commit (and gated SHA) d8ba04da12eabb6f70c629d1f4fedb370dcfaf19.

## Profile (run 44798851060, master f70689f, frontend, 25/s, 60s, 2 pairs, profile=1; Rust arm folded stacks merged, % of all samples)
Top self: memcpy 12.09; _rjem_malloc 1.61; memcmp 1.42; sip Hasher::write 1.20; _rjem_sdallocx 1.11; tokio worker run 0.93;
HistoryEvent::clone 0.87; hashbrown RawTable 0.76; scylla EventTypeV2 drop (symbol alias) 0.73; nf_conntrack 0.69; base64 encode 0.63;
BTreeMap clone_subtree 0.61; thrift read_struct 0.57; serde_json Map insert 0.57; HashMap<String,String> clone 0.56; BTreeMap<String,Value> IntoIter 0.54.
Top inclusive: tchannel handle_call 66.3; yarpc dispatch_with_transport 56.5; HandlerApi::call 47.5; WorkflowRedirector 44; Handler 39/36;
ProductionBackend::dispatch 24; get_raw_history spawn_blocking 12.3 (read_history_branch 11.5, store_read 6.4, history_blob::decode 4.0);
call_with_timeout 11.3; call_thrift (to Matching) 8.8; get_history 4.3; HandlerRateLimit/TokenBucket 3.3-3.6.
Callers: HistoryEvent clone = `ReadHistoryBranchResponse::events()` (Flatten.cloned) 1.3%; BTreeMap<String,ClusterInformation> clone 0.43% =
`AppState::clone` per tchannel call (deep-copies cluster Metadata); attributes_for_request request clone 0.43% (not changed, authorizer attribute
building mirrors Go); HashMap<String,String> clone in HandlerApi::call 0.52% (not changed).

## Change (d8ba04d)
1. `AppState.cluster_metadata` is `Arc<Metadata>`: per-call `state.clone()` no longer deep-copies the cluster map (readers unchanged via Deref).
2. `workflow_matching::read_full_page` moves events out of the page (`batches.into_iter().flatten()`) instead of `response.events()` clone.
No Go-visible, persistence or ordering change.

## Validation (all success)
- rust-ci cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/44803516984
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44803532023
- frontend-mixed-gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44804503581 (`GATE SUMMARY failed: []`)

## A/B (frontend, 25/s, 60s, 3 pairs/run, mysql_tmpfs). Metric: frontend container avg CPU, Rust minus Go %
| run | code | CPU model | per-pair | mean (Rust/Go) |
|---|---|---|---|---|
| 44804507621 | before f70689f | Xeon 6975P-C | -45.3 -46.5 -45.8 | -45.9 (0.54) |
| 44804537566 | after d8ba04d | Xeon 6975P-C | -43.3 -44.0 -41.9 | -43.0 (0.57) |
| 44804531215 | before | EPYC 9V74 | -38.6 -35.9 -39.4 | -38.0 (0.62) |
| 44804525219 | after | Xeon 8573C | -38.8 -37.3 -39.9 | -38.7 (0.61) |
| 44804517170 | before | EPYC 7763 (rejected) | -37.3 | -37.3 |
| 44804511847 | after | run failed (infra), no data | | |
Only the 6975P-C pair is same-CPU: 0.54 -> 0.57, i.e. no measurable gain (within pair noise ~±2). Honest verdict: the two changes are correct and remove
~1.7% inclusive of profiled samples, but that is below A/B resolution; no CPU improvement can be claimed. Frontend Rust is already ~0.55-0.62x of Go CPU in
these runs (not 0.70); remaining Frontend profile is diffuse (memcpy 12%, serialization, syscalls).
