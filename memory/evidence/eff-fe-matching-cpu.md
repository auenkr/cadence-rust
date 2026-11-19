# eff-fe-matching-cpu (2026-11-19) — DONE (fix verified by CI+gates; CPU gain unmeasured)

Branch `eff-fe-matching-cpu` from master 56b58c6c1.

## Profiles (sdk-matching-benchmark profile=1, 25/s)
- Frontend run 44427028756 (success, ~300 samples): self memcpy 10.5%, memcmp 2.4%, jemalloc ~5%,
  serde_json BTreeMap/Value handling, thrift HistoryEvent decode. Inclusive: redirector/ForwardingBackend 35-50%,
  tchannel call_thrift 12.5%, history read_history_branch 10.8%, **history_owner/matching_owner +
  history_addr_for_workflow ~4-5% (Ring::new rebuilt on every request)**, encode_resp 5%.
- Matching run 44427032922 (success, ~140 samples): near idle; kernel net + tokio scheduler + h2 hpack; no clear
  user-space hotspot.

## Fix (no API/logic change)
`cadence-matching/src/matching_ringpop_provider.rs`: `RoleRingCache` caches the per-role hash ring keyed by the
membership snapshot Arc (ptr_eq); rebuilt only when membership changes. Used by Frontend and Matching owner
lookups via `RingpopClusterMembership`. Unit test `role_ring_cache_reuses_ring_until_snapshot_changes`.
Commits 934310a72 (fix), a976c635c (clippy type alias).

## Validation so far
- rust-ci on 934310a72: FAIL (clippy type_complexity) -> fixed in a976c635c; rerun 44441712799 (matching),
  44441716854 (frontend): in progress at pause.
- matching-mixed-gate 44435584343: success on 934310a72. frontend-mixed-gate 44435588756: success on 934310a72.
- Benchmark before (master, actually built 95dd68ed7) 44427068493 FE / 44427073038 MT: success, clean 25/s.
- Benchmark after 44435575570 FE / 44435580285 MT on 934310a72: INVALID — throughput 14-23/s in BOTH arms
  (runner contention with concurrent gates/CI); do not use.
- Pinned reruns in progress at pause: before (source_ref=56b58c6c1) FE 44441894483, MT 44441898274;
  after (source_ref=a976c635c) FE 44441831107, MT 44441839160.

Raw table from first (invalid-after) pair, service container cpu_seconds / peak RSS / completion p50/p99:

### frontend
| pair | arm | before CPU s | before RSS MiB | before p50/p99 ms | after CPU s | after RSS MiB | after p50/p99 ms |
|---|---|---:|---:|---|---:|---:|---|
| 0 | go | 17.81 | 126.7 | 93.4/8890.5 | 24.15 | 189.0 | 193.5/34390.6 |
| 0 | rust | 12.05 | 84.2 | 72.7/6400.7 | 13.76 | 112.1 | 191.1/24455.5 |
| 1 | go | 17.94 | 127.4 | 118.9/9777.7 | 23.94 | 182.4 | 170.1/31745.6 |
| 1 | rust | 12.03 | 84.5 | 71.8/7181.2 | 13.88 | 117.8 | 146.2/27202.6 |
| 2 | go | 17.85 | 118.5 | 100.9/5595.1 | 24.02 | 177.9 | 171.2/30458.4 |
| 2 | rust | 12.03 | 87.2 | 71.4/9023.6 | 14.25 | 126.2 | 161.4/31581.0 |

### matching
| pair | arm | before CPU s | before RSS MiB | before p50/p99 ms | after CPU s | after RSS MiB | after p50/p99 ms |
|---|---|---:|---:|---|---:|---:|---|
| 0 | go | 7.25 | 100.4 | 41.0/167.8 | 10.10 | 101.7 | 3089.7/22176.3 |
| 0 | rust | 6.53 | 62.4 | 40.6/435.2 | 11.29 | 72.1 | 14289.2/40320.0 |
| 1 | go | 7.26 | 101.5 | 43.9/783.1 | 10.31 | 102.3 | 23840.0/59604.5 |
| 1 | rust | 6.51 | 61.8 | 40.0/351.4 | 10.50 | 69.8 | 2042.4/24291.2 |
| 2 | go | 7.21 | 100.4 | 40.3/362.6 | 10.39 | 101.3 | 7552.8/27543.0 |
| 2 | rust | 6.62 | 67.2 | 41.8/590.3 | 9.89 | 62.6 | 360.5/19966.8 |

## Next steps
1. Check rust-ci reruns are green.
2. Build before/after table from pinned reruns (scripted from NN-arm-resources.json + report.json).
3. Re-run matching-mixed-gate + frontend-mixed-gate on final head; avoid running benchmarks concurrently with gates.
4. Candidate further items (not done): Frontend metrics_sink builds BTreeMap<String,String> tags per emit
   (MetricEvent API needs owned map — wider change); serde_json Value round-trips in redirector path.
5. Delete /mnt/storage/service/tmp/effprof after.

## Update (runs finished during pause; no new runs started)
- rust-ci a976c635c: cadence-matching 44441712799 success, cadence-frontend 44441716854 success.
- Pinned benchmarks: before FE 44441894483 success, before MT 44441898274 success, after MT 44441839160 success,
  **after FE 44441831107 FAILURE** (not yet investigated). Table not yet extracted.

## Resume 2026-11-19 (final)
- 44441831107 (FE after) failure cause: report.py aborts on any correctness error; pairs 0/1 had 600-1100
  deadline-exceeded timeouts in BOTH Go and Rust arms, pair 2 clean on both -> runner overload, not the change.
- FE after rerun alone: 44468121697 (success, src 598a2b02f) but again degraded in BOTH arms (Go p99 113 ms -> 10-20 s);
  not comparable with clean before 44441894483.
- Gates on head 598a2b02f: matching-mixed-gate 44473225836 success (no GATE SUMMARY line printed);
  frontend-mixed-gate 44473230543 success, `GATE SUMMARY failed: []`.

### Matching (before 44441898274 src 56b58c6c1 | after 44441839160 src a976c635c), Matching container
before src 56b58c6c1, after src a976c635c
| pair | arm | before CPU s | before RSS MiB | before p50/p99 ms | before tput/s | after CPU s | after RSS MiB | after p50/p99 ms | after tput/s |
|---|---|---:|---:|---|---:|---:|---:|---|---:|
| 0 | go | 7.78 | 100.5 | 39.8/113.1 | 24.99 | 7.75 | 99.6 | 40.1/114.6 | 24.99 |
| 0 | rust | 6.41 | 61.3 | 39.5/115.4 | 25.00 | 7.33 | 61.1 | 44.4/116.3 | 24.99 |
| 1 | go | 7.78 | 100.2 | 40.0/114.8 | 24.98 | 7.96 | 100.2 | 41.2/117.5 | 24.98 |
| 1 | rust | 6.13 | 61.2 | 38.4/107.8 | 24.99 | 7.02 | 60.8 | 41.9/112.9 | 24.99 |
| 2 | go | 8.27 | 100.3 | 42.8/116.7 | 24.98 | 7.73 | 100.7 | 39.7/114.1 | 24.98 |
| 2 | rust | 6.66 | 61.9 | 40.8/114.9 | 24.99 | 6.28 | 64.5 | 40.0/110.9 | 25.00 |

### Frontend (before 44441894483 src 56b58c6c1 | after 44468121697 src 598a2b02f), Frontend container
before src 56b58c6c1, after src 598a2b02f
| pair | arm | before CPU s | before RSS MiB | before p50/p99 ms | before tput/s | after CPU s | after RSS MiB | after p50/p99 ms | after tput/s |
|---|---|---:|---:|---|---:|---:|---:|---|---:|
| 0 | go | 15.13 | 106.1 | 39.9/113.0 | 24.99 | 20.36 | 153.9 | 103.0/20354.0 | 23.18 |
| 0 | rust | 9.16 | 72.5 | 38.3/114.6 | 24.99 | 13.28 | 97.1 | 73.7/13507.6 | 24.88 |
| 1 | go | 15.03 | 106.5 | 40.6/115.3 | 24.98 | 18.65 | 136.4 | 87.8/14342.6 | 24.98 |
| 1 | rust | 9.15 | 71.0 | 38.1/108.1 | 25.00 | 13.21 | 90.3 | 72.1/11992.2 | 24.99 |
| 2 | go | 15.03 | 105.6 | 40.6/115.7 | 24.99 | 17.09 | 127.7 | 67.4/9895.6 | 24.99 |
| 2 | rust | 9.22 | 70.6 | 38.7/111.2 | 25.00 | 13.80 | 91.4 | 74.5/12208.0 | 24.40 |

### Conclusion
Matching: no measurable change (within noise). Frontend: unmeasured; the after-run was degraded in both arms.
Ring-cache fix is behaviour-neutral and gate-green; keep it. Remaining candidates: metrics_sink per-emit
BTreeMap tags, serde_json Value round-trips in the redirector path. A clean FE comparison needs a quiet runner
(before and after dispatched back to back, nothing else running).
