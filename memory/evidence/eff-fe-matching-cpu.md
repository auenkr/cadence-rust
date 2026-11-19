# eff-fe-matching-cpu (2026-11-19) — PAUSED, UNVERIFIED

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
