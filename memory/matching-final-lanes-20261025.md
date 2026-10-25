# Matching final lane evidence 2026-10-25 (in progress)
Workspace rust-ci 40188279906 success on fab70808b (not clean_target; fresh-target run still owed).
Nil-tasklist branch merged (test-only) at f00f378ca after these lanes; Rust production code unchanged since 0a773280d.
Lane results (workflow args runId conclusion headSha); binary SHA256 not yet extracted from artifacts:
```
matching-production-validation.yml -f lane=postgres 40188730635 success 0a773280d766593f59385ed652911db964172799
matching-mixed-gate.yml  40189191634 success 0a773280d766593f59385ed652911db964172799
```
Remaining lanes still running via /tmp/lanes.sh: mysql-probes, thrift, cassandra, active-offer, http-tls, configstore, shard-distributor. Matching NOT declared complete.
