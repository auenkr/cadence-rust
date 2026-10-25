# Matching final lane evidence 2026-10-25
Workspace rust-ci 40188279906 success on fab70808b (not clean_target; fresh-target run still owed).
Rust production code unchanged after 0a773280d; later commits are tests/docs.
```
matching-production-validation.yml -f lane=postgres 40188730635 success 0a773280d766593f59385ed652911db964172799
matching-mixed-gate.yml  40189191634 success 0a773280d766593f59385ed652911db964172799
matching-production-validation.yml -f lane=mysql-probes 40189574487 failure 0a773280d766593f59385ed652911db964172799
matching-production-validation.yml -f lane=thrift 40189954994 success dce2ab05e2860cd8d19820e5918ab5a1e40bf00d
matching-cassandra-mixed-gate.yml  40190336620 success dce2ab05e2860cd8d19820e5918ab5a1e40bf00d
matching-active-offer-gate.yml  40190858400 success dce2ab05e2860cd8d19820e5918ab5a1e40bf00d
matching-http-tls-interop.yml  40191238954 success dce2ab05e2860cd8d19820e5918ab5a1e40bf00d
matching-production-validation.yml -f lane=configstore 40191566892 failure dce2ab05e2860cd8d19820e5918ab5a1e40bf00d
matching-production-validation.yml -f lane=shard-distributor 40192102913 success dce2ab05e2860cd8d19820e5918ab5a1e40bf00d
```
FAILED: mysql-probes 40189574487 and configstore 40191566892 both died at 'Go baseline probe failed' (RuntimeError in run-go-service-baseline.py, probe.log) i.e. the Go-only baseline, before Rust Matching was exercised. Cause unconfirmed (flake vs harness); must rerun and read probe.log. Not evidence of Rust failure, not evidence of pass.
Binary SHA256 not extracted. Cassandra rate item still unconfirmed. Matching NOT declared complete.
