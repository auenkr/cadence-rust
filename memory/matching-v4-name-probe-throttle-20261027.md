# Candidate v4 name probe refresh throttle

Run https://github.com/auenkr/cadence-rust/actions/runs/40482436756 failed at
ListTaskListPartitions for the second newly registered name. Source pin:
64e709b96c398bb1613fd2a9a9b83a8d5babf6f4. Build succeeded; probe exit1;
SDK/recovery phases not reached. Compact evidence is under
memory/evidence/matching-v4-mysql-40482436756/.

Go common/cache/domainCache.go refreshDomainsLocked returns nil without scanning
when less than domainCacheMinRefreshInterval (one second) has elapsed. getDomain
then returns InternalServiceError when the newly existing name is still absent.
Rust matching_domain_cache.rs uses the same one-second suppression. The first
late-name RPC succeeds and refreshes the cache; the fixture then registers a
second domain immediately and calls ListTaskListPartitions inside that interval.
The observed internal response is consistent with the preserved Go cache flow;
it does not establish a production divergence.

Fixture correction waits1100ms after the second registration before its first
name RPC, allowing the minimum refresh interval to elapse. No UUID RPC warms it,
and SDK60s budget and completion assertions are unchanged. A background refresh
may learn the record; the documented late-domain, rather than guaranteed cold
lookup, claim remains. Node syntax and git diff checks passed locally; no Cargo
or Go commands were run locally. Repeat the gate with the corrected fixture.
