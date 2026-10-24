# Unleash lifecycle parity remaining after evaluation

Current master polling implements bootstrap, periodic feature fetch, snapshot
retention and bounded shutdown. The separate active engine worker owns local
strategies/variants and provider integration. Neither slice alone establishes
Go Unleash-client parity.

Primary reference:
`/home/auenkr/go/pkg/mod/github.com/!unleash/unleash-client-go/v4@v4.5.0`.
Go adapter is `go/common/dynamicconfig/openfeatureprovider/unleash`.

Confirmed source gaps in master polling:
- `metricsInterval` is parsed but not consumed. Go client constructs metrics,
  registers `/client/register`, counts enabled/disabled/variant evaluations and
  periodically posts `/client/metrics`; inspect metrics.go for intervals, body,
  headers, retry/error behavior and shutdown semantics.
- Repository fetch does not retain/send ETag or If-None-Match. Inspect Go
  repository.go's exact HTTP 304/cache retention behavior before porting.
- Default instance ID is not necessarily Go's user/hostname identity; compare
  utils.go and option defaults, alongside environment/static context handling.

Next narrow worker should implement lifecycle independently from the engine,
with fake HTTP server tests covering request bodies/headers, count reset and
failed-report behavior, conditional refresh, configured intervals and stopping
all threads. Do not run Cargo locally. Use fresh-target CI and record exact
source/run URL/results. Reconcile engine changes before modifying the shared
Unleash file; do not race the current engine worker.

Review found a separate engine bug: raw Murmur3 was compared directly with
rollout percentage. Go helpers.go uses hash%100+1; worker was notified to fix
and add differential fixtures before integration. Green package tests without
these fixtures were insufficient.

## Completion record (2026-10-24)

The lifecycle task is implemented and validated at source SHA
`cbd1b41ea10403f437e2dbc40ca89f98157bf34d`. Fresh-target service-core and
workspace CI both passed. See
[`memory/evidence/matching-unleash-lifecycle-20261024/notes.md`](evidence/matching-unleash-lifecycle-20261024/notes.md)
for run URLs, source audit, fake-server coverage, and known parity limits.
