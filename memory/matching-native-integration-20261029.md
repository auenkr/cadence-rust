# Native Matching integration — 2026-10-29

Matching remains incomplete. Continue Matching only.

Resumed from master `1b27f897b`. Integrated the completed deadline worker's
four commits from `t3/matching-deadline-cancellation` into
`matching-native-integration-20261029`. The current master launcher already
uses the six-argument scoped registry attachment, so integration retains that
fix instead of reverting to the worker's older launcher. The worker explicitly
calls the byte-store trait for its absent-lease assertion, avoiding the SQLite
inherent method's UTF-8 key type.

Added task partition metadata to `NativePollObserver::poll_match`, passing the
actual delivered task's map through the native dispatcher. Query polls pass
None, matching the legacy query path. The metrics adapter resolves the exact
ASCII Go map keys and converts only metric label values at the existing sink
boundary. Regression cases cover original-group precedence, fallback to current
group, success versus mismatch counters, and absent partition data. This closes
the missing metadata transport described in
`matching-native-scoped-metrics-20261028.md`; that note's invalid-UTF-8 metric
label collision limitation still applies. No routing or persistence conversion
was introduced.

## CI evidence

- Worker run https://github.com/auenkr/cadence-rust/actions/runs/40730103135
  at `58e5d142b3300acb8cabc7d55981fe4423d547e1`: failure at Clippy,
  launcher argument mismatch plus lease-assertion method resolution; tests did
  not run.
- Worker replacement https://github.com/auenkr/cadence-rust/actions/runs/40748657461
  at `5514316dc4c5bfbd3fc2b3fc541bba354e98fbaf`: cancellation requested after
  integration because its launcher predates the correction. No pass claimed.
- Initial integrated run https://github.com/auenkr/cadence-rust/actions/runs/40748782064
  at `bc18bb92eda879df542521827c6c000ba39f86cf`: cancellation requested after
  the metrics addition; superseded, no pass claimed.
- Current six-crate run https://github.com/auenkr/cadence-rust/actions/runs/40749013377
  at `27bda0e326995615957fea3d855cf5a4a2fac54d`: pending at inspection,
  clean target and runner formatting requested. No compile, test or formatting
  pass claimed. Download and apply its formatting patch, address any errors,
  and rerun on the resulting source before final validation.

Local validation: `git diff --check` passed. No local Cargo, rustfmt, Go or
heavy validation ran. Repository visibility verified public; workflows use
standard hosted Ubuntu runners. No replacement subagents were started after
the user's stop instruction.

Next: finish selected CI fixes/formatting, then fresh uncached workspace Clippy
followed by workspace tests. Freeze that source and run mixed gates serially
using `matching-acceptance-evidence-readiness-20261027.md`, preserving exact
source identities, binary SHA256, original Go peer/no Go Matching topology,
SDK assertions, failure/restart/recovery results and cleanup evidence. Earlier
mixed results predate native production activation and cannot close acceptance.

## Continuation audit and CI outcome

Run https://github.com/auenkr/cadence-rust/actions/runs/40749194152 at
`583c10e237368ec607ec7fd04e2e8f0392e07859` concluded failure. Clippy reached
Matching and reported only `select_isolation_bytes` having nine arguments.
Tests did not run. Applied its runner formatting artifact to 26 files; resolved
three overlaps with subsequent cache corrections. Added a narrowly scoped
argument-count allowance consistent with the native assembly helpers.

Source audit found that native control, semaphore domain resolution and
standby cache adapters mapped all cache failures to NotFound. Extracted the
existing legacy handler classification into `DomainCacheError::into_status`
and shared it with these native call sites. Genuine NotFound retains Cadence
EntityNotExistsError details; BadRequest remains InvalidArgument and backend,
stopped-cache and exists-but-not-loaded errors remain Internal. New regression
calls native control name/ID resolution through real cache lookups against
four explicitly classified source failures. This is pending CI execution.

The native registry also omitted the legacy builder's live
`matching.recordTaskStartedTimeout` provider. Installed a byte-aware provider
on its History client before registry construction, resolving domain-filtered
configuration through the shared metadata cache. It retains the one-second
default, reads updates on every attempt and passes zero through for the
existing unbounded-operation behavior. Regression checks default, live update,
zero and global fallback with invalid domain bytes. Pending CI execution.

Cold-domain source audit: native control resolved cache entries but did not
publish cold discoveries to `state.domains`, unlike the legacy handler. Since
the active/standby adapters consume that shared view and Go cache callbacks
omit new entries after registration, a newly created standby domain could
default to active. Native name and ID resolution now publish missing metadata
before returning. Regression exercises both lookup paths without a cache
callback and verifies the discovered domain is passive locally and active in
its configured remote cluster. Existing entries retain the callback update
behavior; the publication helper only inserts absent domains.

Workspace run https://github.com/auenkr/cadence-rust/actions/runs/40749929974
at `0b5b97051` was in progress at the Go raw-string oracle when the cold-domain
correction was made. It predates that correction and will be superseded by a
new whole-workspace run. No tests or Clippy pass is claimed. The next source
pin must include the cold-domain correction, typed-cache correction, timeout
provider and runner formatting. PR https://github.com/auenkr/cadence-rust/pull/1
is draft. T3 link/watch calls have failed with “The calling provider no longer
owns an active thread run”; no successful automatic PR watch is established.

Current workspace validation:
https://github.com/auenkr/cadence-rust/actions/runs/40750262783 at
`78bda6afcc3852eabeb4eade2110ca16609bb533` was confirmed live in the Go
raw-string oracle prerequisite. Run 40749929974 concluded cancelled. Preserve
the live run's eventual result; do not restart it merely for waiting.

Follow-up source correction (not covered by that earlier pin): native poll
observers now retain the raw poller isolation group and compare it with the
task's original/current raw group before diagnostic label conversion. The
shared metric emitter accepts that precomputed match result; existing string
callers compute the same equality as before. Added an observer-factory
regression for raw ff matching itself but not raw fe or U+FFFD, which all
otherwise collapse to the same sanitized label. Label rendering remains the
existing sink behavior; a label collision can no longer cause a false
isolation-success increment. This source still needs runner formatting and
fresh CI after reviewing the live earlier-pin run.
