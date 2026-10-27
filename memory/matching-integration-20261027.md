# Matching integration, October 27

## History wire check

Focused run https://github.com/auenkr/cadence-rust/actions/runs/40464209349
succeeded on fb0cd3d08a6d56c07c3fa17df51583a6a39eeabc. GitHub steps confirm
the regenerated Go History fixture comparison succeeded and Rust tests passed.
Actual artifact exit review remains to record before final acceptance. This is
focused CI, not fresh combined workspace or mixed acceptance.

## Adaptive diagnostics

Integrated worker63cba005db472192b29affa729f6f1f46080356e as f911cb730.
Worker run https://github.com/auenkr/cadence-rust/actions/runs/40464652418
on63cba005d confirms Go SDK formatting/compilation and Node/Python syntax steps
succeeded; full CI was still running when inspected. The integrated fixture
keeps the60s suite budget and all24 workflow and listing assertions. New
workflow input adaptive_retry_only runs an additional fresh-process retry
scenario against the scaled queue before the normal suite. Parent extended
diagnostics to retain complete history events, not only counts, and to collect
successful isolated-retry history too. These follow-up edits require CI.

## Cassandra patch held

Do not merge e2f2bd5cd despite focused CI and the initial no-defect review.
Pinned gocql accepts alternate UUID spellings, but original Go
common/cache/domainCache.go getDomainByID:625-680 performs raw-string cache
lookups before and after metadata refresh. Its final raw-key miss produces
InternalServiceError even when Cassandra persistence finds the canonical row.
Matching handler.domainName returns empty on that error. Thus canonicalizing
before the cache lookup may introduce a metrics/context behavior divergence.
A replacement Luna test task owns an original-Go cache caller oracle. Await
that evidence before either accepting normalization or closing the old finding.

Active assignments: pinned-Go panic quoting repair, Go domain-cache caller
oracle, and Matching isolation production-path audit. Matching incomplete;
no final source pin and no final mixed acceptance is claimed.

## Integrated diagnostic validation and mixed dispatch

Run https://github.com/auenkr/cadence-rust/actions/runs/40465258975 succeeded
on6f001112c67a572a6868be09df7eeed143725889 (focused Matching CI with optional
Go SDK compile checks enabled). This includes the parent's complete-history
diagnostics. Actual artifact exits still need retention/review.

Adaptive mixed experiment https://github.com/auenkr/cadence-rust/actions/runs/40465991711
is running on c268c99cde16c4b12e7ab7edd048c13443329ad3, with lane=adaptive and
adaptive_retry_only=true. This source includes diagnostics but predates pending
panic quoting and cache caller-oracle work; it is a diagnostic run, not final
Matching acceptance. No other mixed gate was active at dispatch. Inspect actual
retry-only/full-suite histories, timing, all24 completion checks and live queue
routing results before drawing a conclusion. Preserve binary SHA256.

## Provisional acceptance source v3

Immutable tag matching-acceptance-20261027-v3 points to
ff3b638b49190f7653c2de3b062232d7ccc57f07. Fresh workspace CI with SDK compile
checks enabled is running at
https://github.com/auenkr/cadence-rust/actions/runs/40467005617.
This is a provisional candidate, not acceptance. Pending Go cache caller-oracle
evidence or the adaptive diagnostic result may require further source changes
and a new tag; never move this tag to conceal such changes.

The candidate integrates worker9740bd025 +4ef51d867 pinned-Go quote tables.
Parent review fixed the lower-bound lookup (printable range starts and singleton
ranges were incorrectly excluded), added boundary comparisons, and exported the
Go oracle path in the *same* Test step before cargo test. The worker's earlier
CI only appended the path to GITHUB_ENV in that step, so its Rust oracle test
could return early without a comparison. Do not cite it as Go-vs-Rust quote
proof. Candidate run40467005617 must establish the actual comparison. Superseded
master runs40466893618 and40466720993 were cancelled.

The running adaptive diagnostic run40465991711 predates these production changes;
regardless of its outcome it cannot satisfy final-candidate mixed acceptance.
