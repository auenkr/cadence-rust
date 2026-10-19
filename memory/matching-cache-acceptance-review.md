# Matching domain-cache acceptance review — 2026-10-19

Draft 66006fd0f remains unintegrated. Parent reviewed Go common/cache/domainCache.go and common/backoff/retrypolicy.go/retry.go against cache source.

Confirmed Go semantics: newly discovered records do not trigger changed callbacks; existing records are replaced even with older/equal notification versions. Do not invent monotonic rejection. Exponential retry policy applies 80–100% jitter in shared ComputeNextDelay; the initial worker review incorrectly described it as plain exponential and was corrected.

Corrections assigned to cache_luna before acceptance:
- Whole metadata+pages refresh inherits the outer three-second context, rather than giving each page an independent full timeout.
- Retry stops when remaining clamped interval is below its initial one second, preserving previous-attempt error on terminal conditions.
- Saturating deadline subtraction in clock wait.
- Avoid joining the cache worker from its own Drop, and join handles correctly when stop races startup/publication.
- Audit SQLite progress-handler cancellation classification against Go deadline errors.

Worker saved actual Go oracle output and structural trace comparison after initial evidence review found only Rust trace output. New fixes are unbuilt; runtime-owner holds the serialized build slot. No claim of production cache wiring, backend parity, or Matching completion follows from these component artifacts.
