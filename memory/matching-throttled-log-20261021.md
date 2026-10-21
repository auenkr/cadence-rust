# Matching throttled log status (source only)

ThrottledLogRPS (`matching.throttledLogRPS`) remains not implemented in Rust.
Go implementation: `go/common/log/throttle.go` (NewThrottledLogger) wraps a
Logger and uses `quotas.NewDynamicRateLimiter` with `rps()` dynamic property;
Fatal/Panic bypass? Note Go's Fatal calls rateLimit (does NOT bypass) in this
version; Panic not defined. All methods Debug/Info/Warn/Error/Fatal/SampleInfo
are token-bucket gated by Allow().

Rust logging uses `tracing` across cadence-server/cadence-common; there is no
equivalent dynamic throttled logger wrapper. Implementing exact semantics
requires a shared logging trait/wrapper in cadence-common and plumbing the
dynamic config property into every Matching logger call site, including
ValidateLongPollContextTimeoutIsSet in longpoll validation. This is a
cross-cutting logging infrastructure dependency, not a Matching-only change, and
is blocked on selecting the Rust logging abstraction. Source-only; no build run.
