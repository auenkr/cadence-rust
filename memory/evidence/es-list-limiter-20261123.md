# ES visibility list limiter (`frontend.esVisibilityListMaxQPS`) - 2026-11-23

Conclusion: no Rust change required. The Go key is deprecated and never read.

## Go references
- go/common/dynamicconfig/dynamicproperties/constants.go:3786 - default 30; description: "deprecated: never read from, all ES reads and writes erroneously use PersistenceMaxQPS".
- go/service/frontend/config/config.go:45,154 - `ESVisibilityListMaxQPS` loaded via `GetIntPropertyFilteredByDomain`.
- go/service/frontend/service.go:98 - copied into `common/service.Config`; go/common/service/config.go:55 (`yaml:"-"`).
- go/service/history/resource/resource.go:124 - set nil, "history service never read".
- `grep -rn ESVisibilityListMaxQPS go --include=*.go` (non-test) shows no consumer: no limiter, no List/Scan/Count guard, no error path.

## Rust
- rust/crates/cadence-frontend/src/config.rs:91 and cadence-persistence/src/dynamic_config_keys.rs:54 already register the key (Int, default 30), matching Go. Unread, as in Go.
- Adding a limiter would be a behavior change versus Go, so none was added. See also memory/frontend-final-audit-2-20261103.md:177.

## CI
No code changed, so no new CI runs were dispatched (branch head equals master plus this file).
