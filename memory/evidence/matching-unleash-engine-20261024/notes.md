# Matching Unleash evaluation engine evidence (2026-10-24)

## Scope and source audit

The Go reference is the cached `github.com/Unleash/unleash-client-go/v4@v4.5.0` plus Cadence's Go OpenFeature adapter at `go/common/dynamicconfig/openfeatureprovider/unleash/unleash.go`. The prior Rust constructor only validated config and returned `NOT_PORTED_MESSAGE`; the master branch already contained repository bootstrap, polling and shutdown from merge `34deee93f`. That polling implementation was integrated and extended only as needed to preserve segment data for evaluation.

The requested preserved checkout `/home/auenkr/.t3/worktrees/cadence-rust/t3-1ac2cfd5` was clean at `f1f576915` with no additional implementation to recover. The repository polling branch was not modified independently; its master merge supplies the existing lifecycle code.

## Evaluation behavior added

- Decode sparse and `null` feature, segment, strategy, dependency, constraint, variant and payload fields as Go zero values.
- Evaluate built-in strategies in server order. Stop at the first match; a matching strategy with variants requires a string `groupId`. A missing referenced segment disables evaluation immediately. Enabled checks and variant selection share one evaluation result, so random strategies are not evaluated twice.
- Match Go rollout normalization (`Murmur3(groupId + ":" + id) % 100 + 1`), variants, overrides, payload defaults and dependencies.
- Port string, numeric, RFC3339 date, semver and IP/CIDR constraints, including IPv4-mapped IPv6 behavior.
- Wire boolean, string, integer, float and object resolution into `UnleashProvider`, including readiness/context errors, caller defaults, payload parsing and type mismatch behavior. Application hostname comes from a process-cached OS hostname, not the mutable `HOSTNAME` environment variable.

## Reproducible tests and CI

Rust fixtures cover sparse Go JSON, Go rollout buckets for IDs `123`, `999`, `alice`, `bob` at 0/1/50/100 percent, ordered strategy selection, absent group IDs, missing segments, typed payload/default/error behavior, constraints and mapped IP matching. `tests/matching-unleash/go_ipnet_oracle.go` runs the standard library `net.ParseCIDR` and `IPNet.Contains` checks under Go 1.25.14: mapped `/120` is IPv4 `/24` and includes `192.0.2.10`; mapped `/121` is IPv4 `/25` and excludes it.

Final run: https://github.com/auenkr/cadence-rust/actions/runs/40085226241

- Source SHA: `23258928956bf84a87edc3de5a1e5fb2bf9dff3b`
- Conclusion: success; job `clippy-test` completed successfully.
- Runner target: `/home/runner/work/_temp/cargo-target-final` (fresh, uncached target).
- Go mapped-IP oracle: passed.
- Clippy exit: `0`.
- Focused Rust formatting check exit: `0`.
- `cadence-service-core` tests exit: `0`.
- Exit files and `commit.txt` were downloaded from the run's `rust-ci-logs` artifact and matched the source SHA above.
- The workflow's workspace-wide `cargo fmt --all -- --check` remains report-only due existing formatting debt; it does not rewrite sources. The focused engine `rustfmt --check` passed.

An earlier run `40084880112` failed one newly added IP exclusion assertion because `/120` was mistakenly expected to exclude `.10`. The test was corrected to `/121`, and the Go oracle confirms both prefix semantics; IP masking was not changed to satisfy the incorrect assertion.

## Remaining lifecycle parity; hand off to the next worker

This evaluator pass does not complete provider lifecycle parity. The Go client `client.go`/`metrics.go` behavior remains unimplemented in Rust:

- `metricsInterval` is parsed but there is no `/client/register` request.
- Evaluation counts and variant metrics are not accumulated or posted to `/client/metrics`.
- Provider shutdown does not flush/join a metrics sender as Go `Client.Close` does.
- Repository polling does not send/use ETags (`If-None-Match`) or handle `304 Not Modified` according to Go repository behavior.

Leave those lifecycle tasks to the next worker after this branch is integrated. Do not use the green evaluation CI run as evidence that these lifecycle gaps are closed. Custom strategy registration and broader differential fixtures across every upstream SDK edge case also remain outside this evaluator pass.
