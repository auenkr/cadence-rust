# Matching panic quote CI evidence

- Workflow run: https://github.com/auenkr/cadence-rust/actions/runs/40407868559
- Workflow: `rust-ci`; terminal conclusion: `success` at 2026-10-26 21:57:39 UTC.
- Source head: `266259e4643e62fb54088e14e6009c0d64fd51eb` (verified in the run's `rust/commit.txt` and Go oracle `source-sha.txt`).
- Runner/limits: `ubuntu-latest`, workflow job timeout 90 minutes, concurrency group cancels superseded runs. This was the `cadence-matching` package run; no database or mixed-cluster gate ran.
- Fresh target: `/home/runner/work/_temp/cargo-target-final` from `rust/target-dir.txt` (`clean_target=true` selected it). Cargo cache step was skipped for the clean target. CI ran Clippy, formatting, then tests sequentially.
- Exit files: `clippy.exit=0`, `test.exit=0`, `fmt.exit=0`, `matching-runtime-format.exit=0`; `workspace-format.exit=1` records the advisory whole-workspace check finding pre-existing formatting differences. The scoped Formatting job step, captured by `fmt.exit`, passed. The Matching crate suite passed (725 passed, 0 failed, 3 ignored), including `go_quote_matches_go_control_and_unicode_escaping`.
- Go oracle: artifact `matching-describe-tasklist-go-oracle`, command runs the original Go `TestMalformedDescribeTaskListOracle` and `TestOptionalPayloadErrorOracle`; `go-oracle.exit=0`, both tests show PASS in `go-oracle/go-oracle.log`. Oracle source is pinned to the same source head.
- The run did not build a production Matching binary or record a Rust binary SHA256. It is a focused crate validation, not mixed-cluster evidence.

## Immutable artifact and file hashes

The original `rust-ci-logs` ZIP is retained as `rust-ci-logs.zip`; GitHub artifact ID `12692656156`, API digest `sha256:b7b165e19461877790ad26d42248e5dc3578b64742350e8f69785049efedde80` (verified against the downloaded ZIP). The Go oracle artifact ID is `12691584025`, API digest `sha256:627851568778c60429e489b0589afed65521e573b89a82e6663f67e04b8cb347`.

`SHA256SUMS` records each retained extracted log/exit/identity file and each Go oracle artifact file. These hashes allow the locally preserved evidence to be checked against the downloaded contents. The workspace-wide `format.log` is available in the immutable `rust-ci-logs` artifact; it is omitted from the extracted subset because it is a large repo-wide diff report.

## Cassandra focused run checked alongside this evidence

Run https://github.com/auenkr/cadence-rust/actions/runs/40408405589 was checked once on 2026-10-27 and remained `in_progress` on head `ca11f306ceffaf37630e291672a1da41a525bbda` (`t3/matching-cassandra-uuid-cache`). Clippy had passed and Test was running. Terminal conclusion and final artifacts are pending; no terminal result is claimed here.
