# Matching SQL fatal subprocess fixture correction (2026-10-26)

## Finding

The failing subprocess case was not reaching `TaskWriter`. The fixture launched `cadence-matching`, whose `main.rs` restricts its service set to Matching. With no `--services` override, the launcher serves only the health router on the HTTP port, so the test's made-up UUID was never registered in `AppState.domains`. `AddDecisionTask` calls `domain_name()` in `grpc_matching.rs` before opening the durable queue; `grpc_internal.rs` returns a domain lookup status at that point. The test accepted any RPC error, then waited for a fatal writer event that could not exist. The writer/runtime typed fatal channel wiring was not the cause of this failure.

Evidence: focused run [40222090786](https://github.com/auenkr/cadence-rust/actions/runs/40222090786) at head `ff91ef4bde65fe86972831f02177e7a6bd862102` passed Clippy and the previous unit/integration suites, then failed at `tests/sql_writer_fatal_process.rs:56` after 20 seconds with `fatal writer notification did not terminate process`. The source path above explains why the child remained healthy.

## Change

Commit `7ec130ced39c109a13f2024efae6dc1c41dfe7e1` on `t3/sqlite-writer-fatal-exit` moves the process test into `cadence-server`, the package that owns the actual multi-service server binary. It now starts `cadence-server` with Frontend and Matching, waits for both listeners, registers a domain through the real HTTP API, and submits malformed RunID to Matching over gRPC. The test asserts the RPC's SQL RunID error before waiting for exit code 2. The child is guarded and logs stdout/stderr to a file; timeout and early-RPC failures include the RPC result and child log. The accepted Go wrapper/URN RunID cases and the nonfatal bypass/error cases were not changed.

## CI evidence and limits

- Focused failure: run `40222090786`, head `ff91ef4bde65fe86972831f02177e7a6bd862102`, failed as described above.
- Candidate workflow observed once: `40222413373`, head `41739ea94d56b00de5eacbb729d513f4abeb6528`, was still in progress at inspection; no result inferred.
- Fix validation: [run 40222934429](https://github.com/auenkr/cadence-rust/actions/runs/40222934429), dispatched on head `7ec130ced39c109a13f2024efae6dc1c41dfe7e1` with package selection `cadence-server -p cadence-matching`. It was queued when recorded. No Cargo, Go, or rustfmt command was run locally. Result remains pending; do not treat the test or crate validation as passing until Actions reports completion.
- The regression still exercises the embedded data-dir SQLite store. It now tests the production multi-service binary and public Frontend registration path; parity against an explicitly configured external default SQL store remains outside this focused subprocess regression.

## Correction to the follow-up fixture (2026-10-26)

The server-package follow-up did register a valid domain and reach the SQL writer, but it asserted the wrong executable contract: `cadence-server/src/main.rs` returns the launcher result directly and does not map `SqlMatchingWriterFatal` to process exit 2. Only `cadence-matching/src/main.rs` owns that mapping. This regression must therefore run the actual Matching-only binary.

The corrected test now lives in `rust/crates/cadence-matching/tests/sql_writer_fatal_process.rs`. It creates a valid domain with `InMemoryDomainStore`, persists `domains.json` in the child data directory, starts `CARGO_BIN_EXE_cadence-matching`, then issues malformed-RunID AddDecisionTask over gRPC. The RPC has an explicit gRPC timeout and an outer Tokio deadline; startup and process termination are bounded; the child guard kills/reaps on failure and logs are retained for assertions. A successful Add is rejected, exit code 2 is required, and the typed `invalid SQL Matching RunID` marker in the child log proves the writer was reached even if process termination races transport closure.

The old pending validation run `40222934429` targeted the flawed server-package fixture with `package=cadence-server -p cadence-matching`; this is not a valid `PKG` selector because rust-ci expands it as `cargo ... -p $PKG`. It must not be cited as validation. Focused CI for the corrected test is dispatched with `package=cadence-matching` on the corrected pushed head. No Cargo, Go, or rustfmt command was run locally. Validation remains limited to the focused Matching crate process regression; no mixed-cluster test or broader parity claim is made here.

Corrected source was committed and pushed as `536d86528f5b0550b7d0805ea8468efc1ab45a15` (parents include `origin/master` and the just-pushed `origin/t3/sqlite-writer-fatal-exit`, whose history includes candidate `41739ea94`). Focused fresh-target dispatch: [run 40223333379](https://github.com/auenkr/cadence-rust/actions/runs/40223333379), workflow `rust-ci`, ref `t3/sqlite-writer-fatal-exit`, `package=cadence-matching`, `clean_target=true`; observed once as pending, so no pass is claimed yet. Obsolete invalid-selector run [40222934429](https://github.com/auenkr/cadence-rust/actions/runs/40222934429) on `7ec130ced39c109a13f2024efae6dc1c41dfe7e1` was canceled because it tests the incorrect server main mapping and has a whitespace-separated invalid package selector. The repaired CI is only a focused package test; it does not validate Cassandra, Go behavior, or mixed-cluster service compatibility.
