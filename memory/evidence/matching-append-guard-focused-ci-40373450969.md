# Matching append guard focused CI evidence

- Run: https://github.com/auenkr/cadence-rust/actions/runs/40373450969
- Workflow conclusion: `success` (2026-10-26)
- Tested source SHA: `f6b29a558a6716f9c428e9a321b7fd8d8252eb63`
- Tag: `matching-append-guard-f6b29a5` (points to the tested SHA)
- Original logs artifact: `rust-ci-logs` (artifact ID `12673352824`, retention 3 days)
- Validation target: `/home/runner/work/_temp/cargo-target-final` (fresh dedicated target)
- Identity recorded by artifact: `commit.txt` equals the tested SHA; `target-dir.txt` equals the target above.

## Results recorded in the artifact

- `clippy.exit`: `0`
- `test.exit`: `0`
- `fmt.exit`: `0` (`Formatting` step)
- `matching-runtime-format.exit`: `0`
- `workspace-format.exit`: `1`. The workflow runs `cargo fmt --all -- --check` and deliberately exits 0 from that step (`continue-on-error: true`); `format.log` contains rustfmt diffs outside the changed Matching runtime files. Therefore the overall CI success does not establish workspace-wide formatting cleanliness.
- `test.log` contains `runtime_shutdown_waits_for_real_delayed_append_and_fatal_notification ... ok`.
- `test.log` contains `standalone_matching_sqlite_malformed_run_id_exits_process_with_two ... ok` (the `sql_writer_fatal_process` integration test; 1 passed).
- The live Go tally versus Rust Matching emissions comparison was skipped for this focused run. This run is CI build/test evidence, not mixed-cluster compatibility evidence.

## Reproduction and log integrity

Download with `gh run download 40373450969 --repo auenkr/cadence-rust --name rust-ci-logs --dir <directory>`. In the extracted artifact, inspect `cadence-rust/cadence-rust/rust/commit.txt`, `target-dir.txt`, the `.exit` files, and the named test entries in `test.log`.

SHA256 of original artifact files:

| Artifact file | SHA256 |
| --- | --- |
| `clippy.log` | `8e94414262015703474491644e7f7d4a3674fe7b95b68b917848c44e76a042d1` |
| `test.log` | `27b815823cf477def288bc043b2f739f2ce922c11c485af06d9a6d4bc002aabd` |
| `format.log` (workspace `cargo fmt --check`) | `1c279918586e74a8cd88c0508fe592d80eae75065f5c07fb73d6e1f787df558c` |
| `fmt.log` (Formatting step) | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `matching-runtime-format.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `clippy.exit` | `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa` |
| `test.exit` | `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa` |
| `fmt.exit` | `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa` |
| `matching-runtime-format.exit` | `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa` |
| `workspace-format.exit` | `4355a46b19d348dc2f57c046f8ef63d4538ebb936000f3c9ee954a27460dd865` |
| `commit.txt` | `adf43446b2707486647652dd1243d8fa0ca2935745b1bc14e3f05025eae9033c` |
| `target-dir.txt` | `f2e8e5be63d5f88f972b21dd7011c9862114aa2a59e1526e8c0671550a6af630` |

The exit-file hashes correspond to the two-byte text `0\n` or `1\n` as listed above.

## Same-SHA cross-check and scope limits

The ordinary master run [40373190531](https://github.com/auenkr/cadence-rust/actions/runs/40373190531) also completed successfully for the same SHA. The combined run [40373705020](https://github.com/auenkr/cadence-rust/actions/runs/40373705020) was observed as in progress for a different SHA (`10692b2d5a5b912da051a80ca15c30f8236484dd`); this note makes no claim about its result. No mixed-cluster gate, binary hash, or Matching service parity completion is established by this evidence.
