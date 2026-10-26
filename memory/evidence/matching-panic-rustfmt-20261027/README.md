# Matching panic/cache rustfmt patch evidence

- Candidate base: `91c481a61` (`origin/t3/matching-combined-20261027`).
- Original CI cited for formatting debt: [run 40407868559](https://github.com/auenkr/cadence-rust/actions/runs/40407868559), head `266259e4643e62fb54088e14e6009c0d64fd51eb` (`t3/matching-panic-string-escaping`). It passed, but had no artifact containing a formatting diff for the three requested files. Its downloadable `rust-ci-logs` artifact was not the formatter patch.
- Added a narrowly scoped workflow dispatch input and step in commit `9d755cf5636fec8eb8aeff61b2f05dd44861c522`, naming only `matching_panic.rs`, `go_strconv_isprint.rs`, and `matching_domain_cache.rs`. GitHub Actions `rustfmt --edition 2021 --config skip_children=true` generated `panic-format.patch`; the uploaded artifact was downloaded verbatim to this directory. SHA256: `59ae35d3c7d173c515c534ffc236cd409f6c8287dc407b7971bc652e5b82e326`.
- Formatter run: [40409338383](https://github.com/auenkr/cadence-rust/actions/runs/40409338383), head `9d755cf5636fec8eb8aeff61b2f05dd44861c522`. The formatting and artifact upload steps succeeded. This workflow's existing dispatch also proceeded into workspace Clippy/test, contrary to the requested formatter-only scope; the run was cancelled after the artifact was uploaded. Do not treat that run as validation evidence.
- Applied the artifact patch with `git apply` after `git apply --check`. Its file list is exactly the three requested Rust files. Only the generated diff is preserved here; no local Cargo or rustfmt command was run. `git diff --check` passed.

The format patch is mechanical output from the runner's pinned stable rustfmt toolchain. The Unicode scalar table in `go_strconv_isprint.rs` is kept as runner output and was not hand formatted.
