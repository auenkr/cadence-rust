# Fresh workspace CI — run 40168414544

Run: https://github.com/auenkr/cadence-rust/actions/runs/40168414544  
Workflow: `rust-ci.yml`, workflow dispatch with `clean_target=true`  
Branch: `t3/matching-validation-http-embedded-20261025`  
Exact head: `fade09c501e23212d4d86e64c653057d589d1910`  
Conclusion: success. No binary is stored here.

The job selected `/home/runner/work/_temp/cargo-target-final`; the fresh-target
step succeeded and the Rust cache step was skipped. The job order was strict
workspace Clippy, formatting, then workspace tests; Clippy and tests both
completed successfully. Exit artifacts are `clippy.exit=0` and `test.exit=0`.
The checked-in matching metrics emission oracle ran after tests and succeeded.

Formatting evidence:

- TLS-touched file check: `tls-format.exit=0`.
- Changed Matching runtime format step: success.
- Other focused format step: `fmt.exit=0`.
- Workspace-wide `cargo fmt --all -- --check`: `workspace-format.exit=1`.
  This is broad unrelated formatting drift (the captured diff includes
  `cadence-thrift/tests/go_fixtures.rs`); it is not reported as a pass.

Additional successful evidence: Go Matching owner-order oracle completed;
the four Go/Rust emission comparisons all returned `result=match` (series
counts 20, 24, 21 and 23); Go and Rust persistence-wrapper outputs are
captured. `rust-binary.sha256` hashes the `matching_metrics_emission_oracle`
example, not the Matching server executable.

Source and exit/target files, compact Clippy/test summaries, owner-order JSON,
persistence outputs, metric comparison results and oracle executable hash
are selectively retained in this directory. Original Actions artifact ZIPs
are stored outside the repository at
`/mnt/storage/service/ci-evidence/run-40168414544/`:

- `rust-ci-logs.zip` SHA256:
  `feedb8836a077f21386cedb5fefa01e8f563997c07cbdb9e182f15515d3946f2`
- `matching-metrics-emission-evidence.zip` SHA256:
  `4234cd2d5257d70832ef26fd6bee7b5a3fe358d04e9f26236ac8e7639d0f5b3b`

## Remaining limits

This proves the full workspace gate on the named source only. It does not
resolve the separate HTTP TLS interop failure 40168201737, which failed before
the TLS assertions; its diagnosis/correction is being handled separately.
The proposed `84cadec2` guard was rejected by independent source review because
Go accepts the empty request through downstream handling and domain precedence
must remain intact. Do not merge that proposal or treat this workspace pass as
HTTP interop or Matching acceptance. No second mixed lane was dispatched.
