# Go DescribeTaskList oracle and workspace CI — run 40171379726

Run: https://github.com/auenkr/cadence-rust/actions/runs/40171379726

Source: `5cafc2dc941fc676cad66d6756fb812c502aa5bf`

Conclusion: success. This dispatch used the standard Rust target and cache;
`clean_target=true` was not set, so this is not a fresh uncached final gate.

The original Go handler/engine oracle ran successfully with exit 0 before
Clippy and workspace tests. It records absent-nested and empty task-list
requests as success with zero pollers, and the malformed reserved task-list
as `InternalServiceError` with the exact invalid-partition message. Unknown
domain errors produced by the synthetic mock are not production error-mapping
evidence. See `go-oracle.log`, `overlay.json`, and the branch's
`memory/evidence/matching-describe-tasklist-go-oracle-40171379726/` record.

Clippy and workspace tests both succeeded, in that order; captured exits are
0 and test summary results are in `test-results.txt`. The job shows the Rust
cache action ran and fresh-target selection was skipped. Focused TLS formatting
passed (`tls-format.exit=0`); the workspace-wide format advisory remains
`workspace-format.exit=1`. The standalone `fmt.exit` was 0. The owner-order
oracle step also succeeded. Exact run metadata and compact logs/exits are
retained here. Original artifact ZIPs are outside the repository at
`/mnt/storage/service/ci-evidence/run-40171379726/`; their hashes are recorded
in `artifact-archives.sha256`. No executable binary was retained.

## Separate HTTP TLS failure

Run [40171381762](https://github.com/auenkr/cadence-rust/actions/runs/40171381762)
failed on the same source before reaching TLS assertions. Parent-reported
stage: configured decision-task delivery over TChannel/HTTP/trailing behavior;
embedded TChannel passed, then a membership “not enough hosts” warning occurred
and TLS was not reached. The Sonnet worker owns diagnosis; this record adds no
independent cause or proposed fix. The unmodified Actions artifact is preserved
at `/mnt/storage/service/ci-evidence/run-40171381762/matching-http-tls-interop.zip`
with SHA256 `b416ad16410b27a6fd1ee18c58c6e0de400404451a2c65cd8e7ac570bf2d266f`.
No workflow was dispatched for this integration task.
