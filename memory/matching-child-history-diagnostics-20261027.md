# Matching child history diagnostics (2026-10-27)

Extended the existing Go SDK failure diagnostic in `tests/go-sdk/mixed_matching.go`.
The five-second independent diagnostic context now covers fetching the failed
parent history and up to two histories for children identified by
`ChildWorkflowExecutionStarted` attributes, including their actual workflow and
run IDs. Child histories are not recursively expanded. The diagnostic reports
truncation, incomplete execution references, and budget exhaustion. The SDK's
original failure remains the returned cause; suite deadlines and assertions are
unchanged. No production Go or Rust code changed.

Evidence:

- Source commit: `bd223d635`
- CI run: https://github.com/auenkr/cadence-rust/actions/runs/40479687432
- CI inputs: `package=cadence-matching`, `go_sdk_compile=true`
- CI conclusion: pending; not polled, as requested.
- No local Go, Cargo, build, test, format, runtime, or mixed-gate command was run.
