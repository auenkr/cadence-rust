# Original Go DescribeTaskList oracle — run 40171379726

Run: https://github.com/auenkr/cadence-rust/actions/runs/40171379726
Source SHA: `5cafc2dc941fc676cad66d6756fb812c502aa5bf`
Oracle exit: 0. The raw log now explicitly records poller counts for successful
requests; `absent-nested` and `empty-task-list` both returned a non-nil response,
zero pollers, and nil error. The test asserts those results.

Other outputs match the earlier oracle capture: absent outer request recovers a
nil-pointer panic; malformed reserved `/__cadence_sys//0` returns
`*types.InternalServiceError` with exact message `invalid partitioned task list
name /__cadence_sys//0`, including when the domain ID is unknown. Unknown-domain
errors for ordinary requests carry a synthetic mock error and do not establish
production error mapping. See `go-oracle.log` for all cases and `command.txt`,
`overlay.json`, `go-oracle.exit`, and `source-sha.txt` for reproduction identity.
