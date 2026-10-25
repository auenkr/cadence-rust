# Go malformed DescribeTaskList oracle setup failure — run 40169963237

Run: https://github.com/auenkr/cadence-rust/actions/runs/40169963237  
Source: `3a1996c800f2d03209116497613bb43c6e065125`  
Oracle artifact: https://github.com/auenkr/cadence-rust/actions/runs/40169963237/artifacts/12589638974  
Artifact ZIP SHA256: `8b63ddd685ddfedb9a27f3ab9f15e19e2dba09731b2d7f39f069aa9f251bbc69`

The Actions-only original Go handler/engine oracle exited 1 before emitting
case results. The fresh `MockDomainCache` omitted an expectation for the empty
domain ID used by the absent-outer case, so gomock reported an unexpected
`GetDomainName("")` call. No malformed-request behavior can be inferred from
this run. The Go oracle source now includes that explicit case. The workflow
failed before any Cargo step; focused Rust validation did not run.
