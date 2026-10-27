# Cold canonical domain cache caller oracle

- Actions run: https://github.com/auenkr/cadence-rust/actions/runs/40474548772
- Head SHA: `e5ac70a8f19c32c281d9f3bed182c7a21504291a`
- Conclusion: success; `cadence-matching` CI completed, including clippy, formatting, tests, and the original-Go overlay oracle.
- Go source SHA: recorded in `source-sha.txt` (overlay compiles against the original Go cache implementation).
- Oracle command: recorded in `command.txt`; exit `0` is recorded in `go-oracle.exit`.

The new `cold_canonical_UUID_after_initial_scan` case performs an initial empty cache scan, then looks up a canonical UUID for a domain reported by persistence and included in the subsequent refresh. The cold call returns the domain name; the warm call returns the same name with no further mock expectation for persistence, so an unexpected extra store call fails the suite. The pre-existing alternate-spelling miss assertions still pass. This establishes the original Go cache caller read-through behavior only; it does not test Matching metric attribution or Cassandra UUID parsing.
