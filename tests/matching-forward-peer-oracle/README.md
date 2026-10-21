# Rust to Go Matching gRPC peer oracle

Run from the repository root with:

```sh
python3 tests/matching-forward-peer-oracle/run.py
```

The runner starts `main.go`, which registers Cadence's generated
`MatchingAPIYARPC` procedures on a real Go YARPC gRPC inbound. It then runs the
Rust test against that listener. The poll assertions traverse
`Parent::Remote` for both task types and verify typed fields, YARPC's
transport-provided service, caller, and encoding values, and that a gRPC
deadline reaches the Go handler. These values come from the Go YARPC call
context; reserved routing metadata does not appear in the application header
map. A second
Rust test calls the production Matching API handlers for decision and activity
adds and workflow queries, with membership routing the child task list locally
and its parent to the Go peer. The runner checks that all five calls reach the
generated Go handlers with nonempty typed request fields, YARPC service,
caller, and encoding values, and a gRPC deadline. Rust asserts typed response
fields.
