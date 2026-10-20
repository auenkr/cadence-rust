# Rust to Go Matching gRPC peer oracle

Run from the repository root with:

```sh
python3 tests/matching-forward-peer-oracle/run.py
```

The runner starts `main.go`, which registers Cadence's generated
`MatchingAPIYARPC` procedures on a real Go YARPC gRPC inbound. It then runs the
Rust test against that listener. The poll assertions traverse
`Parent::Remote` for both task types and verify typed fields, the three YARPC
routing headers, and that a gRPC deadline reaches the Go handler. The same Rust
test exercises the three other remote Matching gRPC methods present in the
Rust source (`AddDecisionTask`, `AddActivityTask`, and `QueryWorkflow`) and
checks their typed responses and observed headers/deadlines.
