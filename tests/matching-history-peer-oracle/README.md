# Matching to History Go placement oracle

Run from the repository root:

```sh
python3 tests/matching-history-peer-oracle/run.py
```

The runner stages `oracle_test.go` into `go/client/history` with Go's file
overlay, so the production `history.NewPeerResolver` is exercised. The test
injects a generated membership mock whose lookups delegate to the real
`common/membership.Ring`, initialized from three named-port History hosts. It
records workflow/domain/shard routes (including an out-of-range rune), both
transport port choices, address lookup, refreshed removal, and service
transient error conversion. It is not an implementation of the routing
algorithm in the test harness.

The runner writes its fixture, console log, and SHA-256 source manifest under
`tests/matching-history-peer-oracle/evidence/` and copies the same files to
`memory/evidence/matching-history-peer-oracle/`.
