# Matching to History Go placement oracle evidence

Reproduce with `python3 tests/matching-history-peer-oracle/run.py`. This runs
only `go test ./client/history` for
`TestCadenceMatchingHistoryPeerPlacementOracle`, staged through a temporary Go
overlay. The test exercises production `history.NewPeerResolver` and
`common/membership.Ring` with an injectable `membership.MockResolver` backed by
the actual Ring. It covers three owners, workflow/domain/shard keys, Go's
invalid-rune conversion, gRPC and TChannel named addresses, address lookup,
membership refresh/removal, and unavailable/transient error conversion.

Observed toolchain: `go1.26.8 linux/arm64`; the source module declares Go
1.25.0/toolchain 1.25.14. Focused oracle passed on the available toolchain. The
exact stdout, JSON result, and source SHA-256 manifest are adjacent.
