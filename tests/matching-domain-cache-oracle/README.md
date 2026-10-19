# Original Go domain-cache oracle

`run-go-oracle.sh` temporarily stages the adjacent Go test into the original
`go/common/cache` package, runs that real `DefaultDomainCache` implementation,
then removes the staged file. The deterministic JSON line is emitted as
`GO_DOMAIN_CACHE_TRACE=...`.

The trace exercises the metadata-first two-page scan and strict notification
cutoff, Matching's max-failover catch-up callback, update callback ordering,
atomic preservation when a later page fails, and name/ID cache-miss errors.
The Go implementation remains unchanged after the runner exits.
