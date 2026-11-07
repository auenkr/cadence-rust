# history-gap-handlerfailover (2026-11-07)
- Handler `NotifyFailoverMarkers` / `GetFailoverInfo` ported from go handler.go using `HistoryEnv.failover_coordinator` (notify: no shutdown check, skips nil markers; get: shutdown check then coordinator, error -> EntityNotExists).
- Outbound TLS: Matching channel resolver (launcher.rs) now dials with `client_tls(services.matching.rpc.tls)` like Frontend/Matching (was always plaintext).
- Not done: replication remote-cluster channels (`replication_clients.rs`) still use `None` TLS; Go uses the remote cluster's client TLS config. History->History peer dials are not implemented in Rust yet.
- CI: see below.
- CI: https://github.com/auenkr/cadence-rust/actions/runs/42341453064 head 677cca98 conclusion success (cadence-history clippy+test).
