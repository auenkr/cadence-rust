# Cadence patch to mysql 26.0.1

This copy of [mysql 26.0.1](https://static.crates.io/crates/mysql/mysql-26.0.1.crate)
starts the synchronous driver's read and write framing buffers at 4 KiB each.
The upstream constructor reserves 4 MiB per buffer on connection setup. The
buffers grow as packets are read and written. The packet codec, its default
packet limit, and limit negotiation are unchanged.

The upstream archive SHA-256 is
`ce2510a735f601bab18202b07ea0a197bd1d130d3a5ce2edf4577d225f0c3ee4`.
