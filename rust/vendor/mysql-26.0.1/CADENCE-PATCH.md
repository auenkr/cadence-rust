# Cadence patch to mysql 26.0.1

This copy of [mysql 26.0.1](https://static.crates.io/crates/mysql/mysql-26.0.1.crate)
starts the synchronous driver's read and write framing buffers at 4 KiB each.
The upstream constructor reserves 4 MiB per buffer on connection setup. The
buffers grow as packets are read and written. The packet codec, its default
packet limit, and limit negotiation are unchanged.

The upstream archive SHA-256 is
`ce2510a735f601bab18202b07ea0a197bd1d130d3a5ce2edf4577d225f0c3ee4`.

Diff versus upstream 26.0.1 (verified against the crates.io archive): the only modified file is `src/conn/mod.rs`. `MySyncFramed::new(stream)` is replaced with `MySyncFramed::construct(BytesMut::with_capacity(4096), BytesMut::with_capacity(4096), PacketCodec::default(), stream)`, plus the `BytesMut` and `PacketCodec` imports. The 4 KiB initial size matches the Go MySQL driver. Reason: upstream reserves 4 MiB per buffer, so six or more live connections hold about 32 MiB in History at c1. Upstream-only files `Cargo.lock`, `Cargo.toml.orig`, `.cargo_vcs_info.json` and `azure-pipelines.yml` are omitted.
