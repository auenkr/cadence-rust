# Cadence patch to `mysql_async` 0.37.1

This directory vendors the unmodified `mysql_async` 0.37.1 crate, then applies
Cadence's socket timeout support in `src/io/mod.rs` and its TLS upgrade modules.
The workspace patches crates.io to this local copy so the existing async MySQL
driver can honor Go MySQL DSN `timeout`, `readTimeout`, and `writeTimeout`
semantics without changing the synchronous task-store driver.

The source archive was downloaded from crates.io:

`https://static.crates.io/crates/mysql_async/mysql_async-0.37.1.crate`

SHA-256: `40d11da0e2d9fad4640c9f9198ee431c6d68444568f83ef1f10f3367270071e4`

The crate's upstream `LICENSE-MIT` and `LICENSE-APACHE` files are included.
The patch also retains upstream's `Cargo.toml` and package metadata.

The TLS handshake wrapper carries the absolute read deadline from the server
greeting and write deadline from the MySQL SSLRequest into the raw TLS
handshake, matching Go's inherited socket deadlines. Once TLS is established,
the wrapper disables those inherited deadlines and `Endpoint` applies fresh
read/write deadlines to each TLS I/O operation.
